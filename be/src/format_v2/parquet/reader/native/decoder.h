// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <gen_cpp/parquet_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <ostream>
#include <vector>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "common/status.h"
#include "core/custom_allocator.h"
#include "core/data_type_serde/parquet_decode_source.h"
#include "core/types.h"
#include "util/rle_encoding.h"
#include "util/slice.h"

namespace doris::format::parquet::native {

inline bool dictionary_indices_in_bounds(const uint32_t* indices, size_t count,
                                         size_t dictionary_size) {
    if (count == 0) {
        return true;
    }
    if (dictionary_size == 0) {
        return false;
    }
    uint32_t max_index = 0;
    size_t row = 0;
#ifdef __AVX2__
    __m256i vector_max = _mm256_setzero_si256();
    for (; row + 8 <= count; row += 8) {
        const auto values = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(indices + row));
        vector_max = _mm256_max_epu32(vector_max, values);
    }
    alignas(32) uint32_t lanes[8];
    _mm256_store_si256(reinterpret_cast<__m256i*>(lanes), vector_max);
    for (const auto lane : lanes) {
        max_index = std::max(max_index, lane);
    }
#endif
    for (; row < count; ++row) {
        max_index = std::max(max_index, indices[row]);
    }
    return static_cast<size_t>(max_index) < dictionary_size;
}

class Decoder : public ParquetDecodeSource {
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    static Status get_decoder(tparquet::Type::type type, tparquet::Encoding::type encoding,
                              std::unique_ptr<Decoder>& decoder);

    // The type with fix length
    void set_type_length(int32_t type_length) { _type_length = type_length; }

    // Set the data to be decoded
    virtual Status set_data(Slice* data) {
        _data = data;
        _offset = 0;
        return Status::OK();
    }

    // Page headers provide an upper bound before encoding-specific headers are trusted.
    virtual void set_expected_values(size_t expected_values) { _expected_values = expected_values; }

    Status decode_fixed_values(size_t num_values, ParquetFixedValueConsumer& consumer) override {
        return Status::NotSupported("Fixed values are not supported by this Parquet decoder");
    }

    Status decode_binary_values(size_t num_values, ParquetBinaryValueConsumer& consumer) override {
        return Status::NotSupported("Binary values are not supported by this Parquet decoder");
    }

    Status skip_values(size_t num_values) override = 0;

    virtual void release_scratch(size_t max_retained_bytes) {}
    virtual size_t retained_scratch_bytes() const { return 0; }
    virtual size_t active_scratch_bytes() const { return 0; }

    virtual Status set_dict(DorisUniqueBufferPtr<uint8_t>& dict, int32_t length,
                            size_t num_values) {
        return Status::NotSupported("set_dict is not supported");
    }

protected:
    template <typename T>
    static void release_vector_if_oversized(std::vector<T>* values, size_t max_retained_bytes) {
        if (values->capacity() * sizeof(T) > max_retained_bytes) {
            std::vector<T>().swap(*values);
        }
    }

    int32_t _type_length = -1;
    Slice* _data = nullptr;
    // Page offsets are host-sized so checked advances cannot wrap at the 4 GiB boundary.
    size_t _offset = 0;
    size_t _expected_values = std::numeric_limits<size_t>::max();
};

class BaseDictDecoder : public Decoder {
public:
    BaseDictDecoder() = default;
    ~BaseDictDecoder() override = default;

    // Set the data to be decoded
    Status set_data(Slice* data) override {
        if (UNLIKELY(data == nullptr || data->size == 0)) {
            return Status::Corruption("Parquet dictionary index stream is empty");
        }
        _data = data;
        _offset = 0;
        uint8_t bit_width = *data->data;
        // Dictionary indices are uint32_t; wider external widths make repeated runs overwrite the
        // decoder's four-byte state before any dictionary-bound check can run.
        if (UNLIKELY(bit_width > 32)) {
            return Status::Corruption("Parquet dictionary index bit width {} exceeds 32",
                                      bit_width);
        }
        _index_batch_decoder = std::make_unique<RleBatchDecoder<uint32_t>>(
                reinterpret_cast<uint8_t*>(data->data) + 1, static_cast<int>(data->size) - 1,
                bit_width);
        return Status::OK();
    }

    bool has_dictionary() const override { return true; }
    uint64_t dictionary_generation() const override { return _dictionary_generation; }

    Status decode_dictionary_indices(size_t num_values, std::vector<uint32_t>* indices) override {
        DORIS_CHECK(indices != nullptr);
        indices->resize(num_values);
        const auto decoded =
                _index_batch_decoder->GetBatch(indices->data(), cast_set<uint32_t>(num_values));
        if (UNLIKELY(decoded != num_values)) {
            return Status::IOError("Can't read enough Parquet dictionary indices");
        }
        const size_t num_dictionary_values = dictionary_size();
        if (UNLIKELY(!dictionary_indices_in_bounds(indices->data(), num_values,
                                                   num_dictionary_values))) {
            // The SIMD common path only computes a bound; recover the exact corrupt row for the
            // diagnostic after the batch has already been proven invalid.
            for (size_t row = 0; row < num_values; ++row) {
                if ((*indices)[row] < num_dictionary_values) {
                    continue;
                }
                return Status::Corruption(
                        "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                        (*indices)[row], row, num_dictionary_values);
            }
        }
        return Status::OK();
    }

    Status decode_selected_dictionary_indices(const ParquetSelection& selection,
                                              std::vector<uint32_t>* indices) override {
        DORIS_CHECK(indices != nullptr);
        const size_t num_dictionary_values = dictionary_size();
        if (_is_fragmented_selection(selection)) {
            return _decode_fragmented_selection(selection, num_dictionary_values, indices);
        }
        indices->resize(selection.selected_values);
        size_t cursor = 0;
        size_t output = 0;
        for (const auto& range : selection.ranges) {
            DORIS_CHECK(range.first >= cursor);
            RETURN_IF_ERROR(_decode_and_validate_skipped(range.first - cursor, cursor,
                                                         num_dictionary_values));
            const auto decoded = _index_batch_decoder->GetBatch(indices->data() + output,
                                                                cast_set<uint32_t>(range.count));
            if (UNLIKELY(decoded != range.count)) {
                return Status::IOError("Can't read enough Parquet dictionary indices");
            }
            if (UNLIKELY(!dictionary_indices_in_bounds(indices->data() + output, range.count,
                                                       num_dictionary_values))) {
                for (size_t row = 0; row < range.count; ++row) {
                    if ((*indices)[output + row] < num_dictionary_values) {
                        continue;
                    }
                    return Status::Corruption(
                            "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                            (*indices)[output + row], range.first + row, num_dictionary_values);
                }
            }
            output += range.count;
            cursor = range.first + range.count;
        }
        DORIS_CHECK(cursor <= selection.total_values);
        RETURN_IF_ERROR(_decode_and_validate_skipped(selection.total_values - cursor, cursor,
                                                     num_dictionary_values));
        DORIS_CHECK_EQ(output, selection.selected_values);
        return Status::OK();
    }

    Status decode_dictionary_values(size_t num_values,
                                    ParquetDictionaryValueConsumer& consumer) override {
        return _decode_dictionary_values(num_values, 0, dictionary_size(), consumer);
    }

    Status decode_selected_dictionary_values(const ParquetSelection& selection,
                                             ParquetDictionaryValueConsumer& consumer) override {
        const size_t num_dictionary_values = dictionary_size();
        if (_is_fragmented_selection(selection)) {
            RETURN_IF_ERROR(_decode_fragmented_selection(selection, num_dictionary_values,
                                                         &_selected_indices));
            return consumer.consume_indices(_selected_indices.data(), selection.selected_values);
        }
        size_t cursor = 0;
        for (const auto& range : selection.ranges) {
            DORIS_CHECK(range.first >= cursor);
            RETURN_IF_ERROR(_decode_and_validate_skipped(range.first - cursor, cursor,
                                                         num_dictionary_values));
            RETURN_IF_ERROR(_decode_dictionary_values(range.count, range.first,
                                                      num_dictionary_values, consumer));
            cursor = range.first + range.count;
        }
        DORIS_CHECK(cursor <= selection.total_values);
        return _decode_and_validate_skipped(selection.total_values - cursor, cursor,
                                            num_dictionary_values);
    }

    void release_scratch(size_t max_retained_bytes) override {
        release_vector_if_oversized(&_skip_indices, max_retained_bytes);
        release_vector_if_oversized(&_selected_indices, max_retained_bytes);
    }
    size_t retained_scratch_bytes() const override {
        return (_skip_indices.capacity() + _selected_indices.capacity()) * sizeof(uint32_t);
    }
    size_t active_scratch_bytes() const override {
        return (_skip_indices.size() + _selected_indices.size()) * sizeof(uint32_t);
    }

protected:
    static bool _is_fragmented_selection(const ParquetSelection& selection) {
        constexpr size_t MIN_FRAGMENTED_RANGES = 8;
        constexpr size_t HIGHLY_FRAGMENTED_RANGES = 64;
        constexpr size_t MAX_AVERAGE_RANGE_VALUES = 4;
        constexpr size_t MAX_DECODE_EXPANSION = 8;
        return selection.ranges.size() >= MIN_FRAGMENTED_RANGES && selection.selected_values != 0 &&
               selection.selected_values <= selection.ranges.size() * MAX_AVERAGE_RANGE_VALUES &&
               (selection.ranges.size() >= HIGHLY_FRAGMENTED_RANGES ||
                selection.total_values / selection.selected_values <= MAX_DECODE_EXPANSION);
    }

    Status _decode_fragmented_selection(const ParquetSelection& selection,
                                        size_t num_dictionary_values,
                                        std::vector<uint32_t>* selected_indices) {
        DORIS_CHECK(selected_indices != nullptr);
        constexpr size_t kDecodeBatchSize = 4096;
        selected_indices->resize(selection.selected_values);
        _skip_indices.resize(std::min(selection.total_values, kDecodeBatchSize));

        size_t chunk_first = 0;
        size_t output = 0;
        size_t range_index = 0;
        while (chunk_first < selection.total_values) {
            const size_t chunk_size =
                    std::min(selection.total_values - chunk_first, kDecodeBatchSize);
            const size_t chunk_end = chunk_first + chunk_size;
            const auto decoded = _index_batch_decoder->GetBatch(_skip_indices.data(),
                                                                cast_set<uint32_t>(chunk_size));
            if (UNLIKELY(decoded != chunk_size)) {
                return Status::IOError("Can't read enough Parquet dictionary indices");
            }
            if (UNLIKELY(!dictionary_indices_in_bounds(_skip_indices.data(), chunk_size,
                                                       num_dictionary_values))) {
                for (size_t row = 0; row < chunk_size; ++row) {
                    if (_skip_indices[row] < num_dictionary_values) {
                        continue;
                    }
                    return Status::Corruption(
                            "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                            _skip_indices[row], chunk_first + row, num_dictionary_values);
                }
            }

            // Filtered ids remain validated above. Gather with a range cursor so fragmented
            // selections do not re-enter the RLE decoder for every survivor while scratch stays
            // bounded independently of the Parquet page size.
            while (range_index < selection.ranges.size()) {
                const auto& range = selection.ranges[range_index];
                DORIS_CHECK(range.first + range.count <= selection.total_values);
                if (range.first >= chunk_end) {
                    break;
                }
                const size_t copy_first = std::max(range.first, chunk_first);
                const size_t copy_end = std::min(range.first + range.count, chunk_end);
                DORIS_CHECK(copy_end >= copy_first);
                const size_t copy_count = copy_end - copy_first;
                memcpy(selected_indices->data() + output,
                       _skip_indices.data() + copy_first - chunk_first,
                       copy_count * sizeof(uint32_t));
                output += copy_count;
                if (range.first + range.count > chunk_end) {
                    break;
                }
                ++range_index;
            }
            chunk_first = chunk_end;
        }
        DORIS_CHECK_EQ(range_index, selection.ranges.size());
        DORIS_CHECK_EQ(output, selection.selected_values);
        return Status::OK();
    }

    Status skip_values(size_t num_values) override {
        return _decode_and_validate_skipped(num_values, 0, dictionary_size());
    }

    Status _decode_and_validate_skipped(size_t num_values, size_t row_offset,
                                        size_t num_dictionary_values) {
        constexpr size_t kSkipBatchSize = 4096;
        // Skipped dictionary ids are still external input and must be bounds-checked, but keeping
        // only one bounded gap buffer avoids the page-sized scratch used by sparse selections.
        _skip_indices.resize(std::min(num_values, kSkipBatchSize));
        size_t skipped_values = 0;
        while (skipped_values < num_values) {
            const size_t batch_size = std::min(num_values - skipped_values, kSkipBatchSize);
            const auto skipped = _index_batch_decoder->GetBatch(_skip_indices.data(),
                                                                static_cast<uint32_t>(batch_size));
            if (UNLIKELY(skipped != batch_size)) {
                return Status::IOError(
                        "Can't skip enough Parquet dictionary indices at row {}: {} of {}",
                        row_offset + skipped_values, skipped, batch_size);
            }
            // Filter gaps may be huge RLE runs; validate them in bounded SIMD-sized batches.
            if (UNLIKELY(!dictionary_indices_in_bounds(_skip_indices.data(), batch_size,
                                                       num_dictionary_values))) {
                for (size_t row = 0; row < batch_size; ++row) {
                    if (_skip_indices[row] < num_dictionary_values) {
                        continue;
                    }
                    return Status::Corruption(
                            "Parquet dictionary index {} at skipped row {} exceeds dictionary "
                            "size {}",
                            _skip_indices[row], row_offset + skipped_values + row,
                            num_dictionary_values);
                }
            }
            skipped_values += batch_size;
        }
        return Status::OK();
    }

    Status _decode_dictionary_values(size_t num_values, size_t row_offset,
                                     size_t num_dictionary_values,
                                     ParquetDictionaryValueConsumer& consumer) {
        constexpr size_t kLiteralBatchSize = 1024;
        size_t decoded_values = 0;
        while (decoded_values < num_values) {
            const int32_t repeats = _index_batch_decoder->NextNumRepeats();
            if (repeats > 0) {
                const size_t run = std::min<size_t>(repeats, num_values - decoded_values);
                const uint32_t index =
                        _index_batch_decoder->GetRepeatedValue(cast_set<int32_t>(run));
                if (UNLIKELY(static_cast<size_t>(index) >= num_dictionary_values)) {
                    return Status::Corruption(
                            "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                            index, row_offset + decoded_values, num_dictionary_values);
                }
                RETURN_IF_ERROR(consumer.consume_repeated(index, run));
                decoded_values += run;
                continue;
            }

            const int32_t literals = _index_batch_decoder->NextNumLiterals();
            if (UNLIKELY(literals == 0)) {
                return Status::IOError("Can't read enough Parquet dictionary indices");
            }
            const size_t batch = std::min({static_cast<size_t>(literals),
                                           num_values - decoded_values, kLiteralBatchSize});
            _skip_indices.resize(batch);
            if (UNLIKELY(!_index_batch_decoder->GetLiteralValues(cast_set<int32_t>(batch),
                                                                 _skip_indices.data()))) {
                return Status::IOError("Can't read enough Parquet dictionary indices");
            }
            if (UNLIKELY(!dictionary_indices_in_bounds(_skip_indices.data(), batch,
                                                       num_dictionary_values))) {
                for (size_t row = 0; row < batch; ++row) {
                    if (_skip_indices[row] < num_dictionary_values) {
                        continue;
                    }
                    return Status::Corruption(
                            "Parquet dictionary index {} at row {} exceeds dictionary size {}",
                            _skip_indices[row], row_offset + decoded_values + row,
                            num_dictionary_values);
                }
            }
            RETURN_IF_ERROR(consumer.consume_indices(_skip_indices.data(), batch));
            decoded_values += batch;
        }
        return Status::OK();
    }

    // For dictionary encoding
    DorisUniqueBufferPtr<uint8_t> _dict;
    std::unique_ptr<RleBatchDecoder<uint32_t>> _index_batch_decoder;
    std::vector<uint32_t> _skip_indices;
    std::vector<uint32_t> _selected_indices;
    uint64_t _dictionary_generation = 0;
};

} // namespace doris::format::parquet::native
