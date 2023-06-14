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

#include "runtime/descriptors.h"
#include "scanner_context.h"

namespace doris {

namespace pipeline {

class PipScannerContext : public vectorized::ScannerContext {
    ENABLE_FACTORY_CREATOR(PipScannerContext);

public:
    PipScannerContext(RuntimeState* state, vectorized::VScanNode* parent,
                      const TupleDescriptor* input_tuple_desc,
                      const TupleDescriptor* output_tuple_desc,
                      const std::list<vectorized::VScannerSPtr>& scanners, int64_t limit,
                      int64_t max_bytes_in_blocks_queue, const std::vector<int>& col_distribute_ids)
            : vectorized::ScannerContext(state, parent, input_tuple_desc, output_tuple_desc,
                                         scanners, limit, max_bytes_in_blocks_queue),
              _col_distribute_ids(col_distribute_ids),
              _need_colocate_distribute(!_col_distribute_ids.empty()) {}

    Status get_block_from_queue(RuntimeState* state, vectorized::BlockUPtr* block, bool* eos,
                                int id, bool wait = false) override {
        {
            if (state->is_cancelled()) {
                _process_status = Status::Cancelled("cancelled");
            }

            if (!_process_status.ok()) {
                return _process_status;
            }
        }

        {
            std::unique_lock<std::mutex> l(*_queue_mutexs[id]);
            if (!_blocks_queues[id].empty()) {
                *block = std::move(_blocks_queues[id].front());
                _blocks_queues[id].pop_front();
            } else {
                *eos = _is_finished || _should_stop;
                return Status::OK();
            }
        }
        _current_used_bytes -= (*block)->allocated_bytes();
        return Status::OK();
    }

    // We should make those method lock free.
    bool done() override { return _is_finished || _should_stop || _status_error; }

    void append_blocks_to_queue(std::vector<vectorized::BlockUPtr>& blocks) override {
        const int queue_size = _queue_mutexs.size();
        const int block_size = blocks.size();
        int64_t local_bytes = 0;

        if (_need_colocate_distribute) {
            std::vector<uint64_t> hash_vals;
            for (const auto& block : blocks) {
                // vectorized calculate hash
                int rows = block->rows();
                const auto element_size = _max_queue_size;
                hash_vals.resize(rows);
                std::fill(hash_vals.begin(), hash_vals.end(), 0);
                auto* __restrict hashes = hash_vals.data();

                for (int j = 0; j < _col_distribute_ids.size(); ++j) {
                    block->get_by_position(_col_distribute_ids[j])
                            .column->update_crcs_with_value(
                                    hash_vals, _output_tuple_desc->slots()[_col_distribute_ids[j]]
                                                       ->type()
                                                       .type);
                }
                for (int i = 0; i < rows; i++) {
                    hashes[i] = hashes[i] % element_size;
                }

                std::vector<int> channel2rows[element_size];
                for (int i = 0; i < rows; i++) {
                    channel2rows[hashes[i]].emplace_back(i);
                }

                for (int i = 0; i < element_size; ++i) {
                    if (!channel2rows[i].empty()) {
                        _add_rows_colocate_blocks(block.get(), i, channel2rows[i]);
                    }
                }
            }
        } else {
            for (const auto& block : blocks) {
                local_bytes += block->allocated_bytes();
            }

            for (int i = 0; i < queue_size && i < block_size; ++i) {
                int queue = _next_queue_to_feed;
                {
                    std::lock_guard<std::mutex> l(*_queue_mutexs[queue]);
                    for (int j = i; j < block_size; j += queue_size) {
                        _blocks_queues[queue].emplace_back(std::move(blocks[j]));
                    }
                }
                _next_queue_to_feed = queue + 1 < queue_size ? queue + 1 : 0;
            }
        }
        _current_used_bytes += local_bytes;
    }

    bool empty_in_queue(int id) override {
        std::unique_lock<std::mutex> l(*_queue_mutexs[id]);
        return _blocks_queues[id].empty();
    }

    void set_max_queue_size(const int max_queue_size, const int free_block_queue_size) override {
        _max_queue_size = max_queue_size;
        _free_block_queue_len = free_block_queue_size;

        for (int i = 0; i < max_queue_size; ++i) {
            _queue_mutexs.emplace_back(new std::mutex);
            _blocks_queues.emplace_back(std::list<vectorized::BlockUPtr>());
        }
        for (int i = 0; i < _free_block_queue_len; i++) {
            _free_block_queue_mutexs.emplace_back(new std::mutex);
            _free_blocks_queues.emplace_back(std::list<vectorized::BlockUPtr>());
        }
    }

    void _init_colocate_block() override {
        if (_need_colocate_distribute) {
            int real_block_size =
                    limit == -1 ? _batch_size : std::min(static_cast<int64_t>(_batch_size), limit);
            int64_t free_blocks_memory_usage = 0;
            for (int i = 0; i < _max_queue_size; ++i) {
                auto block = vectorized::Block::create_unique(_output_tuple_desc->slots(),
                                                              real_block_size,
                                                              true /*ignore invalid slots*/);
                free_blocks_memory_usage += block->allocated_bytes();
                _colocate_mutable_blocks.emplace_back(
                        vectorized::MutableBlock::create_unique(block.get()));
                _colocate_blocks.emplace_back(std::move(block));
                _colocate_block_mutexs.emplace_back(new std::mutex);
            }
            _free_blocks_memory_usage->add(free_blocks_memory_usage);
        }
    }

    bool has_enough_space_in_blocks_queue() const override {
        return _current_used_bytes < _max_bytes_in_queue / 2 * _max_queue_size;
    }

    void _dispose_coloate_blocks_not_in_queue() override {
        if (_need_colocate_distribute) {
            for (int i = 0; i < _max_queue_size; ++i) {
                std::scoped_lock s(*_colocate_block_mutexs[i], *_queue_mutexs[i]);
                if (_colocate_blocks[i] && !_colocate_blocks[i]->empty()) {
                    _current_used_bytes += _colocate_blocks[i]->allocated_bytes();
                    _blocks_queues[i].emplace_back(std::move(_colocate_blocks[i]));
                    _colocate_mutable_blocks[i]->clear();
                }
            }
        }
    }

    vectorized::BlockUPtr get_free_block(bool* has_free_block, bool get_not_empty_block = false,
                                         int id = -1) override {
        id = id % _free_block_queue_len;
        {
            std::unique_lock<std::mutex> l(*_free_block_queue_mutexs[id]);
            if (auto& free_blocks = _free_blocks_queues[id]; !free_blocks.empty()) {
                if (!get_not_empty_block || free_blocks.back()->mem_reuse()) {
                    auto block = std::move(free_blocks.back());
                    free_blocks.pop_back();
                    _total_free_block_num--;
                    _free_blocks_memory_usage->add(-block->allocated_bytes());
                    return block;
                }
            }
        }
        *has_free_block = false;

        COUNTER_UPDATE(_newly_create_free_blocks_num, 1);
        return vectorized::Block::create_unique(_real_tuple_desc->slots(), _batch_size,
                                                true /*ignore invalid slots*/);
    }

    void return_free_block(std::unique_ptr<vectorized::Block> block, int id) override {
        id = id % _free_block_queue_len;
        block->clear_column_data();
        _free_blocks_memory_usage->add(block->allocated_bytes());
        std::unique_lock<std::mutex> l(*_free_block_queue_mutexs[id]);
        _free_blocks_queues[id].emplace_back(std::move(block));
        _total_free_block_num++;
    }

    void _init_free_block(int pre_alloc_block_count, int real_block_size) override {
        // The free blocks is used for final output block of scanners.
        // So use _output_tuple_desc;
        int64_t free_blocks_memory_usage = 0;
        for (int i = 0, j = 0; i < pre_alloc_block_count; ++i, j++) {
            auto block = vectorized::Block::create_unique(
                    _output_tuple_desc->slots(), real_block_size, true /*ignore invalid slots*/);
            free_blocks_memory_usage += block->allocated_bytes();
            _free_blocks_queues[j].emplace_back(std::move(block));
            if (j == _free_block_queue_len - 1) {
                j = -1;
            }
        }
        _total_free_block_num = pre_alloc_block_count;
        _free_blocks_memory_usage->add(free_blocks_memory_usage);
    }

    int cal_thread_slot_num_by_free_block_num() override {
        // using _free_blocks_lock to make ```cal_thread_slot_num_by_free_block_num``` execute sequentially
        int thread_slot_num = 0;
        std::lock_guard f(_free_blocks_lock);
        int local_val = _total_free_block_num;
        thread_slot_num = local_val / _block_per_scanner;
        thread_slot_num += (local_val % _block_per_scanner != 0);
        thread_slot_num = std::min(thread_slot_num, _max_thread_num - _num_running_scanners);
        if (thread_slot_num <= 0) {
            thread_slot_num = 1;
        }
        return thread_slot_num;
    }

private:
    int _max_queue_size = 1;
    int _next_queue_to_feed = 0;
    std::vector<std::unique_ptr<std::mutex>> _queue_mutexs;
    std::vector<std::list<vectorized::BlockUPtr>> _blocks_queues;
    std::atomic_int64_t _current_used_bytes = 0;
    int _free_block_queue_len = 0;

    const std::vector<int>& _col_distribute_ids;
    const bool _need_colocate_distribute;
    std::vector<vectorized::BlockUPtr> _colocate_blocks;
    std::vector<std::unique_ptr<vectorized::MutableBlock>> _colocate_mutable_blocks;
    std::vector<std::unique_ptr<std::mutex>> _colocate_block_mutexs;

    std::vector<std::unique_ptr<std::mutex>> _free_block_queue_mutexs;
    std::vector<std::list<vectorized::BlockUPtr>> _free_blocks_queues;

    void _add_rows_colocate_blocks(vectorized::Block* block, int loc,
                                   const std::vector<int>& rows) {
        int row_wait_add = rows.size();
        const int batch_size = _batch_size;
        const int* begin = &rows[0];
        std::lock_guard<std::mutex> l(*_colocate_block_mutexs[loc]);

        while (row_wait_add > 0) {
            int row_add = 0;
            int max_add = batch_size - _colocate_mutable_blocks[loc]->rows();
            if (row_wait_add >= max_add) {
                row_add = max_add;
            } else {
                row_add = row_wait_add;
            }

            _colocate_mutable_blocks[loc]->add_rows(block, begin, begin + row_add);
            row_wait_add -= row_add;
            begin += row_add;

            if (row_add == max_add) {
                _current_used_bytes += _colocate_blocks[loc]->allocated_bytes();
                {
                    std::lock_guard<std::mutex> queue_l(*_queue_mutexs[loc]);
                    _blocks_queues[loc].emplace_back(std::move(_colocate_blocks[loc]));
                }
                bool get_block_not_empty = true;
                _colocate_blocks[loc] =
                        get_free_block(&get_block_not_empty, get_block_not_empty, loc);
                _colocate_mutable_blocks[loc]->set_muatable_columns(
                        _colocate_blocks[loc]->mutate_columns());
            }
        }
    }
};
} // namespace pipeline
} // namespace doris
