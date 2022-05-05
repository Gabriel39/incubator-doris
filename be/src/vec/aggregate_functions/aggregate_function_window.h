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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.h
// and modified by Doris

#pragma once

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/columns/column_vector.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/io/io_helper.h"
#include "factory_helpers.h"

namespace doris::vectorized {

struct RowNumberData {
    int64_t count;
};

class WindowFunctionRowNumber final
        : public IAggregateFunctionDataHelper<RowNumberData, WindowFunctionRowNumber> {
public:
    WindowFunctionRowNumber(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "row_number"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {
        ++data(place).count;
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        ++data(place).count;
    }

    void reset(AggregateDataPtr place) const override {
        WindowFunctionRowNumber::data(place).count = 0;
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).count);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {}
};

struct RankData {
    int64_t rank;
    int64_t count;
    int64_t peer_group_start;
};

class WindowFunctionRank final : public IAggregateFunctionDataHelper<RankData, WindowFunctionRank> {
public:
    WindowFunctionRank(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "rank"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {
        ++data(place).rank;
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        int64_t peer_group_count = frame_end - frame_start;
        if (WindowFunctionRank::data(place).peer_group_start != frame_start) {
            WindowFunctionRank::data(place).peer_group_start = frame_start;
            WindowFunctionRank::data(place).rank += WindowFunctionRank::data(place).count;
        }
        WindowFunctionRank::data(place).count = peer_group_count;
    }

    void reset(AggregateDataPtr place) const override {
        WindowFunctionRank::data(place).rank = 0;
        WindowFunctionRank::data(place).count = 1;
        WindowFunctionRank::data(place).peer_group_start = -1;
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).rank);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {}
};

struct DenseRankData {
    int64_t rank;
    int64_t peer_group_start;
};
class WindowFunctionDenseRank final
        : public IAggregateFunctionDataHelper<DenseRankData, WindowFunctionDenseRank> {
public:
    WindowFunctionDenseRank(const DataTypes& argument_types_)
            : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String get_name() const override { return "dense_rank"; }

    DataTypePtr get_return_type() const override { return std::make_shared<DataTypeInt64>(); }

    void add(AggregateDataPtr place, const IColumn**, size_t, Arena*) const override {
        ++data(place).rank;
    }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        if (WindowFunctionDenseRank::data(place).peer_group_start != frame_start) {
            WindowFunctionDenseRank::data(place).peer_group_start = frame_start;
            WindowFunctionDenseRank::data(place).rank++;
        }
    }

    void reset(AggregateDataPtr place) const override {
        WindowFunctionDenseRank::data(place).rank = 0;
        WindowFunctionDenseRank::data(place).peer_group_start = -1;
    }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        assert_cast<ColumnInt64&>(to).get_data().push_back(data(place).rank);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {}
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {}
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {}
};

struct Value {
public:
    bool is_null() const { return _is_null; }
    StringRef get_value() const { return _value; }

    void set_null(bool is_null) { _is_null = is_null; }
    void set_value(StringRef value) { _value = value; }
    void reset() {
        _is_null = false;
        _value = {};
    }

protected:
    StringRef _value;
    bool _is_null;
};

struct CopiedValue : public Value {
public:
    void set_value(StringRef value) {
        _copied_value = value.to_string();
        _value = StringRef(_copied_value);
    }

private:
    std::string _copied_value;
};

template <typename T, bool is_nullable, bool is_string, typename StoreType = Value,
          bool init_with_null = false>
struct LeadAndLagData {
public:
    bool has_init() const { return _is_init; }

    void reset() {
        _data_value.reset();
        _default_value.reset();
        _is_init = false;
        _has_value = false;
        if constexpr (init_with_null) {
            this->set_is_null();
        }
    }

    void insert_result_into(IColumn& to) const {
        if constexpr (is_nullable) {
            if (_data_value.is_null()) {
                auto& col = assert_cast<ColumnNullable&>(to);
                col.insert_default();
            } else {
                auto& col = assert_cast<ColumnNullable&>(to);
                if constexpr (is_string) {
                    StringRef value = _data_value.get_value();
                    col.insert_data(value.data, value.size);
                } else {
                    StringRef value = _data_value.get_value();
                    col.insert_data(value.data, 0);
                }
            }
        } else {
            if constexpr (is_string) {
                auto& col = assert_cast<ColumnString&>(to);
                StringRef value = _data_value.get_value();
                col.insert_data(value.data, value.size);
            } else {
                StringRef value = _data_value.get_value();
                to.insert_data(value.data, 0);
            }
        }
    }

    void set_value(const IColumn** columns, int64_t pos) {
        if constexpr (is_nullable) {
            const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[0]);
            if (nullable_column && nullable_column->is_null_at(pos)) {
                _data_value.set_null(true);
                _has_value = true;
                return;
            }
            if constexpr (is_string) {
                const auto* sources = check_and_get_column<ColumnString>(
                        nullable_column->get_nested_column_ptr().get());
                _data_value.set_value(sources->get_data_at(pos));
            } else {
                _data_value.set_value(nullable_column->get_nested_column_ptr()->get_data_at(pos));
            }
        } else {
            if constexpr (is_string) {
                const auto* sources = check_and_get_column<ColumnString>(columns[0]);
                _data_value.set_value(sources->get_data_at(pos));
            } else {
                _data_value.set_value(columns[0]->get_data_at(pos));
            }
        }
        _data_value.set_null(false);
        _has_value = true;
    }

    bool defualt_is_null() { return _default_value.is_null(); }

    void set_is_null() { _data_value.set_null(true); }

    void set_value_from_default() { _data_value.set_value(_default_value.get_value()); }

    bool has_set_value() { return _has_value; }

    void check_default(const IColumn* column) {
        if (!has_init()) {
            if (is_column_nullable(*column)) {
                const auto* nullable_column = check_and_get_column<ColumnNullable>(column);
                if (nullable_column->is_null_at(0)) {
                    _default_value.set_null(true);
                }
            } else {
                if constexpr (is_string) {
                    const auto& col = static_cast<const ColumnString&>(*column);
                    _default_value.set_value(col.get_data_at(0));
                } else {
                    _default_value.set_value(column->get_data_at(0));
                }
            }
            _is_init = true;
        }
    }

private:
    StoreType _data_value;
    StoreType _default_value;
    bool _has_value = false;
    bool _is_init = false;
};

template <typename Data>
struct WindowFunctionLeadData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, size_t frame_start,
                                size_t frame_end, const IColumn** columns) {
        this->check_default(columns[2]);
        if (frame_end > partition_end) { //output default value, win end is under partition
            if (this->defualt_is_null()) {
                this->set_is_null();
            } else {
                this->set_value_from_default();
            }
            return;
        }
        this->set_value(columns, frame_end - 1);
    }
    void add(int64_t row, const IColumn** columns) {
        LOG(FATAL) << "WindowFunctionLeadData do not support add";
    }
    static const char* name() { return "lead"; }
};

template <typename Data>
struct WindowFunctionLagData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        this->check_default(columns[2]);
        if (partition_start >= frame_end) { //[unbound preceding(0), offset preceding(-123)]
            if (this->defualt_is_null()) {  // win start is beyond partition
                this->set_is_null();
            } else {
                this->set_value_from_default();
            }
            return;
        }
        this->set_value(columns, frame_end - 1);
    }
    void add(int64_t row, const IColumn** columns) {
        LOG(FATAL) << "WindowFunctionLagData do not support add";
    }
    static const char* name() { return "lag"; }
};

template <typename Data>
struct WindowFunctionFirstData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        if (frame_start < frame_end &&
            frame_end <= partition_start) { //rewrite last_value when under partition
            this->set_is_null();            //so no need more judge
            return;
        }
        frame_start = std::max<int64_t>(frame_start, partition_start);
        this->set_value(columns, frame_start);
    }
    void add(int64_t row, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        this->set_value(columns, row);
    }
    static const char* name() { return "first_value"; }
};

template <typename Data>
struct WindowFunctionFirstNonNullData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        if (frame_start < frame_end &&
            frame_end <= partition_start) { //rewrite last_value when under partition
            this->set_is_null();            //so no need more judge
            return;
        }
        frame_start = std::max<int64_t>(frame_start, partition_start);
        frame_end = std::min<int64_t>(frame_end, partition_end);
        if (const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[0])) {
            for (int i = frame_start; i < frame_end; i++) {
                if (!nullable_column->is_null_at(i)) {
                    this->set_value(columns, i);
                    return;
                }
            }
        }
        this->set_value(columns, frame_start);
    }

    void add(int64_t row, const IColumn** columns) {
        if (this->has_set_value()) {
            return;
        }
        if (const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[0])) {
            if (nullable_column->is_null_at(row)) {
                return;
            }
        }
        this->set_value(columns, row);
    }
    static const char* name() { return "first_non_null_value"; }
};

template <typename Data>
struct WindowFunctionLastData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        if ((frame_start < frame_end) &&
            ((frame_end <= partition_start) ||
             (frame_start >= partition_end))) { //beyond or under partition, set null
            this->set_is_null();
            return;
        }
        frame_end = std::min<int64_t>(frame_end, partition_end);
        this->set_value(columns, frame_end - 1);
    }
    void add(int64_t row, const IColumn** columns) { this->set_value(columns, row); }
    static const char* name() { return "last_value"; }
};

template <typename Data>
struct WindowFunctionLastNonNullData : Data {
    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, const IColumn** columns) {
        if ((frame_start < frame_end) &&
            ((frame_end <= partition_start) ||
             (frame_start >= partition_end))) { //beyond or under partition, set null
            this->set_is_null();
            return;
        }
        frame_start = std::max<int64_t>(frame_start, partition_start);
        frame_end = std::min<int64_t>(frame_end, partition_end);
        if (const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[0])) {
            for (int i = frame_end - 1; i >= frame_start; i--) {
                if (!nullable_column->is_null_at(i)) {
                    this->set_value(columns, i);
                    return;
                }
            }
        } else {
            this->set_value(columns, frame_end - 1);
        }
    }

    void add(int64_t row, const IColumn** columns) {
        if (const auto* nullable_column = check_and_get_column<ColumnNullable>(columns[0])) {
            if (nullable_column->is_null_at(row)) {
                return;
            }
        }
        this->set_value(columns, row);
    }

    static const char* name() { return "last_non_null_value"; }
};

template <typename Data>
class WindowFunctionData final
        : public IAggregateFunctionDataHelper<Data, WindowFunctionData<Data>> {
public:
    WindowFunctionData(const DataTypes& argument_types)
            : IAggregateFunctionDataHelper<Data, WindowFunctionData<Data>>(argument_types, {}),
              _argument_type(argument_types[0]) {}

    String get_name() const override { return Data::name(); }
    DataTypePtr get_return_type() const override { return _argument_type; }

    void add_range_single_place(int64_t partition_start, int64_t partition_end, int64_t frame_start,
                                int64_t frame_end, AggregateDataPtr place, const IColumn** columns,
                                Arena* arena) const override {
        this->data(place).add_range_single_place(partition_start, partition_end, frame_start,
                                                 frame_end, columns);
    }

    void reset(AggregateDataPtr place) const override { this->data(place).reset(); }

    void insert_result_into(ConstAggregateDataPtr place, IColumn& to) const override {
        this->data(place).insert_result_into(to);
    }

    void add(AggregateDataPtr place, const IColumn** columns, size_t row_num,
             Arena* arena) const override {
        this->data(place).add(row_num, columns);
    }
    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena*) const override {
        LOG(FATAL) << "WindowFunctionData do not support merge";
    }
    void serialize(ConstAggregateDataPtr place, BufferWritable& buf) const override {
        LOG(FATAL) << "WindowFunctionData do not support serialize";
    }
    void deserialize(AggregateDataPtr place, BufferReadable& buf, Arena*) const override {
        LOG(FATAL) << "WindowFunctionData do not support deserialize";
    }

private:
    DataTypePtr _argument_type;
};

template <template <typename> class AggregateFunctionTemplate, template <typename> class Data,
          bool is_nullable, bool is_copy = false, bool init_with_null = false>
static IAggregateFunction* create_function_single_value(const String& name,
                                                        const DataTypes& argument_types,
                                                        const Array& parameters) {
    using StoreType = std::conditional_t<is_copy, CopiedValue, Value>;

    assert_arity_at_most<3>(name, argument_types);

    auto type = remove_nullable(argument_types[0]);
    WhichDataType which(*type);

#define DISPATCH(TYPE)                                                                      \
    if (which.idx == TypeIndex::TYPE)                                                       \
        return new AggregateFunctionTemplate<                                               \
                Data<LeadAndLagData<TYPE, is_nullable, false, StoreType, init_with_null>>>( \
                argument_types);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.is_decimal()) {
        return new AggregateFunctionTemplate<
                Data<LeadAndLagData<Int128, is_nullable, false, StoreType, init_with_null>>>(
                argument_types);
    }
    if (which.is_date_or_datetime()) {
        return new AggregateFunctionTemplate<
                Data<LeadAndLagData<Int64, is_nullable, false, StoreType, init_with_null>>>(
                argument_types);
    }
    if (which.is_string_or_fixed_string()) {
        return new AggregateFunctionTemplate<
                Data<LeadAndLagData<StringRef, is_nullable, true, StoreType, init_with_null>>>(
                argument_types);
    }
    DCHECK(false) << "with unknowed type, failed in  create_aggregate_function_leadlag";
    return nullptr;
}

template <bool is_nullable, bool is_copy, bool replace_if_not_null = false>
AggregateFunctionPtr create_aggregate_function_first(const std::string& name,
                                                     const DataTypes& argument_types,
                                                     const Array& parameters,
                                                     bool result_is_nullable) {
    if constexpr (replace_if_not_null) {
        return AggregateFunctionPtr(
                create_function_single_value<WindowFunctionData, WindowFunctionFirstNonNullData,
                                             is_nullable, is_copy, true>(name, argument_types,
                                                                         parameters));
    } else {
        return AggregateFunctionPtr(
                create_function_single_value<WindowFunctionData, WindowFunctionFirstData,
                                             is_nullable, is_copy>(name, argument_types,
                                                                   parameters));
    }
}

template <bool is_nullable, bool is_copy, bool replace_if_not_null = false>
AggregateFunctionPtr create_aggregate_function_last(const std::string& name,
                                                    const DataTypes& argument_types,
                                                    const Array& parameters,
                                                    bool result_is_nullable) {
    if constexpr (replace_if_not_null) {
        return AggregateFunctionPtr(
                create_function_single_value<WindowFunctionData, WindowFunctionLastNonNullData,
                                             is_nullable, is_copy, true>(name, argument_types,
                                                                         parameters));
    } else {
        return AggregateFunctionPtr(
                create_function_single_value<WindowFunctionData, WindowFunctionLastData,
                                             is_nullable, is_copy>(name, argument_types,
                                                                   parameters));
    }
}

} // namespace doris::vectorized
