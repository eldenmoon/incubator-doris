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

#include "exprs/table_function/vexplode_variant_object.h"

#include <algorithm>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "util/jsonb_writer.h"

namespace doris {

VExplodeVariantObjectTableFunction::VExplodeVariantObjectTableFunction() {
    _fn_name = "vexplode_variant_object";
}

Status VExplodeVariantObjectTableFunction::process_init(Block* block, RuntimeState* /*state*/) {
    CHECK(_expr_context->root()->children().size() == 1)
            << "VExplodeVariantObjectTableFunction only supports one child";

    ColumnPtr value_column;
    RETURN_IF_ERROR(_expr_context->root()->children()[0]->execute_column(
            _expr_context.get(), block, nullptr, block->rows(), value_column));
    const auto& [unpacked, is_const] = unpack_if_const(value_column);
    _source_column = unpacked;
    _is_const = is_const;

    const IColumn* physical = _source_column.get();
    if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
        _outer_nulls = nullable->get_null_map_data().data();
        physical = &nullable->get_nested_column();
    }
    if (typeid(*physical) != typeid(ColumnVariantV2)) {
        return Status::InvalidArgument(
                "explode_variant_object requires an exact ColumnVariantV2, got {}",
                physical->get_name());
    }

    _variant_column = &assert_cast<const ColumnVariantV2&>(*physical);
    DORIS_CHECK_EQ(_variant_column->size(), _source_column->size())
            << "explode_variant_object nullable and nested row counts differ";
    _read_view.emplace(_variant_column->read_view());
    return Status::OK();
}

void VExplodeVariantObjectTableFunction::process_row(size_t row_idx) {
    TableFunction::process_row(row_idx);
    if (_is_const && _kv_pairs.first) {
        return;
    }

    const size_t physical_row = _is_const ? 0 : row_idx;
    if (_outer_nulls != nullptr && _outer_nulls[physical_row] != 0) {
        return;
    }
    DORIS_CHECK(_read_view.has_value());
    if (_read_view->is_typed()) {
        return;
    }

    const VariantRef object = _read_view->value_at(physical_row);
    if (object.basic_type() != VariantBasicType::OBJECT) {
        return;
    }
    _cur_size = object.num_elements();
    if (_cur_size == 0) {
        return;
    }

    _kv_pairs.first = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    _kv_pairs.second = ColumnNullable::create(ColumnString::create(), ColumnUInt8::create());
    _kv_pairs.first->reserve(_cur_size);
    _kv_pairs.second->reserve(_cur_size);

    JsonbWriter writer;
    for (uint32_t index = 0; index < _cur_size; ++index) {
        uint32_t field_id = 0;
        const VariantRef value = object.object_value_at(index, &field_id);
        const StringRef key = object.metadata.key_at(field_id);
        _kv_pairs.first->insert_data(key.data, key.size);
        variant_to_jsonb(value, writer);
        _kv_pairs.second->insert_data(writer.getOutput()->getBuffer(),
                                      writer.getOutput()->getSize());
    }
}

void VExplodeVariantObjectTableFunction::process_close() {
    _read_view.reset();
    _variant_column = nullptr;
    _outer_nulls = nullptr;
    _source_column = nullptr;
    _kv_pairs.first = nullptr;
    _kv_pairs.second = nullptr;
    _cur_size = 0;
}

void VExplodeVariantObjectTableFunction::get_same_many_values(MutableColumnPtr& column,
                                                              int length) {
    if (current_empty()) {
        column->insert_many_defaults(length);
        return;
    }

    ColumnStruct* result = nullptr;
    if (_is_nullable) {
        auto* nullable = assert_cast<ColumnNullable*>(column.get());
        result = assert_cast<ColumnStruct*>(nullable->get_nested_column_ptr().get());
        nullable->get_null_map_column_ptr()->insert_many_defaults(length);
    } else {
        result = assert_cast<ColumnStruct*>(column.get());
    }
    result->get_column(0).insert_many_from(*_kv_pairs.first, _cur_offset, length);
    result->get_column(1).insert_many_from(*_kv_pairs.second, _cur_offset, length);
}

int VExplodeVariantObjectTableFunction::get_value(MutableColumnPtr& column, int max_step) {
    max_step = std::min(max_step, static_cast<int>(_cur_size - _cur_offset));
    if (current_empty()) {
        column->insert_default();
        max_step = 1;
    } else {
        ColumnStruct* result = nullptr;
        if (_is_nullable) {
            auto* nullable = assert_cast<ColumnNullable*>(column.get());
            result = assert_cast<ColumnStruct*>(nullable->get_nested_column_ptr().get());
            nullable->get_null_map_column_ptr()->insert_many_defaults(max_step);
        } else {
            result = assert_cast<ColumnStruct*>(column.get());
        }
        result->get_column(0).insert_range_from(*_kv_pairs.first, _cur_offset, max_step);
        result->get_column(1).insert_range_from(*_kv_pairs.second, _cur_offset, max_step);
    }
    forward(max_step);
    return max_step;
}

} // namespace doris
