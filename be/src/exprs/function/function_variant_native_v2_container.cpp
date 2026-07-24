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

#include <limits>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "exprs/function/function_variant_path_v2_internal.h"
#include "util/utf8_check.h"

namespace doris::variant_native_v2_internal {
namespace {

void append_object_keys(VariantRef object, ColumnArray& result) {
    auto& nested = assert_cast<ColumnNullable&>(result.get_data());
    StringRef previous;
    const uint32_t count = object.num_elements();
    for (uint32_t index = 0; index < count; ++index) {
        uint32_t field_id = 0;
        static_cast<void>(object.object_value_at(index, &field_id));
        const StringRef key = object.metadata.key_at(field_id);
        if (!validate_utf8(key.data, key.size)) {
            throw Exception(ErrorCode::CORRUPTION, "Variant object key is not valid UTF-8");
        }
        if (index != 0 && previous.compare(key) >= 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant object keys are not strictly byte-sorted at field {}", index);
        }
        nested.insert_data(key.data, key.size);
        previous = key;
    }
    result.get_offsets().push_back(nested.size());
}

} // namespace

Status execute_variant_keys_v2(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                               ColumnPtr* const output) {
    auto arrays = ColumnArray::create(
            ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()),
            ColumnArray::ColumnOffsets::create());
    auto nulls = ColumnUInt8::create();
    arrays->reserve(source.size());
    nulls->reserve(source.size());

    if (source.is_typed()) {
        for (size_t row = 0; row < source.size(); ++row) {
            arrays->insert_default();
            nulls->insert_value(1);
        }
    } else {
        auto source_view = source.read_view();
        for (size_t row = 0; row < source.size(); ++row) {
            if (is_outer_null(outer_nulls, row)) {
                arrays->insert_default();
                nulls->insert_value(1);
                continue;
            }
            const VariantRef value = source_view.value_at(row);
            if (value.basic_type() != VariantBasicType::OBJECT) {
                arrays->insert_default();
                nulls->insert_value(1);
                continue;
            }
            append_object_keys(value, *arrays);
            nulls->insert_value(0);
        }
    }

    *output = ColumnNullable::create(std::move(arrays), std::move(nulls));
    return Status::OK();
}

Status execute_variant_length_v2(const ColumnVariantV2& source,
                                 std::span<const uint8_t> outer_nulls, ColumnPtr* const output) {
    auto values = ColumnInt32::create();
    auto nulls = ColumnUInt8::create();
    values->reserve(source.size());
    nulls->reserve(source.size());

    if (source.is_typed()) {
        for (size_t row = 0; row < source.size(); ++row) {
            const bool outer_null = is_outer_null(outer_nulls, row);
            values->insert_value(outer_null ? 0 : 1);
            nulls->insert_value(outer_null);
        }
    } else {
        auto source_view = source.read_view();
        for (size_t row = 0; row < source.size(); ++row) {
            if (is_outer_null(outer_nulls, row)) {
                values->insert_value(0);
                nulls->insert_value(1);
                continue;
            }
            const VariantRef value = source_view.value_at(row);
            const VariantBasicType type = value.basic_type();
            const uint32_t length =
                    type == VariantBasicType::OBJECT || type == VariantBasicType::ARRAY
                            ? value.num_elements()
                            : 1;
            DCHECK_LE(length, static_cast<uint32_t>(std::numeric_limits<int32_t>::max()));
            values->insert_value(static_cast<int32_t>(length));
            nulls->insert_value(0);
        }
    }

    *output = ColumnNullable::create(std::move(values), std::move(nulls));
    return Status::OK();
}

} // namespace doris::variant_native_v2_internal
