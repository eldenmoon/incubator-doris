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

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/value/variant/variant_canonical.h"
#include "exprs/function/function_variant_path_v2_internal.h"
#include "util/utf8_check.h"

namespace doris::variant_native_v2_internal {
namespace {

void require_contains_depth(uint32_t depth) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant contains traversal exceeds maximum depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
}

struct ObjectEntry {
    StringRef key;
    VariantRef value;
};

ObjectEntry object_entry_at(VariantRef object, uint32_t index, StringRef previous,
                            bool has_previous) {
    uint32_t field_id = 0;
    VariantRef value = object.object_value_at(index, &field_id);
    const StringRef key = object.metadata.key_at(field_id);
    if (!validate_utf8(key.data, key.size)) {
        throw Exception(ErrorCode::CORRUPTION, "Variant object key is not valid UTF-8");
    }
    if (has_previous && previous.compare(key) >= 0) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant object keys are not strictly byte-sorted at field {}", index);
    }
    return {.key = key, .value = value};
}

bool contains_value(VariantRef target, VariantRef candidate, uint32_t depth);

bool array_contains_value(VariantRef target, VariantRef candidate, uint32_t depth) {
    const uint32_t count = target.num_elements();
    for (uint32_t index = 0; index < count; ++index) {
        if (contains_value(target.array_at(index), candidate, depth + 1)) {
            return true;
        }
    }
    return false;
}

bool array_contains_array(VariantRef target, VariantRef candidate, uint32_t depth) {
    const uint32_t count = candidate.num_elements();
    for (uint32_t index = 0; index < count; ++index) {
        if (!array_contains_value(target, candidate.array_at(index), depth)) {
            return false;
        }
    }
    return true;
}

bool object_contains_object(VariantRef target, VariantRef candidate, uint32_t depth) {
    const uint32_t target_count = target.num_elements();
    const uint32_t candidate_count = candidate.num_elements();
    uint32_t target_index = 0;
    StringRef previous_target;
    StringRef previous_candidate;
    for (uint32_t candidate_index = 0; candidate_index < candidate_count; ++candidate_index) {
        const ObjectEntry candidate_entry = object_entry_at(
                candidate, candidate_index, previous_candidate, candidate_index != 0);
        previous_candidate = candidate_entry.key;
        bool matched = false;
        while (target_index < target_count) {
            const ObjectEntry target_entry =
                    object_entry_at(target, target_index, previous_target, target_index != 0);
            previous_target = target_entry.key;
            ++target_index;
            const int comparison = target_entry.key.compare(candidate_entry.key);
            if (comparison < 0) {
                continue;
            }
            if (comparison > 0 ||
                !contains_value(target_entry.value, candidate_entry.value, depth + 1)) {
                return false;
            }
            matched = true;
            break;
        }
        if (!matched) {
            return false;
        }
    }
    return true;
}

bool contains_value(VariantRef target, VariantRef candidate, uint32_t depth) {
    require_contains_depth(depth);
    const VariantBasicType target_type = target.basic_type();
    if (target_type == VariantBasicType::ARRAY) {
        return candidate.basic_type() == VariantBasicType::ARRAY
                       ? array_contains_array(target, candidate, depth)
                       : array_contains_value(target, candidate, depth);
    }
    if (target_type == VariantBasicType::OBJECT) {
        return candidate.basic_type() == VariantBasicType::OBJECT &&
               object_contains_object(target, candidate, depth);
    }
    return canonical_equals(target, candidate);
}

ColumnVariantV2::MutablePtr masked_encoded_copy(const ColumnVariantV2& source,
                                                std::span<const uint8_t> skipped_rows) {
    if (!source.is_typed()) {
        return nullptr;
    }
    const auto& typed = assert_cast<const ColumnNullable&>(source.typed_column());
    auto nulls = ColumnUInt8::create();
    nulls->get_data().resize(source.size());
    for (size_t row = 0; row < source.size(); ++row) {
        nulls->get_data()[row] = typed.get_null_map_data()[row] | skipped_rows[row];
    }
    ColumnPtr masked = ColumnNullable::create(typed.get_nested_column_ptr(), std::move(nulls));
    auto encoded = ColumnVariantV2::create_typed(std::move(masked), source.typed_type());
    encoded->ensure_encoded();
    return encoded;
}

} // namespace

Status execute_variant_contains_v2(const ColumnVariantV2& target, const ColumnVariantV2& candidate,
                                   std::span<const uint8_t> target_outer_nulls,
                                   std::span<const uint8_t> candidate_outer_nulls,
                                   ColumnPtr* const output) {
    DorisVector<uint8_t> skipped_rows(target.size(), 0);
    for (size_t row = 0; row < target.size(); ++row) {
        skipped_rows[row] =
                is_outer_null(target_outer_nulls, row) || is_outer_null(candidate_outer_nulls, row);
    }

    ColumnVariantV2::MutablePtr encoded_target = masked_encoded_copy(target, skipped_rows);
    ColumnVariantV2::MutablePtr encoded_candidate = masked_encoded_copy(candidate, skipped_rows);
    const ColumnVariantV2& target_input = encoded_target ? *encoded_target : target;
    const ColumnVariantV2& candidate_input = encoded_candidate ? *encoded_candidate : candidate;
    auto target_view = target_input.read_view();
    auto candidate_view = candidate_input.read_view();

    auto values = ColumnUInt8::create();
    auto nulls = ColumnUInt8::create();
    values->reserve(target.size());
    nulls->reserve(target.size());
    for (size_t row = 0; row < target.size(); ++row) {
        if (skipped_rows[row] != 0) {
            values->insert_value(0);
            nulls->insert_value(1);
            continue;
        }
        values->insert_value(
                contains_value(target_view.value_at(row), candidate_view.value_at(row), 0));
        nulls->insert_value(0);
    }

    ColumnPtr result = ColumnNullable::create(std::move(values), std::move(nulls));
    output->swap(result);
    return Status::OK();
}

} // namespace doris::variant_native_v2_internal
