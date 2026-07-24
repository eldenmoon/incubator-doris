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

#include <algorithm>
#include <memory>
#include <utility>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/function/cast/cast_base.h"
#include "exprs/function/cast/variant_v2/cast_variant_v2_internal.h"
#include "exprs/function_context.h"

namespace doris::CastWrapper::variant_v2_internal {
namespace {

struct CollectedArrayNode {
    DataTypePtr type;
    DorisVector<NullMap::value_type> nulls;
    DorisVector<ColumnArray::Offset64> offsets;
    DorisVector<VariantRef> values;
    std::unique_ptr<CollectedArrayNode> child;

    size_t size() const noexcept { return nulls.size(); }
};

std::unique_ptr<CollectedArrayNode> make_collected_node(const DataTypePtr& type) {
    auto result = std::make_unique<CollectedArrayNode>();
    result->type = remove_nullable(type);
    if (result->type->get_primitive_type() == TYPE_ARRAY) {
        const auto& array_type = assert_cast<const DataTypeArray&>(*result->type);
        result->child = make_collected_node(array_type.get_nested_type());
    }
    return result;
}

void append_collected_value(CollectedArrayNode* node, VariantRef value, bool forced_null) {
    if (node->child == nullptr) {
        node->values.push_back(value);
        node->nulls.push_back(forced_null);
        return;
    }
    if (forced_null || value.is_null() || value.basic_type() != VariantBasicType::ARRAY) {
        node->nulls.push_back(1);
        node->offsets.push_back(node->child->size());
        return;
    }
    node->nulls.push_back(0);
    const uint32_t elements = value.num_elements();
    for (uint32_t element = 0; element < elements; ++element) {
        append_collected_value(node->child.get(), value.array_at(element), false);
    }
    node->offsets.push_back(node->child->size());
}

bool is_string_value(VariantRef value) {
    return value.basic_type() == VariantBasicType::SHORT_STRING ||
           (value.basic_type() == VariantBasicType::PRIMITIVE &&
            value.primitive_id() == VariantPrimitiveId::STRING);
}

Status cast_strings_to_array(FunctionContext* context, const ColumnPtr& source,
                             const DataTypePtr& source_type, const DataTypePtr& target_type,
                             size_t rows, ColumnPtr* output) {
    if (context == nullptr) {
        return Status::InvalidArgument(
                "Variant V2 string-to-ARRAY CAST requires a FunctionContext");
    }
    auto cast_context = context->clone();
    cast_context->set_enable_strict_mode(false);
    const DataTypePtr nullable_target = make_nullable(target_type);
    Block temporary {{source, source_type, "variant-v2-array-string"},
                     {nullable_target->create_column(), nullable_target, "variant-v2-array"}};
    WrapperType wrapper =
            prepare_unpack_dictionaries(cast_context.get(), source_type, nullable_target);
    Status status = wrapper(cast_context.get(), temporary, {0}, 1, rows, nullptr);
    if (!status.ok()) {
        if (status.is<ErrorCode::INVALID_ARGUMENT>()) {
            *output = make_all_null_column(target_type, rows);
            return Status::OK();
        }
        return status;
    }
    *output = temporary.get_by_position(1).column;
    return Status::OK();
}

Status cast_encoded_strings_to_array(FunctionContext* context, const ColumnVariantV2& source,
                                     const DataTypePtr& target_type, ForcedNulls forced_nulls,
                                     DorisVector<uint8_t>* string_rows, ColumnPtr* output) {
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    strings->reserve(source.size());
    nulls->reserve(source.size());
    string_rows->resize(source.size());
    for (size_t row = 0; row < source.size(); ++row) {
        const VariantRef value = source.get_value_ref(row);
        const bool forced_null = !forced_nulls.empty() && forced_nulls[row] != 0;
        const bool is_string = !forced_null && is_string_value(value);
        (*string_rows)[row] = is_string;
        if (is_string) {
            const StringRef text = value.get_string();
            strings->insert_data(text.data, text.size);
            nulls->insert_value(0);
        } else {
            strings->insert_default();
            nulls->insert_value(1);
        }
    }
    const DataTypePtr source_type = make_nullable(std::make_shared<DataTypeString>());
    ColumnPtr nullable_strings = ColumnNullable::create(std::move(strings), std::move(nulls));
    return cast_strings_to_array(context, nullable_strings, source_type, target_type, source.size(),
                                 output);
}

ColumnPtr merge_array_results(const ColumnPtr& structured, const ColumnPtr& parsed_strings,
                              std::span<const uint8_t> string_rows,
                              const DataTypePtr& target_type) {
    const auto& structured_nullable = assert_cast<const ColumnNullable&>(*structured);
    const auto& parsed_nullable = assert_cast<const ColumnNullable&>(*parsed_strings);
    MutableColumnPtr nested = target_type->create_column();
    auto nulls = ColumnUInt8::create();
    nested->reserve(string_rows.size());
    nulls->reserve(string_rows.size());
    for (size_t row = 0; row < string_rows.size(); ++row) {
        const ColumnNullable& selected =
                string_rows[row] != 0 ? parsed_nullable : structured_nullable;
        nested->insert_from(selected.get_nested_column(), row);
        nulls->insert_value(selected.get_null_map_data()[row]);
    }
    return ColumnNullable::create(std::move(nested), std::move(nulls));
}

ColumnPtr variant_column_from_refs(std::span<const VariantRef> values, ForcedNulls nulls) {
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = values.size()});
    for (size_t row_index = 0; row_index < values.size(); ++row_index) {
        auto row = builder.begin_row();
        if (!nulls.empty() && nulls[row_index] != 0) {
            row.add_null();
        } else {
            row.add_value(values[row_index]);
        }
        row.finish();
    }
    VariantBatchBuilder block = builder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    return result;
}

Status finalize_collected_node(FunctionContext* context, const CollectedArrayNode& node,
                               ColumnPtr* output) {
    const ForcedNulls nulls {node.nulls.data(), node.nulls.size()};
    if (node.child != nullptr) {
        ColumnPtr child;
        RETURN_IF_ERROR(finalize_collected_node(context, *node.child, &child));
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->get_data().insert(node.offsets.begin(), node.offsets.end());
        MutableColumnPtr mutable_child = IColumn::mutate(std::move(child));
        auto array = ColumnArray::create(std::move(mutable_child), std::move(offsets));
        auto outer_nulls = ColumnUInt8::create();
        outer_nulls->get_data().insert(node.nulls.begin(), node.nulls.end());
        *output = ColumnNullable::create(std::move(array), std::move(outer_nulls));
        return Status::OK();
    }

    const PrimitiveType primitive = node.type->get_primitive_type();
    if (primitive == TYPE_VARIANT) {
        ColumnPtr encoded = variant_column_from_refs(node.values, nulls);
        return apply_forced_nulls(std::move(encoded), nulls, output);
    }
    if (primitive == TYPE_STRING || primitive == TYPE_CHAR || primitive == TYPE_VARCHAR ||
        primitive == TYPE_JSONB) {
        if (primitive == TYPE_JSONB) {
            return cast_variant_refs_to_jsonb(context, node.values, nulls, output);
        }
        return cast_variant_refs_to_string(context, node.values, nulls, output);
    }
    if (is_supported_scalar_target(node.type)) {
        return cast_variant_refs_to_scalar(context, node.values, node.type, nulls, output);
    }
    return Status::InvalidArgument("Array element type {} cannot be cast from Variant V2",
                                   node.type->get_name());
}

} // namespace

Status cast_variant_to_array(FunctionContext* context, const ColumnVariantV2& source,
                             const DataTypePtr& target_type, size_t rows, ForcedNulls forced_nulls,
                             ColumnPtr* output) {
    if (source.size() != rows || target_type->get_primitive_type() != TYPE_ARRAY ||
        (!forced_nulls.empty() && forced_nulls.size() != rows)) {
        return Status::InvalidArgument("Invalid Variant V2 input shape for ARRAY CAST");
    }
    if (source.is_typed()) {
        if (!is_string_type(source.typed_type()->get_primitive_type())) {
            *output = make_all_null_column(target_type, rows);
            return Status::OK();
        }
        const DataTypePtr source_type = make_nullable(source.typed_type());
        ColumnPtr parsed;
        RETURN_IF_ERROR(cast_strings_to_array(context, source.typed_column().get_ptr(), source_type,
                                              target_type, rows, &parsed));
        RETURN_IF_ERROR(apply_forced_nulls(std::move(parsed), forced_nulls, output));
        return Status::OK();
    }
    std::unique_ptr<CollectedArrayNode> root = make_collected_node(target_type);
    for (size_t row_index = 0; row_index < rows; ++row_index) {
        append_collected_value(root.get(), source.get_value_ref(row_index),
                               !forced_nulls.empty() && forced_nulls[row_index] != 0);
    }
    ColumnPtr structured;
    RETURN_IF_ERROR(finalize_collected_node(context, *root, &structured));
    DorisVector<uint8_t> string_rows;
    ColumnPtr parsed_strings;
    RETURN_IF_ERROR(cast_encoded_strings_to_array(context, source, target_type, forced_nulls,
                                                  &string_rows, &parsed_strings));
    *output = merge_array_results(structured, parsed_strings, string_rows, target_type);
    return Status::OK();
}

} // namespace doris::CastWrapper::variant_v2_internal
