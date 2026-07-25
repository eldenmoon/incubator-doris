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
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "exprs/function/function.h"
#include "exprs/function/function_variant_element_v2.h"
#include "exprs/function/simple_function_factory.h"
#include "util/json/path_in_data.h"

namespace doris {

namespace {

ColumnPtr make_all_null_variant(size_t rows) {
    auto values = ColumnVariantV2::create();
    values->insert_many_defaults(rows);
    return ColumnNullable::create(std::move(values), ColumnUInt8::create(rows, 1));
}

Status resolve_string_path(const IColumn& index_column,
                           std::unique_ptr<ResolvedVariantElementV2Path>* output) {
    const std::string field_name = index_column.get_data_at(0).to_string();
    const PathInData path(field_name);
    std::vector<VariantElementV2PathSegment> segments;
    segments.reserve(path.get_parts().size());
    for (const PathInData::Part& part : path.get_parts()) {
        segments.push_back(
                VariantElementV2PathSegment::object_key({part.key.data(), part.key.size()}));
    }
    return resolve_variant_element_v2_path(segments, output);
}

Status resolve_integer_path(const IColumn& index_column,
                            std::unique_ptr<ResolvedVariantElementV2Path>* output) {
    const int64_t sql_index = index_column.get_int(0);
    if (sql_index == 0) {
        return Status::NotFound("Variant array index {} does not identify an element", sql_index);
    }
    if (sql_index > 0 &&
        sql_index - 1 > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
        return Status::NotFound("Variant array index {} exceeds the supported range", sql_index);
    }
    const auto segment =
            VariantElementV2PathSegment::array_index(sql_index > 0 ? sql_index - 1 : sql_index);
    return resolve_variant_element_v2_path(std::span(&segment, 1), output);
}

} // namespace

class FunctionVariantElement : public IFunction {
public:
    static constexpr auto name = "element_at";
    static FunctionPtr create() { return std::make_shared<FunctionVariantElement>(); }

    String get_name() const override { return name; }
    bool use_default_implementation_for_nulls() const override { return false; }
    size_t get_number_of_arguments() const override { return 2; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeVariant>(), std::make_shared<DataTypeString>()};
    }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return make_nullable(std::make_shared<DataTypeVariant>());
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        if (input_rows_count == 0) {
            block.replace_by_position(result, block.get_by_position(result).type->create_column());
            return Status::OK();
        }

        const ColumnPtr materialized =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const IColumn* source = materialized.get();
        std::span<const uint8_t> outer_nulls;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(source)) {
            outer_nulls = nullable->get_null_map_data();
            source = &nullable->get_nested_column();
        }
        const auto* variant = check_and_get_column<ColumnVariantV2>(source);
        if (variant == nullptr) {
            return Status::RuntimeError("function {} requires ColumnVariantV2, got {}", get_name(),
                                        source->get_name());
        }
        if (variant->size() != input_rows_count) {
            return Status::InternalError("function {} received {} Variant rows, expected {}",
                                         get_name(), variant->size(), input_rows_count);
        }

        const ColumnPtr materialized_index =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        if (materialized_index->is_null_at(0)) {
            block.replace_by_position(result, make_all_null_variant(input_rows_count));
            return Status::OK();
        }
        const IColumn* index = materialized_index.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(index)) {
            index = &nullable->get_nested_column();
        }

        std::unique_ptr<ResolvedVariantElementV2Path> path;
        const Status path_status = resolve_path(*index, &path);
        if (path_status.is<ErrorCode::NOT_FOUND>()) {
            block.replace_by_position(result, make_all_null_variant(input_rows_count));
            return Status::OK();
        }
        RETURN_IF_ERROR(path_status);

        ColumnPtr output;
        RETURN_IF_ERROR(extract_variant_element_v2(*variant, *path, outer_nulls, &output));
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }

protected:
    virtual Status resolve_path(const IColumn& index,
                                std::unique_ptr<ResolvedVariantElementV2Path>* output) const {
        return resolve_string_path(index, output);
    }
};

class FunctionVariantElementByInteger final : public FunctionVariantElement {
public:
    static constexpr auto name = FunctionVariantElement::name;
    static FunctionPtr create() { return std::make_shared<FunctionVariantElementByInteger>(); }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeVariant>(), std::make_shared<DataTypeInt64>()};
    }

protected:
    Status resolve_path(const IColumn& index,
                        std::unique_ptr<ResolvedVariantElementV2Path>* output) const override {
        return resolve_integer_path(index, output);
    }
};

void register_function_variant_element(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionVariantElement>();
    factory.register_function<FunctionVariantElementByInteger>();
}

} // namespace doris
