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
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "exprs/function/function.h"
#include "exprs/function/function_variant_element_v2.h"
#include "exprs/function/function_variant_native_v2.h"
#include "exprs/function/simple_function_factory.h"
#include "util/jsonb_document.h"

namespace doris {
namespace {

struct MaterializedVariantArgument {
    ColumnPtr owner;
    const ColumnVariantV2* values = nullptr;
    std::span<const uint8_t> outer_nulls;
};

Status materialize_variant_argument(const Block& block, uint32_t argument, size_t rows,
                                    std::string_view function_name,
                                    MaterializedVariantArgument* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("function {} Variant argument output is null",
                                       function_name);
    }
    MaterializedVariantArgument candidate;
    candidate.owner = block.get_by_position(argument).column->convert_to_full_column_if_const();
    const IColumn* column = candidate.owner.get();
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        candidate.outer_nulls = nullable->get_null_map_data();
        column = &nullable->get_nested_column();
    }
    candidate.values = check_and_get_column<ColumnVariantV2>(column);
    if (candidate.values == nullptr) {
        return Status::RuntimeError("function {} requires ColumnVariantV2, got {}", function_name,
                                    column->get_name());
    }
    if (candidate.values->size() != rows) {
        return Status::InternalError("function {} received {} Variant rows, expected {}",
                                     function_name, candidate.values->size(), rows);
    }
    *output = std::move(candidate);
    return Status::OK();
}

ColumnPtr make_all_null_variant(size_t rows) {
    auto values = ColumnVariantV2::create();
    values->insert_many_defaults(rows);
    return ColumnNullable::create(std::move(values), ColumnUInt8::create(rows, 1));
}

ColumnPtr make_all_null_bool(size_t rows) {
    return ColumnNullable::create(ColumnUInt8::create(rows, 0), ColumnUInt8::create(rows, 1));
}

ColumnPtr make_root_variant_result(const ColumnVariantV2& source,
                                   std::span<const uint8_t> outer_nulls) {
    MutableColumnPtr values = source.clone_resized(source.size());
    auto nulls = ColumnUInt8::create(source.size(), 0);
    if (!outer_nulls.empty()) {
        std::copy(outer_nulls.begin(), outer_nulls.end(), nulls->get_data().begin());
    }
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

ColumnPtr make_root_exists_result(size_t rows, std::span<const uint8_t> outer_nulls) {
    auto values = ColumnUInt8::create(rows, 1);
    auto nulls = ColumnUInt8::create(rows, 0);
    for (size_t row = 0; row < rows; ++row) {
        if (!outer_nulls.empty() && outer_nulls[row] != 0) {
            values->get_data()[row] = 0;
            nulls->get_data()[row] = 1;
        }
    }
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

struct ResolvedJsonPath {
    bool is_null = false;
    bool is_root = false;
    std::unique_ptr<ResolvedVariantElementV2Path> path;
};

Status resolve_json_path(const Block& block, uint32_t argument, std::string_view function_name,
                         ResolvedJsonPath* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("function {} JSON path output is null", function_name);
    }
    const ColumnPtr materialized =
            block.get_by_position(argument).column->convert_to_full_column_if_const();
    if (materialized->size() == 0) {
        return Status::InvalidArgument("function {} received an empty JSON path column",
                                       function_name);
    }

    ResolvedJsonPath candidate;
    if (materialized->is_null_at(0)) {
        candidate.is_null = true;
        *output = std::move(candidate);
        return Status::OK();
    }
    const IColumn* column = materialized.get();
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        column = &nullable->get_nested_column();
    }
    const auto* paths = check_and_get_column<ColumnString>(column);
    if (paths == nullptr) {
        return Status::RuntimeError("function {} requires a String JSON path, got {}",
                                    function_name, column->get_name());
    }

    const StringRef raw_path = paths->get_data_at(0);
    std::string mutable_path(raw_path.data, raw_path.size);
    JsonbPath parsed;
    if (!parsed.seek(mutable_path.data(), mutable_path.size())) {
        return Status::InvalidJsonPath("Invalid JSON path for function {}: {}", function_name,
                                       raw_path.to_string());
    }
    if (parsed.is_wildcard() || parsed.is_supper_wildcard()) {
        return Status::InvalidJsonPath("JSON path for function {} may not contain wildcard tokens",
                                       function_name);
    }
    if (parsed.get_leg_vector_size() == 0) {
        candidate.is_root = true;
        *output = std::move(candidate);
        return Status::OK();
    }

    std::vector<VariantElementV2PathSegment> segments;
    segments.reserve(parsed.get_leg_vector_size());
    for (size_t index = 0; index < parsed.get_leg_vector_size(); ++index) {
        const leg_info* leg = parsed.get_leg_from_leg_vector(index);
        if (leg->type == MEMBER_CODE) {
            segments.push_back(VariantElementV2PathSegment::object_key(
                    {leg->leg_ptr, static_cast<size_t>(leg->leg_len)}));
            continue;
        }
        if (leg->type != ARRAY_CODE) {
            return Status::InvalidJsonPath("JSON path for function {} has an unknown leg type",
                                           function_name);
        }
        if (leg->array_index < 0) {
            return Status::InvalidJsonPath(
                    "JSON path for function {} does not support last-relative array indexes",
                    function_name);
        }
        segments.push_back(
                VariantElementV2PathSegment::array_index(static_cast<uint32_t>(leg->array_index)));
    }
    RETURN_IF_ERROR(resolve_variant_element_v2_path(segments, &candidate.path));
    *output = std::move(candidate);
    return Status::OK();
}

DataTypePtr nullable_bool_type() {
    return make_nullable(std::make_shared<DataTypeUInt8>());
}

class FunctionVariantGet final : public IFunction {
public:
    static constexpr auto name = "variant_get";
    static FunctionPtr create() { return std::make_shared<FunctionVariantGet>(); }

    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    bool use_default_implementation_for_nulls() const override { return false; }
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
        MaterializedVariantArgument source;
        RETURN_IF_ERROR(materialize_variant_argument(block, arguments[0], input_rows_count,
                                                     get_name(), &source));
        ResolvedJsonPath path;
        RETURN_IF_ERROR(resolve_json_path(block, arguments[1], get_name(), &path));
        if (path.is_null) {
            block.replace_by_position(result, make_all_null_variant(input_rows_count));
            return Status::OK();
        }
        if (path.is_root) {
            block.replace_by_position(result,
                                      make_root_variant_result(*source.values, source.outer_nulls));
            return Status::OK();
        }

        ColumnPtr output;
        RETURN_IF_ERROR(extract_variant_element_v2(*source.values, *path.path, source.outer_nulls,
                                                   &output));
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }
};

class FunctionVariantExistsPath final : public IFunction {
public:
    static constexpr auto name = "variant_exists_path";
    static FunctionPtr create() { return std::make_shared<FunctionVariantExistsPath>(); }

    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    bool use_default_implementation_for_nulls() const override { return false; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeVariant>(), std::make_shared<DataTypeString>()};
    }
    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return nullable_bool_type();
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        if (input_rows_count == 0) {
            block.replace_by_position(result, block.get_by_position(result).type->create_column());
            return Status::OK();
        }
        MaterializedVariantArgument source;
        RETURN_IF_ERROR(materialize_variant_argument(block, arguments[0], input_rows_count,
                                                     get_name(), &source));
        ResolvedJsonPath path;
        RETURN_IF_ERROR(resolve_json_path(block, arguments[1], get_name(), &path));
        if (path.is_null) {
            block.replace_by_position(result, make_all_null_bool(input_rows_count));
            return Status::OK();
        }
        if (path.is_root) {
            block.replace_by_position(
                    result, make_root_exists_result(input_rows_count, source.outer_nulls));
            return Status::OK();
        }

        ColumnPtr output;
        RETURN_IF_ERROR(
                variant_exists_path_v2(*source.values, *path.path, source.outer_nulls, &output));
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }
};

struct VariantIsNullImpl {
    static constexpr auto name = "variant_is_null";
    static DataTypePtr return_type() { return nullable_bool_type(); }
    static Status execute(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                          ColumnPtr* output) {
        return variant_is_null_v2(source, outer_nulls, output);
    }
};

struct VariantKeysImpl {
    static constexpr auto name = "variant_keys";
    static DataTypePtr return_type() {
        return make_nullable(
                std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())));
    }
    static Status execute(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                          ColumnPtr* output) {
        return variant_keys_v2(source, outer_nulls, output);
    }
};

struct VariantLengthImpl {
    static constexpr auto name = "variant_length";
    static DataTypePtr return_type() { return make_nullable(std::make_shared<DataTypeInt32>()); }
    static Status execute(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                          ColumnPtr* output) {
        return variant_length_v2(source, outer_nulls, output);
    }
};

template <typename Impl>
class FunctionVariantUnaryNative final : public IFunction {
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionVariantUnaryNative<Impl>>(); }

    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool use_default_implementation_for_nulls() const override { return false; }
    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeVariant>()};
    }
    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return Impl::return_type();
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        MaterializedVariantArgument source;
        RETURN_IF_ERROR(materialize_variant_argument(block, arguments[0], input_rows_count,
                                                     get_name(), &source));
        ColumnPtr output;
        RETURN_IF_ERROR(Impl::execute(*source.values, source.outer_nulls, &output));
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }
};

using FunctionVariantIsNull = FunctionVariantUnaryNative<VariantIsNullImpl>;
using FunctionVariantKeys = FunctionVariantUnaryNative<VariantKeysImpl>;
using FunctionVariantLength = FunctionVariantUnaryNative<VariantLengthImpl>;

class FunctionVariantContains final : public IFunction {
public:
    static constexpr auto name = "variant_contains";
    static FunctionPtr create() { return std::make_shared<FunctionVariantContains>(); }

    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    bool use_default_implementation_for_nulls() const override { return false; }
    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeVariant>(), std::make_shared<DataTypeVariant>()};
    }
    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return nullable_bool_type();
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        MaterializedVariantArgument target;
        MaterializedVariantArgument candidate;
        RETURN_IF_ERROR(materialize_variant_argument(block, arguments[0], input_rows_count,
                                                     get_name(), &target));
        RETURN_IF_ERROR(materialize_variant_argument(block, arguments[1], input_rows_count,
                                                     get_name(), &candidate));
        ColumnPtr output;
        RETURN_IF_ERROR(variant_contains_v2(*target.values, *candidate.values, target.outer_nulls,
                                            candidate.outer_nulls, &output));
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }
};

} // namespace

void register_function_variant_native(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionVariantGet>();
    factory.register_function<FunctionVariantExistsPath>();
    factory.register_function<FunctionVariantIsNull>();
    factory.register_function<FunctionVariantKeys>();
    factory.register_function<FunctionVariantLength>();
    factory.register_function<FunctionVariantContains>();
}

} // namespace doris
