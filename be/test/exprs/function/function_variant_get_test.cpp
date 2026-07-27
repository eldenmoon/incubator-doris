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

#include "exprs/function/function_variant_get.h"

#include <gtest/gtest.h>

#include <array>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_field.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/runtime_state.h"
#include "util/variant/variant_test_utils.h"

namespace doris {
namespace {

VariantField encode_json(std::string_view json) {
    JsonStringToVariantEncoder encoder({.max_json_key_length = 1024,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    encoder.add_json({json.data(), json.size()});
    VariantBatchBuilder block = encoder.finish_batch();
    return VariantField::from_ref(block.value_at(0));
}

ColumnVariantV2::MutablePtr encoded_column(std::initializer_list<std::string_view> rows) {
    auto result = ColumnVariantV2::create();
    for (std::string_view row : rows) {
        insert_encoded_field(*result, encode_json(row));
    }
    return result;
}

std::unique_ptr<ResolvedVariantGetV2Path> resolve(std::string_view path) {
    std::unique_ptr<ResolvedVariantGetV2Path> result;
    const Status status = resolve_variant_get_v2_path({path.data(), path.size()}, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

ColumnPtr get(const ColumnVariantV2& source, const ResolvedVariantGetV2Path& path,
              std::span<const uint8_t> outer_nulls = {}) {
    ColumnPtr result;
    const Status status = variant_get_v2(source, path, outer_nulls, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

const ColumnNullable& nullable_result(const ColumnPtr& result) {
    return assert_cast<const ColumnNullable&>(*result);
}

const ColumnVariantV2& variant_result(const ColumnPtr& result) {
    return assert_cast<const ColumnVariantV2&>(nullable_result(result).get_nested_column());
}

ColumnPtr constant_string(std::string_view value, size_t rows) {
    auto data = ColumnString::create();
    data->insert_data(value.data(), value.size());
    return ColumnConst::create(std::move(data), rows);
}

ColumnPtr constant_null_string(size_t rows) {
    auto data = ColumnString::create();
    data->insert_default();
    return ColumnConst::create(ColumnNullable::create(std::move(data), ColumnUInt8::create(1, 1)),
                               rows);
}

Status execute_variant_get(ColumnsWithTypeAndName arguments, DataTypePtr result_type,
                           ColumnPtr* output) {
    std::vector<DataTypePtr> argument_types;
    argument_types.reserve(arguments.size());
    for (const auto& argument : arguments) {
        argument_types.push_back(argument.type);
    }
    auto function =
            SimpleFunctionFactory::instance().get_function("variant_get", arguments, result_type);
    if (!function) {
        return Status::NotFound("function variant_get");
    }

    Block block {std::move(arguments)};
    const uint32_t result = block.columns();
    block.insert({nullptr, result_type, "result"});
    RuntimeState state;
    auto context = FunctionContext::create_context(&state, result_type, argument_types);
    std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_columns(argument_types.size());
    if (is_column_const(*block.get_by_position(1).column)) {
        constant_columns[1] = std::make_shared<ColumnPtrWrapper>(block.get_by_position(1).column);
    }
    context->set_constant_cols(constant_columns);

    RETURN_IF_ERROR(function->open(context.get(), FunctionContext::FRAGMENT_LOCAL));
    Status status = function->open(context.get(), FunctionContext::THREAD_LOCAL);
    if (status.ok()) {
        status = function->execute(context.get(), block, {0, 1}, result, block.rows());
        const Status close_status = function->close(context.get(), FunctionContext::THREAD_LOCAL);
        if (status.ok()) {
            status = close_status;
        }
    }
    const Status close_status = function->close(context.get(), FunctionContext::FRAGMENT_LOCAL);
    if (status.ok()) {
        status = close_status;
    }
    RETURN_IF_ERROR(status);
    *output = block.get_by_position(result).column;
    return Status::OK();
}

} // namespace

TEST(FunctionVariantGetTest, DeepArrayAndQuotedObjectKeys) {
    auto source = encoded_column({R"json({
                "a":[{"b":10},null],
                "quoted.key":{"sp ace":30},
                "config":{"key":31,"quote'key":32,"back\\slash":33,"key.with.dot":34,"":35}
            })json",
                                  R"({"a":[]})", R"({"other":1})", R"({"a":[{"b":40}]})"});
    const std::array<uint8_t, 4> outer_nulls {0, 0, 0, 1};

    auto deep = resolve("$.a[0].b");
    ColumnPtr result = get(*source, *deep, outer_nulls);
    const auto& deep_nullable = nullable_result(result);
    const auto& deep_values = variant_result(result);
    EXPECT_FALSE(deep_nullable.is_null_at(0));
    EXPECT_EQ(deep_values.get_value_ref(0).get_int(), 10);
    EXPECT_TRUE(deep_nullable.is_null_at(1));
    EXPECT_TRUE(deep_nullable.is_null_at(2));
    EXPECT_TRUE(deep_nullable.is_null_at(3));

    auto quoted = resolve(R"($."quoted.key"."sp ace")");
    result = get(*source, *quoted);
    const auto& quoted_nullable = nullable_result(result);
    const auto& quoted_values = variant_result(result);
    EXPECT_FALSE(quoted_nullable.is_null_at(0));
    EXPECT_EQ(quoted_values.get_value_ref(0).get_int(), 30);
    EXPECT_TRUE(quoted_nullable.is_null_at(1));

    auto bracket_double = resolve(R"($.config["key"])");
    result = get(*source, *bracket_double);
    EXPECT_EQ(variant_result(result).get_value_ref(0).get_int(), 31);

    auto bracket_single = resolve(R"($.config['quote\'key'])");
    result = get(*source, *bracket_single);
    EXPECT_EQ(variant_result(result).get_value_ref(0).get_int(), 32);

    auto escaped_backslash = resolve(R"($.config["back\\slash"])");
    result = get(*source, *escaped_backslash);
    EXPECT_EQ(variant_result(result).get_value_ref(0).get_int(), 33);

    auto bracket_literal_dot = resolve(R"($.config["key.with.dot"])");
    result = get(*source, *bracket_literal_dot);
    EXPECT_EQ(variant_result(result).get_value_ref(0).get_int(), 34);

    auto bracket_empty = resolve(R"($.config[""])");
    result = get(*source, *bracket_empty);
    EXPECT_EQ(variant_result(result).get_value_ref(0).get_int(), 35);
}

TEST(FunctionVariantGetTest, ArrayIndexesAreZeroBasedAndVariantNullIsAValue) {
    auto source = encoded_column({R"({"a":[10,null]})", R"({"a":[]})", R"({"x":1})"});

    auto first = resolve("$.a[0]");
    ColumnPtr result = get(*source, *first);
    EXPECT_FALSE(nullable_result(result).is_null_at(0));
    EXPECT_EQ(variant_result(result).get_value_ref(0).get_int(), 10);

    auto second = resolve("$.a[1]");
    result = get(*source, *second);
    const auto& nullable = nullable_result(result);
    const auto& values = variant_result(result);
    EXPECT_FALSE(nullable.is_null_at(0));
    EXPECT_TRUE(values.get_value_ref(0).is_null());
    EXPECT_TRUE(nullable.is_null_at(1));
    EXPECT_TRUE(nullable.is_null_at(2));
}

TEST(FunctionVariantGetTest, RootSharesSourceAndPreservesSqlNullBoundary) {
    auto source = encoded_column({"null", R"({"a":1})", R"([1,2])"});
    const std::array<uint8_t, 3> outer_nulls {0, 1, 0};
    auto root = resolve("$");
    ASSERT_TRUE(root->is_root());

    ColumnPtr result = get(*source, *root, outer_nulls);
    const auto& nullable = nullable_result(result);
    const auto& values = variant_result(result);
    EXPECT_EQ(&values, source.get());
    EXPECT_FALSE(nullable.is_null_at(0));
    EXPECT_TRUE(values.get_value_ref(0).is_null());
    EXPECT_TRUE(nullable.is_null_at(1));
    EXPECT_FALSE(nullable.is_null_at(2));
}

TEST(FunctionVariantGetTest, TypedStringRootIsNotReparsedAsJson) {
    auto strings = ColumnString::create();
    strings->insert_data(R"({"a":1})", 7);
    auto source = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(strings), ColumnUInt8::create(1, 0)),
            std::make_shared<DataTypeString>());

    auto nested = resolve("$.a");
    EXPECT_TRUE(nullable_result(get(*source, *nested)).is_null_at(0));

    auto root = resolve("$");
    ColumnPtr result = get(*source, *root);
    EXPECT_EQ(&variant_result(result), source.get());
    EXPECT_FALSE(nullable_result(result).is_null_at(0));
}

TEST(FunctionVariantGetTest, InvalidPathsAreInvalidArgumentAndResolutionIsAtomic) {
    auto path = resolve("$");
    const ResolvedVariantGetV2Path* identity = path.get();

    for (std::string_view invalid :
         {"", "a", "$.a[*]", "$.a[last]", "$.a[-1]", "$.a[1.5]", "$.a[2147483648]", "$..a",
          R"($.a["unterminated])", R"($.a['bad\q'])"}) {
        const Status status = resolve_variant_get_v2_path({invalid.data(), invalid.size()}, &path);
        EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT) << invalid << ": " << status;
        EXPECT_EQ(path.get(), identity) << invalid;
    }
}

TEST(FunctionVariantGetTest, RuntimeAdapterRequiresConstantPathAndVariantV2) {
    auto variant_type = std::make_shared<DataTypeVariantV2>();
    auto source = encoded_column({R"({"a":[10]})", R"({"a":[20]})"});
    auto result_type = make_nullable(variant_type);

    ColumnPtr output;
    Status status = execute_variant_get({{source->get_ptr(), variant_type, "source"},
                                         {constant_string("$.a[0]", source->size()),
                                          std::make_shared<DataTypeString>(), "path"}},
                                        result_type, &output);
    ASSERT_TRUE(status.ok()) << status;
    const auto& values = variant_result(output);
    EXPECT_EQ(values.get_value_ref(0).get_int(), 10);
    EXPECT_EQ(values.get_value_ref(1).get_int(), 20);

    status = execute_variant_get({{source->get_ptr(), variant_type, "source"},
                                  {constant_null_string(source->size()),
                                   make_nullable(std::make_shared<DataTypeString>()), "path"}},
                                 result_type, &output);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(nullable_result(output).is_null_at(0));
    EXPECT_TRUE(nullable_result(output).is_null_at(1));

    status = execute_variant_get({{source->get_ptr(), variant_type, "source"},
                                  {constant_string("$.a[*]", source->size()),
                                   std::make_shared<DataTypeString>(), "path"}},
                                 result_type, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);

    auto nonconstant_path = ColumnString::create();
    nonconstant_path->insert_data("$.a[0]", 6);
    nonconstant_path->insert_data("$.a[0]", 6);
    status = execute_variant_get(
            {{source->get_ptr(), variant_type, "source"},
             {nonconstant_path->get_ptr(), std::make_shared<DataTypeString>(), "path"}},
            result_type, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_NE(status.to_string().find("must be constant"), std::string::npos);

    auto legacy_type = std::make_shared<DataTypeVariant>();
    auto legacy = ColumnVariant::create(0, false);
    legacy->insert_default();
    legacy->finalize();
    status = execute_variant_get(
            {{legacy->get_ptr(), legacy_type, "source"},
             {constant_string("$", legacy->size()), std::make_shared<DataTypeString>(), "path"}},
            make_nullable(legacy_type), &output);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Variant V2 only"), std::string::npos);
    EXPECT_NE(status.to_string().find("ColumnVariant"), std::string::npos);
}

TEST(FunctionVariantGetTest, RuntimeAdapterHandlesNullableAndConstantVariantV2Sources) {
    const auto variant_type = std::make_shared<DataTypeVariantV2>();
    const auto result_type = make_nullable(variant_type);

    auto nullable_values = encoded_column({R"({"a":10})", R"({"a":20})"});
    auto outer_nulls = ColumnUInt8::create();
    outer_nulls->insert_value(0);
    outer_nulls->insert_value(1);
    ColumnPtr nullable_source =
            ColumnNullable::create(std::move(nullable_values), std::move(outer_nulls));

    ColumnPtr output;
    Status status = execute_variant_get(
            {{std::move(nullable_source), make_nullable(variant_type), "source"},
             {constant_string("$.a", 2), std::make_shared<DataTypeString>(), "path"}},
            result_type, &output);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_FALSE(nullable_result(output).is_null_at(0));
    EXPECT_EQ(variant_result(output).get_value_ref(0).get_int(), 10);
    EXPECT_TRUE(nullable_result(output).is_null_at(1));

    auto constant_value = encoded_column({R"({"a":30})"});
    ColumnPtr constant_source = ColumnConst::create(constant_value->get_ptr(), 3);
    status = execute_variant_get(
            {{std::move(constant_source), variant_type, "source"},
             {constant_string("$.a", 3), std::make_shared<DataTypeString>(), "path"}},
            result_type, &output);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(output->size(), 3);
    const auto& constant_result = assert_cast<const ColumnConst&>(*output);
    const auto& nullable_value =
            assert_cast<const ColumnNullable&>(constant_result.get_data_column());
    ASSERT_EQ(nullable_value.size(), 1);
    EXPECT_FALSE(nullable_value.is_null_at(0));
    const auto& variant_value =
            assert_cast<const ColumnVariantV2&>(nullable_value.get_nested_column());
    EXPECT_EQ(variant_value.get_value_ref(0).get_int(), 30);
}

} // namespace doris
