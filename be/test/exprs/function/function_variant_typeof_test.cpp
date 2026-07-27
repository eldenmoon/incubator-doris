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

#include "exprs/function/function_variant_typeof.h"

#include <gtest/gtest.h>

#include <array>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/function_test_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
namespace {

ColumnVariantV2::MutablePtr encode_json_rows(const std::vector<std::string_view>& rows) {
    JsonStringToVariantEncoder encoder({.max_json_key_length = 1024,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    for (const std::string_view row : rows) {
        encoder.add_json({row.data(), row.size()});
    }
    VariantBatchBuilder batch = encoder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(batch);
    return result;
}

ColumnVariantV2::MutablePtr encode_decimal_rows() {
    VariantBatchBuilder builder;
    {
        auto row = builder.begin_row();
        row.add_decimal(10, 1);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_decimal(1, 2);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_decimal(1010, 2);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_decimal(0, 3);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_null();
        row.add_int(1);
        row.add_decimal(10, 1);
        array.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_decimal(1, 2);
        row.add_decimal(123, 1);
        array.finish();
        row.finish();
    }

    VariantBatchBuilder batch = builder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(batch);
    return result;
}

ColumnVariantV2::MutablePtr encode_extended_primitive_rows() {
    VariantBatchBuilder builder;
    {
        auto row = builder.begin_row();
        row.add_date(0);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_timestamp_micros(0, true);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_timestamp_micros(0, false);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_float(1.0F);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        constexpr std::array<char, 2> bytes {1, 2};
        row.add_binary({bytes.data(), bytes.size()});
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_time_ntz_micros(0);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_uuid({});
        row.finish();
    }

    VariantBatchBuilder batch = builder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(batch);
    return result;
}

ColumnVariantV2::MutablePtr encode_numeric_merge_rows() {
    VariantBatchBuilder builder;
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_int(1);
        row.add_float(1.0F);
        array.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_decimal(1, 1);
        row.add_float(1.0F);
        array.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_decimal(1, 1);
        row.add_double(1.0);
        array.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto array = row.start_array();
        row.add_decimal(static_cast<__int128>(VARIANT_DECIMAL16_MAX), 0);
        row.add_decimal(1, 38, 16);
        array.finish();
        row.finish();
    }

    VariantBatchBuilder batch = builder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(batch);
    return result;
}

ColumnPtr schemas(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls = {}) {
    ColumnPtr result;
    const Status status = variant_typeof_v2(source, outer_nulls, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

std::optional<std::string> schema_at(const ColumnPtr& result, size_t row) {
    const IColumn* physical = result.get();
    if (const auto* constant = check_and_get_column<ColumnConst>(physical)) {
        physical = &constant->get_data_column();
        row = 0;
    }
    const auto& nullable = assert_cast<const ColumnNullable&>(*physical);
    if (nullable.is_null_at(row)) {
        return std::nullopt;
    }
    return assert_cast<const ColumnString&>(nullable.get_nested_column())
            .get_data_at(row)
            .to_string();
}

void expect_schemas(const ColumnPtr& result,
                    const std::vector<std::optional<std::string>>& expected) {
    ASSERT_EQ(result->size(), expected.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(schema_at(result, row), expected[row]) << row;
    }
}

struct ExecutionResult {
    Status status;
    ColumnPtr output;
};

ExecutionResult execute_typeof(ColumnPtr input, const DataTypePtr& input_type, size_t rows) {
    const DataTypePtr result_type = make_nullable(std::make_shared<DataTypeString>());
    Block block;
    block.insert({std::move(input), input_type, "variant"});
    FunctionBasePtr function = SimpleFunctionFactory::instance().get_function(
            "variant_typeof", block.get_columns_with_type_and_name(), result_type);
    DORIS_CHECK(function != nullptr);
    block.insert({nullptr, result_type, "result"});

    FunctionUtils function_utils(result_type, {input_type}, false);
    FunctionContext* context = function_utils.get_fn_ctx();
    DORIS_CHECK(function->open(context, FunctionContext::FRAGMENT_LOCAL).ok());
    DORIS_CHECK(function->open(context, FunctionContext::THREAD_LOCAL).ok());
    Status status = function->execute(context, block, {0}, 1, rows);
    static_cast<void>(function->close(context, FunctionContext::THREAD_LOCAL));
    static_cast<void>(function->close(context, FunctionContext::FRAGMENT_LOCAL));
    return {.status = std::move(status), .output = block.get_by_position(1).column};
}

} // namespace

TEST(FunctionVariantTypeofTest, SparkPrimitiveContainerAndQuotedFieldSchemas) {
    auto source = encode_json_rows({"null", "true", "1", "1E0", R"("x")", "[]", R"([null,1])",
                                    R"([1,"1"])", R"({"b":{"c":"x"},"a":["x"]})",
                                    R"([{"b":1},{"a":"x"}])",
                                    R"({"a.b":1,"a`b":true,"1x":"z","_ok":null})"});

    expect_schemas(schemas(*source),
                   {"VOID", "BOOLEAN", "BIGINT", "DOUBLE", "STRING", "ARRAY<VOID>", "ARRAY<BIGINT>",
                    "ARRAY<VARIANT>", "OBJECT<a: ARRAY<STRING>, b: OBJECT<c: STRING>>",
                    "ARRAY<OBJECT<a: STRING, b: BIGINT>>",
                    "OBJECT<`1x`: STRING, _ok: VOID, `a.b`: BIGINT, `a``b`: BOOLEAN>"});
}

TEST(FunctionVariantTypeofTest, DecimalPrecisionScaleUsesTheStoredValue) {
    auto source = encode_decimal_rows();
    expect_schemas(schemas(*source),
                   {"DECIMAL(1,0)", "DECIMAL(2,2)", "DECIMAL(3,1)", "DECIMAL(1,0)",
                    "ARRAY<DECIMAL(20,0)>", "ARRAY<DECIMAL(4,2)>"});
}

TEST(FunctionVariantTypeofTest, CoversEveryExtendedPrimitiveTag) {
    auto source = encode_extended_primitive_rows();
    expect_schemas(schemas(*source),
                   {"DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "FLOAT", "BINARY", "TIME_NTZ", "UUID"});
}

TEST(FunctionVariantTypeofTest, UsesSparkNumericMergeRules) {
    auto source = encode_numeric_merge_rows();
    expect_schemas(schemas(*source),
                   {"ARRAY<FLOAT>", "ARRAY<DOUBLE>", "ARRAY<DOUBLE>", "ARRAY<DOUBLE>"});
}

TEST(FunctionVariantTypeofTest, TypedAndEncodedStatesHaveTheSameNullSemantics) {
    auto values = ColumnInt64::create();
    values->insert_value(1);
    values->insert_value(2);
    values->insert_value(3);
    auto inner_nulls = ColumnUInt8::create();
    inner_nulls->insert_value(0);
    inner_nulls->insert_value(1);
    inner_nulls->insert_value(0);
    auto typed = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(inner_nulls)),
            std::make_shared<DataTypeInt64>());
    const std::array<uint8_t, 3> outer_nulls {0, 0, 1};

    ASSERT_TRUE(typed->is_typed());
    expect_schemas(schemas(*typed, outer_nulls), {"BIGINT", "VOID", std::nullopt});

    MutableColumnPtr encoded_owner = typed->clone();
    auto& encoded = assert_cast<ColumnVariantV2&>(*encoded_owner);
    encoded.ensure_encoded();
    ASSERT_FALSE(encoded.is_typed());
    expect_schemas(schemas(encoded, outer_nulls), {"BIGINT", "VOID", std::nullopt});
    EXPECT_TRUE(typed->is_typed());
}

TEST(FunctionVariantTypeofTest, RejectsInvalidKernelArgumentsWithoutPublishingOutput) {
    auto source = encode_json_rows({"1"});
    auto sentinel = ColumnString::create();
    sentinel->insert_data("sentinel", 8);
    ColumnPtr output = sentinel->get_ptr();
    const IColumn* const original = output.get();

    const std::array<uint8_t, 2> wrong_null_map {0, 0};
    EXPECT_EQ(variant_typeof_v2(*source, wrong_null_map, &output).code(),
              ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), original);
    EXPECT_EQ(variant_typeof_v2(*source, {}, nullptr).code(), ErrorCode::INVALID_ARGUMENT);
}

TEST(FunctionVariantTypeofTest, DoesNotPublishPartialOutputAfterTypedRowFailure) {
    auto dates = ColumnDateV2::create();
    ColumnDateV2::value_type valid_date;
    valid_date.unchecked_set_time(1970, 1, 1, 0, 0, 0, 0);
    const ColumnDateV2::value_type invalid_date {};
    dates->insert_value(valid_date);
    dates->insert_value(invalid_date);

    auto inner_nulls = ColumnUInt8::create(2, 0);
    auto source = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(dates), std::move(inner_nulls)),
            std::make_shared<DataTypeDateV2>());
    auto sentinel = ColumnString::create();
    sentinel->insert_data("sentinel", 8);
    ColumnPtr output = sentinel->get_ptr();
    const IColumn* const original = output.get();

    const Status status = variant_typeof_v2(*source, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_NE(status.to_string().find("invalid DATEV2 value at row 1"), std::string::npos);
    EXPECT_EQ(output.get(), original);
}

TEST(FunctionVariantTypeofTest, FactoryHandlesNullableAndConstantVariantV2Inputs) {
    auto nullable_values = encode_json_rows({"1", R"({"ignored":true})"});
    auto outer_nulls = ColumnUInt8::create();
    outer_nulls->insert_value(0);
    outer_nulls->insert_value(1);
    ColumnPtr nullable_input =
            ColumnNullable::create(std::move(nullable_values), std::move(outer_nulls));
    const DataTypePtr nullable_variant_type = make_nullable(std::make_shared<DataTypeVariantV2>());
    ExecutionResult nullable = execute_typeof(nullable_input, nullable_variant_type, 2);
    ASSERT_TRUE(nullable.status.ok()) << nullable.status.to_string();
    expect_schemas(nullable.output, {"BIGINT", std::nullopt});

    auto constant_value = encode_json_rows({R"({"a":[1]})"});
    ColumnPtr constant_input = ColumnConst::create(constant_value->get_ptr(), 3);
    const DataTypePtr variant_type = std::make_shared<DataTypeVariantV2>();
    ExecutionResult constant = execute_typeof(constant_input, variant_type, 3);
    ASSERT_TRUE(constant.status.ok()) << constant.status.to_string();
    expect_schemas(constant.output, {"OBJECT<a: ARRAY<BIGINT>>", "OBJECT<a: ARRAY<BIGINT>>",
                                     "OBJECT<a: ARRAY<BIGINT>>"});
}

TEST(FunctionVariantTypeofTest, FactoryRejectsLegacyVariantWithClearV2OnlyError) {
    const DataTypePtr legacy_type = std::make_shared<DataTypeVariant>();
    MutableColumnPtr legacy = legacy_type->create_column();
    legacy->insert_default();
    assert_cast<ColumnVariant&>(*legacy).finalize();

    ExecutionResult result = execute_typeof(std::move(legacy), legacy_type, 1);
    EXPECT_FALSE(result.status.ok());
    EXPECT_NE(result.status.to_string().find("Variant V2 only"), std::string::npos)
            << result.status.to_string();
    EXPECT_NE(result.status.to_string().find("ColumnVariant"), std::string::npos)
            << result.status.to_string();
}

} // namespace doris
