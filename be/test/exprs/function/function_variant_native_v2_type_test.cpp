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

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/value/ipv4_value.h"
#include "core/value/timestamptz_value.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/function_variant_native_v2.h"

namespace doris {
namespace {

std::string encoded(VariantScalarEncodingPlan plan) {
    std::string result(plan.size(), '\0');
    plan.write(result.data(), result.size());
    return result;
}

std::string empty_container(VariantBasicType type) {
    return {static_cast<char>(type), 0, 0};
}

ColumnVariantV2::MutablePtr encoded_column(std::span<const std::string> rows) {
    const std::array<char, 3> metadata {
            static_cast<char>(VARIANT_ENCODING_VERSION | VARIANT_METADATA_SORTED_STRINGS_MASK), 0,
            0};
    const std::array<uint32_t, 2> metadata_offsets {0, metadata.size()};
    std::string values;
    std::vector<uint32_t> value_offsets;
    value_offsets.reserve(rows.size() + 1);
    value_offsets.push_back(0);
    for (const std::string& row : rows) {
        values.append(row);
        value_offsets.push_back(values.size());
    }

    auto result = ColumnVariantV2::create();
    result->insert_encoded_rows({.metadata_bytes = {metadata.data(), metadata.size()},
                                 .metadata_offsets = metadata_offsets,
                                 .meta_ids = {},
                                 .value_bytes = {values.data(), values.size()},
                                 .value_offsets = {value_offsets.data(), value_offsets.size()}});
    return result;
}

ColumnVariantV2::MutablePtr raw_encoded_column(std::span<const std::string> rows) {
    const std::string null_value = encoded(VariantScalarEncodingPlan::null_value());
    const std::vector<std::string> valid_rows(rows.size(), null_value);
    auto result = encoded_column(valid_rows);
    auto values = ColumnString::create();
    for (const std::string& row : rows) {
        values->insert_data(row.data(), row.size());
    }
    ColumnVariantV2::TestAccess::replace_encoded_subcolumn(*result, 2, values->get_ptr());
    return result;
}

ColumnPtr wrap_nullable(MutableColumnPtr nested, std::span<const uint8_t> null_map) {
    EXPECT_EQ(nested->size(), null_map.size());
    auto nulls = ColumnUInt8::create();
    nulls->get_data().insert(null_map.begin(), null_map.end());
    return ColumnNullable::create(std::move(nested), std::move(nulls));
}

template <typename ColumnType, typename Value>
ColumnPtr nullable_fixed(std::initializer_list<Value> values,
                         std::initializer_list<uint8_t> null_map) {
    auto nested = ColumnType::create();
    for (const Value& value : values) {
        nested->insert_value(value);
    }
    return wrap_nullable(std::move(nested), {null_map.begin(), null_map.size()});
}

template <typename ColumnType, typename Value>
ColumnPtr nullable_decimal(uint32_t scale, std::initializer_list<Value> values,
                           std::initializer_list<uint8_t> null_map) {
    auto nested = ColumnType::create(0, scale);
    for (const Value& value : values) {
        nested->insert_value(value);
    }
    return wrap_nullable(std::move(nested), {null_map.begin(), null_map.size()});
}

ColumnPtr type_words(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls = {}) {
    ColumnPtr result;
    const Status status = variant_type_v2(source, outer_nulls, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

const ColumnNullable& nullable_result(const ColumnPtr& result) {
    return assert_cast<const ColumnNullable&>(*result);
}

void expect_word(const ColumnPtr& result, size_t row, std::string_view expected,
                 bool is_sql_null = false) {
    const auto& nullable = nullable_result(result);
    EXPECT_EQ(nullable.get_null_map_data()[row] != 0, is_sql_null) << row;
    if (!is_sql_null) {
        const auto& words = assert_cast<const ColumnString&>(nullable.get_nested_column());
        EXPECT_EQ(words.get_data_at(row), StringRef(expected.data(), expected.size())) << row;
    }
}

void expect_same_typed_and_encoded(ColumnVariantV2& typed,
                                   std::span<const std::string_view> expected) {
    ASSERT_TRUE(typed.is_typed());
    const IColumn* const original_typed = &typed.typed_column();
    const ColumnPtr typed_result = type_words(typed);

    MutableColumnPtr encoded_base = typed.clone();
    auto& encoded_copy = assert_cast<ColumnVariantV2&>(*encoded_base);
    encoded_copy.ensure_encoded();
    const ColumnPtr encoded_result = type_words(encoded_copy);

    ASSERT_EQ(expected.size(), typed.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        expect_word(typed_result, row, expected[row]);
        expect_word(encoded_result, row, expected[row]);
    }
    EXPECT_TRUE(typed.is_typed());
    EXPECT_EQ(&typed.typed_column(), original_typed);
}

__int128 power_of_ten_i128(uint8_t exponent) {
    __int128 value = 1;
    for (uint8_t digit = 0; digit < exponent; ++digit) {
        value *= 10;
    }
    return value;
}

} // namespace

TEST(VariantNativeV2TypeTest, EncodedVocabularyCoversAllBasicAndPrimitiveHeaders) {
    const std::string long_string(64, 'L');
    const std::array<uint8_t, 16> uuid {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
    const std::string binary("a\0b", 3);
    const std::vector<std::string> rows {
            encoded(VariantScalarEncodingPlan::string(StringRef("x"))),
            empty_container(VariantBasicType::OBJECT),
            empty_container(VariantBasicType::ARRAY),
            encoded(VariantScalarEncodingPlan::null_value()),
            encoded(VariantScalarEncodingPlan::boolean(true)),
            encoded(VariantScalarEncodingPlan::boolean(false)),
            encoded(VariantScalarEncodingPlan::integer(1, 1)),
            encoded(VariantScalarEncodingPlan::integer(128, 2)),
            encoded(VariantScalarEncodingPlan::integer(32768, 4)),
            encoded(VariantScalarEncodingPlan::integer(int64_t {1} << 40, 8)),
            encoded(VariantScalarEncodingPlan::float64(1.5)),
            encoded(VariantScalarEncodingPlan::decimal(7, 0, 4)),
            encoded(VariantScalarEncodingPlan::decimal(1234, 2, 8)),
            encoded(VariantScalarEncodingPlan::decimal(-12345, 3, 16)),
            encoded(VariantScalarEncodingPlan::date(1)),
            encoded(VariantScalarEncodingPlan::timestamp_micros(2, true)),
            encoded(VariantScalarEncodingPlan::timestamp_micros(3, false)),
            encoded(VariantScalarEncodingPlan::float32(2.5F)),
            encoded(VariantScalarEncodingPlan::binary({binary.data(), binary.size()})),
            encoded(VariantScalarEncodingPlan::string({long_string.data(), long_string.size()})),
            encoded(VariantScalarEncodingPlan::time_ntz_micros(4)),
            encoded(VariantScalarEncodingPlan::timestamp_nanos(5, true)),
            encoded(VariantScalarEncodingPlan::timestamp_nanos(6, false)),
            encoded(VariantScalarEncodingPlan::uuid(uuid)),
            encoded(VariantScalarEncodingPlan::integer(1, 8))};
    const std::array<std::string_view, 25> expected {
            "string",    "object",        "array",         "null",    "bool",
            "bool",      "tinyint",       "smallint",      "int",     "bigint",
            "double",    "decimal",       "decimal",       "decimal", "date",
            "timestamp", "timestamp_ntz", "float",         "binary",  "string",
            "time",      "timestamp",     "timestamp_ntz", "uuid",    "bigint"};

    auto source = encoded_column(rows);
    const ColumnPtr result = type_words(*source);
    ASSERT_EQ(source->size(), expected.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        expect_word(result, row, expected[row]);
    }
    EXPECT_EQ(source->read_view().value_at(6).primitive_id(), VariantPrimitiveId::INT8);
    EXPECT_EQ(source->read_view().value_at(24).primitive_id(), VariantPrimitiveId::INT64);
}

TEST(VariantNativeV2TypeTest, TypedRowsMatchEnsureEncodedNarrowingAndFallbacks) {
    auto integers = ColumnVariantV2::create_typed(
            nullable_fixed<ColumnInt64, Int64>({1, 128, 32768, int64_t {1} << 40}, {0, 0, 0, 0}),
            std::make_shared<DataTypeInt64>());
    const std::array<std::string_view, 4> integer_words {"tinyint", "smallint", "int", "bigint"};
    expect_same_typed_and_encoded(*integers, integer_words);

    const __int128 decimal38_max = power_of_ten_i128(38) - 1;
    const __int128 outside_decimal38 = power_of_ten_i128(38);
    auto largeints = ColumnVariantV2::create_typed(
            nullable_fixed<ColumnInt128, Int128>({decimal38_max, -decimal38_max, outside_decimal38},
                                                 {0, 0, 0}),
            std::make_shared<DataTypeInt128>());
    const std::array<std::string_view, 3> largeint_words {"decimal", "decimal", "string"};
    expect_same_typed_and_encoded(*largeints, largeint_words);

    auto decimals = ColumnVariantV2::create_typed(
            nullable_decimal<ColumnDecimal32, Decimal32>(2, {Decimal32 {1234}}, {0}),
            std::make_shared<DataTypeDecimal32>(9, 2));
    const std::array<std::string_view, 1> decimal_words {"decimal"};
    expect_same_typed_and_encoded(*decimals, decimal_words);

    const std::array<std::string_view, 1> string_word {"string"};

    IPv4 ipv4 {};
    ASSERT_TRUE(IPv4Value::from_string(ipv4, "192.0.2.1"));
    auto address = ColumnVariantV2::create_typed(nullable_fixed<ColumnIPv4, IPv4>({ipv4}, {0}),
                                                 std::make_shared<DataTypeIPv4>());
    expect_same_typed_and_encoded(*address, string_word);

    auto datetime = DateV2Value<DateTimeV2ValueType>::create_from_olap_datetime(19700101000001ULL);
    auto timestamp_ntz = ColumnVariantV2::create_typed(
            nullable_fixed<ColumnDateTimeV2, DateV2Value<DateTimeV2ValueType>>({datetime}, {0}),
            std::make_shared<DataTypeDateTimeV2>(6));
    const std::array<std::string_view, 1> timestamp_ntz_word {"timestamp_ntz"};
    expect_same_typed_and_encoded(*timestamp_ntz, timestamp_ntz_word);

    TimestampTzValue timestamp_value;
    timestamp_value.unchecked_set_time(1970, 1, 1, 0, 0, 2, 345678);
    auto timestamp = ColumnVariantV2::create_typed(
            nullable_fixed<ColumnTimeStampTz, TimestampTzValue>({timestamp_value}, {0}),
            std::make_shared<DataTypeTimeStampTz>(6));
    const std::array<std::string_view, 1> timestamp_word {"timestamp"};
    expect_same_typed_and_encoded(*timestamp, timestamp_word);
}

TEST(VariantNativeV2TypeTest, SqlNullMasksHiddenRowsAndVariantNullRemainsAWord) {
    const std::vector<std::string> encoded_rows {
            std::string(1, static_cast<char>(VariantBasicType::OBJECT)),
            encoded(VariantScalarEncodingPlan::null_value())};
    auto encoded_source = raw_encoded_column(encoded_rows);
    const std::array<uint8_t, 2> encoded_outer_nulls {1, 0};
    const ColumnPtr encoded_result = type_words(*encoded_source, encoded_outer_nulls);
    expect_word(encoded_result, 0, {}, true);
    expect_word(encoded_result, 1, "null");

    auto values = ColumnInt32::create();
    constexpr std::array<int32_t, 3> TYPED_VALUES {1, 2, 3};
    values->get_data().insert(TYPED_VALUES.begin(), TYPED_VALUES.end());
    auto inner_nulls = ColumnUInt8::create();
    constexpr std::array<uint8_t, 3> INNER_NULLS {0, 1, 0};
    inner_nulls->get_data().insert(INNER_NULLS.begin(), INNER_NULLS.end());
    auto typed = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(inner_nulls)),
            std::make_shared<DataTypeInt32>());
    const std::array<uint8_t, 3> typed_outer_nulls {0, 0, 1};
    const ColumnPtr typed_result = type_words(*typed, typed_outer_nulls);
    expect_word(typed_result, 0, "tinyint");
    expect_word(typed_result, 1, "null");
    expect_word(typed_result, 2, {}, true);
    EXPECT_TRUE(typed->is_typed());

    auto invalid_date = ColumnDateV2::create();
    DateV2Value<DateV2ValueType> invalid_date_value;
    ASSERT_FALSE(invalid_date_value.is_valid_date());
    invalid_date->insert_value(invalid_date_value);
    auto invalid_date_nulls = ColumnUInt8::create(1, 0);
    auto masked_typed = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(invalid_date), std::move(invalid_date_nulls)),
            std::make_shared<DataTypeDateV2>());
    const std::array<uint8_t, 1> masked_outer_null {1};
    const ColumnPtr masked_result = type_words(*masked_typed, masked_outer_null);
    expect_word(masked_result, 0, {}, true);
    EXPECT_TRUE(masked_typed->is_typed());

    auto sentinel = ColumnString::create();
    sentinel->insert_data("sentinel", 8);
    ColumnPtr unchanged_output = sentinel->get_ptr();
    const IColumn* const unchanged_identity = unchanged_output.get();
    const Status unmasked_status = variant_type_v2(*masked_typed, {}, &unchanged_output);
    EXPECT_EQ(unmasked_status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(unchanged_output.get(), unchanged_identity);
    EXPECT_TRUE(masked_typed->is_typed());
}

TEST(VariantNativeV2TypeTest, BadEncodedInputFailsAtomically) {
    const std::vector<std::string> truncated_rows {
            std::string(1, static_cast<char>(VariantBasicType::OBJECT))};
    auto truncated = raw_encoded_column(truncated_rows);
    auto sentinel = ColumnString::create();
    sentinel->insert_data("sentinel", 8);
    ColumnPtr output = sentinel->get_ptr();
    const IColumn* const identity = output.get();

    Status status = variant_type_v2(*truncated, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    const auto unknown_header =
            static_cast<char>((VARIANT_MAX_PRIMITIVE_ID + 1) << VARIANT_VALUE_HEADER_SHIFT);
    const std::vector<std::string> unknown_rows {std::string(1, unknown_header)};
    auto unknown = raw_encoded_column(unknown_rows);
    status = variant_type_v2(*unknown, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    std::string invalid_decimal(6, '\0');
    invalid_decimal[0] = static_cast<char>(static_cast<uint8_t>(VariantPrimitiveId::DECIMAL4)
                                           << VARIANT_VALUE_HEADER_SHIFT);
    invalid_decimal[1] = 39;
    const std::vector<std::string> invalid_decimal_rows {invalid_decimal};
    auto invalid_decimal_source = raw_encoded_column(invalid_decimal_rows);
    status = variant_type_v2(*invalid_decimal_source, {}, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);

    const std::array<uint8_t, 2> wrong_outer_nulls {0, 0};
    status = variant_type_v2(*unknown, wrong_outer_nulls, &output);
    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_EQ(output.get(), identity);
    EXPECT_EQ(variant_type_v2(*unknown, {}, nullptr).code(), ErrorCode::INVALID_ARGUMENT);
}

} // namespace doris
