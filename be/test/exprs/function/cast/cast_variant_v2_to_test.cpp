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

#include <array>
#include <limits>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_ipv4.h"
#include "core/data_type/data_type_ipv6.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_time.h"
#include "core/data_type/data_type_variant.h"
#include "core/field.h"
#include "core/value/ipv4_value.h"
#include "core/value/ipv6_value.h"
#include "core/value/large_int_value.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/function/cast/variant_v2/cast_variant_v2.h"
#include "exprs/function_context.h"
#include "gtest/gtest.h"
#include "runtime/runtime_state.h"
#include "util/jsonb_writer.h"

namespace doris::CastWrapper {
namespace {

struct CastResult {
    Status status;
    ColumnPtr column;
    ColumnPtr initial_result;
    DataTypePtr result_type;
};

CastResult execute_to_variant(const ColumnPtr& source, const DataTypePtr& source_type,
                              const NullMap::value_type* null_map = nullptr,
                              DataTypePtr result_type = std::make_shared<DataTypeVariant>()) {
    ColumnPtr initial_result = result_type->create_column();
    Block block {{source, source_type, "source"}, {initial_result, result_type, "result"}};
    RuntimeState state;
    auto context = FunctionContext::create_context(&state, {}, {});
    Status status = create_cast_to_variant_v2_wrapper(source_type)(context.get(), block, {0}, 1,
                                                                   source->size(), null_map);
    return CastResult {.status = std::move(status),
                       .column = block.get_by_position(1).column,
                       .initial_result = std::move(initial_result),
                       .result_type = block.get_by_position(1).type};
}

ColumnVariantV2::MutablePtr one_encoded_int(int64_t value) {
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = 1});
    auto row = builder.begin_row();
    row.add_int(value);
    row.finish();
    VariantBatchBuilder block = builder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    return result;
}

} // namespace

TEST(CastVariantV2ToTest, DormantFactoryExists) {
    EXPECT_TRUE(static_cast<bool>(
            create_cast_to_variant_v2_wrapper(std::make_shared<DataTypeInt32>())));
}

TEST(CastVariantV2ToTest, ScalarCreatesTypedStateAndSharesSourceColumn) {
    auto source = ColumnInt32::create();
    source->insert_value(7);
    source->insert_value(99);
    const IColumn* source_identity = source.get();
    constexpr std::array<NullMap::value_type, 2> NULLS {0, 1};

    CastResult cast =
            execute_to_variant(source->get_ptr(), std::make_shared<DataTypeInt32>(), NULLS.data());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& variant = assert_cast<const ColumnVariantV2&>(*cast.column);
    ASSERT_TRUE(variant.is_typed());
    EXPECT_EQ(variant.typed_type()->get_primitive_type(), TYPE_INT);
    const auto& nullable = assert_cast<const ColumnNullable&>(variant.typed_column());
    EXPECT_EQ(nullable.get_null_map_data()[0], 0);
    EXPECT_EQ(nullable.get_null_map_data()[1], 1);
    EXPECT_EQ(&nullable.get_nested_column(), source_identity);
    EXPECT_EQ(source->get_data()[0], 7);
    EXPECT_EQ(source->get_data()[1], 99);

    MutableColumnPtr mutable_variant = IColumn::mutate(std::move(cast.column));
    assert_cast<ColumnVariantV2&>(*mutable_variant).pop_back(1);
    EXPECT_EQ(mutable_variant->size(), 1);
    EXPECT_EQ(source->size(), 2);
    EXPECT_EQ(source->get_data()[0], 7);
    EXPECT_EQ(source->get_data()[1], 99);
}

TEST(CastVariantV2ToTest, IpScalarSourcesUseTypedIdentityWhitelist) {
    IPv4 ipv4 {};
    ASSERT_TRUE(IPv4Value::from_string(ipv4, "192.0.2.1"));
    auto ipv4_source = ColumnIPv4::create();
    ipv4_source->insert_value(ipv4);
    CastResult ipv4_cast =
            execute_to_variant(ipv4_source->get_ptr(), std::make_shared<DataTypeIPv4>());
    ASSERT_TRUE(ipv4_cast.status.ok()) << ipv4_cast.status;
    const auto& ipv4_variant = assert_cast<const ColumnVariantV2&>(*ipv4_cast.column);
    ASSERT_TRUE(ipv4_variant.is_typed());
    EXPECT_EQ(ipv4_variant.typed_type()->get_primitive_type(), TYPE_IPV4);
    EXPECT_EQ(&assert_cast<const ColumnNullable&>(ipv4_variant.typed_column()).get_nested_column(),
              ipv4_source.get());

    IPv6 ipv6 {};
    ASSERT_TRUE(IPv6Value::from_string(ipv6, "2001:db8::1"));
    auto ipv6_source = ColumnIPv6::create();
    ipv6_source->insert_value(ipv6);
    CastResult ipv6_cast =
            execute_to_variant(ipv6_source->get_ptr(), std::make_shared<DataTypeIPv6>());
    ASSERT_TRUE(ipv6_cast.status.ok()) << ipv6_cast.status;
    const auto& ipv6_variant = assert_cast<const ColumnVariantV2&>(*ipv6_cast.column);
    ASSERT_TRUE(ipv6_variant.is_typed());
    EXPECT_EQ(ipv6_variant.typed_type()->get_primitive_type(), TYPE_IPV6);
    EXPECT_EQ(&assert_cast<const ColumnNullable&>(ipv6_variant.typed_column()).get_nested_column(),
              ipv6_source.get());
}

TEST(CastVariantV2ToTest, StringIsAStringScalarAndIsNotParsed) {
    auto source = ColumnString::create();
    source->insert_data(R"({"a":1})", 7);
    CastResult cast = execute_to_variant(source->get_ptr(), std::make_shared<DataTypeString>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    auto encoded = IColumn::mutate(cast.column);
    auto& variant = assert_cast<ColumnVariantV2&>(*encoded);
    variant.ensure_encoded();
    ASSERT_EQ(variant.get_value_ref(0).basic_type(), VariantBasicType::SHORT_STRING);
    EXPECT_EQ(variant.get_value_ref(0).get_string(), StringRef(R"({"a":1})"));
}

TEST(CastVariantV2ToTest, JsonbObjectUsesDocumentTranscode) {
    JsonbWriter writer;
    ASSERT_TRUE(writer.writeStartObject());
    ASSERT_TRUE(writer.writeKey("a", 1));
    ASSERT_TRUE(writer.writeInt(1));
    ASSERT_TRUE(writer.writeEndObject());
    auto source = ColumnString::create();
    source->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());

    CastResult cast = execute_to_variant(source->get_ptr(), std::make_shared<DataTypeJsonb>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& variant = assert_cast<const ColumnVariantV2&>(*cast.column);
    VariantRef value = variant.get_value_ref(0);
    ASSERT_EQ(value.basic_type(), VariantBasicType::OBJECT);
    VariantRef field;
    ASSERT_TRUE(value.object_find(StringRef("a"), &field));
    EXPECT_EQ(field.get_int(), 1);
}

TEST(CastVariantV2ToTest, ArrayRecursesAndPreservesElementNull) {
    auto nested = ColumnInt32::create();
    nested->insert_value(1);
    nested->insert_value(0);
    nested->insert_value(3);
    auto element_nulls = ColumnUInt8::create();
    element_nulls->get_data().push_back(0);
    element_nulls->get_data().push_back(1);
    element_nulls->get_data().push_back(0);
    auto elements = ColumnNullable::create(std::move(nested), std::move(element_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(3);
    auto source = ColumnArray::create(std::move(elements), std::move(offsets));
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt32>());

    CastResult cast = execute_to_variant(source->get_ptr(), type);
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    VariantRef array = assert_cast<const ColumnVariantV2&>(*cast.column).get_value_ref(0);
    ASSERT_EQ(array.basic_type(), VariantBasicType::ARRAY);
    ASSERT_EQ(array.num_elements(), 3);
    EXPECT_EQ(array.array_at(0).get_int(), 1);
    EXPECT_TRUE(array.array_at(1).is_null());
    EXPECT_EQ(array.array_at(2).get_int(), 3);
}

TEST(CastVariantV2ToTest, EmptyArrayOfNothingIsLegal) {
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());
    MutableColumnPtr source = type->create_column();
    source->insert_many_defaults(2);
    CastResult cast = execute_to_variant(source->get_ptr(), type);
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& variant = assert_cast<const ColumnVariantV2&>(*cast.column);
    ASSERT_EQ(variant.size(), 2);
    EXPECT_EQ(variant.get_value_ref(0).num_elements(), 0);
    EXPECT_EQ(variant.get_value_ref(1).num_elements(), 0);
}

TEST(CastVariantV2ToTest, IpScalarsKeepTheirTypeUntilEncodingAndArrayElementsUseStrings) {
    IPv4 ipv4 {};
    ASSERT_TRUE(IPv4Value::from_string(ipv4, "192.0.2.1"));
    auto ipv4_column = ColumnIPv4::create();
    ipv4_column->insert_value(ipv4);
    auto exact_variant = std::make_shared<DataTypeVariant>(37, true);
    CastResult scalar = execute_to_variant(ipv4_column->get_ptr(), std::make_shared<DataTypeIPv4>(),
                                           nullptr, exact_variant);
    ASSERT_TRUE(scalar.status.ok()) << scalar.status;
    EXPECT_TRUE(scalar.result_type->equals(*exact_variant));
    auto scalar_owner = IColumn::mutate(scalar.column);
    auto& scalar_variant = assert_cast<ColumnVariantV2&>(*scalar_owner);
    ASSERT_TRUE(scalar_variant.is_typed());
    EXPECT_EQ(scalar_variant.typed_type()->get_primitive_type(), TYPE_IPV4);
    scalar_variant.ensure_encoded();
    EXPECT_EQ(scalar_variant.get_value_ref(0).get_string(), StringRef("192.0.2.1"));

    IPv6 ipv6 {};
    ASSERT_TRUE(IPv6Value::from_string(ipv6, "2001:db8::1"));
    auto ipv6_column = ColumnIPv6::create();
    ipv6_column->insert_value(ipv6);
    CastResult scalar6 =
            execute_to_variant(ipv6_column->get_ptr(), std::make_shared<DataTypeIPv6>());
    ASSERT_TRUE(scalar6.status.ok()) << scalar6.status;
    auto scalar6_owner = IColumn::mutate(scalar6.column);
    auto& scalar6_variant = assert_cast<ColumnVariantV2&>(*scalar6_owner);
    ASSERT_TRUE(scalar6_variant.is_typed());
    EXPECT_EQ(scalar6_variant.typed_type()->get_primitive_type(), TYPE_IPV6);
    scalar6_variant.ensure_encoded();
    EXPECT_EQ(scalar6_variant.get_value_ref(0).get_string(), StringRef("2001:db8::1"));

    auto nested = ColumnIPv4::create();
    nested->insert_value(ipv4);
    nested->insert_default();
    auto element_nulls = ColumnUInt8::create();
    element_nulls->insert_value(0);
    element_nulls->insert_value(1);
    auto elements = ColumnNullable::create(std::move(nested), std::move(element_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(2);
    auto source = ColumnArray::create(std::move(elements), std::move(offsets));
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeIPv4>());
    CastResult array = execute_to_variant(source->get_ptr(), type);
    ASSERT_TRUE(array.status.ok()) << array.status;
    VariantRef value = assert_cast<const ColumnVariantV2&>(*array.column).get_value_ref(0);
    ASSERT_EQ(value.basic_type(), VariantBasicType::ARRAY);
    EXPECT_EQ(value.array_at(0).get_string(), StringRef("192.0.2.1"));
    EXPECT_TRUE(value.array_at(1).is_null());

    auto nested6 = ColumnIPv6::create();
    nested6->insert_value(ipv6);
    auto elements6 = ColumnNullable::create(std::move(nested6), ColumnUInt8::create(1, 0));
    auto offsets6 = ColumnArray::ColumnOffsets::create();
    offsets6->insert_value(1);
    auto source6 = ColumnArray::create(std::move(elements6), std::move(offsets6));
    auto type6 = std::make_shared<DataTypeArray>(std::make_shared<DataTypeIPv6>());
    CastResult array6 = execute_to_variant(source6->get_ptr(), type6);
    ASSERT_TRUE(array6.status.ok()) << array6.status;
    VariantRef value6 = assert_cast<const ColumnVariantV2&>(*array6.column).get_value_ref(0);
    ASSERT_EQ(value6.basic_type(), VariantBasicType::ARRAY);
    EXPECT_EQ(value6.array_at(0).get_string(), StringRef("2001:db8::1"));
}

TEST(CastVariantV2ToTest, NullArrayRowDoesNotParseHiddenJsonbElements) {
    auto jsonb = ColumnString::create();
    jsonb->insert_data("not-jsonb", 9);
    auto element_nulls = ColumnUInt8::create(1, 0);
    auto elements = ColumnNullable::create(std::move(jsonb), std::move(element_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(1);
    auto source = ColumnArray::create(std::move(elements), std::move(offsets));
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeJsonb>());
    constexpr std::array<NullMap::value_type, 1> NULLS {1};

    CastResult cast = execute_to_variant(source->get_ptr(), type, NULLS.data());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(*cast.column).get_value_ref(0).is_null());
}

TEST(CastVariantV2ToTest, NullArrayRowDoesNotEncodeHiddenTypedVariantValue) {
    auto invalid_date = ColumnDateV2::create();
    DateV2Value<DateV2ValueType> invalid_date_value;
    ASSERT_FALSE(invalid_date_value.is_valid_date());
    invalid_date->insert_value(invalid_date_value);
    auto internal_nulls = ColumnUInt8::create(1, 0);
    auto typed_variant = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(invalid_date), std::move(internal_nulls)),
            std::make_shared<DataTypeDateV2>());
    const ColumnVariantV2* const typed_identity = typed_variant.get();
    auto element_nulls = ColumnUInt8::create(1, 0);
    auto elements = ColumnNullable::create(std::move(typed_variant), std::move(element_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->get_data().push_back(1);
    auto source = ColumnArray::create(std::move(elements), std::move(offsets));
    auto type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeVariant>());
    constexpr std::array<NullMap::value_type, 1> NULLS {1};

    CastResult unmasked = execute_to_variant(source->get_ptr(), type);
    EXPECT_TRUE(unmasked.status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(unmasked.column.get(), unmasked.initial_result.get());
    EXPECT_TRUE(typed_identity->is_typed());

    CastResult cast = execute_to_variant(source->get_ptr(), type, NULLS.data());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(*cast.column).get_value_ref(0).is_null());
    EXPECT_TRUE(typed_identity->is_typed());
}

TEST(CastVariantV2ToTest, NothingSourceProducesInternalNullRows) {
    auto type = std::make_shared<DataTypeNothing>();
    MutableColumnPtr source = type->create_column();
    source->insert_many_defaults(2);
    constexpr std::array<NullMap::value_type, 2> NULLS {1, 1};
    CastResult cast = execute_to_variant(source->get_ptr(), type, NULLS.data());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    const auto& variant = assert_cast<const ColumnVariantV2&>(*cast.column);
    EXPECT_TRUE(variant.get_value_ref(0).is_null());
    EXPECT_TRUE(variant.get_value_ref(1).is_null());
}

TEST(CastVariantV2ToTest, VariantIdentityKeepsColumnPointer) {
    ColumnPtr source = one_encoded_int(5);
    CastResult cast = execute_to_variant(source, std::make_shared<DataTypeVariant>());
    ASSERT_TRUE(cast.status.ok()) << cast.status;
    EXPECT_EQ(cast.column.get(), source.get());
}

TEST(CastVariantV2ToTest, UnsupportedMapAndConstInputLeaveResultUntouched) {
    auto map_type = std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(),
                                                  std::make_shared<DataTypeInt32>());
    MutableColumnPtr map = map_type->create_column();
    map->insert_default();
    CastResult unsupported = execute_to_variant(map->get_ptr(), map_type);
    EXPECT_TRUE(unsupported.status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(unsupported.column.get(), unsupported.initial_result.get());

    auto time_type = std::make_shared<DataTypeTimeV2>();
    MutableColumnPtr time = time_type->create_column();
    time->insert_default();
    CastResult unsupported_time = execute_to_variant(time->get_ptr(), time_type);
    EXPECT_TRUE(unsupported_time.status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(unsupported_time.column.get(), unsupported_time.initial_result.get());

    auto value = ColumnInt32::create();
    value->insert_value(1);
    ColumnPtr constant = ColumnConst::create(std::move(value), 3);
    CastResult const_result = execute_to_variant(constant, std::make_shared<DataTypeInt32>());
    EXPECT_TRUE(const_result.status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(const_result.column.get(), const_result.initial_result.get());
}

TEST(CastVariantV2ToTest, Decimal256IsRejectedAndLargeIntFallsBackToString) {
    auto decimal39_type = std::make_shared<DataTypeDecimal256>(39, 0);
    MutableColumnPtr decimal39 = decimal39_type->create_column();
    decimal39->insert_default();
    CastResult decimal39_cast = execute_to_variant(decimal39->get_ptr(), decimal39_type);
    EXPECT_TRUE(decimal39_cast.status.is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_EQ(decimal39_cast.column.get(), decimal39_cast.initial_result.get());

    __int128 power_of_ten_38 = 1;
    for (int digit = 0; digit < 38; ++digit) {
        power_of_ten_38 *= 10;
    }
    for (const __int128 value : {power_of_ten_38 - 1, -(power_of_ten_38 - 1)}) {
        auto supported = ColumnInt128::create();
        supported->insert_value(value);
        CastResult supported_cast =
                execute_to_variant(supported->get_ptr(), std::make_shared<DataTypeInt128>());
        ASSERT_TRUE(supported_cast.status.ok()) << supported_cast.status;
        EXPECT_TRUE(assert_cast<const ColumnVariantV2&>(*supported_cast.column).is_typed());
    }

    for (const __int128 value :
         {power_of_ten_38, -power_of_ten_38, std::numeric_limits<__int128>::max(),
          std::numeric_limits<__int128>::min()}) {
        auto large = ColumnInt128::create();
        large->insert_value(value);
        CastResult large_cast =
                execute_to_variant(large->get_ptr(), std::make_shared<DataTypeInt128>());
        ASSERT_TRUE(large_cast.status.ok()) << large_cast.status;
        MutableColumnPtr encoded = large_cast.column->clone();
        auto& variant = assert_cast<ColumnVariantV2&>(*encoded);
        variant.ensure_encoded();
        const VariantRef encoded_value = variant.get_value_ref(0);
        EXPECT_EQ(encoded_value.get_string(), StringRef(LargeIntValue::to_string(value)));
    }

    auto nested = ColumnInt128::create();
    nested->insert_value(1);
    nested->insert_value(power_of_ten_38);
    auto element_nulls = ColumnUInt8::create(2, 0);
    auto elements = ColumnNullable::create(std::move(nested), std::move(element_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(2);
    auto array = ColumnArray::create(std::move(elements), std::move(offsets));
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt128>());
    CastResult array_cast = execute_to_variant(array->get_ptr(), array_type);
    ASSERT_TRUE(array_cast.status.ok()) << array_cast.status;
    EXPECT_EQ(assert_cast<const ColumnVariantV2&>(*array_cast.column)
                      .read_view()
                      .value_at(0)
                      .basic_type(),
              VariantBasicType::ARRAY);
}

} // namespace doris::CastWrapper
