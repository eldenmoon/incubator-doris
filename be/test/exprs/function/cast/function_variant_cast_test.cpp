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

#include <string_view>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exec/common/variant_util.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace {

Status execute_cast(ColumnPtr source, DataTypePtr source_type, DataTypePtr target_type,
                    ColumnPtr* output) {
    ColumnsWithTypeAndName arguments {{std::move(source), std::move(source_type), "source"},
                                      {nullptr, target_type, "target"}};
    auto function = SimpleFunctionFactory::instance().get_function("CAST", arguments, target_type);
    if (!function) {
        return Status::NotFound("CAST overload is not registered");
    }
    Block block {std::move(arguments)};
    const size_t result = block.columns();
    block.insert({nullptr, target_type, "result"});
    RuntimeState state;
    auto context = FunctionContext::create_context(&state, {}, {});
    RETURN_IF_ERROR(function->execute(context.get(), block, {0}, result, block.rows()));
    *output = block.get_by_position(result).column;
    return Status::OK();
}

ColumnVariantV2::MutablePtr encoded_scalars() {
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = 4});
    {
        auto row = builder.begin_row();
        row.add_int(42);
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_string(StringRef("7"));
        row.finish();
    }
    {
        auto row = builder.begin_row();
        auto object = row.start_object();
        object.add_key(StringRef("a"));
        row.add_int(1);
        object.finish();
        row.finish();
    }
    {
        auto row = builder.begin_row();
        row.add_null();
        row.finish();
    }
    VariantBatchBuilder block = builder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    return result;
}

std::string variant_text_at(const IColumn& column, size_t row) {
    DataTypeVariantV2SerDe serde;
    auto output = ColumnString::create();
    BufferWritable writer(*output);
    DataTypeSerDe::FormatOptions options;
    serde.to_string(column, row, writer, options);
    writer.commit();
    return output->get_data_at(0).to_string();
}

} // namespace

TEST(FunctionVariantCast, StorageCastPreservesStringScalarsWithoutSession) {
    auto strings = ColumnString::create();
    for (const std::string_view value : {"", "1", "true", "null", "{}"}) {
        strings->insert_data(value.data(), value.size());
    }

    auto source_type = std::make_shared<DataTypeString>();
    auto target_type = std::make_shared<DataTypeVariant>();
    ColumnPtr output;
    ASSERT_TRUE(variant_util::cast_column({strings->get_ptr(), source_type, "v.path"}, target_type,
                                          &output)
                        .ok());

    const auto& variant = assert_cast<const ColumnVariantV2&>(*output);
    const std::vector<std::string> expected {R"("")", R"("1")", R"("true")", R"("null")",
                                             R"("{}")"};
    ASSERT_EQ(variant.size(), expected.size());
    for (size_t row = 0; row < expected.size(); ++row) {
        EXPECT_EQ(variant_text_at(variant, row), expected[row]);
    }
}

TEST(FunctionVariantCast, PublicCastToVariantCreatesTypedV2) {
    auto source_type = std::make_shared<DataTypeInt32>();
    auto target_type = std::make_shared<DataTypeVariant>(37, true);
    auto source = ColumnInt32::create();
    source->insert_value(42);
    source->insert_value(-3);

    ColumnPtr output;
    ASSERT_TRUE(execute_cast(source->get_ptr(), source_type, target_type, &output).ok());
    const auto& variant = assert_cast<const ColumnVariantV2&>(*output);
    ASSERT_TRUE(variant.is_typed());
    EXPECT_EQ(variant.typed_type()->get_primitive_type(), TYPE_INT);
    EXPECT_EQ(variant_text_at(variant, 0), "42");
    EXPECT_EQ(variant_text_at(variant, 1), "-3");

    auto strings = ColumnString::create();
    strings->insert_data("{\"a\":1}", 7);
    ASSERT_TRUE(execute_cast(strings->get_ptr(), std::make_shared<DataTypeString>(), target_type,
                             &output)
                        .ok());
    const auto& string_variant = assert_cast<const ColumnVariantV2&>(*output);
    ASSERT_TRUE(string_variant.is_typed());
    EXPECT_EQ(string_variant.typed_type()->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(variant_text_at(string_variant, 0), R"("{\"a\":1}")");
}

TEST(FunctionVariantCast, PublicCastFromVariantPreservesNullsAndSource) {
    auto source = encoded_scalars();
    ASSERT_FALSE(source->is_typed());
    std::vector<std::string> before;
    for (size_t row = 0; row < source->size(); ++row) {
        before.push_back(VariantField::from_ref(source->get_value_ref(row)).bytes().to_string());
    }

    auto source_type = std::make_shared<DataTypeVariant>(37, true);
    auto result_type = make_nullable(std::make_shared<DataTypeInt32>());
    ColumnPtr output;
    ASSERT_TRUE(execute_cast(source->get_ptr(), source_type, result_type, &output).ok());
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    const auto& values = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    ASSERT_EQ(values.size(), 4);
    EXPECT_FALSE(nullable.is_null_at(0));
    EXPECT_EQ(values.get_element(0), 42);
    EXPECT_FALSE(nullable.is_null_at(1));
    EXPECT_EQ(values.get_element(1), 7);
    EXPECT_TRUE(nullable.is_null_at(2));
    EXPECT_TRUE(nullable.is_null_at(3));

    EXPECT_FALSE(source->is_typed());
    for (size_t row = 0; row < source->size(); ++row) {
        EXPECT_EQ(VariantField::from_ref(source->get_value_ref(row)).bytes().to_string(),
                  before[row]);
    }
}

TEST(FunctionVariantCast, PublicCastMaterializesConstAndPreservesOuterNulls) {
    auto target_type = std::make_shared<DataTypeVariant>(37, true);
    auto constant_data = ColumnInt32::create();
    constant_data->insert_value(7);
    ColumnPtr constant = ColumnConst::create(std::move(constant_data), 3);

    ColumnPtr output;
    ASSERT_TRUE(
            execute_cast(constant, std::make_shared<DataTypeInt32>(), target_type, &output).ok());
    ColumnPtr materialized = output->convert_to_full_column_if_const();
    const auto& constant_variant = assert_cast<const ColumnVariantV2&>(*materialized);
    ASSERT_EQ(constant_variant.size(), 3);
    for (size_t row = 0; row < constant_variant.size(); ++row) {
        EXPECT_EQ(variant_text_at(constant_variant, row), "7");
    }

    auto values = ColumnString::create();
    values->insert_data("kept", 4);
    values->insert_default();
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(0);
    nulls->insert_value(1);
    ColumnPtr nullable_source = ColumnNullable::create(std::move(values), std::move(nulls));
    auto nullable_source_type = make_nullable(std::make_shared<DataTypeString>());
    auto nullable_target_type = make_nullable(target_type);

    ASSERT_TRUE(execute_cast(nullable_source, nullable_source_type, nullable_target_type, &output)
                        .ok());
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    EXPECT_FALSE(nullable.is_null_at(0));
    EXPECT_TRUE(nullable.is_null_at(1));
    const auto& variant = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_TRUE(variant.is_typed());
    EXPECT_EQ(variant.typed_type()->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(variant_text_at(variant, 0), "\"kept\"");
}

TEST(FunctionVariantCast, DifferentVariantConfigurationsAreCompatibleButWrongPhysicalColumnsFail) {
    auto source_type = std::make_shared<DataTypeVariant>(37, true);
    auto target_type = std::make_shared<DataTypeVariant>(38, true);
    auto source = encoded_scalars();
    ColumnPtr output;
    ASSERT_TRUE(execute_cast(source->get_ptr(), source_type, target_type, &output).ok());
    EXPECT_EQ(output.get(), source.get());
    ASSERT_TRUE(execute_cast(source->get_ptr(), source_type, source_type, &output).ok());
    EXPECT_NE(dynamic_cast<const ColumnVariantV2*>(output.get()), nullptr);

    auto wrong_physical = ColumnString::create();
    wrong_physical->insert_data("not a variant column", 20);
    EXPECT_FALSE(execute_cast(wrong_physical->get_ptr(), source_type, source_type, &output).ok());
    EXPECT_FALSE(execute_cast(wrong_physical->get_ptr(), source_type,
                              std::make_shared<DataTypeString>(), &output)
                         .ok());
}

} // namespace doris
