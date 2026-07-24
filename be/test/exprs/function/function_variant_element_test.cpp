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

#include <limits>
#include <memory>
#include <string_view>

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
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace {

ColumnVariantV2::MutablePtr encoded_json(std::initializer_list<std::string_view> rows) {
    JsonStringToVariantEncoder encoder({.max_json_key_length = 255,
                                        .throw_on_invalid_json = true,
                                        .check_duplicate_json_path = false});
    for (std::string_view row : rows) {
        encoder.add_json({row.data(), row.size()});
    }
    VariantBatchBuilder block = encoder.finish_batch();
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(block);
    return column;
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

Status execute_element(ColumnPtr source, DataTypePtr source_type, ColumnPtr index,
                       DataTypePtr index_type, ColumnPtr* output) {
    auto result_type = make_nullable(remove_nullable(source_type));
    ColumnsWithTypeAndName arguments {{std::move(source), std::move(source_type), "source"},
                                      {std::move(index), std::move(index_type), "index"}};
    auto function =
            SimpleFunctionFactory::instance().get_function("element_at", arguments, result_type);
    if (!function) {
        return Status::NotFound("element_at overload is not registered");
    }
    Block block {std::move(arguments)};
    const size_t result = block.columns();
    block.insert({nullptr, result_type, "result"});
    RuntimeState state;
    auto context = FunctionContext::create_context(&state, {}, {});
    RETURN_IF_ERROR(function->execute(context.get(), block, {0, 1}, result, block.rows()));
    *output = block.get_by_position(result).column;
    return Status::OK();
}

ColumnPtr constant_string(std::string_view value, size_t rows) {
    auto data = ColumnString::create();
    data->insert_data(value.data(), value.size());
    return ColumnConst::create(std::move(data), rows);
}

} // namespace

TEST(function_variant_element_test, StringPathUsesPathInDataContract) {
    auto type = std::make_shared<DataTypeVariant>(37, true);
    auto source = encoded_json(
            {R"({"":{"value":1},"$foo":"dollar","outer":{"":{"leaf":2}},"profile":{"name":"John","nullable":null}})",
             R"({"profile":{}})"});
    ColumnPtr output;

    ASSERT_TRUE(execute_element(source->get_ptr(), type, constant_string("", source->size()),
                                std::make_shared<DataTypeString>(), &output)
                        .ok());
    const auto& empty_key = assert_cast<const ColumnNullable&>(*output);
    EXPECT_FALSE(empty_key.is_null_at(0));
    EXPECT_TRUE(empty_key.is_null_at(1));
    EXPECT_EQ(variant_text_at(empty_key.get_nested_column(), 0), R"({"value":1})");

    ASSERT_TRUE(execute_element(source->get_ptr(), type, constant_string("$foo", source->size()),
                                std::make_shared<DataTypeString>(), &output)
                        .ok());
    const auto& dollar_key = assert_cast<const ColumnNullable&>(*output);
    EXPECT_FALSE(dollar_key.is_null_at(0));
    EXPECT_TRUE(dollar_key.is_null_at(1));
    EXPECT_EQ(variant_text_at(dollar_key.get_nested_column(), 0), "\"dollar\"");

    ASSERT_TRUE(execute_element(source->get_ptr(), type,
                                constant_string("outer..leaf", source->size()),
                                std::make_shared<DataTypeString>(), &output)
                        .ok());
    const auto& empty_segment = assert_cast<const ColumnNullable&>(*output);
    EXPECT_FALSE(empty_segment.is_null_at(0));
    EXPECT_TRUE(empty_segment.is_null_at(1));
    EXPECT_EQ(variant_text_at(empty_segment.get_nested_column(), 0), "2");

    ASSERT_TRUE(execute_element(source->get_ptr(), type,
                                constant_string("profile.name", source->size()),
                                std::make_shared<DataTypeString>(), &output)
                        .ok());
    const auto& names = assert_cast<const ColumnNullable&>(*output);
    EXPECT_FALSE(names.is_null_at(0));
    EXPECT_TRUE(names.is_null_at(1));
    EXPECT_EQ(variant_text_at(names.get_nested_column(), 0), "\"John\"");

    ASSERT_TRUE(execute_element(source->get_ptr(), type,
                                constant_string("profile.nullable", source->size()),
                                std::make_shared<DataTypeString>(), &output)
                        .ok());
    const auto& nullable_value = assert_cast<const ColumnNullable&>(*output);
    EXPECT_FALSE(nullable_value.is_null_at(0));
    EXPECT_TRUE(nullable_value.is_null_at(1));
    EXPECT_EQ(variant_text_at(nullable_value.get_nested_column(), 0), "null");
}

TEST(function_variant_element_test, TypedStringRemainsScalarAndIntegerIsOneBased) {
    auto docs = ColumnString::create();
    std::string_view json = R"({"profile":{"name":"SRFSPXFDVY"},"n":49.98})";
    docs->insert_data(json.data(), json.size());
    auto typed = ColumnVariantV2::create_typed(make_nullable(std::move(docs)),
                                               std::make_shared<DataTypeString>());
    auto type = std::make_shared<DataTypeVariant>(0, false);

    ColumnPtr output;
    ASSERT_TRUE(execute_element(typed->get_ptr(), type, constant_string("profile.name", 1),
                                std::make_shared<DataTypeString>(), &output)
                        .ok());
    EXPECT_TRUE(assert_cast<const ColumnNullable&>(*output).is_null_at(0));

    auto array = encoded_json({R"(["first","second"])"});
    auto index_data = ColumnInt64::create();
    index_data->insert_value(2);
    ASSERT_TRUE(execute_element(array->get_ptr(), type,
                                ColumnConst::create(std::move(index_data), 1),
                                std::make_shared<DataTypeInt64>(), &output)
                        .ok());
    const auto& second = assert_cast<const ColumnNullable&>(*output);
    EXPECT_FALSE(second.is_null_at(0));
    EXPECT_EQ(variant_text_at(second.get_nested_column(), 0), "\"second\"");

    auto negative_one_data = ColumnInt64::create();
    negative_one_data->insert_value(-1);
    ASSERT_TRUE(execute_element(array->get_ptr(), type,
                                ColumnConst::create(std::move(negative_one_data), 1),
                                std::make_shared<DataTypeInt64>(), &output)
                        .ok());
    const auto& negative_last = assert_cast<const ColumnNullable&>(*output);
    EXPECT_FALSE(negative_last.is_null_at(0));
    EXPECT_EQ(variant_text_at(negative_last.get_nested_column(), 0), "\"second\"");

    auto negative_two_data = ColumnInt64::create();
    negative_two_data->insert_value(-2);
    ASSERT_TRUE(execute_element(array->get_ptr(), type,
                                ColumnConst::create(std::move(negative_two_data), 1),
                                std::make_shared<DataTypeInt64>(), &output)
                        .ok());
    const auto& negative_first = assert_cast<const ColumnNullable&>(*output);
    EXPECT_FALSE(negative_first.is_null_at(0));
    EXPECT_EQ(variant_text_at(negative_first.get_nested_column(), 0), "\"first\"");

    auto negative_out_of_bounds_data = ColumnInt64::create();
    negative_out_of_bounds_data->insert_value(-3);
    ASSERT_TRUE(execute_element(array->get_ptr(), type,
                                ColumnConst::create(std::move(negative_out_of_bounds_data), 1),
                                std::make_shared<DataTypeInt64>(), &output)
                        .ok());
    EXPECT_TRUE(assert_cast<const ColumnNullable&>(*output).is_null_at(0));

    auto minimum_data = ColumnInt64::create();
    minimum_data->insert_value(std::numeric_limits<int64_t>::min());
    ASSERT_TRUE(execute_element(array->get_ptr(), type,
                                ColumnConst::create(std::move(minimum_data), 1),
                                std::make_shared<DataTypeInt64>(), &output)
                        .ok());
    EXPECT_TRUE(assert_cast<const ColumnNullable&>(*output).is_null_at(0));

    auto zero_data = ColumnInt64::create();
    zero_data->insert_value(0);
    ASSERT_TRUE(execute_element(array->get_ptr(), type,
                                ColumnConst::create(std::move(zero_data), 1),
                                std::make_shared<DataTypeInt64>(), &output)
                        .ok());
    EXPECT_TRUE(assert_cast<const ColumnNullable&>(*output).is_null_at(0));

    auto overflowing_data = ColumnInt64::create();
    overflowing_data->insert_value(static_cast<int64_t>(std::numeric_limits<uint32_t>::max()) + 2);
    ASSERT_TRUE(execute_element(array->get_ptr(), type,
                                ColumnConst::create(std::move(overflowing_data), 1),
                                std::make_shared<DataTypeInt64>(), &output)
                        .ok());
    EXPECT_TRUE(assert_cast<const ColumnNullable&>(*output).is_null_at(0));
}

} // namespace doris
