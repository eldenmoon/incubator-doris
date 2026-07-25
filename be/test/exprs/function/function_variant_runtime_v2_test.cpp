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

#include <memory>
#include <string_view>

#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/value/variant/variant_batch_builder.h"
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

Status execute_function(std::string_view name, ColumnsWithTypeAndName arguments,
                        const ColumnNumbers& execution_arguments, DataTypePtr result_type,
                        ColumnPtr* output) {
    auto function = SimpleFunctionFactory::instance().get_function(std::string(name), arguments,
                                                                   result_type);
    if (!function) {
        return Status::NotFound("function {}", name);
    }
    Block block {std::move(arguments)};
    const size_t result = block.columns();
    block.insert({nullptr, result_type, "result"});
    RuntimeState state;
    auto context = FunctionContext::create_context(&state, {}, {});
    RETURN_IF_ERROR(
            function->execute(context.get(), block, execution_arguments, result, block.rows()));
    *output = block.get_by_position(result).column;
    return Status::OK();
}

ColumnPtr constant_string(std::string_view value, size_t rows) {
    auto data = ColumnString::create();
    data->insert_data(value.data(), value.size());
    return ColumnConst::create(std::move(data), rows);
}

} // namespace

TEST(FunctionVariantRuntimeV2Test, CastRoutesToV2AndPreservesExactConfiguration) {
    auto source_type = std::make_shared<DataTypeString>();
    auto target_type = std::make_shared<DataTypeVariant>(37, true);
    auto source = ColumnString::create();
    source->insert_data("plain", 5);
    source->insert_data("{\"a\":1}", 7);

    ColumnPtr output;
    ColumnsWithTypeAndName arguments {{source->get_ptr(), source_type, "source"},
                                      {nullptr, target_type, "target"}};
    ASSERT_TRUE(execute_function("CAST", std::move(arguments), {0}, target_type, &output).ok());
    const auto* variant = dynamic_cast<const ColumnVariantV2*>(output.get());
    ASSERT_NE(variant, nullptr);
    EXPECT_TRUE(variant->is_typed());
    EXPECT_EQ(variant->typed_type()->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(variant_text_at(*variant, 0), "\"plain\"");
    EXPECT_EQ(variant_text_at(*variant, 1), R"("{\"a\":1}")");

    auto mismatched_type = std::make_shared<DataTypeVariant>(38, true);
    const auto* identity = output.get();
    ColumnsWithTypeAndName mismatch {{output, target_type, "source"},
                                     {nullptr, mismatched_type, "target"}};
    ASSERT_TRUE(execute_function("CAST", std::move(mismatch), {0}, mismatched_type, &output).ok());
    EXPECT_EQ(output.get(), identity);
}

TEST(FunctionVariantRuntimeV2Test, ElementAtAndVariantTypeUseV2Kernels) {
    auto variant_type = std::make_shared<DataTypeVariant>(37, true);
    auto source_values = encoded_json({R"({"a":{"b":"x"}})", "null", R"({"a":1})"});
    auto source_nulls = ColumnUInt8::create();
    source_nulls->insert_value(0);
    source_nulls->insert_value(0);
    source_nulls->insert_value(1);
    ColumnPtr source = ColumnNullable::create(std::move(source_values), std::move(source_nulls));
    auto source_type = make_nullable(variant_type);
    auto path_data = ColumnString::create();
    path_data->insert_data("a.b", 3);
    ColumnPtr path = ColumnConst::create(std::move(path_data), source->size());

    ColumnPtr extracted;
    ColumnsWithTypeAndName element_arguments {{source, source_type, "source"},
                                              {path, std::make_shared<DataTypeString>(), "path"}};
    ASSERT_TRUE(execute_function("element_at", std::move(element_arguments), {0, 1},
                                 make_nullable(variant_type), &extracted)
                        .ok());
    const auto* nullable = dynamic_cast<const ColumnNullable*>(extracted.get());
    ASSERT_NE(nullable, nullptr);
    const auto* extracted_values =
            dynamic_cast<const ColumnVariantV2*>(&nullable->get_nested_column());
    ASSERT_NE(extracted_values, nullptr);
    ASSERT_EQ(extracted_values->size(), 3);
    EXPECT_FALSE(nullable->is_null_at(0));
    EXPECT_TRUE(nullable->is_null_at(1));
    EXPECT_TRUE(nullable->is_null_at(2));
    EXPECT_EQ(variant_text_at(*extracted_values, 0), "\"x\"");

    ColumnPtr types;
    ColumnsWithTypeAndName type_arguments {{source, source_type, "source"}};
    auto type_function = SimpleFunctionFactory::instance().get_function(
            "variant_type", type_arguments, make_nullable(std::make_shared<DataTypeString>()));
    ASSERT_NE(type_function, nullptr);
    Block block {type_arguments};
    const size_t result = block.columns();
    block.insert({nullptr, make_nullable(std::make_shared<DataTypeString>()), "result"});
    RuntimeState state;
    auto context = FunctionContext::create_context(&state, {}, {});
    ASSERT_TRUE(type_function->execute(context.get(), block, {0}, result, source->size()).ok());
    const auto& type_result =
            assert_cast<const ColumnNullable&>(*block.get_by_position(result).column);
    const auto& type_values = assert_cast<const ColumnString&>(type_result.get_nested_column());
    EXPECT_FALSE(type_result.is_null_at(0));
    EXPECT_FALSE(type_result.is_null_at(1));
    EXPECT_TRUE(type_result.is_null_at(2));
    EXPECT_EQ(type_values.get_data_at(0).to_string(), "object");
    EXPECT_EQ(type_values.get_data_at(1).to_string(), "null");
}

TEST(FunctionVariantRuntimeV2Test, ConstSourcesMaterializeThroughPublicFunctions) {
    auto variant_type = std::make_shared<DataTypeVariant>(37, true);
    auto physical = encoded_json({R"({"a":1})"});
    ColumnPtr source = ColumnConst::create(physical->get_ptr(), 3);
    auto path_data = ColumnString::create();
    path_data->insert_data("a", 1);
    ColumnPtr path = ColumnConst::create(std::move(path_data), source->size());

    ColumnPtr output;
    ColumnsWithTypeAndName element_arguments {{source, variant_type, "source"},
                                              {path, std::make_shared<DataTypeString>(), "path"}};
    ASSERT_TRUE(execute_function("element_at", std::move(element_arguments), {0, 1},
                                 make_nullable(variant_type), &output)
                        .ok());
    ColumnPtr materialized = output->convert_to_full_column_if_const();
    const auto& extracted = assert_cast<const ColumnNullable&>(*materialized);
    const auto& extracted_values =
            assert_cast<const ColumnVariantV2&>(extracted.get_nested_column());
    ASSERT_EQ(extracted_values.size(), 3);
    for (size_t row = 0; row < extracted_values.size(); ++row) {
        EXPECT_FALSE(extracted.is_null_at(row));
        EXPECT_EQ(variant_text_at(extracted_values, row), "1");
    }

    ColumnsWithTypeAndName type_arguments {{source, variant_type, "source"}};
    ASSERT_TRUE(execute_function("variant_type", std::move(type_arguments), {0},
                                 make_nullable(std::make_shared<DataTypeString>()), &output)
                        .ok());
    materialized = output->convert_to_full_column_if_const();
    const auto& types = assert_cast<const ColumnNullable&>(*materialized);
    const auto& type_values = assert_cast<const ColumnString&>(types.get_nested_column());
    ASSERT_EQ(type_values.size(), 3);
    for (size_t row = 0; row < type_values.size(); ++row) {
        EXPECT_FALSE(types.is_null_at(row));
        EXPECT_EQ(type_values.get_data_at(row).to_string(), "object");
    }
}

TEST(FunctionVariantRuntimeV2Test, NativeFunctionsArePubliclyRegistered) {
    auto variant_type = std::make_shared<DataTypeVariant>(37, true);
    auto source = encoded_json({R"({"a":[1,null]})"});
    auto path_values = ColumnString::create();
    path_values->insert_data("$.a[0]", 6);
    ColumnPtr path = ColumnConst::create(std::move(path_values), source->size());

    const auto lookup = [&](std::string_view name, ColumnsWithTypeAndName arguments,
                            DataTypePtr result_type) {
        return SimpleFunctionFactory::instance().get_function(std::string(name), arguments,
                                                              result_type);
    };
    EXPECT_NE(lookup("variant_get",
                     {{source->get_ptr(), variant_type, "source"},
                      {path, std::make_shared<DataTypeString>(), "path"}},
                     make_nullable(variant_type)),
              nullptr);
    EXPECT_NE(lookup("variant_exists_path",
                     {{source->get_ptr(), variant_type, "source"},
                      {path, std::make_shared<DataTypeString>(), "path"}},
                     make_nullable(std::make_shared<DataTypeUInt8>())),
              nullptr);
    EXPECT_NE(lookup("variant_is_null", {{source->get_ptr(), variant_type, "source"}},
                     make_nullable(std::make_shared<DataTypeUInt8>())),
              nullptr);
    EXPECT_NE(lookup("variant_keys", {{source->get_ptr(), variant_type, "source"}},
                     make_nullable(std::make_shared<DataTypeArray>(
                             make_nullable(std::make_shared<DataTypeString>())))),
              nullptr);
    EXPECT_NE(lookup("variant_length", {{source->get_ptr(), variant_type, "source"}},
                     make_nullable(std::make_shared<DataTypeInt32>())),
              nullptr);
    EXPECT_NE(lookup("variant_contains",
                     {{source->get_ptr(), variant_type, "source"},
                      {source->get_ptr(), variant_type, "candidate"}},
                     make_nullable(std::make_shared<DataTypeUInt8>())),
              nullptr);
}

TEST(FunctionVariantRuntimeV2Test, NativeFunctionsExecuteJsonPathConstAndNullableInputs) {
    auto variant_type = std::make_shared<DataTypeVariant>(37, true);
    auto source_values = encoded_json({R"({"a":[1,null],"obj":{"z":1,"a":2},"quoted.key":7})",
                                       R"({"a":[2]})", "null", R"({"ignored":1})"});
    auto source_nulls = ColumnUInt8::create();
    source_nulls->insert_value(0);
    source_nulls->insert_value(0);
    source_nulls->insert_value(0);
    source_nulls->insert_value(1);
    ColumnPtr source = ColumnNullable::create(std::move(source_values), std::move(source_nulls));
    auto source_type = make_nullable(variant_type);
    const size_t rows = source->size();
    const auto nullable_bool = make_nullable(std::make_shared<DataTypeUInt8>());

    ColumnPtr output;
    ASSERT_TRUE(execute_function("variant_get",
                                 {{source, source_type, "source"},
                                  {constant_string("$.a[0]", rows),
                                   std::make_shared<DataTypeString>(), "path"}},
                                 {0, 1}, make_nullable(variant_type), &output)
                        .ok());
    const auto& extracted = assert_cast<const ColumnNullable&>(*output);
    const auto& extracted_values =
            assert_cast<const ColumnVariantV2&>(extracted.get_nested_column());
    EXPECT_EQ(variant_text_at(extracted_values, 0), "1");
    EXPECT_EQ(variant_text_at(extracted_values, 1), "2");
    EXPECT_TRUE(extracted.is_null_at(2));
    EXPECT_TRUE(extracted.is_null_at(3));

    ASSERT_TRUE(execute_function("variant_get",
                                 {{source, source_type, "source"},
                                  {constant_string(R"($."quoted.key")", rows),
                                   std::make_shared<DataTypeString>(), "path"}},
                                 {0, 1}, make_nullable(variant_type), &output)
                        .ok());
    const auto& quoted = assert_cast<const ColumnNullable&>(*output);
    const auto& quoted_values = assert_cast<const ColumnVariantV2&>(quoted.get_nested_column());
    EXPECT_EQ(variant_text_at(quoted_values, 0), "7");
    EXPECT_TRUE(quoted.is_null_at(1));

    ASSERT_TRUE(execute_function(
                        "variant_get",
                        {{source, source_type, "source"},
                         {constant_string("$", rows), std::make_shared<DataTypeString>(), "path"}},
                        {0, 1}, make_nullable(variant_type), &output)
                        .ok());
    const auto& roots = assert_cast<const ColumnNullable&>(*output);
    const auto& root_values = assert_cast<const ColumnVariantV2&>(roots.get_nested_column());
    EXPECT_EQ(variant_text_at(root_values, 0),
              R"({"a":[1,null],"obj":{"a":2,"z":1},"quoted.key":7})");
    EXPECT_EQ(variant_text_at(root_values, 2), "null");
    EXPECT_TRUE(roots.is_null_at(3));

    ASSERT_TRUE(execute_function("variant_exists_path",
                                 {{source, source_type, "source"},
                                  {constant_string("$.a[0]", rows),
                                   std::make_shared<DataTypeString>(), "path"}},
                                 {0, 1}, nullable_bool, &output)
                        .ok());
    const auto& exists = assert_cast<const ColumnNullable&>(*output);
    const auto& exists_values = assert_cast<const ColumnUInt8&>(exists.get_nested_column());
    EXPECT_EQ(exists_values.get_data()[0], 1);
    EXPECT_EQ(exists_values.get_data()[1], 1);
    EXPECT_EQ(exists_values.get_data()[2], 0);
    EXPECT_TRUE(exists.is_null_at(3));

    ASSERT_TRUE(execute_function("variant_is_null", {{source, source_type, "source"}}, {0},
                                 nullable_bool, &output)
                        .ok());
    const auto& is_null = assert_cast<const ColumnNullable&>(*output);
    const auto& is_null_values = assert_cast<const ColumnUInt8&>(is_null.get_nested_column());
    EXPECT_EQ(is_null_values.get_data()[0], 0);
    EXPECT_EQ(is_null_values.get_data()[1], 0);
    EXPECT_EQ(is_null_values.get_data()[2], 1);
    EXPECT_TRUE(is_null.is_null_at(3));

    auto keys_type = make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>())));
    ASSERT_TRUE(execute_function("variant_keys", {{source, source_type, "source"}}, {0}, keys_type,
                                 &output)
                        .ok());
    const auto& keys = assert_cast<const ColumnNullable&>(*output);
    const auto& arrays = assert_cast<const ColumnArray&>(keys.get_nested_column());
    const auto& key_items = assert_cast<const ColumnNullable&>(arrays.get_data());
    const auto& key_strings = assert_cast<const ColumnString&>(key_items.get_nested_column());
    ASSERT_EQ(arrays.get_offsets()[0], 3);
    EXPECT_EQ(key_strings.get_data_at(0).to_string(), "a");
    EXPECT_EQ(key_strings.get_data_at(1).to_string(), "obj");
    EXPECT_EQ(key_strings.get_data_at(2).to_string(), "quoted.key");
    EXPECT_TRUE(keys.is_null_at(2));
    EXPECT_TRUE(keys.is_null_at(3));

    auto length_type = make_nullable(std::make_shared<DataTypeInt32>());
    ASSERT_TRUE(execute_function("variant_length", {{source, source_type, "source"}}, {0},
                                 length_type, &output)
                        .ok());
    const auto& lengths = assert_cast<const ColumnNullable&>(*output);
    const auto& length_values = assert_cast<const ColumnInt32&>(lengths.get_nested_column());
    EXPECT_EQ(length_values.get_data()[0], 3);
    EXPECT_EQ(length_values.get_data()[1], 1);
    EXPECT_EQ(length_values.get_data()[2], 1);
    EXPECT_TRUE(lengths.is_null_at(3));

    auto candidates = encoded_json({R"({"obj":{"a":2}})", R"({"a":[2]})", "null", "1"});
    ASSERT_TRUE(execute_function("variant_contains",
                                 {{source, source_type, "source"},
                                  {candidates->get_ptr(), variant_type, "candidate"}},
                                 {0, 1}, nullable_bool, &output)
                        .ok());
    const auto& contains = assert_cast<const ColumnNullable&>(*output);
    const auto& contains_values = assert_cast<const ColumnUInt8&>(contains.get_nested_column());
    EXPECT_EQ(contains_values.get_data()[0], 1);
    EXPECT_EQ(contains_values.get_data()[1], 1);
    EXPECT_EQ(contains_values.get_data()[2], 1);
    EXPECT_TRUE(contains.is_null_at(3));
}

TEST(FunctionVariantRuntimeV2Test, NativeJsonPathNullAndErrorsAreExplicit) {
    auto variant_type = std::make_shared<DataTypeVariant>(37, true);
    auto source = encoded_json({R"({"a":[1]})", R"({"a":[2]})"});
    const size_t rows = source->size();

    auto null_path_values = ColumnString::create();
    null_path_values->insert_default();
    auto null_path_nulls = ColumnUInt8::create();
    null_path_nulls->insert_value(1);
    ColumnPtr null_path = ColumnConst::create(
            ColumnNullable::create(std::move(null_path_values), std::move(null_path_nulls)), rows);
    ColumnPtr output;
    ASSERT_TRUE(execute_function(
                        "variant_get",
                        {{source->get_ptr(), variant_type, "source"},
                         {null_path, make_nullable(std::make_shared<DataTypeString>()), "path"}},
                        {0, 1}, make_nullable(variant_type), &output)
                        .ok());
    const auto& null_result = assert_cast<const ColumnNullable&>(*output);
    EXPECT_TRUE(null_result.is_null_at(0));
    EXPECT_TRUE(null_result.is_null_at(1));

    for (std::string_view invalid_path : {"a", "$.a[*]", "$.a[last]"}) {
        const Status status = execute_function(
                "variant_get",
                {{source->get_ptr(), variant_type, "source"},
                 {constant_string(invalid_path, rows), std::make_shared<DataTypeString>(), "path"}},
                {0, 1}, make_nullable(variant_type), &output);
        EXPECT_FALSE(status.ok()) << invalid_path;
        EXPECT_TRUE(status.to_string().find("JSON path") != std::string::npos)
                << status.to_string();
    }
}

} // namespace doris
