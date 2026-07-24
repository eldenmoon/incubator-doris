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

#include <gmock/gmock-actions.h>
#include <gmock/gmock-spec-builders.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/mock_vexpr.h"
#include "exprs/table_function/table_function_factory.h"
#include "exprs/table_function/variant_explode_utils.h"
#include "exprs/table_function/vexplode.h"
#include "exprs/table_function/vexplode_v2.h"
#include "runtime/runtime_state.h"
#include "util/jsonb_document.h"
#include "util/jsonb_utils.h"

namespace doris {
namespace {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::SetArgPointee;

MutableColumnPtr encoded_rows(const std::vector<std::string_view>& rows) {
    auto result = ColumnVariantV2::create();
    for (std::string_view row : rows) {
        JsonStringToVariantEncoder encoder;
        encoder.add_json({row.data(), row.size()});
        VariantBatchBuilder block = encoder.finish_batch();
        result->insert_encoded_batch(block);
    }
    return result;
}

ColumnPtr nullable_column(MutableColumnPtr nested, const std::vector<uint8_t>& nulls) {
    auto null_map = ColumnUInt8::create();
    for (uint8_t value : nulls) {
        null_map->insert_value(value);
    }
    return ColumnNullable::create(std::move(nested), std::move(null_map));
}

ColumnPtr typed_int_rows(const std::vector<int32_t>& values, const std::vector<uint8_t>& nulls) {
    auto nested = ColumnInt32::create();
    for (int32_t value : values) {
        nested->insert_value(value);
    }
    auto typed = ColumnVariantV2::create_typed(nullable_column(std::move(nested), nulls),
                                               std::make_shared<DataTypeInt32>());
    return typed->get_ptr();
}

DataTypePtr variant_type(bool nullable = false) {
    DataTypePtr type = std::make_shared<DataTypeVariant>();
    return nullable ? make_nullable(type) : type;
}

std::unique_ptr<Block> input_block(ColumnPtr column, bool nullable = false) {
    auto block = Block::create_unique();
    block->insert({std::move(column), variant_type(nullable), "v"});
    return block;
}

MutableColumnPtr variant_output() {
    return ColumnNullable::create(ColumnVariantV2::create(), ColumnUInt8::create());
}

MutableColumnPtr variant_pair_output() {
    MutableColumns fields;
    fields.emplace_back(variant_output());
    fields.emplace_back(variant_output());
    return ColumnStruct::create(std::move(fields));
}

MutableColumnPtr object_output() {
    MutableColumns fields;
    fields.emplace_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
    fields.emplace_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
    return ColumnNullable::create(ColumnStruct::create(std::move(fields)), ColumnUInt8::create());
}

std::string jsonb_text(const IColumn& column, size_t row) {
    const StringRef bytes = assert_cast<const ColumnString&>(column).get_data_at(row);
    const JsonbDocument* document = nullptr;
    const Status status = JsonbDocument::checkAndCreateDocument(bytes.data, bytes.size, &document);
    EXPECT_TRUE(status.ok()) << status;
    EXPECT_NE(document, nullptr);
    EXPECT_NE(document == nullptr ? nullptr : document->getValue(), nullptr);
    if (!status.ok() || document == nullptr || document->getValue() == nullptr) {
        return {};
    }
    JsonbToJson converter;
    return converter.to_json_string(document->getValue());
}

TFunction builtin_table_function(std::string name) {
    TFunction function;
    TFunctionName function_name;
    function_name.__set_function_name(name);
    function.__set_name(function_name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    return function;
}

class VariantTableFunctionTest : public testing::Test {
protected:
    void init_expr_context(size_t children) {
        _root = std::make_shared<MockVExpr>();
        _children.clear();
        for (size_t child = 0; child < children; ++child) {
            auto expression = std::make_shared<MockVExpr>();
            EXPECT_CALL(*expression, execute(_, _, _))
                    .WillRepeatedly(
                            DoAll(SetArgPointee<2>(static_cast<int>(child)), Return(Status::OK())));
            EXPECT_CALL(*expression, execute_column_impl(_, _, _, _, _))
                    .WillRepeatedly(Invoke([child](VExprContext*, const Block* block,
                                                   const Selector*, size_t, ColumnPtr& result) {
                        result = block->get_by_position(child).column;
                        return Status::OK();
                    }));
            _root->add_child(expression);
            _children.push_back(std::move(expression));
        }
        _context = std::make_shared<VExprContext>(_root);
    }

    void run(TableFunction& function, Block* block, MutableColumnPtr& output) {
        TQueryOptions options;
        TQueryGlobals globals;
        RuntimeState state(options, globals);
        ASSERT_TRUE(function.process_init(block, &state).ok());
        for (size_t row = 0; row < block->rows(); ++row) {
            function.process_row(row);
            if (function.current_empty() && !function.is_outer()) {
                function.forward();
                continue;
            }
            while (!function.eos()) {
                const size_t before = output->size();
                const int emitted = function.get_value(output, 64);
                ASSERT_GT(emitted, 0);
                ASSERT_EQ(output->size(), before + emitted);
            }
        }
        function.process_close();
    }

    VExprContextSPtr _context;
    std::shared_ptr<MockVExpr> _root;
    std::vector<std::shared_ptr<MockVExpr>> _children;
};

// GTest assertion macros expand into branches that inflate this linear helper's score.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
void expect_array_rows(const MutableColumnPtr& output, size_t expected_outer_nulls) {
    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    ASSERT_EQ(nullable.size(), 6);
    const auto& nulls = nullable.get_null_map_data();
    EXPECT_EQ(nulls[0], 0);
    EXPECT_EQ(nulls[1], 0);
    EXPECT_EQ(nulls[2], 0);
    size_t outer_nulls = 0;
    for (size_t row = 3; row < nulls.size(); ++row) {
        EXPECT_EQ(nulls[row], 1) << row;
    }
    for (uint8_t value : nulls) {
        outer_nulls += value != 0;
    }
    EXPECT_EQ(outer_nulls, expected_outer_nulls);

    const auto& values = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    VariantRef field;
    ASSERT_TRUE(values.get_value_ref(0).object_find(StringRef("a"), &field));
    EXPECT_EQ(field.get_int(), 1);
    EXPECT_TRUE(values.get_value_ref(1).is_null());
    ASSERT_TRUE(values.get_value_ref(2).object_find(StringRef("b"), &field));
    EXPECT_EQ(field.get_int(), 2);
    // The outer SQL-NULL placeholders add the canonical empty metadata to the two referenced
    // object-key dictionaries retained by the materializer.
    EXPECT_EQ(values.read_view().metadata_count(), 3);
}

void expect_materializer_keeps_only_referenced_metadata(const ColumnPtr& source) {
    ColumnPtr materialized;
    ASSERT_TRUE(variant_explode_internal::materialize_variant_array(source, &materialized).ok());
    const auto& array = assert_cast<const ColumnArray&>(*materialized);
    const auto& nullable = assert_cast<const ColumnNullable&>(array.get_data());
    const auto& values = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    EXPECT_EQ(values.read_view().metadata_count(), 2);
}

TEST_F(VariantTableFunctionTest, CurrentArrayRoutePreservesVariantNullMetadataAndOuterRows) {
    init_expr_context(1);
    auto source = encoded_rows({R"([{"a":1},null])", R"([{"b":2}])", "42", "[]", "[9]"});
    auto block = input_block(nullable_column(std::move(source), {0, 0, 0, 0, 1}), true);
    expect_materializer_keeps_only_referenced_metadata(block->get_by_position(0).column);

    VExplodeV2TableFunction function;
    function.set_expr_context(_context);
    function.set_nullable();
    function.set_outer();
    auto output = variant_output();
    run(function, block.get(), output);
    expect_array_rows(output, 3);
}

TEST_F(VariantTableFunctionTest, OldAbiArrayRouteUsesTheSameV2OnlySemantics) {
    init_expr_context(1);
    auto source = encoded_rows({R"([{"a":1},null])", R"([{"b":2}])", "42", "[]", "[9]"});
    auto block = input_block(nullable_column(std::move(source), {0, 0, 0, 0, 1}), true);

    VExplodeTableFunction function;
    function.set_expr_context(_context);
    function.set_nullable();
    function.set_outer();
    auto output = variant_output();
    run(function, block.get(), output);
    expect_array_rows(output, 3);
}

TEST_F(VariantTableFunctionTest, ConstAndTypedScalarRowsKeepOuterNullSeparate) {
    init_expr_context(1);
    auto one_array = encoded_rows({"[1,null]"});
    auto constant = ColumnConst::create(std::move(one_array), 3);
    auto block = input_block(std::move(constant));

    VExplodeV2TableFunction function;
    function.set_expr_context(_context);
    function.set_nullable();
    auto output = variant_output();
    run(function, block.get(), output);

    const auto& nullable = assert_cast<const ColumnNullable&>(*output);
    ASSERT_EQ(nullable.size(), 6);
    EXPECT_FALSE(nullable.has_null());
    const auto& values = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    for (size_t row = 0; row < values.size(); row += 2) {
        EXPECT_EQ(values.get_value_ref(row).get_int(), 1);
        EXPECT_TRUE(values.get_value_ref(row + 1).is_null());
    }

    init_expr_context(1);
    auto typed_block = input_block(typed_int_rows({7, 8}, {0, 1}));
    VExplodeV2TableFunction typed_function;
    typed_function.set_expr_context(_context);
    typed_function.set_nullable();
    typed_function.set_outer();
    auto typed_output = variant_output();
    run(typed_function, typed_block.get(), typed_output);
    const auto& typed_nullable = assert_cast<const ColumnNullable&>(*typed_output);
    ASSERT_EQ(typed_nullable.size(), 2);
    EXPECT_TRUE(typed_nullable.is_null_at(0));
    EXPECT_TRUE(typed_nullable.is_null_at(1));
}

TEST_F(VariantTableFunctionTest, MultiArrayMissingValueIsSqlNullButVariantNullIsNot) {
    init_expr_context(2);
    auto block = Block::create_unique();
    block->insert({encoded_rows({"[1,null]"}), variant_type(), "lhs"});
    block->insert({encoded_rows({"[2]"}), variant_type(), "rhs"});

    VExplodeV2TableFunction function;
    function.set_expr_context(_context);
    auto output = variant_pair_output();
    run(function, block.get(), output);

    const auto& structure = assert_cast<const ColumnStruct&>(*output);
    const auto& lhs = assert_cast<const ColumnNullable&>(structure.get_column(0));
    const auto& rhs = assert_cast<const ColumnNullable&>(structure.get_column(1));
    ASSERT_EQ(lhs.size(), 2);
    ASSERT_EQ(rhs.size(), 2);
    EXPECT_FALSE(lhs.is_null_at(0));
    EXPECT_FALSE(lhs.is_null_at(1));
    EXPECT_FALSE(rhs.is_null_at(0));
    EXPECT_TRUE(rhs.is_null_at(1));
    const auto& lhs_values = assert_cast<const ColumnVariantV2&>(lhs.get_nested_column());
    EXPECT_EQ(lhs_values.get_value_ref(0).get_int(), 1);
    EXPECT_TRUE(lhs_values.get_value_ref(1).is_null());
    const auto& rhs_values = assert_cast<const ColumnVariantV2&>(rhs.get_nested_column());
    EXPECT_EQ(rhs_values.get_value_ref(0).get_int(), 2);
}

TEST_F(VariantTableFunctionTest, SimpleFactoryResolvesVariantObjectSignatures) {
    DataTypePtr argument_type = make_nullable(std::make_shared<DataTypeVariant>(2048, false));
    ColumnsWithTypeAndName arguments {{nullptr, argument_type, "v"}};
    DataTypes fields {make_nullable(std::make_shared<DataTypeString>()),
                      make_nullable(std::make_shared<DataTypeJsonb>())};
    DataTypePtr return_type = make_nullable(std::make_shared<DataTypeStruct>(fields));

    auto base = SimpleFunctionFactory::instance().get_function("explode_variant_object", arguments,
                                                               return_type);
    auto outer = SimpleFunctionFactory::instance().get_function("explode_variant_object_outer",
                                                                arguments, return_type);

    EXPECT_NE(base, nullptr);
    EXPECT_NE(outer, nullptr);
    ASSERT_NE(base, nullptr);
    ASSERT_NE(outer, nullptr);
    EXPECT_TRUE(base->get_return_type()->equals(*return_type));
    EXPECT_TRUE(outer->get_return_type()->equals(*return_type));
}

// GTest assertion macros expand into branches that inflate this linear test's score.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST_F(VariantTableFunctionTest, VariantObjectReturnsNativeJsonbAndOuterRows) {
    init_expr_context(1);
    ObjectPool pool;
    TableFunction* function = nullptr;
    TFunction definition = builtin_table_function("explode_variant_object_outer");
    ASSERT_TRUE(TableFunctionFactory::get_fn(definition, &pool, &function,
                                             BeExecVersionManager::get_newest_version())
                        .ok());
    ASSERT_NE(function, nullptr);
    function->set_expr_context(_context);
    function->set_nullable();

    auto source = encoded_rows({R"({"b":null,"a":{"x":1}})", "42", "{}", R"({"z":9})"});
    auto block = input_block(nullable_column(std::move(source), {0, 0, 0, 1}), true);
    auto output = object_output();
    run(*function, block.get(), output);

    const auto& outer = assert_cast<const ColumnNullable&>(*output);
    ASSERT_EQ(outer.size(), 5);
    EXPECT_FALSE(outer.is_null_at(0));
    EXPECT_FALSE(outer.is_null_at(1));
    EXPECT_TRUE(outer.is_null_at(2));
    EXPECT_TRUE(outer.is_null_at(3));
    EXPECT_TRUE(outer.is_null_at(4));
    const auto& structure = assert_cast<const ColumnStruct&>(outer.get_nested_column());
    const auto& keys = assert_cast<const ColumnNullable&>(structure.get_column(0));
    const auto& values = assert_cast<const ColumnNullable&>(structure.get_column(1));
    EXPECT_EQ(assert_cast<const ColumnString&>(keys.get_nested_column()).get_data_at(0),
              StringRef("a"));
    EXPECT_EQ(assert_cast<const ColumnString&>(keys.get_nested_column()).get_data_at(1),
              StringRef("b"));
    EXPECT_FALSE(values.is_null_at(0));
    EXPECT_FALSE(values.is_null_at(1));
    EXPECT_EQ(jsonb_text(values.get_nested_column(), 0), R"({"x":1})");
    EXPECT_EQ(jsonb_text(values.get_nested_column(), 1), "null");

    auto constant_source = encoded_rows({R"({"c":null})"});
    auto constant_block = input_block(ColumnConst::create(std::move(constant_source), 3));
    auto constant_output = object_output();
    run(*function, constant_block.get(), constant_output);

    const auto& constant_outer = assert_cast<const ColumnNullable&>(*constant_output);
    ASSERT_EQ(constant_outer.size(), 3);
    EXPECT_FALSE(constant_outer.has_null());
    const auto& constant_struct =
            assert_cast<const ColumnStruct&>(constant_outer.get_nested_column());
    const auto& constant_keys = assert_cast<const ColumnNullable&>(constant_struct.get_column(0));
    const auto& constant_values = assert_cast<const ColumnNullable&>(constant_struct.get_column(1));
    for (size_t row = 0; row < constant_outer.size(); ++row) {
        EXPECT_EQ(assert_cast<const ColumnString&>(constant_keys.get_nested_column())
                          .get_data_at(row),
                  StringRef("c"));
        EXPECT_FALSE(constant_values.is_null_at(row));
        EXPECT_EQ(jsonb_text(constant_values.get_nested_column(), row), "null");
    }
}

TEST_F(VariantTableFunctionTest, FactoryKeepsBothExplodeAbiRoutes) {
    ObjectPool pool;
    TFunction definition = builtin_table_function("explode_variant_array");
    TableFunction* current = nullptr;
    ASSERT_TRUE(TableFunctionFactory::get_fn(definition, &pool, &current,
                                             BeExecVersionManager::get_newest_version())
                        .ok());
    ASSERT_NE(dynamic_cast<VExplodeV2TableFunction*>(current), nullptr);

    TableFunction* old_abi = nullptr;
    ASSERT_TRUE(TableFunctionFactory::get_fn(
                        definition, &pool, &old_abi,
                        TableFunctionFactory::NEWEST_VERSION_EXPLODE_MULTI_PARAM - 1)
                        .ok());
    ASSERT_NE(dynamic_cast<VExplodeTableFunction*>(old_abi), nullptr);
}

} // namespace
} // namespace doris
