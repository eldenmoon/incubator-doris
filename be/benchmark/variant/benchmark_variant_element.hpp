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

#pragma once

#include <benchmark/benchmark.h>

#include <array>
#include <cstring>
#include <string>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"

namespace doris::variant_element_benchmark {

enum class Shape { SCALAR, STRING, OBJECT, CHAIN, MISSING, ARRAY, TYPED_MISSING };

// Vary row values and build the input before timing. Payload is either an unselected sibling
// (SCALAR/MISSING), the selected string, or a member of the selected object (OBJECT/CHAIN).
static ColumnPtr make_input(Shape shape, size_t rows, size_t payload_size) {
    if (shape == Shape::TYPED_MISSING) {
        auto integers = ColumnInt64::create(rows);
        for (size_t index = 0; index < rows; ++index) {
            integers->get_data()[index] = static_cast<Int64>(index);
        }
        // Typed roots require a nullable physical leaf and a non-nullable scalar identity.
        return ColumnVariantV2::create_typed(
                ColumnNullable::create(std::move(integers), ColumnUInt8::create(rows, 0)),
                std::make_shared<DataTypeInt64>());
    }
    const std::string payload(payload_size, 'x');
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = rows});
    for (size_t index = 0; index < rows; ++index) {
        auto row = builder.begin_row();
        if (shape == Shape::ARRAY) {
            auto array = row.start_array();
            row.add_int(static_cast<int64_t>(index));
            row.add_string(StringRef(payload));
            array.finish();
        } else {
            auto root = row.start_object();
            root.add_key(StringRef("padding"));
            row.add_string(StringRef(payload));
            root.add_key(StringRef(shape == Shape::CHAIN ? "outer" : "target"));
            if (shape == Shape::STRING) {
                row.add_string(StringRef(payload));
            } else if (shape == Shape::OBJECT || shape == Shape::CHAIN) {
                auto nested = row.start_object();
                nested.add_key(StringRef("padding"));
                row.add_string(StringRef(payload));
                nested.add_key(StringRef("target"));
                row.add_int(static_cast<int64_t>(index));
                nested.finish();
            } else {
                row.add_int(static_cast<int64_t>(index));
            }
            root.finish();
        }
        row.finish();
    }
    auto batch = builder.finish_batch();
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(batch);
    return column;
}

static ColumnPtr string_key(const char* key, size_t rows) {
    auto column = ColumnString::create();
    column->insert_data(key, std::strlen(key));
    return ColumnConst::create(std::move(column), rows);
}

static void check_result(const ColumnPtr& result, Shape shape, size_t rows, size_t payload) {
    const auto& nullable = assert_cast<const ColumnNullable&>(*result);
    DORIS_CHECK_EQ(nullable.size(), rows);
    const auto& values = assert_cast<const ColumnVariantV2&>(nullable.get_nested_column());
    for (size_t index : std::array<size_t, 2> {0, rows - 1}) {
        const bool missing = shape == Shape::MISSING || shape == Shape::TYPED_MISSING;
        DORIS_CHECK_EQ(nullable.is_null_at(index), missing);
        if (missing) {
            continue;
        }
        auto value = values.get_value_ref(index);
        if (shape == Shape::STRING || shape == Shape::ARRAY) {
            DORIS_CHECK_EQ(value.get_string().size, payload);
            DORIS_CHECK_EQ(value.get_string().data[0], 'x');
        } else if (shape != Shape::OBJECT) {
            DORIS_CHECK_EQ(value.get_int(), static_cast<int64_t>(index));
        } else {
            DORIS_CHECK_GT(value.value.size, payload);
        }
    }
}

// Invoke the same registered IFunction as SQL element_at, including nullable handling, constant
// key materialization, path resolution, value lookup/copy, and result-column construction.
// CHAIN executes element_at(element_at(v, 'outer'), 'target'), including the intermediate object.
static void run(benchmark::State& state, Shape shape) {
    const size_t rows = state.range(0);
    const size_t payload = state.range(1);
    auto variant_type = make_nullable(std::make_shared<DataTypeVariant>());
    Block block;
    block.insert(
            {ColumnNullable::create(make_input(shape, rows, payload), ColumnUInt8::create(rows, 0)),
             variant_type, "source"});
    DataTypePtr key_type;
    if (shape == Shape::ARRAY) {
        key_type = std::make_shared<DataTypeInt64>();
        block.insert({ColumnConst::create(ColumnInt64::create(1, 2), rows), key_type, "key"});
    } else {
        key_type = std::make_shared<DataTypeString>();
        const char* key = "target";
        if (shape == Shape::CHAIN) {
            key = "outer";
        } else if (shape == Shape::MISSING || shape == Shape::TYPED_MISSING) {
            key = "absent";
        }
        block.insert({string_key(key, rows), key_type, "key"});
    }
    auto function = SimpleFunctionFactory::instance().get_function(
            "element_at", block.get_columns_with_type_and_name(), variant_type);
    DORIS_CHECK(function != nullptr);
    auto context = FunctionContext::create_context(nullptr, variant_type, {variant_type, key_type});
    THROW_IF_ERROR(
            function->open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
    THROW_IF_ERROR(
            function->open(context.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL));
    block.insert({nullptr, variant_type, "result"});
    const bool chain = shape == Shape::CHAIN;
    if (chain) {
        block.insert({string_key("target", rows), key_type, "leaf_key"});
        block.insert({nullptr, variant_type, "leaf_result"});
    }
    auto execute = [&]() {
        THROW_IF_ERROR(function->execute(context.get(), block, {0, 1}, 2, rows));
        if (chain) {
            THROW_IF_ERROR(function->execute(context.get(), block, {2, 3}, 4, rows));
        }
    };
    execute();
    const size_t result_index = chain ? 4 : 2;
    check_result(block.get_by_position(result_index).column, shape, rows, payload);
    for ([[maybe_unused]] auto _ : state) {
        execute();
        benchmark::DoNotOptimize(block.get_by_position(result_index).column);
        benchmark::ClobberMemory();
    }
    check_result(block.get_by_position(result_index).column, shape, rows, payload);
    THROW_IF_ERROR(
            function->close(context.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL));
    THROW_IF_ERROR(
            function->close(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL));
    state.SetItemsProcessed(state.iterations() * rows);
}

#define VARIANT_ELEMENT_BENCHMARK(NAME, SHAPE)  \
    static void NAME(benchmark::State& state) { \
        run(state, Shape::SHAPE);               \
    }                                           \
    BENCHMARK(NAME)

VARIANT_ELEMENT_BENCHMARK(BM_VariantElementScalar, SCALAR)
        ->Args({4096, 32})
        ->Args({4096, 1024})
        ->Args({4096, 16384})
        ->Args({64, 1048576});
VARIANT_ELEMENT_BENCHMARK(BM_VariantElementString, STRING)
        ->Args({4096, 8})
        ->Args({4096, 128})
        ->Args({4096, 1024})
        ->Args({4096, 16384})
        ->Args({64, 1048576});
VARIANT_ELEMENT_BENCHMARK(BM_VariantElementObject, OBJECT)
        ->Args({4096, 32})
        ->Args({4096, 1024})
        ->Args({4096, 16384})
        ->Args({64, 1048576});
VARIANT_ELEMENT_BENCHMARK(BM_VariantElementChain, CHAIN)
        ->Args({4096, 32})
        ->Args({4096, 1024})
        ->Args({4096, 16384})
        ->Args({64, 1048576});
VARIANT_ELEMENT_BENCHMARK(BM_VariantElementMissing, MISSING)
        ->Args({4096, 32})
        ->Args({4096, 1024})
        ->Args({4096, 16384});
VARIANT_ELEMENT_BENCHMARK(BM_VariantElementArray, ARRAY)
        ->Args({4096, 8})
        ->Args({4096, 1024})
        ->Args({4096, 16384})
        ->Args({64, 1048576});
VARIANT_ELEMENT_BENCHMARK(BM_VariantElementTypedMissing, TYPED_MISSING)
        ->Args({4096, 0})
        ->Args({65536, 0});
#undef VARIANT_ELEMENT_BENCHMARK

} // namespace doris::variant_element_benchmark
