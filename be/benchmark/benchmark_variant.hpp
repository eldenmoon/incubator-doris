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

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "common/exception.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/string_buffer.hpp"
#include "core/value/jsonb_value.h"
#include "exprs/function/cast/variant_v2/cast_variant_v2_internal.h"
#include "exprs/function/function_variant_element_v2.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"
#include "storage/segment/variant/variant_path_builder.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/function/parse/variant_jsonb_parse.h"

// Performance evidence from this file is valid only for a RELEASE benchmark binary collected on
// a relatively idle host. Use be/benchmark/run_variant_benchmark.sh: it enforces the RELEASE-only
// benchmark target, rejects a pre-run CPU-idle sample below 80% by default, and records load
// average plus CPU idle before and across the complete run. Timing values are observational
// baselines, never correctness thresholds.

namespace doris {
namespace {

constexpr uint32_t VARIANT_BENCHMARK_BLOCK_ROWS = 4096;
constexpr uint32_t VARIANT_BENCHMARK_JSONB_ROWS = 65'536;
constexpr uint32_t VARIANT_BENCHMARK_ARRAY_ROWS = 1024;
constexpr uint32_t VARIANT_BENCHMARK_ARRAY_ELEMENTS = 8;

struct VariantMb1Workload {
    std::string json;
    uint32_t rows;
};

std::array<VariantMb1Workload, 5> make_variant_mb1_workloads() {
    std::string narrow = "{";
    for (uint32_t index = 0; index < 8; ++index) {
        if (index != 0) {
            narrow.push_back(',');
        }
        narrow += "\"k" + std::to_string(index) + "\":" + std::to_string(index);
    }
    narrow.push_back('}');

    std::string wide = "{";
    for (uint32_t index = 0; index < 1000; ++index) {
        if (index != 0) {
            wide.push_back(',');
        }
        wide += "\"wide_" + std::to_string(index) + "\":" + std::to_string(index);
    }
    wide.push_back('}');

    std::string deep = "1";
    for (uint32_t depth = 0; depth < 8; ++depth) {
        deep = "{\"d" + std::to_string(depth) + "\":" + deep + "}";
    }

    std::string array_heavy = "[";
    for (uint32_t index = 0; index < 100; ++index) {
        if (index != 0) {
            array_heavy.push_back(',');
        }
        array_heavy += std::to_string(index);
    }
    array_heavy.push_back(']');

    return {{{.json = std::move(narrow), .rows = 20'000},
             {.json = std::move(wide), .rows = 1'000},
             {.json = std::move(deep), .rows = 16'000},
             {.json = std::move(array_heavy), .rows = 16'000},
             {.json = R"({"id":7,"ok":true,"s":"text","a":[1,null,{"x":2.5}]})", .rows = 12'536}}};
}

uint64_t variant_mb1_input_bytes(const std::array<VariantMb1Workload, 5>& workloads) {
    uint64_t result = 0;
    for (const VariantMb1Workload& workload : workloads) {
        result += workload.json.size() * workload.rows;
    }
    return result;
}

bool variant_benchmark_status(benchmark::State& state, const Status& status) {
    if (status.ok()) {
        return true;
    }
    const std::string message = status.to_string();
    state.SkipWithError(message.c_str());
    return false;
}

void set_variant_benchmark_work(benchmark::State& state, uint64_t rows, uint64_t bytes) {
    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(rows));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(bytes));
}

template <typename ColumnPointer>
void keep_variant_benchmark_column(const ColumnPointer& output) {
    const IColumn* output_pointer = output.get();
    size_t output_bytes = output->byte_size();
    benchmark::DoNotOptimize(output_pointer);
    benchmark::DoNotOptimize(output_bytes);
}

std::string jsonb_from_json(std::string_view json) {
    JsonBinaryValue value;
    const Status status = value.from_json_string(json.data(), json.size());
    if (!status.ok()) {
        throw Exception(status);
    }
    return {value.value(), value.size()};
}

std::string make_variant_jsonb_block_document() {
    std::string json = "{";
    for (uint8_t index = 0; index < 8; ++index) {
        if (index != 0) {
            json.push_back(',');
        }
        json += "\"key_" + std::to_string(index) + "\":" + std::to_string(index);
    }
    json += R"(,"array":[)";
    for (int32_t value = 0; value < 100; ++value) {
        if (value != 0) {
            json.push_back(',');
        }
        json += std::to_string(value);
    }
    json += "]}";
    return jsonb_from_json(json);
}

uint64_t run_variant_jsonb_per_row(StringRef document, uint32_t rows) {
    uint64_t bytes = 0;
    for (uint32_t row = 0; row < rows; ++row) {
        VariantBatchBuilder builder({.rows = 1,
                                     .metadata_keys = 9,
                                     .scalar_bytes = document.size,
                                     .nodes = 112,
                                     .containers = 2,
                                     .children = 109});
        auto active_row = builder.begin_row();
        jsonb_to_variant(document, active_row);
        active_row.finish();
        VariantBatchBuilder encoded = builder.finish_batch();
        bytes += encoded.value_bytes().size + encoded.metadata_ref().size;
    }
    return bytes;
}

uint64_t run_variant_jsonb_shared_block(StringRef document, uint32_t rows) {
    const size_t row_count = rows;
    JsonbToVariantEncoder encoder({.rows = rows,
                                   .metadata_keys = 9,
                                   .scalar_bytes = document.size * row_count,
                                   .nodes = 112 * row_count,
                                   .containers = 2 * row_count,
                                   .children = 109 * row_count});
    for (uint32_t row = 0; row < rows; ++row) {
        encoder.add_jsonb(document);
    }
    VariantBatchBuilder block = encoder.finish_batch();
    return block.value_bytes().size + block.metadata_ref().size;
}

const std::array<std::string_view, 4>& variant_root_documents() {
    static constexpr std::array<std::string_view, 4> DOCUMENTS {
            R"({"kind":"commit","commit":{"operation":"insert","collection":"issues"},"commits":[{"id":1,"message":"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz"},{"id":2,"message":"plain ascii payload"}]})",
            R"({"kind":"commit","commit":{"operation":"update","collection":"pulls"},"commits":[{"id":3,"message":"quote \" slash \\ newline\n"},{"id":4,"message":"more ascii text"}]})",
            R"({"kind":"commit","commit":{"operation":"delete","collection":"events"},"commits":[{"id":5,"message":"中文-é-данные"},{"id":6,"message":"safe ascii tail"}]})",
            R"({"kind":"snapshot","message":"row without the deep commit object","commits":[]})"};
    return DOCUMENTS;
}

VariantBatchBuilder make_variant_root_block(uint32_t rows = VARIANT_BENCHMARK_BLOCK_ROWS) {
    JsonStringToVariantEncoder encoder({.max_json_key_length = 1024,
                                  .throw_on_invalid_json = true,
                                  .check_duplicate_json_path = false});
    const auto& documents = variant_root_documents();
    for (uint32_t row = 0; row < rows; ++row) {
        const std::string_view json = documents[row % documents.size()];
        encoder.add_json({json.data(), json.size()});
    }
    return encoder.finish_batch();
}

ColumnVariantV2::MutablePtr make_variant_root_column(uint32_t rows = VARIANT_BENCHMARK_BLOCK_ROWS) {
    VariantBatchBuilder block = make_variant_root_block(rows);
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    return result;
}

ColumnVariantV2::MutablePtr make_variant_encoded_scalars(
        uint32_t rows = VARIANT_BENCHMARK_BLOCK_ROWS) {
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = rows});
    for (uint32_t row_index = 0; row_index < rows; ++row_index) {
        auto row = builder.begin_row();
        switch (row_index % 4) {
        case 0:
            row.add_string(StringRef("commit"));
            break;
        case 1:
            row.add_int(static_cast<int64_t>(row_index));
            break;
        case 2:
            row.add_double(static_cast<double>(row_index) + 0.25);
            break;
        default:
            row.add_date(static_cast<int32_t>(row_index));
            break;
        }
        row.finish();
    }
    VariantBatchBuilder block = builder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    return result;
}

VariantBatchBuilder make_variant_integer_block(uint32_t rows = VARIANT_BENCHMARK_BLOCK_ROWS) {
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = rows});
    for (uint32_t row_index = 0; row_index < rows; ++row_index) {
        auto row = builder.begin_row();
        row.add_int(static_cast<int64_t>(row_index));
        row.finish();
    }
    return builder.finish_batch();
}

ColumnVariantV2::MutablePtr make_variant_typed_strings(
        uint32_t rows = VARIANT_BENCHMARK_BLOCK_ROWS) {
    auto strings = ColumnString::create();
    strings->reserve(rows);
    for (uint32_t row = 0; row < rows; ++row) {
        const std::string_view value = row % 2 == 0 ? "commit" : "create";
        strings->insert_data(value.data(), value.size());
    }
    auto nulls = ColumnUInt8::create(rows, 0);
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(strings), std::move(nulls)),
            std::make_shared<DataTypeString>());
}

struct VariantJsonbCastInput {
    ColumnPtr flat;
    ColumnPtr array;
    DataTypePtr array_type;
    size_t leaves = 0;
    size_t input_bytes = 0;
};

VariantJsonbCastInput make_variant_jsonb_cast_input() {
    const std::string document = jsonb_from_json(
            R"({"sha":"0123456789abcdef","author":{"id":7,"name":"alice"},"message":"commit payload","files":[1,2,3,4]})");
    const size_t leaves = VARIANT_BENCHMARK_ARRAY_ROWS * VARIANT_BENCHMARK_ARRAY_ELEMENTS;

    auto flat = ColumnString::create();
    auto array_values = ColumnString::create();
    flat->reserve(leaves);
    array_values->reserve(leaves);
    for (size_t row = 0; row < leaves; ++row) {
        flat->insert_data(document.data(), document.size());
        array_values->insert_data(document.data(), document.size());
    }

    auto element_nulls = ColumnUInt8::create(leaves, 0);
    auto elements = ColumnNullable::create(std::move(array_values), std::move(element_nulls));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->reserve(VARIANT_BENCHMARK_ARRAY_ROWS);
    for (uint32_t row = 1; row <= VARIANT_BENCHMARK_ARRAY_ROWS; ++row) {
        offsets->insert_value(
                static_cast<ColumnArray::Offset64>(row * VARIANT_BENCHMARK_ARRAY_ELEMENTS));
    }

    return {.flat = flat->get_ptr(),
            .array = ColumnArray::create(std::move(elements), std::move(offsets)),
            .array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeJsonb>()),
            .leaves = leaves,
            .input_bytes = document.size() * leaves};
}

void BM_VariantJson_ParseToV2_Mixed(benchmark::State& state) {
    constexpr uint32_t BLOCK_SIZE = 128;
    const auto workloads = make_variant_mb1_workloads();
    const uint64_t input_bytes = variant_mb1_input_bytes(workloads);
    uint64_t checksum = 0;
    for (auto _ : state) {
        checksum = 0;
        for (const VariantMb1Workload& workload : workloads) {
            for (uint32_t begin = 0; begin < workload.rows; begin += BLOCK_SIZE) {
                const uint32_t count = std::min(BLOCK_SIZE, workload.rows - begin);
                JsonStringToVariantEncoder encoder({.max_json_key_length = 1024,
                                              .throw_on_invalid_json = true,
                                              .check_duplicate_json_path = false});
                for (uint32_t row = 0; row < count; ++row) {
                    encoder.add_json(StringRef(workload.json));
                }
                VariantBatchBuilder block = encoder.finish_batch();
                checksum += block.value_bytes().size + block.metadata_ref().size;
            }
        }
        benchmark::DoNotOptimize(checksum);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, VARIANT_BENCHMARK_JSONB_ROWS, input_bytes);
}

void BM_VariantJson_ParseToJsonb_Mixed(benchmark::State& state) {
    constexpr uint32_t BLOCK_SIZE = 128;
    const auto workloads = make_variant_mb1_workloads();
    const uint64_t input_bytes = variant_mb1_input_bytes(workloads);
    uint64_t checksum = 0;
    for (auto _ : state) {
        checksum = 0;
        for (const VariantMb1Workload& workload : workloads) {
            for (uint32_t begin = 0; begin < workload.rows; begin += BLOCK_SIZE) {
                const uint32_t count = std::min(BLOCK_SIZE, workload.rows - begin);
                JsonBinaryValue jsonb;
                for (uint32_t row = 0; row < count; ++row) {
                    if (!variant_benchmark_status(state, jsonb.from_json_string(workload.json))) {
                        return;
                    }
                    checksum += jsonb.size();
                }
            }
        }
        benchmark::DoNotOptimize(checksum);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, VARIANT_BENCHMARK_JSONB_ROWS, input_bytes);
}

void BM_VariantJsonbToV2_PerRow(benchmark::State& state) {
    const std::string document = make_variant_jsonb_block_document();
    uint64_t checksum = 0;
    for (auto _ : state) {
        checksum = run_variant_jsonb_per_row(StringRef(document), VARIANT_BENCHMARK_JSONB_ROWS);
        benchmark::DoNotOptimize(checksum);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, VARIANT_BENCHMARK_JSONB_ROWS,
                               document.size() * VARIANT_BENCHMARK_JSONB_ROWS);
}

void BM_VariantJsonbToV2_SharedBlock(benchmark::State& state) {
    const std::string document = make_variant_jsonb_block_document();
    uint64_t checksum = 0;
    for (auto _ : state) {
        checksum =
                run_variant_jsonb_shared_block(StringRef(document), VARIANT_BENCHMARK_JSONB_ROWS);
        benchmark::DoNotOptimize(checksum);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, VARIANT_BENCHMARK_JSONB_ROWS,
                               document.size() * VARIANT_BENCHMARK_JSONB_ROWS);
}

void BM_VariantV2_InsertEncodedBlock(benchmark::State& state) {
    const VariantBatchBuilder block = make_variant_root_block();
    const uint64_t input_bytes = block.value_bytes().size + block.metadata_ref().size;
    for (auto _ : state) {
        auto result = ColumnVariantV2::create();
        result->insert_encoded_batch(block);
        keep_variant_benchmark_column(result);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, block.num_rows(), input_bytes);
}

void BM_VariantV2_RootSerdeRow(benchmark::State& state) {
    const auto source = make_variant_root_column();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions options;
    for (auto _ : state) {
        auto output = ColumnString::create();
        BufferWritable writer(*output);
        for (size_t row = 0; row < source->size(); ++row) {
            serde.to_string(*source, row, writer, options);
            writer.commit();
        }
        keep_variant_benchmark_column(output);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, source->size(), source->byte_size());
}

void BM_VariantV2_RootSerdeBatch(benchmark::State& state) {
    const auto source = make_variant_root_column();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions options;
    for (auto _ : state) {
        auto output = ColumnString::create();
        serde.to_string_batch(*source, *output, options);
        keep_variant_benchmark_column(output);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, source->size(), source->byte_size());
}

void BM_VariantV2_EncodedRootCastToString(benchmark::State& state) {
    const auto source = make_variant_root_column();
    RuntimeState runtime_state;
    runtime_state.set_timezone("UTC");
    auto context = FunctionContext::create_context(&runtime_state, {}, {});
    for (auto _ : state) {
        ColumnPtr output;
        const Status status = CastWrapper::variant_v2_internal::cast_variant_to_string(
                context.get(), *source, source->size(), {}, &output);
        if (!variant_benchmark_status(state, status)) {
            return;
        }
        keep_variant_benchmark_column(output);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, source->size(), source->byte_size());
}

void run_variant_path_benchmark(benchmark::State& state,
                                std::span<const VariantElementV2PathSegment> segments) {
    const auto source = make_variant_root_column();
    std::unique_ptr<ResolvedVariantElementV2Path> path;
    if (!variant_benchmark_status(state, resolve_variant_element_v2_path(segments, &path))) {
        return;
    }
    for (auto _ : state) {
        ColumnPtr output;
        const Status status = extract_variant_element_v2(*source, *path, {}, &output);
        if (!variant_benchmark_status(state, status)) {
            return;
        }
        keep_variant_benchmark_column(output);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, source->size(), source->byte_size());
}

void BM_VariantV2_ExtractPath_Shallow(benchmark::State& state) {
    const std::array segments {VariantElementV2PathSegment::object_key(StringRef("kind"))};
    run_variant_path_benchmark(state, segments);
}

void BM_VariantV2_ExtractPath_Deep(benchmark::State& state) {
    const std::array segments {VariantElementV2PathSegment::object_key(StringRef("commit")),
                               VariantElementV2PathSegment::object_key(StringRef("operation"))};
    run_variant_path_benchmark(state, segments);
}

void BM_VariantV2_TypedPathCastToString(benchmark::State& state) {
    const auto source = make_variant_typed_strings();
    RuntimeState runtime_state;
    runtime_state.set_timezone("UTC");
    auto context = FunctionContext::create_context(&runtime_state, {}, {});
    for (auto _ : state) {
        ColumnPtr output;
        const Status status = CastWrapper::variant_v2_internal::cast_variant_to_string(
                context.get(), *source, source->size(), {}, &output);
        if (!variant_benchmark_status(state, status)) {
            return;
        }
        keep_variant_benchmark_column(output);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, source->size(), source->byte_size());
}

void BM_VariantV2_EncodedScalarCastToString(benchmark::State& state) {
    const auto source = make_variant_encoded_scalars();
    RuntimeState runtime_state;
    runtime_state.set_timezone("UTC");
    auto context = FunctionContext::create_context(&runtime_state, {}, {});
    for (auto _ : state) {
        ColumnPtr output;
        const Status status = CastWrapper::variant_v2_internal::cast_variant_to_string(
                context.get(), *source, source->size(), {}, &output);
        if (!variant_benchmark_status(state, status)) {
            return;
        }
        keep_variant_benchmark_column(output);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, source->size(), source->byte_size());
}

void BM_VariantV2_FlatJsonbCastToVariant(benchmark::State& state) {
    const VariantJsonbCastInput input = make_variant_jsonb_cast_input();
    for (auto _ : state) {
        ColumnPtr output;
        const Status status = CastWrapper::variant_v2_internal::cast_jsonb_to_variant(
                input.flat, input.leaves, {}, &output);
        if (!variant_benchmark_status(state, status)) {
            return;
        }
        keep_variant_benchmark_column(output);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, input.leaves, input.input_bytes);
}

void BM_VariantV2_ArrayJsonbCastToVariant(benchmark::State& state) {
    const VariantJsonbCastInput input = make_variant_jsonb_cast_input();
    for (auto _ : state) {
        ColumnPtr output;
        const Status status = CastWrapper::variant_v2_internal::cast_array_to_variant(
                input.array, input.array_type, VARIANT_BENCHMARK_ARRAY_ROWS, {}, &output);
        if (!variant_benchmark_status(state, status)) {
            return;
        }
        keep_variant_benchmark_column(output);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, VARIANT_BENCHMARK_ARRAY_ROWS, input.input_bytes);
    state.counters["leaf_rows_per_second"] = benchmark::Counter(
            static_cast<double>(input.leaves), benchmark::Counter::kIsIterationInvariantRate);
}

void BM_VariantV2_ImportPathBuilderHomogeneousInt(benchmark::State& state) {
    const VariantBatchBuilder block = make_variant_integer_block();
    const uint64_t input_bytes = block.value_bytes().size + block.metadata_ref().size;
    for (auto _ : state) {
        segment_v2::VariantPathBuilder builder(PathInData("value"));
        for (size_t row = 0; row < block.num_rows(); ++row) {
            if (!variant_benchmark_status(state, builder.append(block.value_at(row), row))) {
                return;
            }
        }
        ColumnPtr output = builder.column();
        keep_variant_benchmark_column(output);
        benchmark::ClobberMemory();
    }
    set_variant_benchmark_work(state, block.num_rows(), input_bytes);
}

#define DORIS_VARIANT_BENCHMARK(function) \
    BENCHMARK(function)->Unit(benchmark::kMicrosecond)->UseRealTime()

DORIS_VARIANT_BENCHMARK(BM_VariantJson_ParseToV2_Mixed);
DORIS_VARIANT_BENCHMARK(BM_VariantJson_ParseToJsonb_Mixed);
DORIS_VARIANT_BENCHMARK(BM_VariantJsonbToV2_PerRow);
DORIS_VARIANT_BENCHMARK(BM_VariantJsonbToV2_SharedBlock);
DORIS_VARIANT_BENCHMARK(BM_VariantV2_InsertEncodedBlock);
DORIS_VARIANT_BENCHMARK(BM_VariantV2_RootSerdeRow);
DORIS_VARIANT_BENCHMARK(BM_VariantV2_RootSerdeBatch);
DORIS_VARIANT_BENCHMARK(BM_VariantV2_EncodedRootCastToString);
DORIS_VARIANT_BENCHMARK(BM_VariantV2_ExtractPath_Shallow);
DORIS_VARIANT_BENCHMARK(BM_VariantV2_ExtractPath_Deep);
DORIS_VARIANT_BENCHMARK(BM_VariantV2_TypedPathCastToString);
DORIS_VARIANT_BENCHMARK(BM_VariantV2_EncodedScalarCastToString);
DORIS_VARIANT_BENCHMARK(BM_VariantV2_FlatJsonbCastToVariant);
DORIS_VARIANT_BENCHMARK(BM_VariantV2_ArrayJsonbCastToVariant);
DORIS_VARIANT_BENCHMARK(BM_VariantV2_ImportPathBuilderHomogeneousInt);

#undef DORIS_VARIANT_BENCHMARK

} // namespace
} // namespace doris
