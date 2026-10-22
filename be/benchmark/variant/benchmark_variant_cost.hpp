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
#include <gen_cpp/data.pb.h>
#include <gen_cpp/segment_v2.pb.h>

#include <algorithm>
#include <array>
#include <numeric>
#include <string>
#include <vector>

#include "common/exception.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"
#include "exec/sort/hybrid_sorter.h"

namespace doris::variant_cost_benchmark {

// One deterministic high-entropy payload per fixture. Row ids prevent constant folding and
// exercise equal-prefix comparisons; source creation and result validation stay outside timing.
static std::string payload(size_t length) {
    std::string result(length, 'x');
    uint32_t random = 123456789;
    for (auto& byte : result) {
        random ^= random << 13;
        random ^= random >> 17;
        random ^= random << 5;
        byte = static_cast<char>('!' + random % 90);
    }
    return result;
}

// kind: 0 encoded integers, 1 repeated strings, 2 objects, 3 typed control, 4 unique strings.
static ColumnVariantV2::MutablePtr source(size_t rows, size_t bytes, int kind) {
    if (kind == 3) {
        auto integers = ColumnInt64::create(rows);
        std::iota(integers->get_data().begin(), integers->get_data().end(), Int64 {0});
        return ColumnVariantV2::create_typed(
                ColumnNullable::create(std::move(integers), ColumnUInt8::create(rows, 0)),
                std::make_shared<DataTypeInt64>());
    }
    std::string text = payload(bytes);
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = rows});
    for (size_t index = 0; index < rows; ++index) {
        auto row = builder.begin_row();
        if (kind == 0) {
            row.add_int(static_cast<int64_t>(index));
        } else if (kind == 1 || kind == 4) {
            if (kind == 4) {
                // Independent row payloads expose wire-size/copy costs hidden by repeated strings.
                auto random = static_cast<uint32_t>(index + 1);
                for (auto& byte : text) {
                    random ^= random << 13;
                    random ^= random >> 17;
                    random ^= random << 5;
                    byte = static_cast<char>('!' + random % 90);
                }
            }
            row.add_string(StringRef(text));
        } else {
            auto object = row.start_object();
            object.add_key(StringRef("id"));
            row.add_int(static_cast<int64_t>(index));
            object.add_key(StringRef("payload"));
            row.add_string(StringRef(text));
            object.finish();
        }
        row.finish();
    }
    auto batch = builder.finish_batch();
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(batch);
    return column;
}

enum class Operation {
    BUILD,
    APPEND_ROW,
    APPEND_BATCH,
    CLONE,
    FILTER_HALF,
    FILTER_SPARSE,
    FILTER_IN_PLACE,
    PERMUTE,
    COMPARE,
    SORT,
    HASH,
    CRC,
    SIZE,
    SERIALIZE,
    DESERIALIZE,
    ROW_SERIALIZE,
    JSON
};

// Keep the flat operation dispatcher together so every case shares identical timing boundaries.
// NOLINTNEXTLINE(readability-function-size)
static void run(benchmark::State& state, Operation operation) {
    const size_t rows = state.range(0);
    const size_t bytes = state.range(1);
    const int kind = static_cast<int>(state.range(2));
    const int version = static_cast<int>(state.range(3));
    const auto input = source(rows, bytes, kind);
    const DataTypeVariantV2 type;
    IColumn::Filter half(rows), sparse(rows);
    IColumn::Permutation reverse(rows);
    for (size_t row = 0; row < rows; ++row) {
        half[row] = row % 2 == 0;
        sparse[row] = row % 100 == 0;
        reverse[row] = rows - row - 1;
    }
    std::vector<uint64_t> hashes(rows);
    std::vector<uint32_t> crcs(rows);
    std::vector<char> wire(type.get_uncompressed_serialized_bytes(*input, version));
    const char* end = type.serialize(*input, wire.data(), version);
    DORIS_CHECK_LE(end, wire.data() + wire.size());
    const size_t wire_bytes = end - wire.data();
    MutableColumnPtr restored = type.create_column();
    DORIS_CHECK_EQ(type.deserialize(wire.data(), &restored, version), end);
    DORIS_CHECK_EQ(restored->size(), rows);
    DORIS_CHECK_EQ(restored->compare_at(rows - 1, rows - 1, *input, 1), 0);
    const size_t row_bytes = input->get_max_row_byte_size();
    std::vector<char> row_wire(row_bytes * rows);
    std::vector<StringRef> keys(rows);
    ColumnPtr last;
    for ([[maybe_unused]] auto _ : state) {
        switch (operation) {
        case Operation::BUILD:
            last = source(rows, bytes, kind);
            break;
        case Operation::APPEND_ROW: {
            auto output = ColumnVariantV2::create();
            for (size_t row = 0; row < rows; ++row) {
                output->insert_from(*input, row);
            }
            last = std::move(output);
            break;
        }
        case Operation::APPEND_BATCH: {
            auto output = ColumnVariantV2::create();
            output->insert_range_from(*input, 0, rows);
            last = std::move(output);
            break;
        }
        case Operation::CLONE:
            last = input->clone_resized(rows);
            break;
        case Operation::FILTER_HALF:
            last = input->filter(half, (rows + 1) / 2);
            break;
        case Operation::FILTER_SPARSE:
            last = input->filter(sparse, (rows + 99) / 100);
            break;
        case Operation::FILTER_IN_PLACE: {
            state.PauseTiming();
            auto output = input->clone_resized(rows);
            state.ResumeTiming();
            auto filtered = output->filter(half);
            benchmark::DoNotOptimize(filtered);
            state.PauseTiming();
            output.reset();
            state.ResumeTiming();
            break;
        }
        case Operation::PERMUTE:
            last = input->permute(reverse, rows);
            break;
        case Operation::COMPARE: {
            int result = 0;
            for (size_t row = 0; row < rows; ++row) {
                result += input->compare_at(row, rows - row - 1, *input, 1);
            }
            benchmark::DoNotOptimize(result);
            break;
        }
        case Operation::SORT: {
            HybridSorter sorter;
            IColumn::Permutation order;
            input->get_permutation(false, 0, 1, sorter, order);
            auto* order_data = order.data();
            benchmark::DoNotOptimize(order_data);
            break;
        }
        case Operation::HASH: {
            std::ranges::fill(hashes, 0);
            input->update_hashes_with_value(hashes.data(), nullptr);
            auto* hash_data = hashes.data();
            benchmark::DoNotOptimize(hash_data);
            break;
        }
        case Operation::CRC: {
            std::ranges::fill(crcs, 0);
            input->update_crc32c_batch(crcs.data(), nullptr);
            auto* crc_data = crcs.data();
            benchmark::DoNotOptimize(crc_data);
            break;
        }
        case Operation::SIZE: {
            auto size = type.get_uncompressed_serialized_bytes(*input, version);
            benchmark::DoNotOptimize(size);
            break;
        }
        case Operation::SERIALIZE: {
            auto* serialized_end = type.serialize(*input, wire.data(), version);
            benchmark::DoNotOptimize(serialized_end);
            benchmark::ClobberMemory();
            break;
        }
        case Operation::DESERIALIZE: {
            auto output = type.create_column();
            const auto* deserialized_end = type.deserialize(wire.data(), &output, version);
            benchmark::DoNotOptimize(deserialized_end);
            last = std::move(output);
            break;
        }
        case Operation::ROW_SERIALIZE: {
            for (size_t row = 0; row < rows; ++row) {
                keys[row] = StringRef(row_wire.data() + row * row_bytes, 0);
            }
            input->serialize(keys.data(), rows);
            auto* key_data = keys.data();
            benchmark::DoNotOptimize(key_data);
            benchmark::ClobberMemory();
            break;
        }
        case Operation::JSON:
            for (size_t row = 0; row < rows; ++row) {
                auto json = type.to_string(*input, row);
                benchmark::DoNotOptimize(json);
            }
            break;
        }
        benchmark::DoNotOptimize(last);
    }
    if (last) {
        size_t expected = rows;
        if (operation == Operation::FILTER_HALF) {
            expected = (rows + 1) / 2;
        } else if (operation == Operation::FILTER_SPARSE) {
            expected = (rows + 99) / 100;
        }
        DORIS_CHECK_EQ(last->size(), expected);
        const size_t source_row = operation == Operation::PERMUTE ? rows - 1 : 0;
        DORIS_CHECK_EQ(last->compare_at(0, source_row, *input, 1), 0);
    }
    state.counters["input_byte_size"] = static_cast<double>(input->byte_size());
    state.counters["input_allocated"] = static_cast<double>(input->allocated_bytes());
    state.counters["wire_bytes"] = static_cast<double>(wire_bytes);
    state.SetItemsProcessed(state.iterations() * rows);
}

static void arguments(benchmark::internal::Benchmark* benchmark) {
    for (const auto& shape : std::array<std::array<int64_t, 3>, 8> {{{4096, 0, 0},
                                                                     {4096, 8, 1},
                                                                     {4096, 128, 1},
                                                                     {4096, 1024, 2},
                                                                     {512, 16384, 2},
                                                                     {4096, 0, 3},
                                                                     {4096, 1024, 4},
                                                                     {512, 16384, 4}}}) {
        benchmark->Args({shape[0], shape[1], shape[2], 19});
    }
}

#define VARIANT_COST_BENCHMARK(NAME, OP)        \
    static void NAME(benchmark::State& state) { \
        run(state, Operation::OP);              \
    }                                           \
    BENCHMARK(NAME)->Apply(arguments)
VARIANT_COST_BENCHMARK(BM_VariantCostBuild, BUILD);
VARIANT_COST_BENCHMARK(BM_VariantCostAppendRow, APPEND_ROW);
VARIANT_COST_BENCHMARK(BM_VariantCostAppendBatch, APPEND_BATCH);
VARIANT_COST_BENCHMARK(BM_VariantCostClone, CLONE);
VARIANT_COST_BENCHMARK(BM_VariantCostFilterHalf, FILTER_HALF);
VARIANT_COST_BENCHMARK(BM_VariantCostFilterSparse, FILTER_SPARSE);
// Cloning is outside timing. Bound iterations so a fast descriptor-only filter does not
// make automatic calibration clone terabytes of payload just to time a few milliseconds.
VARIANT_COST_BENCHMARK(BM_VariantCostFilterInPlace, FILTER_IN_PLACE)->Iterations(50);
VARIANT_COST_BENCHMARK(BM_VariantCostPermute, PERMUTE);
VARIANT_COST_BENCHMARK(BM_VariantCostCompare, COMPARE);
VARIANT_COST_BENCHMARK(BM_VariantCostSort, SORT);
VARIANT_COST_BENCHMARK(BM_VariantCostHash, HASH);
VARIANT_COST_BENCHMARK(BM_VariantCostCRC, CRC);
VARIANT_COST_BENCHMARK(BM_VariantCostSize, SIZE);
VARIANT_COST_BENCHMARK(BM_VariantCostSerialize, SERIALIZE);
VARIANT_COST_BENCHMARK(BM_VariantCostDeserialize, DESERIALIZE);
VARIANT_COST_BENCHMARK(BM_VariantCostRowSerialize, ROW_SERIALIZE);
VARIANT_COST_BENCHMARK(BM_VariantCostJson, JSON);
#undef VARIANT_COST_BENCHMARK

// Compare containers in the same binary and process to separate layout/append costs from
// changes in unrelated function addresses, process placement, or the Variant codec.
template <typename Column>
static void raw(benchmark::State& state) {
    const size_t rows = state.range(0);
    const size_t bytes = state.range(1);
    const std::string text = payload(bytes);
    auto input = Column::create();
    for (size_t row = 0; row < rows; ++row) {
        input->insert_data(text.data(), bytes);
    }
    std::string data(rows * bytes, 'x');
    std::vector<uint32_t> offsets(rows + 1);
    for (size_t row = 0; row <= rows; ++row) {
        offsets[row] = static_cast<uint32_t>(row * bytes);
    }
    const int operation = static_cast<int>(state.range(2));
    IColumn::Filter half(rows);
    for (size_t row = 0; row < rows; ++row) {
        half[row] = row % 2 == 0;
    }
    ColumnPtr result;
    for ([[maybe_unused]] auto _ : state) {
        if (operation == 0) {
            auto output = Column::create();
            output->insert_many_continuous_binary_data(data.data(), offsets.data(), rows);
            result = std::move(output);
        } else if (operation == 1) {
            result = input->clone_resized(rows);
        } else if (operation == 2) {
            result = input->filter(half, rows / 2);
        } else {
            size_t total = 0;
            for (size_t row = 0; row < rows; ++row) {
                const auto value = input->get_data_at(row);
                total += value.size + static_cast<unsigned char>(value.data[0]);
            }
            benchmark::DoNotOptimize(total);
        }
        benchmark::DoNotOptimize(result);
    }
    if (result) {
        DORIS_CHECK_EQ(result->size(), operation == 2 ? rows / 2 : rows);
        DORIS_CHECK_EQ(result->get_data_at(0).size, bytes);
    }
    state.counters["input_allocated"] = static_cast<double>(input->allocated_bytes());
    state.counters["input_byte_size"] = static_cast<double>(input->byte_size());
    state.SetItemsProcessed(state.iterations() * rows);
}

static void raw_arguments(benchmark::internal::Benchmark* benchmark) {
    for (const int64_t bytes : {1, 9, 32, 128, 1024, 16384}) {
        for (const int64_t operation : {0, 1, 2, 3}) {
            benchmark->Args({4096, bytes, operation});
        }
    }
}
BENCHMARK_TEMPLATE(raw, ColumnString)->Name("BM_VariantRawString")->Apply(raw_arguments);
BENCHMARK_TEMPLATE(raw, ColumnVarbinary)->Name("BM_VariantRawBinary")->Apply(raw_arguments);

// Analytic/window-style reuse can erase consumed rows and append another block while the
// logical row count stays fixed. Report the final allocation as well as time, since an arena
// retains bytes of erased rows until clear(), even when its descriptor vector shrinks.
static void BM_VariantCostRetainAppend(benchmark::State& state) {
    const size_t rows = state.range(0);
    const size_t bytes = state.range(1);
    const size_t cycles = state.range(2);
    auto input = source(rows, bytes, 2);
    size_t allocated = 0;
    size_t logical_bytes = 0;
    for ([[maybe_unused]] auto _ : state) {
        auto output = input->clone_resized(rows);
        for (size_t cycle = 0; cycle < cycles; ++cycle) {
            output->erase(0, rows / 2);
            output->insert_range_from(*input, 0, rows / 2);
        }
        DORIS_CHECK_EQ(output->size(), rows);
        allocated = output->allocated_bytes();
        logical_bytes = output->byte_size();
        benchmark::DoNotOptimize(output);
    }
    state.counters["initial_allocated"] = static_cast<double>(input->allocated_bytes());
    state.counters["final_allocated"] = static_cast<double>(allocated);
    state.counters["final_byte_size"] = static_cast<double>(logical_bytes);
    state.SetItemsProcessed(state.iterations() * rows * cycles / 2);
}
BENCHMARK(BM_VariantCostRetainAppend)
        ->Args({4096, 1024, 1})
        ->Args({4096, 1024, 16})
        ->Args({4096, 1024, 64});

static void BM_VariantCostDefaults(benchmark::State& state) {
    const size_t rows = state.range(0);
    for ([[maybe_unused]] auto _ : state) {
        auto output = ColumnVariantV2::create();
        output->insert_many_defaults(rows);
        benchmark::DoNotOptimize(output);
    }
    state.SetItemsProcessed(state.iterations() * rows);
}
BENCHMARK(BM_VariantCostDefaults)->Arg(4096)->Arg(65536);

// DataTypeString compresses its own buffers, while Block compresses the complete payload again.
// Compare actual LZ4 Block transport, not just the intermediate datatype wire size.
static void block_transport(benchmark::State& state, bool deserialize) {
    const size_t rows = state.range(0);
    auto input = source(rows, state.range(1), static_cast<int>(state.range(2)));
    const int version = static_cast<int>(state.range(3));
    Block block {{input->get_ptr(), std::make_shared<DataTypeVariantV2>(), "v"}};
    PBlock serialized;
    size_t uncompressed = 0;
    size_t compressed = 0;
    int64_t codec_time = 0;
    auto encode = [&]() {
        serialized.Clear();
        THROW_IF_ERROR(block.serialize(version, &serialized, &uncompressed, &compressed,
                                       &codec_time, segment_v2::LZ4));
    };
    encode();
    Block restored;
    THROW_IF_ERROR(restored.deserialize(serialized, &uncompressed, &codec_time));
    DORIS_CHECK_EQ(restored.rows(), rows);
    DORIS_CHECK_EQ(restored.get_by_position(0).column->compare_at(0, 0, *input, 1), 0);
    for ([[maybe_unused]] auto _ : state) {
        if (deserialize) {
            Block decoded;
            THROW_IF_ERROR(decoded.deserialize(serialized, &uncompressed, &codec_time));
            benchmark::DoNotOptimize(decoded);
        } else {
            encode();
            benchmark::DoNotOptimize(serialized);
        }
    }
    state.counters["transport_bytes"] = static_cast<double>(serialized.ByteSizeLong());
    state.counters["compressed_body_bytes"] = static_cast<double>(compressed);
    state.SetItemsProcessed(state.iterations() * rows);
}
static void BM_VariantCostBlockSerialize(benchmark::State& state) {
    block_transport(state, false);
}
static void BM_VariantCostBlockDeserialize(benchmark::State& state) {
    block_transport(state, true);
}
BENCHMARK(BM_VariantCostBlockSerialize)->Apply(arguments);
BENCHMARK(BM_VariantCostBlockDeserialize)->Apply(arguments);

} // namespace doris::variant_cost_benchmark
