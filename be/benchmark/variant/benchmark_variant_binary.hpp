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

#include <string>

#include "common/exception.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/value/variant/variant_batch_builder.h"

namespace doris {

// Exercise the encoded payload retained by a hash-join build side. Construct the source outside
// timing and keep one source row per payload size, so append measures destination storage only.
static void BM_VariantEncodedAppend(benchmark::State& state) {
    const size_t payload_size = state.range(0);
    const size_t rows = state.range(1);
    const std::string payload(payload_size, 'x');
    VariantBatchBuilder builder;
    auto row = builder.begin_row();
    row.add_string(StringRef(payload));
    row.finish();
    auto batch = builder.finish_batch();
    auto source = ColumnVariantV2::create();
    source->insert_encoded_batch(batch);
    size_t allocated = 0;
    try {
        for (auto _ : state) {
            auto destination = ColumnVariantV2::create();
            for (size_t index = 0; index < rows; ++index) {
                destination->insert_range_from(*source, 0, 1);
            }
            allocated = destination->allocated_bytes();
            auto last_value = destination->get_value_ref(rows - 1).value;
            benchmark::DoNotOptimize(last_value);
            if (destination->size() != rows ||
                destination->get_value_ref(rows - 1).get_string().size != payload_size) {
                state.SkipWithError("Variant append produced invalid rows");
                break;
            }
        }
    } catch (const Exception& error) {
        state.SkipWithError(error.what());
    }
    state.counters["allocated_bytes"] = static_cast<double>(allocated);
    state.SetBytesProcessed(state.iterations() * payload_size * rows);
}

static void BM_VariantEncodedBatch(benchmark::State& state) {
    const size_t payload_size = state.range(0);
    const size_t rows = state.range(1);
    const std::string payload(payload_size, 'x');
    for (auto _ : state) {
        VariantBatchBuilder builder;
        for (size_t index = 0; index < rows; ++index) {
            auto row = builder.begin_row();
            row.add_string(StringRef(payload));
            row.finish();
        }
        auto batch = builder.finish_batch();
        auto destination = ColumnVariantV2::create();
        destination->insert_encoded_batch(batch);
        auto last_value = destination->get_value_ref(rows - 1).value;
        benchmark::DoNotOptimize(last_value);
    }
    state.SetBytesProcessed(state.iterations() * payload_size * rows);
}

BENCHMARK(BM_VariantEncodedBatch)
        ->Args({128, 524288})
        ->Args({1024, 65536})
        ->Args({65536, 1024})
        ->Args({1048576, 64});

BENCHMARK(BM_VariantEncodedAppend)
        ->Args({128, 524288})
        ->Args({1024, 65536})
        ->Args({65536, 1024})
        ->Args({1048576, 64});
BENCHMARK(BM_VariantEncodedAppend)
        ->Name("BM_VariantEncodedCapacity")
        ->Args({1048576, 4096})
        ->Iterations(1);

} // namespace doris
