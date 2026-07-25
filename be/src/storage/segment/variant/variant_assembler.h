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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "util/json/path_in_data.h"

namespace doris {

class ColumnMap;

namespace segment_v2 {

enum class VariantAssemblerMode : uint8_t {
    HIERARCHICAL,
    ROOT_FLAT,
    BINARY_EXTRACT,
    DEFAULT_FILL,
};

struct VariantAssemblerMaterializedPath {
    PathInData path;
    DataTypePtr type;
};

struct VariantAssemblerPlanOptions {
    VariantAssemblerMode mode = VariantAssemblerMode::HIERARCHICAL;
    PathInData requested_path;
    DorisVector<VariantAssemblerMaterializedPath> materialized_paths;
    size_t sparse_bucket_count = 0;
    bool has_root = false;
    bool has_doc = false;
    // Persisted hierarchical root objects are a legacy sidecar: materialized/sparse values define
    // the visible object. Direct assembler callers may opt into overlaying the sidecar instead.
    bool merge_root_with_subcolumns = true;
    // For an unknown subtree fallback, distinguish a row with no exact/descendant physical value
    // from a known object subtree whose children are merely absent.
    bool null_on_no_match = false;
};

// Immutable cold-path plan. Paths and physical types are validated and retained so one plan can
// assemble many independently owned batches.
class VariantAssemblerPlan final {
public:
    ~VariantAssemblerPlan();

    VariantAssemblerPlan(const VariantAssemblerPlan&) = delete;
    VariantAssemblerPlan& operator=(const VariantAssemblerPlan&) = delete;

    static Status create(VariantAssemblerPlanOptions options,
                         std::shared_ptr<const VariantAssemblerPlan>* output);

private:
    friend class VariantAssembler;
    struct Impl;

    explicit VariantAssemblerPlan(std::unique_ptr<Impl> impl);
    std::unique_ptr<Impl> _impl;
};

// Borrowed storage batch. Materialized columns match the plan order. Root is String/JSONB,
// optionally Nullable; doc/sparse inputs are Map<String,String> whose values are storage cells.
// BINARY_EXTRACT consumes one storage cell per row from binary_values; binary_missing distinguishes
// path absence from a malformed empty cell.
struct VariantAssemblerBatchView {
    size_t num_rows = 0;
    std::span<const uint8_t> outer_nulls;
    const IColumn* root_jsonb = nullptr;
    std::span<const IColumn* const> materialized_columns;
    std::span<const ColumnMap* const> sparse_buckets;
    const ColumnMap* doc_values = nullptr;
    const ColumnString* binary_values = nullptr;
    std::span<const uint8_t> binary_missing;
};

struct VariantAssembledColumn {
    ColumnVariantV2::MutablePtr values;
    MutableColumnPtr outer_nulls;
};

// A successful assemble is reusable. The first error is terminal and retained; result publication
// is atomic, so a failed call never changes the caller-owned output.
class VariantAssembler final {
public:
    explicit VariantAssembler(std::shared_ptr<const VariantAssemblerPlan> plan);
    ~VariantAssembler();

    VariantAssembler(VariantAssembler&&) noexcept;
    VariantAssembler& operator=(VariantAssembler&&) noexcept;
    VariantAssembler(const VariantAssembler&) = delete;
    VariantAssembler& operator=(const VariantAssembler&) = delete;

    Status assemble(const VariantAssemblerBatchView& batch, VariantAssembledColumn* output);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace segment_v2
} // namespace doris
