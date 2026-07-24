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
#include "core/column/column_string.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "core/value/variant/variant_value.h"
#include "util/json/path_in_data.h"

namespace doris::segment_v2 {

struct VariantPathColumn {
    PathInData path;
    DataTypePtr type;
    ColumnPtr column;
    uint32_t non_null_rows = 0;
    bool is_typed_path = false;
};

// A row-aligned, incrementally promotable builder for one flattened Variant path. Missing rows are
// materialized in batches, while present leaves are appended directly from VariantRef without
// a Field intermediate. The builder owns every byte after append returns.
class VariantPathBuilder final {
public:
    explicit VariantPathBuilder(PathInData path, size_t prefix_rows = 0);
    ~VariantPathBuilder();

    VariantPathBuilder(VariantPathBuilder&&) noexcept;
    VariantPathBuilder& operator=(VariantPathBuilder&&) noexcept;
    VariantPathBuilder(const VariantPathBuilder&) = delete;
    VariantPathBuilder& operator=(const VariantPathBuilder&) = delete;

    Status append(VariantRef value, size_t row);
    Status complete_rows(size_t rows);
    Status convert_to(const DataTypePtr& storage_type);

    const PathInData& path() const;
    const DataTypePtr& type() const;
    ColumnPtr column() const;
    uint32_t non_null_rows() const;
    size_t rows() const;
    size_t promotion_count() const;
    bool is_null_at(size_t row) const;

    Status write_sparse_cell(size_t row, ColumnString::Chars* chars) const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

struct VariantPathSelectionCandidate {
    const VariantPathBuilder* builder = nullptr;
    bool is_typed_path = false;
};

struct VariantPathSelection {
    DorisVector<size_t> materialized;
    DorisVector<size_t> sparse;
};

// Typed paths are fixed unless typed_paths_to_sparse is enabled. Dynamic paths are ordered by the
// authoritative non-null-count/depth/path rule before the materialization budget is applied.
VariantPathSelection select_variant_paths(std::span<const VariantPathSelectionCandidate> candidates,
                                          size_t max_dynamic_materialized_paths,
                                          bool typed_paths_to_sparse);

// Compiles metadata dictionary ids and parent+field transitions once. Callers resolve row data
// through stable numeric ids; only the first occurrence of a unique metadata/path pair constructs
// or hashes a path string.
class VariantMetadataPathPlan final {
public:
    VariantMetadataPathPlan();
    ~VariantMetadataPathPlan();

    VariantMetadataPathPlan(VariantMetadataPathPlan&&) noexcept;
    VariantMetadataPathPlan& operator=(VariantMetadataPathPlan&&) noexcept;
    VariantMetadataPathPlan(const VariantMetadataPathPlan&) = delete;
    VariantMetadataPathPlan& operator=(const VariantMetadataPathPlan&) = delete;

    Status intern_metadata(VariantMetadataRef metadata, uint32_t* plan_id);
    Status resolve_child(uint32_t parent_path_id, uint32_t metadata_plan_id, uint32_t field_id,
                         uint32_t* child_path_id);

    const PathInData& path(uint32_t path_id) const;
    size_t metadata_plan_count() const;
    size_t path_plan_count() const;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

} // namespace doris::segment_v2
