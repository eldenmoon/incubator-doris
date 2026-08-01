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
#include "core/column/column_nullable.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type.h"
#include "util/json/path_in_data.h"

namespace doris {

class ColumnMap;

namespace segment_v2::variant_v2 {

// Describes the physical streams available to one hierarchical reader. Paths are normalized once
// by create().
struct VariantAssemblerOptions {
    struct MaterializedPath {
        PathInData path;
        DataTypePtr type;
    };

    PathInData requested_path;
    DorisVector<MaterializedPath> materialized_paths;
    bool has_sparse = false;
    bool has_root = false;
    bool has_doc = false;
};

// Borrowed storage batch. All spans and pointers only need to remain valid until assemble()
// returns; the assembler retains none of them. Hierarchical materialized columns stay in the same
// order as the paths supplied to create(); root_jsonb may be nullable and sparse/doc use persisted
// Map<String,String>.
struct VariantAssemblerBatchView {
    size_t num_rows = 0;
    std::span<const uint8_t> outer_nulls;
    const IColumn* root_jsonb = nullptr;
    std::span<const IColumn* const> materialized_columns;
    const ColumnMap* sparse_values = nullptr;
    const ColumnMap* doc_values = nullptr;
};

// Owns normalized PathInData/DataType metadata from create(), then prepares and assembles each
// borrowed batch. A failed batch does not alter later calls or publish a partial result.
class VariantAssembler final {
public:
    static Result<std::unique_ptr<VariantAssembler>> create(VariantAssemblerOptions options);

    VariantAssembler(const VariantAssembler&) = delete;
    VariantAssembler& operator=(const VariantAssembler&) = delete;

    // The nullable wrapper is the complete owning result: its nested column is ColumnVariantV2 and
    // its null map is the assembled outer-null state. It is assigned only after the batch succeeds.
    Status assemble(const VariantAssemblerBatchView& batch, ColumnNullable::MutablePtr* output);

private:
    VariantAssembler(bool has_sparse, bool has_root, bool has_doc, PathInData requested,
                     DorisVector<VariantAssemblerOptions::MaterializedPath> materialized,
                     DorisVector<size_t> materialized_source_indices);

    bool _has_sparse;
    bool _has_root;
    bool _has_doc;
    PathInData _requested;
    DorisVector<VariantAssemblerOptions::MaterializedPath> _materialized;
    // _materialized is sorted for row assembly; each entry maps back to the caller's batch order.
    DorisVector<size_t> _materialized_source_indices;
};

} // namespace segment_v2::variant_v2
} // namespace doris
