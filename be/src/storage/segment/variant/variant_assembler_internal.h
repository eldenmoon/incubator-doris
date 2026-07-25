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
#include <string_view>

#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_string.h"
#include "core/custom_allocator.h"
#include "core/value/variant/variant_batch_builder.h"
#include "storage/segment/variant/variant_assembler.h"
#include "storage/types.h"

namespace doris::segment_v2::variant_assembler_internal {

struct CompiledPath {
    PathInData path;
    DorisVector<StringRef> parts;
    DataTypePtr type;
    size_t source_index = 0;
};

struct RootJsonbView {
    const ColumnString* values = nullptr;
    const uint8_t* nulls = nullptr;

    bool is_null_at(size_t row) const noexcept { return nulls != nullptr && nulls[row] != 0; }
    StringRef value_at(size_t row) const { return values->get_data_at(row); }
};

struct MapView {
    const ColumnArray::Offsets64* offsets = nullptr;
    const ColumnString* paths = nullptr;
    const ColumnString* values = nullptr;

    size_t begin(size_t row) const noexcept { return row == 0 ? 0 : (*offsets)[row - 1]; }
    size_t end(size_t row) const noexcept { return (*offsets)[row]; }
};

struct PreparedColumn {
    const IColumn* data = nullptr;
    const uint8_t* nulls = nullptr;
    DataTypePtr type;
    PrimitiveType primitive = INVALID_TYPE;
    uint8_t scale = 0;
    const ColumnArray* array = nullptr;
    size_t nested_size = 0;
    std::unique_ptr<PreparedColumn> nested;

    bool is_null_at(size_t row) const noexcept { return nulls != nullptr && nulls[row] != 0; }
};

struct CellSignature {
    FieldType type = FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    uint8_t precision = 0;
    uint8_t scale = 0;
    bool typed = false;

    bool operator==(const CellSignature& other) const noexcept {
        return type == other.type && precision == other.precision && scale == other.scale &&
               typed == other.typed;
    }
};

Status prepare_root_jsonb(const IColumn* column, size_t rows, RootJsonbView* output);
Status prepare_map(const ColumnMap* column, size_t rows, std::string_view description,
                   MapView* output);
Status prepare_column(const DataTypePtr& type, const IColumn* column, size_t rows,
                      PreparedColumn* output);

Status append_typed_value(const PreparedColumn& column, size_t row,
                          VariantBatchBuilder::Row& output, uint32_t depth = 0);
Status append_storage_cell(StringRef cell, VariantBatchBuilder::Row& output, uint32_t depth = 0);
Status inspect_storage_cell(StringRef cell, CellSignature* signature);

Status try_build_typed_binary(const ColumnString& cells, std::span<const uint8_t> outer_nulls,
                              std::span<const uint8_t> missing, size_t rows,
                              VariantAssembledColumn* output, bool* built);

int compare_path_parts(std::span<const StringRef> left, std::span<const StringRef> right) noexcept;
bool path_is_prefix(std::span<const StringRef> prefix, std::span<const StringRef> path) noexcept;
Status split_sparse_path(StringRef path, DorisVector<StringRef>* parts);

} // namespace doris::segment_v2::variant_assembler_internal

namespace doris::segment_v2 {

struct VariantAssemblerPlan::Impl {
    VariantAssemblerPlanOptions options;
    variant_assembler_internal::CompiledPath requested;
    DorisVector<variant_assembler_internal::CompiledPath> materialized;
};

} // namespace doris::segment_v2
