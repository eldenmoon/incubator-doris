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

#include "storage/segment/variant/v2/variant_assembler.h"

#include <algorithm>
#include <cstring>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_column_utils.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "storage/segment/variant/v2/variant_assembler_internal.h"

namespace doris::segment_v2::variant_v2 {
namespace {

using MaterializedPath = VariantAssemblerOptions::MaterializedPath;

// Assembly has four stages:
// 1. create() makes materialized paths relative to the requested subtree and sorts them once.
// 2. prepare_hierarchical_batch() unwraps concrete columns once per batch.
// 3. The row loop merges already ordered materialized and sparse/doc paths. A row-local cursor
//    exposes persisted cells, while ObjectEmitter owns the open object scopes.
// 4. publish_encoded() finishes the batch and transfers the completed values/null map atomically.
//
// StorageMapRowCursor and ObjectEmitter are deliberately local implementation state. Neither is a
// reusable reader abstraction: the former only advances one persisted map row, and the latter only
// translates this merge's ordered paths into VariantBatchBuilder calls.

bool row_is_outer_null(const VariantAssemblerBatchView& batch, size_t row) noexcept {
    return !batch.outer_nulls.empty() && batch.outer_nulls[row] != 0;
}

// Row-local cursor over one persisted Map<String,String>. It owns no input data or I/O:
// bind() unwraps the concrete columns once per batch; start_row() then selects a row/subtree.
// Persisted paths are normalized dotted object paths, so the hot loop keeps only borrowed raw
// strings and checks the component boundary directly instead of building a parts vector.
struct StorageMapRowCursor {
    const ColumnArray::Offsets64* offsets = nullptr;
    const ColumnString* paths = nullptr;
    const ColumnString* values = nullptr;
    size_t index = 0;
    size_t end = 0;
    bool available = false;
    bool logical_root = false;
    uint32_t depth = 0;
    StringRef cell;
    StringRef sort_key;
    StringRef requested;
    bool requested_is_root = true;

    void bind(const ColumnMap* source, size_t rows) {
        DORIS_CHECK(source != nullptr);
        DORIS_CHECK_EQ(source->size(), rows);
        paths = check_and_get_column<ColumnString>(&source->get_keys());
        values = check_and_get_column<ColumnString>(&source->get_values());
        DORIS_CHECK(paths != nullptr);
        DORIS_CHECK(values != nullptr);
        DORIS_CHECK_EQ(paths->size(), values->size());
        offsets = &source->get_offsets();
        DCHECK(rows == 0 || (*offsets)[rows - 1] == paths->size());
    }

    size_t begin(size_t row) const noexcept { return row == 0 ? 0 : (*offsets)[row - 1]; }
    size_t row_end(size_t row) const noexcept { return (*offsets)[row]; }
    bool row_empty(size_t row) const noexcept { return begin(row) == row_end(row); }

    Status start_row(size_t row_index, StringRef requested_raw, bool is_requested_root) {
        index = begin(row_index);
        end = row_end(row_index);
        requested = requested_raw;
        requested_is_root = is_requested_root;
        if (!requested_is_root) {
            index = find_variant_sparse_path_lower_bound(requested_raw, *paths, index, end);
        }
        available = false;
        return advance();
    }

    Status advance() {
        available = false;
        while (index < end) {
            const StringRef path = paths->get_data_at(index);
            const StringRef value = values->get_data_at(index);
            ++index;

            logical_root = false;
            sort_key = path;
            if (!requested_is_root) {
                if (path.size < requested.size ||
                    (requested.size != 0 &&
                     std::memcmp(path.data, requested.data, requested.size) != 0)) {
                    index = end;
                    return Status::OK();
                }
                if (path.size == requested.size) {
                    logical_root = true;
                    sort_key = {};
                } else {
                    const auto next = static_cast<unsigned char>(path.data[requested.size]);
                    if (next < static_cast<unsigned char>('.')) {
                        continue;
                    }
                    if (next > static_cast<unsigned char>('.')) {
                        index = end;
                        return Status::OK();
                    }
                    sort_key = {path.data + requested.size + 1, path.size - requested.size - 1};
                }
            }

            depth = logical_root ? 0 : 1;
            if (!logical_root) {
                for (size_t offset = 0; offset < sort_key.size; ++offset) {
                    depth += sort_key.data[offset] == '.';
                }
            }
            if (depth > VARIANT_MAX_NESTING_DEPTH) {
                return Status::Corruption("Variant sparse/doc path exceeds maximum depth {}",
                                          VARIANT_MAX_NESTING_DEPTH);
            }
            cell = value;
            available = true;
            return Status::OK();
        }
        return Status::OK();
    }
};

// Emits ordered dotted paths into one VariantBatchBuilder row. The previous borrowed raw path and
// open object scopes are sufficient to find the component LCP; components are scanned only while
// they are emitted and are never materialized into a separate parts container.
struct ObjectEmitter {
    using ObjectScope = VariantBatchBuilder::Row::ObjectScope;

    VariantBatchBuilder::Row* row = nullptr;
    bool emitted = false;
    bool previous_logical_root = false;
    uint32_t previous_depth = 0;
    StringRef previous_path;
    std::vector<ObjectScope> scopes;

    ObjectEmitter() { scopes.reserve(8); }

    void start_row(VariantBatchBuilder::Row* output) {
        row = output;
        previous_path = {};
        previous_logical_root = false;
        previous_depth = 0;
        scopes.clear();
        emitted = false;
    }

    void prepare(StringRef path, bool logical_root, uint32_t depth) {
        if (logical_root) {
            DCHECK(!emitted);
            emitted = true;
            previous_logical_root = true;
            return;
        }

        size_t lcp = 0;
        if (emitted) {
            DCHECK(!previous_logical_root);
            const char* previous_data = previous_path.data == nullptr ? "" : previous_path.data;
            const char* current_data = path.data == nullptr ? "" : path.data;
            size_t previous_offset = 0;
            size_t current_offset = 0;
            while (previous_offset <= previous_path.size && current_offset <= path.size) {
                const char* previous_dot = previous_offset == previous_path.size
                                                   ? nullptr
                                                   : static_cast<const char*>(std::memchr(
                                                             previous_data + previous_offset, '.',
                                                             previous_path.size - previous_offset));
                const char* current_dot = current_offset == path.size
                                                  ? nullptr
                                                  : static_cast<const char*>(std::memchr(
                                                            current_data + current_offset, '.',
                                                            path.size - current_offset));
                const size_t previous_end =
                        previous_dot == nullptr ? previous_path.size
                                                : static_cast<size_t>(previous_dot - previous_data);
                const size_t current_end =
                        current_dot == nullptr ? path.size
                                               : static_cast<size_t>(current_dot - current_data);
                const size_t previous_size = previous_end - previous_offset;
                const size_t current_size = current_end - current_offset;
                if (previous_size != current_size ||
                    (current_size != 0 &&
                     std::memcmp(previous_data + previous_offset, current_data + current_offset,
                                 current_size) != 0)) {
                    break;
                }
                ++lcp;
                if (previous_end == previous_path.size || current_end == path.size) {
                    break;
                }
                previous_offset = previous_end + 1;
                current_offset = current_end + 1;
            }
            DCHECK(lcp != previous_depth || previous_depth >= depth);
        }
        if (!emitted) {
            scopes.push_back(row->start_object());
        }
        while (scopes.size() > lcp + 1) {
            scopes.back().finish();
            scopes.pop_back();
        }

        const char* path_data = path.data == nullptr ? "" : path.data;
        size_t offset = 0;
        for (size_t part = 0; part < depth; ++part) {
            const char* dot = offset == path.size
                                      ? nullptr
                                      : static_cast<const char*>(std::memchr(
                                                path_data + offset, '.', path.size - offset));
            const size_t end = dot == nullptr ? path.size : static_cast<size_t>(dot - path_data);
            if (part >= lcp) {
                scopes.back().add_key({path_data + offset, end - offset});
                if (part + 1 < depth) {
                    scopes.push_back(row->start_object());
                }
            }
            offset = end + 1;
        }
        previous_path = path;
        previous_logical_root = false;
        previous_depth = depth;
        emitted = true;
    }

    Status append_materialized(StringRef path, bool logical_root, uint32_t depth,
                               const variant_assembler_detail::PreparedMaterializedColumn& column,
                               size_t row_index) {
        prepare(path, logical_root, depth);
        return variant_assembler_detail::append_materialized_value(column, row_index, *row, depth);
    }

    // prepare() mutates the emitter state even though all storage is pointer-owned.
    // NOLINTNEXTLINE(readability-make-member-function-const)
    Status append_cell(StringRef path, bool logical_root, uint32_t depth, StringRef value) {
        prepare(path, logical_root, depth);
        return variant_assembler_detail::append_storage_cell(value, *row, depth);
    }

    void finish_row_object() {
        if (!emitted) {
            auto object = row->start_object();
            object.finish();
            return;
        }
        while (!scopes.empty()) {
            scopes.back().finish();
            scopes.pop_back();
        }
    }
};

struct PreparedHierarchicalBatch {
    const ColumnString* root_values = nullptr;
    const uint8_t* root_nulls = nullptr;
    DorisVector<variant_assembler_detail::PreparedMaterializedColumn> materialized;
};

bool has_materialized_value(
        std::span<const variant_assembler_detail::PreparedMaterializedColumn> materialized,
        std::span<const MaterializedPath> materialized_paths, size_t row,
        bool requested_is_root) {
    DORIS_CHECK_EQ(materialized.size(), materialized_paths.size());
    for (size_t index = 0; index < materialized.size(); ++index) {
        if (variant_assembler_detail::is_materialized_value_visible(
                    materialized[index], row,
                    requested_is_root && materialized_paths[index].path.empty())) {
            return true;
        }
    }
    return false;
}

PreparedHierarchicalBatch prepare_hierarchical_batch(
        bool has_sparse, bool has_root, bool has_doc,
        std::span<const MaterializedPath> materialized_paths,
        std::span<const size_t> materialized_source_indices, const VariantAssemblerBatchView& batch,
        StorageMapRowCursor* map_cursor) {
    DORIS_CHECK_EQ(materialized_paths.size(), materialized_source_indices.size());
    DORIS_CHECK_EQ(batch.materialized_columns.size(), materialized_paths.size());
    DORIS_CHECK_EQ(batch.sparse_values != nullptr, has_sparse);
    DORIS_CHECK_EQ(batch.doc_values != nullptr, has_doc);

    PreparedHierarchicalBatch output;
    if (has_root) {
        DORIS_CHECK(batch.root_jsonb != nullptr);
        DORIS_CHECK_EQ(batch.root_jsonb->size(), batch.num_rows);
        const IColumn* root_values = batch.root_jsonb;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(root_values)) {
            output.root_nulls = nullable->get_null_map_data().data();
            root_values = &nullable->get_nested_column();
        }
        output.root_values = check_and_get_column<ColumnString>(root_values);
        DORIS_CHECK(output.root_values != nullptr);
    } else {
        DORIS_CHECK(batch.root_jsonb == nullptr);
    }

    output.materialized.reserve(materialized_paths.size());
    for (size_t index = 0; index < materialized_paths.size(); ++index) {
        output.materialized.push_back(variant_assembler_detail::prepare_materialized_column(
                materialized_paths[index].type,
                batch.materialized_columns[materialized_source_indices[index]], batch.num_rows));
    }
    if (has_sparse) {
        map_cursor->bind(batch.sparse_values, batch.num_rows);
    } else if (has_doc) {
        map_cursor->bind(batch.doc_values, batch.num_rows);
    }
    return output;
}

struct MergeValue {
    StringRef raw_path;
    const variant_assembler_detail::PreparedMaterializedColumn* materialized = nullptr;
    StringRef cell;
    bool logical_root = false;
    uint32_t depth = 0;

    void set_materialized(const PathInData& path, StringRef raw,
                          const variant_assembler_detail::PreparedMaterializedColumn* column) {
        raw_path = raw;
        materialized = column;
        cell = {};
        logical_root = path.empty();
        DORIS_CHECK_LE(path.get_parts().size(), VARIANT_MAX_NESTING_DEPTH);
        depth = static_cast<uint32_t>(path.get_parts().size());
    }

    void set_cell(StringRef raw, StringRef value, bool is_logical_root, uint32_t cell_depth) {
        raw_path = raw;
        materialized = nullptr;
        cell = value;
        logical_root = is_logical_root;
        depth = cell_depth;
    }
};

Status append_merge_value(const MergeValue& value, size_t row, ObjectEmitter* emitter) {
    if (value.materialized != nullptr) {
        return emitter->append_materialized(value.raw_path, value.logical_root, value.depth,
                                            *value.materialized, row);
    }
    return emitter->append_cell(value.raw_path, value.logical_root, value.depth, value.cell);
}

// A raw key such as "a-" sorts between "a" and "a.b". The ancestor therefore cannot be emitted
// until the merge reaches its dotted descendant range or passes it. This byte comparison retains
// that ordering rule without allocating PathInData parts in the row loop.
int compare_with_raw_descendant_start(StringRef value, StringRef ancestor) noexcept {
    const size_t common = std::min(value.size, ancestor.size);
    const int comparison = common == 0 ? 0 : std::memcmp(value.data, ancestor.data, common);
    if (comparison != 0) {
        return comparison;
    }
    if (value.size <= ancestor.size) {
        return -1;
    }
    const auto next = static_cast<unsigned char>(value.data[ancestor.size]);
    return next < static_cast<unsigned char>('.')   ? -1
           : next > static_cast<unsigned char>('.') ? 1
                                                    : 0;
}

Status append_visible_merge_values(const DorisVector<MergeValue>& values, size_t count, size_t row,
                                   ObjectEmitter* emitter) {
    const auto end = values.begin() + count;
    for (auto current = values.begin(); current != end; ++current) {
        bool has_descendant = false;
        if (current->logical_root) {
            has_descendant = std::find_if(current + 1, end, [](const MergeValue& candidate) {
                                 return !candidate.logical_root;
                             }) != end;
        } else {
            const auto first_possible_descendant =
                    std::lower_bound(current + 1, end, *current,
                                     [](const MergeValue& candidate, const MergeValue& ancestor) {
                                         return compare_with_raw_descendant_start(
                                                        candidate.raw_path, ancestor.raw_path) < 0;
                                     });
            has_descendant = first_possible_descendant != end &&
                             compare_with_raw_descendant_start(first_possible_descendant->raw_path,
                                                               current->raw_path) == 0;
        }
        if (!has_descendant) {
            RETURN_IF_ERROR(append_merge_value(*current, row, emitter));
        }
    }
    return Status::OK();
}

Status emit_doc_row(size_t row, StringRef requested_raw, bool requested_is_root,
                    StorageMapRowCursor* cursor, DorisVector<MergeValue>* pending,
                    MergeValue* current, ObjectEmitter* emitter) {
    RETURN_IF_ERROR(cursor->start_row(row, requested_raw, requested_is_root));
    size_t value_count = 0;
    while (cursor->available) {
        current->set_cell(cursor->sort_key, cursor->cell, cursor->logical_root, cursor->depth);
        RETURN_IF_ERROR(cursor->advance());
        if (value_count == pending->size()) {
            pending->push_back(*current);
        } else {
            (*pending)[value_count] = *current;
        }
        ++value_count;
    }
    return append_visible_merge_values(*pending, value_count, row, emitter);
}

Status emit_merged_row(
        std::span<const variant_assembler_detail::PreparedMaterializedColumn> materialized,
        bool has_sparse, std::span<const MaterializedPath> materialized_paths, size_t row,
        StringRef requested_raw, bool requested_is_root, StorageMapRowCursor* sparse_cursor,
        DorisVector<MergeValue>* pending, MergeValue* current, ObjectEmitter* emitter) {
    if (has_sparse) {
        RETURN_IF_ERROR(sparse_cursor->start_row(row, requested_raw, requested_is_root));
    }
    size_t materialized_index = 0;
    size_t value_count = 0;
    while (true) {
        while (materialized_index < materialized.size()) {
            if (variant_assembler_detail::is_materialized_value_visible(
                        materialized[materialized_index], row,
                        requested_is_root && materialized_paths[materialized_index].path.empty())) {
                break;
            }
            ++materialized_index;
        }
        const bool materialized_available = materialized_index < materialized_paths.size();
        StringRef materialized_raw_path;
        if (materialized_available) {
            const std::string& path = materialized_paths[materialized_index].path.get_path();
            materialized_raw_path = {path.data(), path.size()};
        }
        const bool materialized_is_root =
                materialized_available && materialized_paths[materialized_index].path.empty();
        const int path_comparison =
                !has_sparse || !sparse_cursor->available || !materialized_available
                        ? 0
                        : sparse_cursor->sort_key.compare(materialized_raw_path);
        const bool use_sparse =
                has_sparse && sparse_cursor->available &&
                (!materialized_available || path_comparison < 0 ||
                 (path_comparison == 0 && sparse_cursor->logical_root && !materialized_is_root));
        if (!materialized_available && !use_sparse) {
            return append_visible_merge_values(*pending, value_count, row, emitter);
        }
        if (!use_sparse) {
            current->set_materialized(materialized_paths[materialized_index].path,
                                      materialized_raw_path, &materialized[materialized_index]);
            ++materialized_index;
        } else {
            current->set_cell(sparse_cursor->sort_key, sparse_cursor->cell,
                              sparse_cursor->logical_root, sparse_cursor->depth);
            RETURN_IF_ERROR(sparse_cursor->advance());
        }
        if (value_count == pending->size()) {
            pending->push_back(*current);
        } else {
            (*pending)[value_count] = *current;
        }
        ++value_count;
    }
}

Status assemble_hierarchical_row(bool has_sparse, bool has_root, bool has_doc,
                                 const PathInData& requested,
                                 std::span<const MaterializedPath> materialized_paths,
                                 const PreparedHierarchicalBatch& batch, size_t row_index,
                                 VariantBatchBuilder::Row* row, StorageMapRowCursor* map_cursor,
                                 DorisVector<MergeValue>* pending, MergeValue* current,
                                 ObjectEmitter* emitter, bool* is_outer_null) {
    const bool row_has_root = has_root &&
                              (batch.root_nulls == nullptr || batch.root_nulls[row_index] == 0) &&
                              batch.root_values->get_data_at(row_index).size != 0;
    const bool row_has_doc = has_doc && !map_cursor->row_empty(row_index);
    const bool requested_is_root = requested.empty();
    const bool row_has_materialized = has_materialized_value(
            batch.materialized, materialized_paths, row_index, requested_is_root);

    // Doc keeps its row-exclusive priority. A root-only row retains the direct JSONB fast path;
    // otherwise materialized and sparse values define the visible object without the legacy root
    // sidecar.
    if (row_has_root && !row_has_materialized && !row_has_doc &&
        (!has_sparse || map_cursor->row_empty(row_index))) {
        jsonb_to_variant(batch.root_values->get_data_at(row_index), *row, 0, nullptr);
        return Status::OK();
    }

    const std::string& requested_path = requested.get_path();
    const StringRef requested_raw {requested_path.data(), requested_path.size()};
    emitter->start_row(row);
    if (row_has_doc) {
        RETURN_IF_ERROR(emit_doc_row(row_index, requested_raw, requested_is_root, map_cursor,
                                     pending, current, emitter));
    } else {
        RETURN_IF_ERROR(emit_merged_row(batch.materialized, has_sparse, materialized_paths,
                                        row_index, requested_raw, requested_is_root, map_cursor,
                                        pending, current, emitter));
    }
    if (!emitter->emitted) {
        if (requested.empty() && (has_sparse || has_doc || !materialized_paths.empty())) {
            // In hierarchical root storage, an outer-non-null row with no emitted paths is the
            // empty object. Missing subtrees have no such root-object semantics.
            emitter->finish_row_object();
            return Status::OK();
        }
        row->add_null();
        *is_outer_null = true;
        return Status::OK();
    }
    emitter->finish_row_object();
    return Status::OK();
}

Status assemble_hierarchical(bool has_sparse, bool has_root, bool has_doc,
                             const PathInData& requested,
                             std::span<const MaterializedPath> materialized_paths,
                             std::span<const size_t> materialized_source_indices,
                             const VariantAssemblerBatchView& batch,
                             ColumnNullable::MutablePtr* output) {
    StorageMapRowCursor map_cursor;
    PreparedHierarchicalBatch prepared =
            prepare_hierarchical_batch(has_sparse, has_root, has_doc, materialized_paths,
                                       materialized_source_indices, batch, &map_cursor);
    VariantBatchBuilder builder(
            {.rows = batch.num_rows, .metadata_keys = materialized_paths.size() + 8});
    auto outer = ColumnUInt8::create();
    outer->reserve(batch.num_rows);
    DorisVector<MergeValue> pending;
    MergeValue current;
    ObjectEmitter emitter;
    for (size_t row_index = 0; row_index < batch.num_rows; ++row_index) {
        auto row = builder.begin_row();
        if (row_is_outer_null(batch, row_index)) {
            outer->insert_value(1);
            row.add_null();
            row.finish();
            continue;
        }
        bool is_outer_null = false;
        RETURN_IF_ERROR(assemble_hierarchical_row(
                has_sparse, has_root, has_doc, requested, materialized_paths, prepared, row_index,
                &row, &map_cursor, &pending, &current, &emitter, &is_outer_null));
        outer->insert_value(is_outer_null ? 1 : 0);
        row.finish();
    }
    variant_assembler_detail::publish_encoded(&builder, std::move(outer), output);
    return Status::OK();
}

Status check_options(const VariantAssemblerOptions& options) {
    if (options.requested_path.has_nested_part()) {
        return Status::NotSupported(
                "ColumnVariantV2 does not support assembling nested array path '{}'",
                options.requested_path.get_path());
    }
    for (const auto& materialized : options.materialized_paths) {
        if (materialized.path.has_nested_part()) {
            return Status::NotSupported(
                    "ColumnVariantV2 does not support assembling nested array path '{}'",
                    materialized.path.get_path());
        }
    }
    DORIS_CHECK(options.requested_path.empty() || !options.has_root);
    DORIS_CHECK(!(options.has_sparse && options.has_doc));
    return Status::OK();
}

// Normalize the materialized streams once when the iterator is created. The relative paths are
// sorted in the order consumed by the row merge, while source_indices preserve the caller's
// original batch-column order.
void build_materialized_paths(
        const VariantAssemblerOptions& options, DorisVector<MaterializedPath>* output,
        DorisVector<size_t>* source_indices) { // NOLINT(readability-non-const-parameter)
    struct IndexedPath {
        MaterializedPath value;
        size_t source_index;
    };

    DorisVector<IndexedPath> indexed;
    indexed.reserve(options.materialized_paths.size());
    for (size_t source_index = 0; source_index < options.materialized_paths.size();
         ++source_index) {
        const MaterializedPath& source = options.materialized_paths[source_index];
        // Legacy ColumnVariant segments may materialize a scalar/array root at the empty path.
        // The ordered row merge retains it when it is the only value and drops it when descendants
        // from another physical stream form the visible object.
        DORIS_CHECK(source.type != nullptr);
        DORIS_CHECK_LE(options.requested_path.get_parts().size(), source.path.get_parts().size());
        for (size_t part = 0; part < options.requested_path.get_parts().size(); ++part) {
            DORIS_CHECK_EQ(options.requested_path.get_parts()[part].key,
                           source.path.get_parts()[part].key);
        }
        indexed.push_back({.value = {.path = source.path.copy_pop_nfront(
                                             options.requested_path.get_parts().size()),
                                     .type = source.type},
                           .source_index = source_index});
    }
    std::sort(indexed.begin(), indexed.end(), [](const IndexedPath& left, const IndexedPath& right) {
        const std::string& left_raw = left.value.path.get_path();
        const std::string& right_raw = right.value.path.get_path();
        if (left_raw != right_raw) {
            return left_raw < right_raw;
        }
        // PathInData() (logical root) and PathInData("") (empty object key) share the same raw
        // bytes. The root must precede the empty key so the normal ancestor rule remains stable.
        return left.value.path.empty() && !right.value.path.empty();
    });

    output->reserve(indexed.size());
    source_indices->reserve(indexed.size());
    for (IndexedPath& entry : indexed) {
        DORIS_CHECK_LE(entry.value.path.get_parts().size(), VARIANT_MAX_NESTING_DEPTH);
        output->push_back(std::move(entry.value));
        source_indices->push_back(entry.source_index);
    }
}

} // namespace

Result<std::unique_ptr<VariantAssembler>> VariantAssembler::create(
        VariantAssemblerOptions options) {
    RETURN_IF_ERROR_RESULT(check_options(options));
    DorisVector<MaterializedPath> materialized;
    DorisVector<size_t> materialized_source_indices;
    build_materialized_paths(options, &materialized, &materialized_source_indices);
    return std::unique_ptr<VariantAssembler>(
            new VariantAssembler(options.has_sparse, options.has_root, options.has_doc,
                                 std::move(options.requested_path), std::move(materialized),
                                 std::move(materialized_source_indices)));
}

VariantAssembler::VariantAssembler(
        bool has_sparse, bool has_root, bool has_doc, PathInData requested,
        DorisVector<VariantAssemblerOptions::MaterializedPath> materialized,
        DorisVector<size_t> materialized_source_indices)
        : _has_sparse(has_sparse),
          _has_root(has_root),
          _has_doc(has_doc),
          _requested(std::move(requested)),
          _materialized(std::move(materialized)),
          _materialized_source_indices(std::move(materialized_source_indices)) {}

Status VariantAssembler::assemble(const VariantAssemblerBatchView& batch,
                                  ColumnNullable::MutablePtr* output) {
    DORIS_CHECK(output != nullptr);
    DORIS_CHECK(batch.outer_nulls.empty() || batch.outer_nulls.size() == batch.num_rows);
    try {
        ColumnNullable::MutablePtr result;
        const Status status =
                assemble_hierarchical(_has_sparse, _has_root, _has_doc, _requested, _materialized,
                                      _materialized_source_indices, batch, &result);
        if (!status.ok()) {
            return status;
        }
        *output = std::move(result);
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

} // namespace doris::segment_v2::variant_v2
