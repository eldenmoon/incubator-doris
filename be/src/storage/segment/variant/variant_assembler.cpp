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

#include "storage/segment/variant/variant_assembler.h"

#include <algorithm>
#include <optional>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "storage/segment/variant/variant_assembler_internal.h"

namespace doris::segment_v2 {
namespace {

using namespace variant_assembler_internal;

bool row_is_outer_null(const VariantAssemblerBatchView& batch, size_t row) noexcept {
    return !batch.outer_nulls.empty() && batch.outer_nulls[row] != 0;
}

Status publish_encoded(VariantBatchBuilder* builder, MutableColumnPtr outer_nulls,
                       VariantAssembledColumn* output) {
    VariantBatchBuilder block = builder->finish_batch();
    VariantAssembledColumn result;
    result.values = ColumnVariantV2::create();
    result.values->insert_encoded_batch(block);
    result.outer_nulls = std::move(outer_nulls);
    *output = std::move(result);
    return Status::OK();
}

size_t common_prefix(std::span<const StringRef> left, std::span<const StringRef> right) noexcept {
    const size_t maximum = std::min(left.size(), right.size());
    size_t result = 0;
    while (result < maximum && left[result] == right[result]) {
        ++result;
    }
    return result;
}

struct MapCursor {
    const MapView* map = nullptr;
    std::span<const StringRef> requested;
    std::string_view description;
    size_t row = 0;
    size_t index = 0;
    size_t end = 0;
    StringRef previous_path;
    bool have_previous = false;
    bool available = false;
    StringRef cell;
    StringRef sort_key;
    DorisVector<StringRef> parts;

    Status reset(const MapView* source, size_t row_index, std::span<const StringRef> requested_path,
                 StringRef requested_raw, std::string_view source_description) {
        map = source;
        requested = requested_path;
        description = source_description;
        row = row_index;
        index = map->begin(row);
        end = map->end(row);
        previous_path = {};
        have_previous = false;
        available = false;
        return advance(requested_raw);
    }

    Status advance(StringRef requested_raw = {}) {
        available = false;
        while (index < end) {
            const StringRef path = map->paths->get_data_at(index);
            cell = map->values->get_data_at(index);
            ++index;
            if (have_previous && previous_path.compare(path) >= 0) {
                return Status::Corruption("Variant {} row {} paths are not strictly sorted at {}",
                                          description, row, path.to_string());
            }
            previous_path = path;
            have_previous = true;

            RETURN_IF_ERROR(split_sparse_path(path, &parts));
            if (!path_is_prefix(requested, parts)) {
                continue;
            }
            size_t relative_offset = requested_raw.size;
            if (!requested.empty() && parts.size() > requested.size()) {
                ++relative_offset;
            }
            sort_key = relative_offset == path.size ? StringRef {}
                                                    : StringRef {path.data + relative_offset,
                                                                 path.size - relative_offset};
            parts.erase(parts.begin(), parts.begin() + requested.size());
            if (parts.size() > VARIANT_MAX_NESTING_DEPTH) {
                return Status::Corruption("Variant sparse/doc path exceeds maximum depth {}",
                                          VARIANT_MAX_NESTING_DEPTH);
            }
            available = true;
            return Status::OK();
        }
        return Status::OK();
    }
};

struct ObjectEmitter {
    using ObjectScope = VariantBatchBuilder::Row::ObjectScope;

    VariantBatchBuilder::Row* row = nullptr;
    DorisVector<StringRef>* previous = nullptr;
    std::vector<ObjectScope>* scopes = nullptr;
    bool emitted = false;

    void reset(VariantBatchBuilder::Row* output, DorisVector<StringRef>* previous_parts,
               std::vector<ObjectScope>* object_scopes) {
        row = output;
        previous = previous_parts;
        scopes = object_scopes;
        previous->clear();
        scopes->clear();
        emitted = false;
    }

    Status prepare(std::span<const StringRef> parts) {
        if (emitted) {
            if (compare_path_parts(*previous, parts) == 0) {
                return Status::Corruption("Duplicate Variant path across assembled input");
            }
        }
        if (parts.empty()) {
            if (emitted) {
                return Status::Corruption(
                        "Variant scalar root storage cell cannot coexist with child paths");
            }
            emitted = true;
            return Status::OK();
        }
        if (emitted && previous->empty()) {
            return Status::Corruption(
                    "Variant scalar root storage cell cannot coexist with child paths");
        }

        const size_t lcp = common_prefix(*previous, parts);
        if (!previous->empty() && lcp == previous->size() && previous->size() < parts.size()) {
            return Status::Corruption("Variant assembled path conflicts with scalar ancestor");
        }
        if (!emitted) {
            scopes->push_back(row->start_object());
        }
        while (scopes->size() > lcp + 1) {
            scopes->back().finish();
            scopes->pop_back();
        }
        for (size_t part = lcp; part + 1 < parts.size(); ++part) {
            scopes->back().add_key(parts[part]);
            scopes->push_back(row->start_object());
        }
        scopes->back().add_key(parts.back());
        previous->assign(parts.begin(), parts.end());
        emitted = true;
        return Status::OK();
    }

    Status append_materialized(std::span<const StringRef> parts, const PreparedColumn& column,
                               size_t row_index) {
        RETURN_IF_ERROR(prepare(parts));
        return append_typed_value(column, row_index, *row, static_cast<uint32_t>(parts.size()));
    }

    // prepare() mutates the emitter state even though all storage is pointer-owned.
    // NOLINTNEXTLINE(readability-make-member-function-const)
    Status append_cell(std::span<const StringRef> parts, StringRef value) {
        RETURN_IF_ERROR(prepare(parts));
        return append_storage_cell(value, *row, static_cast<uint32_t>(parts.size()));
    }

    // prepare() mutates the emitter state even though all storage is pointer-owned.
    // NOLINTNEXTLINE(readability-make-member-function-const)
    Status append_value(std::span<const StringRef> parts, VariantRef value) {
        RETURN_IF_ERROR(prepare(parts));
        row->add_value(value);
        return Status::OK();
    }

    void finish() const {
        if (!emitted) {
            auto object = row->start_object();
            object.finish();
            return;
        }
        while (!scopes->empty()) {
            scopes->back().finish();
            scopes->pop_back();
        }
    }
};

Status validate_no_hierarchical_streams(const VariantAssemblerBatchView& batch,
                                        std::string_view mode) {
    if (!batch.materialized_columns.empty() || !batch.sparse_buckets.empty() ||
        batch.doc_values != nullptr) {
        return Status::InvalidArgument("Variant {} batch supplied a hierarchical stream", mode);
    }
    return Status::OK();
}

Status validate_no_binary_streams(const VariantAssemblerBatchView& batch, std::string_view mode) {
    if (batch.binary_values != nullptr || !batch.binary_missing.empty()) {
        return Status::InvalidArgument("Variant {} batch supplied a binary stream", mode);
    }
    return Status::OK();
}

Status validate_common_batch(const VariantAssemblerBatchView& batch) {
    if (!batch.outer_nulls.empty() && batch.outer_nulls.size() != batch.num_rows) {
        return Status::InvalidArgument("Variant outer-null span has {} rows, expected {}",
                                       batch.outer_nulls.size(), batch.num_rows);
    }
    return Status::OK();
}

Status assemble_default(const VariantAssemblerBatchView& batch, VariantAssembledColumn* output) {
    RETURN_IF_ERROR(validate_no_hierarchical_streams(batch, "DEFAULT_FILL"));
    RETURN_IF_ERROR(validate_no_binary_streams(batch, "DEFAULT_FILL"));
    if (batch.root_jsonb != nullptr) {
        return Status::InvalidArgument("Variant DEFAULT_FILL batch supplied a root stream");
    }
    VariantBatchBuilder builder({.rows = batch.num_rows});
    auto outer = ColumnUInt8::create();
    for (size_t row_index = 0; row_index < batch.num_rows; ++row_index) {
        auto row = builder.begin_row();
        const bool is_null = row_is_outer_null(batch, row_index);
        outer->insert_value(is_null ? 1 : 0);
        if (is_null) {
            row.add_null();
        } else {
            auto object = row.start_object();
            object.finish();
        }
        row.finish();
    }
    return publish_encoded(&builder, std::move(outer), output);
}

Status assemble_root_flat(const VariantAssemblerBatchView& batch, VariantAssembledColumn* output) {
    RETURN_IF_ERROR(validate_no_hierarchical_streams(batch, "ROOT_FLAT"));
    RETURN_IF_ERROR(validate_no_binary_streams(batch, "ROOT_FLAT"));
    RootJsonbView root;
    RETURN_IF_ERROR(prepare_root_jsonb(batch.root_jsonb, batch.num_rows, &root));
    VariantBatchBuilder builder({.rows = batch.num_rows});
    auto outer = ColumnUInt8::create();
    for (size_t row_index = 0; row_index < batch.num_rows; ++row_index) {
        auto row = builder.begin_row();
        const bool is_null = row_is_outer_null(batch, row_index) || root.is_null_at(row_index);
        outer->insert_value(is_null ? 1 : 0);
        if (is_null) {
            row.add_null();
        } else if (root.value_at(row_index).size == 0) {
            auto object = row.start_object();
            object.finish();
        } else {
            jsonb_to_variant(root.value_at(row_index), row);
        }
        row.finish();
    }
    return publish_encoded(&builder, std::move(outer), output);
}

Status assemble_binary(const VariantAssemblerBatchView& batch, VariantAssembledColumn* output) {
    RETURN_IF_ERROR(validate_no_hierarchical_streams(batch, "BINARY_EXTRACT"));
    if (batch.root_jsonb != nullptr) {
        return Status::InvalidArgument("Variant BINARY_EXTRACT batch supplied a root stream");
    }
    if (batch.binary_values == nullptr || batch.binary_values->size() != batch.num_rows) {
        return Status::Corruption("Variant binary input must contain {} rows", batch.num_rows);
    }
    if (!batch.binary_missing.empty() && batch.binary_missing.size() != batch.num_rows) {
        return Status::InvalidArgument("Variant binary-missing span has {} rows, expected {}",
                                       batch.binary_missing.size(), batch.num_rows);
    }
    bool built_typed = false;
    RETURN_IF_ERROR(try_build_typed_binary(*batch.binary_values, batch.outer_nulls,
                                           batch.binary_missing, batch.num_rows, output,
                                           &built_typed));
    if (built_typed) {
        return Status::OK();
    }

    VariantBatchBuilder builder({.rows = batch.num_rows});
    auto outer = ColumnUInt8::create();
    for (size_t row_index = 0; row_index < batch.num_rows; ++row_index) {
        auto row = builder.begin_row();
        const bool is_missing =
                row_is_outer_null(batch, row_index) ||
                (!batch.binary_missing.empty() && batch.binary_missing[row_index] != 0);
        outer->insert_value(is_missing ? 1 : 0);
        if (is_missing) {
            row.add_null();
        } else {
            RETURN_IF_ERROR(append_storage_cell(batch.binary_values->get_data_at(row_index), row));
        }
        row.finish();
    }
    return publish_encoded(&builder, std::move(outer), output);
}

struct PreparedHierarchicalBatch {
    RootJsonbView root;
    DorisVector<PreparedColumn> materialized;
    DorisVector<MapView> sparse;
    MapView doc;
    std::optional<VariantBatchBuilder> root_overlay;
};

bool has_materialized_value(std::span<const PreparedColumn> materialized, size_t row) {
    return std::any_of(materialized.begin(), materialized.end(),
                       [row](const PreparedColumn& column) { return !column.is_null_at(row); });
}

bool has_sparse_value(std::span<const MapView> sparse, size_t row) {
    return std::any_of(sparse.begin(), sparse.end(),
                       [row](const MapView& map) { return map.begin(row) != map.end(row); });
}

Status prepare_root_overlay(const VariantAssemblerPlanOptions& options,
                            const VariantAssemblerBatchView& source,
                            PreparedHierarchicalBatch* batch) {
    if (!options.has_root || !options.merge_root_with_subcolumns ||
        (batch->materialized.empty() && batch->sparse.empty())) {
        return Status::OK();
    }
    VariantBatchBuilder builder({.rows = source.num_rows});
    for (size_t row_index = 0; row_index < source.num_rows; ++row_index) {
        auto row = builder.begin_row();
        const bool has_doc =
                options.has_doc && batch->doc.begin(row_index) != batch->doc.end(row_index);
        const bool needs_overlay = !row_is_outer_null(source, row_index) &&
                                   !batch->root.is_null_at(row_index) &&
                                   batch->root.value_at(row_index).size != 0 && !has_doc &&
                                   (has_materialized_value(batch->materialized, row_index) ||
                                    has_sparse_value(batch->sparse, row_index));
        if (needs_overlay) {
            jsonb_to_variant(batch->root.value_at(row_index), row);
        } else {
            row.add_null();
        }
        row.finish();
    }
    batch->root_overlay.emplace(builder.finish_batch());
    return Status::OK();
}

Status prepare_hierarchical_batch(const VariantAssemblerPlanOptions& options,
                                  std::span<const CompiledPath> plan_materialized,
                                  const VariantAssemblerBatchView& batch,
                                  PreparedHierarchicalBatch* output) {
    RETURN_IF_ERROR(validate_no_binary_streams(batch, "HIERARCHICAL"));
    if (batch.materialized_columns.size() != plan_materialized.size()) {
        return Status::Corruption("Variant batch has {} materialized columns, expected {}",
                                  batch.materialized_columns.size(), plan_materialized.size());
    }
    if (batch.sparse_buckets.size() != options.sparse_bucket_count) {
        return Status::Corruption("Variant batch has {} sparse buckets, expected {}",
                                  batch.sparse_buckets.size(), options.sparse_bucket_count);
    }
    if (options.has_root) {
        RETURN_IF_ERROR(prepare_root_jsonb(batch.root_jsonb, batch.num_rows, &output->root));
    } else if (batch.root_jsonb != nullptr) {
        return Status::InvalidArgument("Variant batch supplied an unplanned root stream");
    }

    output->materialized.resize(plan_materialized.size());
    for (size_t index = 0; index < output->materialized.size(); ++index) {
        RETURN_IF_ERROR(
                prepare_column(plan_materialized[index].type,
                               batch.materialized_columns[plan_materialized[index].source_index],
                               batch.num_rows, &output->materialized[index]));
    }
    output->sparse.resize(batch.sparse_buckets.size());
    for (size_t bucket = 0; bucket < output->sparse.size(); ++bucket) {
        RETURN_IF_ERROR(prepare_map(batch.sparse_buckets[bucket], batch.num_rows, "sparse",
                                    &output->sparse[bucket]));
    }
    if (options.has_doc) {
        RETURN_IF_ERROR(prepare_map(batch.doc_values, batch.num_rows, "doc", &output->doc));
    } else if (batch.doc_values != nullptr) {
        return Status::InvalidArgument("Variant batch supplied an unplanned doc stream");
    }
    return prepare_root_overlay(options, batch, output);
}

Status emit_doc_row(const MapView& doc, size_t row, std::span<const StringRef> requested,
                    StringRef requested_raw, MapCursor* cursor, ObjectEmitter* emitter) {
    RETURN_IF_ERROR(cursor->reset(&doc, row, requested, requested_raw, "doc"));
    while (cursor->available) {
        RETURN_IF_ERROR(emitter->append_cell(cursor->parts, cursor->cell));
        RETURN_IF_ERROR(cursor->advance(requested_raw));
    }
    return Status::OK();
}

struct MergeSelection {
    size_t bucket = 0;
    std::span<const StringRef> parts;
    bool available = false;
};

struct RootCursor {
    struct Frame {
        VariantRef object;
        uint32_t next = 0;
        uint32_t count = 0;
        size_t prefix_size = 0;
    };

    DorisVector<Frame> frames;
    DorisVector<StringRef> parts;
    VariantRef value;
    bool available = false;

    Status reset(VariantRef root) {
        frames.clear();
        parts.clear();
        available = false;
        if (root.basic_type() != VariantBasicType::OBJECT) {
            return Status::Corruption("Variant root overlay requires an object root value");
        }
        const uint32_t count = root.num_elements();
        if (count == 0) {
            value = root;
            available = true;
            return Status::OK();
        }
        frames.push_back({.object = root, .next = 0, .count = count, .prefix_size = 0});
        return advance();
    }

    Status advance() {
        available = false;
        while (!frames.empty()) {
            if (frames.back().next == frames.back().count) {
                parts.resize(frames.back().prefix_size);
                frames.pop_back();
                continue;
            }
            parts.resize(frames.back().prefix_size);
            uint32_t field_id = 0;
            VariantRef child = frames.back().object.object_value_at(frames.back().next, &field_id);
            ++frames.back().next;
            parts.push_back(child.metadata.key_at(field_id));
            if (parts.size() > VARIANT_MAX_NESTING_DEPTH) {
                return Status::Corruption("Variant root path exceeds maximum depth {}",
                                          VARIANT_MAX_NESTING_DEPTH);
            }
            if (child.basic_type() == VariantBasicType::OBJECT && child.num_elements() != 0) {
                frames.push_back({.object = child,
                                  .next = 0,
                                  .count = child.num_elements(),
                                  .prefix_size = parts.size()});
                continue;
            }
            value = child;
            available = true;
            return Status::OK();
        }
        return Status::OK();
    }
};

Status emit_root_value(RootCursor* root, ObjectEmitter* emitter) {
    RETURN_IF_ERROR(emitter->append_value(root->parts, root->value));
    return root->advance();
}

MergeSelection select_next_source(std::span<const CompiledPath> plan_materialized,
                                  size_t materialized_index, std::span<const MapCursor> cursors) {
    MergeSelection selection {.bucket = cursors.size(), .parts = {}, .available = false};
    StringRef selected_sort_key;
    if (materialized_index < plan_materialized.size()) {
        selection.parts = plan_materialized[materialized_index].parts;
        const std::string& path = plan_materialized[materialized_index].path.get_path();
        selected_sort_key = {path.data(), path.size()};
        selection.available = true;
    }
    for (size_t bucket = 0; bucket < cursors.size(); ++bucket) {
        if (!cursors[bucket].available) {
            continue;
        }
        if (!selection.available || cursors[bucket].sort_key.compare(selected_sort_key) < 0) {
            selection.bucket = bucket;
            selection.parts = cursors[bucket].parts;
            selected_sort_key = cursors[bucket].sort_key;
            selection.available = true;
        }
    }
    return selection;
}

Status emit_selected_value(const MergeSelection& selection,
                           std::span<const PreparedColumn> materialized, size_t row,
                           StringRef requested_raw, size_t* materialized_index,
                           std::span<MapCursor> cursors, ObjectEmitter* emitter) {
    if (selection.bucket == cursors.size()) {
        RETURN_IF_ERROR(emitter->append_materialized(selection.parts,
                                                     materialized[*materialized_index], row));
        ++*materialized_index;
        return Status::OK();
    }
    RETURN_IF_ERROR(emitter->append_cell(selection.parts, cursors[selection.bucket].cell));
    return cursors[selection.bucket].advance(requested_raw);
}

Status merge_root_sources_step(const MergeSelection& selection,
                               std::span<const PreparedColumn> materialized, size_t row,
                               StringRef requested_raw, size_t* materialized_index,
                               std::span<MapCursor> cursors, RootCursor* root,
                               ObjectEmitter* emitter) {
    if (!selection.available) {
        return emit_root_value(root, emitter);
    }
    if (!root->available) {
        return emit_selected_value(selection, materialized, row, requested_raw, materialized_index,
                                   cursors, emitter);
    }

    const int comparison = compare_path_parts(root->parts, selection.parts);
    if (comparison == 0 || path_is_prefix(selection.parts, root->parts)) {
        do {
            RETURN_IF_ERROR(root->advance());
        } while (root->available && path_is_prefix(selection.parts, root->parts));
        return emit_selected_value(selection, materialized, row, requested_raw, materialized_index,
                                   cursors, emitter);
    }
    if (path_is_prefix(root->parts, selection.parts)) {
        return root->advance();
    }
    return comparison < 0 ? emit_root_value(root, emitter)
                          : emit_selected_value(selection, materialized, row, requested_raw,
                                                materialized_index, cursors, emitter);
}

Status emit_root_merged_row(VariantRef root, std::span<const PreparedColumn> materialized,
                            std::span<const MapView> sparse,
                            std::span<const CompiledPath> plan_materialized, size_t row,
                            std::span<const StringRef> requested, StringRef requested_raw,
                            std::span<MapCursor> cursors, RootCursor* root_cursor,
                            ObjectEmitter* emitter) {
    RETURN_IF_ERROR(root_cursor->reset(root));
    for (size_t bucket = 0; bucket < sparse.size(); ++bucket) {
        RETURN_IF_ERROR(
                cursors[bucket].reset(&sparse[bucket], row, requested, requested_raw, "sparse"));
    }
    size_t materialized_index = 0;
    while (true) {
        while (materialized_index < materialized.size() &&
               materialized[materialized_index].is_null_at(row)) {
            ++materialized_index;
        }
        const MergeSelection selection =
                select_next_source(plan_materialized, materialized_index, cursors);
        if (!root_cursor->available && !selection.available) {
            return Status::OK();
        }
        RETURN_IF_ERROR(merge_root_sources_step(selection, materialized, row, requested_raw,
                                                &materialized_index, cursors, root_cursor,
                                                emitter));
    }
}

Status emit_merged_row(std::span<const PreparedColumn> materialized,
                       std::span<const MapView> sparse,
                       std::span<const CompiledPath> plan_materialized, size_t row,
                       std::span<const StringRef> requested, StringRef requested_raw,
                       std::span<MapCursor> cursors, ObjectEmitter* emitter) {
    for (size_t bucket = 0; bucket < sparse.size(); ++bucket) {
        RETURN_IF_ERROR(
                cursors[bucket].reset(&sparse[bucket], row, requested, requested_raw, "sparse"));
    }
    size_t materialized_index = 0;
    while (true) {
        while (materialized_index < materialized.size() &&
               materialized[materialized_index].is_null_at(row)) {
            ++materialized_index;
        }
        const MergeSelection selection =
                select_next_source(plan_materialized, materialized_index, cursors);
        if (!selection.available) {
            return Status::OK();
        }
        RETURN_IF_ERROR(emit_selected_value(selection, materialized, row, requested_raw,
                                            &materialized_index, cursors, emitter));
    }
}

Status assemble_hierarchical_row(const VariantAssemblerPlanOptions& options,
                                 StringRef requested_raw, std::span<const StringRef> requested,
                                 std::span<const CompiledPath> plan_materialized,
                                 const PreparedHierarchicalBatch& batch, size_t row_index,
                                 VariantBatchBuilder::Row* row, std::span<MapCursor> cursors,
                                 DorisVector<StringRef>* previous_parts,
                                 std::vector<VariantBatchBuilder::Row::ObjectScope>* scopes,
                                 RootCursor* root_cursor, ObjectEmitter* emitter, bool* no_match) {
    *no_match = false;
    const bool has_materialized = has_materialized_value(batch.materialized, row_index);
    const bool has_doc = options.has_doc && batch.doc.begin(row_index) != batch.doc.end(row_index);
    const bool has_root = options.has_root && !batch.root.is_null_at(row_index) &&
                          batch.root.value_at(row_index).size != 0;
    const bool has_sparse = has_sparse_value(batch.sparse, row_index);

    // Doc keeps its row-exclusive priority. A root-only row retains the direct JSONB fast path;
    // materialized and sparse values overlay a visible object root by structured path.
    if (has_root && !has_materialized && !has_doc && !has_sparse) {
        jsonb_to_variant(batch.root.value_at(row_index), *row);
        return Status::OK();
    }

    emitter->reset(row, previous_parts, scopes);
    if (has_doc) {
        RETURN_IF_ERROR(
                emit_doc_row(batch.doc, row_index, requested, requested_raw, &cursors[0], emitter));
    } else if (has_root && options.merge_root_with_subcolumns && (has_materialized || has_sparse)) {
        DORIS_CHECK(batch.root_overlay.has_value());
        RETURN_IF_ERROR(emit_root_merged_row(
                batch.root_overlay->value_at(row_index), batch.materialized, batch.sparse,
                plan_materialized, row_index, requested, requested_raw,
                cursors.first(batch.sparse.size()), root_cursor, emitter));
    } else {
        RETURN_IF_ERROR(emit_merged_row(batch.materialized, batch.sparse, plan_materialized,
                                        row_index, requested, requested_raw,
                                        cursors.first(batch.sparse.size()), emitter));
    }
    if (options.null_on_no_match && !emitter->emitted) {
        row->add_null();
        *no_match = true;
        return Status::OK();
    }
    emitter->finish();
    return Status::OK();
}

Status assemble_hierarchical(const VariantAssemblerPlanOptions& options, StringRef requested_raw,
                             std::span<const StringRef> requested,
                             std::span<const CompiledPath> plan_materialized,
                             const VariantAssemblerBatchView& batch,
                             VariantAssembledColumn* output) {
    PreparedHierarchicalBatch prepared;
    RETURN_IF_ERROR(prepare_hierarchical_batch(options, plan_materialized, batch, &prepared));
    VariantBatchBuilder builder(
            {.rows = batch.num_rows, .metadata_keys = plan_materialized.size() + 8});
    auto outer = ColumnUInt8::create();
    DorisVector<StringRef> previous_parts;
    std::vector<VariantBatchBuilder::Row::ObjectScope> scopes;
    scopes.reserve(8);
    DorisVector<MapCursor> cursors(options.sparse_bucket_count + 1);
    RootCursor root_cursor;
    ObjectEmitter emitter;
    for (size_t row_index = 0; row_index < batch.num_rows; ++row_index) {
        auto row = builder.begin_row();
        if (row_is_outer_null(batch, row_index)) {
            outer->insert_value(1);
            row.add_null();
            row.finish();
            continue;
        }
        bool no_match = false;
        RETURN_IF_ERROR(assemble_hierarchical_row(
                options, requested_raw, requested, plan_materialized, prepared, row_index, &row,
                cursors, &previous_parts, &scopes, &root_cursor, &emitter, &no_match));
        outer->insert_value(no_match ? 1 : 0);
        row.finish();
    }
    return publish_encoded(&builder, std::move(outer), output);
}

Status validate_plan_options(const VariantAssemblerPlanOptions& options) {
    switch (options.mode) {
    case VariantAssemblerMode::HIERARCHICAL:
    case VariantAssemblerMode::ROOT_FLAT:
    case VariantAssemblerMode::BINARY_EXTRACT:
    case VariantAssemblerMode::DEFAULT_FILL:
        break;
    default:
        return Status::InvalidArgument("Unknown Variant assembler mode {}",
                                       static_cast<uint8_t>(options.mode));
    }
    if (options.mode != VariantAssemblerMode::HIERARCHICAL &&
        (!options.materialized_paths.empty() || options.sparse_bucket_count != 0 ||
         options.has_doc)) {
        return Status::InvalidArgument(
                "Only HIERARCHICAL Variant plans may contain materialized/sparse/doc streams");
    }
    if (options.mode == VariantAssemblerMode::ROOT_FLAT && !options.has_root) {
        return Status::InvalidArgument("ROOT_FLAT Variant plan requires a root stream");
    }
    if (options.mode != VariantAssemblerMode::HIERARCHICAL &&
        options.mode != VariantAssemblerMode::ROOT_FLAT && options.has_root) {
        return Status::InvalidArgument("Variant plan mode does not consume a root stream");
    }
    if (options.mode != VariantAssemblerMode::HIERARCHICAL && !options.requested_path.empty()) {
        return Status::InvalidArgument("Only HIERARCHICAL Variant plans accept a requested path");
    }
    if (options.null_on_no_match &&
        (options.mode != VariantAssemblerMode::HIERARCHICAL || options.requested_path.empty())) {
        return Status::InvalidArgument(
                "Variant null-on-no-match requires a hierarchical subtree plan");
    }
    if (options.requested_path.has_nested_part()) {
        return Status::InvalidArgument("Variant requested path must be regular");
    }
    if (!options.requested_path.empty() && options.has_root) {
        return Status::InvalidArgument(
                "Variant subtree plans cannot consume an unfiltered root stream");
    }
    return Status::OK();
}

Status compile_requested_path(const VariantAssemblerPlanOptions& options, CompiledPath* requested,
                              bool* has_physical_paths) {
    requested->path = options.requested_path;
    requested->parts.reserve(requested->path.get_parts().size());
    for (const PathInData::Part& part : requested->path.get_parts()) {
        requested->parts.emplace_back(part.key.data(), part.key.size());
    }
    *has_physical_paths = options.sparse_bucket_count != 0 || options.has_doc;
    if (*has_physical_paths) {
        for (const PathInData::Part& part : requested->path.get_parts()) {
            if (part.key.find('.') != std::string_view::npos) {
                return Status::Corruption(
                        "Variant sparse/doc requested path contains an ambiguous dot key");
            }
        }
    }
    return Status::OK();
}

// The output vector is built in place; clang-tidy cannot see mutation through the Doris alias.
Status compile_materialized_paths(
        const VariantAssemblerPlanOptions& options, const CompiledPath& requested,
        bool has_physical_paths,
        DorisVector<CompiledPath>* output) { // NOLINT(readability-non-const-parameter)
    output->reserve(options.materialized_paths.size());
    for (size_t source_index = 0; source_index < options.materialized_paths.size();
         ++source_index) {
        const VariantAssemblerMaterializedPath& source = options.materialized_paths[source_index];
        if (source.path.empty() || source.path.has_nested_part() || source.type == nullptr) {
            return Status::InvalidArgument(
                    "Variant materialized paths must be non-empty regular typed paths");
        }
        if (requested.path.get_parts().size() > source.path.get_parts().size()) {
            return Status::InvalidArgument(
                    "Variant materialized path is outside the requested subtree");
        }
        for (size_t part = 0; part < requested.path.get_parts().size(); ++part) {
            if (requested.path.get_parts()[part].key != source.path.get_parts()[part].key) {
                return Status::InvalidArgument(
                        "Variant materialized path is outside the requested subtree");
            }
        }
        PathInData relative = source.path.copy_pop_nfront(requested.path.get_parts().size());
        output->push_back({.path = std::move(relative),
                           .parts = {},
                           .type = source.type,
                           .source_index = source_index});
    }
    std::sort(output->begin(), output->end(),
              [has_physical_paths](const CompiledPath& left, const CompiledPath& right) {
                  if (has_physical_paths) {
                      return left.path.get_path() < right.path.get_path();
                  }
                  return left.path < right.path;
              });
    for (CompiledPath& compiled : *output) {
        if (compiled.path.get_parts().size() > VARIANT_MAX_NESTING_DEPTH) {
            return Status::InvalidArgument("Variant materialized path exceeds maximum depth {}",
                                           VARIANT_MAX_NESTING_DEPTH);
        }
        compiled.parts.reserve(compiled.path.get_parts().size());
        for (const PathInData::Part& part : compiled.path.get_parts()) {
            compiled.parts.emplace_back(part.key.data(), part.key.size());
            if (has_physical_paths && part.key.find('.') != std::string_view::npos) {
                return Status::Corruption(
                        "Variant materialized path has a dot key ambiguous with sparse/doc input");
            }
        }
    }
    for (size_t index = 1; index < output->size(); ++index) {
        if (compare_path_parts((*output)[index - 1].parts, (*output)[index].parts) == 0) {
            return Status::Corruption("Variant materialized paths must be unique");
        }
    }
    return Status::OK();
}

} // namespace

VariantAssemblerPlan::VariantAssemblerPlan(std::unique_ptr<Impl> impl) : _impl(std::move(impl)) {}
VariantAssemblerPlan::~VariantAssemblerPlan() = default;

Status VariantAssemblerPlan::create(VariantAssemblerPlanOptions options,
                                    std::shared_ptr<const VariantAssemblerPlan>* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("Variant assembler plan output must not be null");
    }
    RETURN_IF_ERROR(validate_plan_options(options));

    auto impl = std::make_unique<Impl>();
    bool has_physical_paths = false;
    RETURN_IF_ERROR(compile_requested_path(options, &impl->requested, &has_physical_paths));
    RETURN_IF_ERROR(compile_materialized_paths(options, impl->requested, has_physical_paths,
                                               &impl->materialized));
    impl->options = std::move(options);

    *output =
            std::shared_ptr<const VariantAssemblerPlan>(new VariantAssemblerPlan(std::move(impl)));
    return Status::OK();
}

struct VariantAssembler::Impl {
    explicit Impl(std::shared_ptr<const VariantAssemblerPlan> plan_) : plan(std::move(plan_)) {
        if (plan == nullptr) {
            state = State::FAILED;
            failure = Status::InvalidArgument("Variant assembler plan must not be null");
        }
    }

    enum class State : uint8_t { READY, FAILED };

    Status require_ready() const { return state == State::FAILED ? failure : Status::OK(); }

    Status fail(Status status) {
        if (state != State::FAILED) {
            failure = std::move(status);
            state = State::FAILED;
        }
        return failure;
    }

    std::shared_ptr<const VariantAssemblerPlan> plan;
    State state = State::READY;
    Status failure;
};

VariantAssembler::VariantAssembler(std::shared_ptr<const VariantAssemblerPlan> plan)
        : _impl(std::make_unique<Impl>(std::move(plan))) {}
VariantAssembler::~VariantAssembler() = default;
VariantAssembler::VariantAssembler(VariantAssembler&&) noexcept = default;
VariantAssembler& VariantAssembler::operator=(VariantAssembler&&) noexcept = default;

Status VariantAssembler::assemble(const VariantAssemblerBatchView& batch,
                                  VariantAssembledColumn* output) {
    RETURN_IF_ERROR(_impl->require_ready());
    if (output == nullptr) {
        return _impl->fail(Status::InvalidArgument("Variant assembler output must not be null"));
    }
    Status status = validate_common_batch(batch);
    if (!status.ok()) {
        return _impl->fail(std::move(status));
    }
    try {
        VariantAssembledColumn result;
        switch (_impl->plan->_impl->options.mode) {
        case VariantAssemblerMode::HIERARCHICAL: {
            const std::string& requested_path = _impl->plan->_impl->requested.path.get_path();
            status = assemble_hierarchical(_impl->plan->_impl->options,
                                           {requested_path.data(), requested_path.size()},
                                           _impl->plan->_impl->requested.parts,
                                           _impl->plan->_impl->materialized, batch, &result);
            break;
        }
        case VariantAssemblerMode::ROOT_FLAT:
            status = assemble_root_flat(batch, &result);
            break;
        case VariantAssemblerMode::BINARY_EXTRACT:
            status = assemble_binary(batch, &result);
            break;
        case VariantAssemblerMode::DEFAULT_FILL:
            status = assemble_default(batch, &result);
            break;
        }
        if (!status.ok()) {
            return _impl->fail(std::move(status));
        }
        *output = std::move(result);
        return Status::OK();
    } catch (const Exception& exception) {
        return _impl->fail(exception.to_status());
    }
}

} // namespace doris::segment_v2
