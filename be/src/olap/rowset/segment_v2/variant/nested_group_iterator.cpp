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

#include "nested_group_iterator.h"

#include <optional>
#include <string_view>

#include "olap/rowset/segment_v2/variant/offset_manager.h"
#include "olap/rowset/segment_v2/variant/variant_column_reader.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_variant.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_array.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

namespace {

struct ArrayReadTarget {
    vectorized::ColumnNullable* nullable = nullptr;
    vectorized::ColumnArray* array = nullptr;
};

bool should_include_field(std::string_view name, std::string_view pruned_prefix_dot,
                          const NestedGroupPathFilter* filter) {
    if (!pruned_prefix_dot.empty() &&
        name.substr(0, pruned_prefix_dot.size()) != pruned_prefix_dot) {
        return false;
    }
    return filter == nullptr || filter->matches_child(std::string(name));
}

std::optional<std::string> maybe_rebase_name(std::string_view name,
                                             std::string_view pruned_prefix_dot) {
    if (pruned_prefix_dot.empty()) {
        return std::string(name);
    }
    std::string rebased;
    if (!vectorized::PathInData::try_strip_prefix(std::string(name), std::string(pruned_prefix_dot),
                                                  &rebased)) {
        return std::nullopt;
    }
    return rebased;
}

ArrayReadTarget get_array_read_target(vectorized::MutableColumnPtr& dst) {
    ArrayReadTarget target;
    if (dst->is_nullable()) {
        target.nullable = assert_cast<vectorized::ColumnNullable*>(dst.get());
        target.array = assert_cast<vectorized::ColumnArray*>(&target.nullable->get_nested_column());
        return target;
    }
    target.array = assert_cast<vectorized::ColumnArray*>(dst.get());
    return target;
}

void append_non_null_array_rows(const ArrayReadTarget& target, size_t num_rows) {
    if (target.nullable) {
        target.nullable->get_null_map_data().resize_fill(
                target.nullable->get_null_map_data().size() + num_rows, 0);
    }
}

void append_array_offsets_from_flat(const std::vector<uint64_t>& flat_offsets, uint64_t start_flat,
                                    size_t expected_rows, bool fill_when_empty,
                                    vectorized::ColumnArray::Offsets64* dst_offsets) {
    DCHECK(dst_offsets != nullptr);
    uint64_t prev_flat = start_flat;
    size_t prev_dst_offset = dst_offsets->empty() ? 0 : dst_offsets->back();

    if (fill_when_empty && flat_offsets.empty() && expected_rows > 0) {
        for (size_t i = 0; i < expected_rows; ++i) {
            dst_offsets->push_back(prev_dst_offset);
        }
        return;
    }

    for (uint64_t curr_flat : flat_offsets) {
        prev_dst_offset += static_cast<size_t>(curr_flat - prev_flat);
        dst_offsets->push_back(prev_dst_offset);
        prev_flat = curr_flat;
    }
}

Status read_child_elements(ColumnIterator* child_iter, bool seek_first, uint64_t start_flat,
                           size_t expected_num_elements, vectorized::ColumnArray* array) {
    if (expected_num_elements == 0) {
        return Status::OK();
    }
    DCHECK(child_iter != nullptr);
    DCHECK(array != nullptr);

    if (seek_first) {
        RETURN_IF_ERROR(child_iter->seek_to_ordinal(start_flat));
    }

    bool child_has_null = false;
    size_t actual_num_elements = expected_num_elements;
    auto nested_dst = array->get_data_ptr()->assume_mutable();
    RETURN_IF_ERROR(child_iter->next_batch(&actual_num_elements, nested_dst, &child_has_null));
    array->get_data_ptr() = std::move(nested_dst);
    if (UNLIKELY(actual_num_elements != expected_num_elements)) {
        return Status::InternalError(
                "NestedGroupIterator child_iter returned {} elements, expected {}",
                actual_num_elements, expected_num_elements);
    }

    return Status::OK();
}

} // namespace

#include "common/compile_check_begin.h"

// ============================================================================
// NestedGroupWholeIterator implementation
// ============================================================================

Status NestedGroupWholeIterator::init(const ColumnIteratorOptions& opts) {
    _iter_opts = opts;
    DCHECK(_group_reader && _group_reader->is_valid());
    const NestedGroupPathFilter* filter_ptr =
            (_path_filter && !_path_filter->empty()) ? &*_path_filter : nullptr;
    RETURN_IF_ERROR(_build_group_state(_root_state, _group_reader, _pruned_prefix, filter_ptr));
    return Status::OK();
}

Status NestedGroupWholeIterator::seek_to_ordinal(ordinal_t ord_idx) {
    _current_ordinal = ord_idx;
    return Status::OK();
}

Status NestedGroupWholeIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                            bool* has_null) {
    RETURN_IF_ERROR(_read_elements_as_variant(_root_state, *n, dst));
    _current_ordinal += *n;
    *has_null = false;
    return Status::OK();
}

Status NestedGroupWholeIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                                vectorized::MutableColumnPtr& dst) {
    // Batch consecutive rowids to reduce seek overhead
    bool has_null = false;
    size_t i = 0;
    while (i < count) {
        rowid_t start_rowid = rowids[i];
        size_t run_len = 1;
        while (i + run_len < count && rowids[i + run_len] == start_rowid + run_len) {
            ++run_len;
        }
        RETURN_IF_ERROR(seek_to_ordinal(start_rowid));
        RETURN_IF_ERROR(next_batch(&run_len, dst, &has_null));
        i += run_len;
    }
    return Status::OK();
}

vectorized::MutableColumnPtr NestedGroupWholeIterator::create_result_column() const {
    // NestedGroupWholeIterator outputs VARIANT per element
    return vectorized::ColumnVariant::create(0);
}

Status NestedGroupWholeIterator::_build_group_state(GroupState& state,
                                                    const NestedGroupReader* reader,
                                                    std::string pruned_prefix,
                                                    const NestedGroupPathFilter* path_filter) {
    state.reader = reader;
    state.pruned_prefix = std::move(pruned_prefix);
    state.pruned_prefix_dot.clear();
    if (!state.pruned_prefix.empty()) {
        state.pruned_prefix_dot = state.pruned_prefix + ".";
    }
    state.path_filter.reset();
    const NestedGroupPathFilter* active_filter = nullptr;
    if (path_filter && !path_filter->empty() && !path_filter->allow_all) {
        state.path_filter = *path_filter;
        active_filter = &state.path_filter.value();
    }
    // Initialize offsets iterator for nested groups (used when serializing nested arrays)
    RETURN_IF_ERROR(reader->offsets_reader->new_iterator(&state.offsets_iter, nullptr));
    RETURN_IF_ERROR(state.offsets_iter->init(_iter_opts));
    // Initialize child column iterators (scalar fields)
    for (const auto& [name, cr] : reader->child_readers) {
        if (!should_include_field(name, state.pruned_prefix_dot, active_filter)) {
            continue;
        }
        ColumnIteratorUPtr it;
        RETURN_IF_ERROR(cr->new_iterator(&it, nullptr));
        RETURN_IF_ERROR(it->init(_iter_opts));
        ChildColumnState child_state;
        child_state.iter = std::move(it);
        child_state.type = cr->get_vec_data_type();
        child_state.path = vectorized::PathInData(name);
        state.children.emplace(name, std::move(child_state));
    }
    // Initialize nested group states (for nested arrays within elements)
    for (const auto& [name, nested] : reader->nested_group_readers) {
        if (!should_include_field(name, state.pruned_prefix_dot, active_filter)) {
            continue;
        }
        auto st = std::make_unique<GroupState>();
        const NestedGroupPathFilter* sub_filter_ptr = nullptr;
        NestedGroupPathFilter sub_filter;
        if (active_filter) {
            sub_filter = active_filter->sub_filter(name);
            if (!sub_filter.empty()) {
                sub_filter_ptr = &sub_filter;
            }
        }
        RETURN_IF_ERROR(_build_group_state(*st, nested.get(), {}, sub_filter_ptr));
        NestedGroupState nested_state;
        nested_state.state = std::move(st);
        nested_state.path = _build_nested_array_path(name);
        state.nested_groups.emplace(name, std::move(nested_state));
    }
    return Status::OK();
}

Status NestedGroupWholeIterator::_read_elements_as_variant(GroupState& state, size_t elem_count,
                                                           vectorized::MutableColumnPtr& dst) {
    if (elem_count == 0) {
        return Status::OK();
    }

    vectorized::MutableColumnPtr batch_variant;
    RETURN_IF_ERROR(_build_variant_column(state, _current_ordinal, elem_count, &batch_variant));

    if (dst->size() == 0) {
        if (dst->is_nullable()) {
            auto* dst_nullable =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(dst.get());
            auto* nested_variant =
                    dst_nullable ? vectorized::check_and_get_column<vectorized::ColumnVariant>(
                                           &dst_nullable->get_nested_column())
                                 : nullptr;
            if (dst_nullable && nested_variant && dst_nullable->get_null_map_data().empty()) {
                vectorized::ColumnPtr nested_ptr = std::move(batch_variant);
                dst_nullable->change_nested_column(nested_ptr);
                dst_nullable->get_null_map_data().resize_fill(elem_count, 0);
                _reset_reusable_buffers(_root_state);
                return Status::OK();
            }
            auto null_map = vectorized::ColumnUInt8::create(elem_count, 0);
            dst = vectorized::ColumnNullable::create(std::move(batch_variant), std::move(null_map));
            _reset_reusable_buffers(_root_state);
        } else {
            dst = std::move(batch_variant);
            _reset_reusable_buffers(_root_state);
        }
        return Status::OK();
    }

    auto* dst_nullable = vectorized::check_and_get_column<vectorized::ColumnNullable>(dst.get());
    auto& dst_variant =
            dst_nullable
                    ? assert_cast<vectorized::ColumnVariant&>(dst_nullable->get_nested_column())
                    : assert_cast<vectorized::ColumnVariant&>(*dst);

    dst_variant.insert_range_from(*batch_variant, 0, batch_variant->size());
    if (dst_nullable) {
        dst_nullable->get_null_map_data().resize_fill(
                dst_nullable->get_null_map_data().size() + elem_count, 0);
    }

    return Status::OK();
}

Status NestedGroupWholeIterator::_build_variant_column(GroupState& state, ordinal_t elem_start,
                                                       size_t elem_count,
                                                       vectorized::MutableColumnPtr* out) {
    auto variant_col = vectorized::ColumnVariant::create(0, elem_count);
    auto& variant = assert_cast<vectorized::ColumnVariant&>(*variant_col);

    if (elem_count > 0) {
        RETURN_IF_ERROR(_seek_child_iters(state, elem_start));
    }
    RETURN_IF_ERROR(_add_child_columns(state, elem_count, variant));
    RETURN_IF_ERROR(_add_nested_group_columns(state, elem_start, elem_count, variant));

    *out = std::move(variant_col);
    return Status::OK();
}

Status NestedGroupWholeIterator::_add_child_columns(GroupState& state, size_t elem_count,
                                                    vectorized::ColumnVariant& out) {
    for (auto& [name, child] : state.children) {
        const auto& type = child.type;
        const auto& path = child.path;

        auto col = type->create_column();
        if (elem_count > 0) {
            size_t to_read = elem_count;
            bool child_has_null = false;
            RETURN_IF_ERROR(child.iter->next_batch(&to_read, col, &child_has_null));
        }

        const auto rebased = maybe_rebase_name(name, state.pruned_prefix_dot);
        if (!rebased.has_value()) {
            continue;
        }
        const vectorized::PathInData output_path =
                state.pruned_prefix_dot.empty() ? path : vectorized::PathInData(*rebased);
        if (!out.add_sub_column(output_path, std::move(col), type)) {
            return Status::InternalError("Failed to add subcolumn {}", name);
        }
    }
    return Status::OK();
}

Status NestedGroupWholeIterator::_add_nested_group_columns(GroupState& state, ordinal_t elem_start,
                                                           size_t elem_count,
                                                           vectorized::ColumnVariant& out) {
    for (auto& [name, nested_state] : state.nested_groups) {
        vectorized::MutableColumnPtr nested_array;
        RETURN_IF_ERROR(_build_nested_array_column(*nested_state.state, elem_start, elem_count,
                                                   &nested_array));
        const auto rebased = maybe_rebase_name(name, state.pruned_prefix_dot);
        if (!rebased.has_value()) {
            continue;
        }
        const vectorized::PathInData output_path = state.pruned_prefix_dot.empty()
                                                           ? nested_state.path
                                                           : _build_nested_array_path(*rebased);
        if (!out.add_sub_column(output_path, std::move(nested_array),
                                vectorized::ColumnVariant::NESTED_TYPE)) {
            return Status::InternalError("Failed to add nested subcolumn {}", name);
        }
    }
    return Status::OK();
}

Status NestedGroupWholeIterator::_build_nested_array_column(GroupState& nested_state,
                                                            ordinal_t elem_start, size_t elem_count,
                                                            vectorized::MutableColumnPtr* out) {
    if (elem_count == 0) {
        auto nested_variant = vectorized::ColumnVariant::create(0);
        auto element_nullable = vectorized::ColumnNullable::create(
                std::move(nested_variant), vectorized::ColumnUInt8::create());
        auto offsets_col = vectorized::ColumnArray::ColumnOffsets::create();
        auto array_col = vectorized::ColumnArray::create(std::move(element_nullable),
                                                         std::move(offsets_col));
        auto array_null_map = vectorized::ColumnUInt8::create(0);
        *out = vectorized::ColumnNullable::create(std::move(array_col), std::move(array_null_map));
        return Status::OK();
    }

    uint64_t prev_off = 0;
    vectorized::MutableColumnPtr offsets_col;
    if (nested_state.offsets_buffer) {
        offsets_col = nested_state.offsets_buffer->assume_mutable();
    } else {
        offsets_col = vectorized::ColumnOffset64::create();
    }
    RETURN_IF_ERROR(OffsetManager::read_offsets_with_prev(
            nested_state.offsets_iter.get(), elem_start, elem_count, &prev_off, &offsets_col));
    nested_state.offsets_buffer = offsets_col->get_ptr();

    auto* offsets_data_col = assert_cast<vectorized::ColumnOffset64*>(offsets_col.get());
    auto& offsets_data = offsets_data_col->get_data();
    uint64_t total_children = offsets_data.empty() ? 0 : (offsets_data.back() - prev_off);
    vectorized::MutableColumnPtr nested_variant;
    if (total_children > 0) {
        RETURN_IF_ERROR(
                _build_variant_column(nested_state, prev_off, total_children, &nested_variant));
    } else {
        nested_variant = vectorized::ColumnVariant::create(0);
    }

    vectorized::MutableColumnPtr element_null_map;
    if (nested_state.element_null_map_buffer) {
        element_null_map = nested_state.element_null_map_buffer->assume_mutable();
    } else {
        element_null_map = vectorized::ColumnUInt8::create();
    }
    auto* element_null_map_col = assert_cast<vectorized::ColumnUInt8*>(element_null_map.get());
    element_null_map_col->get_data().resize_fill(nested_variant->size(), 0);
    nested_state.element_null_map_buffer = element_null_map->get_ptr();
    auto element_nullable = vectorized::ColumnNullable::create(std::move(nested_variant),
                                                               std::move(element_null_map));

    for (auto& off : offsets_data) {
        off -= prev_off;
    }

    auto array_col =
            vectorized::ColumnArray::create(std::move(element_nullable), std::move(offsets_col));
    vectorized::MutableColumnPtr array_null_map;
    if (nested_state.array_null_map_buffer) {
        array_null_map = nested_state.array_null_map_buffer->assume_mutable();
    } else {
        array_null_map = vectorized::ColumnUInt8::create();
    }
    auto* array_null_map_col = assert_cast<vectorized::ColumnUInt8*>(array_null_map.get());
    array_null_map_col->get_data().resize_fill(elem_count, 0);
    nested_state.array_null_map_buffer = array_null_map->get_ptr();
    *out = vectorized::ColumnNullable::create(std::move(array_col), std::move(array_null_map));
    return Status::OK();
}

Status NestedGroupWholeIterator::_seek_child_iters(GroupState& state, uint64_t start_off) {
    for (auto& [_, child] : state.children) {
        RETURN_IF_ERROR(child.iter->seek_to_ordinal(start_off));
    }
    return Status::OK();
}

vectorized::PathInData NestedGroupWholeIterator::_build_nested_array_path(const std::string& name) {
    std::vector<std::string_view> parts;
    parts.reserve(4);

    size_t pos = 0;
    while (pos < name.size()) {
        size_t dot = name.find('.', pos);
        if (dot == std::string::npos) {
            dot = name.size();
        }
        parts.emplace_back(name.data() + pos, dot - pos);
        pos = dot + 1;
    }

    vectorized::PathInData::Parts path_parts;
    path_parts.reserve(parts.size());
    for (size_t i = 0; i < parts.size(); ++i) {
        bool is_nested = (i + 1 == parts.size());
        path_parts.emplace_back(parts[i], is_nested, 0);
    }
    return vectorized::PathInData(path_parts);
}

// ============================================================================
// NestedGroupIterator implementation
// ============================================================================

Status NestedGroupIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(_offsets_iter->init(opts));
    RETURN_IF_ERROR(_child_iter->init(opts));
    return Status::OK();
}

Status NestedGroupIterator::seek_to_ordinal(ordinal_t ord_idx) {
    _current_ordinal = ord_idx;

    // Compute and cache the flat position (element offset) for this row ordinal
    if (ord_idx > 0) {
        // Read offset at ord_idx-1 to get the flat start position
        uint64_t prev_off = 0;
        std::vector<uint64_t> curr_off;
        RETURN_IF_ERROR(OffsetManager::read_offsets_with_prev(_offsets_iter.get(), ord_idx, 1,
                                                              &prev_off, &curr_off));
        _current_flat_pos = prev_off;
    } else {
        _current_flat_pos = 0;
    }

    // Seek child iterator to the flat position
    RETURN_IF_ERROR(_child_iter->seek_to_ordinal(_current_flat_pos));
    return Status::OK();
}

Status NestedGroupIterator::next_batch(size_t* n, vectorized::MutableColumnPtr& dst,
                                       bool* has_null) {
    ArrayReadTarget target = get_array_read_target(dst);

    // Read offsets for these rows
    std::vector<uint64_t> offsets;
    size_t num_rows = *n;
    RETURN_IF_ERROR(_read_offsets(_current_ordinal, &num_rows, &offsets));
    *n = num_rows;
    if (*n == 0) {
        *has_null = false;
        return Status::OK();
    }

    // Use cached flat position as start offset (no extra read needed)
    size_t start_offset = _current_flat_pos;
    size_t end_offset = offsets.empty() ? start_offset : offsets.back();
    size_t num_elements = end_offset - start_offset;

    RETURN_IF_ERROR(read_child_elements(_child_iter.get(), /*seek_first=*/false, start_offset,
                                        num_elements, target.array));

    // Convert offsets to array offsets (relative to start)
    auto& dst_offsets = target.array->get_offsets();
    append_array_offsets_from_flat(offsets, start_offset, *n, /*fill_when_empty=*/false,
                                   &dst_offsets);

    // All reconstructed arrays are present (non-null) for these rows.
    append_non_null_array_rows(target, *n);

    _current_ordinal += *n;
    // Update cached flat position for next sequential read
    _current_flat_pos = end_offset;
    *has_null = false;
    return Status::OK();
}

Status NestedGroupIterator::read_by_rowids(const rowid_t* rowids, const size_t count,
                                           vectorized::MutableColumnPtr& dst) {
    // Batch consecutive rowids together to reduce seek overhead
    // For each batch, we can read offsets and child elements in one go

    if (count == 0) {
        return Status::OK();
    }

    ArrayReadTarget target = get_array_read_target(dst);
    auto& dst_offsets = target.array->get_offsets();

    size_t i = 0;
    while (i < count) {
        // Find run of consecutive rowids starting at i
        rowid_t start_rowid = rowids[i];
        size_t run_len = 1;
        while (i + run_len < count &&
               rowids[i + run_len] == start_rowid + static_cast<rowid_t>(run_len)) {
            ++run_len;
        }

        // Read offsets for this consecutive batch using read_offsets_with_prev
        uint64_t batch_start_flat = 0;
        std::vector<uint64_t> offsets_data;
        RETURN_IF_ERROR(OffsetManager::read_offsets_with_prev(
                _offsets_iter.get(), start_rowid, run_len, &batch_start_flat, &offsets_data));

        uint64_t batch_end_flat = offsets_data.empty() ? batch_start_flat : offsets_data.back();
        size_t total_elements = batch_end_flat - batch_start_flat;

        // Read all child elements for this consecutive batch in one go.
        RETURN_IF_ERROR(read_child_elements(_child_iter.get(), /*seek_first=*/true,
                                            batch_start_flat, total_elements, target.array));

        append_array_offsets_from_flat(offsets_data, batch_start_flat, run_len,
                                       /*fill_when_empty=*/true, &dst_offsets);
        append_non_null_array_rows(target, run_len);

        i += run_len;
    }

    return Status::OK();
}

Status NestedGroupIterator::_read_offsets(ordinal_t start_ordinal, size_t* num_rows,
                                          std::vector<uint64_t>* offsets_out) {
    RETURN_IF_ERROR(_offsets_iter->seek_to_ordinal(start_ordinal));

    vectorized::MutableColumnPtr offset_col = vectorized::ColumnOffset64::create();
    bool has_null = false;
    RETURN_IF_ERROR(_offsets_iter->next_batch(num_rows, offset_col, &has_null));

    auto* offset_data = assert_cast<vectorized::ColumnOffset64*>(offset_col.get());
    offsets_out->assign(offset_data->get_data().begin(), offset_data->get_data().end());

    return Status::OK();
}

void NestedGroupWholeIterator::_reset_reusable_buffers(GroupState& state) {
    state.offsets_buffer.reset();
    state.element_null_map_buffer.reset();
    state.array_null_map_buffer.reset();
    for (auto& [_, nested_state] : state.nested_groups) {
        _reset_reusable_buffers(*nested_state.state);
    }
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
