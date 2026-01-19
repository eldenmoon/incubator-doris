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

#include <algorithm>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "vec/columns/column.h"
#include "vec/data_types/data_type.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

struct NestedGroupReader;

struct NestedGroupPathFilter {
    bool allow_all = false;
    std::unordered_set<std::string> allowed_paths;

    bool empty() const { return !allow_all && allowed_paths.empty(); }

    void set_allow_all() {
        allow_all = true;
        allowed_paths.clear();
    }

    void add_path(std::string path) {
        if (!path.empty()) {
            allowed_paths.emplace(std::move(path));
        }
    }

    bool matches_child(const std::string& name) const {
        // A name matches if:
        // - allow_all is true
        // - name is exactly in allowed_paths
        // - name is a prefix of an allowed path (name="a", allowed="a.b")
        // - an allowed path is a prefix of name (name="a.b.c", allowed="a.b")
        if (allow_all) {
            return true;
        }
        if (allowed_paths.contains(name)) {
            return true;
        }
        std::string prefix = name + ".";
        return std::ranges::any_of(allowed_paths, [&](const auto& path) {
            return path.starts_with(prefix) || name.starts_with(path + ".");
        });
    }

    NestedGroupPathFilter sub_filter(const std::string& prefix) const {
        // Build a child filter relative to the given prefix:
        // - If prefix is explicitly selected, return allow_all (select whole subtree)
        // - Otherwise keep suffixes of allowed paths under "prefix."
        NestedGroupPathFilter sub;
        if (allow_all) {
            sub.allow_all = true;
            return sub;
        }
        std::string prefix_dot = prefix + ".";
        for (const auto& path : allowed_paths) {
            if (path == prefix) {
                sub.set_allow_all();
                return sub;
            }
            if (path.starts_with(prefix_dot)) {
                sub.add_path(path.substr(prefix_dot.size()));
            }
        }
        return sub;
    }
};

/**
 * Iterator for reading NestedGroup element objects as VARIANT.
 *
 * This iterator works at ELEMENT level - each ordinal corresponds to one element
 * in the nested array. The output is one VARIANT object per element. The wrapping
 * NestedGroupIterator handles converting element-level data back to row-level arrays.
 *
 * For example, for data: [{"a":1}, {"a":2}], [{"a":3}]
 * - Element 0: VARIANT {"a":1}
 * - Element 1: VARIANT {"a":2}
 * - Element 2: VARIANT {"a":3}
 */
class NestedGroupWholeIterator : public ColumnIterator {
public:
    /**
     * Construct a NestedGroup element iterator.
     *
     * @param group_reader NestedGroup reader (required)
     */
    explicit NestedGroupWholeIterator(
            const NestedGroupReader* group_reader, std::string pruned_prefix = {},
            std::optional<NestedGroupPathFilter> path_filter = std::nullopt)
            : _group_reader(group_reader),
              _pruned_prefix(std::move(pruned_prefix)),
              _path_filter(std::move(path_filter)) {
        if (!_pruned_prefix.empty()) {
            _pruned_prefix_dot = _pruned_prefix + ".";
        }
    }

    Status init(const ColumnIteratorOptions& opts) override;
    Status seek_to_ordinal(ordinal_t ord_idx) override;
    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;
    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;
    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    // Create a column with the correct result type (VARIANT)
    vectorized::MutableColumnPtr create_result_column() const;

private:
    struct ChildColumnState {
        ColumnIteratorUPtr iter;
        vectorized::DataTypePtr type;
        vectorized::PathInData path;
    };

    struct GroupState;

    struct NestedGroupState {
        std::unique_ptr<GroupState> state;
        vectorized::PathInData path;
    };

    struct GroupState {
        const NestedGroupReader* reader = nullptr; // Reader for this NestedGroup node
        ColumnIteratorUPtr offsets_iter; // Offsets iterator for nested groups under this node
        // Use std::map for deterministic field ordering in JSON output
        std::map<std::string, ChildColumnState> children;      // Child iterators (scalar leaves)
        std::map<std::string, NestedGroupState> nested_groups; // Nested NestedGroups
        std::string pruned_prefix;     // Prefix within this group used for reconstruction
        std::string pruned_prefix_dot; // Cached "pruned_prefix + '.'" for fast checks
        std::optional<NestedGroupPathFilter> path_filter; // Optional field filter for this node
        vectorized::ColumnPtr offsets_buffer;          // Reusable offsets buffer for nested arrays
        vectorized::ColumnPtr element_null_map_buffer; // Reusable element null-map buffer
        vectorized::ColumnPtr array_null_map_buffer;   // Reusable array null-map buffer
    };

    Status _build_group_state(GroupState& state, const NestedGroupReader* reader,
                              std::string pruned_prefix, const NestedGroupPathFilter* path_filter);

    // Read n elements and output n VARIANT objects using columnar data
    Status _read_elements_as_variant(GroupState& state, size_t elem_count,
                                     vectorized::MutableColumnPtr& dst);

    // Build a batch VARIANT column for a group (recursive)
    Status _build_variant_column(GroupState& state, ordinal_t elem_start, size_t elem_count,
                                 vectorized::MutableColumnPtr* out);

    Status _add_child_columns(GroupState& state, size_t elem_count, vectorized::ColumnVariant& out);
    Status _add_nested_group_columns(GroupState& state, ordinal_t elem_start, size_t elem_count,
                                     vectorized::ColumnVariant& out);

    // Build array<variant> column for a nested group (recursive)
    Status _build_nested_array_column(GroupState& nested_state, ordinal_t elem_start,
                                      size_t elem_count, vectorized::MutableColumnPtr* out);

    // Build path with nested flag set on the last part
    static vectorized::PathInData _build_nested_array_path(const std::string& name);

    // Seek child iterators to element ordinal
    Status _seek_child_iters(GroupState& state, uint64_t elem_ord);
    void _reset_reusable_buffers(GroupState& state);

    const NestedGroupReader* _group_reader;
    ColumnIteratorOptions _iter_opts;
    GroupState _root_state;
    ordinal_t _current_ordinal = 0;
    std::string _pruned_prefix;
    std::string _pruned_prefix_dot;
    std::optional<NestedGroupPathFilter> _path_filter;
};

// Iterator for reading a child column from a NestedGroup
// Reads flat child data and reconstructs array semantics using shared offsets
class NestedGroupIterator : public ColumnIterator {
public:
    NestedGroupIterator(ColumnIteratorUPtr offsets_iter, ColumnIteratorUPtr child_iter,
                        vectorized::DataTypePtr result_type)
            : _offsets_iter(std::move(offsets_iter)),
              _child_iter(std::move(child_iter)),
              _result_type(std::move(result_type)) {}

    ~NestedGroupIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override;

    Status seek_to_ordinal(ordinal_t ord_idx) override;

    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override;

    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override;

    ordinal_t get_current_ordinal() const override { return _current_ordinal; }

    // Get the result type for creating the correct column
    const vectorized::DataTypePtr& get_result_type() const { return _result_type; }

    // Create a column with the correct result type
    vectorized::MutableColumnPtr create_result_column() const {
        return _result_type->create_column();
    }

private:
    // Read offsets for the given row range
    Status _read_offsets(ordinal_t start_ordinal, size_t* num_rows,
                         std::vector<uint64_t>* offsets_out);

    ColumnIteratorUPtr _offsets_iter;
    ColumnIteratorUPtr _child_iter;
    vectorized::DataTypePtr _result_type;
    ordinal_t _current_ordinal = 0;
    // Cached flat position (element offset) for the current row ordinal
    // This avoids re-reading the previous row's offset in sequential reads
    uint64_t _current_flat_pos = 0;
};

} // namespace doris::segment_v2
