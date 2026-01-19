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

#include "nested_group_read_planner.h"

#include "nested_group_path.h"
#include "olap/tablet_schema.h"
#include "variant_column_reader.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_variant.h"

namespace doris::segment_v2 {

namespace {

vectorized::DataTypePtr compute_nested_array_type(vectorized::DataTypePtr base_type,
                                                  size_t wrap_count) {
    vectorized::DataTypePtr current_type = std::move(base_type);
    for (size_t i = 0; i < wrap_count; ++i) {
        current_type = std::make_shared<vectorized::DataTypeArray>(current_type);
    }
    return current_type;
}

} // namespace

NestedGroupReadPlanner::NestedGroupReadPlanner(
        const doris::TabletSchema* tablet_schema,
        std::function<const NestedGroupReader*(const std::string&)> root_reader_fn,
        std::function<std::tuple<bool, std::vector<const NestedGroupReader*>, std::string>(
                const std::string&)>
                chain_fn,
        PathFilterBuilder filter_builder)
        : _tablet_schema(tablet_schema),
          _root_reader_fn(std::move(root_reader_fn)),
          _chain_fn(std::move(chain_fn)),
          _filter_builder(std::move(filter_builder)) {}

bool NestedGroupReadPlanner::build_plan(Plan* plan, const doris::TabletColumn& target_col,
                                        const doris::StorageReadOptions* opt, int32_t col_uid,
                                        const vectorized::PathInData& relative_path) const {
    if (!plan) {
        return false;
    }
    if (relative_path.empty()) {
        const auto* root_reader = _root_reader_fn(std::string(kRootNestedGroupPath));
        if (!root_reader || !root_reader->is_valid()) {
            return false;
        }
        plan->relative_path = relative_path;
        plan->nested_group_chain = {root_reader};
        auto base_type = std::make_shared<vectorized::DataTypeVariant>(0);
        plan->type = make_nullable(
                compute_nested_array_type(base_type, plan->nested_group_chain.size()));
        plan->kind = ReadKind::NESTED_GROUP_WHOLE;
        if (_tablet_schema) {
            std::string root_name = _tablet_schema->column_by_uid(col_uid).name();
            plan->nested_group_path_filter =
                    _filter_builder(opt, col_uid, root_name, plan->nested_group_chain);
        }
        return true;
    }

    auto [found, group_chain, child_path] = _chain_fn(relative_path.get_path());
    if (!found || group_chain.empty()) {
        return false;
    }
    plan->relative_path = relative_path;
    plan->nested_group_chain = std::move(group_chain);

    const auto* innermost_group = plan->nested_group_chain.back();
    if (!child_path.empty() && innermost_group->child_readers.contains(child_path)) {
        auto child_type = innermost_group->child_readers.at(child_path)->get_vec_data_type();
        plan->kind = ReadKind::NESTED_GROUP_CHILD;
        plan->type = make_nullable(
                compute_nested_array_type(child_type, plan->nested_group_chain.size()));
        plan->nested_child_path = std::move(child_path);
        return true;
    }

    auto base_type = std::make_shared<vectorized::DataTypeVariant>(0);
    plan->type =
            make_nullable(compute_nested_array_type(base_type, plan->nested_group_chain.size()));
    plan->kind = ReadKind::NESTED_GROUP_WHOLE;
    if (!child_path.empty()) {
        plan->nested_group_pruned_path = std::move(child_path);
    }
    if (_tablet_schema) {
        std::string root_name = _tablet_schema->column_by_uid(col_uid).name();
        plan->nested_group_path_filter =
                _filter_builder(opt, col_uid, root_name, plan->nested_group_chain);
    }
    return true;
}

} // namespace doris::segment_v2
