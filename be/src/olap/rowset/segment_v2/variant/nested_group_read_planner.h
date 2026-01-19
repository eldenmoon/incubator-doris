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

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "nested_group_iterator.h"
// Use forward declarations to minimize coupling.

namespace doris {
class StorageReadOptions;
class TabletColumn;
class TabletSchema;
} // namespace doris

namespace doris::segment_v2 {

struct NestedGroupReader;
struct NestedGroupPathFilter;

class NestedGroupReadPlanner {
public:
    enum class ReadKind { NESTED_GROUP_WHOLE, NESTED_GROUP_CHILD };

    struct Plan {
        ReadKind kind {ReadKind::NESTED_GROUP_WHOLE};
        vectorized::DataTypePtr type;
        vectorized::PathInData relative_path;
        std::string nested_child_path;
        std::string nested_group_pruned_path;
        std::vector<const NestedGroupReader*> nested_group_chain;
        std::optional<NestedGroupPathFilter> nested_group_path_filter;
    };

    using PathFilterBuilder = std::function<std::optional<NestedGroupPathFilter>(
            const doris::StorageReadOptions* opt, int32_t col_uid, const std::string& root_name,
            const std::vector<const NestedGroupReader*>& chain)>;

    NestedGroupReadPlanner(
            const doris::TabletSchema* tablet_schema,
            std::function<const NestedGroupReader*(const std::string&)> root_reader_fn,
            std::function<std::tuple<bool, std::vector<const NestedGroupReader*>, std::string>(
                    const std::string&)>
                    chain_fn,
            PathFilterBuilder filter_builder);

    bool build_plan(Plan* plan, const doris::TabletColumn& target_col,
                    const doris::StorageReadOptions* opt, int32_t col_uid,
                    const vectorized::PathInData& relative_path) const;

private:
    const doris::TabletSchema* _tablet_schema;
    std::function<const NestedGroupReader*(const std::string&)> _root_reader_fn;
    std::function<std::tuple<bool, std::vector<const NestedGroupReader*>, std::string>(
            const std::string&)>
            _chain_fn;
    PathFilterBuilder _filter_builder;
};

} // namespace doris::segment_v2
