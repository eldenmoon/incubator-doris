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

#include "storage/segment/variant/nested_group_routing_plan.h"

#include "common/config.h"

namespace doris::segment_v2 {

std::string format_nested_group_conflict_paths(const std::vector<std::string>& conflict_paths) {
    std::string paths_str;
    for (const auto& path : conflict_paths) {
        if (!paths_str.empty()) {
            paths_str += ", ";
        }
        paths_str += path;
    }
    return paths_str;
}

Status validate_nested_group_conflicts(const std::vector<std::string>& conflict_paths,
                                       NestedGroupConflictPolicy policy) {
    if (policy == NestedGroupConflictPolicy::ERROR && !conflict_paths.empty()) {
        return Status::InvalidArgument("NestedGroup conflict detected (policy=ERROR) at paths: {}",
                                       format_nested_group_conflict_paths(conflict_paths));
    }
    return Status::OK();
}

NestedGroupConflictPolicy get_nested_group_conflict_policy() {
    if (config::variant_nested_group_discard_scalar_on_conflict) {
        return NestedGroupConflictPolicy::DISCARD_SCALAR;
    }
    return NestedGroupConflictPolicy::ERROR;
}

} // namespace doris::segment_v2
