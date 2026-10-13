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

#include "storage/index/inverted/variant_root_index.h"

#include <fmt/compile.h>
#include <fmt/format.h>

#include "core/value/variant/variant_leaf_visitor.h"
#include "storage/tablet/tablet_schema.h"
namespace doris::segment_v2::variant_root_index {
namespace {
std::string_view trim(std::string_view text) {
    while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
        text.remove_prefix(1);
    }
    while (!text.empty() && (text.back() == ' ' || text.back() == '\t')) {
        text.remove_suffix(1);
    }
    return text;
}
bool has_current_format_version(const std::map<std::string, std::string>& properties) {
    const auto version = properties.find(std::string(VARIANT_ROOT_FORMAT_VERSION_KEY));
    return version != properties.end() && version->second == VARIANT_ROOT_FORMAT_VERSION_CURRENT;
}
bool has_values_scope(const std::map<std::string, std::string>& properties) {
    if (const auto scope = properties.find(std::string(VARIANT_INDEX_SCOPE_KEY));
        scope != properties.end()) {
        return trim(scope->second) == VARIANT_INDEX_SCOPE_VALUES;
    }
    const auto mode = properties.find(std::string(VARIANT_INDEX_MODE_KEY));
    return mode != properties.end() && trim(mode->second) == VARIANT_INDEX_MODE_ALL_VALUES;
}
} // namespace
bool is_root_index(const std::map<std::string, std::string>& properties) {
    return has_values_scope(properties) && has_current_format_version(properties);
}

bool is_root_index(const TabletIndex& index) {
    return index.is_inverted_index() && is_root_index(index.properties());
}

InvertedIndexReaderType reader_type(const TabletIndex&) {
    return InvertedIndexReaderType::FULLTEXT;
}
bool canonical_leaf_text(const VariantLeaf& leaf, std::string* text) {
    DORIS_CHECK(text != nullptr);
    switch (leaf.kind) {
    case VariantLeafKind::STRING:
        text->assign(leaf.string_value.data, leaf.string_value.size);
        return true;
    case VariantLeafKind::INT64:
        *text = fmt::format(FMT_COMPILE("{}"), leaf.int64_value);
        return true;
    case VariantLeafKind::UINT64:
        *text = fmt::format(FMT_COMPILE("{}"), leaf.uint64_value);
        return true;
    case VariantLeafKind::DOUBLE:
        *text = fmt::format(FMT_COMPILE("{}"), leaf.double_value);
        return true;
    case VariantLeafKind::BOOL:
        *text = leaf.bool_value ? "true" : "false";
        return true;
    case VariantLeafKind::OTHER:
        return false;
    }
    return false;
}

} // namespace doris::segment_v2::variant_root_index
