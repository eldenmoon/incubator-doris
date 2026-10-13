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

#include <map>
#include <string>
#include <string_view>

#include "storage/index/inverted/inverted_index_query_type.h"
namespace doris {
class TabletIndex;
struct VariantLeaf;
namespace segment_v2::variant_root_index {
// One docs-only token index per VARIANT column. Each row is one document; recursively
// analyze final logical leaves independently. Keys and unsupported primitives contribute
// no tokens. SQL NULL is separate from empty documents. Only direct root MATCH_ANY/ALL
// can select this reader; subpaths, equality, SEARCH, phrases and scores cannot.
inline constexpr std::string_view VARIANT_INDEX_SCOPE_KEY = "variant_index_scope";
inline constexpr std::string_view VARIANT_INDEX_SCOPE_VALUES = "values";
inline constexpr std::string_view VARIANT_INDEX_MODE_KEY = "variant_index_mode";
inline constexpr std::string_view VARIANT_INDEX_MODE_ALL_VALUES = "all_values";
inline constexpr std::string_view VARIANT_ROOT_FORMAT_VERSION_KEY = "variant_root_format_version";
// TOKEN bytes and canonical text retain format 4. Removed exact terms are never queried.
inline constexpr std::string_view VARIANT_ROOT_FORMAT_VERSION_CURRENT = "4";
bool is_root_index(const std::map<std::string, std::string>& properties);
bool is_root_index(const TabletIndex& index);
InvertedIndexReaderType reader_type(const TabletIndex& index);
// Shared by the import/rebuild writer and scalar MATCH.
bool canonical_leaf_text(const VariantLeaf& leaf, std::string* text);
} // namespace segment_v2::variant_root_index
} // namespace doris
