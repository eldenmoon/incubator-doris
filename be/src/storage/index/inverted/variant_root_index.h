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

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/data_type/primitive_type.h"

namespace doris {

class Field;
class TabletIndex;
struct VariantLeaf;
struct VariantRef;

namespace segment_v2::variant_root_index {

inline constexpr std::string_view VARIANT_INDEX_MODE_KEY = "variant_index_mode";
inline constexpr std::string_view VARIANT_INDEX_MODE_ROOT = "root";
inline constexpr std::string_view VARIANT_INDEX_MODE_ALL_VALUES = "all_values";
inline constexpr std::string_view VARIANT_ROOT_FORMAT_VERSION_KEY = "variant_root_format_version";
inline constexpr std::string_view VARIANT_ROOT_FORMAT_VERSION_V1 = "1";
// AllValues v2 indexes recursive scalar leaves inside arrays. Root retains v1.
inline constexpr std::string_view VARIANT_ROOT_FORMAT_VERSION_V2 = "2";
inline constexpr std::string_view VARIANT_ROOT_QUERY_PATH_KEY = "variant_root_query_path";
inline constexpr std::string_view VARIANT_ROOT_QUERY_VALUE_FAMILY_KEY =
        "variant_root_query_value_family";
// Present when the bound path is a dynamic (VARIANT typed) sub-document: the predicate then
// applies to every scalar leaf at or below the path, and an empty path is the whole document.
inline constexpr std::string_view VARIANT_ROOT_QUERY_SUBTREE_KEY = "variant_root_query_subtree";

bool is_root_mode_properties(const std::map<std::string, std::string>& properties);
bool is_path_root_mode_properties(const std::map<std::string, std::string>& properties);
bool is_all_values_mode_properties(const std::map<std::string, std::string>& properties);
bool is_root_index(const TabletIndex& index);
bool is_all_values_index(const TabletIndex& index);
std::string_view query_value_family(PrimitiveType type);

// Term layout (see variant_term_codec.h): [tag][value][sep][path]. Both index modes share it:
//   - a Root index stores every scalar leaf as term(value, path);
//   - an AllValues index stores the same leaves as term(value, "") -- the root prefix, i.e. the
//     path-independent "values" namespace. Equality against any path is one exact term lookup.
// Values are typed: the string "3" and the number 3 are different terms, integral doubles fold
// into INT64 / UINT64, -0.0 folds into 0.0, and NaN has no term.
std::string encode_int64_term(std::string_view path, int64_t value);
std::string encode_uint64_term(std::string_view path, uint64_t value);
std::string encode_double_term(std::string_view path, double value);
std::string encode_bool_term(std::string_view path, bool value);
std::string encode_string_term(std::string_view path, std::string_view value);
std::string encode_token_term(std::string_view path, std::string_view value);
// AllValues terms are path-less terms of the same layout.
std::string encode_all_value_term(std::string_view value);
std::string encode_all_value_token_term(std::string_view value);

// Appends the exact equality term for one Variant scalar leaf (none for containers, JSON null,
// NaN, decimals, temporal, binary and UUID values). Path "" yields the AllValues term.
Status append_variant_value_terms(std::string_view path, const VariantRef& value,
                                  std::vector<std::string>* terms);
// Same, for an already classified leaf.
void append_variant_leaf_terms(std::string_view path, const VariantLeaf& leaf,
                               std::vector<std::string>* terms);

// Encodes one scalar predicate value into the same exact-term domain used by the writer. A
// successful call with an empty result means the type is intentionally unsupported by the root
// index and the caller must fall back to scalar evaluation. Path "" targets the AllValues terms.
Status encode_query_value_terms(std::string_view path, const Field& value,
                                std::vector<std::string>* terms);

// AllValues equality: typed like encode_query_value_terms(""). Strings longer than ignore_above
// and unsupported types return INVERTED_INDEX_EVALUATE_SKIPPED so the caller falls back to scalar
// evaluation rather than returning an empty (wrong) result.
Status encode_all_values_query_value_terms(const Field& value, std::vector<std::string>* terms,
                                           size_t ignore_above = std::string::npos);

std::shared_ptr<TabletIndex> make_query_index(const TabletIndex& root_index,
                                              std::string_view relative_path,
                                              PrimitiveType path_type);

} // namespace segment_v2::variant_root_index
} // namespace doris
