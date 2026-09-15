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

namespace segment_v2::variant_root_index {

// The VARIANT values index: one logical SNII index per analyzer on a VARIANT column that stores
// every scalar leaf of every document as a path-less, typed term (see variant_term_codec.h, the
// terms are the codec's root prefixes: [tag][value]). Arrays and objects are recursed, JSON null
// produces no term, and index docid == segment rowid (SQL NULL rows are index nulls).
//
// What it answers:
//   - whole-document search `v MATCH_ANY / MATCH_ALL 'q'` exactly: on an analyzed index every
//     token of every leaf (numbers and booleans by their canonical text) and MATCH_ALL is
//     satisfied across leaves; on an exact index a leaf whose canonical text equals the query;
//   - path predicates (`v['a'] = 'x'`, IN, `v['a'] MATCH 'x'`) as CANDIDATES: the value exists
//     somewhere in the row; the iterator marks the result requires_recheck and the scalar
//     expression keeps the exact SQL semantics. Candidate sets are supersets by construction:
//     string literals are expanded to the typed terms whose canonical text they spell, a leaf
//     the codec cannot spell (decimal, temporal, binary, ...) leaves one OTHER marker term per
//     document that every equality over the binary storage probes, and predicates whose
//     superset cannot be derived (numeric casts over untyped paths, whole-document equality
//     `CAST(v AS STRING) = 'x'`, NOT, IS NULL) are left to scalar evaluation.
//
// Properties: `variant_index_scope = values` (the legacy spelling `variant_index_mode =
// all_values` is normalized by the FE); `variant_root_format_version` is stamped by the FE and
// only the version this BE writes is usable, any other version makes the index invisible to
// writers and readers instead of being misread.
inline constexpr std::string_view VARIANT_INDEX_SCOPE_KEY = "variant_index_scope";
inline constexpr std::string_view VARIANT_INDEX_SCOPE_VALUES = "values";
inline constexpr std::string_view VARIANT_INDEX_MODE_KEY = "variant_index_mode";
inline constexpr std::string_view VARIANT_INDEX_MODE_ALL_VALUES = "all_values";
inline constexpr std::string_view VARIANT_ROOT_FORMAT_VERSION_KEY = "variant_root_format_version";
// Value-first layout [tag][value] with typed, order-preserving values.
inline constexpr std::string_view VARIANT_ROOT_FORMAT_VERSION_V3 = "3";
inline constexpr std::string_view VARIANT_ROOT_FORMAT_VERSION_CURRENT =
        VARIANT_ROOT_FORMAT_VERSION_V3;
// Query-time bindings added by make_query_index(): the relative path a sub-column predicate is
// bound to (empty for the whole document) and the value family of that path when it is a typed
// materialized sub-column ("string" / "boolean" / "integral" / "float" / "double"; absent for a
// path read from the binary storage, whose type is only known after the cast).
inline constexpr std::string_view VARIANT_ROOT_QUERY_PATH_KEY = "variant_root_query_path";
inline constexpr std::string_view VARIANT_ROOT_QUERY_VALUE_FAMILY_KEY =
        "variant_root_query_value_family";

// True when the properties describe a usable values index (scope / mode and current version).
bool is_root_mode_properties(const std::map<std::string, std::string>& properties);
bool is_root_index(const TabletIndex& index);
std::string_view query_value_family(PrimitiveType type);

// Typed value terms. Values are typed: the string "3" and the number 3 are different terms,
// integral doubles fold into INT64 / UINT64, -0.0 folds into 0.0 and NaN has no term.
std::string encode_int64_term(int64_t value);
std::string encode_uint64_term(uint64_t value);
std::string encode_double_term(double value);
std::string encode_bool_term(bool value);
std::string encode_string_term(std::string_view value);
std::string encode_token_term(std::string_view value);
// The marker term of a non-null scalar leaf that has no value term (decimal, temporal, binary,
// UUID, NaN); the exact index writes it once per document.
std::string encode_other_term();

// Appends the exact term of one classified scalar leaf (none for OTHER: containers, JSON null,
// NaN, decimals, temporal, binary and UUID values).
void append_variant_leaf_terms(const VariantLeaf& leaf, std::vector<std::string>* terms);

// The canonical text of a scalar leaf, shared by the analyzed index writer and the scalar MATCH
// fallback so both tokenize the same bytes: strings as they are, integers in decimal, doubles in
// the JSON shortest form, booleans as true / false. False for OTHER leaves.
bool canonical_leaf_text(const VariantLeaf& leaf, std::string* text);

// Terms a literal must probe on an exact values index so that every leaf whose canonical text
// equals `text` is found: the STRING term plus the INT64 / UINT64 / DOUBLE term when `text`
// spells that number canonically and the BOOL term for true / false. With `sql_cast_text` the
// literal is compared against CAST(leaf AS STRING), which prints booleans as 1 / 0 and negative
// zero as -0, so those spellings also probe the boolean and zero terms.
void append_string_literal_terms(std::string_view text, bool sql_cast_text,
                                 std::vector<std::string>* terms);

// Encodes one typed predicate literal into the writer's term domain. A successful call with an
// empty result means the literal's type has no term and the caller must fall back to scalar
// evaluation.
Status encode_query_value_terms(const Field& value, std::vector<std::string>* terms);

std::shared_ptr<TabletIndex> make_query_index(const TabletIndex& root_index,
                                              std::string_view relative_path,
                                              PrimitiveType path_type);

} // namespace segment_v2::variant_root_index
} // namespace doris
