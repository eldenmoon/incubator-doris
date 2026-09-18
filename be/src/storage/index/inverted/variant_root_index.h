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

#include "core/data_type/primitive_type.h"
#include "storage/index/inverted/inverted_index_query_type.h"

namespace doris {

class Field;
class TabletIndex;
struct VariantLeaf;

namespace segment_v2::variant_root_index {

// The VARIANT root index, declared on the VARIANT column itself with
// `variant_index_scope = values` (legacy spelling `variant_index_mode = all_values`). One
// logical SNII index per analyzer stores every scalar leaf of every document as a path-less
// typed term (variant_term_codec.h). It is the single place that decides what is indexed, what
// a query literal probes, and how exact a result is; the reader, the iterator and the scalar
// MATCH fallback consume this contract instead of restating it.
//
// 1. What is indexed (writer, variant_root_index_writer.h)
//    - Objects and arrays are recursed; every scalar leaf is one term wherever it is stored
//      (materialized sub-column, sparse bucket, JSONB sub-document). JSON null, empty objects
//      and empty arrays produce nothing; a SQL NULL row is an index null. docid == segment rowid.
//    - Values are typed (variant_leaf_visitor.h): every integer is INT64, integral doubles and
//      floats fold into INT64 / UINT64, other doubles stay DOUBLE and a FLOAT widens to the
//      DOUBLE it denotes, "3" and 3 are different terms. Strings longer than `ignore_above` are
//      not in the exact index. A scalar the codec cannot spell (decimal, temporal, binary, UUID,
//      NaN, infinities, integers beyond UINT64) leaves one OTHER marker per document in the exact
//      index and nothing in an analyzed index.
//    - Object keys are never indexed.
//    - An exact index (`parser = none`) stores the typed terms; an analyzed index stores the
//      analyzer's tokens of every leaf's canonical text (numbers and booleans included:
//      `{"code": 42}` is found by `v MATCH_ANY '42' USING ANALYZER english`). Neither stores
//      positions or norms: no phrase, no score.
//
// 2. Canonical text (canonical_leaf_text) is what an analyzed index tokenizes, what an exact
//    whole-document MATCH compares, and what the scalar MATCH fallback evaluates: strings as they
//    are, integers in decimal, doubles in the shortest round-trip form ("0.1", "1e-07",
//    "0.30000000000000004"), booleans as `true` / `false`. OTHER leaves have no text.
//
// 3. Query contracts (reader, snii_index_reader.cpp). A whole-document predicate is EXACT; a
//    predicate bound to a sub-column path (QueryBinding::path_bound, including the empty key
//    `v['']`) yields CANDIDATES: the value exists somewhere in the row, and the iterator marks
//    the result requires_recheck so the scalar expression settles it on the candidate rows.
//    Candidate sets are supersets by construction; anything without a derivable superset is
//    left to scalar evaluation (INVERTED_INDEX_EVALUATE_SKIPPED).
//    - `v MATCH_ANY / MATCH_ALL 'q'` (whole document, no CAST): exact. On an analyzed index every
//      token of every leaf, MATCH_ALL across leaves of the same row; on an exact index the rows
//      holding a leaf whose canonical text equals the query (exact_text_terms). Object keys never
//      match. `CAST(v AS STRING) MATCH 'q'` is a different predicate: it tokenizes the JSON text,
//      keys included, and is never served by this index.
//    - `CAST(v AS STRING) = 'x'` / IN: never served (no leaf term bounds the JSON text).
//    - `v['p'] MATCH_ANY / MATCH_ALL 'q'` (no CAST): candidates by the same terms as above.
//    - `CAST(v['p'] AS STRING) = 'x'` / IN on a path read from the binary storage: candidates
//      from cast_text_candidate_terms (every typed term whose CAST(leaf AS STRING) can spell
//      `x`) plus the OTHER marker (a leaf without a term may cast to anything). Container
//      literals (`{`, `[`) and strings longer than `ignore_above` are not served.
//    - A typed materialized path binds a literal of its own value family only (typed_literal_terms):
//      integral widths may differ, int / double and float / double never mix.
//    - Numeric or boolean casts of binary paths, ranges, `!=`, `NOT IN`, `IS [NOT] NULL` and any
//      CAST-wrapped MATCH stay scalar. A candidate result is never negated and never counted
//      from a term's document frequency (inverted_index_reader.h, inverted_index_iterator.h).
//    Candidates cost their residual: a value that is common under other keys makes every row
//    holding it a candidate, and so does every document carrying the OTHER marker.
//
// 4. Properties. `variant_root_format_version` is stamped by the FE and names the term layout;
//    only the version this BE writes is usable, any other version makes the index invisible to
//    writers and readers instead of being misread. The two query-binding keys below exist only
//    on the copy of the index that bind_to_path() creates at query time and are never persisted.
inline constexpr std::string_view VARIANT_INDEX_SCOPE_KEY = "variant_index_scope";
inline constexpr std::string_view VARIANT_INDEX_SCOPE_VALUES = "values";
inline constexpr std::string_view VARIANT_INDEX_MODE_KEY = "variant_index_mode";
inline constexpr std::string_view VARIANT_INDEX_MODE_ALL_VALUES = "all_values";
inline constexpr std::string_view VARIANT_ROOT_FORMAT_VERSION_KEY = "variant_root_format_version";
// Path-less typed terms `[tag][value]`, strings raw (variant_term_codec.h).
inline constexpr std::string_view VARIANT_ROOT_FORMAT_VERSION_CURRENT = "4";
inline constexpr std::string_view VARIANT_ROOT_QUERY_PATH_KEY = "variant_root_query_path";
inline constexpr std::string_view VARIANT_ROOT_QUERY_VALUE_FAMILY_KEY =
        "variant_root_query_value_family";

// ---------------------------------------------------------------------------------------------
// Declaration.
// ---------------------------------------------------------------------------------------------

// True when the properties declare a root index at the version this BE implements.
bool is_root_index(const std::map<std::string, std::string>& properties);
bool is_root_index(const TabletIndex& index);
// FULLTEXT for an analyzed root index, STRING_TYPE for an exact one: the reader type its SNII
// reader is registered under and that the iterator selects by.
InvertedIndexReaderType reader_type(const TabletIndex& index);

// ---------------------------------------------------------------------------------------------
// Query binding.
// ---------------------------------------------------------------------------------------------

// The value family of a typed materialized path or of a query literal: "string", "boolean",
// "integral", "float", "double"; empty for every other type (VARIANT, JSONB, arrays, ...).
std::string_view value_family(PrimitiveType type);

struct QueryBinding {
    // Bound to one sub-column path: results are candidates. False for the whole document.
    bool path_bound = false;
    // The relative path; may be empty (the empty key `v['']`), which is why presence and not
    // emptiness distinguishes a binding from the whole document.
    std::string_view path;
    // The family of a typed materialized path; empty for a path read from the binary storage,
    // whose type is only known after the cast.
    std::string_view family;

    bool binary_path() const { return path_bound && family.empty(); }
};

QueryBinding query_binding(const std::map<std::string, std::string>& properties);
// A root index whose results are candidates for the residual expression.
bool yields_candidates(const std::map<std::string, std::string>& properties);
// The query-time copy of `root_index` bound to `relative_path`; `path_type` is the path's data
// type (TYPE_VARIANT for the binary storage, else a type with a value family).
std::shared_ptr<TabletIndex> bind_to_path(const TabletIndex& root_index,
                                          std::string_view relative_path, PrimitiveType path_type);

// ---------------------------------------------------------------------------------------------
// Value model: leaf -> term / text, literal -> terms.
// ---------------------------------------------------------------------------------------------

// The exact term of one classified leaf; empty for OTHER.
std::string leaf_term(const VariantLeaf& leaf);
// The marker term of a document holding a leaf the codec cannot spell.
std::string unspellable_marker_term();
// Canonical text of a leaf (contract 2). False for OTHER.
bool canonical_leaf_text(const VariantLeaf& leaf, std::string* text);

// Terms of every leaf whose canonical text equals `text`: the STRING term, the number term when
// `text` is the canonical spelling of an INT64 / UINT64 / DOUBLE, the BOOL term for `true` /
// `false`. Exact for whole-document MATCH on an exact index.
void exact_text_terms(std::string_view text, std::vector<std::string>* terms);
// A superset of the terms of every leaf whose CAST(leaf AS STRING) equals `text`: the STRING
// term, the integer term, the (folded) DOUBLE term of the number `text` denotes and of its FLOAT
// narrowing (a FLOAT leaf casts through float formatting but is indexed as the double it
// denotes), the BOOL term for `1` / `0`. Candidates only; the OTHER marker is added by the
// caller for binary paths.
void cast_text_candidate_terms(std::string_view text, std::vector<std::string>* terms);
// The term of a typed literal on a typed materialized path (numbers fold like leaves). Empty
// when the literal has no term (NaN, infinities, unsupported types).
void typed_literal_terms(const Field& value, std::vector<std::string>* terms);

} // namespace segment_v2::variant_root_index
} // namespace doris
