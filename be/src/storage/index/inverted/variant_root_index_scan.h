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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "common/status.h"

namespace doris {
class Field;
namespace snii::reader {
class LogicalIndexReader;
}

namespace segment_v2::variant_root_index {

// Dictionary scans over the value-first term layout. All results are exact row sets: the
// dictionary is ordered [tag][value][sep][path], so every term of one value is a contiguous run
// and every numeric interval is a contiguous run per tag. The reader and the parity tests share
// these so index execution and its test oracle cannot drift.
struct ScanStats {
    size_t terms_visited = 0; // dictionary entries enumerated (including filtered-out ones)
    size_t terms_matched = 0; // entries whose postings were merged
};

// Union of the postings of every term carrying `root_prefix` whose path lies in `subtree`:
// "" is the whole document (any path), "a.b" accepts the leaf a.b itself and everything below
// it (a.b.c, a.b.d.e, ...). This is the whole-root equality `v = c`, the equality on a dynamic
// sub-document `v['a'] = c`, and the per-token building block of MATCH on either.
Status scan_root_prefix(const snii::reader::LogicalIndexReader& reader,
                        std::string_view root_prefix, std::string_view subtree,
                        std::vector<uint32_t>* docids, ScanStats* stats);

bool path_in_subtree(std::string_view path, std::string_view subtree);

// Numeric interval on one path. Bounds are inclusive when the flag says so; a bound may be
// -inf / +inf. Covers the INT64, UINT64 and DOUBLE tags of the canonical value rules, so an
// integral bound matches folded integers and a fractional bound matches DOUBLE leaves. Terms are
// filtered by their path suffix after decoding: the cost is every path holding a value inside
// the interval, which is why a typed child index is preferred when one exists.
struct NumericBound {
    double value = 0.0;
    bool inclusive = true;
    // Set for integer literals so INT64 terms compare in the integer domain (doubles lose
    // precision above 2^53). `value` still carries the double view for the DOUBLE segment.
    bool integral = false;
    int64_t int_value = 0;

    static NumericBound unbounded_low();
    static NumericBound unbounded_high();
    static NumericBound integer(int64_t value, bool inclusive);
    static NumericBound floating(double value, bool inclusive);
};

Status scan_numeric_range(const snii::reader::LogicalIndexReader& reader, std::string_view path,
                          NumericBound lower, NumericBound upper, std::vector<uint32_t>* docids,
                          ScanStats* stats);

// A range predicate literal as a bound: integer types keep the integer domain, floating types
// the double domain. nullopt for non-numeric types and for NaN.
std::optional<NumericBound> numeric_query_bound(const Field& value, bool inclusive);

} // namespace segment_v2::variant_root_index
} // namespace doris
