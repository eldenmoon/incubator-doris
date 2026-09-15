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
#include <functional>
#include <optional>
#include <string>
#include <string_view>

#include "common/status.h"
#include "core/string_ref.h"
#include "core/value/variant/variant_value.h"

namespace doris {

// The one leaf classification shared by everything that needs "one callback per scalar leaf"
// semantics over a Variant document: the VARIANT inverted index writer, the scalar MATCH / equality
// fallback, and any future per-leaf function. Keeping the traversal and the canonical value rules
// in one place is what lets index results and scalar evaluation agree.
//
// Canonical value rules (shared with query literals through canonical_numeric_*):
//   - every signed integer width is INT64;
//   - a finite integral float/double folds into INT64 when it fits, into UINT64 when it lies in
//     [2^63, 2^64), and stays DOUBLE otherwise; NaN is OTHER (no indexable value);
//   - strings keep their bytes; "3" and 3 are different leaves (typed semantics);
//   - decimal, temporal, binary, UUID and any other primitive are OTHER: the path exists but has
//     no indexable value.
enum class VariantLeafKind : uint8_t { STRING, INT64, UINT64, DOUBLE, BOOL, OTHER };

struct VariantCanonicalNumber {
    VariantLeafKind kind = VariantLeafKind::OTHER; // INT64, UINT64 or DOUBLE
    int64_t int64_value = 0;
    uint64_t uint64_value = 0;
    double double_value = 0.0;
};

// Canonical numeric classification for a literal. Returns nullopt for NaN, which has no term.
std::optional<VariantCanonicalNumber> canonical_numeric_from_int64(int64_t value);
std::optional<VariantCanonicalNumber> canonical_numeric_from_uint64(uint64_t value);
std::optional<VariantCanonicalNumber> canonical_numeric_from_double(double value);

struct VariantLeaf {
    // Dotted path of the leaf. Array elements report the array's own path ("a.b" for a.b[0]),
    // matching the on-disk subcolumn namespace where {"a.b":1} and {"a":{"b":1}} coincide.
    std::string_view path;
    VariantLeafKind kind = VariantLeafKind::OTHER;
    VariantRef value; // borrowed from the document; valid only inside the callback

    // Filled according to `kind`.
    StringRef string_value;
    int64_t int64_value = 0;
    uint64_t uint64_value = 0;
    double double_value = 0.0;
    bool bool_value = false;
};

struct VariantVisitOptions {
    // When true, arrays are traversed and their scalar elements are reported under the array's
    // path; nested arrays and objects inside arrays recurse the same way. When false an array is
    // reported once as an OTHER leaf.
    bool recurse_arrays = true;
    // Dotted path of `root` itself. Leaves below it are reported as "<prefix>.<sub path>"; a
    // scalar root is reported as exactly `path_prefix`. Lets callers that already know where a
    // sub-document lives (the shredder handing over an array leaf) reuse the same traversal.
    std::string_view path_prefix;
};

// Classifies one scalar value. Objects and arrays are reported as OTHER by this function; use
// visit_variant_leaves() to descend into them.
VariantLeaf classify_variant_leaf(std::string_view path, const VariantRef& value);

// Visits every leaf of `root` in document order:
//   - objects recurse; arrays recurse when options.recurse_arrays, otherwise they are OTHER leaves;
//   - JSON null never calls back (it is neither a value nor a path with a value);
//   - empty objects / arrays never call back;
//   - every other value calls back exactly once, including OTHER leaves, so callers can record path
//     existence for values that produce no term.
// A scalar root (the whole document is "abc" or 42) is reported with an empty path.
// The callback's leaf borrows the document; it must not be retained past the callback.
using VariantLeafCallback = std::function<Status(const VariantLeaf&)>;

Status visit_variant_leaves(const VariantRef& root, const VariantVisitOptions& options,
                            const VariantLeafCallback& callback);

} // namespace doris
