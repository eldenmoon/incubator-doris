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

#include "storage/index/inverted/variant_root_index_scan.h"

#include <cmath>
#include <limits>
#include <optional>
#include <string>
#include <utility>

#include "core/field.h"
#include "storage/index/inverted/variant_term_codec.h"
#include "storage/index/snii/query/docid_sink.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/query/internal/docid_union.h"
#include "storage/index/snii/reader/logical_index_reader.h"

namespace doris::segment_v2::variant_root_index {
namespace {

using ::doris::snii::query::internal::ResolvedDocidPosting;
using ::doris::snii::reader::LogicalIndexReader;

Status emit_union(const LogicalIndexReader& reader, std::vector<ResolvedDocidPosting>* postings,
                  std::vector<uint32_t>* docids) {
    docids->clear();
    if (postings->empty()) {
        return Status::OK();
    }
    ::doris::snii::query::VectorDocIdSink sink(*docids);
    return ::doris::snii::query::internal::emit_docid_union(reader, *postings, &sink);
}

// Enumerates [lower, upper_exclusive) and keeps the entries whose decoded term `accept`s.
template <typename Accept>
Status collect_range(const LogicalIndexReader& reader, std::string_view lower,
                     std::optional<std::string_view> upper_exclusive, const Accept& accept,
                     std::vector<ResolvedDocidPosting>* postings, ScanStats* stats) {
    return reader.visit_term_range(
            lower, upper_exclusive, [&](LogicalIndexReader::PrefixHit&& hit, bool* stop) -> Status {
                *stop = false;
                ++stats->terms_visited;
                const auto decoded = variant_term_codec::decode(hit.term);
                if (!decoded.has_value() || !accept(*decoded)) {
                    return Status::OK();
                }
                ++stats->terms_matched;
                postings->push_back({std::move(hit.entry), hit.frq_base, hit.prx_base});
                return Status::OK();
            });
}

std::optional<std::string_view> upper_bound_view(const std::string& upper) {
    if (upper.empty()) {
        return std::nullopt; // prefix was all 0xff: run to the end of the dictionary
    }
    return std::string_view(upper);
}

} // namespace

bool path_in_subtree(std::string_view path, std::string_view subtree) {
    if (subtree.empty()) {
        return true;
    }
    if (!path.starts_with(subtree)) {
        return false;
    }
    return path.size() == subtree.size() || path[subtree.size()] == '.';
}

NumericBound NumericBound::unbounded_low() {
    return {.value = -std::numeric_limits<double>::infinity(), .inclusive = true};
}

NumericBound NumericBound::unbounded_high() {
    return {.value = std::numeric_limits<double>::infinity(), .inclusive = true};
}

NumericBound NumericBound::integer(int64_t value, bool inclusive) {
    return {.value = static_cast<double>(value),
            .inclusive = inclusive,
            .integral = true,
            .int_value = value};
}

NumericBound NumericBound::floating(double value, bool inclusive) {
    return {.value = value, .inclusive = inclusive};
}

Status scan_root_prefix(const LogicalIndexReader& reader, std::string_view root_prefix,
                        std::string_view subtree, std::vector<uint32_t>* docids, ScanStats* stats) {
    DORIS_CHECK(docids != nullptr);
    DORIS_CHECK(stats != nullptr);
    std::vector<ResolvedDocidPosting> postings;
    // Value-first terms are raw dictionary keys: their tags (0x02..0x07) never enter the SNII
    // internal namespace (0x1e / 0x1f), so no plain-term escaping or CommonGrams validation
    // applies and the physical prefix run is enumerated directly.
    RETURN_IF_ERROR(reader.visit_prefix_terms(
            root_prefix, [&](LogicalIndexReader::PrefixHit&& hit, bool* stop) -> Status {
                *stop = false;
                ++stats->terms_visited;
                if (!subtree.empty()) {
                    // The value is fixed by the prefix; only the path suffix decides.
                    const auto decoded = variant_term_codec::decode(hit.term);
                    if (!decoded.has_value() || !path_in_subtree(decoded->path, subtree)) {
                        return Status::OK();
                    }
                }
                ++stats->terms_matched;
                postings.push_back({std::move(hit.entry), hit.frq_base, hit.prx_base});
                return Status::OK();
            }));
    return emit_union(reader, &postings, docids);
}

namespace {

// Bound comparisons happen in long double (64-bit mantissa or wider on every supported target),
// so an int64 literal never rounds against a double term and INT64_MAX stays distinct from 2^63.
long double bound_value(const NumericBound& bound) {
    return bound.integral ? static_cast<long double>(bound.int_value)
                          : static_cast<long double>(bound.value);
}

bool above_lower(long double v, const NumericBound& lower) {
    const long double lo = bound_value(lower);
    return v > lo || (v == lo && lower.inclusive);
}

bool below_upper(long double v, const NumericBound& upper) {
    const long double hi = bound_value(upper);
    return v < hi || (v == hi && upper.inclusive);
}

constexpr long double kInt64Min = -9223372036854775808.0L;
constexpr long double kInt64Max = 9223372036854775807.0L;
constexpr long double kUint64Max = 18446744073709551615.0L;

// Smallest int64 satisfying the lower bound, if any.
std::optional<int64_t> int64_lower(const NumericBound& lower) {
    if (lower.integral) {
        if (lower.inclusive) {
            return lower.int_value;
        }
        if (lower.int_value == std::numeric_limits<int64_t>::max()) {
            return std::nullopt;
        }
        return lower.int_value + 1;
    }
    long double v = std::ceil(static_cast<long double>(lower.value));
    if (!lower.inclusive && v == static_cast<long double>(lower.value)) {
        v += 1; // an integral exclusive bound excludes itself
    }
    if (v > kInt64Max) {
        return std::nullopt;
    }
    return v < kInt64Min ? std::numeric_limits<int64_t>::min() : static_cast<int64_t>(v);
}

// Largest int64 satisfying the upper bound, if any.
std::optional<int64_t> int64_upper(const NumericBound& upper) {
    if (upper.integral) {
        if (upper.inclusive) {
            return upper.int_value;
        }
        if (upper.int_value == std::numeric_limits<int64_t>::min()) {
            return std::nullopt;
        }
        return upper.int_value - 1;
    }
    long double v = std::floor(static_cast<long double>(upper.value));
    if (!upper.inclusive && v == static_cast<long double>(upper.value)) {
        v -= 1;
    }
    if (v < kInt64Min) {
        return std::nullopt;
    }
    return v > kInt64Max ? std::numeric_limits<int64_t>::max() : static_cast<int64_t>(v);
}

} // namespace

Status scan_numeric_range(const LogicalIndexReader& reader, std::string_view path,
                          NumericBound lower, NumericBound upper, std::vector<uint32_t>* docids,
                          ScanStats* stats) {
    DORIS_CHECK(docids != nullptr);
    DORIS_CHECK(stats != nullptr);
    docids->clear();
    if (std::isnan(lower.value) || std::isnan(upper.value)) {
        return Status::OK();
    }
    const auto in_range = [&](long double v) {
        return above_lower(v, lower) && below_upper(v, upper);
    };
    std::vector<ResolvedDocidPosting> postings;

    // INT64 segment: the dictionary run is bounded by the exact integer envelope of the
    // interval; the decoded check settles nothing more than the path.
    if (const auto lo = int64_lower(lower), hi = int64_upper(upper);
        lo.has_value() && hi.has_value() && *lo <= *hi) {
        const std::string begin = variant_term_codec::root_prefix_int64(*lo);
        const std::string end =
                variant_term_codec::prefix_upper_bound(variant_term_codec::root_prefix_int64(*hi));
        RETURN_IF_ERROR(collect_range(
                reader, begin, upper_bound_view(end),
                [&](const variant_term_codec::DecodedTerm& term) {
                    return term.tag == variant_term_codec::Tag::INT64 && term.path == path &&
                           term.int64_value >= *lo && term.int64_value <= *hi;
                },
                &postings, stats));
    }
    // UINT64 segment: only values in [2^63, 2^64) live here, above every int64 literal, so an
    // integral upper bound never reaches it and an integral lower bound always admits it.
    if (!upper.integral && static_cast<long double>(upper.value) >= 9223372036854775808.0L) {
        uint64_t lo = uint64_t {1} << 63;
        if (!lower.integral) {
            long double v = std::ceil(static_cast<long double>(lower.value));
            if (!lower.inclusive && v == static_cast<long double>(lower.value)) {
                v += 1;
            }
            if (v > kUint64Max) {
                v = kUint64Max + 1; // empty
            }
            lo = v <= 9223372036854775808.0L ? lo : static_cast<uint64_t>(v);
            if (v > kUint64Max) {
                lo = std::numeric_limits<uint64_t>::max();
            }
        }
        long double hi_ld = std::floor(static_cast<long double>(upper.value));
        if (!upper.inclusive && hi_ld == static_cast<long double>(upper.value)) {
            hi_ld -= 1;
        }
        if (hi_ld >= 9223372036854775808.0L) {
            const uint64_t hi = hi_ld >= kUint64Max ? std::numeric_limits<uint64_t>::max()
                                                    : static_cast<uint64_t>(hi_ld);
            if (lo <= hi) {
                const std::string begin = variant_term_codec::root_prefix_uint64(lo);
                const std::string end = variant_term_codec::prefix_upper_bound(
                        variant_term_codec::root_prefix_uint64(hi));
                RETURN_IF_ERROR(collect_range(
                        reader, begin, upper_bound_view(end),
                        [&](const variant_term_codec::DecodedTerm& term) {
                            return term.tag == variant_term_codec::Tag::UINT64 &&
                                   term.path == path && term.uint64_value >= lo &&
                                   term.uint64_value <= hi;
                        },
                        &postings, stats));
            }
        }
    }
    // DOUBLE segment: fractional values, infinities and magnitudes beyond 2^64. The ordered
    // encoding makes [lower, upper] one run; the endpoints are settled by the exact check.
    {
        const std::string begin = variant_term_codec::root_prefix_double(lower.value);
        const std::string end = variant_term_codec::prefix_upper_bound(
                variant_term_codec::root_prefix_double(upper.value));
        RETURN_IF_ERROR(collect_range(
                reader, begin, upper_bound_view(end),
                [&](const variant_term_codec::DecodedTerm& term) {
                    return term.tag == variant_term_codec::Tag::DOUBLE && term.path == path &&
                           in_range(static_cast<long double>(term.double_value));
                },
                &postings, stats));
    }
    return emit_union(reader, &postings, docids);
}

std::optional<NumericBound> numeric_query_bound(const Field& value, bool inclusive) {
    switch (value.get_type()) {
    case PrimitiveType::TYPE_TINYINT:
        return NumericBound::integer(value.get<PrimitiveType::TYPE_TINYINT>(), inclusive);
    case PrimitiveType::TYPE_SMALLINT:
        return NumericBound::integer(value.get<PrimitiveType::TYPE_SMALLINT>(), inclusive);
    case PrimitiveType::TYPE_INT:
        return NumericBound::integer(value.get<PrimitiveType::TYPE_INT>(), inclusive);
    case PrimitiveType::TYPE_BIGINT:
        return NumericBound::integer(value.get<PrimitiveType::TYPE_BIGINT>(), inclusive);
    case PrimitiveType::TYPE_UINT32:
        return NumericBound::integer(value.get<PrimitiveType::TYPE_UINT32>(), inclusive);
    case PrimitiveType::TYPE_UINT64: {
        const uint64_t unsigned_value = value.get<PrimitiveType::TYPE_UINT64>();
        if (unsigned_value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return NumericBound::integer(static_cast<int64_t>(unsigned_value), inclusive);
        }
        return NumericBound::floating(static_cast<double>(unsigned_value), inclusive);
    }
    case PrimitiveType::TYPE_FLOAT: {
        const auto v = static_cast<double>(value.get<PrimitiveType::TYPE_FLOAT>());
        return std::isnan(v) ? std::nullopt
                             : std::optional<NumericBound>(NumericBound::floating(v, inclusive));
    }
    case PrimitiveType::TYPE_DOUBLE: {
        const double v = value.get<PrimitiveType::TYPE_DOUBLE>();
        return std::isnan(v) ? std::nullopt
                             : std::optional<NumericBound>(NumericBound::floating(v, inclusive));
    }
    default:
        return std::nullopt;
    }
}

} // namespace doris::segment_v2::variant_root_index
