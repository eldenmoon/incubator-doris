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

#include "core/value/variant/variant_leaf_visitor.h"

#include <gtest/gtest.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "core/column/variant_v2/column_variant_v2.h"
#include "core/value/variant/variant_field.h"
#include "core/value/variant/variant_scalar.h"
#include "exprs/function/parse/variant_string_parse.h"

namespace doris {
namespace {

struct SeenLeaf {
    std::string path;
    VariantLeafKind kind;
    std::string text; // STRING bytes, or a rendering of the canonical number / bool
};

std::string render(const VariantLeaf& leaf) {
    switch (leaf.kind) {
    case VariantLeafKind::STRING:
        return std::string(leaf.string_value.data, leaf.string_value.size);
    case VariantLeafKind::INT64:
        return "i" + std::to_string(leaf.int64_value);
    case VariantLeafKind::UINT64:
        return "u" + std::to_string(leaf.uint64_value);
    case VariantLeafKind::DOUBLE:
        return "d" + std::to_string(leaf.double_value);
    case VariantLeafKind::BOOL:
        return leaf.bool_value ? "true" : "false";
    case VariantLeafKind::OTHER:
        return "<other>";
    }
    return "?";
}

bool operator==(const SeenLeaf& a, const SeenLeaf& b) {
    return a.path == b.path && a.kind == b.kind && a.text == b.text;
}

std::ostream& operator<<(std::ostream& os, const SeenLeaf& leaf) {
    return os << "{" << leaf.path << ", " << static_cast<int>(leaf.kind) << ", " << leaf.text
              << "}";
}

std::vector<SeenLeaf> visit_json(std::string_view json, VariantVisitOptions options = {}) {
    JsonStringToVariantEncoder encoder;
    encoder.add_json({json.data(), json.size()});
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(encoder.finish_batch());
    std::vector<SeenLeaf> seen;
    const Status status = visit_variant_leaves(
            column->read_view().value_at(0), options, [&](const VariantLeaf& leaf) {
                seen.push_back({std::string(leaf.path), leaf.kind, render(leaf)});
                return Status::OK();
            });
    EXPECT_TRUE(status.ok()) << status;
    return seen;
}

std::vector<SeenLeaf> visit_scalar(const VariantScalarRef& scalar) {
    const VariantField field = VariantField::from_scalar(scalar);
    std::vector<SeenLeaf> seen;
    const Status status = visit_variant_leaves(field.ref(), {}, [&](const VariantLeaf& leaf) {
        seen.push_back({std::string(leaf.path), leaf.kind, render(leaf)});
        return Status::OK();
    });
    EXPECT_TRUE(status.ok()) << status;
    return seen;
}

using K = VariantLeafKind;

TEST(VariantLeafVisitorTest, ObjectsRecurseIntoDottedPathsInDocumentOrder) {
    EXPECT_EQ(visit_json(R"({"a":1,"b":{"c":"x","d":{"e":true}},"f":2.5})"),
              (std::vector<SeenLeaf> {{"a", K::INT64, "i1"},
                                      {"b.c", K::STRING, "x"},
                                      {"b.d.e", K::BOOL, "true"},
                                      {"f", K::DOUBLE, "d2.500000"}}));
    // Dotted keys and nested objects share one path namespace, like the on-disk subcolumns.
    EXPECT_EQ(visit_json(R"({"a.b":1})"), visit_json(R"({"a":{"b":1}})"));
}

TEST(VariantLeafVisitorTest, ArraysRecurseUnderTheArrayPath) {
    EXPECT_EQ(visit_json(R"({"a":[{"b":1},{"b":"two"},[3,[4]],5,null,[]]})"),
              (std::vector<SeenLeaf> {{"a.b", K::INT64, "i1"},
                                      {"a.b", K::STRING, "two"},
                                      {"a", K::INT64, "i3"},
                                      {"a", K::INT64, "i4"},
                                      {"a", K::INT64, "i5"}}));
    // Root arrays report their scalars with an empty path.
    EXPECT_EQ(visit_json(R"([1,"x"])"),
              (std::vector<SeenLeaf> {{"", K::INT64, "i1"}, {"", K::STRING, "x"}}));
}

TEST(VariantLeafVisitorTest, ArraysAreOtherLeavesWhenNotRecursed) {
    VariantVisitOptions options;
    options.recurse_arrays = false;
    EXPECT_EQ(visit_json(R"({"a":[1,2],"b":"x","c":[]})", options),
              (std::vector<SeenLeaf> {{"a", K::OTHER, "<other>"}, {"b", K::STRING, "x"}}));
}

TEST(VariantLeafVisitorTest, PathPrefixAnchorsTheTraversal) {
    VariantVisitOptions options;
    options.path_prefix = "root.arr";
    EXPECT_EQ(visit_json(R"([{"b":1},"x",[2]])", options),
              (std::vector<SeenLeaf> {{"root.arr.b", K::INT64, "i1"},
                                      {"root.arr", K::STRING, "x"},
                                      {"root.arr", K::INT64, "i2"}}));
    EXPECT_EQ(visit_json(R"("scalar")", options),
              (std::vector<SeenLeaf> {{"root.arr", K::STRING, "scalar"}}));
    EXPECT_EQ(visit_json(R"({"c":{"d":true}})", options),
              (std::vector<SeenLeaf> {{"root.arr.c.d", K::BOOL, "true"}}));
}

TEST(VariantLeafVisitorTest, JsonNullAndEmptyContainersNeverCallBack) {
    EXPECT_TRUE(visit_json("null").empty());
    EXPECT_TRUE(visit_json("{}").empty());
    EXPECT_TRUE(visit_json("[]").empty());
    EXPECT_TRUE(visit_json(R"({"a":null,"b":{},"c":[],"d":{"e":null},"f":[null,[]]})").empty());
    EXPECT_EQ(visit_json(R"({"a":null,"b":1})"), (std::vector<SeenLeaf> {{"b", K::INT64, "i1"}}));
}

TEST(VariantLeafVisitorTest, ScalarRootsHaveAnEmptyPath) {
    EXPECT_EQ(visit_json(R"("abc")"), (std::vector<SeenLeaf> {{"", K::STRING, "abc"}}));
    EXPECT_EQ(visit_json("42"), (std::vector<SeenLeaf> {{"", K::INT64, "i42"}}));
    EXPECT_EQ(visit_json("false"), (std::vector<SeenLeaf> {{"", K::BOOL, "false"}}));
}

TEST(VariantLeafVisitorTest, NumbersFollowTheCanonicalRules) {
    EXPECT_EQ(visit_json(R"({"a":42.0,"b":-0.0,"c":9223372036854775807,"d":1.5})"),
              (std::vector<SeenLeaf> {{"a", K::INT64, "i42"},
                                      {"b", K::INT64, "i0"},
                                      {"c", K::INT64, "i9223372036854775807"},
                                      {"d", K::DOUBLE, "d1.500000"}}));
    EXPECT_EQ(visit_scalar(VariantScalarRef::float64(0x1p63)),
              (std::vector<SeenLeaf> {{"", K::UINT64, "u9223372036854775808"}}));
    EXPECT_EQ(visit_scalar(VariantScalarRef::float64(0x1p64)),
              (std::vector<SeenLeaf> {{"", K::DOUBLE, "d" + std::to_string(0x1p64)}}));
    EXPECT_EQ(visit_scalar(VariantScalarRef::float32(7.0F)),
              (std::vector<SeenLeaf> {{"", K::INT64, "i7"}}));
    EXPECT_EQ(visit_scalar(VariantScalarRef::float64(-std::numeric_limits<double>::infinity())),
              (std::vector<SeenLeaf> {
                      {"", K::DOUBLE,
                       "d" + std::to_string(-std::numeric_limits<double>::infinity())}}));
}

TEST(VariantLeafVisitorTest, ValuesWithoutAnIndexableTermAreStillReportedAsOther) {
    // NaN has a path but no term; the visitor must still call back so path existence is recorded.
    EXPECT_EQ(visit_scalar(VariantScalarRef::float64(std::numeric_limits<double>::quiet_NaN())),
              (std::vector<SeenLeaf> {{"", K::OTHER, "<other>"}}));
}

TEST(VariantLeafVisitorTest, CanonicalNumericLiterals) {
    const auto kind_of = [](const std::optional<VariantCanonicalNumber>& number) {
        return number.has_value() ? number->kind : VariantLeafKind::OTHER;
    };
    EXPECT_EQ(kind_of(canonical_numeric_from_int64(-5)), K::INT64);
    EXPECT_EQ(canonical_numeric_from_int64(-5)->int64_value, -5);
    EXPECT_EQ(kind_of(canonical_numeric_from_uint64(5)), K::INT64);
    EXPECT_EQ(canonical_numeric_from_uint64(5)->int64_value, 5);
    EXPECT_EQ(kind_of(canonical_numeric_from_uint64(uint64_t {1} << 63)), K::UINT64);
    EXPECT_EQ(canonical_numeric_from_uint64(std::numeric_limits<uint64_t>::max())->uint64_value,
              std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(kind_of(canonical_numeric_from_double(42.0)), K::INT64);
    EXPECT_EQ(canonical_numeric_from_double(42.0)->int64_value, 42);
    EXPECT_EQ(kind_of(canonical_numeric_from_double(-0.0)), K::INT64);
    EXPECT_EQ(canonical_numeric_from_double(-0.0)->int64_value, 0);
    EXPECT_EQ(kind_of(canonical_numeric_from_double(0x1p63)), K::UINT64);
    EXPECT_EQ(canonical_numeric_from_double(0x1p63)->uint64_value, uint64_t {1} << 63);
    EXPECT_EQ(kind_of(canonical_numeric_from_double(0x1p64)), K::DOUBLE);
    EXPECT_EQ(kind_of(canonical_numeric_from_double(1.5)), K::DOUBLE);
    EXPECT_EQ(kind_of(canonical_numeric_from_double(std::numeric_limits<double>::infinity())),
              K::DOUBLE);
    EXPECT_FALSE(
            canonical_numeric_from_double(std::numeric_limits<double>::quiet_NaN()).has_value());
}

TEST(VariantLeafVisitorTest, CallbackErrorsStopTheTraversal) {
    JsonStringToVariantEncoder encoder;
    const std::string_view json = R"({"a":1,"b":2,"c":3})";
    encoder.add_json({json.data(), json.size()});
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(encoder.finish_batch());
    int calls = 0;
    const Status status =
            visit_variant_leaves(column->read_view().value_at(0), {}, [&](const VariantLeaf& leaf) {
                ++calls;
                if (leaf.path == "b") {
                    return Status::InternalError("stop");
                }
                return Status::OK();
            });
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(calls, 2);
}

} // namespace
} // namespace doris
