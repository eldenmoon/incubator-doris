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

#include <array>
#include <string_view>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/function_variant_element_v2.h"

namespace doris {
namespace {

using Segment = VariantElementV2PathSegment;

std::unique_ptr<ResolvedVariantElementV2Path> resolve(std::vector<Segment> segments) {
    std::unique_ptr<ResolvedVariantElementV2Path> result;
    Status status = resolve_variant_element_v2_path(segments, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

ColumnPtr extract(const ColumnVariantV2& source, const ResolvedVariantElementV2Path& path,
                  std::span<const uint8_t> outer_nulls = {}) {
    ColumnPtr result;
    Status status = extract_variant_element_v2(source, path, outer_nulls, &result);
    EXPECT_TRUE(status.ok()) << status;
    return result;
}

const ColumnNullable& nullable_result(const ColumnPtr& result) {
    return assert_cast<const ColumnNullable&>(*result);
}

const ColumnVariantV2& variant_result(const ColumnPtr& result) {
    return assert_cast<const ColumnVariantV2&>(nullable_result(result).get_nested_column());
}

ColumnVariantV2::MutablePtr typed_strings(std::span<const std::string_view> values,
                                          std::span<const uint8_t> null_map) {
    EXPECT_EQ(values.size(), null_map.size());
    auto strings = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    for (size_t row = 0; row < values.size(); ++row) {
        strings->insert_data(values[row].data(), values[row].size());
        nulls->insert_value(null_map[row]);
    }
    return ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(strings), std::move(nulls)),
            std::make_shared<DataTypeString>());
}

} // namespace

TEST(VariantElementV2StringTest, StringScalarRootsReturnSqlNull) {
    const std::array<std::string_view, 10> documents {
            R"({"a":"x"})", R"({"a":7})", R"({"a":[1,2]})",   R"({"a":{"b":9}})", R"({"a":null})",
            "not json",     "7",          R"({"missing":1})", R"({"a":10})",      R"({"a":11})"};
    const std::array<uint8_t, 10> typed_nulls {0, 0, 0, 0, 0, 0, 0, 0, 1, 0};
    const std::array<uint8_t, 10> outer_nulls {0, 0, 0, 0, 0, 0, 0, 0, 0, 1};
    auto source = typed_strings(documents, typed_nulls);
    auto path = resolve({Segment::object_key(StringRef("a"))});

    ColumnPtr result = extract(*source, *path, outer_nulls);
    const auto& nullable = nullable_result(result);
    const auto& values = variant_result(result);
    ASSERT_EQ(values.size(), documents.size());
    for (size_t row = 0; row < documents.size(); ++row) {
        EXPECT_EQ(nullable.get_null_map_data()[row], 1) << row;
    }
    EXPECT_TRUE(source->is_typed());
}

TEST(VariantElementV2StringTest, NestedPathsOnStringScalarRootsReturnSqlNull) {
    const std::array<std::string_view, 2> documents {R"({"a":{"items":[{"v":1},{"v":2}]}})",
                                                     R"({"a":{"items":[]}})"};
    const std::array<uint8_t, 2> nulls {0, 0};
    auto source = typed_strings(documents, nulls);
    auto path =
            resolve({Segment::object_key(StringRef("a")), Segment::object_key(StringRef("items")),
                     Segment::array_index(1), Segment::object_key(StringRef("v"))});

    ColumnPtr result = extract(*source, *path);
    EXPECT_EQ(nullable_result(result).get_null_map_data()[0], 1);
    EXPECT_EQ(nullable_result(result).get_null_map_data()[1], 1);
}

TEST(VariantElementV2StringTest, NonStringTypedStateReturnsAllSqlNull) {
    auto values = ColumnInt32::create();
    values->insert_value(1);
    values->insert_value(2);
    values->insert_value(3);
    auto typed_nulls = ColumnUInt8::create();
    typed_nulls->insert_value(0);
    typed_nulls->insert_value(1);
    typed_nulls->insert_value(0);
    auto source = ColumnVariantV2::create_typed(
            ColumnNullable::create(std::move(values), std::move(typed_nulls)),
            std::make_shared<DataTypeInt32>());
    auto path = resolve({Segment::object_key(StringRef("a"))});

    ColumnPtr result = extract(*source, *path);
    const auto& nullable = nullable_result(result);
    ASSERT_EQ(nullable.size(), 3);
    EXPECT_EQ(nullable.get_null_map_data()[0], 1);
    EXPECT_EQ(nullable.get_null_map_data()[1], 1);
    EXPECT_EQ(nullable.get_null_map_data()[2], 1);
    EXPECT_TRUE(source->is_typed());
}

} // namespace doris
