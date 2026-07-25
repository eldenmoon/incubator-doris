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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_canonical.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "storage/segment/variant/variant_assembler.h"
#include "storage/segment/variant/variant_shredder.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

using RowWriter = std::function<void(VariantBatchBuilder::Row&)>;

struct StringWriter {
    void write(const char* data, size_t size) { value.append(data, size); }
    std::string value;
};

struct DocCell {
    size_t bucket = 0;
    size_t row = 0;
    std::string path;
    std::string raw;
    bool operator==(const DocCell&) const = default;
};

struct C3DocFixture {
    std::vector<std::string> source;
    std::vector<std::string> assembled;
    std::map<std::pair<size_t, std::string>, std::string> cells;
};

ColumnVariantV2::MutablePtr encoded_rows(const std::vector<RowWriter>& writers) {
    VariantBatchBuilder builder({.rows = writers.size(), .metadata_keys = 16});
    for (const RowWriter& writer : writers) {
        auto row = builder.begin_row();
        writer(row);
        row.finish();
    }
    VariantBatchBuilder block = builder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    return result;
}

ColumnVariantV2::MutablePtr encoded_jsons(const std::vector<std::string>& jsons) {
    JsonStringToVariantEncoder encoder;
    for (const std::string& json : jsons) {
        encoder.add_json({json.data(), json.size()});
    }
    VariantBatchBuilder block = encoder.finish_batch();
    auto result = ColumnVariantV2::create();
    result->insert_encoded_batch(block);
    return result;
}

VariantShreddedColumns shred(const ColumnVariantV2& input, VariantShredderOptions options,
                             std::span<const uint8_t> outer_nulls = {}) {
    VariantShredder shredder(std::move(options));
    EXPECT_TRUE(shredder.append(input.read_view(), 0, input.size(), outer_nulls).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    EXPECT_TRUE(status.ok()) << status.to_string();
    return result;
}

std::string hex(StringRef bytes) {
    constexpr std::string_view digits = "0123456789abcdef";
    std::string result(bytes.size * 2, '0');
    for (size_t index = 0; index < bytes.size; ++index) {
        const auto value = static_cast<uint8_t>(bytes.data[index]);
        result[index * 2] = digits[value >> 4];
        result[index * 2 + 1] = digits[value & 0x0F];
    }
    return result;
}

std::vector<DocCell> doc_cells(const VariantShreddedColumns& result) {
    std::vector<DocCell> cells;
    for (size_t bucket = 0; bucket < result.doc_buckets.size(); ++bucket) {
        const auto& map = assert_cast<const ColumnMap&>(*result.doc_buckets[bucket].column);
        const auto& keys = assert_cast<const ColumnString&>(map.get_keys());
        const auto& values = assert_cast<const ColumnString&>(map.get_values());
        for (size_t row = 0; row < result.num_rows; ++row) {
            const size_t begin = row == 0 ? 0 : map.get_offsets()[row - 1];
            const size_t end = map.get_offsets()[row];
            for (size_t index = begin; index < end; ++index) {
                cells.push_back({.bucket = bucket,
                                 .row = row,
                                 .path = keys.get_data_at(index).to_string(),
                                 .raw = hex(values.get_data_at(index))});
            }
        }
    }
    return cells;
}

std::string print_json(VariantRef value) {
    StringWriter writer;
    to_json(value, writer);
    return writer.value;
}

VariantAssembledColumn assemble_doc(const VariantShreddedColumns& shredded,
                                    std::span<const uint8_t> outer_nulls = {}) {
    EXPECT_EQ(shredded.doc_buckets.size(), 1);
    VariantAssemblerPlanOptions options;
    options.has_root = true;
    options.has_doc = true;
    std::shared_ptr<const VariantAssemblerPlan> plan;
    const Status plan_status = VariantAssemblerPlan::create(std::move(options), &plan);
    EXPECT_TRUE(plan_status.ok()) << plan_status.to_string();
    VariantAssemblerBatchView batch;
    batch.num_rows = shredded.num_rows;
    batch.outer_nulls = outer_nulls;
    batch.root_jsonb = shredded.root_jsonb.get();
    batch.doc_values = &assert_cast<const ColumnMap&>(*shredded.doc_buckets[0].column);
    VariantAssembler assembler(std::move(plan));
    VariantAssembledColumn result;
    const Status status = assembler.assemble(batch, &result);
    EXPECT_TRUE(status.ok()) << status.to_string();
    return result;
}

std::vector<std::string> split_tsv(const std::string& line) {
    std::vector<std::string> fields;
    size_t begin = 0;
    while (true) {
        const size_t separator = line.find('\t', begin);
        fields.emplace_back(line.substr(begin, separator - begin));
        if (separator == std::string::npos) {
            return fields;
        }
        begin = separator + 1;
    }
}

std::string sample_path(std::string_view file) {
    const char* doris_home = std::getenv("DORIS_HOME");
    EXPECT_NE(doris_home, nullptr);
    std::filesystem::path root =
            std::filesystem::weakly_canonical(doris_home == nullptr ? "." : doris_home);
    while (!std::filesystem::exists(root / "docs") && root.has_parent_path() &&
           root != root.parent_path()) {
        root = root.parent_path();
    }
    return (root / "docs/design/variant_v2/baseline/segments/samples" / std::string(file)).string();
}

C3DocFixture load_c3_doc_fixture() {
    C3DocFixture fixture;
    std::ifstream source(sample_path("source.tsv"));
    EXPECT_TRUE(source.is_open());
    std::string line;
    std::getline(source, line);
    while (std::getline(source, line)) {
        const auto fields = split_tsv(line);
        if (fields.size() == 5 && fields[0] == "doc") {
            const size_t row = std::stoull(fields[1]);
            fixture.source.resize(std::max(fixture.source.size(), row + 1));
            fixture.source[row] = fields[4];
        }
    }

    std::ifstream manifest(sample_path("manifest.tsv"));
    EXPECT_TRUE(manifest.is_open());
    std::getline(manifest, line);
    while (std::getline(manifest, line)) {
        const auto fields = split_tsv(line);
        if (fields.size() != 9 || fields[1] != "doc") {
            continue;
        }
        const size_t row = std::stoull(fields[3]);
        if (fields[0] == "cell") {
            fixture.cells.emplace(std::pair {row, fields[4]}, fields[7]);
        } else if (fields[0] == "assembled") {
            fixture.assembled.resize(std::max(fixture.assembled.size(), row + 1));
            fixture.assembled[row] = fields[8];
        }
    }
    return fixture;
}

TabletSchemaSPtr schema_with_typed_int(std::string_view path) {
    TabletColumn parent;
    parent.set_unique_id(1);
    parent.set_name("v");
    parent.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    parent.set_variant_enable_typed_paths_to_sparse(true);

    ColumnPB typed_pb;
    typed_pb.set_unique_id(-1);
    typed_pb.set_name(std::string(path));
    typed_pb.set_type("INT");
    typed_pb.set_is_nullable(true);
    typed_pb.set_pattern_type(PatternTypePB::MATCH_NAME);
    TabletColumn typed;
    typed.init_from_pb(typed_pb);
    parent.add_sub_column(typed);

    auto schema = std::make_shared<TabletSchema>();
    schema->append_column(std::move(parent));
    return schema;
}

const VariantPathColumn& only_materialized(const VariantShreddedColumns& result) {
    EXPECT_EQ(result.materialized.size(), 1);
    return result.materialized.front();
}

} // namespace

TEST(VariantDocShredderTest, C3DocCellsStatisticsAndAssemblerMatchCommittedManifest) {
    const C3DocFixture fixture = load_c3_doc_fixture();
    ASSERT_EQ(fixture.source.size(), 8);
    ASSERT_EQ(fixture.cells.size(), 40);
    auto input = encoded_jsons(fixture.source);
    VariantShreddedColumns result =
            shred(*input, {.physical_layout = VariantShredderPhysicalLayout::DOC,
                           .doc_materialization_min_rows = fixture.source.size() + 1});

    ASSERT_EQ(result.doc_buckets.size(), 1);
    const auto& root = assert_cast<const ColumnNullable&>(*result.root_jsonb);
    EXPECT_EQ(root.get_null_map_data(), (NullMap(fixture.source.size(), 1)));
    std::map<std::pair<size_t, std::string>, std::string> actual;
    for (const DocCell& cell : doc_cells(result)) {
        actual.emplace(std::pair {cell.row, cell.path}, cell.raw);
    }
    ASSERT_EQ(actual.size(), fixture.cells.size());
    for (const auto& [key, bytes] : fixture.cells) {
        const auto found = actual.find(key);
        ASSERT_NE(found, actual.end()) << key.first << ":" << key.second;
        EXPECT_EQ(found->second, bytes) << key.first << ":" << key.second;
    }
    const std::map<std::string, uint32_t> expected_stats = {
            {"flag", 8}, {"id", 8}, {"name", 8}, {"nested.x", 8}, {"ratio", 8},
    };
    EXPECT_EQ(result.statistics.doc_value_column_non_null_size, expected_stats);
    EXPECT_EQ(result.doc_buckets[0].statistics.doc_value_column_non_null_size, expected_stats);

    VariantAssembledColumn assembled = assemble_doc(result);
    ASSERT_EQ(assembled.values->size(), fixture.assembled.size());
    const auto view = assembled.values->read_view();
    for (size_t row = 0; row < fixture.assembled.size(); ++row) {
        const std::string actual_row = print_json(view.value_at(row));
        EXPECT_EQ(actual_row, fixture.assembled[row]);
    }
}

TEST(VariantDocShredderTest, ThresholdKeepsDocBytesAndTypedConversionHappensAfterEncoding) {
    auto input = encoded_jsons({R"({"id":7})", R"({"id":8})"});
    auto schema = schema_with_typed_int("id");
    VariantShredderOptions low {.tablet_schema = schema.get(),
                                .parent_column_unique_id = 1,
                                .physical_layout = VariantShredderPhysicalLayout::DOC,
                                .typed_paths_to_sparse = true,
                                .doc_materialization_min_rows = 0};
    VariantShredderOptions high = low;
    high.doc_materialization_min_rows = input->size() + 1;
    const VariantShreddedColumns materialized = shred(*input, low);
    const VariantShreddedColumns doc_only = shred(*input, high);

    EXPECT_EQ(doc_cells(materialized), doc_cells(doc_only));
    ASSERT_EQ(doc_cells(materialized).size(), 2);
    EXPECT_EQ(doc_cells(materialized)[0].raw, "070700000000000000");
    EXPECT_EQ(doc_cells(materialized)[1].raw, "070800000000000000");
    EXPECT_TRUE(doc_only.materialized.empty());
    const VariantPathColumn& id = only_materialized(materialized);
    EXPECT_TRUE(id.is_typed_path);
    EXPECT_EQ(remove_nullable(id.type)->get_primitive_type(), TYPE_INT);
    const auto& nullable = assert_cast<const ColumnNullable&>(*id.column);
    EXPECT_EQ(assert_cast<const ColumnInt32&>(nullable.get_nested_column()).get_data(),
              (PaddedPODArray<int32_t> {7, 8}));
    EXPECT_EQ(materialized.statistics.doc_value_column_non_null_size.at("id"), 2);
    EXPECT_EQ(materialized.statistics.subcolumns_non_null_size.at("id"), 2);

    const VariantAssembledColumn assembled = assemble_doc(materialized);
    EXPECT_EQ(print_json(assembled.values->read_view().value_at(0)), R"({"id":7})");
}

TEST(VariantDocShredderTest, RawDottedOrderAndBucketRoutingAreCompiledOncePerPath) {
    auto input = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"a", 1});
        auto nested = row.start_object();
        nested.add_key({"b", 1});
        row.add_int(1);
        nested.add_key({"c", 1});
        row.add_int(2);
        nested.finish();
        object.add_key({"a-", 2});
        row.add_int(3);
        object.add_key({"z", 1});
        row.add_int(4);
        object.finish();
    }});
    constexpr uint32_t buckets = 3;
    const VariantShreddedColumns result =
            shred(*input, {.physical_layout = VariantShredderPhysicalLayout::DOC,
                           .doc_bucket_count = buckets,
                           .doc_materialization_min_rows = 2});
    std::vector<std::string> paths;
    for (size_t bucket = 0; bucket < result.doc_buckets.size(); ++bucket) {
        const auto& map = assert_cast<const ColumnMap&>(*result.doc_buckets[bucket].column);
        const auto& keys = assert_cast<const ColumnString&>(map.get_keys());
        std::string previous;
        for (size_t index = 0; index < keys.size(); ++index) {
            const std::string path = keys.get_data_at(index).to_string();
            EXPECT_TRUE(previous.empty() || previous < path);
            EXPECT_EQ(variant_util::variant_binary_shard_of({path.data(), path.size()}, buckets),
                      bucket);
            previous = path;
            paths.push_back(path);
        }
    }
    std::sort(paths.begin(), paths.end());
    EXPECT_EQ(paths, (std::vector<std::string> {"a-", "a.b", "a.c", "z"}));
}

TEST(VariantDocShredderTest, ScalarArrayEmptyObjectJsonNullAndOuterNullStayDistinct) {
    auto input = encoded_rows({
            [](auto& row) { row.add_int(42); },
            [](auto& row) {
                auto array = row.start_array();
                row.add_int(1);
                row.add_null();
                row.add_int(2);
                array.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"n", 1});
                row.add_null();
                object.add_key({"v", 1});
                row.add_int(1);
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"masked", 6});
                row.add_int(9);
                object.finish();
            },
    });
    const std::array<uint8_t, 5> outer_nulls = {0, 0, 0, 0, 1};
    const VariantShreddedColumns result =
            shred(*input,
                  {.physical_layout = VariantShredderPhysicalLayout::DOC,
                   .doc_materialization_min_rows = 6},
                  outer_nulls);
    const auto& root = assert_cast<const ColumnNullable&>(*result.root_jsonb);
    EXPECT_EQ(root.get_null_map_data(), (NullMap {0, 0, 1, 1, 1}));
    const std::vector<DocCell> cells = doc_cells(result);
    ASSERT_EQ(cells.size(), 1);
    EXPECT_EQ(cells[0].row, 3);
    EXPECT_EQ(cells[0].path, "v");

    const VariantAssembledColumn assembled = assemble_doc(result, outer_nulls);
    const auto view = assembled.values->read_view();
    EXPECT_EQ(print_json(view.value_at(0)), "42");
    EXPECT_EQ(print_json(view.value_at(1)), "[1,null,2]");
    EXPECT_EQ(print_json(view.value_at(2)), "{}");
    EXPECT_EQ(print_json(view.value_at(3)), R"({"v":1})");
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*assembled.outer_nulls).get_data(),
              (PaddedPODArray<uint8_t> {0, 0, 0, 0, 1}));
}

TEST(VariantDocShredderTest, JsonNullPathsAreOmittedFromDocCellsAcrossPromotions) {
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"null_then_scalar", 16});
                row.add_null();
                object.add_key({"scalar_then_null", 16});
                row.add_int(1);
                object.add_key({"present_null", 12});
                row.add_null();
                object.add_key({"present", 7});
                row.add_int(1);
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"null_then_scalar", 16});
                row.add_int(2);
                object.add_key({"scalar_then_null", 16});
                row.add_null();
                object.add_key({"present", 7});
                row.add_int(1);
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"present", 7});
                row.add_int(9);
                object.finish();
            },
    });
    const std::array<uint8_t, 3> outer_nulls = {0, 0, 1};
    const VariantShreddedColumns result =
            shred(*input,
                  {.physical_layout = VariantShredderPhysicalLayout::DOC,
                   .doc_materialization_min_rows = input->size() + 1},
                  outer_nulls);

    std::map<std::pair<size_t, std::string>, std::string> cells;
    for (const DocCell& cell : doc_cells(result)) {
        cells.emplace(std::pair {cell.row, cell.path}, cell.raw);
    }
    EXPECT_EQ(cells.at({0, "scalar_then_null"}).substr(0, 2), "07");
    EXPECT_EQ(cells.at({0, "present"}).substr(0, 2), "07");
    EXPECT_EQ(cells.at({1, "null_then_scalar"}).substr(0, 2), "07");
    EXPECT_EQ(cells.at({1, "present"}).substr(0, 2), "07");
    EXPECT_FALSE(cells.contains({0, "null_then_scalar"}));
    EXPECT_FALSE(cells.contains({0, "present_null"}));
    EXPECT_FALSE(cells.contains({1, "scalar_then_null"}));
    EXPECT_FALSE(cells.contains({1, "present_null"}));
    EXPECT_EQ(cells.lower_bound({2, ""}), cells.end());

    EXPECT_EQ(result.statistics.doc_value_column_non_null_size.at("null_then_scalar"), 1);
    EXPECT_EQ(result.statistics.doc_value_column_non_null_size.at("scalar_then_null"), 1);
    EXPECT_FALSE(result.statistics.doc_value_column_non_null_size.contains("present_null"));
    EXPECT_EQ(result.statistics.doc_value_column_non_null_size.at("present"), 2);
    const VariantAssembledColumn assembled = assemble_doc(result, outer_nulls);
    const auto view = assembled.values->read_view();
    EXPECT_EQ(print_json(view.value_at(0)), R"({"present":1,"scalar_then_null":1})");
    EXPECT_EQ(print_json(view.value_at(1)), R"({"null_then_scalar":2,"present":1})");
    const auto& actual_outer_nulls =
            assert_cast<const ColumnUInt8&>(*assembled.outer_nulls).get_data();
    ASSERT_EQ(actual_outer_nulls.size(), outer_nulls.size());
    for (size_t row = 0; row < outer_nulls.size(); ++row) {
        EXPECT_EQ(actual_outer_nulls[row], outer_nulls[row]);
    }
}

TEST(VariantDocShredderTest, EmptyArrayWithUnresolvedElementIsOmittedFromDocBinary) {
    auto input = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"a", 1});
        auto array = row.start_array();
        array.finish();
        object.add_key({"sibling", 7});
        row.add_int(1);
        object.finish();
    }});
    const VariantShreddedColumns result =
            shred(*input, {.physical_layout = VariantShredderPhysicalLayout::DOC,
                           .doc_materialization_min_rows = 0});
    ASSERT_EQ(result.materialized.size(), 1);
    EXPECT_EQ(result.materialized[0].path.get_path(), "sibling");
    const std::vector<DocCell> cells = doc_cells(result);
    ASSERT_EQ(cells.size(), 1);
    EXPECT_EQ(cells[0].path, "sibling");

    const VariantAssembledColumn assembled = assemble_doc(result);
    const auto actual = assembled.values->read_view();
    EXPECT_EQ(print_json(actual.value_at(0)), R"({"sibling":1})");
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): GTest macros inflate this table test.
TEST(VariantDocShredderTest, MultipleMetadataAppendsPreserveArrayAndJsonbCells) {
    auto first = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"arr", 3});
                auto arr = row.start_array();
                row.add_int(1);
                row.add_null();
                row.add_int(2);
                arr.finish();
                object.add_key({"shape", 5});
                auto shape = row.start_array();
                row.add_int(3);
                shape.finish();
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"shape", 5});
                auto shape = row.start_array();
                auto inner = row.start_array();
                row.add_int(5);
                inner.finish();
                shape.finish();
                object.add_key({"arr", 3});
                auto arr = row.start_array();
                row.add_int(4);
                arr.finish();
                object.finish();
            },
    });
    auto second = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"objects", 7});
                auto objects = row.start_array();
                auto element = row.start_object();
                element.add_key({"x", 1});
                row.add_int(1);
                element.finish();
                objects.finish();
                object.add_key({"arr", 3});
                auto arr = row.start_array();
                row.add_int(6);
                row.add_int(7);
                arr.finish();
                object.add_key({"nested", 6});
                auto nested = row.start_array();
                auto left = row.start_array();
                row.add_int(8);
                left.finish();
                auto right = row.start_array();
                row.add_int(9);
                row.add_null();
                right.finish();
                nested.finish();
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"nested", 6});
                auto nested = row.start_array();
                auto inner = row.start_array();
                row.add_int(10);
                row.add_int(11);
                inner.finish();
                nested.finish();
                object.add_key({"objects", 7});
                auto objects = row.start_array();
                auto left = row.start_object();
                left.add_key({"x", 1});
                row.add_int(2);
                left.finish();
                auto right = row.start_object();
                right.add_key({"x", 1});
                row.add_int(3);
                right.finish();
                objects.finish();
                object.add_key({"arr", 3});
                auto arr = row.start_array();
                arr.finish();
                object.finish();
            },
    });

    VariantShredder shredder({.physical_layout = VariantShredderPhysicalLayout::DOC,
                              .doc_materialization_min_rows = 5});
    ASSERT_TRUE(shredder.append(first->read_view(), 0, first->size()).ok());
    ASSERT_TRUE(shredder.append(second->read_view(), 0, second->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(result.doc_buckets.size(), 1);
    const auto& map = assert_cast<const ColumnMap&>(*result.doc_buckets[0].column);
    EXPECT_EQ(map.get_offsets(), (ColumnArray::Offsets64 {2, 4, 7, 10}));
    const std::map<std::string, uint32_t> expected_statistics = {
            {"arr", 4}, {"nested", 2}, {"objects", 2}, {"shape", 2}};
    EXPECT_EQ(result.statistics.doc_value_column_non_null_size, expected_statistics);
    EXPECT_EQ(result.doc_buckets[0].statistics.doc_value_column_non_null_size, expected_statistics);
    EXPECT_EQ(result.debug.metadata_plans, 2);

    std::map<std::pair<size_t, std::string>, std::string> raw;
    for (const DocCell& cell : doc_cells(result)) {
        raw.emplace(std::pair {cell.row, cell.path}, cell.raw);
    }
    EXPECT_EQ(raw.at({0, "arr"}), "13030000000000000007010000000000000016070200000000000000");
    EXPECT_EQ(raw.at({2, "nested"}).substr(0, 2), "13");
    EXPECT_EQ(raw.at({2, "nested"}).substr(18, 2), "13");
    EXPECT_EQ(raw.at({2, "objects"}).substr(0, 2), "13");
    EXPECT_EQ(raw.at({2, "objects"}).substr(18, 2), "22");
    EXPECT_EQ(raw.at({0, "shape"}).substr(0, 2), "22");
    EXPECT_EQ(raw.at({1, "shape"}).substr(0, 2), "22");

    const VariantAssembledColumn assembled = assemble_doc(result);
    auto expected =
            encoded_jsons({R"({"arr":[1,null,2],"shape":[3]})", R"({"arr":[4],"shape":[[5]]})",
                           R"({"arr":[6,7],"nested":[[8],[9,null]],"objects":[{"x":1}]})",
                           R"({"arr":[],"nested":[[10,11]],"objects":[{"x":2},{"x":3}]})"});
    const auto actual_view = assembled.values->read_view();
    const auto expected_view = expected->read_view();
    for (size_t row = 0; row < result.num_rows; ++row) {
        EXPECT_TRUE(canonical_equals(actual_view.value_at(row), expected_view.value_at(row)));
    }
}

TEST(VariantDocShredderTest, BadPathsFailAtomicallyAndRetainFirstError) {
    auto collision = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"a", 1});
        auto nested = row.start_object();
        nested.add_key({"b", 1});
        row.add_int(1);
        nested.finish();
        object.add_key({"a.b", 3});
        row.add_int(2);
        object.finish();
    }});
    VariantShredder shredder({.physical_layout = VariantShredderPhysicalLayout::DOC});
    const Status append_status = shredder.append(collision->read_view(), 0, collision->size());
    ASSERT_FALSE(append_status.ok());
    VariantShreddedColumns output;
    output.num_rows = 99;
    const Status first = shredder.finish(&output);
    EXPECT_FALSE(first.ok());
    EXPECT_EQ(first.to_string(), append_status.to_string());
    EXPECT_EQ(output.num_rows, 99);
    const Status second = shredder.finish(&output);
    EXPECT_EQ(second.to_string(), first.to_string());
    EXPECT_EQ(output.num_rows, 99);

    auto literal_dot = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"x.y", 3});
        row.add_int(1);
        object.finish();
    }});
    VariantShredder dot_shredder({.physical_layout = VariantShredderPhysicalLayout::DOC});
    ASSERT_TRUE(dot_shredder.append(literal_dot->read_view(), 0, literal_dot->size()).ok());
    VariantShreddedColumns dotted_output;
    const Status dotted_status = dot_shredder.finish(&dotted_output);
    ASSERT_TRUE(dotted_status.ok()) << dotted_status.to_string();
    ASSERT_EQ(dotted_output.materialized.size(), 1);
    ASSERT_EQ(dotted_output.materialized[0].path.get_parts().size(), 2);
    EXPECT_EQ(dotted_output.materialized[0].path.get_parts()[0].key, "x");
    EXPECT_EQ(dotted_output.materialized[0].path.get_parts()[1].key, "y");
    const VariantAssembledColumn dotted_assembled = assemble_doc(dotted_output);
    EXPECT_EQ(print_json(dotted_assembled.values->read_view().value_at(0)), R"({"x":{"y":1}})");

    auto empty_part = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"", 0});
        row.add_int(1);
        object.finish();
    }});
    VariantShredder empty_shredder({.physical_layout = VariantShredderPhysicalLayout::DOC});
    const Status empty_status =
            empty_shredder.append(empty_part->read_view(), 0, empty_part->size());
    EXPECT_FALSE(empty_status.ok());
    VariantShreddedColumns ignored;
    EXPECT_EQ(empty_shredder.finish(&ignored).to_string(), empty_status.to_string());
}

} // namespace doris::segment_v2
