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

#include "storage/segment/variant/variant_shredder.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <string_view>
#include <tuple>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_canonical.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "storage/segment/variant/variant_assembler.h"
#include "storage/tablet/tablet_schema.h"
#include "util/jsonb_utils.h"

namespace doris::segment_v2 {
namespace {

using RowWriter = std::function<void(VariantBatchBuilder::Row&)>;

struct StringWriter {
    void write(const char* data, size_t size) { value.append(data, size); }
    std::string value;
};

std::string print_json(VariantRef value) {
    StringWriter writer;
    to_json(value, writer);
    return writer.value;
}

ColumnVariantV2::MutablePtr encoded_rows(std::initializer_list<RowWriter> writers) {
    VariantBatchBuilder builder(
            VariantBatchBuilder::ReserveHint {.rows = writers.size(), .metadata_keys = 8});
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

const VariantPathColumn& path_column(const VariantShreddedColumns& result, std::string_view path) {
    const auto found = std::find_if(
            result.materialized.begin(), result.materialized.end(),
            [&](const VariantPathColumn& column) { return column.path.get_path() == path; });
    EXPECT_NE(found, result.materialized.end()) << path;
    return *found;
}

void add_one_key_int(VariantBatchBuilder::Row& row, std::string_view key, int64_t value) {
    auto object = row.start_object();
    object.add_key({key.data(), key.size()});
    row.add_int(value);
    object.finish();
}

std::string root_json_at(const VariantShreddedColumns& result, size_t row) {
    const auto& nullable = assert_cast<const ColumnNullable&>(*result.root_jsonb);
    EXPECT_FALSE(nullable.is_null_at(row));
    const StringRef jsonb =
            assert_cast<const ColumnString&>(nullable.get_nested_column()).get_data_at(row);
    return JsonbToJson::jsonb_to_json_string(jsonb.data, jsonb.size);
}

TabletSchemaSPtr schema_with_typed_int(std::string_view path, bool typed_to_sparse) {
    TabletColumn parent;
    parent.set_unique_id(1);
    parent.set_name("v");
    parent.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    parent.set_variant_enable_typed_paths_to_sparse(typed_to_sparse);

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

TabletSchemaSPtr schema_with_typed_float(std::string_view path, bool typed_to_sparse) {
    TabletColumn parent;
    parent.set_unique_id(1);
    parent.set_name("v");
    parent.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    parent.set_variant_enable_typed_paths_to_sparse(typed_to_sparse);

    ColumnPB typed_pb;
    typed_pb.set_unique_id(-1);
    typed_pb.set_name(std::string(path));
    typed_pb.set_type("FLOAT");
    typed_pb.set_is_nullable(true);
    typed_pb.set_pattern_type(PatternTypePB::MATCH_NAME);
    TabletColumn typed;
    typed.init_from_pb(typed_pb);
    parent.add_sub_column(typed);

    auto schema = std::make_shared<TabletSchema>();
    schema->append_column(std::move(parent));
    return schema;
}

TabletSchemaSPtr schema_with_typed_string(std::string_view path,
                                          PatternTypePB pattern_type = PatternTypePB::MATCH_NAME) {
    TabletColumn parent;
    parent.set_unique_id(1);
    parent.set_name("v");
    parent.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);

    ColumnPB typed_pb;
    typed_pb.set_unique_id(-1);
    typed_pb.set_name(std::string(path));
    typed_pb.set_type("STRING");
    typed_pb.set_is_nullable(true);
    typed_pb.set_pattern_type(pattern_type);
    TabletColumn typed;
    typed.init_from_pb(typed_pb);
    parent.add_sub_column(typed);

    auto schema = std::make_shared<TabletSchema>();
    schema->append_column(std::move(parent));
    return schema;
}

TabletSchemaSPtr schema_with_typed_int_array(std::string_view path) {
    TabletColumn parent;
    parent.set_unique_id(1);
    parent.set_name("v");
    parent.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);

    ColumnPB typed_pb;
    typed_pb.set_unique_id(-1);
    typed_pb.set_name(std::string(path));
    typed_pb.set_type("ARRAY");
    typed_pb.set_is_nullable(true);
    typed_pb.set_pattern_type(PatternTypePB::MATCH_NAME);
    auto* element = typed_pb.add_children_columns();
    element->set_unique_id(-1);
    element->set_name("element");
    element->set_type("INT");
    element->set_is_nullable(true);
    TabletColumn typed;
    typed.init_from_pb(typed_pb);
    parent.add_sub_column(typed);

    auto schema = std::make_shared<TabletSchema>();
    schema->append_column(std::move(parent));
    return schema;
}

TabletSchemaSPtr schema_with_typed_temporal_paths() {
    TabletColumn parent;
    parent.set_unique_id(1);
    parent.set_name("v");
    parent.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);

    ColumnPB date_pb;
    date_pb.set_unique_id(-1);
    date_pb.set_name("a");
    date_pb.set_type("DATEV2");
    date_pb.set_is_nullable(true);
    date_pb.set_pattern_type(PatternTypePB::MATCH_NAME);
    TabletColumn date;
    date.init_from_pb(date_pb);
    parent.add_sub_column(date);

    ColumnPB datetime_pb;
    datetime_pb.set_unique_id(-1);
    datetime_pb.set_name("c");
    datetime_pb.set_type("DATETIMEV2");
    datetime_pb.set_is_nullable(true);
    datetime_pb.set_frac(0);
    datetime_pb.set_pattern_type(PatternTypePB::MATCH_NAME);
    TabletColumn datetime;
    datetime.init_from_pb(datetime_pb);
    parent.add_sub_column(datetime);

    ColumnPB timestamptz_pb;
    timestamptz_pb.set_unique_id(-1);
    timestamptz_pb.set_name("t");
    timestamptz_pb.set_type("TIMESTAMPTZ");
    timestamptz_pb.set_is_nullable(true);
    timestamptz_pb.set_frac(6);
    timestamptz_pb.set_pattern_type(PatternTypePB::MATCH_NAME);
    TabletColumn timestamptz;
    timestamptz.init_from_pb(timestamptz_pb);
    parent.add_sub_column(timestamptz);

    auto schema = std::make_shared<TabletSchema>();
    schema->append_column(std::move(parent));
    return schema;
}

enum class RoundtripExpectation : uint8_t { CANONICAL, JSON_SURFACE };

void expect_roundtrip(const VariantShreddedColumns& shredded, const ColumnVariantV2& input,
                      RoundtripExpectation expectation = RoundtripExpectation::CANONICAL,
                      std::span<const uint8_t> outer_nulls = {}) {
    VariantAssemblerPlanOptions options;
    options.has_root = true;
    options.sparse_bucket_count = shredded.sparse_buckets.size();
    for (const VariantPathColumn& column : shredded.materialized) {
        options.materialized_paths.push_back({.path = column.path, .type = column.type});
    }
    std::shared_ptr<const VariantAssemblerPlan> plan;
    ASSERT_TRUE(VariantAssemblerPlan::create(std::move(options), &plan).ok());

    DorisVector<const IColumn*> materialized;
    for (const VariantPathColumn& column : shredded.materialized) {
        materialized.push_back(column.column.get());
    }
    DorisVector<const ColumnMap*> sparse;
    for (const VariantShreddedColumns::SparseBucket& bucket : shredded.sparse_buckets) {
        sparse.push_back(&assert_cast<const ColumnMap&>(*bucket.column));
    }
    VariantAssemblerBatchView batch;
    batch.num_rows = shredded.num_rows;
    batch.outer_nulls = outer_nulls;
    batch.root_jsonb = shredded.root_jsonb.get();
    batch.materialized_columns = materialized;
    batch.sparse_buckets = sparse;
    VariantAssembler assembler(std::move(plan));
    VariantAssembledColumn assembled;
    const Status status = assembler.assemble(batch, &assembled);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto actual = assembled.values->read_view();
    const auto expected = input.read_view();
    ASSERT_EQ(actual.size(), expected.size());
    if (!outer_nulls.empty()) {
        ASSERT_NE(assembled.outer_nulls.get(), nullptr);
        const auto& actual_outer_nulls =
                assert_cast<const ColumnUInt8&>(*assembled.outer_nulls).get_data();
        ASSERT_EQ(actual_outer_nulls.size(), outer_nulls.size());
        for (size_t row = 0; row < outer_nulls.size(); ++row) {
            EXPECT_EQ(actual_outer_nulls[row], outer_nulls[row]);
        }
    }
    for (size_t row = 0; row < actual.size(); ++row) {
        if (!outer_nulls.empty() && outer_nulls[row] != 0) {
            continue;
        }
        if (expectation == RoundtripExpectation::JSON_SURFACE) {
            EXPECT_EQ(print_json(actual.value_at(row)), print_json(expected.value_at(row)));
            continue;
        }
        EXPECT_TRUE(canonical_equals(actual.value_at(row), expected.value_at(row)))
                << "actual=" << print_json(actual.value_at(row))
                << " expected=" << print_json(expected.value_at(row));
    }
}

struct C3SourceEntry {
    std::string path;
    std::string source_type;
    std::string value;
};

struct C3Fixture {
    std::map<size_t, std::vector<C3SourceEntry>> rows;
    std::map<std::pair<size_t, std::string>, std::string> sparse_cells;
    std::set<std::string> materialized_paths;
    uint32_t sparse_bucket_count = 0;
};

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

C3Fixture load_c3_fixture(std::string_view case_id) {
    C3Fixture fixture;
    std::ifstream source(sample_path("source.tsv"));
    EXPECT_TRUE(source.is_open());
    std::string line;
    std::getline(source, line);
    while (std::getline(source, line)) {
        const auto fields = split_tsv(line);
        EXPECT_EQ(fields.size(), 5);
        if (fields.size() != 5) {
            continue;
        }
        if (fields[0] == case_id) {
            fixture.rows[std::stoull(fields[1])].push_back(
                    {.path = fields[2], .source_type = fields[3], .value = fields[4]});
        }
    }

    std::ifstream manifest(sample_path("manifest.tsv"));
    EXPECT_TRUE(manifest.is_open());
    std::getline(manifest, line);
    while (std::getline(manifest, line)) {
        const auto fields = split_tsv(line);
        EXPECT_EQ(fields.size(), 9);
        if (fields.size() != 9) {
            continue;
        }
        if (fields[1] != case_id) {
            continue;
        }
        if (fields[0] == "cell") {
            fixture.sparse_cells.emplace(std::pair {std::stoull(fields[3]), fields[4]}, fields[7]);
        } else if (fields[0] == "column") {
            if (fields[4].starts_with("__DORIS_VARIANT_SPARSE__")) {
                ++fixture.sparse_bucket_count;
            } else if (fields[4] != "$key" && fields[4] != "$variant" &&
                       !fields[4].starts_with("__DORIS_")) {
                fixture.materialized_paths.insert(fields[4]);
            }
        }
    }
    EXPECT_FALSE(fixture.rows.empty());
    EXPECT_GT(fixture.sparse_bucket_count, 0);
    return fixture;
}

bool c3_entry_is_excluded(std::string_view case_id, const C3SourceEntry& entry) {
    if (entry.source_type == "NULL") {
        return true;
    }
    if (case_id == "ordinary_sparse") {
        // The legacy `$` object sidecar is not representable together with the Variant V2 object
        // tree. The scalar `$` row remains directly representable.
        return entry.path == "$" && entry.value != "42";
    }
    return false;
}

void add_c3_value(VariantBatchBuilder::Row& row, const C3SourceEntry& entry) {
    if (entry.path == "$" && entry.source_type == "JSONB" && entry.value == "42") {
        row.add_int(42);
    } else if (entry.source_type == "BIGINT" || entry.source_type == "INT" ||
               entry.source_type == "SMALLINT" || entry.source_type == "TINYINT") {
        row.add_int(std::stoll(entry.value));
    } else if (entry.source_type == "DOUBLE") {
        row.add_double(std::stod(entry.value));
    } else if (entry.source_type == "BOOL") {
        row.add_bool(entry.value == "true");
    } else if (entry.source_type == "STRING") {
        row.add_string({entry.value.data(), entry.value.size()});
    } else if (entry.source_type == "ARRAY") {
        auto array = row.start_array();
        const std::string_view body(entry.value.data() + 1, entry.value.size() - 2);
        const size_t separator = body.find(',');
        row.add_int(std::stoll(std::string(body.substr(0, separator))));
        row.add_int(std::stoll(std::string(body.substr(separator + 1))));
        array.finish();
    } else if (entry.source_type == "JSONB" && entry.path == "object") {
        constexpr std::string_view prefix = R"({"x":)";
        const bool known_fixture_shape =
                entry.value.starts_with(prefix) && entry.value.ends_with('}');
        EXPECT_TRUE(known_fixture_shape) << entry.value;
        if (!known_fixture_shape) {
            row.add_null();
            return;
        }
        auto object = row.start_object();
        object.add_key({"x", 1});
        row.add_int(std::stoll(
                entry.value.substr(prefix.size(), entry.value.size() - prefix.size() - 1)));
        object.finish();
    } else {
        ADD_FAILURE() << "unsupported C3 source type " << entry.source_type;
        row.add_null();
    }
}

ColumnVariantV2::MutablePtr build_c3_input(std::string_view case_id, const C3Fixture& fixture,
                                           size_t* included_path_values) {
    const size_t num_rows = fixture.rows.rbegin()->first + 1;
    VariantBatchBuilder builder(
            VariantBatchBuilder::ReserveHint {.rows = num_rows, .metadata_keys = 16});
    *included_path_values = 0;
    for (size_t row_index = 0; row_index < num_rows; ++row_index) {
        auto row = builder.begin_row();
        const auto& entries = fixture.rows.at(row_index);
        const auto scalar = std::find_if(entries.begin(), entries.end(), [](const auto& entry) {
            return entry.path == "$" && entry.value == "42";
        });
        if (scalar != entries.end()) {
            add_c3_value(row, *scalar);
        } else {
            auto object = row.start_object();
            for (const auto& entry : entries) {
                if (c3_entry_is_excluded(case_id, entry) || entry.path == "$") {
                    continue;
                }
                object.add_key({entry.path.data(), entry.path.size()});
                add_c3_value(row, entry);
                ++*included_path_values;
            }
            object.finish();
        }
        row.finish();
    }
    VariantBatchBuilder block = builder.finish_batch();
    auto input = ColumnVariantV2::create();
    input->insert_encoded_batch(block);
    return input;
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

void expect_c3_sparse_cells_and_statistics(const VariantShreddedColumns& result,
                                           const C3Fixture& fixture) {
    std::map<std::pair<size_t, std::string>, std::string> actual_cells;
    std::map<std::string, uint32_t> expected_statistics;
    std::vector<std::map<std::string, uint32_t>> expected_bucket_statistics(
            fixture.sparse_bucket_count);
    for (const auto& [row_path, raw] : fixture.sparse_cells) {
        ++expected_statistics[row_path.second];
        const size_t bucket = variant_util::variant_binary_shard_of(
                {row_path.second.data(), row_path.second.size()}, fixture.sparse_bucket_count);
        ++expected_bucket_statistics[bucket][row_path.second];
    }

    ASSERT_EQ(result.sparse_buckets.size(), fixture.sparse_bucket_count);
    for (size_t bucket = 0; bucket < result.sparse_buckets.size(); ++bucket) {
        const auto& sparse = result.sparse_buckets[bucket];
        const auto& map = assert_cast<const ColumnMap&>(*sparse.column);
        const auto& keys = assert_cast<const ColumnString&>(map.get_keys());
        const auto& values = assert_cast<const ColumnString&>(map.get_values());
        const auto& offsets = map.get_offsets();
        for (size_t row = 0; row < result.num_rows; ++row) {
            const size_t begin = row == 0 ? 0 : offsets[row - 1];
            const size_t end = offsets[row];
            for (size_t index = begin; index < end; ++index) {
                const std::string path = keys.get_data_at(index).to_string();
                EXPECT_EQ(variant_util::variant_binary_shard_of({path.data(), path.size()},
                                                                fixture.sparse_bucket_count),
                          bucket);
                actual_cells.emplace(std::pair {row, path}, hex(values.get_data_at(index)));
            }
        }
        EXPECT_EQ(sparse.statistics.sparse_column_non_null_size,
                  expected_bucket_statistics[bucket]);
    }
    EXPECT_EQ(actual_cells, fixture.sparse_cells);
    EXPECT_EQ(result.statistics.sparse_column_non_null_size, expected_statistics);
}

VariantShreddedColumns shred_c3_case(std::string_view case_id, const C3Fixture& fixture,
                                     size_t* included_path_values) {
    auto input = build_c3_input(case_id, fixture, included_path_values);
    VariantShredder shredder({.max_subcolumns_count = fixture.materialized_paths.size(),
                              .sparse_bucket_count = fixture.sparse_bucket_count});
    EXPECT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    EXPECT_TRUE(status.ok()) << status.to_string();
    return result;
}

} // namespace

TEST(VariantShredderTest, EmptyInputProducesAnEmptySelfOwnedResult) {
    VariantShredder shredder({});
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(result.num_rows, 0);
}

TEST(VariantShredderTest, MetadataPlansAreReusedAcrossBatchesAndPathsBackfill) {
    auto first = encoded_rows({[](auto& row) { add_one_key_int(row, "a", 1); },
                               [](auto& row) { add_one_key_int(row, "b", 2); }});
    auto second = encoded_rows({[](auto& row) { add_one_key_int(row, "a", 3); },
                                [](auto& row) { add_one_key_int(row, "b", 4); }});

    VariantShredder shredder({});
    ASSERT_TRUE(shredder.append(first->read_view(), 0, first->size()).ok());
    ASSERT_TRUE(shredder.append(second->read_view(), 0, second->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(result.num_rows, 4);
    ASSERT_EQ(result.debug.metadata_plans, 1);
    ASSERT_EQ(result.debug.path_plans, 3);
    const auto& a = assert_cast<const ColumnNullable&>(*path_column(result, "a").column);
    const auto& b = assert_cast<const ColumnNullable&>(*path_column(result, "b").column);
    EXPECT_EQ(a.get_null_map_data(), (NullMap {0, 1, 0, 1}));
    EXPECT_EQ(b.get_null_map_data(), (NullMap {1, 0, 1, 0}));
    EXPECT_EQ(assert_cast<const ColumnInt64&>(a.get_nested_column()).get_data(),
              (PaddedPODArray<int64_t> {1, 0, 3, 0}));
    EXPECT_EQ(assert_cast<const ColumnInt64&>(b.get_nested_column()).get_data(),
              (PaddedPODArray<int64_t> {0, 2, 0, 4}));
}

TEST(VariantShredderTest, PrimitiveLctPromotesColdAndUnpromotableFallsBackToJsonb) {
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"i", 1});
                row.add_int(1);
                object.add_key({"n", 1});
                row.add_int(2);
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"i", 1});
                row.add_int(3);
                object.add_key({"n", 1});
                row.add_double(2.5);
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"i", 1});
                row.add_int(5);
                object.add_key({"n", 1});
                row.add_string({"x", 1});
                object.finish();
            },
    });

    VariantShredder shredder({});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const VariantPathColumn& integers = path_column(result, "i");
    EXPECT_EQ(remove_nullable(integers.type)->get_primitive_type(), TYPE_BIGINT);
    const auto& integer_values = assert_cast<const ColumnInt64&>(
            assert_cast<const ColumnNullable&>(*integers.column).get_nested_column());
    EXPECT_EQ(integer_values.get_data(), (PaddedPODArray<int64_t> {1, 3, 5}));

    const VariantPathColumn& mixed = path_column(result, "n");
    EXPECT_EQ(remove_nullable(mixed.type)->get_primitive_type(), TYPE_JSONB);
    EXPECT_GE(result.debug.promotions, 2);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): GTest macros inflate this table test.
TEST(VariantShredderTest, NumericAndDecimalLctAreInputOrderIndependent) {
    constexpr __int128 wide = (static_cast<__int128>(1) << 80) + 9;
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"d1", 2});
                row.add_decimal(123, 2);
                object.add_key({"d2", 2});
                row.add_int(2);
                object.add_key({"p", 1});
                row.add_double(1.5);
                object.add_key({"q", 1});
                row.add_largeint(wide);
                object.add_key({"s1", 2});
                row.add_decimal(1234, 3);
                object.add_key({"s2", 2});
                row.add_decimal(567, 2);
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"d1", 2});
                row.add_int(2);
                object.add_key({"d2", 2});
                row.add_decimal(123, 2);
                object.add_key({"p", 1});
                row.add_largeint(wide);
                object.add_key({"q", 1});
                row.add_double(1.5);
                object.add_key({"s1", 2});
                row.add_decimal(567, 2);
                object.add_key({"s2", 2});
                row.add_decimal(1234, 3);
                object.finish();
            },
    });
    VariantShredder shredder({});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    EXPECT_EQ(remove_nullable(path_column(result, "p").type)->get_primitive_type(), TYPE_JSONB);
    EXPECT_EQ(remove_nullable(path_column(result, "q").type)->get_primitive_type(), TYPE_JSONB);

    const auto decimal_values = [&](std::string_view path) -> const auto& {
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*path_column(result, path).column);
        return assert_cast<const ColumnDecimal128V3&>(nullable.get_nested_column()).get_data();
    };
    EXPECT_EQ(decimal_values("d1")[0].value, 123);
    EXPECT_EQ(decimal_values("d1")[1].value, 200);
    EXPECT_EQ(decimal_values("d2")[0].value, 200);
    EXPECT_EQ(decimal_values("d2")[1].value, 123);
    EXPECT_EQ(remove_nullable(path_column(result, "s1").type)->get_scale(), 3);
    EXPECT_EQ(remove_nullable(path_column(result, "s2").type)->get_scale(), 3);
    EXPECT_EQ(decimal_values("s1")[0].value, 1234);
    EXPECT_EQ(decimal_values("s1")[1].value, 5670);
    EXPECT_EQ(decimal_values("s2")[0].value, 5670);
    EXPECT_EQ(decimal_values("s2")[1].value, 1234);
}

TEST(VariantShredderTest, ArrayDecimalLctIsElementOrderIndependent) {
    auto input = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"forward", 7});
        auto forward = row.start_array();
        row.add_decimal(1234, 3);
        row.add_decimal(567, 2);
        forward.finish();
        object.add_key({"reverse", 7});
        auto reverse = row.start_array();
        row.add_decimal(567, 2);
        row.add_decimal(1234, 3);
        reverse.finish();
        object.finish();
    }});
    VariantShredder shredder({});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto decimal_elements = [&](std::string_view path) -> const auto& {
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*path_column(result, path).column);
        const auto& array = assert_cast<const ColumnArray&>(nullable.get_nested_column());
        EXPECT_EQ(array.get_offsets(), (ColumnArray::Offsets64 {2}));
        const auto& elements = assert_cast<const ColumnNullable&>(array.get_data());
        EXPECT_EQ(elements.get_null_map_data(), (NullMap {0, 0}));
        return assert_cast<const ColumnDecimal128V3&>(elements.get_nested_column()).get_data();
    };
    const auto array_scale = [&](std::string_view path) {
        const auto& array =
                assert_cast<const DataTypeArray&>(*remove_nullable(path_column(result, path).type));
        return remove_nullable(array.get_nested_type())->get_scale();
    };
    EXPECT_EQ(array_scale("forward"), 3);
    EXPECT_EQ(array_scale("reverse"), 3);
    EXPECT_EQ(decimal_elements("forward")[0].value, 1234);
    EXPECT_EQ(decimal_elements("forward")[1].value, 5670);
    EXPECT_EQ(decimal_elements("reverse")[0].value, 5670);
    EXPECT_EQ(decimal_elements("reverse")[1].value, 1234);
}

TEST(VariantShredderTest, ExactTypedStringPathStringifiesWholeArrayObjectValueOnce) {
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"d", 1});
                auto array = row.start_array();
                auto element = row.start_object();
                element.add_key({"x", 1});
                row.add_int(1);
                element.finish();
                array.finish();
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"d", 1});
                auto array = row.start_array();
                row.add_int(1);
                row.add_int(2);
                array.finish();
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"d", 1});
                auto value = row.start_object();
                value.add_key({"y", 1});
                row.add_int(2);
                value.finish();
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"d", 1});
                auto array = row.start_array();
                array.finish();
                object.finish();
            },
            [](auto& row) { add_one_key_int(row, "other", 2); },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"d", 1});
                auto array = row.start_array();
                row.add_int(9);
                array.finish();
                object.finish();
            },
    });
    auto schema = schema_with_typed_string("d");
    VariantShredder shredder({.tablet_schema = schema.get(), .parent_column_unique_id = 1});
    const std::array<uint8_t, 6> outer_nulls {0, 0, 0, 0, 0, 1};
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size(), outer_nulls).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& path = path_column(result, "d");
    EXPECT_EQ(remove_nullable(path.type)->get_primitive_type(), TYPE_STRING);
    const auto& nullable = assert_cast<const ColumnNullable&>(*path.column);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 0, 0, 0, 1, 1}));
    const auto& strings = assert_cast<const ColumnString&>(nullable.get_nested_column());
    EXPECT_EQ(strings.get_data_at(0).to_string(), R"([{"x":1}])");
    EXPECT_EQ(strings.get_data_at(1).to_string(), "[1,2]");
    EXPECT_EQ(strings.get_data_at(2).to_string(), R"({"y":2})");
    EXPECT_EQ(strings.get_data_at(3).to_string(), "[]");
    EXPECT_EQ(std::ranges::count_if(result.materialized,
                                    [](const auto& candidate) {
                                        return candidate.path.get_path().starts_with("d.");
                                    }),
              0);
    EXPECT_FALSE(result.statistics.subcolumns_non_null_size.contains("d.y"));
    EXPECT_FALSE(result.statistics.sparse_column_non_null_size.contains("d.y"));
}

TEST(VariantShredderTest, GlobTypedStringObjectRecursesAndMatchesFullLeafPaths) {
    auto input = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"string_1_nested", 15});
        auto nested = row.start_object();
        nested.add_key({"message", 7});
        row.add_string({"Hello from nested object", 24});
        nested.add_key({"metadata", 8});
        auto metadata = row.start_object();
        metadata.add_key({"timestamp", 9});
        row.add_string({"2023-10-27T12:00:00Z", 20});
        metadata.finish();
        nested.finish();
        object.finish();
    }});
    auto schema = schema_with_typed_string("string_*", PatternTypePB::MATCH_NAME_GLOB);
    VariantShredder shredder({.tablet_schema = schema.get(), .parent_column_unique_id = 1});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    ASSERT_TRUE(shredder.finish(&result).ok());

    ASSERT_EQ(result.materialized.size(), 2);
    const std::set<std::string> paths = {result.materialized[0].path.get_path(),
                                         result.materialized[1].path.get_path()};
    EXPECT_EQ(paths, (std::set<std::string> {"string_1_nested.message",
                                             "string_1_nested.metadata.timestamp"}));
    for (const auto& path : paths) {
        const auto& column = path_column(result, path);
        EXPECT_TRUE(column.is_typed_path);
        EXPECT_EQ(remove_nullable(column.type)->get_primitive_type(), TYPE_STRING);
    }
    EXPECT_EQ(result.statistics.subcolumns_non_null_size.at("string_1_nested.message"), 1);
    EXPECT_EQ(result.statistics.subcolumns_non_null_size.at("string_1_nested.metadata.timestamp"),
              1);
    expect_roundtrip(result, *input);
}

TEST(VariantShredderTest, TypedStringPathStringifiesArrayColumnWithoutElementStringification) {
    auto input = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"d", 1});
        auto array = row.start_array();
        auto element = row.start_object();
        element.add_key({"x", 1});
        row.add_int(1);
        element.finish();
        array.finish();
        object.finish();
    }});
    auto schema = schema_with_typed_string("d");
    VariantShredder shredder({.tablet_schema = schema.get(), .parent_column_unique_id = 1});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    ASSERT_TRUE(shredder.finish(&result).ok());

    const auto& path = path_column(result, "d");
    EXPECT_EQ(remove_nullable(path.type)->get_primitive_type(), TYPE_STRING);
    const auto& nullable = assert_cast<const ColumnNullable&>(*path.column);
    EXPECT_EQ(assert_cast<const ColumnString&>(nullable.get_nested_column())
                      .get_data_at(0)
                      .to_string(),
              R"([{"x":1}])");
}

TEST(VariantShredderTest, NonStringPredefinedObjectStillRecursesIntoChildren) {
    auto input = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"d", 1});
        auto value = row.start_object();
        value.add_key({"y", 1});
        row.add_int(2);
        value.finish();
        object.finish();
    }});
    auto schema = schema_with_typed_int("d", false);
    VariantShredder shredder({.tablet_schema = schema.get(),
                              .parent_column_unique_id = 1,
                              .max_subcolumns_count = 1});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    ASSERT_TRUE(shredder.finish(&result).ok());

    ASSERT_EQ(result.materialized.size(), 1);
    EXPECT_EQ(result.materialized[0].path.get_path(), "d.y");
    EXPECT_FALSE(result.materialized[0].is_typed_path);
    EXPECT_EQ(remove_nullable(result.materialized[0].type)->get_primitive_type(), TYPE_BIGINT);
}

TEST(VariantShredderTest, OrdinaryObjectRowsOmitRootAndReassembleFromLeaves) {
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"a", 1});
                row.add_int(7);
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.finish();
            },
    });
    VariantShredder shredder({});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto& root = assert_cast<const ColumnNullable&>(*result.root_jsonb);
    EXPECT_EQ(root.get_null_map_data(), (NullMap {1, 1}));
    expect_roundtrip(result, *input);
}

TEST(VariantShredderTest, OuterNullJsonNullMissingAndScalarRootRemainDistinct) {
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"a", 1});
                row.add_null();
                object.add_key({"b", 1});
                row.add_int(1);
                object.finish();
            },
            [](auto& row) { add_one_key_int(row, "b", 2); },
            [](auto& row) { row.add_int(42); },
            [](auto& row) { row.add_null(); },
            [](auto& row) {
                auto array = row.start_array();
                row.add_int(1);
                row.add_null();
                row.add_int(2);
                array.finish();
            },
    });
    const std::array<uint8_t, 5> outer_nulls = {0, 1, 0, 0, 0};
    VariantShredder shredder({});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size(), outer_nulls).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(result.materialized.size(), 1);
    EXPECT_EQ(result.materialized[0].path.get_path(), "b");
    const auto& b = assert_cast<const ColumnNullable&>(*result.materialized[0].column);
    EXPECT_EQ(b.get_null_map_data(), (NullMap {0, 1, 1, 1, 1}));

    const auto& root = assert_cast<const ColumnNullable&>(*result.root_jsonb);
    EXPECT_EQ(root.get_null_map_data(), (NullMap {1, 1, 0, 0, 0}));
    EXPECT_EQ(root_json_at(result, 2), "42");
    EXPECT_EQ(root_json_at(result, 3), "null");
    EXPECT_EQ(root_json_at(result, 4), "[1,null,2]");
    EXPECT_FALSE(result.statistics.subcolumns_non_null_size.contains(""));
    EXPECT_FALSE(result.statistics.sparse_column_non_null_size.contains(""));
}

TEST(VariantShredderTest, JsonNullPathsAreOmittedFromSparseCellsAcrossPromotions) {
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
    VariantShredder shredder({.max_subcolumns_count = 1});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size(), outer_nulls).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    std::map<std::pair<size_t, std::string>, std::string> cells;
    for (const auto& bucket : result.sparse_buckets) {
        const auto& map = assert_cast<const ColumnMap&>(*bucket.column);
        const auto& keys = assert_cast<const ColumnString&>(map.get_keys());
        const auto& values = assert_cast<const ColumnString&>(map.get_values());
        for (size_t row = 0; row < result.num_rows; ++row) {
            const size_t begin = row == 0 ? 0 : map.get_offsets()[row - 1];
            const size_t end = map.get_offsets()[row];
            for (size_t index = begin; index < end; ++index) {
                cells.emplace(std::pair {row, keys.get_data_at(index).to_string()},
                              values.get_data_at(index).to_string());
            }
        }
    }
    const auto expect_cell_type = [&](size_t row, std::string_view path, FieldType type) {
        const auto found = cells.find({row, std::string(path)});
        ASSERT_NE(found, cells.end()) << path;
        ASSERT_FALSE(found->second.empty()) << path;
        EXPECT_EQ(static_cast<uint8_t>(found->second.front()), static_cast<uint8_t>(type)) << path;
    };
    expect_cell_type(0, "scalar_then_null", FieldType::OLAP_FIELD_TYPE_BIGINT);
    expect_cell_type(1, "null_then_scalar", FieldType::OLAP_FIELD_TYPE_BIGINT);
    EXPECT_FALSE(cells.contains({0, "null_then_scalar"}));
    EXPECT_FALSE(cells.contains({0, "present_null"}));
    EXPECT_FALSE(cells.contains({1, "scalar_then_null"}));
    EXPECT_FALSE(cells.contains({1, "present_null"}));
    EXPECT_EQ(cells.lower_bound({2, ""}), cells.end());

    EXPECT_EQ(result.statistics.sparse_column_non_null_size.at("null_then_scalar"), 1);
    EXPECT_EQ(result.statistics.sparse_column_non_null_size.at("scalar_then_null"), 1);
    EXPECT_FALSE(result.statistics.sparse_column_non_null_size.contains("present_null"));
    EXPECT_FALSE(result.statistics.subcolumns_non_null_size.contains("present_null"));
    EXPECT_EQ(result.statistics.subcolumns_non_null_size.at("present"), 2);
    const auto& root = assert_cast<const ColumnNullable&>(*result.root_jsonb);
    EXPECT_EQ(root.get_null_map_data(), (NullMap {1, 1, 1}));
    auto expected = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"scalar_then_null", 16});
                row.add_int(1);
                object.add_key({"present", 7});
                row.add_int(1);
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"null_then_scalar", 16});
                row.add_int(2);
                object.add_key({"present", 7});
                row.add_int(1);
                object.finish();
            },
            [](auto& row) { add_one_key_int(row, "present", 9); },
    });
    expect_roundtrip(result, *expected, RoundtripExpectation::CANONICAL, outer_nulls);
}

TEST(VariantShredderTest, TypedPathJsonNullUsesExistingNullableRule) {
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"typed", 5});
                row.add_null();
                object.finish();
            },
            [](auto& row) { add_one_key_int(row, "typed", 7); },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"typed", 5});
                row.add_null();
                object.finish();
            },
            [](auto& row) { add_one_key_int(row, "present", 1); },
    });
    auto schema = schema_with_typed_int("typed", false);
    VariantShredder shredder({.tablet_schema = schema.get(), .parent_column_unique_id = 1});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const VariantPathColumn& typed = path_column(result, "typed");
    EXPECT_TRUE(typed.is_typed_path);
    EXPECT_EQ(remove_nullable(typed.type)->get_primitive_type(), TYPE_INT);
    EXPECT_EQ(typed.non_null_rows, 1);
    const auto& nullable = assert_cast<const ColumnNullable&>(*typed.column);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {1, 0, 1, 1}));
    EXPECT_EQ(assert_cast<const ColumnInt32&>(nullable.get_nested_column()).get_data(),
              (PaddedPODArray<int32_t> {0, 7, 0, 0}));
    EXPECT_EQ(result.statistics.subcolumns_non_null_size.at("typed"), 1);
    auto expected = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.finish();
            },
            [](auto& row) { add_one_key_int(row, "typed", 7); },
            [](auto& row) {
                auto object = row.start_object();
                object.finish();
            },
            [](auto& row) { add_one_key_int(row, "present", 1); },
    });
    expect_roundtrip(result, *expected);
}

TEST(VariantShredderTest, AllJsonNullFixedTypedPathStaysMaterialized) {
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"typed", 5});
                row.add_null();
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"typed", 5});
                row.add_null();
                object.finish();
            },
    });
    auto schema = schema_with_typed_int("typed", false);
    VariantShredder shredder({.tablet_schema = schema.get(), .parent_column_unique_id = 1});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    ASSERT_TRUE(shredder.finish(&result).ok());

    ASSERT_EQ(result.materialized.size(), 1);
    const VariantPathColumn& typed = result.materialized[0];
    EXPECT_EQ(typed.path.get_path(), "typed");
    EXPECT_TRUE(typed.is_typed_path);
    EXPECT_EQ(remove_nullable(typed.type)->get_primitive_type(), TYPE_INT);
    EXPECT_EQ(typed.non_null_rows, 0);
    const auto& nullable = assert_cast<const ColumnNullable&>(*typed.column);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {1, 1}));
    EXPECT_EQ(result.statistics.subcolumns_non_null_size.at("typed"), 0);
}

TEST(VariantShredderTest, LogicalRootPathRestoresExactTypedCompactionLeaf) {
    constexpr float value = 8.12F;
    auto input = encoded_rows(
            {[](auto& row) { row.add_null(); }, [](auto& row) { row.add_float(value); }});
    auto schema = schema_with_typed_float("float_1", false);
    VariantShredder shredder({.tablet_schema = schema.get(),
                              .parent_column_unique_id = 1,
                              .logical_root_path = PathInData("float_1")});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    ASSERT_TRUE(shredder.finish(&result).ok());

    ASSERT_EQ(result.materialized.size(), 1);
    const auto& path = result.materialized[0];
    EXPECT_EQ(path.path.get_path(), "float_1");
    EXPECT_TRUE(path.is_typed_path);
    EXPECT_EQ(remove_nullable(path.type)->get_primitive_type(), TYPE_FLOAT);
    EXPECT_EQ(path.non_null_rows, 1);
    const auto& nullable = assert_cast<const ColumnNullable&>(*path.column);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {1, 0}));
    const auto& floats = assert_cast<const ColumnFloat32&>(nullable.get_nested_column());
    EXPECT_FLOAT_EQ(floats.get_data()[1], value);
    EXPECT_EQ(result.statistics.subcolumns_non_null_size.at("float_1"), 1);
}

TEST(VariantShredderTest, LogicalRootTypedSparseKeepsFloatBitsOffsetsAndStats) {
    constexpr float value = 8.12F;
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"float_1", 7});
                row.add_float(value);
                object.add_key({"hot", 3});
                row.add_int(1);
                object.finish();
            },
    });
    auto schema = schema_with_typed_float("parent.float_1", true);
    VariantShredder shredder({.tablet_schema = schema.get(),
                              .parent_column_unique_id = 1,
                              .logical_root_path = PathInData("parent"),
                              .max_subcolumns_count = 1,
                              .typed_paths_to_sparse = true});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    ASSERT_TRUE(shredder.finish(&result).ok());

    ASSERT_EQ(result.materialized.size(), 1);
    EXPECT_EQ(result.materialized[0].path.get_path(), "parent.hot");
    ASSERT_EQ(result.sparse_buckets.size(), 1);
    const auto& sparse = assert_cast<const ColumnMap&>(*result.sparse_buckets[0].column);
    EXPECT_EQ(sparse.get_offsets(), (ColumnArray::Offsets64 {0, 1}));
    const auto& keys = assert_cast<const ColumnString&>(sparse.get_keys());
    const auto& values = assert_cast<const ColumnString&>(sparse.get_values());
    ASSERT_EQ(keys.size(), 1);
    EXPECT_EQ(keys.get_data_at(0).to_string(), "parent.float_1");
    const StringRef raw = values.get_data_at(0);
    ASSERT_EQ(raw.size, sizeof(uint8_t) + sizeof(float));
    EXPECT_EQ(static_cast<uint8_t>(raw.data[0]),
              static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_FLOAT));
    EXPECT_EQ(std::memcmp(raw.data + sizeof(uint8_t), &value, sizeof(value)), 0);
    EXPECT_EQ(result.statistics.sparse_column_non_null_size.at("parent.float_1"), 1);
    EXPECT_EQ(result.sparse_buckets[0].statistics.sparse_column_non_null_size.at("parent.float_1"),
              1);
}

TEST(VariantShredderTest, LogicalRootPathWithoutSchemaMatchKeepsInferredType) {
    auto input = encoded_rows({[](auto& row) { row.add_int(812); }});
    auto schema = schema_with_typed_float("float_1", false);
    VariantShredder shredder({.tablet_schema = schema.get(),
                              .parent_column_unique_id = 1,
                              .logical_root_path = PathInData("other")});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    ASSERT_TRUE(shredder.finish(&result).ok());

    ASSERT_EQ(result.materialized.size(), 1);
    const auto& path = result.materialized[0];
    EXPECT_EQ(path.path.get_path(), "other");
    EXPECT_FALSE(path.is_typed_path);
    EXPECT_EQ(remove_nullable(path.type)->get_primitive_type(), TYPE_BIGINT);
}

TEST(VariantShredderTest, NestedPathsAndArrayLctPreserveDimensionOrFallbackToJsonb) {
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"a", 1});
                auto a = row.start_array();
                row.add_int(1);
                row.add_null();
                row.add_int(2);
                a.finish();
                object.add_key({"d", 1});
                auto d = row.start_array();
                row.add_int(3);
                d.finish();
                object.add_key({"n", 1});
                auto nested = row.start_object();
                nested.add_key({"x", 1});
                auto x = row.start_array();
                auto inner = row.start_array();
                row.add_int(4);
                inner.finish();
                x.finish();
                nested.finish();
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"a", 1});
                auto a = row.start_array();
                row.add_double(2.5);
                a.finish();
                object.add_key({"d", 1});
                auto d = row.start_array();
                auto inner_d = row.start_array();
                row.add_int(5);
                inner_d.finish();
                d.finish();
                object.add_key({"n", 1});
                auto nested = row.start_object();
                nested.add_key({"x", 1});
                auto x = row.start_array();
                auto inner = row.start_array();
                row.add_int(6);
                row.add_int(7);
                inner.finish();
                x.finish();
                nested.finish();
                object.finish();
            },
    });
    VariantShredder shredder({});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(remove_nullable(path_column(result, "a").type)->get_name(),
              "Array(Nullable(DOUBLE))");
    EXPECT_EQ(remove_nullable(path_column(result, "n.x").type)->get_name(),
              "Array(Nullable(Array(Nullable(BIGINT))))");

    const auto& a_nullable = assert_cast<const ColumnNullable&>(*path_column(result, "a").column);
    const auto& a = assert_cast<const ColumnArray&>(a_nullable.get_nested_column());
    EXPECT_EQ(a_nullable.get_null_map_data(), (NullMap {0, 0}));
    EXPECT_EQ(a.get_offsets(), (ColumnArray::Offsets64 {3, 4}));
    const auto& a_elements = assert_cast<const ColumnNullable&>(a.get_data());
    EXPECT_EQ(a_elements.get_null_map_data(), (NullMap {0, 1, 0, 0}));
    EXPECT_EQ(assert_cast<const ColumnFloat64&>(a_elements.get_nested_column()).get_data(),
              (PaddedPODArray<double> {1, 0, 2, 2.5}));

    const auto& d = path_column(result, "d");
    EXPECT_EQ(remove_nullable(d.type)->get_primitive_type(), TYPE_JSONB);
    const auto& d_values = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(*d.column).get_nested_column());
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(d_values.get_data_at(0).data,
                                                d_values.get_data_at(0).size),
              "[3]");
    EXPECT_EQ(JsonbToJson::jsonb_to_json_string(d_values.get_data_at(1).data,
                                                d_values.get_data_at(1).size),
              "[[5]]");

    const auto& nested_nullable =
            assert_cast<const ColumnNullable&>(*path_column(result, "n.x").column);
    const auto& outer = assert_cast<const ColumnArray&>(nested_nullable.get_nested_column());
    EXPECT_EQ(nested_nullable.get_null_map_data(), (NullMap {0, 0}));
    EXPECT_EQ(outer.get_offsets(), (ColumnArray::Offsets64 {1, 2}));
    const auto& inner_nullable = assert_cast<const ColumnNullable&>(outer.get_data());
    EXPECT_EQ(inner_nullable.get_null_map_data(), (NullMap {0, 0}));
    const auto& inner = assert_cast<const ColumnArray&>(inner_nullable.get_nested_column());
    EXPECT_EQ(inner.get_offsets(), (ColumnArray::Offsets64 {1, 3}));
    const auto& nested_elements = assert_cast<const ColumnNullable&>(inner.get_data());
    EXPECT_EQ(nested_elements.get_null_map_data(), (NullMap {0, 0, 0}));
    EXPECT_EQ(assert_cast<const ColumnInt64&>(nested_elements.get_nested_column()).get_data(),
              (PaddedPODArray<int64_t> {4, 6, 7}));
}

TEST(VariantShredderTest, EmptyArrayWithUnresolvedElementIsOmittedUnlessTyped) {
    auto input = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"a", 1});
        auto array = row.start_array();
        array.finish();
        object.add_key({"sibling", 7});
        row.add_int(1);
        object.finish();
    }});

    VariantShredder dynamic({});
    ASSERT_TRUE(dynamic.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns dynamic_result;
    ASSERT_TRUE(dynamic.finish(&dynamic_result).ok());
    ASSERT_EQ(dynamic_result.materialized.size(), 1);
    const VariantPathColumn& sibling = path_column(dynamic_result, "sibling");
    EXPECT_EQ(remove_nullable(sibling.type)->get_primitive_type(), TYPE_BIGINT);
    const auto& sibling_nullable = assert_cast<const ColumnNullable&>(*sibling.column);
    EXPECT_EQ(sibling_nullable.get_null_map_data(), (NullMap {0}));
    EXPECT_EQ(assert_cast<const ColumnInt64&>(sibling_nullable.get_nested_column()).get_data(),
              (PaddedPODArray<int64_t> {1}));
    EXPECT_EQ(dynamic_result.statistics.subcolumns_non_null_size.at("sibling"), 1);
    EXPECT_FALSE(dynamic_result.statistics.subcolumns_non_null_size.contains("a"));
    EXPECT_FALSE(dynamic_result.statistics.sparse_column_non_null_size.contains("a"));
    EXPECT_TRUE(dynamic_result.statistics.sparse_column_non_null_size.empty());
    const auto& root = assert_cast<const ColumnNullable&>(*dynamic_result.root_jsonb);
    EXPECT_EQ(root.get_null_map_data(), (NullMap {1}));
    auto dynamic_expected = encoded_rows({[](auto& row) { add_one_key_int(row, "sibling", 1); }});
    expect_roundtrip(dynamic_result, *dynamic_expected);

    auto typed_input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"a", 1});
                auto array = row.start_array();
                array.finish();
                object.add_key({"sibling", 7});
                row.add_int(1);
                object.finish();
            },
            [](auto& row) { add_one_key_int(row, "sibling", 2); },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"a", 1});
                auto array = row.start_array();
                row.add_int(3);
                array.finish();
                object.add_key({"sibling", 7});
                row.add_int(3);
                object.finish();
            },
    });
    TabletSchemaSPtr schema = schema_with_typed_int_array("a");
    VariantShredder typed({.tablet_schema = schema.get(), .parent_column_unique_id = 1});
    ASSERT_TRUE(typed.append(typed_input->read_view(), 0, typed_input->size()).ok());
    VariantShreddedColumns typed_result;
    ASSERT_TRUE(typed.finish(&typed_result).ok());
    const VariantPathColumn& a = path_column(typed_result, "a");
    EXPECT_TRUE(a.is_typed_path);
    EXPECT_EQ(remove_nullable(a.type)->get_name(), "Array(Nullable(INT))");
    EXPECT_FALSE(typed_result.statistics.sparse_column_non_null_size.contains("a"));
    const auto& nullable = assert_cast<const ColumnNullable&>(*a.column);
    EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 1, 0}));
    const auto& arrays = assert_cast<const ColumnArray&>(nullable.get_nested_column());
    EXPECT_EQ(arrays.get_offsets(), (ColumnArray::Offsets64 {0, 0, 1}));
    expect_roundtrip(typed_result, *typed_input);
}

TEST(VariantShredderTest, ExactTypedTargetsMaterializeAllEmptyArray) {
    auto input = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"a", 1});
        auto array = row.start_array();
        array.finish();
        object.finish();
    }});

    TabletSchemaSPtr array_schema = schema_with_typed_int_array("a");
    VariantShredder array_shredder(
            {.tablet_schema = array_schema.get(), .parent_column_unique_id = 1});
    ASSERT_TRUE(array_shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns array_result;
    ASSERT_TRUE(array_shredder.finish(&array_result).ok());
    const VariantPathColumn& array_path = path_column(array_result, "a");
    EXPECT_TRUE(array_path.is_typed_path);
    EXPECT_EQ(remove_nullable(array_path.type)->get_name(), "Array(Nullable(INT))");
    const auto& array_nullable = assert_cast<const ColumnNullable&>(*array_path.column);
    EXPECT_EQ(array_nullable.get_null_map_data(), (NullMap {0}));
    EXPECT_EQ(assert_cast<const ColumnArray&>(array_nullable.get_nested_column()).get_offsets(),
              (ColumnArray::Offsets64 {0}));

    TabletSchemaSPtr string_schema = schema_with_typed_string("a");
    VariantShredder string_shredder(
            {.tablet_schema = string_schema.get(), .parent_column_unique_id = 1});
    ASSERT_TRUE(string_shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns string_result;
    ASSERT_TRUE(string_shredder.finish(&string_result).ok());
    const VariantPathColumn& string_path = path_column(string_result, "a");
    EXPECT_TRUE(string_path.is_typed_path);
    EXPECT_EQ(remove_nullable(string_path.type)->get_primitive_type(), TYPE_STRING);
    const auto& string_nullable = assert_cast<const ColumnNullable&>(*string_path.column);
    EXPECT_EQ(string_nullable.get_null_map_data(), (NullMap {0}));
    EXPECT_EQ(assert_cast<const ColumnString&>(string_nullable.get_nested_column())
                      .get_data_at(0)
                      .to_string(),
              "[]");
}

TEST(VariantShredderTest, DocLiteralDottedTypedPathNormalizesAfterConflictValidation) {
    auto input = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"a.b.c", 5});
        auto array = row.start_array();
        row.add_int(1);
        row.add_int(2);
        row.add_int(3);
        array.finish();
        object.finish();
    }});

    for (bool flatten_nested : {false, true}) {
        SCOPED_TRACE(flatten_nested);
        TabletSchemaSPtr schema = schema_with_typed_int_array("a.b.c");
        schema->set_deprecated_variant_flatten_nested(flatten_nested);
        VariantShredder shredder({.tablet_schema = schema.get(),
                                  .parent_column_unique_id = 1,
                                  .physical_layout = VariantShredderPhysicalLayout::DOC});
        ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
        VariantShreddedColumns result;
        const Status status = shredder.finish(&result);
        ASSERT_TRUE(status.ok()) << status.to_string();

        const VariantPathColumn& typed = path_column(result, "a.b.c");
        ASSERT_EQ(typed.path.get_parts().size(), 3);
        EXPECT_EQ(typed.path.get_parts()[0].key, "a");
        EXPECT_EQ(typed.path.get_parts()[1].key, "b");
        EXPECT_EQ(typed.path.get_parts()[2].key, "c");
        EXPECT_TRUE(typed.is_typed_path);
        EXPECT_EQ(remove_nullable(typed.type)->get_name(), "Array(Nullable(INT))");

        ASSERT_EQ(result.doc_buckets.size(), 1);
        const auto& doc = assert_cast<const ColumnMap&>(*result.doc_buckets[0].column);
        const auto& keys = assert_cast<const ColumnString&>(doc.get_keys());
        ASSERT_EQ(keys.size(), 1);
        EXPECT_EQ(keys.get_data_at(0).to_string(), "a.b.c");
        EXPECT_EQ(result.statistics.doc_value_column_non_null_size.at("a.b.c"), 1);
    }
}

TEST(VariantShredderTest, AmbiguousDottedPathsAreRejected) {
    auto dotted = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"a.b", 3});
        row.add_int(1);
        object.add_key({"a", 1});
        auto nested = row.start_object();
        nested.add_key({"b", 1});
        row.add_int(2);
        nested.finish();
        object.add_key({"sibling", 7});
        row.add_int(3);
        object.finish();
    }});
    VariantShredder dotted_shredder({});
    const Status dotted_status = dotted_shredder.append(dotted->read_view(), 0, dotted->size());
    EXPECT_FALSE(dotted_status.ok());
    EXPECT_NE(dotted_status.to_string().find("collide at dotted path a.b"), std::string::npos);

    auto empty_segment = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"a..b", 4});
        row.add_int(1);
        object.finish();
    }});
    VariantShredder empty_segment_shredder({.physical_layout = VariantShredderPhysicalLayout::DOC});
    ASSERT_TRUE(empty_segment_shredder.append(empty_segment->read_view(), 0, empty_segment->size())
                        .ok());
    VariantShreddedColumns ignored;
    const Status empty_segment_status = empty_segment_shredder.finish(&ignored);
    EXPECT_FALSE(empty_segment_status.ok());
    EXPECT_NE(empty_segment_status.to_string().find("empty part"), std::string::npos);
}

TEST(VariantShredderTest, EmptyKeyPathIsDistinctFromLogicalRoot) {
    auto input = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"", 0});
        row.add_int(1);
        object.add_key({"sibling", 7});
        row.add_int(2);
        object.finish();
    }});
    VariantShredder shredder({});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());

    VariantShreddedColumns result;
    ASSERT_TRUE(shredder.finish(&result).ok());
    ASSERT_EQ(result.materialized.size(), 2);
    const VariantPathColumn& empty_key = result.materialized[0];
    ASSERT_EQ(empty_key.path.get_parts().size(), 1);
    EXPECT_TRUE(empty_key.path.get_parts()[0].key.empty());
    EXPECT_FALSE(empty_key.path.empty());
    expect_roundtrip(result, *input);
}

TEST(VariantShredderTest, TypedPathsStayMaterializedOrUseStorageTypeInSparse) {
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"hot", 3});
                row.add_int(1);
                object.add_key({"typed_i", 7});
                row.add_int(10);
                object.finish();
            },
            [](auto& row) { add_one_key_int(row, "hot", 2); },
    });

    auto fixed_schema = schema_with_typed_int("typed_i", false);
    VariantShredder fixed({.tablet_schema = fixed_schema.get(),
                           .parent_column_unique_id = 1,
                           .max_subcolumns_count = 1});
    ASSERT_TRUE(fixed.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns fixed_result;
    ASSERT_TRUE(fixed.finish(&fixed_result).ok());
    EXPECT_TRUE(path_column(fixed_result, "typed_i").is_typed_path);
    EXPECT_EQ(remove_nullable(path_column(fixed_result, "typed_i").type)->get_primitive_type(),
              TYPE_INT);

    auto sparse_schema = schema_with_typed_int("typed_i", true);
    VariantShredder sparse({.tablet_schema = sparse_schema.get(),
                            .parent_column_unique_id = 1,
                            .max_subcolumns_count = 1,
                            .typed_paths_to_sparse = true});
    ASSERT_TRUE(sparse.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns sparse_result;
    const Status status = sparse.finish(&sparse_result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(sparse_result.materialized.size(), 1);
    EXPECT_EQ(sparse_result.materialized[0].path.get_path(), "hot");
    const auto& map = assert_cast<const ColumnMap&>(*sparse_result.sparse_buckets[0].column);
    const auto& values = assert_cast<const ColumnString&>(map.get_values());
    ASSERT_EQ(values.size(), 1);
    EXPECT_EQ(static_cast<uint8_t>(values.get_data_at(0).data[0]),
              static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_INT));
}

TEST(VariantShredderTest, TypedTemporalVariantPrimitivesStayMaterialized) {
    constexpr int32_t date_days = 20'194;
    constexpr int64_t datetime_micros = static_cast<int64_t>(date_days + 1) * 86'400'000'000 +
                                        17 * 3'600'000'000 + 9 * 60'000'000 + 9'000'000;
    constexpr int64_t timestamptz_micros = datetime_micros + 1'000'000;
    auto input = encoded_rows({
            [=](auto& row) {
                auto object = row.start_object();
                object.add_key({"a", 1});
                row.add_date(date_days);
                object.add_key({"c", 1});
                row.add_timestamp_micros(datetime_micros, false);
                object.add_key({"t", 1});
                row.add_timestamp_micros(timestamptz_micros, true);
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.finish();
            },
    });

    auto schema = schema_with_typed_temporal_paths();
    VariantShredder shredder({.tablet_schema = schema.get(), .parent_column_unique_id = 1});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const VariantPathColumn& date = path_column(result, "a");
    EXPECT_EQ(remove_nullable(date.type)->get_primitive_type(), TYPE_DATEV2);
    EXPECT_EQ(assert_cast<const ColumnNullable&>(*date.column).get_null_map_data(),
              (NullMap {0, 1}));
    const VariantPathColumn& datetime = path_column(result, "c");
    EXPECT_EQ(remove_nullable(datetime.type)->get_primitive_type(), TYPE_DATETIMEV2);
    EXPECT_EQ(remove_nullable(datetime.type)->get_scale(), 0);
    EXPECT_EQ(assert_cast<const ColumnNullable&>(*datetime.column).get_null_map_data(),
              (NullMap {0, 1}));
    const VariantPathColumn& timestamptz = path_column(result, "t");
    EXPECT_EQ(remove_nullable(timestamptz.type)->get_primitive_type(), TYPE_TIMESTAMPTZ);
    EXPECT_EQ(assert_cast<const ColumnNullable&>(*timestamptz.column).get_null_map_data(),
              (NullMap {0, 1}));
    expect_roundtrip(result, *input);
}

TEST(VariantShredderTest, OutOfDorisRangeTemporalArraysFallbackBeforeAppendingElements) {
    constexpr int32_t OUT_OF_RANGE_DATE = std::numeric_limits<int32_t>::max();
    constexpr int64_t OUT_OF_RANGE_TIMESTAMP = std::numeric_limits<int64_t>::max();
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"dates", 5});
                auto dates = row.start_array();
                row.add_date(0);
                row.add_date(OUT_OF_RANGE_DATE);
                dates.finish();
                object.add_key({"ntz", 3});
                auto ntz = row.start_array();
                row.add_timestamp_micros(0, false);
                row.add_timestamp_micros(OUT_OF_RANGE_TIMESTAMP, false);
                ntz.finish();
                object.add_key({"tz", 2});
                auto tz = row.start_array();
                row.add_timestamp_micros(OUT_OF_RANGE_TIMESTAMP, true);
                tz.finish();
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"dates", 5});
                auto dates = row.start_array();
                row.add_date(OUT_OF_RANGE_DATE);
                dates.finish();
                object.add_key({"ntz", 3});
                auto ntz = row.start_array();
                row.add_timestamp_micros(OUT_OF_RANGE_TIMESTAMP, false);
                ntz.finish();
                object.add_key({"tz", 2});
                auto tz = row.start_array();
                row.add_timestamp_micros(0, true);
                row.add_timestamp_micros(OUT_OF_RANGE_TIMESTAMP, true);
                tz.finish();
                object.finish();
            },
    });

    VariantShredder shredder({});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    struct ExpectedOffsets {
        std::string_view path;
        uint64_t first;
        uint64_t second;
    };
    for (const ExpectedOffsets& expected :
         std::array {ExpectedOffsets {"dates", 2, 3}, ExpectedOffsets {"ntz", 2, 3},
                     ExpectedOffsets {"tz", 1, 3}}) {
        const VariantPathColumn& path_result = path_column(result, expected.path);
        EXPECT_EQ(remove_nullable(path_result.type)->get_name(), "Array(Nullable(JSONB))");
        const auto& nullable = assert_cast<const ColumnNullable&>(*path_result.column);
        EXPECT_EQ(nullable.get_null_map_data(), (NullMap {0, 0}));
        const auto& array = assert_cast<const ColumnArray&>(nullable.get_nested_column());
        EXPECT_EQ(array.get_offsets(), (ColumnArray::Offsets64 {expected.first, expected.second}));
    }
    // JSONB fallback preserves the exact JSON surface, but not the temporal primitive tag.
    expect_roundtrip(result, *input, RoundtripExpectation::JSON_SURFACE);
}

TEST(VariantShredderTest, EqualFrequencyBudgetTieUsesDeterministicPathOrder) {
    auto input = encoded_rows({[](auto& row) {
        auto object = row.start_object();
        object.add_key({"a", 1});
        row.add_int(1);
        object.add_key({"b", 1});
        row.add_int(2);
        object.finish();
    }});
    VariantShredder shredder({.max_subcolumns_count = 1});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    ASSERT_TRUE(shredder.finish(&result).ok());
    ASSERT_EQ(result.materialized.size(), 1);
    EXPECT_EQ(result.materialized[0].path.get_path(), "b");
    const auto& keys = assert_cast<const ColumnString&>(
            assert_cast<const ColumnMap&>(*result.sparse_buckets[0].column).get_keys());
    ASSERT_EQ(keys.size(), 1);
    EXPECT_EQ(keys.get_data_at(0).to_string(), "a");
}

TEST(VariantShredderTest, BudgetPrefersDepthAndSparseRowsKeepOrderedKeysAndOffsets) {
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"a", 1});
                row.add_int(1);
                object.add_key({"b", 1});
                row.add_int(2);
                object.add_key({"n", 1});
                auto nested = row.start_object();
                nested.add_key({"x", 1});
                row.add_int(3);
                nested.finish();
                object.finish();
            },
            [](auto& row) {
                auto object = row.start_object();
                object.finish();
            },
    });
    VariantShredder shredder({.max_subcolumns_count = 1});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    ASSERT_TRUE(shredder.finish(&result).ok());

    ASSERT_EQ(result.materialized.size(), 1);
    EXPECT_EQ(result.materialized[0].path.get_path(), "n.x");
    ASSERT_EQ(result.sparse_buckets.size(), 1);
    const auto& sparse = assert_cast<const ColumnMap&>(*result.sparse_buckets[0].column);
    const auto& keys = assert_cast<const ColumnString&>(sparse.get_keys());
    ASSERT_EQ(keys.size(), 2);
    EXPECT_EQ(keys.get_data_at(0).to_string(), "a");
    EXPECT_EQ(keys.get_data_at(1).to_string(), "b");
    EXPECT_EQ(sparse.get_offsets(), (ColumnArray::Offsets64 {2, 2}));
}

TEST(VariantShredderTest, Decimal16ScaleZeroSparseCellUsesLargeintRawEncoding) {
    constexpr __int128 wide = (static_cast<__int128>(1) << 80) + 17;
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"hot", 3});
                row.add_int(1);
                object.add_key({"wide", 4});
                row.add_largeint(wide);
                object.finish();
            },
            [](auto& row) { add_one_key_int(row, "hot", 2); },
    });
    VariantShredder shredder({.max_subcolumns_count = 1});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(result.sparse_buckets.size(), 1);
    const auto& map = assert_cast<const ColumnMap&>(*result.sparse_buckets[0].column);
    const auto& keys = assert_cast<const ColumnString&>(map.get_keys());
    const auto& values = assert_cast<const ColumnString&>(map.get_values());
    ASSERT_EQ(keys.size(), 1);
    EXPECT_EQ(keys.get_data_at(0).to_string(), "wide");
    const StringRef raw = values.get_data_at(0);
    ASSERT_EQ(raw.size, sizeof(uint8_t) + sizeof(__int128));
    EXPECT_EQ(static_cast<uint8_t>(raw.data[0]),
              static_cast<uint8_t>(FieldType::OLAP_FIELD_TYPE_LARGEINT));
    EXPECT_EQ(std::memcmp(raw.data + sizeof(uint8_t), &wide, sizeof(wide)), 0);
}

TEST(VariantShredderTest, BucketStatisticsAreBoundedAndBoundToTheirPhysicalColumn) {
    auto input = encoded_rows({
            [](auto& row) {
                auto object = row.start_object();
                object.add_key({"a", 1});
                row.add_int(1);
                object.add_key({"b", 1});
                row.add_int(2);
                object.add_key({"c", 1});
                row.add_int(3);
                object.add_key({"d", 1});
                row.add_int(4);
                object.add_key({"hot", 3});
                row.add_int(5);
                object.finish();
            },
            [](auto& row) { add_one_key_int(row, "hot", 6); },
    });
    VariantShredder shredder({.max_subcolumns_count = 1,
                              .sparse_bucket_count = 2,
                              .max_sparse_column_statistics_size = 1});
    ASSERT_TRUE(shredder.append(input->read_view(), 0, input->size()).ok());
    VariantShreddedColumns result;
    const Status status = shredder.finish(&result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(result.sparse_buckets.size(), 2);

    size_t sparse_cells = 0;
    size_t aggregate_statistics = 0;
    for (size_t bucket = 0; bucket < result.sparse_buckets.size(); ++bucket) {
        const auto& sparse = result.sparse_buckets[bucket];
        const auto& map = assert_cast<const ColumnMap&>(*sparse.column);
        sparse_cells += map.get_keys().size();
        EXPECT_LE(sparse.statistics.sparse_column_non_null_size.size(), 1);
        for (const auto& [path, count] : sparse.statistics.sparse_column_non_null_size) {
            EXPECT_EQ(variant_util::variant_binary_shard_of({path.data(), path.size()}, 2), bucket);
            EXPECT_EQ(result.statistics.sparse_column_non_null_size.at(path), count);
            ++aggregate_statistics;
        }
    }
    EXPECT_EQ(sparse_cells, 4);
    EXPECT_EQ(result.statistics.sparse_column_non_null_size.size(), aggregate_statistics);
}

TEST(VariantShredderTest, C3BucketedSparseSourceAndManifestMatchRawCellsAndStatistics) {
    const C3Fixture fixture = load_c3_fixture("bucketed_sparse");
    ASSERT_EQ(fixture.rows.size(), 12);
    ASSERT_EQ(fixture.materialized_paths, (std::set<std::string> {"hot"}));
    ASSERT_EQ(fixture.sparse_bucket_count, 3);
    ASSERT_EQ(fixture.sparse_cells.size(), 24);

    size_t included_path_values = 0;
    const VariantShreddedColumns result =
            shred_c3_case("bucketed_sparse", fixture, &included_path_values);
    ASSERT_EQ(result.num_rows, 12);
    EXPECT_EQ(included_path_values, 36);
    ASSERT_EQ(result.materialized.size(), 1);
    EXPECT_EQ(result.materialized[0].path.get_path(), "hot");
    expect_c3_sparse_cells_and_statistics(result, fixture);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): GTest macros inflate manifest checks.
TEST(VariantShredderTest, C3OrdinaryRepresentableSubsetMatchesLctSparseAndScalarRoot) {
    C3Fixture fixture = load_c3_fixture("ordinary_sparse");
    ASSERT_EQ(fixture.rows.size(), 13);
    ASSERT_EQ(fixture.materialized_paths, (std::set<std::string> {"collision", "hot"}));
    ASSERT_EQ(fixture.sparse_bucket_count, 1);
    ASSERT_EQ(fixture.sparse_cells.size(), 45);

    size_t included_path_values = 0;
    const VariantShreddedColumns result =
            shred_c3_case("ordinary_sparse", fixture, &included_path_values);
    ASSERT_EQ(result.num_rows, 13);
    EXPECT_EQ(included_path_values, 68);
    ASSERT_EQ(result.materialized.size(), 2);
    EXPECT_EQ(result.materialized[0].path.get_path(), "collision");
    EXPECT_EQ(result.materialized[1].path.get_path(), "hot");
    EXPECT_EQ(root_json_at(result, 12), "42");
    EXPECT_EQ(fixture.sparse_cells.at({0, "lct_double"}), "0b0000000000000000");
    EXPECT_EQ(fixture.sparse_cells.at({0, "lct_int"}), "070100000000000000");
    EXPECT_EQ(fixture.sparse_cells.at({0, "array"}),
              "130200000000000000070000000000000000070100000000000000");
    expect_c3_sparse_cells_and_statistics(result, fixture);
}

TEST(VariantShredderTest, TypedInputAndBadRangeEnterTerminalFailedStateWithoutPublishing) {
    auto values = ColumnInt32::create();
    values->insert_value(7);
    auto nulls = ColumnUInt8::create(1, 0);
    ColumnPtr nullable = ColumnNullable::create(std::move(values), std::move(nulls));
    auto typed =
            ColumnVariantV2::create_typed(std::move(nullable), std::make_shared<DataTypeInt32>());

    VariantShredder shredder({});
    const Status first = shredder.append(typed->read_view(), 0, 1);
    ASSERT_FALSE(first.ok());
    VariantShreddedColumns sentinel;
    sentinel.num_rows = 99;
    const Status finish = shredder.finish(&sentinel);
    EXPECT_EQ(finish.code(), first.code());
    EXPECT_EQ(finish.msg(), first.msg());
    EXPECT_EQ(sentinel.num_rows, 99);

    auto encoded = encoded_rows({[](auto& row) { row.add_int(1); }});
    VariantShredder bad_range({});
    const Status range = bad_range.append(encoded->read_view(), 1, 1);
    ASSERT_FALSE(range.ok());
    const Status repeated = bad_range.append(encoded->read_view(), 0, 1);
    EXPECT_EQ(repeated.code(), range.code());
    EXPECT_EQ(repeated.msg(), range.msg());
}

TEST(VariantShredderTest, MalformedEncodedValueFailsTerminallyWithoutPublishing) {
    auto malformed = encoded_rows({[](auto& row) { row.add_int(1); }});
    const VariantRef value = malformed->get_value_ref(0);
    std::string values(value.value.data, value.value.size);
    values.push_back('\0');
    auto malformed_values = ColumnString::create();
    malformed_values->insert_data(values.data(), values.size());
    ColumnVariantV2::TestAccess::replace_encoded_subcolumn(*malformed, 2,
                                                           std::move(malformed_values));

    VariantShredder shredder({});
    const Status append = shredder.append(malformed->read_view(), 0, 1);
    ASSERT_FALSE(append.ok());
    VariantShreddedColumns sentinel;
    sentinel.num_rows = 77;
    const Status finish = shredder.finish(&sentinel);
    EXPECT_EQ(finish.code(), append.code());
    EXPECT_EQ(finish.msg(), append.msg());
    EXPECT_EQ(sentinel.num_rows, 77);
}

} // namespace doris::segment_v2
