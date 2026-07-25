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

#include "storage/segment/variant/variant_assembler.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/value/timestamptz_value.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "storage/types.h"
#include "util/jsonb_parser_simd.h"
#include "util/variant/variant_test_utils.h"

namespace doris::segment_v2 {
namespace {

struct JsonWriter {
    void write(const char* data, size_t size) { value.append(data, size); }

    std::string value;
};

std::string json_at(ColumnVariantV2& column, size_t row) {
    if (column.is_typed()) {
        column.ensure_encoded();
    }
    JsonWriter writer;
    to_json(column.get_value_ref(row), writer);
    return writer.value;
}

template <typename T>
void append_pod(std::string& output, const T& value) {
    output.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

constexpr uint32_t pack_olap_date(uint32_t year, uint32_t month, uint32_t day) {
    return (year << 9) | (month << 5) | day;
}

template <typename T>
std::string fixed_cell(FieldType type, const T& value) {
    std::string result(1, static_cast<char>(type));
    append_pod(result, value);
    return result;
}

std::string string_cell(std::string_view value) {
    std::string result(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_STRING));
    append_pod(result, value.size());
    result.append(value);
    return result;
}

std::string jsonb_bytes(std::string_view json) {
    JsonbWriter writer;
    const Status status = JsonbParser::parse(json.data(), json.size(), writer);
    EXPECT_TRUE(status.ok()) << status;
    return {writer.getOutput()->getBuffer(), static_cast<size_t>(writer.getOutput()->getSize())};
}

std::string jsonb_cell(std::string_view json) {
    const std::string bytes = jsonb_bytes(json);
    std::string result(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_JSONB));
    append_pod(result, bytes.size());
    result.append(bytes);
    return result;
}

std::string array_cell(std::initializer_list<std::string> elements) {
    std::string result(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_ARRAY));
    append_pod(result, elements.size());
    for (const std::string& element : elements) {
        result.append(element);
    }
    return result;
}

std::string decimal32_cell(uint8_t precision, uint8_t scale, int32_t value) {
    std::string result(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_DECIMAL32));
    result.push_back(static_cast<char>(precision));
    result.push_back(static_cast<char>(scale));
    append_pod(result, value);
    return result;
}

ColumnMap::MutablePtr map_column(
        const std::vector<std::vector<std::pair<std::string, std::string>>>& rows) {
    auto paths = ColumnString::create();
    auto values = ColumnString::create();
    auto offsets = ColumnArray::ColumnOffsets::create();
    size_t offset = 0;
    for (const auto& row : rows) {
        for (const auto& [path, value] : row) {
            paths->insert_data(path.data(), path.size());
            values->insert_data(value.data(), value.size());
            ++offset;
        }
        offsets->insert_value(offset);
    }
    return ColumnMap::create(std::move(paths), std::move(values), std::move(offsets));
}

ColumnString::MutablePtr binary_column(const std::vector<std::string>& cells) {
    auto result = ColumnString::create();
    for (const std::string& cell : cells) {
        result->insert_data(cell.data(), cell.size());
    }
    return result;
}

ColumnString::MutablePtr root_column(const std::vector<std::string>& jsons) {
    auto result = ColumnString::create();
    for (const std::string& json : jsons) {
        const std::string bytes = jsonb_bytes(json);
        result->insert_data(bytes.data(), bytes.size());
    }
    return result;
}

ColumnPtr nullable_int64_column(const std::vector<std::optional<int64_t>>& values) {
    auto nested = ColumnInt64::create();
    auto nulls = ColumnUInt8::create();
    for (const auto& value : values) {
        nested->insert_value(value.value_or(0));
        nulls->insert_value(value.has_value() ? 0 : 1);
    }
    return ColumnNullable::create(std::move(nested), std::move(nulls));
}

std::shared_ptr<const VariantAssemblerPlan> create_plan(VariantAssemblerPlanOptions options) {
    std::shared_ptr<const VariantAssemblerPlan> plan;
    const Status status = VariantAssemblerPlan::create(std::move(options), &plan);
    EXPECT_TRUE(status.ok()) << status;
    return plan;
}

VariantAssembledColumn assemble_once(const std::shared_ptr<const VariantAssemblerPlan>& plan,
                                     const VariantAssemblerBatchView& batch) {
    VariantAssembler assembler(plan);
    VariantAssembledColumn result;
    const Status status = assembler.assemble(batch, &result);
    EXPECT_TRUE(status.ok()) << status;
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

uint8_t from_hex(char digit) {
    if (digit >= '0' && digit <= '9') {
        return static_cast<uint8_t>(digit - '0');
    }
    if (digit >= 'a' && digit <= 'f') {
        return static_cast<uint8_t>(digit - 'a' + 10);
    }
    return static_cast<uint8_t>(digit - 'A' + 10);
}

std::string decode_hex(std::string_view hex) {
    EXPECT_EQ(hex.size() % 2, 0);
    std::string result;
    result.reserve(hex.size() / 2);
    for (size_t index = 0; index + 1 < hex.size(); index += 2) {
        result.push_back(static_cast<char>((from_hex(hex[index]) << 4) | from_hex(hex[index + 1])));
    }
    return result;
}

std::string dotted_path(size_t parts) {
    std::string result;
    for (size_t part = 0; part < parts; ++part) {
        if (!result.empty()) {
            result.push_back('.');
        }
        result.push_back('p');
    }
    return result;
}

struct C3Fixture {
    struct SourceEntry {
        std::string path;
        std::string type;
        std::string value;
    };

    std::map<size_t, std::vector<SourceEntry>> source;
    std::map<size_t, std::vector<std::pair<std::string, std::string>>> cells;
    std::map<size_t, std::string> assembled;
    size_t cell_count = 0;
};

C3Fixture load_c3_manifest(std::string_view case_id) {
    C3Fixture fixture;
    std::ifstream source(sample_path("source.tsv"));
    EXPECT_TRUE(source.is_open());
    std::string line;
    std::getline(source, line);
    while (std::getline(source, line)) {
        const auto fields = split_tsv(line);
        EXPECT_EQ(fields.size(), 5);
        if (fields.size() == 5 && fields[0] == case_id) {
            fixture.source[std::stoull(fields[1])].push_back(
                    {.path = fields[2], .type = fields[3], .value = fields[4]});
        }
    }

    std::ifstream manifest(sample_path("manifest.tsv"));
    EXPECT_TRUE(manifest.is_open());
    std::getline(manifest, line);
    while (std::getline(manifest, line)) {
        const auto fields = split_tsv(line);
        EXPECT_EQ(fields.size(), 9);
        if (fields.size() != 9 || fields[1] != case_id) {
            continue;
        }
        const size_t row = std::stoull(fields[3]);
        if (fields[0] == "cell") {
            fixture.cells[row].emplace_back(fields[4], decode_hex(fields[7]));
            ++fixture.cell_count;
        } else if (fields[0] == "assembled") {
            fixture.assembled.emplace(row, fields[8]);
        }
    }
    return fixture;
}

std::optional<std::string_view> source_value(const C3Fixture& fixture, size_t row,
                                             std::string_view path) {
    const auto found_row = fixture.source.find(row);
    if (found_row == fixture.source.end()) {
        return std::nullopt;
    }
    for (const C3Fixture::SourceEntry& entry : found_row->second) {
        if (entry.path == path) {
            return entry.value;
        }
    }
    return std::nullopt;
}

std::vector<std::vector<std::pair<std::string, std::string>>> manifest_rows(
        const C3Fixture& fixture, size_t rows) {
    std::vector<std::vector<std::pair<std::string, std::string>>> result(rows);
    for (const auto& [row, cells] : fixture.cells) {
        EXPECT_LT(row, rows);
        if (row < rows) {
            result[row] = cells;
        }
    }
    return result;
}

std::string compact_json_whitespace(std::string_view json) {
    std::string result;
    bool in_string = false;
    bool escaped = false;
    for (char byte : json) {
        if (in_string) {
            result.push_back(byte);
            if (escaped) {
                escaped = false;
            } else if (byte == '\\') {
                escaped = true;
            } else if (byte == '"') {
                in_string = false;
            }
        } else if (byte == '"') {
            in_string = true;
            result.push_back(byte);
        } else if (byte != ' ' && byte != '\t' && byte != '\n' && byte != '\r') {
            result.push_back(byte);
        }
    }
    EXPECT_FALSE(in_string);
    return result;
}

void expect_manifest_rows(VariantAssembledColumn* result, const C3Fixture& fixture, size_t rows) {
    ASSERT_NE(result, nullptr);
    ASSERT_TRUE(static_cast<bool>(result->values));
    ASSERT_EQ(result->values->size(), rows);
    ASSERT_EQ(fixture.assembled.size(), rows);
    for (size_t row = 0; row < rows; ++row) {
        ASSERT_TRUE(fixture.assembled.contains(row));
        EXPECT_EQ(json_at(*result->values, row), compact_json_whitespace(fixture.assembled.at(row)))
                << row;
    }
}

TEST(VariantAssemblerTest, PlanIsReusableAndFirstFailureIsTerminal) {
    std::shared_ptr<const VariantAssemblerPlan> plan;
    VariantAssemblerPlanOptions options;
    options.mode = VariantAssemblerMode::DEFAULT_FILL;
    ASSERT_TRUE(VariantAssemblerPlan::create(std::move(options), &plan).ok());

    VariantAssembler assembler(plan);
    VariantAssemblerBatchView batch;
    batch.num_rows = 2;
    VariantAssembledColumn first;
    ASSERT_TRUE(assembler.assemble(batch, &first).ok());
    ASSERT_TRUE(static_cast<bool>(first.values));
    EXPECT_EQ(first.values->size(), 2);

    VariantAssembledColumn second;
    ASSERT_TRUE(assembler.assemble(batch, &second).ok());
    ASSERT_TRUE(static_cast<bool>(second.values));
    EXPECT_EQ(second.values->size(), 2);

    VariantAssembledColumn sentinel = std::move(second);
    const auto* sentinel_values = sentinel.values.get();
    EXPECT_FALSE(assembler.assemble(batch, nullptr).ok());
    EXPECT_FALSE(assembler.assemble(batch, &sentinel).ok());
    EXPECT_EQ(sentinel.values.get(), sentinel_values);
}

TEST(VariantAssemblerTest, RootFlatAndDefaultFillPreserveOuterNulls) {
    auto root = ColumnString::create();
    const std::string valid = jsonb_bytes(R"({"a":1})");
    root->insert_data(valid.data(), valid.size());
    root->insert_data("bad", 3);
    constexpr std::array<uint8_t, 2> NULLS {0, 1};

    VariantAssemblerPlanOptions root_options;
    root_options.mode = VariantAssemblerMode::ROOT_FLAT;
    root_options.has_root = true;
    VariantAssemblerBatchView root_batch;
    root_batch.num_rows = 2;
    root_batch.outer_nulls = NULLS;
    root_batch.root_jsonb = root.get();
    VariantAssembledColumn root_result =
            assemble_once(create_plan(std::move(root_options)), root_batch);
    EXPECT_EQ(json_at(*root_result.values, 0), R"({"a":1})");
    EXPECT_EQ(json_at(*root_result.values, 1), "null");
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*root_result.outer_nulls).get_data(),
              (PaddedPODArray<uint8_t> {0, 1}));

    VariantAssemblerPlanOptions default_options;
    default_options.mode = VariantAssemblerMode::DEFAULT_FILL;
    VariantAssemblerBatchView default_batch;
    default_batch.num_rows = 2;
    default_batch.outer_nulls = NULLS;
    VariantAssembledColumn default_result =
            assemble_once(create_plan(std::move(default_options)), default_batch);
    EXPECT_EQ(json_at(*default_result.values, 0), "{}");
    EXPECT_EQ(json_at(*default_result.values, 1), "null");
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*default_result.outer_nulls).get_data(),
              (PaddedPODArray<uint8_t> {0, 1}));
}

TEST(VariantAssemblerTest, RootFlatDistinguishesEmptyRootFromPhysicalNull) {
    auto root_values = ColumnString::create();
    root_values->insert_default();
    root_values->insert_default();
    auto root_nulls = ColumnUInt8::create();
    root_nulls->insert_value(0);
    root_nulls->insert_value(1);
    auto roots = ColumnNullable::create(std::move(root_values), std::move(root_nulls));

    VariantAssemblerPlanOptions options;
    options.mode = VariantAssemblerMode::ROOT_FLAT;
    options.has_root = true;
    VariantAssemblerBatchView batch;
    batch.num_rows = 2;
    batch.root_jsonb = roots.get();
    VariantAssembledColumn result = assemble_once(create_plan(std::move(options)), batch);

    EXPECT_EQ(json_at(*result.values, 0), "{}");
    EXPECT_EQ(json_at(*result.values, 1), "null");
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*result.outer_nulls).get_data(),
              (PaddedPODArray<uint8_t> {0, 1}));
}

TEST(VariantAssemblerTest, HierarchicalEmptyRootFallsBackToEmptyObject) {
    auto roots = ColumnString::create();
    roots->insert_default();

    VariantAssemblerPlanOptions options;
    options.mode = VariantAssemblerMode::HIERARCHICAL;
    options.has_root = true;
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.root_jsonb = roots.get();
    VariantAssembledColumn result = assemble_once(create_plan(std::move(options)), batch);

    EXPECT_EQ(json_at(*result.values, 0), "{}");
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*result.outer_nulls).get_data(),
              (PaddedPODArray<uint8_t> {0}));
}

TEST(VariantAssemblerTest, HierarchicalRootFallbackPreservesEmptyObjectAndJsonNull) {
    auto roots = root_column({"{}", "null"});
    VariantAssemblerPlanOptions options;
    options.has_root = true;
    VariantAssemblerBatchView batch;
    batch.num_rows = 2;
    batch.root_jsonb = roots.get();
    VariantAssembledColumn result = assemble_once(create_plan(std::move(options)), batch);
    EXPECT_EQ(json_at(*result.values, 0), "{}");
    EXPECT_EQ(json_at(*result.values, 1), "null");
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*result.outer_nulls).get_data(),
              (PaddedPODArray<uint8_t> {0, 0}));
}

TEST(VariantAssemblerTest, HierarchicalPriorityMatrixAndMissingValues) {
    struct Case {
        bool root;
        bool materialized;
        bool sparse;
        bool doc;
        std::string_view expected;
    };
    constexpr std::array<Case, 9> CASES {{
            {true, false, false, false, R"({"root":1})"},
            {false, true, false, false, R"({"m":2})"},
            {false, false, true, false, R"({"s":3})"},
            {false, false, false, true, R"({"d":4})"},
            {true, true, false, false, R"({"m":2,"root":1})"},
            {true, false, true, false, R"({"root":1,"s":3})"},
            {true, false, false, true, R"({"d":4})"},
            {false, true, true, false, R"({"m":2,"s":3})"},
            {true, true, true, true, R"({"d":4})"},
    }};
    auto root = root_column({R"({"root":1})"});
    auto materialized = ColumnInt64::create();
    materialized->insert_value(2);
    auto sparse = map_column({{{"s", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {3})}}});
    auto doc = map_column({{{"d", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {4})}}});
    const IColumn* materialized_ptr = materialized.get();
    const ColumnMap* sparse_ptr = sparse.get();
    for (const Case& test : CASES) {
        VariantAssemblerPlanOptions options;
        options.has_root = test.root;
        options.has_doc = test.doc;
        if (test.materialized) {
            options.materialized_paths.push_back(
                    {.path = PathInData("m"), .type = std::make_shared<DataTypeInt64>()});
        }
        options.sparse_bucket_count = test.sparse ? 1 : 0;
        const auto plan = create_plan(std::move(options));
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.root_jsonb = test.root ? root.get() : nullptr;
        batch.materialized_columns = test.materialized
                                             ? std::span<const IColumn* const>(&materialized_ptr, 1)
                                             : std::span<const IColumn* const>();
        batch.sparse_buckets = test.sparse ? std::span<const ColumnMap* const>(&sparse_ptr, 1)
                                           : std::span<const ColumnMap* const>();
        batch.doc_values = test.doc ? doc.get() : nullptr;
        VariantAssembledColumn result = assemble_once(plan, batch);
        EXPECT_EQ(json_at(*result.values, 0), test.expected);
    }

    auto missing = nullable_int64_column({std::nullopt});
    const IColumn* missing_ptr = missing.get();
    VariantAssemblerPlanOptions missing_options;
    missing_options.materialized_paths.push_back(
            {.path = PathInData("missing"),
             .type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});
    const auto missing_plan = create_plan(std::move(missing_options));
    VariantAssemblerBatchView missing_batch;
    missing_batch.num_rows = 1;
    missing_batch.materialized_columns = {&missing_ptr, 1};
    VariantAssembledColumn missing_result = assemble_once(missing_plan, missing_batch);
    EXPECT_EQ(json_at(*missing_result.values, 0), "{}");
}

TEST(VariantAssemblerTest, HierarchicalRootMaterializedOverlayPreservesFieldsAndAlignment) {
    auto root_values = root_column({
            R"({"present_null":null,"m":"old","keep":1})",
            R"({"present_null":null,"m":"old","keep":2})",
            R"({"present_null":null,"keep":3})",
            "{}",
    });
    root_values->insert_data("bad", 3);
    auto root_nulls = ColumnUInt8::create();
    for (uint8_t value : {0, 0, 0, 1, 0}) {
        root_nulls->insert_value(value);
    }
    auto roots = ColumnNullable::create(std::move(root_values), std::move(root_nulls));
    ColumnPtr materialized = nullable_int64_column(
            {int64_t {2}, std::nullopt, std::nullopt, int64_t {4}, int64_t {5}});
    const IColumn* materialized_ptr = materialized.get();
    constexpr std::array<uint8_t, 5> OUTER_NULLS {0, 0, 0, 0, 1};

    VariantAssemblerPlanOptions options;
    options.has_root = true;
    options.materialized_paths.push_back(
            {.path = PathInData("m"),
             .type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});
    VariantAssemblerBatchView batch;
    batch.num_rows = OUTER_NULLS.size();
    batch.outer_nulls = OUTER_NULLS;
    batch.root_jsonb = roots.get();
    batch.materialized_columns = {&materialized_ptr, 1};
    VariantAssembledColumn result = assemble_once(create_plan(std::move(options)), batch);

    ASSERT_EQ(result.values->size(), OUTER_NULLS.size());
    EXPECT_EQ(json_at(*result.values, 0), R"({"keep":1,"m":2,"present_null":null})");
    EXPECT_EQ(json_at(*result.values, 1), R"({"keep":2,"m":"old","present_null":null})");
    EXPECT_EQ(json_at(*result.values, 2), R"({"keep":3,"present_null":null})");
    EXPECT_EQ(json_at(*result.values, 3), R"({"m":4})");
    EXPECT_EQ(json_at(*result.values, 4), "null");
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*result.outer_nulls).get_data(),
              (PaddedPODArray<uint8_t> {0, 0, 0, 0, 1}));
}

TEST(VariantAssemblerTest, HierarchicalRootSparseOverlayPreservesTypesAndPriority) {
    auto roots = root_column({
            R"({"f":8.119999999999999,"keep":1})",
            R"({"f":8.119999999999999,"keep":2})",
            R"({"f":8.119999999999999,"keep":3})",
            R"({"keep":4})",
            "null",
            R"({"a":{"b":1,"c":2},"keep":5})",
            R"({"a":1,"keep":6})",
    });
    ColumnPtr materialized =
            nullable_int64_column({std::nullopt, std::nullopt, int64_t {7}, std::nullopt,
                                   std::nullopt, std::nullopt, std::nullopt});
    const IColumn* materialized_ptr = materialized.get();
    auto first = map_column({
            {{"new", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {9})}},
            {},
            {},
            {{"extra", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {4})}},
            {},
            {{"a", jsonb_cell(R"({"z":3})")}},
            {{"a.b", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {2})}},
    });
    auto second = map_column({
            {{"f", fixed_cell(FieldType::OLAP_FIELD_TYPE_FLOAT, float {8.12F})}},
            {},
            {{"f", fixed_cell(FieldType::OLAP_FIELD_TYPE_FLOAT, float {8.12F})}},
            {},
            {},
            {},
            {},
    });
    std::array<const ColumnMap*, 2> sparse {first.get(), second.get()};

    VariantAssemblerPlanOptions options;
    options.has_root = true;
    options.materialized_paths.push_back(
            {.path = PathInData("m"),
             .type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});
    options.sparse_bucket_count = sparse.size();
    VariantAssemblerBatchView batch;
    batch.num_rows = 7;
    batch.root_jsonb = roots.get();
    batch.materialized_columns = {&materialized_ptr, 1};
    batch.sparse_buckets = sparse;
    VariantAssembledColumn result = assemble_once(create_plan(std::move(options)), batch);

    EXPECT_EQ(json_at(*result.values, 0), R"({"f":8.12,"keep":1,"new":9})");
    EXPECT_EQ(json_at(*result.values, 1), R"({"f":8.119999999999999,"keep":2})");
    EXPECT_EQ(json_at(*result.values, 2), R"({"f":8.12,"keep":3,"m":7})");
    EXPECT_EQ(json_at(*result.values, 3), R"({"extra":4,"keep":4})");
    EXPECT_EQ(json_at(*result.values, 4), "null");
    EXPECT_EQ(json_at(*result.values, 5), R"({"a":{"z":3},"keep":5})");
    EXPECT_EQ(json_at(*result.values, 6), R"({"a":{"b":2},"keep":6})");

    VariantRef float_value;
    ASSERT_TRUE(result.values->get_value_ref(0).object_find({"f", 1}, &float_value));
    EXPECT_EQ(float_value.primitive_id(), VariantPrimitiveId::FLOAT);
    EXPECT_EQ(float_value.get_float(), 8.12F);
}

TEST(VariantAssemblerTest, HierarchicalRootDoesNotMaskBadSparseOverlayBytes) {
    auto roots = root_column({R"({"f":1,"keep":2})"});
    std::string malformed(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_FLOAT));
    auto sparse = map_column({{{"f", malformed}}});
    const ColumnMap* sparse_ptr = sparse.get();
    VariantAssemblerPlanOptions options;
    options.has_root = true;
    options.sparse_bucket_count = 1;
    VariantAssembler assembler(create_plan(std::move(options)));
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.root_jsonb = roots.get();
    batch.sparse_buckets = {&sparse_ptr, 1};
    VariantAssembledColumn sentinel;
    sentinel.values = ColumnVariantV2::create();
    const auto* sentinel_values = sentinel.values.get();
    EXPECT_TRUE(assembler.assemble(batch, &sentinel).is<ErrorCode::CORRUPTION>());
    EXPECT_EQ(sentinel.values.get(), sentinel_values);
}

TEST(VariantAssemblerTest, HierarchicalRootSparseOverlayDistinguishesJsonAndSqlNull) {
    auto sparse = map_column({{{"s", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {1})}}});
    const ColumnMap* sparse_ptr = sparse.get();

    VariantAssemblerPlanOptions json_null_options;
    json_null_options.has_root = true;
    json_null_options.sparse_bucket_count = 1;
    VariantAssembler json_null_assembler(create_plan(std::move(json_null_options)));
    auto json_null_root = root_column({"null"});
    VariantAssemblerBatchView json_null_batch;
    json_null_batch.num_rows = 1;
    json_null_batch.root_jsonb = json_null_root.get();
    json_null_batch.sparse_buckets = {&sparse_ptr, 1};
    VariantAssembledColumn json_null_result;
    EXPECT_TRUE(json_null_assembler.assemble(json_null_batch, &json_null_result)
                        .is<ErrorCode::CORRUPTION>());

    auto sql_null_values = root_column({"{}"});
    auto sql_nulls = ColumnUInt8::create();
    sql_nulls->insert_value(1);
    auto sql_null_root = ColumnNullable::create(std::move(sql_null_values), std::move(sql_nulls));
    VariantAssemblerPlanOptions sql_null_options;
    sql_null_options.has_root = true;
    sql_null_options.sparse_bucket_count = 1;
    VariantAssemblerBatchView sql_null_batch;
    sql_null_batch.num_rows = 1;
    sql_null_batch.root_jsonb = sql_null_root.get();
    sql_null_batch.sparse_buckets = {&sparse_ptr, 1};
    VariantAssembledColumn sql_null_result =
            assemble_once(create_plan(std::move(sql_null_options)), sql_null_batch);
    EXPECT_EQ(json_at(*sql_null_result.values, 0), R"({"s":1})");
}

TEST(VariantAssemblerTest, RequestedPathUsesStructuredMaterializedAndExactMapRoots) {
    auto value = ColumnInt64::create();
    value->insert_value(7);
    const IColumn* value_ptr = value.get();

    PathInData::Parts dotted_parts;
    dotted_parts.emplace_back("a.b", false, 0);
    VariantAssemblerPlanOptions dotted_options;
    dotted_options.materialized_paths.push_back(
            {.path = PathInData(dotted_parts), .type = std::make_shared<DataTypeInt64>()});
    VariantAssemblerBatchView dotted_batch;
    dotted_batch.num_rows = 1;
    dotted_batch.materialized_columns = {&value_ptr, 1};
    VariantAssembledColumn dotted =
            assemble_once(create_plan(std::move(dotted_options)), dotted_batch);
    EXPECT_EQ(json_at(*dotted.values, 0), R"({"a.b":7})");

    VariantAssemblerPlanOptions subtree_options;
    subtree_options.requested_path = PathInData("a.b");
    subtree_options.materialized_paths.push_back(
            {.path = PathInData("a.b.c"), .type = std::make_shared<DataTypeInt64>()});
    VariantAssemblerBatchView subtree_batch;
    subtree_batch.num_rows = 1;
    subtree_batch.materialized_columns = {&value_ptr, 1};
    VariantAssembledColumn subtree =
            assemble_once(create_plan(std::move(subtree_options)), subtree_batch);
    EXPECT_EQ(json_at(*subtree.values, 0), R"({"c":7})");

    for (bool doc_mode : {false, true}) {
        auto exact =
                map_column({{{"a.b", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {9})}}});
        const ColumnMap* exact_ptr = exact.get();
        VariantAssemblerPlanOptions exact_options;
        exact_options.requested_path = PathInData("a.b");
        exact_options.has_doc = doc_mode;
        exact_options.sparse_bucket_count = doc_mode ? 0 : 1;
        const auto exact_plan = create_plan(std::move(exact_options));
        VariantAssemblerBatchView exact_batch;
        exact_batch.num_rows = 1;
        exact_batch.doc_values = doc_mode ? exact.get() : nullptr;
        exact_batch.sparse_buckets = doc_mode ? std::span<const ColumnMap* const>()
                                              : std::span<const ColumnMap* const>(&exact_ptr, 1);
        VariantAssembledColumn exact_result = assemble_once(exact_plan, exact_batch);
        EXPECT_EQ(json_at(*exact_result.values, 0), "9");
    }
    for (bool doc_mode : {false, true}) {
        auto descendant = map_column({{
                {"a.b.c", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {10})},
                {"a.x", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {11})},
        }});
        const ColumnMap* descendant_ptr = descendant.get();
        VariantAssemblerPlanOptions descendant_options;
        descendant_options.requested_path = PathInData("a.b");
        descendant_options.has_doc = doc_mode;
        descendant_options.sparse_bucket_count = doc_mode ? 0 : 1;
        VariantAssemblerBatchView descendant_batch;
        descendant_batch.num_rows = 1;
        descendant_batch.doc_values = doc_mode ? descendant.get() : nullptr;
        descendant_batch.sparse_buckets =
                doc_mode ? std::span<const ColumnMap* const>()
                         : std::span<const ColumnMap* const>(&descendant_ptr, 1);
        VariantAssembledColumn descendant_result =
                assemble_once(create_plan(std::move(descendant_options)), descendant_batch);
        EXPECT_EQ(json_at(*descendant_result.values, 0), R"({"c":10})");
    }

    VariantAssemblerPlanOptions ambiguous;
    ambiguous.sparse_bucket_count = 1;
    ambiguous.materialized_paths.push_back(
            {.path = PathInData(dotted_parts), .type = std::make_shared<DataTypeInt64>()});
    std::shared_ptr<const VariantAssemblerPlan> rejected;
    EXPECT_TRUE(VariantAssemblerPlan::create(std::move(ambiguous), &rejected)
                        .is<ErrorCode::CORRUPTION>());
}

TEST(VariantAssemblerTest, HierarchicalUnknownFallbackNullsOnlyRowsWithoutPathMatches) {
    auto sparse = map_column({
            {{"a.b", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {9})}},
            {{"a.b.c", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {10})}},
            {{"a.x", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {11})}},
            {},
    });
    const ColumnMap* sparse_ptr = sparse.get();

    VariantAssemblerPlanOptions fallback_options;
    fallback_options.requested_path = PathInData("a.b");
    fallback_options.sparse_bucket_count = 1;
    fallback_options.null_on_no_match = true;
    VariantAssemblerBatchView batch;
    batch.num_rows = 4;
    batch.sparse_buckets = {&sparse_ptr, 1};
    VariantAssembledColumn fallback =
            assemble_once(create_plan(std::move(fallback_options)), batch);
    EXPECT_EQ(json_at(*fallback.values, 0), "9");
    EXPECT_EQ(json_at(*fallback.values, 1), R"({"c":10})");
    EXPECT_EQ(json_at(*fallback.values, 2), "null");
    EXPECT_EQ(json_at(*fallback.values, 3), "null");
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*fallback.outer_nulls).get_data(),
              (PaddedPODArray<uint8_t> {0, 0, 1, 1}));

    VariantAssemblerPlanOptions known_prefix_options;
    known_prefix_options.requested_path = PathInData("a.b");
    known_prefix_options.sparse_bucket_count = 1;
    VariantAssembledColumn known_prefix =
            assemble_once(create_plan(std::move(known_prefix_options)), batch);
    EXPECT_EQ(json_at(*known_prefix.values, 2), "{}");
    EXPECT_EQ(json_at(*known_prefix.values, 3), "{}");
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*known_prefix.outer_nulls).get_data(),
              (PaddedPODArray<uint8_t> {0, 0, 0, 0}));
}

TEST(VariantAssemblerTest, SparseEmptyKeyIsDistinctFromLogicalRoot) {
    auto empty_key = map_column({{
            {"", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {9})},
            {"sibling", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {10})},
    }});
    const ColumnMap* empty_key_ptr = empty_key.get();

    VariantAssemblerPlanOptions root_options;
    root_options.sparse_bucket_count = 1;
    VariantAssemblerBatchView root_batch;
    root_batch.num_rows = 1;
    root_batch.sparse_buckets = {&empty_key_ptr, 1};
    VariantAssembler root_assembler(create_plan(std::move(root_options)));
    VariantAssembledColumn root;
    const Status root_status = root_assembler.assemble(root_batch, &root);
    ASSERT_TRUE(root_status.ok()) << root_status;
    EXPECT_EQ(json_at(*root.values, 0), R"({"":9,"sibling":10})");

    VariantAssemblerPlanOptions exact_options;
    exact_options.requested_path = PathInData("");
    exact_options.sparse_bucket_count = 1;
    VariantAssemblerBatchView exact_batch;
    exact_batch.num_rows = 1;
    exact_batch.sparse_buckets = {&empty_key_ptr, 1};
    VariantAssembler exact_assembler(create_plan(std::move(exact_options)));
    VariantAssembledColumn exact;
    const Status exact_status = exact_assembler.assemble(exact_batch, &exact);
    ASSERT_TRUE(exact_status.ok()) << exact_status;
    EXPECT_EQ(json_at(*exact.values, 0), "9");

    auto descendant =
            map_column({{{".child", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {11})}}});
    const ColumnMap* descendant_ptr = descendant.get();
    VariantAssemblerPlanOptions descendant_options;
    descendant_options.sparse_bucket_count = 1;
    VariantAssemblerBatchView descendant_batch;
    descendant_batch.num_rows = 1;
    descendant_batch.sparse_buckets = {&descendant_ptr, 1};
    VariantAssembler descendant_assembler(create_plan(std::move(descendant_options)));
    VariantAssembledColumn descendant_result;
    const Status descendant_status =
            descendant_assembler.assemble(descendant_batch, &descendant_result);
    ASSERT_TRUE(descendant_status.ok()) << descendant_status;
    EXPECT_EQ(json_at(*descendant_result.values, 0), R"({"":{"child":11}})");

    auto empty_root_with_child = map_column({{
            {"", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {12})},
            {".child", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {13})},
    }});
    const ColumnMap* empty_root_with_child_ptr = empty_root_with_child.get();
    VariantAssemblerPlanOptions empty_conflict_options;
    empty_conflict_options.requested_path = PathInData("");
    empty_conflict_options.sparse_bucket_count = 1;
    VariantAssembler empty_conflict(create_plan(std::move(empty_conflict_options)));
    VariantAssemblerBatchView empty_conflict_batch;
    empty_conflict_batch.num_rows = 1;
    empty_conflict_batch.sparse_buckets = {&empty_root_with_child_ptr, 1};
    VariantAssembledColumn empty_conflict_result;
    EXPECT_TRUE(empty_conflict.assemble(empty_conflict_batch, &empty_conflict_result)
                        .is<ErrorCode::CORRUPTION>());

    auto root_with_child = map_column({{
            {"a", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {14})},
            {"a.b", fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {15})},
    }});
    const ColumnMap* root_with_child_ptr = root_with_child.get();
    VariantAssemblerPlanOptions conflict_options;
    conflict_options.requested_path = PathInData("a");
    conflict_options.sparse_bucket_count = 1;
    VariantAssembler conflict(create_plan(std::move(conflict_options)));
    VariantAssemblerBatchView conflict_batch;
    conflict_batch.num_rows = 1;
    conflict_batch.sparse_buckets = {&root_with_child_ptr, 1};
    VariantAssembledColumn conflict_result;
    EXPECT_TRUE(conflict.assemble(conflict_batch, &conflict_result).is<ErrorCode::CORRUPTION>());
}

TEST(VariantAssemblerTest, MaterializedDecimalAndArrayUsePreparedShape) {
    auto elements = ColumnInt64::create();
    elements->insert_value(1);
    elements->insert_value(2);
    auto element_nulls = ColumnUInt8::create();
    element_nulls->insert_value(0);
    element_nulls->insert_value(0);
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(2);
    auto array = ColumnArray::create(
            ColumnNullable::create(std::move(elements), std::move(element_nulls)),
            std::move(offsets));
    auto decimal = ColumnDecimal64::create(0, 2);
    decimal->insert_value(Decimal64 {1234});
    std::array<const IColumn*, 2> columns {array.get(), decimal.get()};

    VariantAssemblerPlanOptions options;
    options.materialized_paths.push_back(
            {.path = PathInData("a"),
             .type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>())});
    options.materialized_paths.push_back(
            {.path = PathInData("d"), .type = std::make_shared<DataTypeDecimal64>(10, 2)});
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.materialized_columns = columns;
    VariantAssembledColumn result = assemble_once(create_plan(std::move(options)), batch);
    EXPECT_EQ(json_at(*result.values, 0), R"({"a":[1,2],"d":12.34})");
}

TEST(VariantAssemblerTest, RejectsExtraModeStreamsAndUnorderedOrDuplicatePaths) {
    auto root = root_column({"{}"});
    auto binary = binary_column({fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {1})});
    struct ModeCase {
        VariantAssemblerMode mode;
        VariantAssemblerPlanOptions options;
        VariantAssemblerBatchView batch;
    };
    std::vector<ModeCase> cases;
    VariantAssemblerPlanOptions default_options;
    default_options.mode = VariantAssemblerMode::DEFAULT_FILL;
    VariantAssemblerBatchView default_batch;
    default_batch.num_rows = 1;
    default_batch.root_jsonb = root.get();
    cases.push_back(
            {VariantAssemblerMode::DEFAULT_FILL, std::move(default_options), default_batch});
    VariantAssemblerPlanOptions root_options;
    root_options.mode = VariantAssemblerMode::ROOT_FLAT;
    root_options.has_root = true;
    VariantAssemblerBatchView root_batch;
    root_batch.num_rows = 1;
    root_batch.root_jsonb = root.get();
    root_batch.binary_values = binary.get();
    cases.push_back({VariantAssemblerMode::ROOT_FLAT, std::move(root_options), root_batch});
    VariantAssemblerPlanOptions binary_options;
    binary_options.mode = VariantAssemblerMode::BINARY_EXTRACT;
    VariantAssemblerBatchView binary_batch;
    binary_batch.num_rows = 1;
    binary_batch.root_jsonb = root.get();
    binary_batch.binary_values = binary.get();
    cases.push_back(
            {VariantAssemblerMode::BINARY_EXTRACT, std::move(binary_options), binary_batch});
    VariantAssemblerPlanOptions hierarchical_options;
    VariantAssemblerBatchView hierarchical_batch;
    hierarchical_batch.num_rows = 1;
    hierarchical_batch.binary_values = binary.get();
    cases.push_back({VariantAssemblerMode::HIERARCHICAL, std::move(hierarchical_options),
                     hierarchical_batch});
    for (ModeCase& test : cases) {
        const auto plan = create_plan(std::move(test.options));
        VariantAssembler assembler(plan);
        VariantAssembledColumn output;
        EXPECT_FALSE(assembler.assemble(test.batch, &output).ok()) << static_cast<int>(test.mode);
    }
    VariantAssemblerPlanOptions unknown;
    unknown.mode = static_cast<VariantAssemblerMode>(0xff);
    std::shared_ptr<const VariantAssemblerPlan> rejected;
    EXPECT_TRUE(VariantAssemblerPlan::create(std::move(unknown), &rejected)
                        .is<ErrorCode::INVALID_ARGUMENT>());

    VariantAssemblerPlanOptions logical_duplicate;
    logical_duplicate.materialized_paths.push_back(
            {.path = PathInData("a", false), .type = std::make_shared<DataTypeInt64>()});
    logical_duplicate.materialized_paths.push_back(
            {.path = PathInData("a", true), .type = std::make_shared<DataTypeInt64>()});
    EXPECT_TRUE(VariantAssemblerPlan::create(std::move(logical_duplicate), &rejected)
                        .is<ErrorCode::CORRUPTION>());

    for (const auto& rows : {
                 std::vector<std::vector<std::pair<std::string, std::string>>> {
                         {{"b", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {1})},
                          {"a", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {2})}}},
                 std::vector<std::vector<std::pair<std::string, std::string>>> {
                         {{"a", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {1})},
                          {"a", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {2})}}},
                 std::vector<std::vector<std::pair<std::string, std::string>>> {
                         {{"a", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {1})},
                          {"a.b", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {2})}}},
         }) {
        auto map = map_column(rows);
        const ColumnMap* map_ptr = map.get();
        VariantAssemblerPlanOptions options;
        options.sparse_bucket_count = 1;
        VariantAssembler assembler(create_plan(std::move(options)));
        VariantAssembledColumn output;
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.sparse_buckets = {&map_ptr, 1};
        EXPECT_TRUE(assembler.assemble(batch, &output).is<ErrorCode::CORRUPTION>());
    }
}

TEST(VariantAssemblerTest, RawDottedOrderMergesAcrossBucketsAndBuilderCanonicalizes) {
    auto first = map_column({{
            {"a-", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {1})},
            {"a.b", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {2})},
    }});
    auto second = map_column({{{"a.c", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {3})}}});
    std::array<const ColumnMap*, 2> buckets {first.get(), second.get()};
    VariantAssemblerPlanOptions options;
    options.sparse_bucket_count = 2;
    VariantAssemblerBatchView batch;
    batch.num_rows = 1;
    batch.sparse_buckets = buckets;
    VariantAssembledColumn result = assemble_once(create_plan(std::move(options)), batch);
    EXPECT_EQ(json_at(*result.values, 0), R"({"a":{"b":2,"c":3},"a-":1})");
    validate_canonical(result.values->get_value_ref(0));

    auto nested = ColumnInt64::create();
    nested->insert_value(2);
    auto delimiter = ColumnInt64::create();
    delimiter->insert_value(1);
    std::array<const IColumn*, 2> materialized {nested.get(), delimiter.get()};
    auto sparse = map_column({{{"a.c", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {3})}}});
    const ColumnMap* sparse_ptr = sparse.get();
    VariantAssemblerPlanOptions mixed_options;
    mixed_options.materialized_paths.push_back(
            {.path = PathInData("a.b"), .type = std::make_shared<DataTypeInt64>()});
    mixed_options.materialized_paths.push_back(
            {.path = PathInData("a-"), .type = std::make_shared<DataTypeInt64>()});
    mixed_options.sparse_bucket_count = 1;
    VariantAssemblerBatchView mixed_batch;
    mixed_batch.num_rows = 1;
    mixed_batch.materialized_columns = materialized;
    mixed_batch.sparse_buckets = {&sparse_ptr, 1};
    VariantAssembledColumn mixed_result =
            assemble_once(create_plan(std::move(mixed_options)), mixed_batch);
    EXPECT_EQ(json_at(*mixed_result.values, 0), R"({"a":{"b":2,"c":3},"a-":1})");
    validate_canonical(mixed_result.values->get_value_ref(0));
}

TEST(VariantAssemblerTest, CrossSourceDuplicatesFailAndOuterNullMasksBadHierarchicalBytes) {
    auto materialized = ColumnInt64::create();
    materialized->insert_value(1);
    const IColumn* materialized_ptr = materialized.get();
    auto duplicate = map_column({{{"a", fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {2})}}});
    const ColumnMap* duplicate_ptr = duplicate.get();
    VariantAssemblerPlanOptions duplicate_options;
    duplicate_options.materialized_paths.push_back(
            {.path = PathInData("a"), .type = std::make_shared<DataTypeInt64>()});
    duplicate_options.sparse_bucket_count = 1;
    VariantAssemblerBatchView duplicate_batch;
    duplicate_batch.num_rows = 1;
    duplicate_batch.materialized_columns = {&materialized_ptr, 1};
    duplicate_batch.sparse_buckets = {&duplicate_ptr, 1};
    VariantAssembler duplicate_assembler(create_plan(std::move(duplicate_options)));
    VariantAssembledColumn sentinel;
    sentinel.values = ColumnVariantV2::create();
    const auto* sentinel_values = sentinel.values.get();
    EXPECT_TRUE(
            duplicate_assembler.assemble(duplicate_batch, &sentinel).is<ErrorCode::CORRUPTION>());
    EXPECT_EQ(sentinel.values.get(), sentinel_values);

    auto roots = ColumnString::create();
    roots->insert_data("bad", 3);
    const std::string valid = jsonb_bytes(R"({"ok":1})");
    roots->insert_data(valid.data(), valid.size());
    std::string malformed(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_INT));
    auto sparse = map_column({{{"bad", malformed}}, {}});
    const ColumnMap* sparse_ptr = sparse.get();
    VariantAssemblerPlanOptions masked_options;
    masked_options.has_root = true;
    masked_options.sparse_bucket_count = 1;
    const auto masked_plan = create_plan(std::move(masked_options));
    constexpr std::array<uint8_t, 2> MASKED {1, 0};
    VariantAssemblerBatchView masked_batch;
    masked_batch.num_rows = 2;
    masked_batch.outer_nulls = MASKED;
    masked_batch.root_jsonb = roots.get();
    masked_batch.sparse_buckets = {&sparse_ptr, 1};
    VariantAssembledColumn masked_result = assemble_once(masked_plan, masked_batch);
    EXPECT_EQ(json_at(*masked_result.values, 0), "null");
    EXPECT_EQ(json_at(*masked_result.values, 1), R"({"ok":1})");

    VariantAssembler failing(masked_plan);
    VariantAssemblerBatchView unmasked_batch = masked_batch;
    unmasked_batch.outer_nulls = {};
    VariantAssembledColumn atomic;
    atomic.values = ColumnVariantV2::create();
    const auto* atomic_values = atomic.values.get();
    EXPECT_TRUE(failing.assemble(unmasked_batch, &atomic).is<ErrorCode::CORRUPTION>());
    EXPECT_EQ(atomic.values.get(), atomic_values);
}

TEST(VariantAssemblerTest, BinaryExtractCoversStorageFieldTypesAndMaskedBadBytes) {
    const __int128 decimal38_max = [] {
        __int128 value = 1;
        for (int digit = 0; digit < 38; ++digit) {
            value *= 10;
        }
        return value - 1;
    }();
    const auto date =
            DateV2Value<DateV2ValueType>::create_from_olap_date(pack_olap_date(1970, 1, 2));
    auto datetime = DateV2Value<DateTimeV2ValueType>::create_from_olap_datetime(19700101000001ULL);
    datetime.set_microsecond(234567);
    TimestampTzValue timestamp_tz;
    timestamp_tz.unchecked_set_time(1970, 1, 1, 0, 0, 2, 345678);
    std::string datetime_cell(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_DATETIMEV2));
    datetime_cell.push_back(6);
    append_pod(datetime_cell, binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(datetime));
    std::string timestamp_tz_cell(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ));
    timestamp_tz_cell.push_back(6);
    append_pod(timestamp_tz_cell, binary_cast<TimestampTzValue, UInt64>(timestamp_tz));
    std::string decimal64(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_DECIMAL64));
    decimal64.push_back(10);
    decimal64.push_back(2);
    append_pod(decimal64, int64_t {1234});
    std::string decimal128(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_DECIMAL128I));
    decimal128.push_back(20);
    decimal128.push_back(3);
    append_pod(decimal128, __int128 {12345});
    std::string decimal256(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_DECIMAL256));
    decimal256.push_back(40);
    decimal256.push_back(4);
    append_pod(decimal256, wide::Int256 {123456});
    struct Case {
        std::string cell;
        std::string_view expected;
        bool typed;
    };
    const std::vector<Case> cases {
            {std::string(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_NONE)), "null", false},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_BOOL, uint8_t {1}), "true", true},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_TINYINT, int8_t {-1}), "-1", true},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_SMALLINT, int16_t {-2}), "-2", true},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {-3}), "-3", true},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {-4}), "-4", true},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_LARGEINT, __int128 {5}), "5", true},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_LARGEINT, decimal38_max),
             "99999999999999999999999999999999999999", true},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_FLOAT, float {1.25}), "1.25", true},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_DOUBLE, double {2.5}), "2.5", true},
            {string_cell("text"), R"("text")", true},
            {jsonb_cell(R"({"j":1})"), R"({"j":1})", false},
            {array_cell({fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {1}),
                         string_cell("x")}),
             R"([1,"x"])", false},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_IPV4, IPv4 {0}), R"("0.0.0.0")", true},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_IPV6, IPv6 {}), R"("::")", true},
            {fixed_cell(FieldType::OLAP_FIELD_TYPE_DATEV2,
                        binary_cast<DateV2Value<DateV2ValueType>, UInt32>(date)),
             R"("1970-01-02")", true},
            {datetime_cell, R"("1970-01-01 00:00:01.234567")", true},
            {timestamp_tz_cell, R"("1970-01-01 00:00:02.345678+00:00")", true},
            {decimal32_cell(8, 2, 123), "1.23", true},
            {decimal64, "12.34", true},
            {decimal128, "12.345", true},
            {decimal256, R"("12.3456")", false},
    };
    VariantAssemblerPlanOptions options;
    options.mode = VariantAssemblerMode::BINARY_EXTRACT;
    const auto plan = create_plan(std::move(options));
    VariantAssembler assembler(plan);
    for (const Case& test : cases) {
        auto binary = binary_column({test.cell});
        VariantAssembledColumn output;
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.binary_values = binary.get();
        const Status status = assembler.assemble(batch, &output);
        ASSERT_TRUE(status.ok()) << static_cast<int>(static_cast<uint8_t>(test.cell[0])) << ": "
                                 << status;
        ASSERT_TRUE(static_cast<bool>(output.values));
        EXPECT_EQ(output.values->size(), 1);
        EXPECT_EQ(output.values->is_typed(), test.typed);
        EXPECT_EQ(json_at(*output.values, 0), test.expected);
    }

    for (const __int128 value : {decimal38_max + 1, -(decimal38_max + 1)}) {
        VariantAssembler outside_decimal38_assembler(plan);
        auto outside_decimal38 =
                binary_column({fixed_cell(FieldType::OLAP_FIELD_TYPE_LARGEINT, value)});
        VariantAssemblerBatchView outside_decimal38_batch;
        outside_decimal38_batch.num_rows = 1;
        outside_decimal38_batch.binary_values = outside_decimal38.get();
        VariantAssembledColumn outside_decimal38_output;
        ASSERT_TRUE(outside_decimal38_assembler
                            .assemble(outside_decimal38_batch, &outside_decimal38_output)
                            .ok());
        ASSERT_TRUE(outside_decimal38_output.values->is_typed());
        std::string expected = value < 0 ? "\"-" : "\"";
        expected.push_back('1');
        expected.append(38, '0');
        expected.push_back('"');
        EXPECT_EQ(json_at(*outside_decimal38_output.values, 0), expected);
    }

    auto mixed = binary_column({fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {1}),
                                fixed_cell(FieldType::OLAP_FIELD_TYPE_BIGINT, int64_t {2})});
    VariantAssemblerBatchView mixed_batch;
    mixed_batch.num_rows = 2;
    mixed_batch.binary_values = mixed.get();
    VariantAssembledColumn mixed_output;
    ASSERT_TRUE(assembler.assemble(mixed_batch, &mixed_output).ok());
    EXPECT_FALSE(mixed_output.values->is_typed());
    EXPECT_EQ(json_at(*mixed_output.values, 0), "1");
    EXPECT_EQ(json_at(*mixed_output.values, 1), "2");

    std::string malformed(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_INT));
    auto masked = binary_column(
            {malformed, malformed, fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {8})});
    constexpr std::array<uint8_t, 3> OUTER_NULLS {1, 0, 0};
    constexpr std::array<uint8_t, 3> MISSING {0, 1, 0};
    VariantAssemblerBatchView masked_batch;
    masked_batch.num_rows = 3;
    masked_batch.outer_nulls = OUTER_NULLS;
    masked_batch.binary_values = masked.get();
    masked_batch.binary_missing = MISSING;
    VariantAssembledColumn masked_output;
    ASSERT_TRUE(assembler.assemble(masked_batch, &masked_output).ok());
    ASSERT_TRUE(masked_output.values->is_typed());
    const auto& typed_nullable =
            assert_cast<const ColumnNullable&>(masked_output.values->typed_column());
    EXPECT_EQ(typed_nullable.get_null_map_data(), (NullMap {1, 1, 0}));
    EXPECT_EQ(assert_cast<const ColumnUInt8&>(*masked_output.outer_nulls).get_data(),
              (PaddedPODArray<uint8_t> {1, 1, 0}));
    EXPECT_EQ(json_at(*masked_output.values, 2), "8");
}

TEST(VariantAssemblerTest, BinaryRejectsMalformedCellsAtomically) {
    std::string depth_128(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_NONE));
    for (size_t depth = 0; depth < VARIANT_MAX_NESTING_DEPTH; ++depth) {
        depth_128 = array_cell({depth_128});
    }
    VariantAssemblerPlanOptions depth_options;
    depth_options.mode = VariantAssemblerMode::BINARY_EXTRACT;
    auto depth_binary = binary_column({depth_128});
    VariantAssemblerBatchView depth_batch;
    depth_batch.num_rows = 1;
    depth_batch.binary_values = depth_binary.get();
    VariantAssembledColumn depth_output =
            assemble_once(create_plan(std::move(depth_options)), depth_batch);
    EXPECT_EQ(depth_output.values->size(), 1);

    std::vector<std::string> malformed;
    malformed.emplace_back(1, static_cast<char>(0xff));
    malformed.emplace_back(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_INT));
    malformed.push_back(fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {1}) + "x");
    malformed.push_back(fixed_cell(FieldType::OLAP_FIELD_TYPE_BOOL, uint8_t {2}));
    malformed.push_back(fixed_cell(FieldType::OLAP_FIELD_TYPE_DATEV2, UInt32 {0}));
    std::string bad_datetime(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_DATETIMEV2));
    bad_datetime.push_back(7);
    append_pod(bad_datetime, UInt64 {0});
    malformed.push_back(std::move(bad_datetime));
    std::string invalid_datetime(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_DATETIMEV2));
    invalid_datetime.push_back(6);
    append_pod(invalid_datetime, UInt64 {0});
    malformed.push_back(std::move(invalid_datetime));
    std::string invalid_timestamp_tz(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ));
    invalid_timestamp_tz.push_back(6);
    append_pod(invalid_timestamp_tz, UInt64 {0});
    malformed.push_back(std::move(invalid_timestamp_tz));
    malformed.push_back(decimal32_cell(2, 0, 123));
    malformed.push_back(decimal32_cell(0, 0, 0));
    std::string invalid_jsonb(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_JSONB));
    append_pod(invalid_jsonb, size_t {3});
    invalid_jsonb.append("bad", 3);
    malformed.push_back(std::move(invalid_jsonb));
    std::string truncated_array(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_ARRAY));
    append_pod(truncated_array, size_t {2});
    truncated_array.append(fixed_cell(FieldType::OLAP_FIELD_TYPE_INT, int32_t {1}));
    malformed.push_back(std::move(truncated_array));
    malformed.push_back(array_cell({depth_128}));

    for (const std::string& cell : malformed) {
        VariantAssemblerPlanOptions options;
        options.mode = VariantAssemblerMode::BINARY_EXTRACT;
        VariantAssembler assembler(create_plan(std::move(options)));
        auto binary = binary_column({cell});
        VariantAssembledColumn sentinel;
        sentinel.values = ColumnVariantV2::create();
        const auto* sentinel_values = sentinel.values.get();
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.binary_values = binary.get();
        EXPECT_TRUE(assembler.assemble(batch, &sentinel).is<ErrorCode::CORRUPTION>());
        EXPECT_EQ(sentinel.values.get(), sentinel_values);
        EXPECT_TRUE(assembler.assemble(batch, &sentinel).is<ErrorCode::CORRUPTION>());
    }
}

TEST(VariantAssemblerTest, PathAndValueDepthShareOneLimit) {
    const std::string valid_path = dotted_path(VARIANT_MAX_NESTING_DEPTH - 1);
    const std::string invalid_path = dotted_path(VARIANT_MAX_NESTING_DEPTH);
    const std::string one_array =
            array_cell({std::string(1, static_cast<char>(FieldType::OLAP_FIELD_TYPE_NONE))});
    for (const auto& [path, expected_ok] :
         {std::pair {valid_path, true}, std::pair {invalid_path, false}}) {
        auto sparse = map_column({{{path, one_array}}});
        const ColumnMap* sparse_ptr = sparse.get();
        VariantAssemblerPlanOptions options;
        options.sparse_bucket_count = 1;
        VariantAssembler assembler(create_plan(std::move(options)));
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.sparse_buckets = {&sparse_ptr, 1};
        VariantAssembledColumn output;
        const Status status = assembler.assemble(batch, &output);
        EXPECT_EQ(status.ok(), expected_ok) << status;
        if (expected_ok) {
            validate_canonical(output.values->get_value_ref(0));
        } else {
            EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>());
        }
    }

    const std::string empty_object = jsonb_bytes("{}");
    for (const auto& [path, expected_ok] :
         {std::pair {valid_path, true}, std::pair {invalid_path, false}}) {
        auto jsonb = ColumnString::create();
        jsonb->insert_data(empty_object.data(), empty_object.size());
        const IColumn* jsonb_ptr = jsonb.get();
        VariantAssemblerPlanOptions options;
        options.materialized_paths.push_back(
                {.path = PathInData(path), .type = std::make_shared<DataTypeJsonb>()});
        VariantAssembler assembler(create_plan(std::move(options)));
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.materialized_columns = {&jsonb_ptr, 1};
        VariantAssembledColumn output;
        const Status status = assembler.assemble(batch, &output);
        EXPECT_EQ(status.ok(), expected_ok) << status;
        if (expected_ok) {
            validate_canonical(output.values->get_value_ref(0));
        } else {
            EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>() ||
                        status.is<ErrorCode::CORRUPTION>());
        }
    }

    for (const auto& [path, expected_ok] :
         {std::pair {valid_path, true}, std::pair {invalid_path, false}}) {
        auto elements = ColumnInt64::create();
        elements->insert_value(1);
        auto element_nulls = ColumnUInt8::create();
        element_nulls->insert_value(0);
        auto offsets = ColumnArray::ColumnOffsets::create();
        offsets->insert_value(1);
        auto array = ColumnArray::create(
                ColumnNullable::create(std::move(elements), std::move(element_nulls)),
                std::move(offsets));
        const IColumn* array_ptr = array.get();
        VariantAssemblerPlanOptions options;
        options.materialized_paths.push_back(
                {.path = PathInData(path),
                 .type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeInt64>())});
        VariantAssembler assembler(create_plan(std::move(options)));
        VariantAssemblerBatchView batch;
        batch.num_rows = 1;
        batch.materialized_columns = {&array_ptr, 1};
        VariantAssembledColumn output;
        const Status status = assembler.assemble(batch, &output);
        EXPECT_EQ(status.ok(), expected_ok) << status;
        if (expected_ok) {
            validate_canonical(output.values->get_value_ref(0));
        } else {
            EXPECT_TRUE(status.is<ErrorCode::CORRUPTION>());
        }
    }
}

TEST(VariantAssemblerTest, C3BucketedSparseAndDocManifestCellsAssembleExactly) {
    const C3Fixture bucketed = load_c3_manifest("bucketed_sparse");
    ASSERT_EQ(bucketed.assembled.size(), 12);
    ASSERT_EQ(bucketed.cell_count, 24);
    std::vector<std::vector<std::pair<std::string, std::string>>> bucket_rows[3];
    for (auto& rows : bucket_rows) {
        rows.resize(12);
    }
    for (const auto& [row, cells] : bucketed.cells) {
        for (const auto& cell : cells) {
            const size_t bucket = variant_util::variant_binary_shard_of(
                    {cell.first.data(), cell.first.size()}, 3);
            bucket_rows[bucket][row].push_back(cell);
        }
    }
    auto bucket0 = map_column(bucket_rows[0]);
    auto bucket1 = map_column(bucket_rows[1]);
    auto bucket2 = map_column(bucket_rows[2]);
    std::array<const ColumnMap*, 3> bucket_ptrs {bucket0.get(), bucket1.get(), bucket2.get()};
    std::vector<std::optional<int64_t>> hot_values;
    ASSERT_EQ(bucketed.source.size(), 12);
    for (size_t row = 0; row < 12; ++row) {
        const auto value = source_value(bucketed, row, "hot");
        ASSERT_TRUE(value.has_value());
        hot_values.emplace_back(std::stoll(std::string(*value)));
    }
    ColumnPtr hot = nullable_int64_column(hot_values);
    const IColumn* hot_ptr = hot.get();
    VariantAssemblerPlanOptions bucket_options;
    bucket_options.materialized_paths.push_back(
            {.path = PathInData("hot"),
             .type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>())});
    bucket_options.sparse_bucket_count = 3;
    VariantAssemblerBatchView bucket_batch;
    bucket_batch.num_rows = 12;
    bucket_batch.materialized_columns = {&hot_ptr, 1};
    bucket_batch.sparse_buckets = bucket_ptrs;
    VariantAssembledColumn bucket_output =
            assemble_once(create_plan(std::move(bucket_options)), bucket_batch);
    expect_manifest_rows(&bucket_output, bucketed, 12);

    const C3Fixture doc = load_c3_manifest("doc");
    ASSERT_EQ(doc.assembled.size(), 8);
    ASSERT_EQ(doc.cell_count, 40);
    auto doc_map = map_column(manifest_rows(doc, 8));
    VariantAssemblerPlanOptions doc_options;
    doc_options.has_doc = true;
    VariantAssemblerBatchView doc_batch;
    doc_batch.num_rows = 8;
    doc_batch.doc_values = doc_map.get();
    VariantAssembledColumn doc_output =
            assemble_once(create_plan(std::move(doc_options)), doc_batch);
    expect_manifest_rows(&doc_output, doc, 8);
}

TEST(VariantAssemblerTest, C3OrdinaryRootMaterializedSparsePriorityAssemblesExactly) {
    const C3Fixture fixture = load_c3_manifest("ordinary_sparse");
    ASSERT_EQ(fixture.assembled.size(), 13);
    ASSERT_EQ(fixture.cell_count, 45);
    auto sparse = map_column(manifest_rows(fixture, 13));
    const ColumnMap* sparse_ptr = sparse.get();
    std::vector<std::string> roots;
    std::vector<std::optional<int64_t>> collision;
    std::vector<std::optional<int64_t>> hot;
    ASSERT_EQ(fixture.source.size(), 13);
    for (size_t row = 0; row < 13; ++row) {
        const auto root_value = source_value(fixture, row, "$");
        ASSERT_TRUE(root_value.has_value());
        roots.emplace_back(*root_value);
        const auto collision_value = source_value(fixture, row, "collision");
        collision.emplace_back(collision_value.has_value() ? std::optional<int64_t>(std::stoll(
                                                                     std::string(*collision_value)))
                                                           : std::nullopt);
        const auto hot_value = source_value(fixture, row, "hot");
        hot.emplace_back(hot_value.has_value()
                                 ? std::optional<int64_t>(std::stoll(std::string(*hot_value)))
                                 : std::nullopt);
    }
    auto root = root_column(roots);
    ColumnPtr collision_column = nullable_int64_column(collision);
    ColumnPtr hot_column = nullable_int64_column(hot);
    std::array<const IColumn*, 2> materialized {collision_column.get(), hot_column.get()};
    VariantAssemblerPlanOptions options;
    options.has_root = true;
    const auto nullable_bigint =
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    options.materialized_paths.push_back(
            {.path = PathInData("collision"), .type = nullable_bigint});
    options.materialized_paths.push_back({.path = PathInData("hot"), .type = nullable_bigint});
    options.sparse_bucket_count = 1;
    VariantAssemblerBatchView batch;
    batch.num_rows = 13;
    batch.root_jsonb = root.get();
    batch.materialized_columns = materialized;
    batch.sparse_buckets = {&sparse_ptr, 1};
    VariantAssembledColumn output = assemble_once(create_plan(std::move(options)), batch);
    ASSERT_EQ(output.values->size(), 13);
    for (size_t row = 0; row < 11; ++row) {
        std::string expected = compact_json_whitespace(fixture.assembled.at(row));
        const std::string root_only = "\"root_only\":" + std::to_string(row);
        const size_t text_position = expected.find("\"text\"");
        if (text_position == std::string::npos) {
            expected.insert(expected.size() - 1, "," + root_only);
        } else {
            expected.insert(text_position, root_only + ",");
        }
        EXPECT_EQ(json_at(*output.values, row), expected) << row;

        VariantRef collision_value;
        VariantRef root_only_value;
        ASSERT_TRUE(
                output.values->get_value_ref(row).object_find({"collision", 9}, &collision_value));
        ASSERT_TRUE(
                output.values->get_value_ref(row).object_find({"root_only", 9}, &root_only_value));
        ASSERT_TRUE(collision[row].has_value());
        EXPECT_EQ(collision_value.get_int(), *collision[row]);
        EXPECT_EQ(root_only_value.get_int(), static_cast<int64_t>(row));
    }
    EXPECT_EQ(json_at(*output.values, 11), R"({"collision":1011,"hot":11,"root_only":11})");
    EXPECT_EQ(json_at(*output.values, 12), compact_json_whitespace(fixture.assembled.at(12)));
}

} // namespace
} // namespace doris::segment_v2
