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

#include "storage/index/inverted/variant_root_index.h"

#include <gtest/gtest.h>

#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/value/variant/variant_field.h"
#include "core/value/variant/variant_scalar.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/segment/variant/v2/variant_root_index_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2::variant_root_index {
namespace {

std::string bytes(std::initializer_list<uint8_t> values) {
    std::string result;
    result.reserve(values.size());
    for (uint8_t value : values) {
        result.push_back(static_cast<char>(value));
    }
    return result;
}

std::string legacy_path_term(std::string_view path) {
    const auto path_size = static_cast<uint32_t>(path.size());
    std::string result;
    result.push_back(static_cast<char>(1));
    for (int shift = 24; shift >= 0; shift -= 8) {
        result.push_back(static_cast<char>((path_size >> shift) & 0xff));
    }
    result.append(path);
    result.push_back(static_cast<char>(1));
    return result;
}

TEST(VariantRootIndexCodecTest, GoldenTermsKeepPathTypeAndValueDistinct) {
    EXPECT_EQ(encode_string_term("键", "值"), bytes({1, 0, 0, 0, 3}) + "键" + bytes({6}) + "值");
    EXPECT_EQ(encode_string_term("payload.action", "opened"),
              bytes({1, 0, 0, 0, 14}) + "payload.action" + bytes({6}) + "opened");
    EXPECT_EQ(encode_token_term("payload.comment.body", "index"),
              bytes({1, 0, 0, 0, 20}) + "payload.comment.body" + bytes({7}) + "index");
    EXPECT_NE(encode_string_term("a", "bc"), encode_string_term("ab", "c"));
    EXPECT_NE(encode_string_term("a", "1"), encode_int64_term("a", 1));
}

TEST(VariantRootIndexCodecTest, NumericTermsUseStableBigEndianPayloads) {
    EXPECT_EQ(encode_int64_term("n", -1),
              bytes({1, 0, 0, 0, 1, 'n', 2, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}));
    EXPECT_EQ(encode_uint64_term("n", 42), bytes({1, 0, 0, 0, 1, 'n', 3, 0, 0, 0, 0, 0, 0, 0, 42}));
    EXPECT_EQ(encode_double_term("n", 1.5),
              bytes({1, 0, 0, 0, 1, 'n', 4, 0x3f, 0xf8, 0, 0, 0, 0, 0, 0}));
    EXPECT_EQ(encode_double_term("n", -0.0), encode_double_term("n", 0.0));
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions pin the numeric matrix.
TEST(VariantRootIndexCodecTest, NumericValuesUseOneCanonicalEqualityTerm) {
    const auto query_terms = [](const Field& field) {
        std::vector<std::string> terms;
        EXPECT_TRUE(encode_query_value_terms("n", field, &terms).ok());
        return terms;
    };
    const auto value_terms = [](const VariantScalarRef& scalar) {
        const VariantField field = VariantField::from_scalar(scalar);
        std::vector<std::string> terms;
        EXPECT_TRUE(append_variant_value_terms("n", field.ref(), &terms).ok());
        return terms;
    };

    const std::vector<std::string> positive_integer {encode_int64_term("n", 42)};
    EXPECT_EQ(query_terms(Field::create_field<TYPE_BIGINT>(42)), positive_integer);
    EXPECT_EQ(query_terms(Field::create_field<TYPE_UINT64>(42)), positive_integer);
    EXPECT_EQ(query_terms(Field::create_field<TYPE_FLOAT>(42.0F)), positive_integer);
    EXPECT_EQ(query_terms(Field::create_field<TYPE_DOUBLE>(42.0)), positive_integer);
    EXPECT_EQ(value_terms(VariantScalarRef::integer(42)), positive_integer);
    EXPECT_EQ(value_terms(VariantScalarRef::float32(42.0F)), positive_integer);
    EXPECT_EQ(value_terms(VariantScalarRef::float64(42.0)), positive_integer);

    const std::vector<std::string> zero {encode_int64_term("n", 0)};
    EXPECT_EQ(query_terms(Field::create_field<TYPE_DOUBLE>(-0.0)), zero);
    EXPECT_EQ(value_terms(VariantScalarRef::float64(-0.0)), zero);

    const uint64_t first_unsigned_only = uint64_t {1} << 63;
    const std::vector<std::string> unsigned_integer {encode_uint64_term("n", first_unsigned_only)};
    EXPECT_EQ(query_terms(Field::create_field<TYPE_UINT64>(first_unsigned_only)), unsigned_integer);
    EXPECT_EQ(query_terms(Field::create_field<TYPE_DOUBLE>(0x1p63)), unsigned_integer);
    EXPECT_EQ(value_terms(VariantScalarRef::float64(0x1p63)), unsigned_integer);

    const uint64_t largest_unsigned = std::numeric_limits<uint64_t>::max();
    const std::vector<std::string> largest_unsigned_integer {
            encode_uint64_term("n", largest_unsigned)};
    EXPECT_EQ(query_terms(Field::create_field<TYPE_UINT64>(largest_unsigned)),
              largest_unsigned_integer);
    const std::vector<std::string> first_floating_only {encode_double_term("n", 0x1p64)};
    EXPECT_EQ(query_terms(Field::create_field<TYPE_DOUBLE>(0x1p64)), first_floating_only);
    EXPECT_EQ(value_terms(VariantScalarRef::float64(0x1p64)), first_floating_only);

    const std::vector<std::string> fractional {encode_double_term("n", 1.5)};
    EXPECT_EQ(query_terms(Field::create_field<TYPE_DOUBLE>(1.5)), fractional);
    EXPECT_EQ(value_terms(VariantScalarRef::float64(1.5)), fractional);

    EXPECT_TRUE(
            query_terms(Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN()))
                    .empty());
    EXPECT_TRUE(value_terms(VariantScalarRef::float64(std::numeric_limits<double>::quiet_NaN()))
                        .empty());
}

TEST(VariantRootIndexCodecTest, RootModeRecognitionRequiresSupportedFormat) {
    EXPECT_TRUE(is_root_mode_properties(
            {{std::string(VARIANT_INDEX_MODE_KEY), std::string(VARIANT_INDEX_MODE_ROOT)},
             {std::string(VARIANT_ROOT_FORMAT_VERSION_KEY),
              std::string(VARIANT_ROOT_FORMAT_VERSION_V1)}}));
    EXPECT_FALSE(is_root_mode_properties({{std::string(VARIANT_INDEX_MODE_KEY), "children"}}));
    EXPECT_FALSE(is_root_mode_properties(
            {{std::string(VARIANT_INDEX_MODE_KEY), std::string(VARIANT_INDEX_MODE_ROOT)},
             {std::string(VARIANT_ROOT_FORMAT_VERSION_KEY), "2"}}));
}

TEST(VariantRootIndexCodecTest, CandidateRecheckSurvivesBooleanCombination) {
    auto exact_rows = std::make_shared<roaring::Roaring>();
    exact_rows->add(1);
    auto candidate_rows = std::make_shared<roaring::Roaring>();
    candidate_rows->add(1);
    candidate_rows->add(2);
    InvertedIndexResultBitmap exact(exact_rows, std::make_shared<roaring::Roaring>());
    InvertedIndexResultBitmap candidate(candidate_rows, std::make_shared<roaring::Roaring>(),
                                        /*requires_recheck=*/true);

    InvertedIndexResultBitmap conjunction = exact;
    conjunction &= candidate;
    EXPECT_TRUE(conjunction.requires_recheck());
    EXPECT_EQ(conjunction.get_data_bitmap()->cardinality(), 1U);

    InvertedIndexResultBitmap disjunction = exact;
    disjunction |= candidate;
    EXPECT_TRUE(disjunction.requires_recheck());
    EXPECT_EQ(disjunction.get_data_bitmap()->cardinality(), 2U);
}

class VariantRootIndexWriterTest : public testing::Test {
protected:
    static constexpr const char* TEST_DIR = "./ut_dir/variant_root_index_test";

    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(TEST_DIR).ok());
    }

    void TearDown() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
    }
};

TEST_F(VariantRootIndexWriterTest, KeepsOneDocumentPerVariantRow) {
    TabletIndexPB pb;
    pb.set_index_id(71);
    pb.set_index_name("payload_root_idx");
    pb.set_index_type(IndexType::INVERTED);
    pb.add_col_unique_id(3);
    (*pb.mutable_properties())[std::string(VARIANT_INDEX_MODE_KEY)] = VARIANT_INDEX_MODE_ROOT;
    (*pb.mutable_properties())[std::string(VARIANT_ROOT_FORMAT_VERSION_KEY)] =
            VARIANT_ROOT_FORMAT_VERSION_V1;
    (*pb.mutable_properties())["parser"] = "none";
    TabletIndex index;
    index.init_from_pb(pb);

    const std::string prefix = std::string(TEST_DIR) + "/root";
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                      &file_writer)
                        .ok());
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "root_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer));
    SniiIndexColumnWriter writer(&index_file_writer, &index, FieldType::OLAP_FIELD_TYPE_VARCHAR);
    ASSERT_TRUE(writer.init().ok());

    const std::vector<std::string> first_row {encode_string_term("action", "opened")};
    const std::vector<std::string> empty_object;
    const std::vector<std::string> json_null_row;
    ASSERT_TRUE(writer.add_document(first_row, {}).ok());
    ASSERT_TRUE(writer.add_document(empty_object, {}).ok());
    ASSERT_TRUE(writer.add_nulls(1).ok());
    ASSERT_TRUE(writer.add_document(json_null_row, {}).ok());
    ASSERT_TRUE(writer.finish().ok());
    ASSERT_TRUE(index_file_writer.begin_close().ok());
    ASSERT_TRUE(index_file_writer.finish_close().ok());

    IndexFileReader index_file_reader(io::global_local_filesystem(), prefix,
                                      InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(index_file_reader.init().ok());
    auto opened = index_file_reader.open_snii_index(&index);
    ASSERT_TRUE(opened.has_value()) << opened.error();
    EXPECT_EQ((*opened)->stats().doc_count, 4U);
    EXPECT_EQ((*opened)->stats().indexed_doc_count, 3U);
    EXPECT_EQ((*opened)->stats().null_count, 1U);

    std::vector<uint32_t> null_docids;
    ASSERT_TRUE((*opened)->read_null_docids(&null_docids).ok());
    EXPECT_EQ(null_docids, std::vector<uint32_t>({2}));

    std::vector<uint32_t> docids;
    ASSERT_TRUE(snii::query::term_query(**opened, encode_string_term("action", "opened"), &docids)
                        .ok());
    EXPECT_EQ(docids, std::vector<uint32_t>({0}));
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- One fixture pins both analyzer streams.
TEST_F(VariantRootIndexWriterTest, FansOutOneTraversalToExactAndTokenIndexes) {
    TabletIndexPB exact_pb;
    exact_pb.set_index_id(72);
    exact_pb.set_index_name("payload_root_exact_idx");
    exact_pb.set_index_type(IndexType::INVERTED);
    exact_pb.add_col_unique_id(3);
    (*exact_pb.mutable_properties())[std::string(VARIANT_INDEX_MODE_KEY)] = VARIANT_INDEX_MODE_ROOT;
    (*exact_pb.mutable_properties())[std::string(VARIANT_ROOT_FORMAT_VERSION_KEY)] =
            VARIANT_ROOT_FORMAT_VERSION_V1;
    (*exact_pb.mutable_properties())["parser"] = "none";
    TabletIndex exact_index;
    exact_index.init_from_pb(exact_pb);

    TabletIndexPB token_pb = exact_pb;
    token_pb.set_index_id(73);
    token_pb.set_index_name("payload_root_token_idx");
    (*token_pb.mutable_properties())["parser"] = "english";
    (*token_pb.mutable_properties())["support_phrase"] = "false";
    TabletIndex token_index;
    token_index.init_from_pb(token_pb);

    const std::string prefix = std::string(TEST_DIR) + "/traversal";
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                      &file_writer)
                        .ok());
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "root_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer));
    ::doris::segment_v2::VariantRootIndexWriter exact_writer(&index_file_writer, &exact_index,
                                                             /*is_direct_load=*/false,
                                                             /*check_duplicate_json_path=*/false);
    ::doris::segment_v2::VariantRootIndexWriter token_writer(&index_file_writer, &token_index,
                                                             /*is_direct_load=*/false,
                                                             /*check_duplicate_json_path=*/false);
    ASSERT_TRUE(exact_writer.init().ok());
    ASSERT_TRUE(token_writer.init().ok());

    auto values = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions format_options;
    for (const std::string_view json : {
                 R"({"":"empty","action":"opened","number":42,"nested":{"body":"Root search"}})",
                 R"({"action":"closed","number":7,"nullable":null})",
                 R"({})",
         }) {
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*values, slice, format_options).ok());
    }
    const std::vector<uint8_t> outer_nulls {0, 0, 0, 1};
    values->insert_default();
    std::array<::doris::segment_v2::VariantRootIndexWriter*, 2> writers = {&exact_writer,
                                                                           &token_writer};
    ASSERT_TRUE(append_variant_root_indexes(writers, values->read_view(), 0, values->size(),
                                            outer_nulls)
                        .ok());
    ASSERT_TRUE(exact_writer.finish().ok());
    ASSERT_TRUE(token_writer.finish().ok());
    ASSERT_TRUE(index_file_writer.begin_close().ok());
    ASSERT_TRUE(index_file_writer.finish_close().ok());

    IndexFileReader index_file_reader(io::global_local_filesystem(), prefix,
                                      InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(index_file_reader.init().ok());
    auto exact = index_file_reader.open_snii_index(&exact_index);
    ASSERT_TRUE(exact.has_value()) << exact.error();
    auto token = index_file_reader.open_snii_index(&token_index);
    ASSERT_TRUE(token.has_value()) << token.error();
    for (const auto* reader : {exact->get(), token->get()}) {
        EXPECT_EQ(reader->stats().doc_count, 4U);
        EXPECT_EQ(reader->stats().indexed_doc_count, 3U);
        EXPECT_EQ(reader->stats().null_count, 1U);
    }

    const auto expect_term = [](const snii::reader::LogicalIndexReader& reader,
                                const std::string& term, std::vector<uint32_t> expected) {
        std::vector<uint32_t> docids;
        ASSERT_TRUE(snii::query::term_query(reader, term, &docids).ok());
        EXPECT_EQ(docids, expected);
    };
    expect_term(**exact, legacy_path_term("nested"), {});
    expect_term(**exact, encode_string_term("", "empty"), {0});
    expect_term(**exact, encode_string_term("action", "opened"), {0});
    expect_term(**exact, encode_int64_term("number", 7), {1});
    expect_term(**exact, encode_double_term("number", 7.0), {});
    expect_term(**exact, encode_token_term("nested.body", "root"), {});

    expect_term(**token, legacy_path_term("nullable"), {});
    expect_term(**token, encode_string_term("action", "opened"), {});
    expect_term(**token, encode_int64_term("number", 7), {});
    expect_term(**token, encode_token_term("action", "opened"), {0});
    expect_term(**token, encode_token_term("nested.body", "root"), {0});
}

} // namespace
} // namespace doris::segment_v2::variant_root_index
