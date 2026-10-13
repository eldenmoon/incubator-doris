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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/field.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_leaf_visitor.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/variant_term_codec.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/segment/variant/v2/variant_root_index_writer.h"
#include "storage/segment/variant/v2/variant_shredder.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2::variant_root_index {
namespace {

using variant_term_codec::term_token;

std::map<std::string, std::string> values_properties(
        std::string_view version = VARIANT_ROOT_FORMAT_VERSION_CURRENT) {
    return {{std::string(VARIANT_INDEX_SCOPE_KEY), std::string(VARIANT_INDEX_SCOPE_VALUES)},
            {std::string(VARIANT_ROOT_FORMAT_VERSION_KEY), std::string(version)},
            {"parser", "none"}};
}

TabletIndex make_root_index(std::string_view parser) {
    TabletIndexPB pb;
    pb.set_index_id(7);
    pb.set_index_name("root");
    pb.set_index_type(IndexType::INVERTED);
    pb.add_col_unique_id(3);
    for (const auto& [key, value] : values_properties()) {
        (*pb.mutable_properties())[key] = value;
    }
    (*pb.mutable_properties())["parser"] = std::string(parser);
    TabletIndex index;
    index.init_from_pb(pb);
    return index;
}

TEST(VariantRootIndexTest, PropertiesDeclareARootIndexAtTheCurrentVersionOnly) {
    EXPECT_TRUE(is_root_index(values_properties()));
    EXPECT_TRUE(is_root_index(
            {{"variant_index_mode", "all_values"},
             {"variant_root_format_version", std::string(VARIANT_ROOT_FORMAT_VERSION_CURRENT)}}));
    EXPECT_TRUE(is_root_index(
            {{"variant_index_scope", " values "},
             {"variant_root_format_version", std::string(VARIANT_ROOT_FORMAT_VERSION_CURRENT)}}));
    // Older layouts (PR-era 3, value-first with a path suffix) are invisible, never misread.
    EXPECT_FALSE(is_root_index(values_properties("1")));
    EXPECT_FALSE(is_root_index(values_properties("3")));
    EXPECT_FALSE(is_root_index({{"variant_index_scope", "values"}}));
    EXPECT_FALSE(is_root_index(
            {{"variant_index_mode", "root"},
             {"variant_root_format_version", std::string(VARIANT_ROOT_FORMAT_VERSION_CURRENT)}}));
    EXPECT_FALSE(is_root_index(
            {{"variant_index_scope", "paths"},
             {"variant_root_format_version", std::string(VARIANT_ROOT_FORMAT_VERSION_CURRENT)}}));
    EXPECT_FALSE(is_root_index({{"parser", "none"}}));
    EXPECT_EQ(reader_type(make_root_index("none")), InvertedIndexReaderType::FULLTEXT);
    EXPECT_EQ(reader_type(make_root_index("english")), InvertedIndexReaderType::FULLTEXT);
}

struct LeafText {
    VariantLeafKind kind;
    std::string text;
};

ColumnVariantV2::MutablePtr encode_documents(const std::vector<std::string>& docs) {
    JsonStringToVariantEncoder encoder;
    for (const std::string& json : docs) {
        encoder.add_json({json.data(), json.size()});
    }
    VariantBatchBuilder batch = encoder.finish_batch();
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(batch);
    return column;
}

std::vector<LeafText> leaf_texts(const std::string& json) {
    auto column = encode_documents({json});
    std::vector<LeafText> out;
    EXPECT_TRUE(
            visit_variant_leaves(column->read_view().value_at(0), {}, [&](const VariantLeaf& leaf) {
                std::string text;
                if (canonical_leaf_text(leaf, &text)) {
                    out.push_back({leaf.kind, std::move(text)});
                }
                return Status::OK();
            }).ok());
    return out;
}

TEST(VariantRootIndexTest, CanonicalLeafTextIsWhatTheWriterAndTheScalarFallbackTokenize) {
    const std::vector<LeafText> leaves = leaf_texts(
            R"({"s":"x y","i":42,"neg":-5,"f":42.0,"d":42.7,"z":-0.0,"big":9223372036854775808.0,)"
            R"("t":true,"fl":false,"arr":[1,"a",{"k":"deep"}],"n":null,"e":{},"o":{"p":[]},)"
            R"("tiny":1e-7,"sum":0.30000000000000004})");
    std::vector<std::string> actual;
    for (const LeafText& leaf : leaves) {
        actual.push_back(leaf.text);
    }
    std::ranges::sort(actual);
    // Doubles print their shortest round-trip text: "1e-07" and "0.30000000000000004", not the
    // 16-digit rounding "0.3" that would name a different leaf.
    std::vector<std::string> expected = {
            "x y",  "42",    "-5", "42", "42.7", "0",     "9223372036854775808",
            "true", "false", "1",  "a",  "deep", "1e-07", "0.30000000000000004"};
    std::ranges::sort(expected);
    EXPECT_EQ(actual, expected);
}

class VariantRootIndexWriterTest : public testing::Test {
protected:
    static constexpr const char* TEST_DIR = "./ut_dir/variant_values_index_test";

    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(TEST_DIR).ok());
    }

    void TearDown() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
    }

    static TabletIndex make_index(int64_t id, std::string_view parser,
                                  std::string_view ignore_above = "") {
        TabletIndexPB pb;
        pb.set_index_id(id);
        pb.set_index_name("values_" + std::to_string(id));
        pb.set_index_type(IndexType::INVERTED);
        pb.add_col_unique_id(3);
        (*pb.mutable_properties())[std::string(VARIANT_INDEX_SCOPE_KEY)] =
                std::string(VARIANT_INDEX_SCOPE_VALUES);
        (*pb.mutable_properties())[std::string(VARIANT_ROOT_FORMAT_VERSION_KEY)] =
                std::string(VARIANT_ROOT_FORMAT_VERSION_CURRENT);
        (*pb.mutable_properties())["parser"] = std::string(parser);
        (*pb.mutable_properties())["lower_case"] = "true";
        (*pb.mutable_properties())["support_phrase"] = "false";
        if (!ignore_above.empty()) {
            (*pb.mutable_properties())["ignore_above"] = std::string(ignore_above);
        }
        TabletIndex index;
        index.init_from_pb(pb);
        return index;
    }

    static std::vector<uint32_t> docs_of(const snii::reader::LogicalIndexReader& reader,
                                         const std::string& term) {
        std::vector<uint32_t> docids;
        EXPECT_TRUE(snii::query::term_query(reader, term, &docids).ok());
        return docids;
    }
};

// NOLINTNEXTLINE(readability-function-cognitive-complexity): one writer/readback fixture pins every leaf rule.
TEST_F(VariantRootIndexWriterTest, IndexesEveryScalarLeafOncePerRow) {
    const TabletIndex token = make_index(72, "english");
    const std::vector<std::string> docs = {
            R"({"a":"x","n":42,"arr":[1,{"k":"deep"}],"b":true,"f":42.0,"d":-0.0,"nul":null,)"
            R"("s":"Apache Doris"})",
            R"({})",
            R"({})", // SQL NULL row: the encoded placeholder is never visited
            R"("root scalar")",
            R"({"n":"42","s":"apache"})",
    };
    const std::vector<uint8_t> outer_nulls = {0, 0, 1, 0, 0};
    auto column = encode_documents(docs);

    const std::string prefix = std::string(TEST_DIR) + "/values";
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                      &file_writer)
                        .ok());
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "values_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer));
    VariantRootIndexWriter token_writer(&index_file_writer, &token, false);
    std::vector<VariantRootIndexWriter*> writers = {&token_writer};
    for (VariantRootIndexWriter* writer : writers) {
        ASSERT_TRUE(writer->init().ok());
    }
    // One traversal feeds both indexes.
    const Status appended =
            append_variant_root_indexes(writers, column->read_view(), 0, docs.size(), outer_nulls);
    ASSERT_TRUE(appended.ok()) << appended;
    ASSERT_TRUE(finish_variant_root_indexes(writers).ok());
    ASSERT_TRUE(index_file_writer.begin_close().ok());
    ASSERT_TRUE(index_file_writer.finish_close().ok());

    IndexFileReader index_file_reader(io::global_local_filesystem(), prefix,
                                      InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(index_file_reader.init().ok());
    auto token_reader = index_file_reader.open_snii_index(&token);
    ASSERT_TRUE(token_reader.has_value()) << token_reader.error();
    for (const auto* reader : {token_reader->get()}) {
        // docid == rowid: empty documents and NULL rows keep their slot.
        EXPECT_EQ(reader->stats().doc_count, docs.size());
        EXPECT_EQ(reader->stats().indexed_doc_count, docs.size() - 1);
        EXPECT_EQ(reader->stats().null_count, 1U);
        std::vector<uint32_t> null_docids;
        ASSERT_TRUE(reader->read_null_docids(&null_docids).ok());
        EXPECT_EQ(null_docids, std::vector<uint32_t>({2}));
    }
    using Docs = std::vector<uint32_t>;
    const auto& t = **token_reader;
    // The token index analyzes strings and the canonical text of numbers and booleans.
    EXPECT_EQ(docs_of(t, term_token("apache")), (Docs {0, 4}));
    EXPECT_EQ(docs_of(t, term_token("doris")), Docs {0});
    EXPECT_EQ(docs_of(t, term_token("42")), (Docs {0, 4}));
    EXPECT_EQ(docs_of(t, term_token("true")), Docs {0});
    EXPECT_EQ(docs_of(t, term_token("deep")), Docs {0});
    EXPECT_EQ(docs_of(t, term_token("root")), Docs {3});
    EXPECT_EQ(docs_of(t, term_token("scalar")), Docs {3});
    EXPECT_EQ(docs_of(t, term_token("absent")), Docs {});
}

// NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity): keep storage and reader lifetimes together while checking both physical layouts.
TEST_F(VariantRootIndexWriterTest, IndexesFinalDynamicTypesAfterShredding) {
    for (auto layout :
         {VariantShredderPhysicalLayout::ORDINARY, VariantShredderPhysicalLayout::DOC}) {
        SCOPED_TRACE(static_cast<int>(layout));
        const TabletIndex token = make_index(72, "english");
        const std::vector<std::string> docs = {R"({"n":true})", R"({"n":2})", R"({})", R"({})"};
        const std::vector<uint8_t> outer_nulls = {0, 0, 0, 1};
        auto column = encode_documents(docs);

        const std::string prefix =
                std::string(TEST_DIR) + "/final-" + std::to_string(static_cast<int>(layout));
        io::FileWriterPtr file_writer;
        ASSERT_TRUE(io::global_local_filesystem()
                            ->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                          &file_writer)
                            .ok());
        IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "values_rowset",
                                          /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                          std::move(file_writer));
        VariantRootIndexWriter token_writer(&index_file_writer, &token, false);
        std::vector<VariantRootIndexWriter*> writers = {&token_writer};
        for (VariantRootIndexWriter* writer : writers) {
            ASSERT_TRUE(writer->init().ok());
        }
        VariantShredderOptions options;
        options.physical_layout = layout;
        options.root_index_writers = writers;
        VariantShredder shredder(std::move(options));
        ASSERT_TRUE(shredder.append(column->read_view(), 0, docs.size(), outer_nulls).ok());
        VariantShreddedColumns finalized;
        const auto finish_status = shredder.finish(&finalized);
        ASSERT_TRUE(finish_status.ok()) << finish_status;
        ASSERT_EQ(finalized.materialized.size(), 1U);
        EXPECT_NE(remove_nullable(finalized.materialized[0].type)->get_primitive_type(),
                  TYPE_BOOLEAN);
        ASSERT_TRUE(finish_variant_root_indexes(writers).ok());
        ASSERT_TRUE(index_file_writer.begin_close().ok());
        ASSERT_TRUE(index_file_writer.finish_close().ok());

        IndexFileReader index_file_reader(io::global_local_filesystem(), prefix,
                                          InvertedIndexStorageFormatPB::SNII);
        ASSERT_TRUE(index_file_reader.init().ok());
        auto token_reader = index_file_reader.open_snii_index(&token);
        ASSERT_TRUE(token_reader.has_value()) << token_reader.error();
        for (const auto* reader : {token_reader->get()}) {
            // docid == rowid: empty documents and NULL rows keep their slot.
            EXPECT_EQ(reader->stats().doc_count, docs.size());
            EXPECT_EQ(reader->stats().indexed_doc_count, docs.size() - 1);
            EXPECT_EQ(reader->stats().null_count, 1U);
            std::vector<uint32_t> null_docids;
            ASSERT_TRUE(reader->read_null_docids(&null_docids).ok());
            EXPECT_EQ(null_docids, std::vector<uint32_t>({3}));
        }
        using Docs = std::vector<uint32_t>;
        const auto& t = **token_reader;
        EXPECT_EQ(docs_of(t, term_token("true")), Docs {});
        EXPECT_EQ(docs_of(t, term_token("1")), Docs {0});
        EXPECT_EQ(docs_of(t, term_token("2")), Docs {1});
    }
}

TEST_F(VariantRootIndexWriterTest, RejectsExactValueIndex) {
    const auto index = make_index(71, "none");
    IndexFileWriter file(io::global_local_filesystem(), std::string(TEST_DIR) + "/none", "rowset",
                         0, InvertedIndexStorageFormatPB::SNII, nullptr);
    VariantRootIndexWriter writer(&file, &index, false);
    EXPECT_FALSE(writer.init().ok());
}
} // namespace
} // namespace doris::segment_v2::variant_root_index
