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
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2::variant_root_index {
namespace {

using Terms = std::vector<std::string>;

std::map<std::string, std::string> values_properties(
        std::string_view version = VARIANT_ROOT_FORMAT_VERSION_CURRENT) {
    return {{std::string(VARIANT_INDEX_SCOPE_KEY), std::string(VARIANT_INDEX_SCOPE_VALUES)},
            {std::string(VARIANT_ROOT_FORMAT_VERSION_KEY), std::string(version)},
            {"parser", "none"}};
}

TEST(VariantValuesIndexTest, PropertiesRecognizeTheValuesScopeAtTheCurrentVersionOnly) {
    EXPECT_TRUE(is_root_mode_properties(values_properties()));
    EXPECT_TRUE(is_root_mode_properties(
            {{"variant_index_mode", "all_values"}, {"variant_root_format_version", "3"}}));
    EXPECT_TRUE(is_root_mode_properties(
            {{"variant_index_scope", " values "}, {"variant_root_format_version", "3"}}));
    EXPECT_FALSE(is_root_mode_properties(values_properties("1")));
    EXPECT_FALSE(is_root_mode_properties(values_properties("2")));
    EXPECT_FALSE(is_root_mode_properties({{"variant_index_scope", "values"}}));
    EXPECT_FALSE(is_root_mode_properties(
            {{"variant_index_mode", "root"}, {"variant_root_format_version", "3"}}));
    EXPECT_FALSE(is_root_mode_properties(
            {{"variant_index_scope", "paths"}, {"variant_root_format_version", "3"}}));
    EXPECT_FALSE(is_root_mode_properties(
            {{"variant_index_scope", "paths,values"}, {"variant_root_format_version", "3"}}));
    EXPECT_FALSE(is_root_mode_properties({{"parser", "none"}}));
}

TEST(VariantValuesIndexTest, TermsAreThePathlessTypedRootPrefixes) {
    EXPECT_EQ(encode_string_term("abc"), variant_term_codec::root_prefix_string("abc"));
    EXPECT_EQ(encode_token_term("abc"), variant_term_codec::root_prefix_token("abc"));
    EXPECT_EQ(encode_int64_term(42), variant_term_codec::root_prefix_int64(42));
    EXPECT_EQ(encode_uint64_term(uint64_t {1} << 63),
              variant_term_codec::root_prefix_uint64(uint64_t {1} << 63));
    EXPECT_EQ(encode_double_term(1.5), variant_term_codec::root_prefix_double(1.5));
    EXPECT_EQ(encode_bool_term(true), variant_term_codec::root_prefix_bool(true));
    // Values are typed: "3" and 3 are different terms, exact and token terms never collide.
    EXPECT_NE(encode_string_term("3"), encode_int64_term(3));
    EXPECT_NE(encode_string_term("abc"), encode_token_term("abc"));
}

TEST(VariantValuesIndexTest, TypedLiteralsFoldLikeLeaves) {
    const auto typed = [](const Field& field) {
        Terms terms;
        EXPECT_TRUE(encode_query_value_terms(field, &terms).ok());
        return terms;
    };
    EXPECT_EQ(typed(Field::create_field<TYPE_BIGINT>(42)), Terms {encode_int64_term(42)});
    EXPECT_EQ(typed(Field::create_field<TYPE_DOUBLE>(3.0)), Terms {encode_int64_term(3)});
    EXPECT_EQ(typed(Field::create_field<TYPE_DOUBLE>(-0.0)), Terms {encode_int64_term(0)});
    EXPECT_EQ(typed(Field::create_field<TYPE_DOUBLE>(2.5)), Terms {encode_double_term(2.5)});
    EXPECT_EQ(typed(Field::create_field<TYPE_DOUBLE>(9223372036854775808.0)),
              Terms {encode_uint64_term(uint64_t {1} << 63)});
    EXPECT_EQ(typed(Field::create_field<TYPE_UINT64>(std::numeric_limits<uint64_t>::max())),
              Terms {encode_uint64_term(std::numeric_limits<uint64_t>::max())});
    EXPECT_TRUE(typed(Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN()))
                        .empty());
    EXPECT_EQ(typed(Field::create_field<TYPE_BOOLEAN>(true)), Terms {encode_bool_term(true)});
    EXPECT_EQ(typed(Field::create_field<TYPE_STRING>(std::string("3"))),
              Terms {encode_string_term("3")});
}

TEST(VariantValuesIndexTest, StringLiteralsProbeEveryTypedSpellingTheyName) {
    const auto expand = [](std::string_view text, bool sql_cast_text) {
        Terms terms;
        append_string_literal_terms(text, sql_cast_text, &terms);
        return terms;
    };
    EXPECT_EQ(expand("42", false), (Terms {encode_string_term("42"), encode_int64_term(42)}));
    EXPECT_EQ(expand("-5", false), (Terms {encode_string_term("-5"), encode_int64_term(-5)}));
    EXPECT_EQ(expand("42.7", false),
              (Terms {encode_string_term("42.7"), encode_double_term(42.7)}));
    EXPECT_EQ(expand("9223372036854775808", false),
              (Terms {encode_string_term("9223372036854775808"),
                      encode_uint64_term(uint64_t {1} << 63)}));
    EXPECT_EQ(expand("true", false), (Terms {encode_string_term("true"), encode_bool_term(true)}));
    EXPECT_EQ(expand("false", false),
              (Terms {encode_string_term("false"), encode_bool_term(false)}));
    // Spellings no leaf prints are only strings.
    for (const std::string_view text :
         {"042", "+42", "42.0", " 42", "4.2e1", "abc", "", "0x2a", "inf", "nan", "1e400", "-0"}) {
        EXPECT_EQ(expand(text, false), Terms {encode_string_term(text)}) << text;
    }
    // CAST(leaf AS STRING) spells booleans as 1 / 0 and negative zero as -0.
    EXPECT_EQ(expand("1", false), (Terms {encode_string_term("1"), encode_int64_term(1)}));
    EXPECT_EQ(expand("1", true),
              (Terms {encode_string_term("1"), encode_int64_term(1), encode_bool_term(true)}));
    EXPECT_EQ(expand("0", true),
              (Terms {encode_string_term("0"), encode_int64_term(0), encode_bool_term(false)}));
    EXPECT_EQ(expand("-0", true), (Terms {encode_string_term("-0"), encode_int64_term(0)}));
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

TEST(VariantValuesIndexTest, CanonicalLeafTextIsWhatTheWriterAndTheScalarFallbackTokenize) {
    const std::vector<LeafText> leaves = leaf_texts(
            R"({"s":"x y","i":42,"neg":-5,"f":42.0,"d":42.7,"z":-0.0,"big":9223372036854775808.0,)"
            R"("t":true,"fl":false,"arr":[1,"a",{"k":"deep"}],"n":null,"e":{},"o":{"p":[]}})");
    std::vector<std::string> actual;
    for (const LeafText& leaf : leaves) {
        actual.push_back(leaf.text);
    }
    std::ranges::sort(actual);
    std::vector<std::string> expected = {
            "x y",  "42",    "-5", "42", "42.7", "0", "9223372036854775808",
            "true", "false", "1",  "a",  "deep"};
    std::ranges::sort(expected);
    EXPECT_EQ(actual, expected);
    // Every number and boolean is reachable through the literal spelled like its text, so a
    // string predicate over an untyped path is a superset of CAST(leaf AS STRING) = text.
    for (const LeafText& leaf : leaves) {
        Terms terms;
        append_string_literal_terms(leaf.text, /*sql_cast_text=*/true, &terms);
        EXPECT_EQ(terms.size() > 1, leaf.kind != VariantLeafKind::STRING) << leaf.text;
    }
}

class VariantValuesIndexWriterTest : public testing::Test {
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
TEST_F(VariantValuesIndexWriterTest, IndexesEveryScalarLeafOncePerRow) {
    const TabletIndex exact = make_index(71, "none");
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
    VariantRootIndexWriter exact_writer(&index_file_writer, &exact, false, false);
    VariantRootIndexWriter token_writer(&index_file_writer, &token, false, false);
    std::vector<VariantRootIndexWriter*> writers = {&exact_writer, &token_writer};
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
    auto exact_reader = index_file_reader.open_snii_index(&exact);
    auto token_reader = index_file_reader.open_snii_index(&token);
    ASSERT_TRUE(exact_reader.has_value()) << exact_reader.error();
    ASSERT_TRUE(token_reader.has_value()) << token_reader.error();
    for (const auto* reader : {exact_reader->get(), token_reader->get()}) {
        // docid == rowid: empty documents and NULL rows keep their slot.
        EXPECT_EQ(reader->stats().doc_count, docs.size());
        EXPECT_EQ(reader->stats().indexed_doc_count, docs.size() - 1);
        EXPECT_EQ(reader->stats().null_count, 1U);
        std::vector<uint32_t> null_docids;
        ASSERT_TRUE(reader->read_null_docids(&null_docids).ok());
        EXPECT_EQ(null_docids, std::vector<uint32_t>({2}));
    }
    using Docs = std::vector<uint32_t>;
    const auto& e = **exact_reader;
    const auto& t = **token_reader;
    // Exact terms are typed and path-less; 42.0 folds into 42, -0.0 into 0, "42" stays a string.
    EXPECT_EQ(docs_of(e, encode_string_term("x")), Docs {0});
    EXPECT_EQ(docs_of(e, encode_int64_term(42)), Docs {0});
    EXPECT_EQ(docs_of(e, encode_string_term("42")), Docs {4});
    EXPECT_EQ(docs_of(e, encode_double_term(42.0)), Docs {});
    EXPECT_EQ(docs_of(e, encode_int64_term(0)), Docs {0});
    EXPECT_EQ(docs_of(e, encode_bool_term(true)), Docs {0});
    // Arrays and nested objects are recursed; JSON null has no term.
    EXPECT_EQ(docs_of(e, encode_int64_term(1)), Docs {0});
    EXPECT_EQ(docs_of(e, encode_string_term("deep")), Docs {0});
    EXPECT_EQ(docs_of(e, encode_string_term("null")), Docs {});
    // A scalar root document is one leaf at the root.
    EXPECT_EQ(docs_of(e, encode_string_term("root scalar")), Docs {3});
    EXPECT_EQ(docs_of(e, encode_string_term("Apache Doris")), Docs {0});
    // The token index analyzes strings and the canonical text of numbers and booleans.
    EXPECT_EQ(docs_of(t, encode_token_term("apache")), (Docs {0, 4}));
    EXPECT_EQ(docs_of(t, encode_token_term("doris")), Docs {0});
    EXPECT_EQ(docs_of(t, encode_token_term("42")), (Docs {0, 4}));
    EXPECT_EQ(docs_of(t, encode_token_term("true")), Docs {0});
    EXPECT_EQ(docs_of(t, encode_token_term("deep")), Docs {0});
    EXPECT_EQ(docs_of(t, encode_token_term("root")), Docs {3});
    EXPECT_EQ(docs_of(t, encode_token_term("scalar")), Docs {3});
    EXPECT_EQ(docs_of(t, encode_string_term("x")), Docs {});
}

TEST_F(VariantValuesIndexWriterTest, UnspellableScalarsLeaveOneMarkerTermInTheExactIndex) {
    const TabletIndex exact = make_index(75, "none");
    const TabletIndex token = make_index(76, "english");
    // Above INT64_MAX the parser keeps a wide integer that the codec cannot spell.
    const std::vector<std::string> docs = {
            R"({"a":18446744073709551615,"b":"x"})",
            R"({"arr":[18446744073709551615,{"k":18446744073709551615}],"nul":null})",
            R"({"nul":null,"e":{},"arr":[1]})",
            R"("scalar")",
    };
    const std::vector<uint8_t> outer_nulls(docs.size(), 0);
    auto column = encode_documents(docs);

    const std::string prefix = std::string(TEST_DIR) + "/marker";
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                      &file_writer)
                        .ok());
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "marker_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer));
    VariantRootIndexWriter exact_writer(&index_file_writer, &exact, false, false);
    VariantRootIndexWriter token_writer(&index_file_writer, &token, false, false);
    std::vector<VariantRootIndexWriter*> writers = {&exact_writer, &token_writer};
    for (VariantRootIndexWriter* writer : writers) {
        ASSERT_TRUE(writer->init().ok());
    }
    const Status appended =
            append_variant_root_indexes(writers, column->read_view(), 0, docs.size(), outer_nulls);
    ASSERT_TRUE(appended.ok()) << appended;
    ASSERT_TRUE(finish_variant_root_indexes(writers).ok());
    ASSERT_TRUE(index_file_writer.begin_close().ok());
    ASSERT_TRUE(index_file_writer.finish_close().ok());

    IndexFileReader index_file_reader(io::global_local_filesystem(), prefix,
                                      InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(index_file_reader.init().ok());
    auto exact_reader = index_file_reader.open_snii_index(&exact);
    auto token_reader = index_file_reader.open_snii_index(&token);
    ASSERT_TRUE(exact_reader.has_value()) << exact_reader.error();
    ASSERT_TRUE(token_reader.has_value()) << token_reader.error();
    using Docs = std::vector<uint32_t>;
    const auto& e = **exact_reader;
    const auto& t = **token_reader;
    // One marker per document however many unspellable leaves it holds; JSON null, containers
    // and spellable values leave none.
    EXPECT_EQ(docs_of(e, encode_other_term()), (Docs {0, 1}));
    EXPECT_EQ(docs_of(e, encode_string_term("x")), Docs {0});
    EXPECT_EQ(docs_of(e, encode_int64_term(1)), Docs {2});
    EXPECT_EQ(docs_of(e, encode_string_term("scalar")), Docs {3});
    // The token index has no text to analyze for such a leaf.
    EXPECT_EQ(docs_of(t, encode_other_term()), Docs {});
    EXPECT_EQ(docs_of(t, encode_token_term("x")), Docs {0});
}

TEST_F(VariantValuesIndexWriterTest, IgnoreAboveDropsLongStringsFromTheExactIndexOnly) {
    const TabletIndex exact = make_index(73, "none", "3");
    const TabletIndex token = make_index(74, "english", "3");
    const std::vector<std::string> docs = {R"({"s":"abcd","t":"ab","n":123456})"};
    auto column = encode_documents(docs);

    const std::string prefix = std::string(TEST_DIR) + "/ignore_above";
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                      &file_writer)
                        .ok());
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "ignore_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer));
    VariantRootIndexWriter exact_writer(&index_file_writer, &exact, false, false);
    VariantRootIndexWriter token_writer(&index_file_writer, &token, false, false);
    std::vector<VariantRootIndexWriter*> writers = {&exact_writer, &token_writer};
    for (VariantRootIndexWriter* writer : writers) {
        ASSERT_TRUE(writer->init().ok());
    }
    ASSERT_TRUE(append_variant_root_indexes(writers, column->read_view(), 0, docs.size(), {}).ok());
    ASSERT_TRUE(finish_variant_root_indexes(writers).ok());
    ASSERT_TRUE(index_file_writer.begin_close().ok());
    ASSERT_TRUE(index_file_writer.finish_close().ok());

    IndexFileReader index_file_reader(io::global_local_filesystem(), prefix,
                                      InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(index_file_reader.init().ok());
    auto exact_reader = index_file_reader.open_snii_index(&exact);
    auto token_reader = index_file_reader.open_snii_index(&token);
    ASSERT_TRUE(exact_reader.has_value()) << exact_reader.error();
    ASSERT_TRUE(token_reader.has_value()) << token_reader.error();
    using Docs = std::vector<uint32_t>;
    EXPECT_EQ(docs_of(**exact_reader, encode_string_term("abcd")), Docs {});
    EXPECT_EQ(docs_of(**exact_reader, encode_string_term("ab")), Docs {0});
    // ignore_above bounds string bytes, never numbers.
    EXPECT_EQ(docs_of(**exact_reader, encode_int64_term(123456)), Docs {0});
    EXPECT_EQ(docs_of(**token_reader, encode_token_term("abcd")), Docs {0});
    EXPECT_EQ(docs_of(**token_reader, encode_token_term("123456")), Docs {0});
}

} // namespace
} // namespace doris::segment_v2::variant_root_index
