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
using variant_term_codec::term_bool;
using variant_term_codec::term_double;
using variant_term_codec::term_int64;
using variant_term_codec::term_other;
using variant_term_codec::term_string;
using variant_term_codec::term_token;
using variant_term_codec::term_uint64;

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
    EXPECT_EQ(reader_type(make_root_index("none")), InvertedIndexReaderType::STRING_TYPE);
    EXPECT_EQ(reader_type(make_root_index("english")), InvertedIndexReaderType::FULLTEXT);
}

TEST(VariantRootIndexTest, QueryBindingIsPresenceOfThePathNotItsEmptiness) {
    const TabletIndex root = make_root_index("none");
    const QueryBinding whole = query_binding(root.properties());
    EXPECT_FALSE(whole.path_bound);
    EXPECT_FALSE(yields_candidates(root.properties()));

    // The empty key `v['']` is a path like any other: candidates, not the whole document.
    const auto empty_key = bind_to_path(root, "", PrimitiveType::TYPE_VARIANT);
    const QueryBinding empty = query_binding(empty_key->properties());
    EXPECT_TRUE(empty.path_bound);
    EXPECT_TRUE(empty.path.empty());
    EXPECT_TRUE(empty.binary_path());
    EXPECT_TRUE(yields_candidates(empty_key->properties()));

    const auto typed = bind_to_path(root, "a.b", PrimitiveType::TYPE_BIGINT);
    const QueryBinding typed_binding = query_binding(typed->properties());
    EXPECT_TRUE(typed_binding.path_bound);
    EXPECT_EQ(typed_binding.path, "a.b");
    EXPECT_EQ(typed_binding.family, "integral");
    EXPECT_FALSE(typed_binding.binary_path());
    // The bound copy still declares the same index.
    EXPECT_TRUE(is_root_index(*typed));
    EXPECT_EQ(typed->index_id(), root.index_id());
    // A non-root index never yields candidates whatever keys it carries.
    EXPECT_FALSE(yields_candidates(
            {{"parser", "none"}, {std::string(VARIANT_ROOT_QUERY_PATH_KEY), "a"}}));
}

TEST(VariantRootIndexTest, ValueFamiliesGroupTheTypesACastCannotMix) {
    EXPECT_EQ(value_family(PrimitiveType::TYPE_STRING), "string");
    EXPECT_EQ(value_family(PrimitiveType::TYPE_VARCHAR), "string");
    EXPECT_EQ(value_family(PrimitiveType::TYPE_BOOLEAN), "boolean");
    EXPECT_EQ(value_family(PrimitiveType::TYPE_TINYINT), "integral");
    EXPECT_EQ(value_family(PrimitiveType::TYPE_BIGINT), "integral");
    EXPECT_EQ(value_family(PrimitiveType::TYPE_FLOAT), "float");
    EXPECT_EQ(value_family(PrimitiveType::TYPE_DOUBLE), "double");
    EXPECT_TRUE(value_family(PrimitiveType::TYPE_VARIANT).empty());
    EXPECT_TRUE(value_family(PrimitiveType::TYPE_JSONB).empty());
    EXPECT_TRUE(value_family(PrimitiveType::TYPE_ARRAY).empty());
    EXPECT_TRUE(value_family(PrimitiveType::TYPE_DECIMAL64).empty());
}

TEST(VariantRootIndexTest, TypedLiteralsFoldLikeLeaves) {
    const auto typed = [](const Field& field) {
        Terms terms;
        typed_literal_terms(field, &terms);
        return terms;
    };
    EXPECT_EQ(typed(Field::create_field<TYPE_BIGINT>(42)), Terms {term_int64(42)});
    EXPECT_EQ(typed(Field::create_field<TYPE_DOUBLE>(3.0)), Terms {term_int64(3)});
    EXPECT_EQ(typed(Field::create_field<TYPE_DOUBLE>(-0.0)), Terms {term_int64(0)});
    EXPECT_EQ(typed(Field::create_field<TYPE_DOUBLE>(2.5)), Terms {term_double(2.5)});
    EXPECT_EQ(typed(Field::create_field<TYPE_DOUBLE>(9223372036854775808.0)),
              Terms {term_uint64(uint64_t {1} << 63)});
    EXPECT_EQ(typed(Field::create_field<TYPE_UINT64>(std::numeric_limits<uint64_t>::max())),
              Terms {term_uint64(std::numeric_limits<uint64_t>::max())});
    // A FLOAT literal names the double it denotes, the same term a FLOAT leaf gets.
    EXPECT_EQ(typed(Field::create_field<TYPE_FLOAT>(0.1F)),
              Terms {term_double(static_cast<double>(0.1F))});
    EXPECT_TRUE(typed(Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::quiet_NaN()))
                        .empty());
    EXPECT_TRUE(typed(Field::create_field<TYPE_DOUBLE>(std::numeric_limits<double>::infinity()))
                        .empty());
    EXPECT_EQ(typed(Field::create_field<TYPE_BOOLEAN>(true)), Terms {term_bool(true)});
    EXPECT_EQ(typed(Field::create_field<TYPE_STRING>(std::string("3"))), Terms {term_string("3")});
}

TEST(VariantRootIndexTest, ExactTextTermsNameEveryLeafWithThatCanonicalText) {
    const auto expand = [](std::string_view text) {
        Terms terms;
        exact_text_terms(text, &terms);
        return terms;
    };
    EXPECT_EQ(expand("42"), (Terms {term_string("42"), term_int64(42)}));
    EXPECT_EQ(expand("-5"), (Terms {term_string("-5"), term_int64(-5)}));
    EXPECT_EQ(expand("42.7"), (Terms {term_string("42.7"), term_double(42.7)}));
    EXPECT_EQ(expand("1e-07"), (Terms {term_string("1e-07"), term_double(1e-07)}));
    EXPECT_EQ(expand("0.30000000000000004"),
              (Terms {term_string("0.30000000000000004"), term_double(0.1 + 0.2)}));
    EXPECT_EQ(expand("9223372036854775808"),
              (Terms {term_string("9223372036854775808"), term_uint64(uint64_t {1} << 63)}));
    EXPECT_EQ(expand("true"), (Terms {term_string("true"), term_bool(true)}));
    EXPECT_EQ(expand("false"), (Terms {term_string("false"), term_bool(false)}));
    EXPECT_EQ(expand("0.3"), (Terms {term_string("0.3"), term_double(0.3)}));
    EXPECT_EQ(expand("1"), (Terms {term_string("1"), term_int64(1)}));
    // Spellings no leaf prints are only strings: the canonical text of a number is unique.
    for (const std::string_view text : {"042", "+42", "42.0", " 42", "4.2e1", "abc", "", "0x2a",
                                        "inf", "Infinity", "nan", "1e400", "-0"}) {
        EXPECT_EQ(expand(text), Terms {term_string(text)}) << text;
    }
}

TEST(VariantRootIndexTest, CastTextCandidateTermsCoverEveryLeafThatCastsToTheText) {
    const auto expand = [](std::string_view text) {
        Terms terms;
        cast_text_candidate_terms(text, &terms);
        return terms;
    };
    // Integers and their CAST spellings.
    EXPECT_EQ(expand("42"), (Terms {term_string("42"), term_int64(42)}));
    EXPECT_EQ(expand("9223372036854775808"),
              (Terms {term_string("9223372036854775808"), term_uint64(uint64_t {1} << 63)}));
    // CAST(boolean AS STRING) prints 1 / 0, never true / false.
    EXPECT_EQ(expand("1"), (Terms {term_string("1"), term_bool(true), term_int64(1)}));
    EXPECT_EQ(expand("0"), (Terms {term_string("0"), term_bool(false), term_int64(0)}));
    EXPECT_EQ(expand("true"), Terms {term_string("true")});
    // CAST(-0.0 AS STRING) prints -0; the leaf folded into the integer 0.
    EXPECT_EQ(expand("-0"), (Terms {term_string("-0"), term_int64(0)}));
    // A double prints its shortest round-trip text, which names one double; the float that
    // text reads back to is probed as well (a FLOAT leaf 42.7f casts to the same text).
    EXPECT_EQ(expand("42.7"), (Terms {term_string("42.7"), term_double(42.7),
                                      term_double(static_cast<double>(42.7F))}));
    // A text that is exact in float names one term only.
    EXPECT_EQ(expand("2.5"), (Terms {term_string("2.5"), term_double(2.5)}));
    // A FLOAT leaf casts through float formatting ("0.1") but is indexed as the double it
    // denotes: both the double 0.1 and the widened float are probed.
    EXPECT_EQ(expand("0.1"), (Terms {term_string("0.1"), term_double(0.1),
                                     term_double(static_cast<double>(0.1F))}));
    // A text a double leaf never prints still names the double it reads as: the residual
    // drops the row, the superset holds.
    EXPECT_EQ(expand("42.0"), (Terms {term_string("42.0"), term_int64(42)}));
    EXPECT_EQ(expand("1e3"), (Terms {term_string("1e3"), term_int64(1000)}));
    // Beyond float range only the double is probed; NaN / infinity spellings are strings.
    EXPECT_EQ(expand("1e300"), (Terms {term_string("1e300"), term_double(1e300)}));
    for (const std::string_view text : {"abc", "", "Infinity", "NaN", "inf", "1e400"}) {
        EXPECT_EQ(expand(text), Terms {term_string(text)}) << text;
    }
    // Whatever strtod reads as a number is probed (a superset): the residual settles it.
    EXPECT_EQ(expand("0x2a"), (Terms {term_string("0x2a"), term_int64(42)}));
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
    // Every leaf is found again through its own canonical text (whole-document exact MATCH)
    // and through what CAST(leaf AS STRING) prints (path candidates).
    for (const LeafText& leaf : leaves) {
        Terms exact;
        exact_text_terms(leaf.text, &exact);
        EXPECT_EQ(exact.size() > 1, leaf.kind != VariantLeafKind::STRING) << leaf.text;
        Terms cast;
        cast_text_candidate_terms(leaf.kind == VariantLeafKind::BOOL
                                          ? std::string(leaf.text == "true" ? "1" : "0")
                                          : leaf.text,
                                  &cast);
        EXPECT_EQ(cast.size() > 1, leaf.kind != VariantLeafKind::STRING) << leaf.text;
    }
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
    VariantRootIndexWriter exact_writer(&index_file_writer, &exact, false);
    VariantRootIndexWriter token_writer(&index_file_writer, &token, false);
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
    EXPECT_EQ(docs_of(e, term_string("x")), Docs {0});
    EXPECT_EQ(docs_of(e, term_int64(42)), Docs {0});
    EXPECT_EQ(docs_of(e, term_string("42")), Docs {4});
    EXPECT_EQ(docs_of(e, term_double(42.0)), Docs {});
    EXPECT_EQ(docs_of(e, term_int64(0)), Docs {0});
    EXPECT_EQ(docs_of(e, term_bool(true)), Docs {0});
    // Arrays and nested objects are recursed; JSON null has no term.
    EXPECT_EQ(docs_of(e, term_int64(1)), Docs {0});
    EXPECT_EQ(docs_of(e, term_string("deep")), Docs {0});
    EXPECT_EQ(docs_of(e, term_string("null")), Docs {});
    // A scalar root document is one leaf at the root.
    EXPECT_EQ(docs_of(e, term_string("root scalar")), Docs {3});
    EXPECT_EQ(docs_of(e, term_string("Apache Doris")), Docs {0});
    // The token index analyzes strings and the canonical text of numbers and booleans.
    EXPECT_EQ(docs_of(t, term_token("apache")), (Docs {0, 4}));
    EXPECT_EQ(docs_of(t, term_token("doris")), Docs {0});
    EXPECT_EQ(docs_of(t, term_token("42")), (Docs {0, 4}));
    EXPECT_EQ(docs_of(t, term_token("true")), Docs {0});
    EXPECT_EQ(docs_of(t, term_token("deep")), Docs {0});
    EXPECT_EQ(docs_of(t, term_token("root")), Docs {3});
    EXPECT_EQ(docs_of(t, term_token("scalar")), Docs {3});
    EXPECT_EQ(docs_of(t, term_string("x")), Docs {});
}

TEST_F(VariantRootIndexWriterTest, UnspellableScalarsLeaveOneMarkerTermInTheExactIndex) {
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
    VariantRootIndexWriter exact_writer(&index_file_writer, &exact, false);
    VariantRootIndexWriter token_writer(&index_file_writer, &token, false);
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
    EXPECT_EQ(docs_of(e, unspellable_marker_term()), (Docs {0, 1}));
    EXPECT_EQ(docs_of(e, term_string("x")), Docs {0});
    EXPECT_EQ(docs_of(e, term_int64(1)), Docs {2});
    EXPECT_EQ(docs_of(e, term_string("scalar")), Docs {3});
    // The token index has no text to analyze for such a leaf.
    EXPECT_EQ(docs_of(t, unspellable_marker_term()), Docs {});
    EXPECT_EQ(docs_of(t, term_token("x")), Docs {0});
}

TEST_F(VariantRootIndexWriterTest, IgnoreAboveDropsLongStringsFromTheExactIndexOnly) {
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
    VariantRootIndexWriter exact_writer(&index_file_writer, &exact, false);
    VariantRootIndexWriter token_writer(&index_file_writer, &token, false);
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
    EXPECT_EQ(docs_of(**exact_reader, term_string("abcd")), Docs {});
    EXPECT_EQ(docs_of(**exact_reader, term_string("ab")), Docs {0});
    // ignore_above bounds string bytes, never numbers.
    EXPECT_EQ(docs_of(**exact_reader, term_int64(123456)), Docs {0});
    EXPECT_EQ(docs_of(**token_reader, term_token("abcd")), Docs {0});
    EXPECT_EQ(docs_of(**token_reader, term_token("123456")), Docs {0});
}

} // namespace
} // namespace doris::segment_v2::variant_root_index
