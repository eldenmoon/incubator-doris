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

// Index-vs-scan parity for the VARIANT root / all-values indexes.
//
// Random documents are written through the real writers into a real SNII index; every query is
// then answered twice: through the index (the same term encoders the reader uses) and through an
// independent oracle that walks the Variant value directly and re-implements the canonical value
// rules on its own. The two must agree row for row. The oracle deliberately shares nothing with
// VariantLeafVisitor so a traversal bug cannot hide on both sides.

#include <gtest/gtest.h>

#include <array>
#include <cmath>
#include <cstdint>
#include <limits>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type_serde/data_type_variant_v2_serde.h"
#include "core/field.h"
#include "core/value/variant/variant_value.h"
#include "io/fs/local_file_system.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/variant_root_index.h"
#include "storage/index/inverted/variant_term_codec.h"
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/segment/variant/v2/variant_root_index_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2::variant_root_index {
namespace {

using variant_term_codec::term_bool;
using variant_term_codec::term_double;
using variant_term_codec::term_int64;
using variant_term_codec::term_string;
using variant_term_codec::term_token;
using variant_term_codec::term_uint64;

// ------------------------------------------------------------------------------------------
// Independent oracle
// ------------------------------------------------------------------------------------------

enum class OracleKind { STRING, INT64, UINT64, DOUBLE, BOOL, OTHER };

struct OracleLeaf {
    std::string path;
    OracleKind kind = OracleKind::OTHER;
    std::string text;
    int64_t i64 = 0;
    uint64_t u64 = 0;
    double d = 0.0;
    bool b = false;
};

OracleLeaf oracle_number(double value) {
    OracleLeaf leaf;
    if (!std::isfinite(value)) {
        leaf.kind = OracleKind::OTHER;
        return leaf;
    }
    if (std::floor(value) == value) {
        if (value >= -9223372036854775808.0 && value < 9223372036854775808.0) {
            leaf.kind = OracleKind::INT64;
            leaf.i64 = static_cast<int64_t>(value);
            return leaf;
        }
        if (value >= 9223372036854775808.0 && value < 18446744073709551616.0) {
            leaf.kind = OracleKind::UINT64;
            leaf.u64 = static_cast<uint64_t>(value);
            return leaf;
        }
    }
    leaf.kind = OracleKind::DOUBLE;
    leaf.d = value == 0.0 ? 0.0 : value;
    return leaf;
}

void oracle_walk(const VariantRef& value, std::string& path, std::vector<OracleLeaf>* out) {
    switch (value.basic_type()) {
    case VariantBasicType::OBJECT: {
        const VariantRef::ObjectView object = value.object_view();
        for (uint32_t index = 0; index < object.size(); ++index) {
            uint32_t field = 0;
            const VariantRef child = object.value_at(index, &field);
            const size_t saved = path.size();
            if (!path.empty()) {
                path.push_back('.');
            }
            const StringRef key = value.metadata.key_at(field);
            path.append(key.data, key.size);
            oracle_walk(child, path, out);
            path.resize(saved);
        }
        return;
    }
    case VariantBasicType::ARRAY:
        for (uint32_t index = 0; index < value.num_elements(); ++index) {
            oracle_walk(value.array_at(index), path, out);
        }
        return;
    case VariantBasicType::SHORT_STRING: {
        OracleLeaf leaf;
        leaf.path = path;
        leaf.kind = OracleKind::STRING;
        leaf.text = value.get_string().to_string();
        out->push_back(std::move(leaf));
        return;
    }
    default:
        break;
    }
    if (value.is_null()) {
        return;
    }
    OracleLeaf leaf;
    leaf.path = path;
    switch (value.primitive_id()) {
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        leaf.kind = OracleKind::BOOL;
        leaf.b = value.get_bool();
        break;
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        leaf.kind = OracleKind::INT64;
        leaf.i64 = value.get_int();
        break;
    case VariantPrimitiveId::FLOAT:
        leaf = oracle_number(static_cast<double>(value.get_float()));
        leaf.path = path;
        break;
    case VariantPrimitiveId::DOUBLE:
        leaf = oracle_number(value.get_double());
        leaf.path = path;
        break;
    case VariantPrimitiveId::STRING:
        leaf.kind = OracleKind::STRING;
        leaf.text = value.get_string().to_string();
        break;
    default:
        leaf.kind = OracleKind::OTHER;
        break;
    }
    out->push_back(std::move(leaf));
}

std::vector<std::string> split_words(std::string_view text) {
    std::vector<std::string> words;
    size_t begin = 0;
    while (begin <= text.size()) {
        size_t end = text.find(' ', begin);
        if (end == std::string_view::npos) {
            end = text.size();
        }
        if (end > begin) {
            words.emplace_back(text.substr(begin, end - begin));
        }
        begin = end + 1;
    }
    return words;
}

// ------------------------------------------------------------------------------------------
// Random documents and queries
// ------------------------------------------------------------------------------------------

const std::vector<std::string> kKeys = {"a", "b", "c", "d", "e"};
const std::vector<std::string> kInts = {
        "0", "1", "-1", "42", "2147483648", "-9223372036854775808", "9223372036854775807"};
const std::vector<std::string> kDoubles = {"1.5",
                                           "-0.0",
                                           "0.5",
                                           "1e300",
                                           "42.0",
                                           "-2.25",
                                           "9223372036854775808.0",
                                           "18446744073709551616.0"};
// Above INT64_MAX the parser keeps a wide integer the codec cannot spell: an OTHER leaf.
const std::vector<std::string> kWideInts = {"18446744073709551615", "9223372036854775808"};
const std::vector<std::string> kWords = {"alpha",      "beta", "gamma", "delta",
                                         "alpha beta", "",     "3",     "gamma delta alpha"};
struct Literal {
    enum Type { INT, DOUBLE, BOOL, STRING } type = INT;
    int64_t i = 0;
    double d = 0.0;
    bool b = false;
    std::string s;

    Field field() const {
        switch (type) {
        case INT:
            return Field::create_field<TYPE_BIGINT>(i);
        case DOUBLE:
            return Field::create_field<TYPE_DOUBLE>(d);
        case BOOL:
            return Field::create_field<TYPE_BOOLEAN>(b);
        case STRING:
            return Field::create_field<TYPE_STRING>(s);
        }
        return Field::create_field<TYPE_BIGINT>(0);
    }

    // The literal as a string predicate would spell it (booleans and doubles probe "true").
    std::string cast_text() const {
        switch (type) {
        case INT:
            return std::to_string(i);
        case STRING:
            return s;
        default:
            return "true";
        }
    }

    bool matches(const OracleLeaf& leaf) const {
        switch (type) {
        case INT:
            return leaf.kind == OracleKind::INT64 && leaf.i64 == i;
        case DOUBLE: {
            const OracleLeaf canonical = oracle_number(d);
            switch (canonical.kind) {
            case OracleKind::INT64:
                return leaf.kind == OracleKind::INT64 && leaf.i64 == canonical.i64;
            case OracleKind::UINT64:
                return leaf.kind == OracleKind::UINT64 && leaf.u64 == canonical.u64;
            case OracleKind::DOUBLE:
                return leaf.kind == OracleKind::DOUBLE && leaf.d == canonical.d;
            default:
                return false;
            }
        }
        case BOOL:
            return leaf.kind == OracleKind::BOOL && leaf.b == b;
        case STRING:
            return leaf.kind == OracleKind::STRING && leaf.text == s;
        }
        return false;
    }
};

class DocGenerator {
public:
    explicit DocGenerator(uint64_t seed) : _rng(seed) {}

    std::string document() { return value(0); }

    Literal literal() {
        Literal literal;
        switch (_rng() % 4) {
        case 0:
            literal.type = Literal::INT;
            literal.i = std::stoll(pick(kInts));
            break;
        case 1:
            literal.type = Literal::DOUBLE;
            literal.d = std::stod(pick(kDoubles));
            break;
        case 2:
            literal.type = Literal::BOOL;
            literal.b = _rng() % 2 == 0;
            break;
        default:
            literal.type = Literal::STRING;
            literal.s = pick(kWords);
            break;
        }
        return literal;
    }

    std::string match_query() {
        static const std::vector<std::string> queries = {"alpha",       "beta",       "gamma delta",
                                                         "delta alpha", "alpha beta", "beta gamma"};
        return pick(queries);
    }

    uint64_t next() { return _rng(); }

private:
    const std::string& pick(const std::vector<std::string>& pool) {
        return pool[_rng() % pool.size()];
    }

    std::string scalar() {
        switch (_rng() % 6) {
        case 0:
            return pick(kInts);
        case 1:
            return pick(kDoubles);
        case 2:
            return _rng() % 2 == 0 ? "true" : "false";
        case 3:
            return "null";
        case 4:
            return pick(kWideInts);
        default:
            return "\"" + pick(kWords) + "\"";
        }
    }

    std::string value(int depth) {
        const uint64_t roll = _rng() % 10;
        if (depth < 3 && roll < 3) {
            return object(depth);
        }
        if (depth < 3 && roll < 5) {
            return array(depth);
        }
        return scalar();
    }

    std::string object(int depth) {
        std::vector<std::string> keys = kKeys;
        std::shuffle(keys.begin(), keys.end(), _rng);
        const size_t count = 1 + _rng() % 4;
        std::string out = "{";
        for (size_t i = 0; i < count; ++i) {
            if (i > 0) {
                out += ",";
            }
            out += "\"" + keys[i] + "\":" + value(depth + 1);
        }
        return out + "}";
    }

    std::string array(int depth) {
        const size_t count = _rng() % 4;
        std::string out = "[";
        for (size_t i = 0; i < count; ++i) {
            if (i > 0) {
                out += ",";
            }
            out += value(depth + 1);
        }
        return out + "]";
    }

    std::mt19937_64 _rng;
};

// ------------------------------------------------------------------------------------------
// Fixture: four writers over one document stream
// ------------------------------------------------------------------------------------------

class VariantIndexParityTest : public testing::Test {
protected:
    static constexpr const char* TEST_DIR = "./ut_dir/variant_index_parity_test";

    void SetUp() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(TEST_DIR).ok());
    }

    void TearDown() override {
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(TEST_DIR).ok());
    }

    static TabletIndex make_index(int64_t id, std::string_view mode, std::string_view parser) {
        TabletIndexPB pb;
        pb.set_index_id(id);
        pb.set_index_name("parity_" + std::to_string(id));
        pb.set_index_type(IndexType::INVERTED);
        pb.add_col_unique_id(3);
        (*pb.mutable_properties())[std::string(VARIANT_INDEX_MODE_KEY)] = std::string(mode);
        (*pb.mutable_properties())[std::string(VARIANT_ROOT_FORMAT_VERSION_KEY)] =
                std::string(VARIANT_ROOT_FORMAT_VERSION_CURRENT);
        (*pb.mutable_properties())["parser"] = std::string(parser);
        (*pb.mutable_properties())["support_phrase"] = "false";
        TabletIndex index;
        index.init_from_pb(pb);
        return index;
    }

    static std::vector<uint32_t> term_docs(const snii::reader::LogicalIndexReader& reader,
                                           const std::string& term) {
        std::vector<uint32_t> docids;
        EXPECT_TRUE(snii::query::term_query(reader, term, &docids).ok());
        return docids;
    }

    static std::vector<uint32_t> union_docs(const snii::reader::LogicalIndexReader& reader,
                                            const std::vector<std::string>& terms) {
        std::set<uint32_t> all;
        for (const std::string& term : terms) {
            for (const uint32_t docid : term_docs(reader, term)) {
                all.insert(docid);
            }
        }
        return {all.begin(), all.end()};
    }

    static std::vector<uint32_t> and_docs(const snii::reader::LogicalIndexReader& reader,
                                          const std::vector<std::string>& terms) {
        std::vector<uint32_t> docids;
        EXPECT_TRUE(snii::query::boolean_and(reader, terms, &docids).ok());
        return docids;
    }
};

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- one fixture drives every predicate class.
TEST_F(VariantIndexParityTest, IndexAndScanAgreeOnRandomDocuments) {
    const TabletIndex all_exact = make_index(92, VARIANT_INDEX_MODE_ALL_VALUES, "none");
    const TabletIndex all_token = make_index(93, VARIANT_INDEX_MODE_ALL_VALUES, "english");

    // Documents: hand-picked edge cases first, then random ones.
    std::vector<std::string> docs = {
            R"({"a":"3","b":3,"c":3.0,"d":-0.0,"e":[{"a":"alpha beta"},{"a":"gamma"}]})",
            R"({"a":9223372036854775808.0,"b":18446744073709551616.0,"c":1e300,"d":9223372036854775807})",
            R"("root scalar")",
            R"([1,"alpha",{"a":"beta"}])",
            R"({})",
            R"(null)",
            R"({"a":{"b":{"c":"delta"}},"b":{"a":"alpha"},"c.d":"beta"})",
            R"({"a":[[["gamma"]],[]],"b":[null,true,false]})",
            R"({"a":18446744073709551615,"b":null})",
    };
    DocGenerator generator(0x9a17e);
    for (int i = 0; i < 400; ++i) {
        docs.push_back(generator.document());
    }
    std::vector<uint8_t> outer_nulls(docs.size(), 0);
    for (size_t i = 0; i < docs.size(); ++i) {
        if (i >= 9 && generator.next() % 10 == 0) {
            outer_nulls[i] = 1;
        }
    }

    auto values = ColumnVariantV2::create();
    DataTypeVariantV2SerDe serde;
    DataTypeSerDe::FormatOptions format_options;
    for (const std::string& json : docs) {
        Slice slice(json.data(), json.size());
        ASSERT_TRUE(serde.deserialize_one_cell_from_json(*values, slice, format_options).ok())
                << json;
    }
    ASSERT_EQ(values->size(), docs.size());

    // Oracle view of every row.
    std::vector<std::vector<OracleLeaf>> oracle(docs.size());
    const ColumnVariantV2::ReadView view = values->read_view();
    for (size_t row = 0; row < docs.size(); ++row) {
        if (outer_nulls[row] != 0) {
            continue;
        }
        std::string path;
        oracle_walk(view.value_at(row), path, &oracle[row]);
    }

    // Write both indexes in one traversal.
    const std::string prefix = std::string(TEST_DIR) + "/parity";
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                      &file_writer)
                        .ok());
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "parity_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer));
    VariantRootIndexWriter w_all_exact(&index_file_writer, &all_exact, false);
    VariantRootIndexWriter w_all_token(&index_file_writer, &all_token, false);
    std::array<VariantRootIndexWriter*, 2> writers = {&w_all_exact, &w_all_token};
    for (VariantRootIndexWriter* writer : writers) {
        ASSERT_TRUE(writer->init().ok());
    }
    const Status append_status =
            append_variant_root_indexes(writers, view, 0, docs.size(), outer_nulls);
    ASSERT_TRUE(append_status.ok()) << append_status;
    ASSERT_TRUE(finish_variant_root_indexes(writers).ok());
    ASSERT_TRUE(index_file_writer.begin_close().ok());
    ASSERT_TRUE(index_file_writer.finish_close().ok());

    IndexFileReader index_file_reader(io::global_local_filesystem(), prefix,
                                      InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(index_file_reader.init().ok());
    auto r_all_exact = index_file_reader.open_snii_index(&all_exact);
    auto r_all_token = index_file_reader.open_snii_index(&all_token);
    ASSERT_TRUE(r_all_exact.has_value()) << r_all_exact.error();
    ASSERT_TRUE(r_all_token.has_value()) << r_all_token.error();
    for (const auto* reader : {r_all_exact->get(), r_all_token->get()}) {
        // docid == rowid, including NULL rows and empty documents.
        EXPECT_EQ(reader->stats().doc_count, docs.size());
    }

    const auto expected_rows = [&](const auto& predicate) {
        std::vector<uint32_t> rows;
        for (size_t row = 0; row < docs.size(); ++row) {
            if (outer_nulls[row] == 0 && predicate(oracle[row])) {
                rows.push_back(static_cast<uint32_t>(row));
            }
        }
        return rows;
    };
    // The words every query draws from are alphabetic, so the canonical texts of number and
    // boolean leaves (which the token index also stores) never collide with them.
    const auto tokens_of = [](const std::vector<OracleLeaf>& leaves) {
        std::set<std::string> tokens;
        for (const OracleLeaf& leaf : leaves) {
            if (leaf.kind != OracleKind::STRING) {
                continue;
            }
            for (std::string& word : split_words(leaf.text)) {
                tokens.insert(std::move(word));
            }
        }
        return tokens;
    };
    // CAST(leaf AS STRING) = text for the leaf kinds the literal pools can spell.
    const auto text_matches = [](const OracleLeaf& leaf, const std::string& text) {
        switch (leaf.kind) {
        case OracleKind::STRING:
            return leaf.text == text;
        case OracleKind::INT64:
            return std::to_string(leaf.i64) == text;
        case OracleKind::UINT64:
            return std::to_string(leaf.u64) == text;
        default:
            return false;
        }
    };

    size_t checks = 0;
    size_t non_empty = 0;
    for (int round = 0; round < 600; ++round) {
        const Literal literal = generator.literal();
        const Field field = literal.field();

        // 1. typed any-path equality: the literal's own term
        {
            std::vector<std::string> terms;
            typed_literal_terms(field, &terms);
            ASSERT_EQ(terms.size(), 1U);
            const auto actual = term_docs(**r_all_exact, terms[0]);
            const auto expected = expected_rows([&](const std::vector<OracleLeaf>& leaves) {
                return std::ranges::any_of(
                        leaves, [&](const OracleLeaf& leaf) { return literal.matches(leaf); });
            });
            EXPECT_EQ(actual, expected) << "round=" << round;
            ++checks;
            non_empty += !expected.empty();
        }
        // 2. whole-document exact MATCH: every leaf whose canonical text is the literal, the
        //    string, the number and the boolean it spells alike
        {
            const std::string text = literal.cast_text();
            std::vector<std::string> terms;
            exact_text_terms(text, &terms);
            const auto actual = union_docs(**r_all_exact, terms);
            const auto expected = expected_rows([&](const std::vector<OracleLeaf>& leaves) {
                return std::ranges::any_of(leaves, [&](const OracleLeaf& leaf) {
                    return text_matches(leaf, text) ||
                           (text == "true" && leaf.kind == OracleKind::BOOL && leaf.b);
                });
            });
            EXPECT_EQ(actual, expected) << "text=" << text << " round=" << round;
            ++checks;
        }
        // 5. the same literal bound to a path read from the binary storage: the CAST(path AS
        //    STRING) spellings of booleans (1 / 0) join the probe and every document holding a
        //    leaf the codec cannot spell is a candidate through the marker term
        {
            const std::string text = literal.cast_text();
            std::vector<std::string> terms;
            cast_text_candidate_terms(text, &terms);
            terms.push_back(unspellable_marker_term());
            const auto actual = union_docs(**r_all_exact, terms);
            const auto expected = expected_rows([&](const std::vector<OracleLeaf>& leaves) {
                return std::ranges::any_of(leaves, [&](const OracleLeaf& leaf) {
                    return text_matches(leaf, text) || leaf.kind == OracleKind::OTHER ||
                           (leaf.kind == OracleKind::BOOL &&
                            ((text == "1" && leaf.b) || (text == "0" && !leaf.b)));
                });
            });
            EXPECT_EQ(actual, expected) << "cast text=" << text << " round=" << round;
            ++checks;
        }
        // 3./4. any-path MATCH_ANY / MATCH_ALL on the token index
        {
            const std::string query = generator.match_query();
            const std::vector<std::string> words = split_words(query);
            std::vector<std::string> terms;
            for (const std::string& word : words) {
                terms.push_back(term_token(word));
            }
            const auto covers = [&](const std::set<std::string>& tokens) {
                return std::ranges::all_of(
                        words, [&](const std::string& word) { return tokens.contains(word); });
            };
            const auto touches = [&](const std::set<std::string>& tokens) {
                return std::ranges::any_of(
                        words, [&](const std::string& word) { return tokens.contains(word); });
            };
            EXPECT_EQ(union_docs(**r_all_token, terms),
                      expected_rows([&](const std::vector<OracleLeaf>& leaves) {
                          return touches(tokens_of(leaves));
                      }))
                    << "MATCH_ANY query=" << query;
            EXPECT_EQ(and_docs(**r_all_token, terms),
                      expected_rows([&](const std::vector<OracleLeaf>& leaves) {
                          return covers(tokens_of(leaves));
                      }))
                    << "MATCH_ALL query=" << query;
            checks += 2;
        }
    }
    EXPECT_GE(checks, 3000U);
    // The generator must actually produce hits, or the parity is vacuous.
    EXPECT_GT(non_empty, 200U);

    // Typed semantics pinned on the edge documents. Random documents draw from the same value
    // pools, so the pins check membership of the edge row rather than an exact posting list.
    const auto hits = [&](const snii::reader::LogicalIndexReader& reader, const std::string& term,
                          uint32_t row) {
        const std::vector<uint32_t> docids = term_docs(reader, term);
        return std::ranges::find(docids, row) != docids.end();
    };
    const auto& exact = **r_all_exact;
    const auto& token = **r_all_token;
    // "3", 3 and 3.0 are two values, not three; -0.0 is 0.
    EXPECT_TRUE(hits(exact, term_string("3"), 0));
    EXPECT_TRUE(hits(exact, term_int64(3), 0));
    EXPECT_FALSE(hits(exact, term_double(3.0), 0));
    EXPECT_TRUE(hits(exact, term_int64(0), 0));
    EXPECT_TRUE(hits(exact, term_string("gamma"), 0));
    // The token index spells numbers and booleans as text.
    EXPECT_TRUE(hits(token, term_token("3"), 0));
    EXPECT_TRUE(hits(token, term_token("true"), 7));
    EXPECT_TRUE(hits(token, term_token("false"), 7));
    // 2^63 lands in UINT64, 2^64 stays DOUBLE, INT64 max stays INT64.
    EXPECT_TRUE(hits(exact, term_uint64(uint64_t {1} << 63), 1));
    EXPECT_TRUE(hits(exact, term_double(18446744073709551616.0), 1));
    EXPECT_TRUE(hits(exact, term_double(1e300), 1));
    EXPECT_TRUE(hits(exact, term_int64(9223372036854775807), 1));
    // Root scalars and root array elements are leaves like any other.
    EXPECT_TRUE(hits(exact, term_string("root scalar"), 2));
    EXPECT_TRUE(hits(exact, term_int64(1), 3));
    EXPECT_TRUE(hits(exact, term_string("alpha"), 3));
    EXPECT_TRUE(hits(exact, term_string("beta"), 3));
    // Deeply nested arrays are recursed; JSON null inside arrays is skipped.
    EXPECT_TRUE(hits(token, term_token("gamma"), 7));
    EXPECT_TRUE(hits(exact, term_bool(true), 7));
    EXPECT_TRUE(hits(exact, term_bool(false), 7));
    EXPECT_TRUE(hits(exact, term_string("delta"), 6));
    EXPECT_TRUE(hits(exact, term_string("beta"), 6));
    // A leaf the codec cannot spell leaves the marker in the exact index only; documents made
    // of spellable leaves carry none.
    EXPECT_TRUE(hits(exact, unspellable_marker_term(), 8));
    EXPECT_FALSE(hits(token, unspellable_marker_term(), 8));
    EXPECT_FALSE(hits(exact, unspellable_marker_term(), 0));
    // Empty objects and JSON null documents are indexed documents without terms; NULL rows are
    // SQL NULLs.
    EXPECT_EQ(exact.stats().null_count,
              static_cast<uint64_t>(std::ranges::count(outer_nulls, uint8_t {1})));
}

} // namespace
} // namespace doris::segment_v2::variant_root_index
