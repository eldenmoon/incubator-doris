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
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/term_query.h"
#include "storage/segment/variant/v2/variant_root_index_writer.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2::variant_root_index {
namespace {

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
    if (std::isnan(value)) {
        leaf.kind = OracleKind::OTHER;
        return leaf;
    }
    if (std::isfinite(value) && std::floor(value) == value) {
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
const std::vector<std::string> kWords = {"alpha",      "beta", "gamma", "delta",
                                         "alpha beta", "",     "3",     "gamma delta alpha"};
const std::vector<std::string> kPaths = {"a",   "b",   "c",   "d",     "e", "a.b",
                                         "a.c", "b.a", "c.d", "a.b.c", ""};

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

    std::string path() { return pick(kPaths); }

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
        switch (_rng() % 5) {
        case 0:
            return pick(kInts);
        case 1:
            return pick(kDoubles);
        case 2:
            return _rng() % 2 == 0 ? "true" : "false";
        case 3:
            return "null";
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
                std::string(VARIANT_ROOT_FORMAT_VERSION_V1);
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
    const TabletIndex root_exact = make_index(90, VARIANT_INDEX_MODE_ROOT, "none");
    const TabletIndex root_token = make_index(91, VARIANT_INDEX_MODE_ROOT, "english");
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
    };
    DocGenerator generator(0x9a17e);
    for (int i = 0; i < 400; ++i) {
        docs.push_back(generator.document());
    }
    std::vector<uint8_t> outer_nulls(docs.size(), 0);
    for (size_t i = 0; i < docs.size(); ++i) {
        if (i >= 8 && generator.next() % 10 == 0) {
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

    // Write the four indexes in one traversal.
    const std::string prefix = std::string(TEST_DIR) + "/parity";
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->create_file(InvertedIndexDescriptor::get_index_file_path_v2(prefix),
                                      &file_writer)
                        .ok());
    IndexFileWriter index_file_writer(io::global_local_filesystem(), prefix, "parity_rowset",
                                      /*seg_id=*/0, InvertedIndexStorageFormatPB::SNII,
                                      std::move(file_writer));
    VariantRootIndexWriter w_root_exact(&index_file_writer, &root_exact, false, false);
    VariantRootIndexWriter w_root_token(&index_file_writer, &root_token, false, false);
    VariantRootIndexWriter w_all_exact(&index_file_writer, &all_exact, false, false);
    VariantRootIndexWriter w_all_token(&index_file_writer, &all_token, false, false);
    std::array<VariantRootIndexWriter*, 4> writers = {&w_root_exact, &w_root_token, &w_all_exact,
                                                      &w_all_token};
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
    auto r_root_exact = index_file_reader.open_snii_index(&root_exact);
    auto r_root_token = index_file_reader.open_snii_index(&root_token);
    auto r_all_exact = index_file_reader.open_snii_index(&all_exact);
    auto r_all_token = index_file_reader.open_snii_index(&all_token);
    ASSERT_TRUE(r_root_exact.has_value()) << r_root_exact.error();
    ASSERT_TRUE(r_root_token.has_value()) << r_root_token.error();
    ASSERT_TRUE(r_all_exact.has_value()) << r_all_exact.error();
    ASSERT_TRUE(r_all_token.has_value()) << r_all_token.error();
    for (const auto* reader :
         {r_root_exact->get(), r_root_token->get(), r_all_exact->get(), r_all_token->get()}) {
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
    const auto tokens_at = [](const std::vector<OracleLeaf>& leaves, std::string_view path,
                              bool any_path) {
        std::set<std::string> tokens;
        for (const OracleLeaf& leaf : leaves) {
            if (leaf.kind != OracleKind::STRING || (!any_path && leaf.path != path)) {
                continue;
            }
            for (std::string& word : split_words(leaf.text)) {
                tokens.insert(std::move(word));
            }
        }
        return tokens;
    };

    size_t checks = 0;
    size_t non_empty = 0;
    for (int round = 0; round < 600; ++round) {
        const std::string path = generator.path();
        const Literal literal = generator.literal();
        const Field field = literal.field();

        // 1. path equality on the Root exact index
        {
            std::vector<std::string> terms;
            ASSERT_TRUE(encode_query_value_terms(path, field, &terms).ok());
            ASSERT_EQ(terms.size(), 1U);
            const auto actual = term_docs(**r_root_exact, terms[0]);
            const auto expected = expected_rows([&](const std::vector<OracleLeaf>& leaves) {
                return std::ranges::any_of(leaves, [&](const OracleLeaf& leaf) {
                    return leaf.path == path && literal.matches(leaf);
                });
            });
            EXPECT_EQ(actual, expected) << "path=" << path << " round=" << round;
            ++checks;
            non_empty += !expected.empty();
        }
        // 2. any-path equality on the AllValues exact index
        {
            std::vector<std::string> terms;
            ASSERT_TRUE(encode_all_values_query_value_terms(field, &terms).ok());
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
        // 3./4. path MATCH_ANY / MATCH_ALL on the Root token index
        // 5./6. any-path MATCH_ANY / MATCH_ALL on the AllValues token index
        {
            const std::string query = generator.match_query();
            const std::vector<std::string> words = split_words(query);
            std::vector<std::string> path_terms;
            std::vector<std::string> root_terms;
            for (const std::string& word : words) {
                path_terms.push_back(encode_token_term(path, word));
                root_terms.push_back(encode_all_value_token_term(word));
            }
            const auto covers = [&](const std::set<std::string>& tokens) {
                return std::ranges::all_of(
                        words, [&](const std::string& word) { return tokens.contains(word); });
            };
            const auto touches = [&](const std::set<std::string>& tokens) {
                return std::ranges::any_of(
                        words, [&](const std::string& word) { return tokens.contains(word); });
            };
            EXPECT_EQ(union_docs(**r_root_token, path_terms),
                      expected_rows([&](const std::vector<OracleLeaf>& leaves) {
                          return touches(tokens_at(leaves, path, false));
                      }))
                    << "MATCH_ANY path=" << path << " query=" << query;
            EXPECT_EQ(and_docs(**r_root_token, path_terms),
                      expected_rows([&](const std::vector<OracleLeaf>& leaves) {
                          return covers(tokens_at(leaves, path, false));
                      }))
                    << "MATCH_ALL path=" << path << " query=" << query;
            EXPECT_EQ(union_docs(**r_all_token, root_terms),
                      expected_rows([&](const std::vector<OracleLeaf>& leaves) {
                          return touches(tokens_at(leaves, {}, true));
                      }))
                    << "root MATCH_ANY query=" << query;
            EXPECT_EQ(and_docs(**r_all_token, root_terms),
                      expected_rows([&](const std::vector<OracleLeaf>& leaves) {
                          return covers(tokens_at(leaves, {}, true));
                      }))
                    << "root MATCH_ALL query=" << query;
            checks += 4;
        }
    }
    EXPECT_GE(checks, 3600U);
    // The generator must actually produce hits, or the parity is vacuous.
    EXPECT_GT(non_empty, 200U);

    // Typed semantics pinned on the edge documents. Random documents draw from the same value
    // pools, so the pins check membership of the edge row rather than an exact posting list.
    const auto hits = [&](const snii::reader::LogicalIndexReader& reader, const std::string& term,
                          uint32_t row) {
        const std::vector<uint32_t> docids = term_docs(reader, term);
        return std::ranges::find(docids, row) != docids.end();
    };
    // "3", 3 and 3.0 are two values, not three; -0.0 is 0.
    EXPECT_TRUE(hits(**r_root_exact, encode_string_term("a", "3"), 0));
    EXPECT_TRUE(hits(**r_root_exact, encode_int64_term("b", 3), 0));
    EXPECT_TRUE(hits(**r_root_exact, encode_int64_term("c", 3), 0));
    EXPECT_FALSE(hits(**r_root_exact, encode_int64_term("a", 3), 0));
    EXPECT_FALSE(hits(**r_root_exact, encode_string_term("b", "3"), 0));
    EXPECT_FALSE(hits(**r_root_exact, encode_double_term("c", 3.0), 0));
    EXPECT_TRUE(hits(**r_root_exact, encode_int64_term("d", 0), 0));
    EXPECT_TRUE(hits(**r_root_exact, encode_string_term("e.a", "gamma"), 0));
    // 2^63 lands in UINT64, 2^64 stays DOUBLE, INT64 max stays INT64.
    EXPECT_TRUE(hits(**r_root_exact, encode_uint64_term("a", uint64_t {1} << 63), 1));
    EXPECT_TRUE(hits(**r_root_exact, encode_double_term("b", 18446744073709551616.0), 1));
    EXPECT_TRUE(hits(**r_root_exact, encode_double_term("c", 1e300), 1));
    EXPECT_TRUE(hits(**r_root_exact, encode_int64_term("d", 9223372036854775807), 1));
    // Root scalars and root array elements live at the empty path (all-values: any path).
    EXPECT_TRUE(hits(**r_all_exact, encode_all_value_term("root scalar"), 2));
    EXPECT_TRUE(hits(**r_root_exact, encode_string_term("", "root scalar"), 2));
    EXPECT_TRUE(hits(**r_root_exact, encode_int64_term("", 1), 3));
    EXPECT_TRUE(hits(**r_root_exact, encode_string_term("", "alpha"), 3));
    EXPECT_TRUE(hits(**r_root_exact, encode_string_term("a", "beta"), 3));
    EXPECT_TRUE(hits(**r_all_exact, encode_all_value_term("beta"), 3));
    // Deeply nested arrays keep the array path; JSON null inside arrays is skipped.
    EXPECT_TRUE(hits(**r_root_token, encode_token_term("a", "gamma"), 7));
    EXPECT_TRUE(hits(**r_root_exact, encode_bool_term("b", true), 7));
    EXPECT_TRUE(hits(**r_root_exact, encode_bool_term("b", false), 7));
    // Nested objects and dotted keys both land in the dotted path namespace.
    EXPECT_TRUE(hits(**r_root_exact, encode_string_term("a.b.c", "delta"), 6));
    EXPECT_TRUE(hits(**r_root_exact, encode_string_term("b.a", "alpha"), 6));
    EXPECT_TRUE(hits(**r_root_exact, encode_string_term("c.d", "beta"), 6));
    EXPECT_FALSE(hits(**r_root_exact, encode_string_term("a.b.c", "alpha"), 6));
    // Empty objects and JSON null documents are indexed documents without terms; NULL rows are
    // SQL NULLs.
    EXPECT_EQ((*r_root_exact)->stats().null_count,
              static_cast<uint64_t>(std::ranges::count(outer_nulls, uint8_t {1})));
}

} // namespace
} // namespace doris::segment_v2::variant_root_index
