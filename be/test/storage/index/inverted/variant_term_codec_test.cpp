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

#include "storage/index/inverted/variant_term_codec.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <map>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <vector>

namespace doris::segment_v2::variant_term_codec {
namespace {

std::string bytes(std::initializer_list<uint8_t> values) {
    std::string result;
    result.reserve(values.size());
    for (uint8_t value : values) {
        result.push_back(static_cast<char>(value));
    }
    return result;
}

std::string random_bytes(std::mt19937_64& rng, size_t max_len, bool nul_heavy) {
    std::uniform_int_distribution<size_t> len_dist(0, max_len);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    std::uniform_int_distribution<int> nul_dist(0, 3);
    std::string result;
    const size_t len = len_dist(rng);
    for (size_t i = 0; i < len; ++i) {
        if (nul_heavy && nul_dist(rng) == 0) {
            result.push_back(nul_dist(rng) == 0 ? '\1' : '\0');
        } else {
            result.push_back(static_cast<char>(byte_dist(rng)));
        }
    }
    return result;
}

std::vector<int64_t> int64_boundaries() {
    return {std::numeric_limits<int64_t>::min(),
            std::numeric_limits<int64_t>::min() + 1,
            -(int64_t {1} << 62),
            -(int64_t {1} << 32),
            -65536,
            -256,
            -2,
            -1,
            0,
            1,
            2,
            255,
            256,
            65535,
            65536,
            int64_t {1} << 32,
            int64_t {1} << 62,
            std::numeric_limits<int64_t>::max() - 1,
            std::numeric_limits<int64_t>::max()};
}

std::vector<double> double_boundaries() {
    return {-std::numeric_limits<double>::infinity(),
            -std::numeric_limits<double>::max(),
            -0x1p64,
            -0x1p63,
            -1.5,
            -1.0,
            -std::numeric_limits<double>::min(),
            -std::numeric_limits<double>::denorm_min(),
            -0.0,
            0.0,
            std::numeric_limits<double>::denorm_min(),
            std::numeric_limits<double>::min(),
            0.5,
            1.0,
            1.5,
            42.0,
            0x1p63,
            0x1p64,
            std::numeric_limits<double>::max(),
            std::numeric_limits<double>::infinity()};
}

double random_double(std::mt19937_64& rng) {
    // Mix uniformly random bit patterns (covers denormals, huge exponents) with "ordinary"
    // magnitudes so both regimes are exercised. NaN is excluded: canonical rules never emit it.
    std::uniform_int_distribution<int> mode(0, 2);
    while (true) {
        double value;
        switch (mode(rng)) {
        case 0:
            value = std::bit_cast<double>(rng());
            break;
        case 1:
            value = std::uniform_real_distribution<double>(-1e6, 1e6)(rng);
            break;
        default:
            value = static_cast<double>(std::uniform_int_distribution<int64_t>(-1000, 1000)(rng)) /
                    4.0;
            break;
        }
        if (!std::isnan(value)) {
            return value;
        }
    }
}

// ---------------------------------------------------------------------------------------------

TEST(VariantTermCodecTest, GoldenLayoutIsTagValueSeparatorPath) {
    EXPECT_EQ(term_int64(3, "a.b"), bytes({0x02, 0x80, 0, 0, 0, 0, 0, 0, 3, 'a', '.', 'b'}));
    EXPECT_EQ(term_int64(-1, "n"),
              bytes({0x02, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 'n'}));
    EXPECT_EQ(term_int64(std::numeric_limits<int64_t>::min(), ""),
              bytes({0x02, 0, 0, 0, 0, 0, 0, 0, 0}));
    EXPECT_EQ(term_uint64(uint64_t {1} << 63, "n"), bytes({0x03, 0x80, 0, 0, 0, 0, 0, 0, 0, 'n'}));
    EXPECT_EQ(term_uint64(std::numeric_limits<uint64_t>::max(), ""),
              bytes({0x03, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}));
    EXPECT_EQ(term_double(1.5, "n"), bytes({0x04, 0xbf, 0xf8, 0, 0, 0, 0, 0, 0, 'n'}));
    EXPECT_EQ(term_double(-1.0, "n"),
              bytes({0x04, 0x40, 0x0f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 'n'}));
    EXPECT_EQ(term_double(0.0, ""), bytes({0x04, 0x80, 0, 0, 0, 0, 0, 0, 0}));
    EXPECT_EQ(term_double(-0.0, ""), term_double(0.0, ""));
    EXPECT_EQ(term_bool(true, "p"), bytes({0x05, 0x01, 'p'}));
    EXPECT_EQ(term_bool(false, "p"), bytes({0x05, 0x00, 'p'}));
    EXPECT_EQ(term_string("x", "p"), bytes({0x06, 'x', 0x00, 0x00, 'p'}));
    EXPECT_EQ(term_string("", "p"), bytes({0x06, 0x00, 0x00, 'p'}));
    EXPECT_EQ(term_string("", ""), bytes({0x06, 0x00, 0x00}));
    EXPECT_EQ(term_string(std::string_view("a\0b", 3), "p"),
              bytes({0x06, 'a', 0x00, 0x01, 'b', 0x00, 0x00, 'p'}));
    EXPECT_EQ(term_string(std::string_view("\0\0", 2), "p"),
              bytes({0x06, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 'p'}));
    EXPECT_EQ(term_token("index", "payload.comment.body"),
              bytes({0x07, 'i', 'n', 'd', 'e', 'x', 0x00, 0x00}) + "payload.comment.body");
    EXPECT_EQ(term_string("键", "值"), bytes({0x06}) + "键" + bytes({0x00, 0x00}) + "值");

    // The root prefix is the term without its path.
    EXPECT_EQ(root_prefix_int64(3), bytes({0x02, 0x80, 0, 0, 0, 0, 0, 0, 3}));
    EXPECT_EQ(root_prefix_uint64(uint64_t {1} << 63), bytes({0x03, 0x80, 0, 0, 0, 0, 0, 0, 0}));
    EXPECT_EQ(root_prefix_double(1.5), bytes({0x04, 0xbf, 0xf8, 0, 0, 0, 0, 0, 0}));
    EXPECT_EQ(root_prefix_bool(true), bytes({0x05, 0x01}));
    EXPECT_EQ(root_prefix_string("x"), bytes({0x06, 'x', 0x00, 0x00}));
    EXPECT_EQ(root_prefix_token("x"), bytes({0x07, 'x', 0x00, 0x00}));
    EXPECT_EQ(term_int64(3, ""), root_prefix_int64(3));
    EXPECT_EQ(term_string("x", ""), root_prefix_string("x"));

    // Typed semantics: the same bytes under different tags are different terms.
    EXPECT_NE(term_string("1", "a"), term_int64(1, "a"));
    EXPECT_NE(term_string("x", "a"), term_token("x", "a"));
    EXPECT_NE(term_int64(1, "a"), term_uint64(1, "a"));
    EXPECT_NE(term_int64(1, "a"), term_double(1.0, "a"));
    // Path and value never blur into each other.
    EXPECT_NE(term_string("a", "bc"), term_string("ab", "c"));
}

TEST(VariantTermCodecTest, Int64EncodingPreservesOrder) {
    std::mt19937_64 rng(0x5eed1);
    std::vector<int64_t> values = int64_boundaries();
    std::uniform_int_distribution<int64_t> full(std::numeric_limits<int64_t>::min(),
                                                std::numeric_limits<int64_t>::max());
    std::uniform_int_distribution<int64_t> small(-4096, 4096);
    for (int i = 0; i < 4000; ++i) {
        values.push_back(full(rng));
        values.push_back(small(rng));
    }
    size_t checks = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        const int64_t a = values[i];
        const std::string ea = root_prefix_int64(a);
        for (size_t j = i; j < values.size() && j < i + 64; ++j) {
            const int64_t b = values[j];
            const std::string eb = root_prefix_int64(b);
            ASSERT_EQ(a < b, ea < eb) << a << " vs " << b;
            ASSERT_EQ(a == b, ea == eb) << a << " vs " << b;
            ++checks;
        }
    }
    EXPECT_GT(checks, 100000U);
    // Order survives an appended path because the value is fixed width.
    EXPECT_LT(term_int64(-1, "zzz"), term_int64(0, "aaa"));
    EXPECT_LT(term_int64(0, "zzz"), term_int64(1, "aaa"));
    EXPECT_LT(term_int64(std::numeric_limits<int64_t>::min(), "zzz"),
              term_int64(std::numeric_limits<int64_t>::max(), ""));
}

TEST(VariantTermCodecTest, Uint64EncodingPreservesOrder) {
    std::mt19937_64 rng(0x5eed2);
    std::vector<uint64_t> values {0,
                                  1,
                                  uint64_t {1} << 63,
                                  (uint64_t {1} << 63) + 1,
                                  std::numeric_limits<uint64_t>::max() - 1,
                                  std::numeric_limits<uint64_t>::max()};
    for (int i = 0; i < 4000; ++i) {
        values.push_back(rng());
        values.push_back(rng() | (uint64_t {1} << 63));
    }
    for (size_t i = 0; i < values.size(); ++i) {
        const std::string ea = root_prefix_uint64(values[i]);
        for (size_t j = i; j < values.size() && j < i + 64; ++j) {
            const std::string eb = root_prefix_uint64(values[j]);
            ASSERT_EQ(values[i] < values[j], ea < eb);
            ASSERT_EQ(values[i] == values[j], ea == eb);
        }
    }
}

TEST(VariantTermCodecTest, DoubleEncodingPreservesOrderIncludingInfinitiesAndDenormals) {
    std::mt19937_64 rng(0x5eed3);
    std::vector<double> values = double_boundaries();
    for (int i = 0; i < 8000; ++i) {
        values.push_back(random_double(rng));
    }
    size_t checks = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        const double a = values[i];
        const std::string ea = root_prefix_double(a);
        for (size_t j = i; j < values.size() && j < i + 64; ++j) {
            const double b = values[j];
            const std::string eb = root_prefix_double(b);
            ASSERT_EQ(a < b, ea < eb) << a << " vs " << b;
            ASSERT_EQ(a == b, ea == eb) << a << " vs " << b;
            ++checks;
        }
    }
    EXPECT_GT(checks, 100000U);
    // -0.0 folds into 0.0 before encoding, so it is neither below nor distinct from 0.0.
    EXPECT_EQ(root_prefix_double(-0.0), root_prefix_double(0.0));
    EXPECT_LT(root_prefix_double(-std::numeric_limits<double>::denorm_min()),
              root_prefix_double(0.0));
    EXPECT_LT(root_prefix_double(0.0),
              root_prefix_double(std::numeric_limits<double>::denorm_min()));
    EXPECT_LT(root_prefix_double(-std::numeric_limits<double>::infinity()),
              root_prefix_double(-std::numeric_limits<double>::max()));
    EXPECT_LT(root_prefix_double(std::numeric_limits<double>::max()),
              root_prefix_double(std::numeric_limits<double>::infinity()));
    // Every NaN payload folds into one canonical term so equality stays deterministic.
    EXPECT_EQ(root_prefix_double(std::numeric_limits<double>::quiet_NaN()),
              root_prefix_double(-std::numeric_limits<double>::quiet_NaN()));
    EXPECT_EQ(root_prefix_double(std::numeric_limits<double>::quiet_NaN()),
              root_prefix_double(std::numeric_limits<double>::signaling_NaN()));
}

TEST(VariantTermCodecTest, EscapedStringsPreserveOrderAndStayPrefixFree) {
    std::mt19937_64 rng(0x5eed4);
    std::vector<std::string> values {"",
                                     std::string("\0", 1),
                                     std::string("\0\0", 2),
                                     std::string("\0\1", 2),
                                     std::string("\1", 1),
                                     "a",
                                     std::string("a\0", 2),
                                     std::string("a\0\0", 3),
                                     "ab",
                                     "abc",
                                     "b",
                                     std::string("\xff", 1),
                                     std::string("\xff\xff", 2)};
    for (int i = 0; i < 3000; ++i) {
        values.push_back(random_bytes(rng, 12, /*nul_heavy=*/true));
        values.push_back(random_bytes(rng, 40, /*nul_heavy=*/false));
    }
    // Shared prefixes are the interesting case, so derive a few from each other.
    const size_t base = values.size();
    for (size_t i = 0; i < base; i += 7) {
        values.push_back(values[i] + std::string("\0", 1));
        values.push_back(values[i] + "x");
        if (!values[i].empty()) {
            values.push_back(values[i].substr(0, values[i].size() - 1));
        }
    }
    size_t checks = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        const std::string& a = values[i];
        const std::string ea = root_prefix_string(a);
        for (size_t j = i; j < values.size() && j < i + 48; ++j) {
            const std::string& b = values[j];
            const std::string eb = root_prefix_string(b);
            ASSERT_EQ(a < b, ea < eb);
            ASSERT_EQ(a == b, ea == eb);
            ++checks;
        }
    }
    EXPECT_GT(checks, 100000U);
    // The terminator makes the value self-delimiting: the path suffix cannot reorder terms
    // across different values, only within one value.
    EXPECT_LT(term_string("a", "zzz"), term_string("ab", ""));
    EXPECT_LT(term_string("ab", "zzz"), term_string("b", ""));
    EXPECT_LT(term_string(std::string_view("a\0", 2), "zzz"), term_string("a\1", ""));
}

TEST(VariantTermCodecTest, RootPrefixMatchesExactlyTheEqualValue) {
    std::mt19937_64 rng(0x5eed5);
    std::vector<std::string> values {"",
                                     std::string("\0", 1),
                                     std::string("\0\0", 2),
                                     "a",
                                     std::string("a\0", 2),
                                     std::string("a\0\0", 3),
                                     std::string("a\0\0b", 4),
                                     "ab",
                                     "abc"};
    for (int i = 0; i < 1500; ++i) {
        values.push_back(random_bytes(rng, 10, /*nul_heavy=*/true));
    }
    const size_t base = values.size();
    for (size_t i = 0; i < base; i += 3) {
        values.push_back(values[i] + std::string("\0\0", 2));
        values.push_back(values[i] + std::string("\0", 1));
    }
    const std::vector<std::string> paths {
            "", "p", "p.q", std::string("\0", 1), std::string("\0\0", 2), std::string("x\0y", 3)};
    size_t checks = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        const std::string prefix = root_prefix_string(values[i]);
        for (size_t j = std::max<size_t>(i, 0); j < values.size() && j < i + 24; ++j) {
            for (const std::string& path : paths) {
                const std::string term = term_string(values[j], path);
                ASSERT_EQ(values[i] == values[j], has_root_prefix(term, prefix))
                        << "value " << i << " vs " << j;
                ++checks;
            }
        }
    }
    EXPECT_GT(checks, 100000U);

    // Fixed width prefixes match by value only, whatever bytes the path contributes.
    for (const int64_t v : int64_boundaries()) {
        for (const int64_t w : int64_boundaries()) {
            EXPECT_EQ(v == w, has_root_prefix(term_int64(w, "\x80\x00\xff"), root_prefix_int64(v)));
        }
    }
    for (const double v : double_boundaries()) {
        for (const double w : double_boundaries()) {
            EXPECT_EQ(v == w, has_root_prefix(term_double(w, "path"), root_prefix_double(v)));
        }
    }
    EXPECT_TRUE(has_root_prefix(term_bool(true, "p"), root_prefix_bool(true)));
    EXPECT_FALSE(has_root_prefix(term_bool(false, "p"), root_prefix_bool(true)));
    // Tags partition the dictionary: a STRING prefix never matches a TOKEN term and vice versa.
    EXPECT_FALSE(has_root_prefix(term_token("x", "p"), root_prefix_string("x")));
    EXPECT_FALSE(has_root_prefix(term_string("x", "p"), root_prefix_token("x")));
}

TEST(VariantTermCodecTest, DecodeRoundTripsEveryTag) {
    std::mt19937_64 rng(0x5eed6);
    const std::vector<std::string> paths {
            "", "a", "a.b.c", std::string("\0", 1), std::string("p\0\0q", 4), "键.值"};
    for (const int64_t v : int64_boundaries()) {
        for (const std::string& path : paths) {
            const std::string term = term_int64(v, path);
            const auto decoded = decode(term);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->tag, Tag::INT64);
            EXPECT_EQ(decoded->int64_value, v);
            EXPECT_EQ(decoded->path, path);
            EXPECT_EQ(decoded->path.data(), term.data() + term.size() - path.size());
        }
    }
    for (const uint64_t v :
         {uint64_t {0}, uint64_t {1} << 63, std::numeric_limits<uint64_t>::max()}) {
        const std::string term = term_uint64(v, "u");
        const auto decoded = decode(term);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->tag, Tag::UINT64);
        EXPECT_EQ(decoded->uint64_value, v);
        EXPECT_EQ(decoded->path, "u");
    }
    for (const double v : double_boundaries()) {
        const std::string term = term_double(v, "d");
        const auto decoded = decode(term);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->tag, Tag::DOUBLE);
        if (v == 0.0) {
            EXPECT_EQ(std::bit_cast<uint64_t>(decoded->double_value), std::bit_cast<uint64_t>(0.0));
        } else {
            EXPECT_EQ(std::bit_cast<uint64_t>(decoded->double_value), std::bit_cast<uint64_t>(v));
        }
        EXPECT_EQ(decoded->path, "d");
    }
    for (int i = 0; i < 5000; ++i) {
        const double v = random_double(rng);
        const std::string term = term_double(v, "");
        const auto decoded = decode(term);
        ASSERT_TRUE(decoded.has_value());
        ASSERT_EQ(decoded->double_value, v);
    }
    for (const bool v : {false, true}) {
        const std::string term = term_bool(v, "b");
        const auto decoded = decode(term);
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->tag, Tag::BOOL);
        EXPECT_EQ(decoded->bool_value, v);
        EXPECT_EQ(decoded->path, "b");
    }
    for (int i = 0; i < 5000; ++i) {
        const std::string value = random_bytes(rng, 16, /*nul_heavy=*/true);
        const std::string& path = paths[i % paths.size()];
        for (const bool token : {false, true}) {
            const std::string term = token ? term_token(value, path) : term_string(value, path);
            const auto decoded = decode(term);
            ASSERT_TRUE(decoded.has_value());
            ASSERT_EQ(decoded->tag, token ? Tag::TOKEN : Tag::STRING);
            ASSERT_EQ(decoded->string_value, value);
            ASSERT_EQ(decoded->path, path);
        }
    }
}

TEST(VariantTermCodecTest, DecodeRejectsMalformedTerms) {
    EXPECT_FALSE(decode("").has_value());
    EXPECT_FALSE(decode(bytes({0x01})).has_value());          // unknown tag
    EXPECT_FALSE(decode(bytes({0x08, 1, 2})).has_value());    // unknown tag
    EXPECT_FALSE(decode(bytes({0x02, 1, 2, 3})).has_value()); // truncated fixed width
    EXPECT_FALSE(decode(bytes({0x04, 1, 2, 3, 4, 5, 6, 7})).has_value());
    EXPECT_FALSE(decode(bytes({0x05})).has_value());       // missing bool byte
    EXPECT_FALSE(decode(bytes({0x05, 0x02})).has_value()); // invalid bool byte
    EXPECT_FALSE(decode(bytes({0x06})).has_value());       // missing terminator
    EXPECT_FALSE(decode(bytes({0x06, 'a'})).has_value());
    EXPECT_FALSE(decode(bytes({0x06, 'a', 0x00})).has_value());            // dangling escape
    EXPECT_FALSE(decode(bytes({0x06, 'a', 0x00, 0x02, 'p'})).has_value()); // bad escape
    EXPECT_FALSE(decode(bytes({0x07, 0x00, 0x01})).has_value());           // escaped NUL, no end
    EXPECT_TRUE(decode(bytes({0x06, 0x00, 0x00})).has_value());
    EXPECT_TRUE(decode(bytes({0x02, 0, 0, 0, 0, 0, 0, 0, 0})).has_value());
}

TEST(VariantTermCodecTest, SameValueAcrossPathsIsOneContiguousDictionaryRun) {
    std::mt19937_64 rng(0x5eed7);
    std::vector<std::string> values {"", "a", std::string("a\0", 2), "ab", "b"};
    for (int i = 0; i < 300; ++i) {
        values.push_back(random_bytes(rng, 8, /*nul_heavy=*/true));
    }
    std::vector<std::string> paths {"", "a", "a.b", "b", std::string("\0", 1), "z"};
    for (int i = 0; i < 40; ++i) {
        paths.push_back(random_bytes(rng, 12, /*nul_heavy=*/true));
    }
    std::set<std::string> dictionary;
    std::map<std::string, size_t> expected_counts;
    for (const std::string& value : values) {
        for (const std::string& path : paths) {
            dictionary.insert(term_string(value, path));
            dictionary.insert(term_int64(static_cast<int64_t>(value.size()) - 3, path));
        }
        // Distinct (value, path) pairs are distinct terms; count what a prefix scan must see.
        std::set<std::string> distinct(paths.begin(), paths.end());
        expected_counts[value] = distinct.size();
    }
    for (const std::string& value : values) {
        const std::string prefix = root_prefix_string(value);
        const std::string upper = prefix_upper_bound(prefix);
        auto it = dictionary.lower_bound(prefix);
        size_t seen = 0;
        while (it != dictionary.end() && has_root_prefix(*it, prefix)) {
            const auto decoded = decode(*it);
            ASSERT_TRUE(decoded.has_value());
            ASSERT_EQ(decoded->tag, Tag::STRING);
            ASSERT_EQ(decoded->string_value, value);
            ++seen;
            ++it;
        }
        ASSERT_EQ(seen, expected_counts[value]) << "value size " << value.size();
        // The run ends exactly where the exclusive upper bound begins.
        if (!upper.empty()) {
            ASSERT_TRUE(it == dictionary.end() || *it >= upper);
        }
        // Nothing with this value exists outside the run.
        size_t total = 0;
        for (const std::string& term : dictionary) {
            const auto decoded = decode(term);
            ASSERT_TRUE(decoded.has_value());
            if (decoded->tag == Tag::STRING && decoded->string_value == value) {
                ++total;
            }
        }
        ASSERT_EQ(total, seen);
    }
}

TEST(VariantTermCodecTest, IntegerRangeScanIsADictionaryIntervalFilteredByPathSuffix) {
    std::mt19937_64 rng(0x5eed8);
    const std::vector<std::string> paths {"a", "a.b", "b", "c.d.e", "n"};
    std::set<std::string> dictionary;
    std::map<std::string, std::vector<int64_t>> per_path;
    std::uniform_int_distribution<int64_t> dist(-500, 500);
    for (int i = 0; i < 2000; ++i) {
        const int64_t v = dist(rng);
        const std::string& path = paths[rng() % paths.size()];
        dictionary.insert(term_int64(v, path));
        per_path[path].push_back(v);
        // Neighbouring tags and values must never leak into an INT64 interval.
        dictionary.insert(term_double(static_cast<double>(v) + 0.5, path));
        dictionary.insert(term_string(std::to_string(v), path));
        dictionary.insert(term_uint64(static_cast<uint64_t>(v) | (uint64_t {1} << 63), path));
    }
    for (int round = 0; round < 200; ++round) {
        int64_t lo = dist(rng);
        int64_t hi = dist(rng);
        if (lo > hi) {
            std::swap(lo, hi);
        }
        const std::string& path = paths[rng() % paths.size()];
        // Exclusive end: the smallest prefix strictly above every term carrying `hi`.
        const std::string begin = root_prefix_int64(lo);
        const std::string end = prefix_upper_bound(root_prefix_int64(hi));
        std::set<int64_t> found;
        size_t visited = 0;
        for (auto it = dictionary.lower_bound(begin); it != dictionary.end() && *it < end; ++it) {
            ++visited;
            const auto decoded = decode(*it);
            ASSERT_TRUE(decoded.has_value());
            ASSERT_EQ(decoded->tag, Tag::INT64);
            ASSERT_GE(decoded->int64_value, lo);
            ASSERT_LE(decoded->int64_value, hi);
            if (decoded->path == path) {
                found.insert(decoded->int64_value);
            }
        }
        std::set<int64_t> expected;
        size_t expected_visited = 0;
        for (const auto& [p, vs] : per_path) {
            std::set<int64_t> distinct(vs.begin(), vs.end());
            for (const int64_t v : distinct) {
                if (v >= lo && v <= hi) {
                    ++expected_visited;
                    if (p == path) {
                        expected.insert(v);
                    }
                }
            }
        }
        ASSERT_EQ(found, expected);
        // The documented cost model: the scan touches every path carrying a value in range,
        // nothing more.
        ASSERT_EQ(visited, expected_visited);
    }
}

TEST(VariantTermCodecTest, TokenPrefixAndSuffixComposeTheTokenTerm) {
    // The analyzer lane builds prefix + escape(token) + suffix; it must equal term_token().
    const auto compose = [](std::string_view token, std::string_view path) {
        std::string out(token_prefix());
        for (const char c : token) {
            out.push_back(c);
            if (c == '\0') {
                out.push_back('\1');
            }
        }
        out.append(token_suffix(path));
        return out;
    };
    EXPECT_EQ(compose("index", "payload.body"), term_token("index", "payload.body"));
    EXPECT_EQ(compose("", ""), term_token("", ""));
    EXPECT_EQ(compose(std::string_view("a\0b", 3), "p"),
              term_token(std::string_view("a\0b", 3), "p"));
}

TEST(VariantTermCodecTest, PrefixUpperBoundIsTheSmallestTermAboveThePrefix) {
    EXPECT_EQ(prefix_upper_bound(bytes({0x02, 0x80, 0x00})), bytes({0x02, 0x80, 0x01}));
    EXPECT_EQ(prefix_upper_bound(bytes({0x02, 0xff, 0xff})), bytes({0x03}));
    EXPECT_EQ(prefix_upper_bound(bytes({0xff, 0xff})), "");
    EXPECT_EQ(prefix_upper_bound(""), "");
    std::mt19937_64 rng(0x5eed9);
    for (int i = 0; i < 2000; ++i) {
        const std::string prefix = random_bytes(rng, 6, /*nul_heavy=*/false);
        const std::string upper = prefix_upper_bound(prefix);
        for (int j = 0; j < 8; ++j) {
            const std::string suffix = random_bytes(rng, 6, /*nul_heavy=*/true);
            const std::string term = prefix + suffix;
            ASSERT_GE(term, prefix);
            if (!upper.empty()) {
                ASSERT_LT(term, upper);
            }
        }
        if (!upper.empty()) {
            ASSERT_FALSE(has_root_prefix(upper, prefix));
        }
    }
}

} // namespace
} // namespace doris::segment_v2::variant_term_codec
