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
#include <bit>
#include <cmath>
#include <cstdint>
#include <limits>
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

std::string random_bytes(std::mt19937_64& rng, size_t max_len) {
    std::uniform_int_distribution<size_t> len_dist(0, max_len);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    std::string result;
    const size_t len = len_dist(rng);
    for (size_t i = 0; i < len; ++i) {
        result.push_back(static_cast<char>(byte_dist(rng)));
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

TEST(VariantTermCodecTest, GoldenLayoutIsTagThenValue) {
    EXPECT_EQ(term_int64(3), bytes({0x02, 0x80, 0, 0, 0, 0, 0, 0, 3}));
    EXPECT_EQ(term_int64(-1), bytes({0x02, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}));
    EXPECT_EQ(term_uint64(1), bytes({0x03, 0, 0, 0, 0, 0, 0, 0, 1}));
    EXPECT_EQ(term_double(0.0), bytes({0x04, 0x80, 0, 0, 0, 0, 0, 0, 0}));
    EXPECT_EQ(term_double(-0.0), term_double(0.0));
    EXPECT_EQ(term_bool(true), bytes({0x05, 1}));
    EXPECT_EQ(term_bool(false), bytes({0x05, 0}));
    // Strings and tokens are raw: an embedded NUL byte, an empty value and a value that looks
    // like another tag are all just bytes after the tag.
    EXPECT_EQ(term_string("ab"), bytes({0x06, 'a', 'b'}));
    EXPECT_EQ(term_string(std::string_view("a\0b", 3)), bytes({0x06, 'a', 0, 'b'}));
    EXPECT_EQ(term_string(""), bytes({0x06}));
    EXPECT_EQ(term_string(std::string_view("\x02", 1)), bytes({0x06, 0x02}));
    EXPECT_EQ(term_token("ab"), bytes({0x07, 'a', 'b'}));
    EXPECT_EQ(term_other(), bytes({0x08}));
    EXPECT_EQ(std::string(token_term_prefix()) + "ab", term_token("ab"));
}

TEST(VariantTermCodecTest, TypesNeverCollide) {
    EXPECT_NE(term_string("1"), term_int64(1));
    EXPECT_NE(term_string("x"), term_token("x"));
    EXPECT_NE(term_int64(1), term_uint64(1));
    EXPECT_NE(term_int64(1), term_double(1.0));
    EXPECT_NE(term_bool(true), term_int64(1));
    EXPECT_NE(term_string(""), term_other());
    // Every tag is a distinct first byte, so terms of different types never share a prefix.
    const std::set<char> tags = {term_int64(0)[0],   term_uint64(0)[0],  term_double(0)[0],
                                 term_bool(true)[0], term_string("")[0], term_token("")[0],
                                 term_other()[0]};
    EXPECT_EQ(tags.size(), 7U);
}

TEST(VariantTermCodecTest, Int64EncodingPreservesOrder) {
    const std::vector<int64_t> values = int64_boundaries();
    for (size_t i = 0; i < values.size(); ++i) {
        for (size_t j = 0; j < values.size(); ++j) {
            EXPECT_EQ(values[i] < values[j], term_int64(values[i]) < term_int64(values[j]))
                    << values[i] << " vs " << values[j];
        }
    }
    std::mt19937_64 rng(0x5eed1);
    for (int round = 0; round < 20000; ++round) {
        const auto a = static_cast<int64_t>(rng());
        const auto b = static_cast<int64_t>(rng());
        EXPECT_EQ(a < b, term_int64(a) < term_int64(b));
        EXPECT_EQ(a == b, term_int64(a) == term_int64(b));
    }
}

TEST(VariantTermCodecTest, Uint64EncodingPreservesOrder) {
    const std::vector<uint64_t> values = {0,
                                          1,
                                          255,
                                          256,
                                          uint64_t {1} << 32,
                                          uint64_t {1} << 63,
                                          (uint64_t {1} << 63) + 1,
                                          std::numeric_limits<uint64_t>::max() - 1,
                                          std::numeric_limits<uint64_t>::max()};
    for (size_t i = 0; i < values.size(); ++i) {
        for (size_t j = 0; j < values.size(); ++j) {
            EXPECT_EQ(values[i] < values[j], term_uint64(values[i]) < term_uint64(values[j]));
        }
    }
}

TEST(VariantTermCodecTest, DoubleEncodingPreservesOrderIncludingInfinitiesAndDenormals) {
    const std::vector<double> values = double_boundaries();
    for (size_t i = 0; i < values.size(); ++i) {
        for (size_t j = 0; j < values.size(); ++j) {
            const bool less = values[i] < values[j];
            const bool equal = values[i] == values[j]; // -0.0 == 0.0
            EXPECT_EQ(less, term_double(values[i]) < term_double(values[j]))
                    << values[i] << " vs " << values[j];
            EXPECT_EQ(equal, term_double(values[i]) == term_double(values[j]))
                    << values[i] << " vs " << values[j];
        }
    }
    std::mt19937_64 rng(0x5eed2);
    for (int round = 0; round < 20000; ++round) {
        const double a = random_double(rng);
        const double b = random_double(rng);
        EXPECT_EQ(a < b, term_double(a) < term_double(b)) << a << " vs " << b;
        EXPECT_EQ(a == b, term_double(a) == term_double(b)) << a << " vs " << b;
    }
    // Every NaN payload folds into one term (callers never produce one, but the codec is total).
    EXPECT_EQ(term_double(std::numeric_limits<double>::quiet_NaN()),
              term_double(-std::numeric_limits<double>::quiet_NaN()));
    EXPECT_EQ(term_double(std::numeric_limits<double>::quiet_NaN()),
              term_double(std::numeric_limits<double>::signaling_NaN()));
}

TEST(VariantTermCodecTest, StringsPreserveByteOrderAndAreInjective) {
    std::mt19937_64 rng(0x5eed3);
    for (int round = 0; round < 20000; ++round) {
        const std::string a = random_bytes(rng, 12);
        const std::string b = random_bytes(rng, 12);
        EXPECT_EQ(a < b, term_string(a) < term_string(b));
        EXPECT_EQ(a == b, term_string(a) == term_string(b));
        EXPECT_EQ(a == b, term_token(a) == term_token(b));
    }
}

TEST(VariantTermCodecTest, DecodeRoundTripsEveryTag) {
    for (const int64_t v : int64_boundaries()) {
        const auto decoded = decode(term_int64(v));
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->tag, Tag::INT64);
        EXPECT_EQ(decoded->int64_value, v);
    }
    for (const uint64_t v :
         {uint64_t {0}, uint64_t {1} << 63, std::numeric_limits<uint64_t>::max()}) {
        const auto decoded = decode(term_uint64(v));
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->tag, Tag::UINT64);
        EXPECT_EQ(decoded->uint64_value, v);
    }
    for (const double v : double_boundaries()) {
        const auto decoded = decode(term_double(v));
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->tag, Tag::DOUBLE);
        EXPECT_EQ(std::bit_cast<uint64_t>(decoded->double_value),
                  std::bit_cast<uint64_t>(v == 0.0 ? 0.0 : v));
    }
    for (const bool v : {true, false}) {
        const auto decoded = decode(term_bool(v));
        ASSERT_TRUE(decoded.has_value());
        EXPECT_EQ(decoded->tag, Tag::BOOL);
        EXPECT_EQ(decoded->bool_value, v);
    }
    std::mt19937_64 rng(0x5eed4);
    for (int round = 0; round < 2000; ++round) {
        const std::string value = random_bytes(rng, 24);
        for (const bool token : {false, true}) {
            const std::string term = token ? term_token(value) : term_string(value);
            const auto decoded = decode(term);
            ASSERT_TRUE(decoded.has_value());
            EXPECT_EQ(decoded->tag, token ? Tag::TOKEN : Tag::STRING);
            EXPECT_EQ(decoded->value, value);
        }
    }
    const auto other = decode(term_other());
    ASSERT_TRUE(other.has_value());
    EXPECT_EQ(other->tag, Tag::OTHER);
}

TEST(VariantTermCodecTest, DecodeRejectsMalformedTerms) {
    EXPECT_FALSE(decode("").has_value());
    EXPECT_FALSE(decode(bytes({0x01})).has_value());          // unknown tag
    EXPECT_FALSE(decode(bytes({0x02, 1, 2, 3})).has_value()); // short fixed-width value
    EXPECT_FALSE(decode(term_int64(1) + "x").has_value());    // trailing bytes
    EXPECT_FALSE(decode(bytes({0x05})).has_value());          // missing bool
    EXPECT_FALSE(decode(bytes({0x05, 2})).has_value());       // invalid bool
    EXPECT_FALSE(decode(bytes({0x08, 'x'})).has_value());     // marker carries no value
}

} // namespace
} // namespace doris::segment_v2::variant_term_codec
