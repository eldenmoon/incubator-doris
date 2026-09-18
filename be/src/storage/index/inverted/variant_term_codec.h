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

#pragma once

// Term codec of the VARIANT root index (variant_root_index.h).
//
// This header is intentionally self-contained (standard library only) so the byte layout can be
// unit-tested and reasoned about in isolation from the rest of the storage engine. Every byte
// that reaches the SNII dictionary of a VARIANT root index is produced here.
//
// Layout (format version 4, see VARIANT_ROOT_FORMAT_VERSION_CURRENT)
//
//   term := tag(1 byte) value
//
//   tag:   0x02 INT64 | 0x03 UINT64 | 0x04 DOUBLE | 0x05 BOOL | 0x06 STRING | 0x07 TOKEN
//          0x08 OTHER
//   value: INT64  -> BE64(u64(v) ^ (1 << 63))                      order preserving
//          UINT64 -> BE64(v)                                        order preserving
//          DOUBLE -> BE64(bits < 0 ? ~bits : bits | (1 << 63))      order preserving; -0.0 folds to 0.0
//          BOOL   -> 0x00 / 0x01
//          STRING / TOKEN -> the raw bytes; a dictionary entry is self-delimiting, so the
//                            value needs neither a terminator nor escaping
//          OTHER  -> nothing: the marker of a document that holds a scalar the codec cannot
//                    spell (decimal, temporal, binary, UUID, NaN, infinities, integers beyond
//                    UINT64), written once per document by the exact index
//
// A term names a value only, never the path it was found under: the index is path-less. Values
// of different types never collide because of the tag, and a STRING term never collides with the
// TOKEN term of the same bytes. Numeric encodings preserve order so that a range would be a
// dictionary interval; nothing scans intervals today.
//
// Versioning: the layout is identified by the `variant_root_format_version` index property that
// the FE stamps at CREATE TABLE. Any change to these bytes bumps that version; a BE that does not
// know a version treats the index as absent instead of misreading it.
//
// The codec is total over its inputs. The canonical value rules (integral doubles fold into
// INT64 / UINT64, NaN and infinities have no term, ...) belong to the caller, see
// variant_leaf_visitor.h.

#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

namespace doris::segment_v2::variant_term_codec {

enum class Tag : uint8_t {
    INT64 = 0x02,
    UINT64 = 0x03,
    DOUBLE = 0x04,
    BOOL = 0x05,
    STRING = 0x06,
    TOKEN = 0x07,
    OTHER = 0x08,
};

inline constexpr size_t TAG_WIDTH = 1;
inline constexpr size_t FIXED64_WIDTH = sizeof(uint64_t);
inline constexpr size_t BOOL_WIDTH = 1;

// ---------------------------------------------------------------------------------------------
// Order preserving bit mappings.
// ---------------------------------------------------------------------------------------------

constexpr uint64_t ordered_int64_bits(int64_t value) {
    return static_cast<uint64_t>(value) ^ (uint64_t {1} << 63);
}

constexpr int64_t int64_from_ordered_bits(uint64_t bits) {
    return static_cast<int64_t>(bits ^ (uint64_t {1} << 63));
}

// Folds -0.0 into 0.0 and every NaN payload into one canonical NaN so equal values always
// produce identical bytes. Callers following the canonical rules never pass NaN.
inline uint64_t ordered_double_bits(double value) {
    if (value == 0.0) {
        value = 0.0;
    } else if (std::isnan(value)) {
        value = std::numeric_limits<double>::quiet_NaN();
    }
    const uint64_t bits = std::bit_cast<uint64_t>(value);
    if ((bits >> 63) != 0) {
        return ~bits;
    }
    return bits | (uint64_t {1} << 63);
}

inline double double_from_ordered_bits(uint64_t bits) {
    if ((bits >> 63) != 0) {
        return std::bit_cast<double>(bits ^ (uint64_t {1} << 63));
    }
    return std::bit_cast<double>(~bits);
}

inline void append_be64(std::string* out, uint64_t bits) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out->push_back(static_cast<char>((bits >> shift) & 0xff));
    }
}

inline uint64_t read_be64(const char* data) {
    uint64_t bits = 0;
    for (size_t i = 0; i < FIXED64_WIDTH; ++i) {
        bits = (bits << 8) | static_cast<uint8_t>(data[i]);
    }
    return bits;
}

// ---------------------------------------------------------------------------------------------
// Terms.
// ---------------------------------------------------------------------------------------------

inline std::string term_int64(int64_t value) {
    std::string out;
    out.reserve(TAG_WIDTH + FIXED64_WIDTH);
    out.push_back(static_cast<char>(Tag::INT64));
    append_be64(&out, ordered_int64_bits(value));
    return out;
}

inline std::string term_uint64(uint64_t value) {
    std::string out;
    out.reserve(TAG_WIDTH + FIXED64_WIDTH);
    out.push_back(static_cast<char>(Tag::UINT64));
    append_be64(&out, value);
    return out;
}

inline std::string term_double(double value) {
    std::string out;
    out.reserve(TAG_WIDTH + FIXED64_WIDTH);
    out.push_back(static_cast<char>(Tag::DOUBLE));
    append_be64(&out, ordered_double_bits(value));
    return out;
}

inline std::string term_bool(bool value) {
    std::string out;
    out.reserve(TAG_WIDTH + BOOL_WIDTH);
    out.push_back(static_cast<char>(Tag::BOOL));
    out.push_back(value ? '\1' : '\0');
    return out;
}

inline std::string term_string(std::string_view value) {
    std::string out;
    out.reserve(TAG_WIDTH + value.size());
    out.push_back(static_cast<char>(Tag::STRING));
    out.append(value);
    return out;
}

inline std::string term_token(std::string_view value) {
    std::string out;
    out.reserve(TAG_WIDTH + value.size());
    out.push_back(static_cast<char>(Tag::TOKEN));
    out.append(value);
    return out;
}

inline std::string term_other() {
    return std::string(1, static_cast<char>(Tag::OTHER));
}

// The bytes an analyzer-produced token receives in front of it: a TOKEN term is
// token_term_prefix() + token, so the SNII writer can shape tokens without materializing them
// through this header.
inline std::string_view token_term_prefix() {
    static constexpr char prefix[] = {static_cast<char>(Tag::TOKEN)};
    return {prefix, 1};
}

// ---------------------------------------------------------------------------------------------
// Decoding. `value` views the input term for STRING / TOKEN and is only valid while the input
// is alive.
// ---------------------------------------------------------------------------------------------

struct DecodedTerm {
    Tag tag = Tag::OTHER;
    int64_t int64_value = 0;
    uint64_t uint64_value = 0;
    double double_value = 0.0;
    bool bool_value = false;
    std::string_view value;
};

inline std::optional<DecodedTerm> decode(std::string_view term) {
    if (term.size() < TAG_WIDTH) {
        return std::nullopt;
    }
    DecodedTerm decoded;
    const auto tag_byte = static_cast<uint8_t>(term[0]);
    const std::string_view rest = term.substr(TAG_WIDTH);
    switch (tag_byte) {
    case static_cast<uint8_t>(Tag::INT64):
    case static_cast<uint8_t>(Tag::UINT64):
    case static_cast<uint8_t>(Tag::DOUBLE): {
        if (rest.size() != FIXED64_WIDTH) {
            return std::nullopt;
        }
        decoded.tag = static_cast<Tag>(tag_byte);
        const uint64_t bits = read_be64(rest.data());
        if (decoded.tag == Tag::INT64) {
            decoded.int64_value = int64_from_ordered_bits(bits);
        } else if (decoded.tag == Tag::UINT64) {
            decoded.uint64_value = bits;
        } else {
            decoded.double_value = double_from_ordered_bits(bits);
        }
        return decoded;
    }
    case static_cast<uint8_t>(Tag::BOOL): {
        if (rest.size() != BOOL_WIDTH || static_cast<uint8_t>(rest[0]) > 1) {
            return std::nullopt;
        }
        decoded.tag = Tag::BOOL;
        decoded.bool_value = rest[0] == '\1';
        return decoded;
    }
    case static_cast<uint8_t>(Tag::STRING):
    case static_cast<uint8_t>(Tag::TOKEN):
        decoded.tag = static_cast<Tag>(tag_byte);
        decoded.value = rest;
        return decoded;
    case static_cast<uint8_t>(Tag::OTHER):
        if (!rest.empty()) {
            return std::nullopt;
        }
        decoded.tag = Tag::OTHER;
        return decoded;
    default:
        return std::nullopt;
    }
}

} // namespace doris::segment_v2::variant_term_codec
