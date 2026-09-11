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

// Term codec for the value-first VARIANT inverted index.
//
// This header is intentionally self-contained (standard library only) so the term layout can be
// unit-tested and reasoned about in isolation from the rest of the storage engine. Every byte
// that reaches the SNII dictionary for a VARIANT index is produced here.
//
// Layout
//
//   term        := tag(1 byte) value (sep)? path
//   root_prefix := tag(1 byte) value (sep)?
//
//   tag:   0x02 INT64 | 0x03 UINT64 | 0x04 DOUBLE | 0x05 BOOL | 0x06 STRING | 0x07 TOKEN
//   value: INT64  -> BE64(u64(v) ^ (1 << 63))                      order preserving
//          UINT64 -> BE64(v)                                        order preserving
//          DOUBLE -> BE64(bits < 0 ? ~bits : bits | (1 << 63))      order preserving; -0.0 folds to 0.0
//          BOOL   -> 0x00 / 0x01
//          STRING / TOKEN -> escape(v) 0x00 0x00                    escape: 0x00 -> 0x00 0x01
//   sep:   only variable-width values (STRING / TOKEN) carry the 0x00 0x00 terminator; fixed-width
//          values derive their width from the tag.
//   path:  raw UTF-8, never escaped (it is a suffix and never participates in prefix matching).
//
// Value-first ordering means that all terms sharing one value form a contiguous run in the
// dictionary: an exact path lookup is a single term, a root ("any path") lookup is a seek to
// root_prefix followed by a sequential read while the prefix still matches. Order preserving
// numeric encodings additionally make `[tag][lo] .. [tag][hi]` a dictionary interval.
//
// The codec is total over its inputs but the canonical value rules (integral doubles fold into
// INT64 / UINT64, NaN produces no term, values above 2^64 produce no term) belong to the caller.

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
};

inline constexpr size_t TAG_WIDTH = 1;
inline constexpr size_t FIXED64_WIDTH = sizeof(uint64_t);
inline constexpr size_t BOOL_WIDTH = 1;
inline constexpr size_t STRING_TERMINATOR_WIDTH = 2;

inline constexpr char ESCAPE_BYTE = '\0';
inline constexpr char ESCAPED_NUL_SUFFIX = '\1';
inline constexpr char TERMINATOR_SUFFIX = '\0';

// ---------------------------------------------------------------------------------------------
// Order preserving bit mappings. Exposed so other term layouts can share one numeric encoding.
// ---------------------------------------------------------------------------------------------

constexpr uint64_t ordered_int64_bits(int64_t value) {
    return static_cast<uint64_t>(value) ^ (uint64_t {1} << 63);
}

constexpr int64_t int64_from_ordered_bits(uint64_t bits) {
    return static_cast<int64_t>(bits ^ (uint64_t {1} << 63));
}

constexpr uint64_t ordered_uint64_bits(uint64_t value) {
    return value;
}

constexpr uint64_t uint64_from_ordered_bits(uint64_t bits) {
    return bits;
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

// Appends escape(value) followed by the 0x00 0x00 terminator.
inline void append_escaped_value(std::string* out, std::string_view value) {
    out->reserve(out->size() + value.size() + STRING_TERMINATOR_WIDTH);
    for (const char c : value) {
        out->push_back(c);
        if (c == ESCAPE_BYTE) {
            out->push_back(ESCAPED_NUL_SUFFIX);
        }
    }
    out->push_back(ESCAPE_BYTE);
    out->push_back(TERMINATOR_SUFFIX);
}

// ---------------------------------------------------------------------------------------------
// Root prefixes: tag + encoded value. Also the exact term for the "values" scope (no path).
// ---------------------------------------------------------------------------------------------

inline std::string root_prefix_int64(int64_t value) {
    std::string out;
    out.reserve(TAG_WIDTH + FIXED64_WIDTH);
    out.push_back(static_cast<char>(Tag::INT64));
    append_be64(&out, ordered_int64_bits(value));
    return out;
}

inline std::string root_prefix_uint64(uint64_t value) {
    std::string out;
    out.reserve(TAG_WIDTH + FIXED64_WIDTH);
    out.push_back(static_cast<char>(Tag::UINT64));
    append_be64(&out, ordered_uint64_bits(value));
    return out;
}

inline std::string root_prefix_double(double value) {
    std::string out;
    out.reserve(TAG_WIDTH + FIXED64_WIDTH);
    out.push_back(static_cast<char>(Tag::DOUBLE));
    append_be64(&out, ordered_double_bits(value));
    return out;
}

inline std::string root_prefix_bool(bool value) {
    std::string out;
    out.reserve(TAG_WIDTH + BOOL_WIDTH);
    out.push_back(static_cast<char>(Tag::BOOL));
    out.push_back(value ? '\1' : '\0');
    return out;
}

inline std::string root_prefix_string(std::string_view value) {
    std::string out;
    out.reserve(TAG_WIDTH + value.size() + STRING_TERMINATOR_WIDTH);
    out.push_back(static_cast<char>(Tag::STRING));
    append_escaped_value(&out, value);
    return out;
}

inline std::string root_prefix_token(std::string_view value) {
    std::string out;
    out.reserve(TAG_WIDTH + value.size() + STRING_TERMINATOR_WIDTH);
    out.push_back(static_cast<char>(Tag::TOKEN));
    append_escaped_value(&out, value);
    return out;
}

// ---------------------------------------------------------------------------------------------
// Full terms: root prefix + path.
// ---------------------------------------------------------------------------------------------

inline std::string term_int64(int64_t value, std::string_view path) {
    std::string out = root_prefix_int64(value);
    out.append(path);
    return out;
}

inline std::string term_uint64(uint64_t value, std::string_view path) {
    std::string out = root_prefix_uint64(value);
    out.append(path);
    return out;
}

inline std::string term_double(double value, std::string_view path) {
    std::string out = root_prefix_double(value);
    out.append(path);
    return out;
}

inline std::string term_bool(bool value, std::string_view path) {
    std::string out = root_prefix_bool(value);
    out.append(path);
    return out;
}

inline std::string term_string(std::string_view value, std::string_view path) {
    std::string out;
    out.reserve(TAG_WIDTH + value.size() + STRING_TERMINATOR_WIDTH + path.size());
    out.push_back(static_cast<char>(Tag::STRING));
    append_escaped_value(&out, value);
    out.append(path);
    return out;
}

inline std::string term_token(std::string_view value, std::string_view path) {
    std::string out;
    out.reserve(TAG_WIDTH + value.size() + STRING_TERMINATOR_WIDTH + path.size());
    out.push_back(static_cast<char>(Tag::TOKEN));
    append_escaped_value(&out, value);
    out.append(path);
    return out;
}

// Smallest term that sorts after every term carrying `prefix`. Empty when no such term exists
// (the prefix is all 0xff bytes), in which case the scan runs to the end of the dictionary.
inline std::string prefix_upper_bound(std::string_view prefix) {
    std::string out(prefix);
    while (!out.empty()) {
        const auto last = static_cast<uint8_t>(out.back());
        if (last != 0xff) {
            out.back() = static_cast<char>(last + 1);
            return out;
        }
        out.pop_back();
    }
    return out;
}

inline bool has_root_prefix(std::string_view term, std::string_view root_prefix) {
    return term.size() >= root_prefix.size() &&
           std::memcmp(term.data(), root_prefix.data(), root_prefix.size()) == 0;
}

// ---------------------------------------------------------------------------------------------
// Decoding. `path` is a view into the input term and is only valid while the input is alive;
// `string_value` is the unescaped value for STRING / TOKEN terms.
// ---------------------------------------------------------------------------------------------

struct DecodedTerm {
    Tag tag = Tag::STRING;
    int64_t int64_value = 0;
    uint64_t uint64_value = 0;
    double double_value = 0.0;
    bool bool_value = false;
    std::string string_value;
    std::string_view path;
};

inline std::optional<DecodedTerm> decode(std::string_view term) {
    if (term.size() < TAG_WIDTH) {
        return std::nullopt;
    }
    DecodedTerm decoded;
    const auto tag_byte = static_cast<uint8_t>(term[0]);
    std::string_view rest = term.substr(TAG_WIDTH);
    switch (tag_byte) {
    case static_cast<uint8_t>(Tag::INT64):
    case static_cast<uint8_t>(Tag::UINT64):
    case static_cast<uint8_t>(Tag::DOUBLE): {
        if (rest.size() < FIXED64_WIDTH) {
            return std::nullopt;
        }
        decoded.tag = static_cast<Tag>(tag_byte);
        const uint64_t bits = read_be64(rest.data());
        if (decoded.tag == Tag::INT64) {
            decoded.int64_value = int64_from_ordered_bits(bits);
        } else if (decoded.tag == Tag::UINT64) {
            decoded.uint64_value = uint64_from_ordered_bits(bits);
        } else {
            decoded.double_value = double_from_ordered_bits(bits);
        }
        decoded.path = rest.substr(FIXED64_WIDTH);
        return decoded;
    }
    case static_cast<uint8_t>(Tag::BOOL): {
        if (rest.size() < BOOL_WIDTH) {
            return std::nullopt;
        }
        const auto flag = static_cast<uint8_t>(rest[0]);
        if (flag > 1) {
            return std::nullopt;
        }
        decoded.tag = Tag::BOOL;
        decoded.bool_value = flag == 1;
        decoded.path = rest.substr(BOOL_WIDTH);
        return decoded;
    }
    case static_cast<uint8_t>(Tag::STRING):
    case static_cast<uint8_t>(Tag::TOKEN): {
        decoded.tag = static_cast<Tag>(tag_byte);
        size_t i = 0;
        while (true) {
            if (i >= rest.size()) {
                return std::nullopt; // missing terminator
            }
            const char c = rest[i];
            if (c != ESCAPE_BYTE) {
                decoded.string_value.push_back(c);
                ++i;
                continue;
            }
            if (i + 1 >= rest.size()) {
                return std::nullopt; // dangling escape byte
            }
            const char next = rest[i + 1];
            if (next == ESCAPED_NUL_SUFFIX) {
                decoded.string_value.push_back(ESCAPE_BYTE);
                i += 2;
                continue;
            }
            if (next == TERMINATOR_SUFFIX) {
                i += 2;
                break;
            }
            return std::nullopt; // invalid escape sequence
        }
        decoded.path = rest.substr(i);
        return decoded;
    }
    default:
        return std::nullopt;
    }
}

} // namespace doris::segment_v2::variant_term_codec
