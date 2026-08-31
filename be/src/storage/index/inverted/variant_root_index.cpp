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

#include <bit>
#include <cmath>
#include <limits>
#include <type_traits>
#include <utility>

#include "common/cast_set.h"
#include "core/field.h"
#include "core/value/variant/variant_value.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2::variant_root_index {
namespace {

inline constexpr uint8_t FORMAT_VERSION = 1;

enum class TermTag : uint8_t {
    INT64 = 2,
    UINT64 = 3,
    DOUBLE = 4,
    BOOL = 5,
    STRING = 6,
    TOKEN = 7,
    ALL_VALUE = 8,
    ALL_VALUE_TOKEN = 9,
};

struct StringWriter {
    std::string* value = nullptr;

    void write(const char* data, size_t size) { value->append(data, size); }
};

std::string formatted_scalar_to_string(const variant_json::FormattedScalar& value) {
    return {value.bytes.data(), value.size};
}

std::string term_prefix(std::string_view path, TermTag tag) {
    const auto path_size = cast_set<uint32_t>(path.size());
    std::string result;
    result.reserve(1 + sizeof(uint32_t) + path.size() + 1);
    result.push_back(static_cast<char>(FORMAT_VERSION));
    for (int shift = 24; shift >= 0; shift -= 8) {
        result.push_back(static_cast<char>((path_size >> shift) & 0xff));
    }
    result.append(path);
    result.push_back(static_cast<char>(tag));
    return result;
}

template <typename T>
std::string fixed_width_term(std::string_view path, TermTag tag, T value) {
    static_assert(sizeof(T) == sizeof(uint64_t));
    std::string result = term_prefix(path, tag);
    uint64_t bits;
    if constexpr (std::is_floating_point_v<T>) {
        bits = std::bit_cast<uint64_t>(value);
    } else {
        bits = static_cast<uint64_t>(value);
    }
    for (int shift = static_cast<int>((sizeof(T) - 1) * 8); shift >= 0; shift -= 8) {
        result.push_back(static_cast<char>((bits >> shift) & 0xff));
    }
    return result;
}

void append_signed_numeric_term(std::string_view path, int64_t value,
                                std::vector<std::string>* terms) {
    terms->push_back(encode_int64_term(path, value));
}

void append_unsigned_numeric_term(std::string_view path, uint64_t value,
                                  std::vector<std::string>* terms) {
    if (value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        terms->push_back(encode_int64_term(path, static_cast<int64_t>(value)));
    } else {
        terms->push_back(encode_uint64_term(path, value));
    }
}

void append_floating_numeric_term(std::string_view path, double value,
                                  std::vector<std::string>* terms) {
    if (std::isnan(value)) {
        return;
    }
    // Mixed numeric VARIANT paths are promoted to FLOAT/DOUBLE only when the floating mantissa
    // can represent the integer width losslessly. Canonicalizing integral floating values into
    // the same signed/unsigned domain therefore keeps that cross-type equality searchable with
    // one posting, while paths requiring a lossy promotion remain JSONB and skip root exact.
    if (std::isfinite(value) && std::trunc(value) == value) {
        if (value >= -0x1p63 && value < 0x1p63) {
            append_signed_numeric_term(path, static_cast<int64_t>(value), terms);
            return;
        }
        if (value >= 0x1p63 && value < 0x1p64) {
            append_unsigned_numeric_term(path, static_cast<uint64_t>(value), terms);
            return;
        }
    }
    terms->push_back(encode_double_term(path, value));
}

std::string_view query_value_family_impl(PrimitiveType type) {
    if (is_string_type(type)) {
        return "string";
    }
    if (type == PrimitiveType::TYPE_BOOLEAN) {
        return "boolean";
    }
    switch (type) {
    case PrimitiveType::TYPE_TINYINT:
    case PrimitiveType::TYPE_SMALLINT:
    case PrimitiveType::TYPE_INT:
    case PrimitiveType::TYPE_BIGINT:
    case PrimitiveType::TYPE_UINT32:
    case PrimitiveType::TYPE_UINT64:
        return "integral";
    case PrimitiveType::TYPE_FLOAT:
    case PrimitiveType::TYPE_DOUBLE:
        return "floating";
    default:
        return {};
    }
}

} // namespace

std::string_view query_value_family(PrimitiveType type) {
    return query_value_family_impl(type);
}

bool is_root_mode_properties(const std::map<std::string, std::string>& properties) {
    return is_path_root_mode_properties(properties) || is_all_values_mode_properties(properties);
}

bool is_path_root_mode_properties(const std::map<std::string, std::string>& properties) {
    const auto mode = properties.find(std::string(VARIANT_INDEX_MODE_KEY));
    const auto version = properties.find(std::string(VARIANT_ROOT_FORMAT_VERSION_KEY));
    return mode != properties.end() && mode->second == VARIANT_INDEX_MODE_ROOT &&
           version != properties.end() && version->second == VARIANT_ROOT_FORMAT_VERSION_V1;
}

bool is_all_values_mode_properties(const std::map<std::string, std::string>& properties) {
    const auto mode = properties.find(std::string(VARIANT_INDEX_MODE_KEY));
    const auto version = properties.find(std::string(VARIANT_ROOT_FORMAT_VERSION_KEY));
    return mode != properties.end() && mode->second == VARIANT_INDEX_MODE_ALL_VALUES &&
           version != properties.end() && version->second == VARIANT_ROOT_FORMAT_VERSION_V1;
}

bool is_root_index(const TabletIndex& index) {
    return index.is_inverted_index() && is_root_mode_properties(index.properties());
}

bool is_all_values_index(const TabletIndex& index) {
    return index.is_inverted_index() && is_all_values_mode_properties(index.properties());
}

std::string encode_int64_term(std::string_view path, int64_t value) {
    return fixed_width_term(path, TermTag::INT64, value);
}

std::string encode_uint64_term(std::string_view path, uint64_t value) {
    return fixed_width_term(path, TermTag::UINT64, value);
}

std::string encode_double_term(std::string_view path, double value) {
    if (value == 0) {
        value = 0;
    }
    return fixed_width_term(path, TermTag::DOUBLE, value);
}

std::string encode_bool_term(std::string_view path, bool value) {
    std::string result = term_prefix(path, TermTag::BOOL);
    result.push_back(static_cast<char>(value ? 1 : 0));
    return result;
}

std::string encode_string_term(std::string_view path, std::string_view value) {
    std::string result = term_prefix(path, TermTag::STRING);
    result.append(value);
    return result;
}

std::string encode_token_term(std::string_view path, std::string_view value) {
    std::string result = term_prefix(path, TermTag::TOKEN);
    result.append(value);
    return result;
}

std::string encode_all_value_term(std::string_view value) {
    std::string result = term_prefix({}, TermTag::ALL_VALUE);
    result.append(value);
    return result;
}

std::string encode_all_value_token_term(std::string_view value) {
    std::string result = term_prefix({}, TermTag::ALL_VALUE_TOKEN);
    result.append(value);
    return result;
}

Status serialize_all_value(const VariantRef& value, std::string* serialized) {
    DORIS_CHECK(serialized != nullptr);
    serialized->clear();
    if (value.is_null()) {
        return Status::OK();
    }
    try {
        StringWriter writer {.value = serialized};
        VariantJsonFormatOptions options;
        to_sql_string(value, writer, options);
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

Status append_variant_value_terms(std::string_view path, const VariantRef& value,
                                  std::vector<std::string>* terms) {
    DORIS_CHECK(terms != nullptr);
    try {
        if (value.basic_type() == VariantBasicType::SHORT_STRING) {
            const StringRef string = value.get_string();
            terms->push_back(encode_string_term(path, string.to_string_view()));
            return Status::OK();
        }
        if (value.basic_type() != VariantBasicType::PRIMITIVE || value.is_null()) {
            return Status::OK();
        }
        switch (value.primitive_id()) {
        case VariantPrimitiveId::TRUE_VALUE:
        case VariantPrimitiveId::FALSE_VALUE:
            terms->push_back(encode_bool_term(path, value.get_bool()));
            break;
        case VariantPrimitiveId::INT8:
        case VariantPrimitiveId::INT16:
        case VariantPrimitiveId::INT32:
        case VariantPrimitiveId::INT64:
            append_signed_numeric_term(path, value.get_int(), terms);
            break;
        case VariantPrimitiveId::FLOAT:
            append_floating_numeric_term(path, static_cast<double>(value.get_float()), terms);
            break;
        case VariantPrimitiveId::DOUBLE:
            append_floating_numeric_term(path, value.get_double(), terms);
            break;
        case VariantPrimitiveId::STRING: {
            const StringRef string = value.get_string();
            terms->push_back(encode_string_term(path, string.to_string_view()));
            break;
        }
        default:
            break;
        }
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

Status encode_query_value_terms(std::string_view path, const Field& value,
                                std::vector<std::string>* terms) {
    DORIS_CHECK(terms != nullptr);
    switch (value.get_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        terms->push_back(encode_bool_term(path, value.get<PrimitiveType::TYPE_BOOLEAN>()));
        break;
    case PrimitiveType::TYPE_TINYINT:
        append_signed_numeric_term(path, value.get<PrimitiveType::TYPE_TINYINT>(), terms);
        break;
    case PrimitiveType::TYPE_SMALLINT:
        append_signed_numeric_term(path, value.get<PrimitiveType::TYPE_SMALLINT>(), terms);
        break;
    case PrimitiveType::TYPE_INT:
        append_signed_numeric_term(path, value.get<PrimitiveType::TYPE_INT>(), terms);
        break;
    case PrimitiveType::TYPE_BIGINT:
        append_signed_numeric_term(path, value.get<PrimitiveType::TYPE_BIGINT>(), terms);
        break;
    case PrimitiveType::TYPE_UINT32:
        append_unsigned_numeric_term(path, value.get<PrimitiveType::TYPE_UINT32>(), terms);
        break;
    case PrimitiveType::TYPE_UINT64:
        append_unsigned_numeric_term(path, value.get<PrimitiveType::TYPE_UINT64>(), terms);
        break;
    case PrimitiveType::TYPE_FLOAT:
        append_floating_numeric_term(
                path, static_cast<double>(value.get<PrimitiveType::TYPE_FLOAT>()), terms);
        break;
    case PrimitiveType::TYPE_DOUBLE:
        append_floating_numeric_term(path, value.get<PrimitiveType::TYPE_DOUBLE>(), terms);
        break;
    case PrimitiveType::TYPE_CHAR:
        terms->push_back(encode_string_term(path, value.get<PrimitiveType::TYPE_CHAR>()));
        break;
    case PrimitiveType::TYPE_VARCHAR:
        terms->push_back(encode_string_term(path, value.get<PrimitiveType::TYPE_VARCHAR>()));
        break;
    case PrimitiveType::TYPE_STRING:
        terms->push_back(encode_string_term(path, value.get<PrimitiveType::TYPE_STRING>()));
        break;
    default:
        break;
    }
    return Status::OK();
}

Status serialize_all_values_query_value(const Field& value, std::string* serialized) {
    DORIS_CHECK(serialized != nullptr);
    serialized->clear();
    switch (value.get_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        *serialized = value.get<PrimitiveType::TYPE_BOOLEAN>() ? "true" : "false";
        break;
    case PrimitiveType::TYPE_TINYINT:
        *serialized = formatted_scalar_to_string(
                variant_json::format_json_int(value.get<PrimitiveType::TYPE_TINYINT>()));
        break;
    case PrimitiveType::TYPE_SMALLINT:
        *serialized = formatted_scalar_to_string(
                variant_json::format_json_int(value.get<PrimitiveType::TYPE_SMALLINT>()));
        break;
    case PrimitiveType::TYPE_INT:
        *serialized = formatted_scalar_to_string(
                variant_json::format_json_int(value.get<PrimitiveType::TYPE_INT>()));
        break;
    case PrimitiveType::TYPE_BIGINT:
        *serialized = formatted_scalar_to_string(
                variant_json::format_json_int(value.get<PrimitiveType::TYPE_BIGINT>()));
        break;
    case PrimitiveType::TYPE_UINT32:
        *serialized = formatted_scalar_to_string(
                variant_json::format_json_int(value.get<PrimitiveType::TYPE_UINT32>()));
        break;
    case PrimitiveType::TYPE_UINT64: {
        const uint64_t unsigned_value = value.get<PrimitiveType::TYPE_UINT64>();
        if (unsigned_value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
            return Status::OK();
        }
        *serialized = formatted_scalar_to_string(
                variant_json::format_json_int(static_cast<int64_t>(unsigned_value)));
        break;
    }
    case PrimitiveType::TYPE_FLOAT: {
        const float float_value = value.get<PrimitiveType::TYPE_FLOAT>();
        if (!std::isfinite(float_value)) {
            return Status::OK();
        }
        *serialized = formatted_scalar_to_string(variant_json::format_json_float(float_value));
        break;
    }
    case PrimitiveType::TYPE_DOUBLE: {
        const double double_value = value.get<PrimitiveType::TYPE_DOUBLE>();
        if (!std::isfinite(double_value)) {
            return Status::OK();
        }
        *serialized = formatted_scalar_to_string(variant_json::format_json_double(double_value));
        break;
    }
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
        *serialized = std::string(value.as_string_view());
        break;
    default:
        return Status::OK();
    }
    return Status::OK();
}

Status encode_all_values_query_value_terms(const Field& value, std::vector<std::string>* terms) {
    DORIS_CHECK(terms != nullptr);
    std::string serialized;
    RETURN_IF_ERROR(serialize_all_values_query_value(value, &serialized));
    if (!serialized.empty() ||
        (is_string_type(value.get_type()) && value.as_string_view().empty())) {
        terms->push_back(encode_all_value_term(serialized));
    }
    return Status::OK();
}

std::shared_ptr<TabletIndex> make_query_index(const TabletIndex& root_index,
                                              std::string_view relative_path,
                                              PrimitiveType path_type) {
    const std::string_view family = query_value_family(path_type);
    DORIS_CHECK(!family.empty() ||
                (path_type == PrimitiveType::TYPE_VARIANT && is_all_values_index(root_index)));
    TabletIndexPB index_pb;
    root_index.to_schema_pb(&index_pb);
    (*index_pb.mutable_properties())[std::string(VARIANT_ROOT_QUERY_PATH_KEY)] = relative_path;
    if (!family.empty()) {
        (*index_pb.mutable_properties())[std::string(VARIANT_ROOT_QUERY_VALUE_FAMILY_KEY)] = family;
    }
    auto result = std::make_shared<TabletIndex>();
    result->init_from_pb(index_pb);
    return result;
}

} // namespace doris::segment_v2::variant_root_index
