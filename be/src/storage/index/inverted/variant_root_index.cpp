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

#include <charconv>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/field.h"
#include "core/value/variant/variant_leaf_visitor.h"
#include "core/value/variant/variant_value.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "gen_cpp/olap_file.pb.h"
#include "storage/index/inverted/variant_term_codec.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2::variant_root_index {
namespace {

std::string_view trim(std::string_view text) {
    while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
        text.remove_prefix(1);
    }
    while (!text.empty() && (text.back() == ' ' || text.back() == '\t')) {
        text.remove_suffix(1);
    }
    return text;
}

std::string formatted_scalar_to_string(const variant_json::FormattedScalar& value) {
    return {value.bytes.data(), value.size};
}

std::string canonical_number_text(const VariantCanonicalNumber& number) {
    switch (number.kind) {
    case VariantLeafKind::INT64:
        return formatted_scalar_to_string(variant_json::format_json_int(number.int64_value));
    case VariantLeafKind::UINT64:
        return std::to_string(number.uint64_value);
    case VariantLeafKind::DOUBLE:
        return formatted_scalar_to_string(variant_json::format_json_double(number.double_value));
    default:
        return {};
    }
}

void append_canonical_number(const std::optional<VariantCanonicalNumber>& number,
                             std::vector<std::string>* terms) {
    if (!number.has_value()) {
        return; // NaN: no term
    }
    switch (number->kind) {
    case VariantLeafKind::INT64:
        terms->push_back(encode_int64_term(number->int64_value));
        break;
    case VariantLeafKind::UINT64:
        terms->push_back(encode_uint64_term(number->uint64_value));
        break;
    case VariantLeafKind::DOUBLE:
        terms->push_back(encode_double_term(number->double_value));
        break;
    default:
        break;
    }
}

// The canonical number spelled by `text`, if any: the text is parsed leniently and accepted only
// when the canonical text of the folded number reproduces it byte for byte, so "42", "-5",
// "42.7" and "1e+300" are numbers while "042", "+42", "42.0" and " 42" are only strings.
std::optional<VariantCanonicalNumber> canonical_number_of_text(std::string_view text) {
    if (text.empty() || text.size() > 64) {
        return std::nullopt;
    }
    int64_t signed_value = 0;
    auto [signed_end, signed_error] =
            std::from_chars(text.data(), text.data() + text.size(), signed_value);
    if (signed_error == std::errc() && signed_end == text.data() + text.size()) {
        const auto number = canonical_numeric_from_int64(signed_value);
        if (canonical_number_text(*number) == text) {
            return number;
        }
        return std::nullopt;
    }
    uint64_t unsigned_value = 0;
    auto [unsigned_end, unsigned_error] =
            std::from_chars(text.data(), text.data() + text.size(), unsigned_value);
    if (unsigned_error == std::errc() && unsigned_end == text.data() + text.size()) {
        const auto number = canonical_numeric_from_uint64(unsigned_value);
        if (canonical_number_text(*number) == text) {
            return number;
        }
        return std::nullopt;
    }
    const std::string owned(text);
    char* end = nullptr;
    const double value = std::strtod(owned.c_str(), &end);
    if (end != owned.c_str() + owned.size() || !std::isfinite(value)) {
        return std::nullopt;
    }
    const auto number = canonical_numeric_from_double(value);
    if (number.has_value() && canonical_number_text(*number) == text) {
        return number;
    }
    return std::nullopt;
}

bool has_current_format_version(const std::map<std::string, std::string>& properties) {
    const auto version = properties.find(std::string(VARIANT_ROOT_FORMAT_VERSION_KEY));
    return version != properties.end() && version->second == VARIANT_ROOT_FORMAT_VERSION_CURRENT;
}

bool has_values_scope(const std::map<std::string, std::string>& properties) {
    if (const auto scope = properties.find(std::string(VARIANT_INDEX_SCOPE_KEY));
        scope != properties.end()) {
        return trim(scope->second) == VARIANT_INDEX_SCOPE_VALUES;
    }
    const auto mode = properties.find(std::string(VARIANT_INDEX_MODE_KEY));
    return mode != properties.end() && trim(mode->second) == VARIANT_INDEX_MODE_ALL_VALUES;
}

} // namespace

std::string_view query_value_family(PrimitiveType type) {
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
        return "float";
    case PrimitiveType::TYPE_DOUBLE:
        return "double";
    default:
        return {};
    }
}

bool is_root_mode_properties(const std::map<std::string, std::string>& properties) {
    return has_values_scope(properties) && has_current_format_version(properties);
}

bool is_root_index(const TabletIndex& index) {
    return index.is_inverted_index() && is_root_mode_properties(index.properties());
}

std::string encode_int64_term(int64_t value) {
    return variant_term_codec::root_prefix_int64(value);
}

std::string encode_uint64_term(uint64_t value) {
    return variant_term_codec::root_prefix_uint64(value);
}

std::string encode_double_term(double value) {
    return variant_term_codec::root_prefix_double(value);
}

std::string encode_bool_term(bool value) {
    return variant_term_codec::root_prefix_bool(value);
}

std::string encode_string_term(std::string_view value) {
    return variant_term_codec::root_prefix_string(value);
}

std::string encode_token_term(std::string_view value) {
    return variant_term_codec::root_prefix_token(value);
}

std::string encode_other_term() {
    return variant_term_codec::root_prefix_other();
}

void append_variant_leaf_terms(const VariantLeaf& leaf, std::vector<std::string>* terms) {
    DORIS_CHECK(terms != nullptr);
    switch (leaf.kind) {
    case VariantLeafKind::STRING:
        terms->push_back(encode_string_term({leaf.string_value.data, leaf.string_value.size}));
        break;
    case VariantLeafKind::INT64:
        terms->push_back(encode_int64_term(leaf.int64_value));
        break;
    case VariantLeafKind::UINT64:
        terms->push_back(encode_uint64_term(leaf.uint64_value));
        break;
    case VariantLeafKind::DOUBLE:
        terms->push_back(encode_double_term(leaf.double_value));
        break;
    case VariantLeafKind::BOOL:
        terms->push_back(encode_bool_term(leaf.bool_value));
        break;
    case VariantLeafKind::OTHER:
        break;
    }
}

bool canonical_leaf_text(const VariantLeaf& leaf, std::string* text) {
    DORIS_CHECK(text != nullptr);
    switch (leaf.kind) {
    case VariantLeafKind::STRING:
        text->assign(leaf.string_value.data, leaf.string_value.size);
        return true;
    case VariantLeafKind::INT64:
        *text = formatted_scalar_to_string(variant_json::format_json_int(leaf.int64_value));
        return true;
    case VariantLeafKind::UINT64:
        *text = std::to_string(leaf.uint64_value);
        return true;
    case VariantLeafKind::DOUBLE:
        *text = formatted_scalar_to_string(variant_json::format_json_double(leaf.double_value));
        return true;
    case VariantLeafKind::BOOL:
        *text = leaf.bool_value ? "true" : "false";
        return true;
    case VariantLeafKind::OTHER:
        return false;
    }
    return false;
}

void append_string_literal_terms(std::string_view text, bool sql_cast_text,
                                 std::vector<std::string>* terms) {
    DORIS_CHECK(terms != nullptr);
    terms->push_back(encode_string_term(text));
    append_canonical_number(canonical_number_of_text(text), terms);
    if (text == "true") {
        terms->push_back(encode_bool_term(true));
    } else if (text == "false") {
        terms->push_back(encode_bool_term(false));
    }
    if (!sql_cast_text) {
        return;
    }
    // CAST(leaf AS STRING) spells booleans as 1 / 0 and negative zero as -0; the index folds
    // -0.0 into the integer 0.
    if (text == "1") {
        terms->push_back(encode_bool_term(true));
    } else if (text == "0") {
        terms->push_back(encode_bool_term(false));
    } else if (text == "-0") {
        terms->push_back(encode_int64_term(0));
    }
}

Status encode_query_value_terms(const Field& value, std::vector<std::string>* terms) {
    DORIS_CHECK(terms != nullptr);
    switch (value.get_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        terms->push_back(encode_bool_term(value.get<PrimitiveType::TYPE_BOOLEAN>()));
        break;
    case PrimitiveType::TYPE_TINYINT:
        append_canonical_number(
                canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_TINYINT>()), terms);
        break;
    case PrimitiveType::TYPE_SMALLINT:
        append_canonical_number(
                canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_SMALLINT>()), terms);
        break;
    case PrimitiveType::TYPE_INT:
        append_canonical_number(canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_INT>()),
                                terms);
        break;
    case PrimitiveType::TYPE_BIGINT:
        append_canonical_number(
                canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_BIGINT>()), terms);
        break;
    case PrimitiveType::TYPE_UINT32:
        append_canonical_number(
                canonical_numeric_from_uint64(value.get<PrimitiveType::TYPE_UINT32>()), terms);
        break;
    case PrimitiveType::TYPE_UINT64:
        append_canonical_number(
                canonical_numeric_from_uint64(value.get<PrimitiveType::TYPE_UINT64>()), terms);
        break;
    case PrimitiveType::TYPE_FLOAT:
        append_canonical_number(canonical_numeric_from_double(static_cast<double>(
                                        value.get<PrimitiveType::TYPE_FLOAT>())),
                                terms);
        break;
    case PrimitiveType::TYPE_DOUBLE:
        append_canonical_number(
                canonical_numeric_from_double(value.get<PrimitiveType::TYPE_DOUBLE>()), terms);
        break;
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
        terms->push_back(encode_string_term(value.as_string_view()));
        break;
    default:
        break;
    }
    return Status::OK();
}

std::shared_ptr<TabletIndex> make_query_index(const TabletIndex& root_index,
                                              std::string_view relative_path,
                                              PrimitiveType path_type) {
    const std::string_view family = query_value_family(path_type);
    DORIS_CHECK(!family.empty() || path_type == PrimitiveType::TYPE_VARIANT);
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
