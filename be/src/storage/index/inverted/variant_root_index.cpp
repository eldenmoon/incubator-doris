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

#include <fmt/compile.h>
#include <fmt/format.h>

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
#include "gen_cpp/olap_file.pb.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/variant_term_codec.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2::variant_root_index {
namespace {

// Query literals longer than this never spell a number.
constexpr size_t MAX_NUMBER_TEXT = 64;

std::string_view trim(std::string_view text) {
    while (!text.empty() && (text.front() == ' ' || text.front() == '\t')) {
        text.remove_prefix(1);
    }
    while (!text.empty() && (text.back() == ' ' || text.back() == '\t')) {
        text.remove_suffix(1);
    }
    return text;
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

// The canonical text of a number (contract 2): integers in decimal, doubles in the shortest
// form that reads back to the same value.
std::string canonical_number_text(const VariantCanonicalNumber& number) {
    switch (number.kind) {
    case VariantLeafKind::INT64:
        return fmt::format(FMT_COMPILE("{}"), number.int64_value);
    case VariantLeafKind::UINT64:
        return fmt::format(FMT_COMPILE("{}"), number.uint64_value);
    case VariantLeafKind::DOUBLE:
        return fmt::format(FMT_COMPILE("{}"), number.double_value);
    default:
        return {};
    }
}

std::string number_term(const VariantCanonicalNumber& number) {
    switch (number.kind) {
    case VariantLeafKind::INT64:
        return variant_term_codec::term_int64(number.int64_value);
    case VariantLeafKind::UINT64:
        return variant_term_codec::term_uint64(number.uint64_value);
    case VariantLeafKind::DOUBLE:
        return variant_term_codec::term_double(number.double_value);
    default:
        return {};
    }
}

void append_number_term(const std::optional<VariantCanonicalNumber>& number,
                        std::vector<std::string>* terms) {
    if (number.has_value()) {
        terms->push_back(number_term(*number));
    }
}

// `text` as an integer, when every byte of it is one: "42", "-5", "18446744073709551615".
std::optional<VariantCanonicalNumber> integer_of_text(std::string_view text) {
    int64_t signed_value = 0;
    if (auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), signed_value);
        error == std::errc() && end == text.data() + text.size()) {
        return canonical_numeric_from_int64(signed_value);
    }
    uint64_t unsigned_value = 0;
    if (auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), unsigned_value);
        error == std::errc() && end == text.data() + text.size()) {
        return canonical_numeric_from_uint64(unsigned_value);
    }
    return std::nullopt;
}

// `text` as a finite double, when every byte of it is one.
std::optional<double> double_of_text(std::string_view text) {
    const std::string owned(text);
    char* end = nullptr;
    const double value = std::strtod(owned.c_str(), &end);
    if (end != owned.c_str() + owned.size() || !std::isfinite(value)) {
        return std::nullopt;
    }
    return value;
}

} // namespace

bool is_root_index(const std::map<std::string, std::string>& properties) {
    return has_values_scope(properties) && has_current_format_version(properties);
}

bool is_root_index(const TabletIndex& index) {
    return index.is_inverted_index() && is_root_index(index.properties());
}

InvertedIndexReaderType reader_type(const TabletIndex& index) {
    return inverted_index::InvertedIndexAnalyzer::should_analyzer(index.properties())
                   ? InvertedIndexReaderType::FULLTEXT
                   : InvertedIndexReaderType::STRING_TYPE;
}

std::string_view value_family(PrimitiveType type) {
    if (is_string_type(type)) {
        return "string";
    }
    switch (type) {
    case PrimitiveType::TYPE_BOOLEAN:
        return "boolean";
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

QueryBinding query_binding(const std::map<std::string, std::string>& properties) {
    QueryBinding binding;
    const auto path = properties.find(std::string(VARIANT_ROOT_QUERY_PATH_KEY));
    if (path == properties.end()) {
        return binding;
    }
    binding.path_bound = true;
    binding.path = path->second;
    if (const auto family = properties.find(std::string(VARIANT_ROOT_QUERY_VALUE_FAMILY_KEY));
        family != properties.end()) {
        binding.family = family->second;
    }
    return binding;
}

bool yields_candidates(const std::map<std::string, std::string>& properties) {
    return is_root_index(properties) && query_binding(properties).path_bound;
}

std::shared_ptr<TabletIndex> bind_to_path(const TabletIndex& root_index,
                                          std::string_view relative_path, PrimitiveType path_type) {
    DORIS_CHECK(is_root_index(root_index));
    const std::string_view family = value_family(path_type);
    DORIS_CHECK(!family.empty() || path_type == PrimitiveType::TYPE_VARIANT);
    TabletIndexPB index_pb;
    root_index.to_schema_pb(&index_pb);
    (*index_pb.mutable_properties())[std::string(VARIANT_ROOT_QUERY_PATH_KEY)] = relative_path;
    if (!family.empty()) {
        (*index_pb.mutable_properties())[std::string(VARIANT_ROOT_QUERY_VALUE_FAMILY_KEY)] = family;
    }
    auto bound = std::make_shared<TabletIndex>();
    bound->init_from_pb(index_pb);
    return bound;
}

std::string leaf_term(const VariantLeaf& leaf) {
    switch (leaf.kind) {
    case VariantLeafKind::STRING:
        return variant_term_codec::term_string({leaf.string_value.data, leaf.string_value.size});
    case VariantLeafKind::INT64:
        return variant_term_codec::term_int64(leaf.int64_value);
    case VariantLeafKind::UINT64:
        return variant_term_codec::term_uint64(leaf.uint64_value);
    case VariantLeafKind::DOUBLE:
        return variant_term_codec::term_double(leaf.double_value);
    case VariantLeafKind::BOOL:
        return variant_term_codec::term_bool(leaf.bool_value);
    case VariantLeafKind::OTHER:
        return {};
    }
    return {};
}

std::string unspellable_marker_term() {
    return variant_term_codec::term_other();
}

bool canonical_leaf_text(const VariantLeaf& leaf, std::string* text) {
    DORIS_CHECK(text != nullptr);
    switch (leaf.kind) {
    case VariantLeafKind::STRING:
        text->assign(leaf.string_value.data, leaf.string_value.size);
        return true;
    case VariantLeafKind::INT64:
        *text = fmt::format(FMT_COMPILE("{}"), leaf.int64_value);
        return true;
    case VariantLeafKind::UINT64:
        *text = fmt::format(FMT_COMPILE("{}"), leaf.uint64_value);
        return true;
    case VariantLeafKind::DOUBLE:
        *text = fmt::format(FMT_COMPILE("{}"), leaf.double_value);
        return true;
    case VariantLeafKind::BOOL:
        *text = leaf.bool_value ? "true" : "false";
        return true;
    case VariantLeafKind::OTHER:
        return false;
    }
    return false;
}

void exact_text_terms(std::string_view text, std::vector<std::string>* terms) {
    DORIS_CHECK(terms != nullptr);
    terms->push_back(variant_term_codec::term_string(text));
    if (text == "true") {
        terms->push_back(variant_term_codec::term_bool(true));
        return;
    }
    if (text == "false") {
        terms->push_back(variant_term_codec::term_bool(false));
        return;
    }
    if (text.empty() || text.size() > MAX_NUMBER_TEXT) {
        return;
    }
    // A number is named only by its canonical text: "42", "-5", "42.7" and "1e+300" are
    // numbers, "042", "+42", "42.0" and " 42" are only strings.
    std::optional<VariantCanonicalNumber> number = integer_of_text(text);
    if (!number.has_value()) {
        if (const auto value = double_of_text(text); value.has_value()) {
            number = canonical_numeric_from_double(*value);
        }
    }
    if (number.has_value() && canonical_number_text(*number) == text) {
        terms->push_back(number_term(*number));
    }
}

void cast_text_candidate_terms(std::string_view text, std::vector<std::string>* terms) {
    DORIS_CHECK(terms != nullptr);
    terms->push_back(variant_term_codec::term_string(text));
    // CAST(boolean AS STRING) prints 1 / 0.
    if (text == "1") {
        terms->push_back(variant_term_codec::term_bool(true));
    } else if (text == "0") {
        terms->push_back(variant_term_codec::term_bool(false));
    }
    if (text.empty() || text.size() > MAX_NUMBER_TEXT) {
        return;
    }
    if (const auto integer = integer_of_text(text); integer.has_value()) {
        append_number_term(integer, terms);
        return;
    }
    const auto value = double_of_text(text);
    if (!value.has_value()) {
        return;
    }
    // Every double leaf prints its shortest round-trip text, so the text names one double,
    // which folds like the leaf did ("-0" and "3" fold into integers). A FLOAT leaf prints
    // through float formatting instead but is indexed as the double it denotes: the float the
    // text reads back to names that term.
    append_number_term(canonical_numeric_from_double(*value), terms);
    const auto narrowed = static_cast<float>(*value);
    if (std::isfinite(narrowed) && static_cast<double>(narrowed) != *value) {
        append_number_term(canonical_numeric_from_double(static_cast<double>(narrowed)), terms);
    }
}

void typed_literal_terms(const Field& value, std::vector<std::string>* terms) {
    DORIS_CHECK(terms != nullptr);
    switch (value.get_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        terms->push_back(variant_term_codec::term_bool(value.get<PrimitiveType::TYPE_BOOLEAN>()));
        break;
    case PrimitiveType::TYPE_TINYINT:
        append_number_term(canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_TINYINT>()),
                           terms);
        break;
    case PrimitiveType::TYPE_SMALLINT:
        append_number_term(canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_SMALLINT>()),
                           terms);
        break;
    case PrimitiveType::TYPE_INT:
        append_number_term(canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_INT>()),
                           terms);
        break;
    case PrimitiveType::TYPE_BIGINT:
        append_number_term(canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_BIGINT>()),
                           terms);
        break;
    case PrimitiveType::TYPE_UINT32:
        append_number_term(canonical_numeric_from_uint64(value.get<PrimitiveType::TYPE_UINT32>()),
                           terms);
        break;
    case PrimitiveType::TYPE_UINT64:
        append_number_term(canonical_numeric_from_uint64(value.get<PrimitiveType::TYPE_UINT64>()),
                           terms);
        break;
    case PrimitiveType::TYPE_FLOAT:
        append_number_term(canonical_numeric_from_double(
                                   static_cast<double>(value.get<PrimitiveType::TYPE_FLOAT>())),
                           terms);
        break;
    case PrimitiveType::TYPE_DOUBLE:
        append_number_term(canonical_numeric_from_double(value.get<PrimitiveType::TYPE_DOUBLE>()),
                           terms);
        break;
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
        terms->push_back(variant_term_codec::term_string(value.as_string_view()));
        break;
    default:
        break;
    }
}

} // namespace doris::segment_v2::variant_root_index
