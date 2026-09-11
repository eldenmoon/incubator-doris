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

#include <iterator>
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

void append_canonical_number(std::string_view path,
                             const std::optional<VariantCanonicalNumber>& number,
                             std::vector<std::string>* terms) {
    if (!number.has_value()) {
        return; // NaN: no term
    }
    switch (number->kind) {
    case VariantLeafKind::INT64:
        terms->push_back(encode_int64_term(path, number->int64_value));
        break;
    case VariantLeafKind::UINT64:
        terms->push_back(encode_uint64_term(path, number->uint64_value));
        break;
    case VariantLeafKind::DOUBLE:
        terms->push_back(encode_double_term(path, number->double_value));
        break;
    default:
        break;
    }
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
    case PrimitiveType::TYPE_DOUBLE:
        return "floating";
    default:
        return {};
    }
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
           version != properties.end() &&
           (version->second == VARIANT_ROOT_FORMAT_VERSION_V1 ||
            version->second == VARIANT_ROOT_FORMAT_VERSION_V2);
}

bool is_root_index(const TabletIndex& index) {
    return index.is_inverted_index() && is_root_mode_properties(index.properties());
}

bool is_all_values_index(const TabletIndex& index) {
    return index.is_inverted_index() && is_all_values_mode_properties(index.properties());
}

std::string encode_int64_term(std::string_view path, int64_t value) {
    return variant_term_codec::term_int64(value, path);
}

std::string encode_uint64_term(std::string_view path, uint64_t value) {
    return variant_term_codec::term_uint64(value, path);
}

std::string encode_double_term(std::string_view path, double value) {
    return variant_term_codec::term_double(value, path);
}

std::string encode_bool_term(std::string_view path, bool value) {
    return variant_term_codec::term_bool(value, path);
}

std::string encode_string_term(std::string_view path, std::string_view value) {
    return variant_term_codec::term_string(value, path);
}

std::string encode_token_term(std::string_view path, std::string_view value) {
    return variant_term_codec::term_token(value, path);
}

std::string encode_all_value_term(std::string_view value) {
    return variant_term_codec::root_prefix_string(value);
}

std::string encode_all_value_token_term(std::string_view value) {
    return variant_term_codec::root_prefix_token(value);
}

void append_variant_leaf_terms(std::string_view path, const VariantLeaf& leaf,
                               std::vector<std::string>* terms) {
    DORIS_CHECK(terms != nullptr);
    switch (leaf.kind) {
    case VariantLeafKind::STRING:
        terms->push_back(
                encode_string_term(path, {leaf.string_value.data, leaf.string_value.size}));
        break;
    case VariantLeafKind::INT64:
        terms->push_back(encode_int64_term(path, leaf.int64_value));
        break;
    case VariantLeafKind::UINT64:
        terms->push_back(encode_uint64_term(path, leaf.uint64_value));
        break;
    case VariantLeafKind::DOUBLE:
        terms->push_back(encode_double_term(path, leaf.double_value));
        break;
    case VariantLeafKind::BOOL:
        terms->push_back(encode_bool_term(path, leaf.bool_value));
        break;
    case VariantLeafKind::OTHER:
        break;
    }
}

Status append_variant_value_terms(std::string_view path, const VariantRef& value,
                                  std::vector<std::string>* terms) {
    DORIS_CHECK(terms != nullptr);
    try {
        append_variant_leaf_terms(path, classify_variant_leaf(path, value), terms);
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
        append_canonical_number(
                path, canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_TINYINT>()),
                terms);
        break;
    case PrimitiveType::TYPE_SMALLINT:
        append_canonical_number(
                path, canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_SMALLINT>()),
                terms);
        break;
    case PrimitiveType::TYPE_INT:
        append_canonical_number(
                path, canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_INT>()), terms);
        break;
    case PrimitiveType::TYPE_BIGINT:
        append_canonical_number(
                path, canonical_numeric_from_int64(value.get<PrimitiveType::TYPE_BIGINT>()), terms);
        break;
    case PrimitiveType::TYPE_UINT32:
        append_canonical_number(
                path, canonical_numeric_from_uint64(value.get<PrimitiveType::TYPE_UINT32>()),
                terms);
        break;
    case PrimitiveType::TYPE_UINT64:
        append_canonical_number(
                path, canonical_numeric_from_uint64(value.get<PrimitiveType::TYPE_UINT64>()),
                terms);
        break;
    case PrimitiveType::TYPE_FLOAT:
        append_canonical_number(path,
                                canonical_numeric_from_double(static_cast<double>(
                                        value.get<PrimitiveType::TYPE_FLOAT>())),
                                terms);
        break;
    case PrimitiveType::TYPE_DOUBLE:
        append_canonical_number(
                path, canonical_numeric_from_double(value.get<PrimitiveType::TYPE_DOUBLE>()),
                terms);
        break;
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
    case PrimitiveType::TYPE_STRING:
        terms->push_back(encode_string_term(path, value.as_string_view()));
        break;
    default:
        break;
    }
    return Status::OK();
}

Status encode_all_values_query_value_terms(const Field& value, std::vector<std::string>* terms,
                                           size_t ignore_above) {
    DORIS_CHECK(terms != nullptr);
    if (is_string_type(value.get_type()) && value.as_string_view().size() > ignore_above) {
        return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                "VARIANT all-values equality value exceeds ignore_above");
    }
    std::vector<std::string> encoded;
    RETURN_IF_ERROR(encode_query_value_terms({}, value, &encoded));
    if (encoded.empty()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                "VARIANT all-values query value cannot be represented exactly");
    }
    terms->insert(terms->end(), std::make_move_iterator(encoded.begin()),
                  std::make_move_iterator(encoded.end()));
    return Status::OK();
}

std::shared_ptr<TabletIndex> make_query_index(const TabletIndex& root_index,
                                              std::string_view relative_path,
                                              PrimitiveType path_type) {
    const std::string_view family = query_value_family(path_type);
    // A dynamic path has no single value family: on a Root index it binds as a subtree scan
    // (any typed literal, every leaf below the path); on an AllValues index the path is ignored.
    const bool subtree =
            path_type == PrimitiveType::TYPE_VARIANT && !is_all_values_index(root_index);
    DORIS_CHECK(!family.empty() || path_type == PrimitiveType::TYPE_VARIANT);
    TabletIndexPB index_pb;
    root_index.to_schema_pb(&index_pb);
    (*index_pb.mutable_properties())[std::string(VARIANT_ROOT_QUERY_PATH_KEY)] = relative_path;
    if (!family.empty()) {
        (*index_pb.mutable_properties())[std::string(VARIANT_ROOT_QUERY_VALUE_FAMILY_KEY)] = family;
    }
    if (subtree) {
        (*index_pb.mutable_properties())[std::string(VARIANT_ROOT_QUERY_SUBTREE_KEY)] = "1";
    }
    auto result = std::make_shared<TabletIndex>();
    result->init_from_pb(index_pb);
    return result;
}

} // namespace doris::segment_v2::variant_root_index
