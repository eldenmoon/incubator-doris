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

#include "exprs/function/function_variant_get.h"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>

#include "common/check.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "exprs/function/function.h"
#include "exprs/function/function_variant_element_v2.h"
#include "exprs/function/simple_function_factory.h"
#include "util/jsonb_document.h"

namespace doris {

struct ResolvedVariantGetV2Path::Impl {
    bool is_root = false;
    std::unique_ptr<ResolvedVariantElementV2Path> element_path;
};

ResolvedVariantGetV2Path::ResolvedVariantGetV2Path(std::unique_ptr<Impl> impl)
        : _impl(std::move(impl)) {}

ResolvedVariantGetV2Path::~ResolvedVariantGetV2Path() = default;
ResolvedVariantGetV2Path::ResolvedVariantGetV2Path(ResolvedVariantGetV2Path&&) noexcept = default;
ResolvedVariantGetV2Path& ResolvedVariantGetV2Path::operator=(ResolvedVariantGetV2Path&&) noexcept =
        default;

bool ResolvedVariantGetV2Path::is_root() const noexcept {
    return _impl->is_root;
}

namespace {

Status invalid_variant_get_path(StringRef path) {
    if (path.data == nullptr) {
        return Status::InvalidArgument("Invalid JSONPath for variant_get");
    }
    return Status::InvalidArgument("Invalid JSONPath for variant_get: {}", path.to_string());
}

bool append_normalized_escape(std::string_view path, size_t* position, char source_quote,
                              std::string* output) {
    if (*position == path.size()) {
        return false;
    }
    const char escaped = path[(*position)++];
    const bool common_escape = escaped == '\\' || escaped == '/' || escaped == 'b' ||
                               escaped == 'f' || escaped == 'n' || escaped == 'r' || escaped == 't';
    if (escaped == source_quote) {
        if (escaped == '"') {
            output->append("\\\"");
        } else {
            output->push_back(escaped);
        }
        return true;
    }
    if (escaped == '"' && source_quote == '\'') {
        output->append("\\\"");
        return true;
    }
    if (common_escape) {
        output->push_back('\\');
        output->push_back(escaped);
        return true;
    }
    return false;
}

bool append_normalized_quoted_key(std::string_view path, size_t* position, char source_quote,
                                  std::string_view empty_key_marker, std::string* output) {
    output->push_back('"');
    ++*position;
    bool has_key_byte = false;
    while (*position < path.size()) {
        const char current = path[(*position)++];
        if (current == source_quote) {
            if (!has_key_byte) {
                output->append(empty_key_marker);
            }
            output->push_back('"');
            return true;
        }
        if (static_cast<unsigned char>(current) < 0x20) {
            return false;
        }
        if (current == '\\') {
            if (!append_normalized_escape(path, position, source_quote, output)) {
                return false;
            }
            has_key_byte = true;
        } else if (current == '"' && source_quote == '\'') {
            output->append("\\\"");
            has_key_byte = true;
        } else {
            output->push_back(current);
            has_key_byte = true;
        }
    }
    return false;
}

// JsonbPath already parses dotted members and array indexes. Normalize StarRocks-compatible
// bracket-quoted object keys into JsonbPath's ."key" form, then reuse that parser.
Status normalize_variant_get_path(StringRef raw_path, std::string_view empty_key_marker,
                                  std::string* output) {
    const std::string_view path(raw_path.data, raw_path.size);
    output->reserve(path.size());
    size_t position = 0;
    while (position < path.size()) {
        if (path[position] == '"') {
            if (position == 0 || path[position - 1] != '.') {
                return invalid_variant_get_path(raw_path);
            }
            if (!append_normalized_quoted_key(path, &position, '"', empty_key_marker, output)) {
                return invalid_variant_get_path(raw_path);
            }
            continue;
        }
        if (path[position] != '[') {
            if (path[position] == '\'' || path[position] == '\\' || path[position] == ']') {
                return invalid_variant_get_path(raw_path);
            }
            output->push_back(path[position++]);
            continue;
        }

        ++position;
        if (position == path.size()) {
            return invalid_variant_get_path(raw_path);
        }
        if (path[position] == '"' || path[position] == '\'') {
            const char quote = path[position];
            output->push_back('.');
            if (!append_normalized_quoted_key(path, &position, quote, empty_key_marker, output) ||
                position == path.size() || path[position] != ']') {
                return invalid_variant_get_path(raw_path);
            }
            ++position;
            continue;
        }

        output->push_back('[');
        const size_t index_begin = position;
        while (position < path.size() && std::isdigit(static_cast<unsigned char>(path[position]))) {
            output->push_back(path[position++]);
        }
        if (position == index_begin || position == path.size() || path[position] != ']') {
            return invalid_variant_get_path(raw_path);
        }
        output->push_back(']');
        ++position;
    }
    return Status::OK();
}

} // namespace

Status resolve_variant_get_v2_path(
        StringRef path,
        // Mutable smart-pointer output is published only after full path validation.
        // NOLINTNEXTLINE(readability-non-const-parameter)
        std::unique_ptr<ResolvedVariantGetV2Path>* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("variant_get resolved path output is null");
    }
    if (path.size == 0 || path.data == nullptr || path.data[0] != '$') {
        return invalid_variant_get_path(path);
    }

    auto impl = std::make_unique<ResolvedVariantGetV2Path::Impl>();
    if (path.size == 1) {
        impl->is_root = true;
    } else {
        const std::string_view raw_path(path.data, path.size);
        std::string empty_key_marker = "__DORIS_VARIANT_GET_EMPTY_KEY__";
        while (raw_path.find(empty_key_marker) != std::string_view::npos) {
            empty_key_marker.push_back('_');
        }
        std::string normalized_path;
        RETURN_IF_ERROR(normalize_variant_get_path(path, empty_key_marker, &normalized_path));
        JsonbPath parsed;
        if (!parsed.seek(normalized_path.data(), normalized_path.size()) || parsed.is_wildcard() ||
            parsed.is_supper_wildcard()) {
            return invalid_variant_get_path(path);
        }
        DorisVector<VariantElementV2PathSegment> segments;
        segments.reserve(parsed.get_leg_vector_size());
        for (size_t index = 0; index < parsed.get_leg_vector_size(); ++index) {
            const leg_info* leg = parsed.get_leg_from_leg_vector(index);
            if (leg->type == MEMBER_CODE) {
                StringRef key {leg->leg_ptr, static_cast<size_t>(leg->leg_len)};
                if (key.to_string_view() == empty_key_marker) {
                    key = {};
                }
                segments.push_back(VariantElementV2PathSegment::object_key(key));
                continue;
            }
            if (leg->type != ARRAY_CODE || leg->array_index < 0) {
                return invalid_variant_get_path(path);
            }
            segments.push_back(VariantElementV2PathSegment::array_index(leg->array_index));
        }
        RETURN_IF_ERROR(resolve_variant_element_v2_path(segments, &impl->element_path));
    }

    auto candidate = std::unique_ptr<ResolvedVariantGetV2Path>(
            new ResolvedVariantGetV2Path(std::move(impl)));
    output->swap(candidate);
    return Status::OK();
}

namespace {

Status wrap_variant_source(const ColumnVariantV2& source, ColumnUInt8::MutablePtr nulls,
                           // Mutable smart-pointer output is published only on success.
                           // NOLINTNEXTLINE(readability-non-const-parameter)
                           ColumnPtr* const output) {
    DORIS_CHECK_EQ(nulls->size(), source.size());
    ColumnPtr null_map = std::move(nulls);
    ColumnPtr candidate = ColumnNullable::create(source.get_ptr(), null_map);
    output->swap(candidate);
    return Status::OK();
}

struct MaterializedVariantArgument {
    ColumnPtr owner;
    const ColumnVariantV2* values = nullptr;
    std::span<const uint8_t> outer_nulls;
};

Status materialize_variant_argument(const Block& block, uint32_t argument, size_t rows,
                                    std::string_view function_name,
                                    MaterializedVariantArgument* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("function {} Variant argument output is null",
                                       function_name);
    }

    MaterializedVariantArgument candidate;
    candidate.owner = block.get_by_position(argument).column->convert_to_full_column_if_const();
    const IColumn* physical = candidate.owner.get();
    if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
        candidate.outer_nulls = nullable->get_null_map_data();
        physical = &nullable->get_nested_column();
    }
    candidate.values = check_and_get_column<ColumnVariantV2>(physical);
    if (candidate.values == nullptr) {
        return Status::NotSupported(
                "function {} requires ColumnVariantV2 (Variant V2 only), got {}", function_name,
                physical->get_name());
    }
    if (candidate.values->size() != rows) {
        return Status::InternalError("function {} received {} Variant rows, expected {}",
                                     function_name, candidate.values->size(), rows);
    }
    *output = std::move(candidate);
    return Status::OK();
}

Status resolve_constant_path_column(const IColumn& argument_column, std::string_view function_name,
                                    bool* is_sql_null,
                                    std::unique_ptr<ResolvedVariantGetV2Path>* output) {
    if (is_sql_null == nullptr || output == nullptr) {
        return Status::InvalidArgument("function {} JSONPath output is null", function_name);
    }

    const IColumn* path_column = &argument_column;
    if (const auto* constant = check_and_get_column<ColumnConst>(path_column)) {
        path_column = &constant->get_data_column();
    }
    if (path_column->empty()) {
        return Status::InvalidArgument("function {} constant JSONPath has no rows", function_name);
    }
    if (path_column->is_null_at(0)) {
        *is_sql_null = true;
        return Status::OK();
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(path_column)) {
        path_column = &nullable->get_nested_column();
    }
    const auto* strings = check_and_get_column<ColumnString>(path_column);
    if (strings == nullptr) {
        return Status::InvalidArgument("function {} requires a constant String JSONPath, got {}",
                                       function_name, path_column->get_name());
    }

    *is_sql_null = false;
    return resolve_variant_get_v2_path(strings->get_data_at(0), output);
}

struct VariantGetFunctionState {
    bool path_is_sql_null = false;
    std::unique_ptr<ResolvedVariantGetV2Path> path;
};

class FunctionVariantGet final : public IFunction {
public:
    static constexpr auto name = "variant_get";
    static FunctionPtr create() { return std::make_shared<FunctionVariantGet>(); }

    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }
    bool use_default_implementation_for_nulls() const override { return false; }
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    DataTypes get_variadic_argument_types_impl() const override {
        return {std::make_shared<DataTypeVariant>(), std::make_shared<DataTypeString>()};
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DORIS_CHECK_EQ(arguments.size(), 2);
        DataTypePtr variant_type = remove_nullable(arguments[0]);
        DORIS_CHECK_EQ(variant_type->get_primitive_type(), TYPE_VARIANT);
        return make_nullable(variant_type);
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        if (context == nullptr || context->get_num_args() != 2 || !context->is_col_constant(1) ||
            context->get_constant_col(1) == nullptr) {
            return Status::InvalidArgument("function variant_get JSONPath must be constant");
        }

        auto state = std::make_shared<VariantGetFunctionState>();
        RETURN_IF_ERROR(resolve_constant_path_column(*context->get_constant_col(1)->column_ptr,
                                                     get_name(), &state->path_is_sql_null,
                                                     &state->path));
        context->set_function_state(scope, std::move(state));
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        MaterializedVariantArgument source;
        RETURN_IF_ERROR(materialize_variant_argument(block, arguments[0], input_rows_count,
                                                     get_name(), &source));

        DORIS_CHECK(context != nullptr);
        const auto* state = reinterpret_cast<const VariantGetFunctionState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        DORIS_CHECK(state != nullptr);
        if (state->path_is_sql_null) {
            ColumnPtr output;
            RETURN_IF_ERROR(wrap_variant_source(*source.values,
                                                ColumnUInt8::create(input_rows_count, 1), &output));
            block.replace_by_position(result, std::move(output));
            return Status::OK();
        }

        ColumnPtr output;
        DORIS_CHECK(state->path != nullptr);
        RETURN_IF_ERROR(variant_get_v2(*source.values, *state->path, source.outer_nulls, &output));
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }
};

} // namespace

Status variant_get_v2(const ColumnVariantV2& source, const ResolvedVariantGetV2Path& path,
                      std::span<const uint8_t> outer_nulls,
                      // Mutable smart-pointer output is published only on success.
                      // NOLINTNEXTLINE(readability-non-const-parameter)
                      ColumnPtr* const output) {
    if (output == nullptr) {
        return Status::InvalidArgument("variant_get output is null");
    }
    if (!outer_nulls.empty() && outer_nulls.size() != source.size()) {
        return Status::InvalidArgument("variant_get outer null map has {} rows, expected {}",
                                       outer_nulls.size(), source.size());
    }
    if (!path.is_root()) {
        DORIS_CHECK(path._impl->element_path != nullptr);
        return extract_variant_element_v2(source, *path._impl->element_path, outer_nulls, output);
    }

    auto nulls = ColumnUInt8::create(source.size(), 0);
    if (!outer_nulls.empty()) {
        std::ranges::copy(outer_nulls, nulls->get_data().begin());
    }
    return wrap_variant_source(source, std::move(nulls), output);
}

void register_function_variant_get(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionVariantGet>();
}

} // namespace doris
