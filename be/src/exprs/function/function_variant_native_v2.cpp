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

#include "exprs/function/function_variant_native_v2.h"

#include <limits>
#include <string_view>

#include "common/check.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/value/variant/variant_field.h"
#include "exprs/function/function_variant_path_v2_internal.h"

namespace doris {
namespace {

Status native_exception_status(const Exception& exception) {
    if (exception.code() == ErrorCode::CORRUPTION) {
        return Status::InvalidArgument("Invalid Variant V2 input: {}", exception.message());
    }
    return exception.to_status();
}

Status validate_outer_nulls(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls) {
    if (!outer_nulls.empty() && outer_nulls.size() != source.size()) {
        return Status::InvalidArgument("Variant V2 outer null map has {} rows, expected {}",
                                       outer_nulls.size(), source.size());
    }
    return Status::OK();
}

void validate_encoded_rows(const ColumnVariantV2& source, std::span<const uint8_t> skipped_rows) {
    if (source.is_typed()) {
        return;
    }
    auto view = source.read_view();
    DorisVector<uint8_t> validated_metadata(view.metadata_count(), 0);
    for (size_t row = 0; row < view.size(); ++row) {
        if (variant_native_v2_internal::is_outer_null(skipped_rows, row)) {
            continue;
        }
        const uint32_t metadata_id = view.metadata_id_at(row);
        if (validated_metadata[metadata_id] == 0) {
            validate_variant_metadata(view.metadata_at(metadata_id));
            validated_metadata[metadata_id] = 1;
        }
        validate_variant_payload(view.value_at(row));
    }
}

template <typename Execute>
Status execute_unary_native(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                            ColumnPtr* const output, Execute&& execute) {
    if (output == nullptr) {
        return Status::InvalidArgument("Variant V2 native output is null");
    }
    RETURN_IF_ERROR(validate_outer_nulls(source, outer_nulls));
    ColumnPtr candidate;
    try {
        validate_encoded_rows(source, outer_nulls);
        RETURN_IF_ERROR(execute(&candidate));
    } catch (const Exception& exception) {
        return native_exception_status(exception);
    }
    if (!candidate || candidate->size() != source.size()) {
        return Status::InternalError("Variant V2 native kernel produced {} rows, expected {}",
                                     candidate ? candidate->size() : 0, source.size());
    }
    output->swap(candidate);
    return Status::OK();
}

Status execute_variant_is_null_v2(const ColumnVariantV2& source,
                                  std::span<const uint8_t> outer_nulls, ColumnPtr* const output) {
    auto values = ColumnUInt8::create();
    auto nulls = ColumnUInt8::create();
    values->reserve(source.size());
    nulls->reserve(source.size());

    if (source.is_typed()) {
        const auto& typed = assert_cast<const ColumnNullable&>(source.typed_column());
        for (size_t row = 0; row < source.size(); ++row) {
            const bool outer_null = variant_native_v2_internal::is_outer_null(outer_nulls, row);
            values->insert_value(outer_null ? 0 : typed.is_null_at(row));
            nulls->insert_value(outer_null);
        }
    } else {
        auto source_view = source.read_view();
        for (size_t row = 0; row < source.size(); ++row) {
            const bool outer_null = variant_native_v2_internal::is_outer_null(outer_nulls, row);
            values->insert_value(outer_null ? 0 : source_view.value_at(row).is_null());
            nulls->insert_value(outer_null);
        }
    }

    *output = ColumnNullable::create(std::move(values), std::move(nulls));
    return Status::OK();
}

void append_type_word(ColumnString& values, ColumnUInt8& nulls, std::string_view word) {
    values.insert_data(word.data(), word.size());
    nulls.insert_value(0);
}

void append_type_null(ColumnString& values, ColumnUInt8& nulls) {
    values.insert_default();
    nulls.insert_value(1);
}

std::string_view encoded_type_word(const VariantRef value) {
    switch (value.basic_type()) {
    case VariantBasicType::SHORT_STRING:
        return "string";
    case VariantBasicType::OBJECT:
        return "object";
    case VariantBasicType::ARRAY:
        return "array";
    case VariantBasicType::PRIMITIVE:
        break;
    }

    switch (value.primitive_id()) {
    case VariantPrimitiveId::NULL_VALUE:
        return "null";
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        return "bool";
    case VariantPrimitiveId::INT8:
        return "tinyint";
    case VariantPrimitiveId::INT16:
        return "smallint";
    case VariantPrimitiveId::INT32:
        return "int";
    case VariantPrimitiveId::INT64:
        return "bigint";
    case VariantPrimitiveId::DOUBLE:
        return "double";
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16:
        static_cast<void>(value.get_decimal());
        return "decimal";
    case VariantPrimitiveId::DATE:
        return "date";
    case VariantPrimitiveId::TIMESTAMP_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
        return "timestamp";
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        return "timestamp_ntz";
    case VariantPrimitiveId::FLOAT:
        return "float";
    case VariantPrimitiveId::BINARY:
        return "binary";
    case VariantPrimitiveId::STRING:
        return "string";
    case VariantPrimitiveId::TIME_NTZ_MICROS:
        return "time";
    case VariantPrimitiveId::UUID:
        return "uuid";
    }
    DORIS_CHECK(false) << "validated Variant primitive id has no type word";
    return {};
}

template <PrimitiveType Type>
uint8_t typed_variant_scale(uint32_t scale) {
    if constexpr (Type == TYPE_DECIMALV2 || Type == TYPE_DECIMAL32 || Type == TYPE_DECIMAL64 ||
                  Type == TYPE_DECIMAL128I) {
        DORIS_CHECK_LE(scale, static_cast<uint32_t>(std::numeric_limits<uint8_t>::max()))
                << "typed decimal scale exceeds the Variant scale domain";
        return static_cast<uint8_t>(scale);
    }
    return 0;
}

std::string_view typed_integer_type_word(const VariantScalarEncodingPlan& plan) {
    switch (plan.size()) {
    case 2:
        return "tinyint";
    case 3:
        return "smallint";
    case 5:
        return "int";
    case 9:
        return "bigint";
    default:
        DORIS_CHECK(false) << "typed integer has invalid Variant encoding size " << plan.size();
        return {};
    }
}

template <PrimitiveType Type>
std::string_view typed_type_word(const VariantScalarEncodingPlan& plan) {
    if constexpr (Type == TYPE_LARGEINT) {
        return plan.size() == 18 ? "decimal" : "string";
    }

    if constexpr (Type == TYPE_BOOLEAN) {
        return "bool";
    } else if constexpr (Type == TYPE_TINYINT || Type == TYPE_SMALLINT || Type == TYPE_INT ||
                         Type == TYPE_BIGINT) {
        return typed_integer_type_word(plan);
    } else if constexpr (Type == TYPE_FLOAT) {
        return "float";
    } else if constexpr (Type == TYPE_DOUBLE) {
        return "double";
    } else if constexpr (Type == TYPE_DECIMALV2 || Type == TYPE_DECIMAL32 ||
                         Type == TYPE_DECIMAL64 || Type == TYPE_DECIMAL128I) {
        return "decimal";
    } else if constexpr (Type == TYPE_DATE || Type == TYPE_DATEV2) {
        return "date";
    } else if constexpr (Type == TYPE_DATETIME || Type == TYPE_DATETIMEV2) {
        return "timestamp_ntz";
    } else if constexpr (Type == TYPE_TIMESTAMPTZ) {
        return "timestamp";
    } else if constexpr (Type == TYPE_CHAR || Type == TYPE_VARCHAR || Type == TYPE_STRING ||
                         Type == TYPE_IPV4 || Type == TYPE_IPV6) {
        return "string";
    }
    DORIS_CHECK(false) << "supported typed Variant identity has no type word";
    return {};
}

Status execute_variant_type_v2(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                               ColumnPtr* const output) {
    auto values = ColumnString::create();
    auto nulls = ColumnUInt8::create();
    values->reserve(source.size());
    nulls->reserve(source.size());

    auto source_view = source.read_view();
    if (!source_view.is_typed()) {
        for (size_t row = 0; row < source_view.size(); ++row) {
            if (variant_native_v2_internal::is_outer_null(outer_nulls, row)) {
                append_type_null(*values, *nulls);
                continue;
            }
            append_type_word(*values, *nulls, encoded_type_word(source_view.value_at(row)));
        }
    } else {
        const auto& typed = assert_cast<const ColumnNullable&>(source_view.typed_column());
        const DataTypePtr& typed_type = source_view.typed_type();
        const auto& typed_nulls = typed.get_null_map_data();
        dispatch_variant_typed_column(
                typed.get_nested_column(), typed_type->get_primitive_type(),
                [&]<PrimitiveType Type>(const auto& column) {
                    const uint8_t scale = typed_variant_scale<Type>(typed_type->get_scale());
                    for (size_t row = 0; row < source_view.size(); ++row) {
                        if (variant_native_v2_internal::is_outer_null(outer_nulls, row)) {
                            append_type_null(*values, *nulls);
                            continue;
                        }
                        if (typed_nulls[row] != 0) {
                            append_type_word(*values, *nulls, "null");
                            continue;
                        }
                        with_variant_typed_scalar<Type>(
                                column, row, scale, [&](auto&& physical_factory, auto&&) {
                                    const VariantScalarEncodingPlan plan = physical_factory();
                                    append_type_word(*values, *nulls, typed_type_word<Type>(plan));
                                });
                    }
                });
    }

    *output = ColumnNullable::create(std::move(values), std::move(nulls));
    return Status::OK();
}

} // namespace

Status variant_exists_path_v2(const ColumnVariantV2& source,
                              const ResolvedVariantElementV2Path& path,
                              std::span<const uint8_t> outer_nulls, ColumnPtr* const output) {
    if (path.size() == 0) {
        return Status::InvalidArgument("Variant V2 exists path must not be empty");
    }
    return execute_unary_native(source, outer_nulls, output, [&](ColumnPtr* const candidate) {
        return variant_native_v2_internal::execute_variant_exists_path_v2(source, path, outer_nulls,
                                                                          candidate);
    });
}

Status variant_is_null_v2(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                          ColumnPtr* const output) {
    return execute_unary_native(source, outer_nulls, output, [&](ColumnPtr* const candidate) {
        return execute_variant_is_null_v2(source, outer_nulls, candidate);
    });
}

Status variant_type_v2(
        const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
        // NOLINTNEXTLINE(readability-non-const-parameter) -- shared publisher mutates result slot.
        ColumnPtr* const output) {
    return execute_unary_native(source, outer_nulls, output, [&](ColumnPtr* const candidate) {
        return execute_variant_type_v2(source, outer_nulls, candidate);
    });
}

Status variant_keys_v2(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                       ColumnPtr* const output) {
    return execute_unary_native(source, outer_nulls, output, [&](ColumnPtr* const candidate) {
        return variant_native_v2_internal::execute_variant_keys_v2(source, outer_nulls, candidate);
    });
}

Status variant_length_v2(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                         ColumnPtr* const output) {
    return execute_unary_native(source, outer_nulls, output, [&](ColumnPtr* const candidate) {
        return variant_native_v2_internal::execute_variant_length_v2(source, outer_nulls,
                                                                     candidate);
    });
}

Status variant_contains_v2(const ColumnVariantV2& target, const ColumnVariantV2& candidate,
                           std::span<const uint8_t> target_outer_nulls,
                           std::span<const uint8_t> candidate_outer_nulls,
                           ColumnPtr* const output) {
    if (output == nullptr) {
        return Status::InvalidArgument("Variant V2 native output is null");
    }
    if (target.size() != candidate.size()) {
        return Status::InvalidArgument("Variant V2 contains inputs have {} and {} rows",
                                       target.size(), candidate.size());
    }
    RETURN_IF_ERROR(validate_outer_nulls(target, target_outer_nulls));
    RETURN_IF_ERROR(validate_outer_nulls(candidate, candidate_outer_nulls));

    ColumnPtr result;
    try {
        DorisVector<uint8_t> skipped_rows(target.size(), 0);
        for (size_t row = 0; row < target.size(); ++row) {
            skipped_rows[row] =
                    variant_native_v2_internal::is_outer_null(target_outer_nulls, row) ||
                    variant_native_v2_internal::is_outer_null(candidate_outer_nulls, row);
        }
        validate_encoded_rows(target, skipped_rows);
        validate_encoded_rows(candidate, skipped_rows);
        RETURN_IF_ERROR(variant_native_v2_internal::execute_variant_contains_v2(
                target, candidate, target_outer_nulls, candidate_outer_nulls, &result));
    } catch (const Exception& exception) {
        return native_exception_status(exception);
    }
    if (!result || result->size() != target.size()) {
        return Status::InternalError("Variant V2 contains kernel produced {} rows, expected {}",
                                     result ? result->size() : 0, target.size());
    }
    output->swap(result);
    return Status::OK();
}

} // namespace doris
