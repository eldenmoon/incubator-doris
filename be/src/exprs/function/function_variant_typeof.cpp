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

#include "exprs/function/function_variant_typeof.h"

#include <algorithm>
#include <cstdint>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/string_buffer.hpp"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
namespace {

enum class SchemaKind : uint8_t {
    VOID,
    BOOLEAN,
    BIGINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    STRING,
    BINARY,
    DATE,
    TIMESTAMP,
    TIMESTAMP_NTZ,
    TIME_NTZ,
    UUID,
    OBJECT,
    ARRAY,
    VARIANT,
};

struct VariantSchema {
    SchemaKind kind = SchemaKind::VOID;
    uint8_t precision = 0;
    uint8_t scale = 0;
    DorisVector<StringRef> field_names;
    DorisVector<VariantSchema> children;
};

VariantSchema scalar_schema(SchemaKind kind) {
    VariantSchema result;
    result.kind = kind;
    return result;
}

VariantSchema decimal_schema(uint8_t precision, uint8_t scale) {
    VariantSchema result;
    result.kind = SchemaKind::DECIMAL;
    result.precision = precision;
    result.scale = scale;
    return result;
}

VariantSchema decimal_schema(VariantDecimal decimal) {
    unsigned __int128 magnitude = variant_unsigned_magnitude(decimal.unscaled);
    uint8_t scale = decimal.scale;
    while (scale != 0 && magnitude % 10 == 0) {
        magnitude /= 10;
        --scale;
    }

    uint8_t digits = 1;
    for (unsigned __int128 rest = magnitude; rest >= 10; rest /= 10) {
        ++digits;
    }
    const uint8_t precision = std::max(digits, scale);
    if (precision > 38) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant decimal precision {} exceeds the supported maximum 38", precision);
    }
    return decimal_schema(precision, scale);
}

bool is_numeric(SchemaKind kind) {
    return kind == SchemaKind::BIGINT || kind == SchemaKind::FLOAT || kind == SchemaKind::DOUBLE ||
           kind == SchemaKind::DECIMAL;
}

VariantSchema merge_schemas(VariantSchema left, VariantSchema right);

VariantSchema merge_decimals(VariantSchema left, VariantSchema right) {
    DORIS_CHECK(left.kind == SchemaKind::DECIMAL);
    DORIS_CHECK(right.kind == SchemaKind::DECIMAL);
    const uint8_t scale = std::max(left.scale, right.scale);
    const uint8_t integral_digits =
            std::max<uint8_t>(left.precision - left.scale, right.precision - right.scale);
    const uint16_t precision = static_cast<uint16_t>(integral_digits) + scale;
    if (precision > 38) {
        return scalar_schema(SchemaKind::DOUBLE);
    }
    return decimal_schema(static_cast<uint8_t>(precision), scale);
}

VariantSchema merge_objects(VariantSchema left, VariantSchema right) {
    DORIS_CHECK(left.kind == SchemaKind::OBJECT);
    DORIS_CHECK(right.kind == SchemaKind::OBJECT);
    DORIS_CHECK_EQ(left.field_names.size(), left.children.size());
    DORIS_CHECK_EQ(right.field_names.size(), right.children.size());

    VariantSchema result = scalar_schema(SchemaKind::OBJECT);
    result.field_names.reserve(left.field_names.size() + right.field_names.size());
    result.children.reserve(left.children.size() + right.children.size());
    size_t left_index = 0;
    size_t right_index = 0;
    while (left_index < left.field_names.size() && right_index < right.field_names.size()) {
        const StringRef left_name = left.field_names[left_index];
        const StringRef right_name = right.field_names[right_index];
        if (left_name < right_name) {
            result.field_names.push_back(left_name);
            result.children.push_back(std::move(left.children[left_index]));
            ++left_index;
        } else if (right_name < left_name) {
            result.field_names.push_back(right_name);
            result.children.push_back(std::move(right.children[right_index]));
            ++right_index;
        } else {
            result.field_names.push_back(left_name);
            result.children.push_back(merge_schemas(std::move(left.children[left_index]),
                                                    std::move(right.children[right_index])));
            ++left_index;
            ++right_index;
        }
    }
    while (left_index < left.field_names.size()) {
        result.field_names.push_back(left.field_names[left_index]);
        result.children.push_back(std::move(left.children[left_index]));
        ++left_index;
    }
    while (right_index < right.field_names.size()) {
        result.field_names.push_back(right.field_names[right_index]);
        result.children.push_back(std::move(right.children[right_index]));
        ++right_index;
    }
    return result;
}

VariantSchema merge_numeric(VariantSchema left, VariantSchema right) {
    DORIS_CHECK(is_numeric(left.kind));
    DORIS_CHECK(is_numeric(right.kind));
    if (left.kind == SchemaKind::DOUBLE || right.kind == SchemaKind::DOUBLE) {
        return scalar_schema(SchemaKind::DOUBLE);
    }
    if ((left.kind == SchemaKind::FLOAT && right.kind == SchemaKind::DECIMAL) ||
        (left.kind == SchemaKind::DECIMAL && right.kind == SchemaKind::FLOAT)) {
        return scalar_schema(SchemaKind::DOUBLE);
    }
    if (left.kind == SchemaKind::FLOAT || right.kind == SchemaKind::FLOAT) {
        return scalar_schema(SchemaKind::FLOAT);
    }
    if (left.kind == SchemaKind::BIGINT) {
        left = decimal_schema(20, 0);
    }
    if (right.kind == SchemaKind::BIGINT) {
        right = decimal_schema(20, 0);
    }
    return merge_decimals(std::move(left), std::move(right));
}

VariantSchema merge_datetime(VariantSchema left, VariantSchema right) {
    const bool has_timestamp =
            left.kind == SchemaKind::TIMESTAMP || right.kind == SchemaKind::TIMESTAMP;
    if (has_timestamp) {
        return scalar_schema(SchemaKind::TIMESTAMP);
    }
    return scalar_schema(SchemaKind::TIMESTAMP_NTZ);
}

VariantSchema merge_schemas(VariantSchema left, VariantSchema right) {
    if (left.kind == SchemaKind::VOID) {
        return right;
    }
    if (right.kind == SchemaKind::VOID) {
        return left;
    }
    if (left.kind == right.kind) {
        if (left.kind == SchemaKind::DECIMAL) {
            return merge_decimals(std::move(left), std::move(right));
        }
        if (left.kind == SchemaKind::OBJECT) {
            return merge_objects(std::move(left), std::move(right));
        }
        if (left.kind == SchemaKind::ARRAY) {
            DORIS_CHECK_EQ(left.children.size(), 1);
            DORIS_CHECK_EQ(right.children.size(), 1);
            left.children[0] =
                    merge_schemas(std::move(left.children[0]), std::move(right.children[0]));
        }
        return left;
    }
    if (is_numeric(left.kind) && is_numeric(right.kind)) {
        return merge_numeric(std::move(left), std::move(right));
    }

    const bool left_datetime = left.kind == SchemaKind::DATE ||
                               left.kind == SchemaKind::TIMESTAMP ||
                               left.kind == SchemaKind::TIMESTAMP_NTZ;
    const bool right_datetime = right.kind == SchemaKind::DATE ||
                                right.kind == SchemaKind::TIMESTAMP ||
                                right.kind == SchemaKind::TIMESTAMP_NTZ;
    if (left_datetime && right_datetime) {
        return merge_datetime(std::move(left), std::move(right));
    }
    return scalar_schema(SchemaKind::VARIANT);
}

VariantSchema infer_primitive_schema(VariantRef value) {
    DORIS_CHECK(value.basic_type() == VariantBasicType::PRIMITIVE);
    switch (value.primitive_id()) {
    case VariantPrimitiveId::NULL_VALUE:
        return scalar_schema(SchemaKind::VOID);
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        return scalar_schema(SchemaKind::BOOLEAN);
    case VariantPrimitiveId::INT8:
    case VariantPrimitiveId::INT16:
    case VariantPrimitiveId::INT32:
    case VariantPrimitiveId::INT64:
        return scalar_schema(SchemaKind::BIGINT);
    case VariantPrimitiveId::DOUBLE:
        return scalar_schema(SchemaKind::DOUBLE);
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16:
        return decimal_schema(value.get_decimal());
    case VariantPrimitiveId::DATE:
        return scalar_schema(SchemaKind::DATE);
    case VariantPrimitiveId::TIMESTAMP_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
        return scalar_schema(SchemaKind::TIMESTAMP);
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
        return scalar_schema(SchemaKind::TIMESTAMP_NTZ);
    case VariantPrimitiveId::FLOAT:
        return scalar_schema(SchemaKind::FLOAT);
    case VariantPrimitiveId::BINARY:
        return scalar_schema(SchemaKind::BINARY);
    case VariantPrimitiveId::STRING:
        return scalar_schema(SchemaKind::STRING);
    case VariantPrimitiveId::TIME_NTZ_MICROS:
        return scalar_schema(SchemaKind::TIME_NTZ);
    case VariantPrimitiveId::UUID:
        return scalar_schema(SchemaKind::UUID);
    }
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Unsupported Variant primitive id {}",
                    static_cast<uint8_t>(value.primitive_id()));
}

VariantSchema infer_schema(VariantRef value, uint32_t depth);

VariantSchema infer_array_schema_range(VariantRef array, uint32_t child_depth, uint32_t begin,
                                       uint32_t end) {
    DORIS_CHECK(array.basic_type() == VariantBasicType::ARRAY);
    DORIS_CHECK_LE(begin, end);
    DORIS_CHECK_LE(end, array.num_elements());
    if (begin == end) {
        return {};
    }
    if (end - begin == 1) {
        return infer_schema(array.array_at(begin), child_depth);
    }
    const uint32_t middle = begin + (end - begin) / 2;
    return merge_schemas(infer_array_schema_range(array, child_depth, begin, middle),
                         infer_array_schema_range(array, child_depth, middle, end));
}

VariantSchema infer_schema(VariantRef value, uint32_t depth) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant schema nesting depth exceeds the supported maximum {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }

    switch (value.basic_type()) {
    case VariantBasicType::SHORT_STRING:
        return scalar_schema(SchemaKind::STRING);
    case VariantBasicType::OBJECT: {
        VariantSchema result = scalar_schema(SchemaKind::OBJECT);
        const uint32_t count = value.num_elements();
        result.field_names.reserve(count);
        result.children.reserve(count);
        for (uint32_t index = 0; index < count; ++index) {
            uint32_t field_id = 0;
            const VariantRef child = value.object_value_at(index, &field_id);
            const StringRef field_name = value.metadata.key_at(field_id);
            if (!result.field_names.empty() && !(result.field_names.back() < field_name)) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "Variant object fields must be unique and sorted");
            }
            result.field_names.push_back(field_name);
            result.children.push_back(infer_schema(child, depth + 1));
        }
        return result;
    }
    case VariantBasicType::ARRAY: {
        const uint32_t count = value.num_elements();
        VariantSchema result = scalar_schema(SchemaKind::ARRAY);
        result.children.push_back(infer_array_schema_range(value, depth + 1, 0, count));
        return result;
    }
    case VariantBasicType::PRIMITIVE:
        return infer_primitive_schema(value);
    }
    throw Exception(ErrorCode::INVALID_ARGUMENT, "Unsupported Variant basic type");
}

template <size_t Size>
void write_literal(VectorBufferWriter* writer, const char (&literal)[Size]) {
    writer->write(literal, Size - 1);
}

bool is_ascii_letter(unsigned char value) {
    return (value >= 'a' && value <= 'z') || (value >= 'A' && value <= 'Z');
}

bool is_simple_identifier(StringRef name) {
    if (name.size == 0) {
        return false;
    }
    const auto first = static_cast<unsigned char>(name.data[0]);
    if (!is_ascii_letter(first) && first != '_') {
        return false;
    }
    for (size_t index = 1; index < name.size; ++index) {
        const auto current = static_cast<unsigned char>(name.data[index]);
        if (!is_ascii_letter(current) && (current < '0' || current > '9') && current != '_') {
            return false;
        }
    }
    return true;
}

void write_field_name(StringRef name, VectorBufferWriter* writer) {
    if (is_simple_identifier(name)) {
        writer->write(name.data, name.size);
        return;
    }
    writer->write_char('`');
    for (size_t index = 0; index < name.size; ++index) {
        writer->write_char(name.data[index]);
        if (name.data[index] == '`') {
            writer->write_char('`');
        }
    }
    writer->write_char('`');
}

void write_schema(const VariantSchema& schema, VectorBufferWriter* writer) {
    switch (schema.kind) {
    case SchemaKind::VOID:
        write_literal(writer, "VOID");
        return;
    case SchemaKind::BOOLEAN:
        write_literal(writer, "BOOLEAN");
        return;
    case SchemaKind::BIGINT:
        write_literal(writer, "BIGINT");
        return;
    case SchemaKind::FLOAT:
        write_literal(writer, "FLOAT");
        return;
    case SchemaKind::DOUBLE:
        write_literal(writer, "DOUBLE");
        return;
    case SchemaKind::DECIMAL:
        write_literal(writer, "DECIMAL(");
        writer->write_number(static_cast<uint32_t>(schema.precision));
        writer->write_char(',');
        writer->write_number(static_cast<uint32_t>(schema.scale));
        writer->write_char(')');
        return;
    case SchemaKind::STRING:
        write_literal(writer, "STRING");
        return;
    case SchemaKind::BINARY:
        write_literal(writer, "BINARY");
        return;
    case SchemaKind::DATE:
        write_literal(writer, "DATE");
        return;
    case SchemaKind::TIMESTAMP:
        write_literal(writer, "TIMESTAMP");
        return;
    case SchemaKind::TIMESTAMP_NTZ:
        write_literal(writer, "TIMESTAMP_NTZ");
        return;
    case SchemaKind::TIME_NTZ:
        write_literal(writer, "TIME_NTZ");
        return;
    case SchemaKind::UUID:
        write_literal(writer, "UUID");
        return;
    case SchemaKind::VARIANT:
        write_literal(writer, "VARIANT");
        return;
    case SchemaKind::ARRAY:
        DORIS_CHECK_EQ(schema.children.size(), 1);
        write_literal(writer, "ARRAY<");
        write_schema(schema.children[0], writer);
        writer->write_char('>');
        return;
    case SchemaKind::OBJECT:
        DORIS_CHECK_EQ(schema.field_names.size(), schema.children.size());
        write_literal(writer, "OBJECT<");
        for (size_t index = 0; index < schema.field_names.size(); ++index) {
            if (index != 0) {
                write_literal(writer, ", ");
            }
            write_field_name(schema.field_names[index], writer);
            write_literal(writer, ": ");
            write_schema(schema.children[index], writer);
        }
        writer->write_char('>');
        return;
    }
}

class FunctionVariantTypeof final : public IFunction {
public:
    static constexpr auto name = "variant_typeof";

    static FunctionPtr create() { return std::make_shared<FunctionVariantTypeof>(); }

    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr materialized =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const IColumn* physical = materialized.get();
        std::span<const uint8_t> outer_nulls;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
            outer_nulls = nullable->get_null_map_data();
            physical = &nullable->get_nested_column();
        }

        const auto* variant = check_and_get_column<ColumnVariantV2>(physical);
        if (variant == nullptr) {
            return Status::NotSupported(
                    "function variant_typeof requires ColumnVariantV2 (Variant V2 only), got {}",
                    physical->get_name());
        }
        if (variant->size() != input_rows_count) {
            return Status::InternalError(
                    "function variant_typeof received {} Variant rows, expected {}",
                    variant->size(), input_rows_count);
        }

        ColumnPtr output;
        RETURN_IF_ERROR(variant_typeof_v2(*variant, outer_nulls, &output));
        DORIS_CHECK_EQ(output->size(), input_rows_count);
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }
};

} // namespace

Status variant_typeof_v2(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                         // Mutable smart-pointer output is published only on success.
                         // NOLINTNEXTLINE(readability-non-const-parameter)
                         ColumnPtr* const output) {
    if (output == nullptr) {
        return Status::InvalidArgument("variant_typeof output is null");
    }
    if (!outer_nulls.empty() && outer_nulls.size() != source.size()) {
        return Status::InvalidArgument("variant_typeof null map has {} rows, expected {}",
                                       outer_nulls.size(), source.size());
    }

    auto values = ColumnString::create();
    values->reserve(source.size());
    auto nulls = ColumnUInt8::create(source.size(), uint8_t {0});
    try {
        visit_variant_v2_values(
                source, 0, source.size(), outer_nulls,
                [&](size_t row) {
                    values->insert_default();
                    nulls->get_data()[row] = 1;
                },
                [&](size_t, VariantRef value) {
                    const VariantSchema schema = infer_schema(value, 0);
                    VectorBufferWriter writer(*values);
                    write_schema(schema, &writer);
                    writer.commit();
                });
    } catch (const Exception& exception) {
        return exception.to_status();
    }

    DORIS_CHECK_EQ(values->size(), source.size());
    ColumnPtr candidate = ColumnNullable::create(std::move(values), std::move(nulls));
    output->swap(candidate);
    return Status::OK();
}

void register_function_variant_typeof(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionVariantTypeof>();
}

} // namespace doris
