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

#include "storage/segment/variant/v2/variant_path_builder.h"

#include <cctz/time_zone.h>

#include <algorithm>
#include <limits>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_array.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nothing.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/data_type/get_least_supertype.h"
#include "core/data_type/primitive_type.h"
#include "core/typeid_cast.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "util/jsonb_document.h"
#include "util/jsonb_writer.h"

namespace doris::segment_v2 {
namespace {

enum class ScratchKind : uint8_t {
    NULL_VALUE,
    BOOL,
    INT64,
    LARGEINT,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    STRING,
    JSONB_REF,
    ARRAY,
};

struct ValueScratch {
    ScratchKind kind = ScratchKind::NULL_VALUE;
    bool bool_value = false;
    int64_t int_value = 0;
    PrimitiveType int_type = INVALID_TYPE;
    float float_value = 0;
    double double_value = 0;
    VariantDecimal decimal_value;
    int32_t date_days = 0;
    int64_t timestamp_micros = 0;
    StringRef string_value;
    VariantRef jsonb_ref;
    DorisVector<ValueScratch> elements;
};

DataTypePtr path_least_common_type(const DataTypePtr& left, const DataTypePtr& right);

bool date_fits_doris_range(int32_t days) {
    const cctz::civil_day civil = cctz::civil_day(1970, 1, 1) + days;
    return civil.year() >= 1 && civil.year() <= 9999;
}

std::pair<int64_t, uint32_t> split_epoch_micros(int64_t micros) {
    constexpr int64_t MICROS_PER_SECOND = 1'000'000;
    int64_t seconds = micros / MICROS_PER_SECOND;
    int64_t fraction = micros % MICROS_PER_SECOND;
    if (fraction < 0) {
        --seconds;
        fraction += MICROS_PER_SECOND;
    }
    return {seconds, static_cast<uint32_t>(fraction)};
}

bool epoch_micros_to_civil(int64_t micros, cctz::civil_second* civil, uint32_t* fraction) {
    const auto [seconds, micros_fraction] = split_epoch_micros(micros);
    const auto lookup =
            cctz::utc_time_zone().lookup(cctz::time_point<cctz::seconds>(cctz::seconds(seconds)));
    if (lookup.cs.year() < 1 || lookup.cs.year() > 9999) {
        return false;
    }
    *civil = lookup.cs;
    *fraction = micros_fraction;
    return true;
}

bool timestamp_fits_doris_range(int64_t micros) {
    cctz::civil_second civil;
    uint32_t fraction = 0;
    return epoch_micros_to_civil(micros, &civil, &fraction);
}

// Keep the exhaustive primitive-id mapping in one switch so newly added ids cannot bypass the
// explicit storage fallback.
// NOLINTNEXTLINE(readability-function-size)
ValueScratch collect_primitive(VariantRef value) {
    ValueScratch result;
    switch (value.primitive_id()) {
    case VariantPrimitiveId::NULL_VALUE:
        result.kind = ScratchKind::NULL_VALUE;
        return result;
    case VariantPrimitiveId::TRUE_VALUE:
    case VariantPrimitiveId::FALSE_VALUE:
        result.kind = ScratchKind::BOOL;
        result.bool_value = value.get_bool();
        return result;
    case VariantPrimitiveId::INT8:
        result.kind = ScratchKind::INT64;
        result.int_value = value.get_int();
        result.int_type = TYPE_TINYINT;
        return result;
    case VariantPrimitiveId::INT16:
        result.kind = ScratchKind::INT64;
        result.int_value = value.get_int();
        result.int_type = TYPE_SMALLINT;
        return result;
    case VariantPrimitiveId::INT32:
        result.kind = ScratchKind::INT64;
        result.int_value = value.get_int();
        result.int_type = TYPE_INT;
        return result;
    case VariantPrimitiveId::INT64:
        result.kind = ScratchKind::INT64;
        result.int_value = value.get_int();
        result.int_type = TYPE_BIGINT;
        return result;
    case VariantPrimitiveId::FLOAT:
        result.kind = ScratchKind::FLOAT;
        result.float_value = value.get_float();
        return result;
    case VariantPrimitiveId::DOUBLE:
        result.kind = ScratchKind::DOUBLE;
        result.double_value = value.get_double();
        return result;
    case VariantPrimitiveId::DECIMAL4:
    case VariantPrimitiveId::DECIMAL8:
    case VariantPrimitiveId::DECIMAL16: {
        const VariantDecimal decimal = value.get_decimal();
        if (decimal.width == 16 && decimal.scale == 0) {
            result.kind = ScratchKind::LARGEINT;
            result.decimal_value = decimal;
            return result;
        }
        result.kind = ScratchKind::DECIMAL;
        result.decimal_value = decimal;
        return result;
    }
    case VariantPrimitiveId::STRING:
        result.kind = ScratchKind::STRING;
        result.string_value = value.get_string();
        return result;
    case VariantPrimitiveId::DATE:
        result.kind = ScratchKind::DATE;
        result.date_days = value.get_date();
        result.jsonb_ref = value;
        return result;
    case VariantPrimitiveId::TIMESTAMP_MICROS:
        result.kind = ScratchKind::TIMESTAMP_TZ;
        result.timestamp_micros = value.get_timestamp_micros();
        result.jsonb_ref = value;
        return result;
    case VariantPrimitiveId::TIMESTAMP_NTZ_MICROS:
        result.kind = ScratchKind::TIMESTAMP_NTZ;
        result.timestamp_micros = value.get_timestamp_ntz_micros();
        result.jsonb_ref = value;
        return result;
    case VariantPrimitiveId::BINARY:
    case VariantPrimitiveId::TIME_NTZ_MICROS:
    case VariantPrimitiveId::TIMESTAMP_NANOS:
    case VariantPrimitiveId::TIMESTAMP_NTZ_NANOS:
    case VariantPrimitiveId::UUID:
        result.kind = ScratchKind::JSONB_REF;
        result.jsonb_ref = value;
        return result;
    }
    throw Exception(ErrorCode::CORRUPTION, "Unknown Variant primitive id");
}

ValueScratch collect_value(VariantRef value) {
    ValueScratch result;
    switch (value.basic_type()) {
    case VariantBasicType::SHORT_STRING:
        result.kind = ScratchKind::STRING;
        result.string_value = value.get_string();
        return result;
    case VariantBasicType::OBJECT:
        result.kind = ScratchKind::JSONB_REF;
        result.jsonb_ref = value;
        return result;
    case VariantBasicType::ARRAY: {
        result.kind = ScratchKind::ARRAY;
        const uint32_t count = value.num_elements();
        result.elements.reserve(count);
        for (uint32_t index = 0; index < count; ++index) {
            result.elements.push_back(collect_value(value.array_at(index)));
        }
        return result;
    }
    case VariantBasicType::PRIMITIVE:
        return collect_primitive(value);
    }
    throw Exception(ErrorCode::CORRUPTION, "Unknown Variant basic type");
}

DataTypePtr infer_type(const ValueScratch& value) {
    switch (value.kind) {
    case ScratchKind::NULL_VALUE:
        return std::make_shared<DataTypeNothing>();
    case ScratchKind::BOOL:
        return std::make_shared<DataTypeBool>();
    case ScratchKind::INT64:
        return DataTypeFactory::instance().create_data_type(value.int_type, false);
    case ScratchKind::LARGEINT:
        return std::make_shared<DataTypeInt128>();
    case ScratchKind::FLOAT:
        return std::make_shared<DataTypeFloat32>();
    case ScratchKind::DOUBLE:
        return std::make_shared<DataTypeFloat64>();
    case ScratchKind::DECIMAL:
        return std::make_shared<DataTypeDecimal128>(38, value.decimal_value.scale);
    case ScratchKind::DATE:
        if (!date_fits_doris_range(value.date_days)) {
            return std::make_shared<DataTypeJsonb>();
        }
        return std::make_shared<DataTypeDateV2>();
    case ScratchKind::TIMESTAMP_NTZ:
        if (!timestamp_fits_doris_range(value.timestamp_micros)) {
            return std::make_shared<DataTypeJsonb>();
        }
        return std::make_shared<DataTypeDateTimeV2>(6);
    case ScratchKind::TIMESTAMP_TZ:
        if (!timestamp_fits_doris_range(value.timestamp_micros)) {
            return std::make_shared<DataTypeJsonb>();
        }
        return std::make_shared<DataTypeTimeStampTz>(6);
    case ScratchKind::STRING:
        return std::make_shared<DataTypeString>();
    case ScratchKind::JSONB_REF:
        return std::make_shared<DataTypeJsonb>();
    case ScratchKind::ARRAY:
        break;
    }

    DataTypePtr element_type;
    for (const ValueScratch& element : value.elements) {
        if (element.kind == ScratchKind::ARRAY ||
            (element.kind == ScratchKind::JSONB_REF &&
             element.jsonb_ref.basic_type() == VariantBasicType::OBJECT)) {
            return std::make_shared<DataTypeJsonb>();
        }
        DataTypePtr inferred = infer_type(element);
        if (inferred->get_primitive_type() == INVALID_TYPE) {
            continue;
        }
        element_type = element_type == nullptr ? std::move(inferred)
                                               : path_least_common_type(element_type, inferred);
    }

    if (element_type == nullptr) {
        element_type = std::make_shared<DataTypeNothing>();
    }
    return std::make_shared<DataTypeArray>(element_type);
}

bool is_small_or_regular_integer(PrimitiveType type) {
    return type == TYPE_TINYINT || type == TYPE_SMALLINT || type == TYPE_INT || type == TYPE_BIGINT;
}

size_t array_dimensions(const DataTypePtr& type) {
    size_t dimensions = 0;
    DataTypePtr current = remove_nullable(type);
    while (const auto* array = typeid_cast<const DataTypeArray*>(current.get())) {
        ++dimensions;
        current = remove_nullable(array->get_nested_type());
    }
    return dimensions;
}

DataTypePtr path_least_common_type(const DataTypePtr& left, const DataTypePtr& right) {
    if (left.get() == right.get() || left->equals(*right)) {
        return left;
    }
    const auto* left_array = typeid_cast<const DataTypeArray*>(left.get());
    const auto* right_array = typeid_cast<const DataTypeArray*>(right.get());
    if (left_array != nullptr || right_array != nullptr) {
        if (left_array == nullptr || right_array == nullptr) {
            return std::make_shared<DataTypeJsonb>();
        }
        if (array_dimensions(left) != array_dimensions(right)) {
            return std::make_shared<DataTypeJsonb>();
        }
        return std::make_shared<DataTypeArray>(path_least_common_type(
                left_array->get_nested_type(), right_array->get_nested_type()));
    }
    const PrimitiveType left_primitive = left->get_primitive_type();
    const PrimitiveType right_primitive = right->get_primitive_type();
    const bool left_decimal = left_primitive == TYPE_DECIMAL128I;
    const bool right_decimal = right_primitive == TYPE_DECIMAL128I;
    if (left_decimal && right_decimal) {
        return std::make_shared<DataTypeDecimal128>(
                38, std::max(left->get_scale(), right->get_scale()));
    }
    if ((left_decimal && is_small_or_regular_integer(right_primitive)) ||
        (right_decimal && is_small_or_regular_integer(left_primitive))) {
        const DataTypePtr& decimal = left_decimal ? left : right;
        if (decimal->get_scale() <= 19) {
            return std::make_shared<DataTypeDecimal128>(38, decimal->get_scale());
        }
        return std::make_shared<DataTypeJsonb>();
    }
    DataTypePtr result;
    get_least_supertype_jsonb(DataTypes {left, right}, &result);
    return result ? result : std::make_shared<DataTypeJsonb>();
}

bool rescale_decimal(__int128 source, uint32_t source_scale, uint32_t target_scale,
                     __int128* result) {
    if (source_scale == target_scale) {
        *result = source;
        return true;
    }
    if (source_scale < target_scale) {
        __int128 value = source;
        for (uint32_t scale = source_scale; scale < target_scale; ++scale) {
            if (__builtin_mul_overflow(value, static_cast<__int128>(10), &value)) {
                return false;
            }
        }
        *result = value;
        return true;
    }
    __int128 divisor = 1;
    for (uint32_t scale = target_scale; scale < source_scale; ++scale) {
        if (__builtin_mul_overflow(divisor, static_cast<__int128>(10), &divisor)) {
            return false;
        }
    }
    if (source % divisor != 0) {
        return false;
    }
    *result = source / divisor;
    return true;
}

bool try_rescale_decimal_value(const ValueScratch& value, const DataTypePtr& target_type,
                               __int128* result) {
    if (value.kind != ScratchKind::DECIMAL && value.kind != ScratchKind::INT64) {
        return false;
    }
    const uint32_t source_scale =
            value.kind == ScratchKind::DECIMAL ? value.decimal_value.scale : 0;
    const __int128 source_value = value.kind == ScratchKind::DECIMAL
                                          ? value.decimal_value.unscaled
                                          : static_cast<__int128>(value.int_value);
    if (!rescale_decimal(source_value, source_scale, target_type->get_scale(), result)) {
        return false;
    }
    const __int128 max_value =
            DataTypeDecimal128::get_max_digits_number(target_type->get_precision());
    return *result >= -max_value && *result <= max_value;
}

bool value_is_representable(const ValueScratch& value, const DataTypePtr& target_type) {
    switch (target_type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        return value.kind == ScratchKind::BOOL;
    case TYPE_TINYINT:
        return value.kind == ScratchKind::INT64 &&
               value.int_value >= std::numeric_limits<int8_t>::min() &&
               value.int_value <= std::numeric_limits<int8_t>::max();
    case TYPE_SMALLINT:
        return value.kind == ScratchKind::INT64 &&
               value.int_value >= std::numeric_limits<int16_t>::min() &&
               value.int_value <= std::numeric_limits<int16_t>::max();
    case TYPE_INT:
        return value.kind == ScratchKind::INT64 &&
               value.int_value >= std::numeric_limits<int32_t>::min() &&
               value.int_value <= std::numeric_limits<int32_t>::max();
    case TYPE_BIGINT:
        return value.kind == ScratchKind::INT64;
    case TYPE_LARGEINT:
        return value.kind == ScratchKind::LARGEINT || value.kind == ScratchKind::INT64;
    case TYPE_FLOAT:
        return value.kind == ScratchKind::FLOAT;
    case TYPE_DOUBLE:
        return value.kind == ScratchKind::DOUBLE || value.kind == ScratchKind::FLOAT ||
               value.kind == ScratchKind::INT64;
    case TYPE_DECIMAL128I: {
        __int128 converted = 0;
        return try_rescale_decimal_value(value, target_type, &converted);
    }
    case TYPE_DATEV2:
        return value.kind == ScratchKind::DATE && date_fits_doris_range(value.date_days);
    case TYPE_DATETIMEV2:
        return value.kind == ScratchKind::TIMESTAMP_NTZ &&
               timestamp_fits_doris_range(value.timestamp_micros);
    case TYPE_TIMESTAMPTZ:
        return value.kind == ScratchKind::TIMESTAMP_TZ &&
               timestamp_fits_doris_range(value.timestamp_micros);
    case TYPE_STRING:
        return value.kind == ScratchKind::STRING;
    case TYPE_JSONB:
        return true;
    case TYPE_ARRAY: {
        if (value.kind != ScratchKind::ARRAY) {
            return false;
        }
        const DataTypePtr element_type =
                remove_nullable(assert_cast<const DataTypeArray&>(*target_type).get_nested_type());
        return std::ranges::all_of(value.elements, [&](const ValueScratch& element) {
            return element.kind == ScratchKind::NULL_VALUE ||
                   value_is_representable(element, element_type);
        });
    }
    case INVALID_TYPE:
        return value.kind == ScratchKind::NULL_VALUE;
    default:
        return false;
    }
}

void require_jsonb_write(bool ok, std::string_view description) {
    if (!ok) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Failed to write {} to JSONB", description);
    }
}

void write_scratch_jsonb(const ValueScratch& value, JsonbWriter* writer) {
    switch (value.kind) {
    case ScratchKind::NULL_VALUE:
        require_jsonb_write(writer->writeNull(), "null");
        return;
    case ScratchKind::BOOL:
        require_jsonb_write(writer->writeBool(value.bool_value), "boolean");
        return;
    case ScratchKind::INT64:
        require_jsonb_write(writer->writeInt64(value.int_value), "integer");
        return;
    case ScratchKind::LARGEINT:
        require_jsonb_write(writer->writeInt128(value.decimal_value.unscaled), "large integer");
        return;
    case ScratchKind::FLOAT:
        require_jsonb_write(writer->writeFloat(value.float_value), "float");
        return;
    case ScratchKind::DOUBLE:
        require_jsonb_write(writer->writeDouble(value.double_value), "double");
        return;
    case ScratchKind::DECIMAL:
        require_jsonb_write(writer->writeDecimal(Decimal128V3 {value.decimal_value.unscaled}, 38,
                                                 value.decimal_value.scale),
                            "decimal");
        return;
    case ScratchKind::STRING:
        require_jsonb_write(writer->writeStartString(), "string start");
        require_jsonb_write(writer->writeString(value.string_value.data, value.string_value.size),
                            "string bytes");
        require_jsonb_write(writer->writeEndString(), "string end");
        return;
    case ScratchKind::DATE:
    case ScratchKind::TIMESTAMP_NTZ:
    case ScratchKind::TIMESTAMP_TZ:
    case ScratchKind::JSONB_REF: {
        JsonbWriter nested;
        variant_to_jsonb(value.jsonb_ref, nested);
        const JsonbValue* nested_value = JsonbDocument::createValue(nested.getOutput()->getBuffer(),
                                                                    nested.getOutput()->getSize());
        require_jsonb_write(writer->writeValue(nested_value), "Variant subtree");
        return;
    }
    case ScratchKind::ARRAY:
        require_jsonb_write(writer->writeStartArray(), "array start");
        for (const ValueScratch& element : value.elements) {
            write_scratch_jsonb(element, writer);
        }
        require_jsonb_write(writer->writeEndArray(), "array end");
        return;
    }
}

void append_default(const DataTypePtr& type, IColumn* column) {
    switch (type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        assert_cast<ColumnUInt8&>(*column).insert_value(0);
        return;
    case TYPE_BIGINT:
        assert_cast<ColumnInt64&>(*column).insert_value(0);
        return;
    case TYPE_TINYINT:
        assert_cast<ColumnInt8&>(*column).insert_value(0);
        return;
    case TYPE_SMALLINT:
        assert_cast<ColumnInt16&>(*column).insert_value(0);
        return;
    case TYPE_INT:
        assert_cast<ColumnInt32&>(*column).insert_value(0);
        return;
    case TYPE_LARGEINT:
        assert_cast<ColumnInt128&>(*column).insert_value(0);
        return;
    case TYPE_FLOAT:
        assert_cast<ColumnFloat32&>(*column).insert_value(0);
        return;
    case TYPE_DOUBLE:
        assert_cast<ColumnFloat64&>(*column).insert_value(0);
        return;
    case TYPE_DECIMAL128I:
        assert_cast<ColumnDecimal128V3&>(*column).insert_value(Decimal128V3 {});
        return;
    case TYPE_DATEV2:
        assert_cast<ColumnDateV2&>(*column).insert_default();
        return;
    case TYPE_DATETIMEV2:
        assert_cast<ColumnDateTimeV2&>(*column).insert_default();
        return;
    case TYPE_TIMESTAMPTZ:
        assert_cast<ColumnTimeStampTz&>(*column).insert_default();
        return;
    case TYPE_STRING:
    case TYPE_JSONB:
        assert_cast<ColumnString&>(*column).insert_default();
        return;
    case TYPE_ARRAY: {
        auto& array = assert_cast<ColumnArray&>(*column);
        array.get_offsets().push_back(array.get_data().size());
        return;
    }
    case INVALID_TYPE:
        assert_cast<ColumnNothing&>(*column).insert_default();
        return;
    default:
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant path builder cannot append a default for type {}",
                        type->get_name());
    }
}

void append_jsonb(const ValueScratch& value, ColumnString* column) {
    JsonbWriter writer;
    write_scratch_jsonb(value, &writer);
    column->insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
}

void append_integer(const ValueScratch& value, PrimitiveType target_type, IColumn* target) {
    if (value.kind != ScratchKind::INT64) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant value to integer path builder");
    }
    switch (target_type) {
    case TYPE_TINYINT:
        assert_cast<ColumnInt8&>(*target).insert_value(static_cast<int8_t>(value.int_value));
        return;
    case TYPE_SMALLINT:
        assert_cast<ColumnInt16&>(*target).insert_value(static_cast<int16_t>(value.int_value));
        return;
    case TYPE_INT:
        assert_cast<ColumnInt32&>(*target).insert_value(static_cast<int32_t>(value.int_value));
        return;
    case TYPE_BIGINT:
        assert_cast<ColumnInt64&>(*target).insert_value(value.int_value);
        return;
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid integer target type {}", target_type);
    }
}

void append_largeint(const ValueScratch& value, IColumn* target) {
    __int128 converted = 0;
    if (value.kind == ScratchKind::LARGEINT) {
        converted = value.decimal_value.unscaled;
    } else if (value.kind == ScratchKind::INT64) {
        converted = value.int_value;
    } else {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant value to LARGEINT path builder");
    }
    assert_cast<ColumnInt128&>(*target).insert_value(converted);
}

void append_floating(const ValueScratch& value, PrimitiveType target_type, IColumn* target) {
    if (target_type == TYPE_FLOAT) {
        if (value.kind != ScratchKind::FLOAT) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot append Variant value to FLOAT path builder");
        }
        assert_cast<ColumnFloat32&>(*target).insert_value(value.float_value);
        return;
    }
    double converted = 0;
    if (value.kind == ScratchKind::DOUBLE) {
        converted = value.double_value;
    } else if (value.kind == ScratchKind::FLOAT) {
        converted = value.float_value;
    } else if (value.kind == ScratchKind::INT64) {
        converted = value.int_value;
    } else {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant value to DOUBLE path builder");
    }
    assert_cast<ColumnFloat64&>(*target).insert_value(converted);
}

void append_decimal(const ValueScratch& value, const DataTypePtr& target_type, IColumn* target) {
    if (value.kind != ScratchKind::DECIMAL && value.kind != ScratchKind::INT64) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant value to DECIMAL path builder");
    }
    __int128 converted = 0;
    if (!try_rescale_decimal_value(value, target_type, &converted)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant decimal value cannot be represented at scale {}",
                        target_type->get_scale());
    }
    assert_cast<ColumnDecimal128V3&>(*target).insert_value(Decimal128V3 {converted});
}

void append_date(const ValueScratch& value, IColumn* target) {
    if (value.kind != ScratchKind::DATE) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant value to DATEV2 path builder");
    }
    const cctz::civil_day civil = cctz::civil_day(1970, 1, 1) + value.date_days;
    if (civil.year() < 1 || civil.year() > 9999) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant date is outside the Doris DATEV2 range");
    }
    DateV2Value<DateV2ValueType> converted;
    converted.unchecked_set_time(static_cast<uint16_t>(civil.year()),
                                 static_cast<uint8_t>(civil.month()),
                                 static_cast<uint8_t>(civil.day()), 0, 0, 0);
    assert_cast<ColumnDateV2&>(*target).insert_value(converted);
}

void append_timestamp(const ValueScratch& value, PrimitiveType target_type, IColumn* target) {
    cctz::civil_second civil;
    uint32_t fraction = 0;
    if (!epoch_micros_to_civil(value.timestamp_micros, &civil, &fraction)) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant timestamp is outside the Doris datetime range");
    }
    if (target_type == TYPE_DATETIMEV2) {
        if (value.kind != ScratchKind::TIMESTAMP_NTZ) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot append timezone-adjusted Variant timestamp to DATETIMEV2 "
                            "path builder");
        }
        DateV2Value<DateTimeV2ValueType> converted;
        converted.unchecked_set_time(
                static_cast<uint16_t>(civil.year()), static_cast<uint8_t>(civil.month()),
                static_cast<uint8_t>(civil.day()), static_cast<uint8_t>(civil.hour()),
                static_cast<uint8_t>(civil.minute()), static_cast<uint8_t>(civil.second()),
                fraction);
        assert_cast<ColumnDateTimeV2&>(*target).insert_value(converted);
        return;
    }
    if (target_type != TYPE_TIMESTAMPTZ || value.kind != ScratchKind::TIMESTAMP_TZ) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append Variant timestamp to TIMESTAMPTZ path builder");
    }
    TimestampTzValue converted;
    converted.unchecked_set_time(
            static_cast<uint16_t>(civil.year()), static_cast<uint8_t>(civil.month()),
            static_cast<uint8_t>(civil.day()), static_cast<uint8_t>(civil.hour()),
            static_cast<uint8_t>(civil.minute()), static_cast<uint8_t>(civil.second()), fraction);
    assert_cast<ColumnTimeStampTz&>(*target).insert_value(converted);
}

void append_value(const ValueScratch& value, const DataTypePtr& target_type, IColumn* target);

void append_array(const ValueScratch& value, const DataTypePtr& target_type, IColumn* target) {
    if (value.kind != ScratchKind::ARRAY) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Cannot append scalar Variant value to ARRAY path builder");
    }
    const auto& array_type = assert_cast<const DataTypeArray&>(*target_type);
    auto& array = assert_cast<ColumnArray&>(*target);
    auto& elements = assert_cast<ColumnNullable&>(array.get_data());
    const DataTypePtr element_type = remove_nullable(array_type.get_nested_type());
    for (const ValueScratch& element : value.elements) {
        if (element.kind == ScratchKind::NULL_VALUE) {
            append_default(element_type, &elements.get_nested_column());
            elements.get_null_map_data().push_back(1);
        } else {
            append_value(element, element_type, &elements.get_nested_column());
            elements.get_null_map_data().push_back(0);
        }
    }
    array.get_offsets().push_back(elements.size());
}

void append_value(const ValueScratch& value, const DataTypePtr& target_type, IColumn* target) {
    switch (target_type->get_primitive_type()) {
    case TYPE_BOOLEAN:
        if (value.kind != ScratchKind::BOOL) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot append Variant value to BOOLEAN path builder");
        }
        assert_cast<ColumnUInt8&>(*target).insert_value(value.bool_value);
        return;
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
        append_integer(value, target_type->get_primitive_type(), target);
        return;
    case TYPE_LARGEINT:
        append_largeint(value, target);
        return;
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
        append_floating(value, target_type->get_primitive_type(), target);
        return;
    case TYPE_DECIMAL128I:
        append_decimal(value, target_type, target);
        return;
    case TYPE_DATEV2:
        append_date(value, target);
        return;
    case TYPE_DATETIMEV2:
    case TYPE_TIMESTAMPTZ:
        append_timestamp(value, target_type->get_primitive_type(), target);
        return;
    case TYPE_STRING:
        if (value.kind != ScratchKind::STRING) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot append Variant value to STRING path builder");
        }
        assert_cast<ColumnString&>(*target).insert_data(value.string_value.data,
                                                        value.string_value.size);
        return;
    case TYPE_JSONB:
        append_jsonb(value, &assert_cast<ColumnString&>(*target));
        return;
    case TYPE_ARRAY:
        append_array(value, target_type, target);
        return;
    case INVALID_TYPE:
        if (value.kind != ScratchKind::NULL_VALUE) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Cannot append non-null Variant value to Nothing path builder");
        }
        assert_cast<ColumnNothing&>(*target).insert_default();
        return;
    default:
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant path builder does not support target type {}",
                        target_type->get_name());
    }
}

bool contains_nothing(const DataTypePtr& type) {
    const DataTypePtr nested = remove_nullable(type);
    if (nested->get_primitive_type() == INVALID_TYPE) {
        return true;
    }
    if (nested->get_primitive_type() != TYPE_ARRAY) {
        return false;
    }
    return contains_nothing(assert_cast<const DataTypeArray&>(*nested).get_nested_type());
}

Status stringify_complex_column(const DataTypePtr& source_type, const ColumnNullable& source,
                                ColumnPtr* result) {
    auto jsonb = ColumnString::create();
    RETURN_IF_ERROR(source_type->get_serde()->serialize_column_to_jsonb_vector(
            source.get_nested_column(), *jsonb));

    auto strings = ColumnString::create();
    DataTypeSerDe::FormatOptions options;
    DataTypeJsonb().get_serde()->to_string_batch(*jsonb, *strings, options);
    *result = ColumnNullable::create(std::move(strings),
                                     source.get_null_map_column().clone_resized(source.size()));
    return Status::OK();
}

Status replace_array_nothing(const DataTypePtr& source_type, const IColumn& source,
                             const DataTypePtr& target_type, MutableColumnPtr* result) {
    if (source_type->is_nullable()) {
        const auto* target_nullable = typeid_cast<const DataTypeNullable*>(target_type.get());
        if (target_nullable == nullptr) {
            return Status::InvalidArgument("Cannot convert nullable Variant array path to {}",
                                           target_type->get_name());
        }
        const auto& source_nullable = assert_cast<const ColumnNullable&>(source);
        MutableColumnPtr nested;
        RETURN_IF_ERROR(replace_array_nothing(
                assert_cast<const DataTypeNullable&>(*source_type).get_nested_type(),
                source_nullable.get_nested_column(), target_nullable->get_nested_type(), &nested));
        *result = ColumnNullable::create(
                std::move(nested),
                source_nullable.get_null_map_column().clone_resized(source.size()));
        return Status::OK();
    }

    if (source_type->get_primitive_type() == INVALID_TYPE) {
        if (check_and_get_column<ColumnNothing>(&source) == nullptr) {
            return Status::InternalError("Variant Nothing type does not use ColumnNothing");
        }
        MutableColumnPtr materialized = target_type->create_column();
        materialized->insert_many_defaults(source.size());
        *result = std::move(materialized);
        return Status::OK();
    }

    if (source_type->get_primitive_type() != TYPE_ARRAY ||
        target_type->get_primitive_type() != TYPE_ARRAY) {
        return Status::InvalidArgument(
                "Cannot preserve Variant array shape while converting {} to {}",
                source_type->get_name(), target_type->get_name());
    }
    const auto& source_array_type = assert_cast<const DataTypeArray&>(*source_type);
    const auto& target_array_type = assert_cast<const DataTypeArray&>(*target_type);
    const auto& source_array = assert_cast<const ColumnArray&>(source);
    MutableColumnPtr nested;
    RETURN_IF_ERROR(replace_array_nothing(source_array_type.get_nested_type(),
                                          source_array.get_data(),
                                          target_array_type.get_nested_type(), &nested));
    *result = ColumnArray::create(
            std::move(nested),
            source_array.get_offsets_column().clone_resized(source_array.size()));
    return Status::OK();
}

size_t dotted_path_depth(const PathInData& path) {
    return path.get_parts().size();
}

size_t path_allocated_bytes(const PathInData& path) {
    return path.get_path().capacity() + path.get_parts().capacity() * sizeof(PathInData::Part);
}

size_t recursive_null_count(const IColumn& column) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        size_t count = 0;
        for (UInt8 is_null : nullable->get_null_map_data()) {
            count += is_null != 0;
        }
        return count + recursive_null_count(nullable->get_nested_column());
    }
    if (const auto* array = check_and_get_column<ColumnArray>(column)) {
        return recursive_null_count(array->get_data());
    }
    return 0;
}

bool cast_introduced_null(const IColumn& source, const IColumn& target) {
    const auto* source_nullable = check_and_get_column<ColumnNullable>(source);
    const auto* target_nullable = check_and_get_column<ColumnNullable>(target);
    if (source_nullable != nullptr || target_nullable != nullptr) {
        if (source_nullable != nullptr && target_nullable != nullptr &&
            source_nullable->size() == target_nullable->size()) {
            for (size_t index = 0; index < source_nullable->size(); ++index) {
                if (!source_nullable->is_null_at(index) && target_nullable->is_null_at(index)) {
                    return true;
                }
            }
            return cast_introduced_null(source_nullable->get_nested_column(),
                                        target_nullable->get_nested_column());
        }
        return recursive_null_count(target) > recursive_null_count(source);
    }

    const auto* source_array = check_and_get_column<ColumnArray>(source);
    const auto* target_array = check_and_get_column<ColumnArray>(target);
    if (source_array != nullptr || target_array != nullptr) {
        if (source_array != nullptr && target_array != nullptr &&
            source_array->size() == target_array->size()) {
            return cast_introduced_null(source_array->get_data(), target_array->get_data());
        }
        return recursive_null_count(target) > recursive_null_count(source);
    }
    return false;
}

} // namespace

struct VariantPathBuilder::Impl {
    explicit Impl(PathInData path_, size_t prefix_rows_)
            : path(path_), logical_rows(prefix_rows_) {}

    Status initialize(const DataTypePtr& initial_type) {
        type = remove_nullable(initial_type);
        nullable_type = make_nullable(type);
        column = nullable_type->create_column();
        return Status::OK();
    }

    Status promote(const DataTypePtr& target_type, bool filter_cast_nulls) {
        DataTypePtr target = remove_nullable(target_type);
        if (target->equals(*type)) {
            return Status::OK();
        }
        ColumnPtr promoted;
        if (target->get_primitive_type() == TYPE_STRING &&
            type->get_primitive_type() == TYPE_ARRAY) {
            RETURN_IF_ERROR(stringify_complex_column(
                    type, assert_cast<const ColumnNullable&>(*column), &promoted));
        } else if (type->get_primitive_type() == TYPE_ARRAY &&
                   target->get_primitive_type() == TYPE_ARRAY && contains_nothing(type)) {
            MutableColumnPtr materialized;
            RETURN_IF_ERROR(replace_array_nothing(nullable_type, *column, make_nullable(target),
                                                  &materialized));
            promoted = std::move(materialized);
        } else {
            RETURN_IF_ERROR(
                    variant_util::cast_column({column->get_ptr(), nullable_type, path.get_path()},
                                              make_nullable(target), &promoted));
        }
        // Inferred widening is only a storage representation choice. If CAST loses a valid value
        // at any array depth, preserve the whole path as JSONB instead. Forced typed-path
        // conversion intentionally retains its existing cast-null filtering below.
        if (!filter_cast_nulls && target->get_primitive_type() != TYPE_JSONB &&
            cast_introduced_null(*column, *promoted)) {
            target = std::make_shared<DataTypeJsonb>();
            RETURN_IF_ERROR(
                    variant_util::cast_column({column->get_ptr(), nullable_type, path.get_path()},
                                              make_nullable(target), &promoted));
            DORIS_CHECK(!assert_cast<const ColumnNullable&>(*promoted).has_null());
        }
        const auto& nullable = assert_cast<const ColumnNullable&>(*promoted);
        if (nullable.has_null()) {
            DORIS_CHECK(filter_cast_nulls);
            DORIS_CHECK_EQ(promoted->size(), rowids.size());
            IColumn::Filter non_null_filter(promoted->size(), 1);
            DorisVector<uint32_t> non_null_rowids;
            non_null_rowids.reserve(rowids.size());
            for (size_t index = 0; index < rowids.size(); ++index) {
                if (nullable.is_null_at(index)) {
                    non_null_filter[index] = 0;
                } else {
                    non_null_rowids.push_back(rowids[index]);
                }
            }
            promoted =
                    promoted->filter(non_null_filter, static_cast<ssize_t>(non_null_rowids.size()));
            rowids = std::move(non_null_rowids);
            non_null = static_cast<uint32_t>(rowids.size());
        }
        column = IColumn::mutate(std::move(promoted));
        type = std::move(target);
        nullable_type = make_nullable(type);
        ++promotions;
        return Status::OK();
    }

    PathInData path;
    DataTypePtr type;
    DataTypePtr nullable_type;
    MutableColumnPtr column;
    DorisVector<uint32_t> rowids;
    size_t logical_rows = 0;
    uint32_t non_null = 0;
    size_t promotions = 0;
};

VariantPathBuilder::VariantPathBuilder(PathInData path, size_t prefix_rows)
        : _impl(std::make_unique<Impl>(std::move(path), prefix_rows)) {}
VariantPathBuilder::~VariantPathBuilder() = default;
VariantPathBuilder::VariantPathBuilder(VariantPathBuilder&&) noexcept = default;
VariantPathBuilder& VariantPathBuilder::operator=(VariantPathBuilder&&) noexcept = default;

Status VariantPathBuilder::append(VariantRef value, size_t row) {
    try {
        if (value.is_null()) {
            return Status::InvalidArgument("Variant path builder {} must not append JSON null",
                                           _impl->path.get_path());
        }
        if (row < _impl->logical_rows) {
            return Status::InvalidArgument("Variant path builder {} already has row {}",
                                           _impl->path.get_path(), row);
        }
        if (row > std::numeric_limits<uint32_t>::max()) {
            return Status::InvalidArgument("Variant path builder {} row {} exceeds uint32 limit",
                                           _impl->path.get_path(), row);
        }
        RETURN_IF_ERROR(complete_rows(row));

        ValueScratch scratch = collect_value(value);
        DataTypePtr inferred_type = infer_type(scratch);
        if (!_impl->column) {
            RETURN_IF_ERROR(_impl->initialize(inferred_type));
        } else {
            DataTypePtr common_type = path_least_common_type(_impl->type, inferred_type);
            RETURN_IF_ERROR(_impl->promote(common_type, false));
        }
        if (scratch.kind == ScratchKind::ARRAY && !value_is_representable(scratch, _impl->type)) {
            RETURN_IF_ERROR(_impl->promote(std::make_shared<DataTypeJsonb>(), false));
        }

        try {
            auto& nullable = assert_cast<ColumnNullable&>(*_impl->column);
            append_value(scratch, _impl->type, &nullable.get_nested_column());
            nullable.get_null_map_data().push_back(0);
        } catch (const Exception&) {
            if (scratch.kind == ScratchKind::ARRAY ||
                _impl->type->get_primitive_type() == TYPE_JSONB) {
                throw;
            }
            RETURN_IF_ERROR(_impl->promote(std::make_shared<DataTypeJsonb>(), false));
            auto& nullable = assert_cast<ColumnNullable&>(*_impl->column);
            append_value(scratch, _impl->type, &nullable.get_nested_column());
            nullable.get_null_map_data().push_back(0);
        }
        _impl->rowids.push_back(static_cast<uint32_t>(row));
        _impl->logical_rows = row + 1;
        ++_impl->non_null;
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

Status VariantPathBuilder::complete_rows(size_t rows) {
    if (rows < _impl->logical_rows) {
        return Status::InvalidArgument("Variant path builder {} cannot shrink from {} to {} rows",
                                       _impl->path.get_path(), _impl->logical_rows, rows);
    }
    _impl->logical_rows = rows;
    return Status::OK();
}

Status VariantPathBuilder::convert_to(const DataTypePtr& storage_type) {
    if (!_impl->column) {
        return _impl->initialize(storage_type);
    }
    return _impl->promote(storage_type, true);
}

const PathInData& VariantPathBuilder::path() const {
    return _impl->path;
}

const DataTypePtr& VariantPathBuilder::type() const {
    return _impl->nullable_type;
}

ColumnPtr VariantPathBuilder::column() const {
    return _impl->column ? _impl->column->get_ptr() : nullptr;
}

std::span<const uint32_t> VariantPathBuilder::rowids() const {
    return _impl->rowids;
}

uint32_t VariantPathBuilder::non_null_rows() const {
    return _impl->non_null;
}

size_t VariantPathBuilder::rows() const {
    return _impl->logical_rows;
}

size_t VariantPathBuilder::promotion_count() const {
    return _impl->promotions;
}

size_t VariantPathBuilder::byte_size() const {
    return sizeof(Impl) + path_allocated_bytes(_impl->path) +
           _impl->rowids.capacity() * sizeof(uint32_t) +
           (_impl->column ? _impl->column->allocated_bytes() : 0);
}

bool VariantPathBuilder::is_null_at(size_t row) const {
    if (row >= _impl->logical_rows) {
        throw Exception(ErrorCode::OUT_OF_BOUND, "Variant path row {} exceeds {} rows for path {}",
                        row, _impl->logical_rows, _impl->path.get_path());
    }
    if (row > std::numeric_limits<uint32_t>::max()) {
        return true;
    }
    return !std::binary_search(_impl->rowids.begin(), _impl->rowids.end(),
                               static_cast<uint32_t>(row));
}

Status VariantPathBuilder::materialize(ColumnPtr* result) const {
    if (result == nullptr) {
        return Status::InvalidArgument("Variant materialized output must not be null");
    }
    if (!_impl->column || _impl->column->size() != _impl->rowids.size()) {
        return Status::InternalError(
                "Variant path {} has {} compact values for {} row ids", _impl->path.get_path(),
                _impl->column ? _impl->column->size() : 0, _impl->rowids.size());
    }

    MutableColumnPtr materialized = _impl->column->clone_empty();
    materialized->reserve(_impl->logical_rows);
    size_t next_row = 0;
    size_t value_index = 0;
    while (value_index < _impl->rowids.size()) {
        const size_t row = _impl->rowids[value_index];
        DORIS_CHECK_GE(row, next_row);
        materialized->insert_many_defaults(row - next_row);

        size_t run_length = 1;
        while (value_index + run_length < _impl->rowids.size() &&
               _impl->rowids[value_index + run_length] == row + run_length) {
            ++run_length;
        }
        materialized->insert_range_from(*_impl->column, value_index, run_length);
        value_index += run_length;
        next_row = row + run_length;
    }
    DORIS_CHECK_LE(next_row, _impl->logical_rows);
    materialized->insert_many_defaults(_impl->logical_rows - next_row);
    *result = std::move(materialized);
    return Status::OK();
}

// NOLINTNEXTLINE(readability-non-const-parameter): the serde appends bytes through this pointer.
Status VariantPathBuilder::write_sparse_cell(size_t value_index, ColumnString::Chars* chars) const {
    if (chars == nullptr) {
        return Status::InvalidArgument("Sparse output chars must not be null");
    }
    if (!_impl->column || value_index >= _impl->column->size()) {
        return Status::InvalidArgument("Sparse value {} is out of range for path {}", value_index,
                                       _impl->path.get_path());
    }
    const auto& nullable = assert_cast<const ColumnNullable&>(*_impl->column);
    if (nullable.is_null_at(value_index)) {
        return Status::InternalError("Compact sparse value {} is null for path {}", value_index,
                                     _impl->path.get_path());
    }
    try {
        _impl->type->get_serde(2)->write_one_cell_to_binary(nullable.get_nested_column(), *chars,
                                                            value_index);
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

VariantPathSelection select_variant_paths(std::span<const VariantPathSelectionCandidate> candidates,
                                          size_t max_dynamic_materialized_paths,
                                          bool typed_paths_to_sparse) {
    struct DynamicCandidate {
        size_t index = 0;
        uint32_t non_null_rows = 0;
    };

    VariantPathSelection result;
    DorisVector<DynamicCandidate> dynamic;
    dynamic.reserve(candidates.size());
    for (size_t index = 0; index < candidates.size(); ++index) {
        const VariantPathSelectionCandidate& candidate = candidates[index];
        DORIS_CHECK(candidate.builder != nullptr);
        if (candidate.is_typed_path && !typed_paths_to_sparse) {
            result.materialized.push_back(index);
        } else if (candidate.builder->non_null_rows() == 0) {
            continue;
        } else {
            dynamic.push_back(
                    {.index = index, .non_null_rows = candidate.builder->non_null_rows()});
        }
    }

    std::ranges::sort(dynamic, [&](const auto& left, const auto& right) {
        if (left.non_null_rows != right.non_null_rows) {
            return left.non_null_rows > right.non_null_rows;
        }
        const PathInData& left_path = candidates[left.index].builder->path();
        const PathInData& right_path = candidates[right.index].builder->path();
        if (dotted_path_depth(left_path) != dotted_path_depth(right_path)) {
            return dotted_path_depth(left_path) > dotted_path_depth(right_path);
        }
        return left_path.get_path() > right_path.get_path();
    });

    const size_t selected_dynamic =
            max_dynamic_materialized_paths == 0
                    ? dynamic.size()
                    : std::min(max_dynamic_materialized_paths, dynamic.size());
    for (size_t index = 0; index < dynamic.size(); ++index) {
        (index < selected_dynamic ? result.materialized : result.sparse)
                .push_back(dynamic[index].index);
    }
    const auto by_path = [&](size_t left, size_t right) {
        return candidates[left].builder->path() < candidates[right].builder->path();
    };
    std::ranges::sort(result.materialized, by_path);
    std::ranges::sort(result.sparse, by_path);
    return result;
}

struct VariantMetadataPathPlan::Impl {
    struct MetadataPlan {
        std::string bytes;
        DorisVector<std::string> keys;
    };

    struct PathPlan {
        PathInData path;
        // Only transitions actually seen for this parent are cached. A metadata-id-indexed vector
        // makes high-cardinality nested paths allocate a paths x metadata matrix.
        std::unordered_map<uint64_t, uint32_t> transitions;
    };

    std::unordered_map<std::string, uint32_t> metadata_ids;
    DorisVector<MetadataPlan> metadata;
    std::unordered_map<PathInData, uint32_t, PathInData::Hash> path_ids = {{PathInData(), 0}};
    DorisVector<PathPlan> paths = {PathPlan {.path = PathInData(), .transitions = {}}};
};

VariantMetadataPathPlan::VariantMetadataPathPlan() : _impl(std::make_unique<Impl>()) {}
VariantMetadataPathPlan::~VariantMetadataPathPlan() = default;
VariantMetadataPathPlan::VariantMetadataPathPlan(VariantMetadataPathPlan&&) noexcept = default;
VariantMetadataPathPlan& VariantMetadataPathPlan::operator=(VariantMetadataPathPlan&&) noexcept =
        default;

Status VariantMetadataPathPlan::intern_metadata(VariantMetadataRef metadata, uint32_t* plan_id) {
    if (plan_id == nullptr) {
        return Status::InvalidArgument("Variant metadata plan id must not be null");
    }
    try {
        std::string bytes(metadata.data, metadata.size);
        if (const auto found = _impl->metadata_ids.find(bytes);
            found != _impl->metadata_ids.end()) {
            *plan_id = found->second;
            return Status::OK();
        }
        const size_t new_id = _impl->metadata.size();
        if (new_id > std::numeric_limits<uint32_t>::max()) {
            return Status::InvalidArgument("Variant metadata plan count exceeds uint32 limit");
        }
        Impl::MetadataPlan plan {.bytes = std::move(bytes), .keys = {}};
        const uint32_t key_count = metadata.dict_size();
        plan.keys.reserve(key_count);
        for (uint32_t field_id = 0; field_id < key_count; ++field_id) {
            const StringRef key = metadata.key_at(field_id);
            plan.keys.emplace_back(key.data, key.size);
        }
        *plan_id = static_cast<uint32_t>(new_id);
        _impl->metadata_ids.emplace(plan.bytes, *plan_id);
        _impl->metadata.push_back(std::move(plan));
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

Status VariantMetadataPathPlan::resolve_child(uint32_t parent_path_id, uint32_t metadata_plan_id,
                                              uint32_t field_id, uint32_t* child_path_id) {
    if (child_path_id == nullptr) {
        return Status::InvalidArgument("Variant child path id must not be null");
    }
    if (parent_path_id >= _impl->paths.size() || metadata_plan_id >= _impl->metadata.size()) {
        return Status::InvalidArgument("Invalid Variant path plan ids parent={} metadata={}",
                                       parent_path_id, metadata_plan_id);
    }
    const auto& metadata = _impl->metadata[metadata_plan_id];
    if (field_id >= metadata.keys.size()) {
        return Status::InvalidArgument("Variant field id {} exceeds metadata key count {}",
                                       field_id, metadata.keys.size());
    }

    const uint64_t transition_key = (static_cast<uint64_t>(metadata_plan_id) << 32) | field_id;
    const auto& parent = _impl->paths[parent_path_id];
    if (const auto found = parent.transitions.find(transition_key);
        found != parent.transitions.end()) {
        *child_path_id = found->second;
        return Status::OK();
    }

    PathInDataBuilder builder;
    builder.append(parent.path.get_parts(), false).append(metadata.keys[field_id], false);
    PathInData child = builder.build();
    // The encoded V2 plan traverses objects and keeps arrays as leaves, so discovered paths are
    // plain. Intern them in the dotted on-disk namespace across metadata and rows. Same-row
    // collision handling belongs to the shredder, where row identity is available.
    child = PathInData(child.get_path());

    uint32_t id = 0;
    if (const auto found = _impl->path_ids.find(child); found != _impl->path_ids.end()) {
        id = found->second;
    } else {
        if (_impl->paths.size() > std::numeric_limits<uint32_t>::max()) {
            return Status::InvalidArgument("Variant path plan count exceeds uint32 limit");
        }
        id = static_cast<uint32_t>(_impl->paths.size());
        _impl->path_ids.emplace(child, id);
        _impl->paths.push_back(Impl::PathPlan {.path = child, .transitions = {}});
    }
    // Creating a path can reallocate _impl->paths, so reacquire the parent before publishing.
    auto& stable_parent = _impl->paths[parent_path_id];
    stable_parent.transitions.emplace(transition_key, id);
    *child_path_id = id;
    return Status::OK();
}

const PathInData& VariantMetadataPathPlan::path(uint32_t path_id) const {
    if (path_id >= _impl->paths.size()) {
        throw Exception(ErrorCode::OUT_OF_BOUND, "Variant path plan id {} exceeds {} paths",
                        path_id, _impl->paths.size());
    }
    return _impl->paths[path_id].path;
}

size_t VariantMetadataPathPlan::metadata_plan_count() const {
    return _impl->metadata.size();
}

size_t VariantMetadataPathPlan::path_plan_count() const {
    return _impl->paths.size();
}

size_t VariantMetadataPathPlan::byte_size() const {
    size_t size = sizeof(Impl);

    size += _impl->metadata_ids.bucket_count() * sizeof(void*);
    for (const auto& [bytes, id] : _impl->metadata_ids) {
        static_cast<void>(id);
        size += sizeof(std::pair<const std::string, uint32_t>) + bytes.capacity();
    }
    size += _impl->metadata.capacity() * sizeof(Impl::MetadataPlan);
    for (const auto& metadata : _impl->metadata) {
        size += metadata.bytes.capacity();
        size += metadata.keys.capacity() * sizeof(std::string);
        for (const auto& key : metadata.keys) {
            size += key.capacity();
        }
    }

    size += _impl->path_ids.bucket_count() * sizeof(void*);
    for (const auto& [path, id] : _impl->path_ids) {
        static_cast<void>(id);
        size += sizeof(std::pair<const PathInData, uint32_t>) + path_allocated_bytes(path);
    }
    size += _impl->paths.capacity() * sizeof(Impl::PathPlan);
    for (const auto& path : _impl->paths) {
        size += path_allocated_bytes(path.path);
        size += path.transitions.bucket_count() * sizeof(void*);
        size += path.transitions.size() * sizeof(std::pair<const uint64_t, uint32_t>);
    }
    return size;
}

} // namespace doris::segment_v2
