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

#include <array>
#include <cstring>
#include <limits>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/typeid_cast.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exec/common/format_ip.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "storage/segment/variant/variant_assembler_internal.h"

namespace doris::segment_v2::variant_assembler_internal {
namespace {

class CellCursor {
public:
    explicit CellCursor(StringRef cell)
            : _current(reinterpret_cast<const uint8_t*>(cell.data)), _remaining(cell.size) {
        if (cell.data == nullptr && cell.size != 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant storage cell has a null pointer for {} bytes", cell.size);
        }
    }

    template <typename T>
    T read(std::string_view description) {
        require(sizeof(T), description);
        T value;
        std::memcpy(&value, _current, sizeof(T));
        _current += sizeof(T);
        _remaining -= sizeof(T);
        return value;
    }

    StringRef read_bytes(size_t size, std::string_view description) {
        require(size, description);
        const StringRef result {reinterpret_cast<const char*>(_current), size};
        _current += size;
        _remaining -= size;
        return result;
    }

    size_t remaining() const noexcept { return _remaining; }
    bool empty() const noexcept { return _remaining == 0; }

private:
    void require(size_t size, std::string_view description) const {
        if (size > _remaining) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Truncated Variant storage cell while reading {}: need {} bytes, "
                            "have {}",
                            description, size, _remaining);
        }
    }

    const uint8_t* _current;
    size_t _remaining;
};

template <typename Integer>
uint32_t decimal_digits(Integer value) {
    uint32_t result = 0;
    do {
        value /= 10;
        ++result;
    } while (value != 0);
    return result;
}

template <typename Integer>
void validate_decimal(Integer value, uint8_t precision, uint8_t scale, uint8_t maximum,
                      std::string_view description) {
    if (precision == 0 || precision > maximum || scale > precision) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant storage {} precision/scale {}/{} is invalid", description,
                        precision, scale);
    }
    if (decimal_digits(value) > precision) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant storage {} value exceeds declared precision {}", description,
                        precision);
    }
}

void validate_depth(uint32_t depth) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant storage cell exceeds maximum nesting depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
}

void validate_container_depth(uint32_t depth) {
    if (depth >= VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant storage container exceeds maximum nesting depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }
}

template <typename DateValue>
int32_t storage_date_days(DateValue value, std::string_view description) {
    if (!value.is_valid_date()) {
        throw Exception(ErrorCode::CORRUPTION, "Invalid {} in Variant storage cell", description);
    }
    return variant_days_since_epoch(value, 0, description);
}

template <typename DateTimeValue>
int64_t storage_timestamp_micros(DateTimeValue value, std::string_view description) {
    if (!value.is_valid_date()) {
        throw Exception(ErrorCode::CORRUPTION, "Invalid {} in Variant storage cell", description);
    }
    return variant_timestamp_micros(value, 0, description);
}

void add_largeint(VariantBatchBuilder::Row& output, __int128 value) {
    output.add_largeint(value);
}

void add_decimal256(VariantBatchBuilder::Row& output, wide::Int256 value, uint8_t scale) {
    const std::string text = Decimal256 {value}.to_string(scale);
    output.add_string(StringRef(text));
}

void add_ipv4(VariantBatchBuilder::Row& output, IPv4 value) {
    std::array<char, IPV4_MAX_TEXT_LENGTH + 1> buffer {};
    char* end = buffer.data();
    const auto* address = reinterpret_cast<const unsigned char*>(&value);
    format_ipv4(address, end);
    output.add_string({buffer.data(), static_cast<size_t>(end - buffer.data())});
}

void add_ipv6(VariantBatchBuilder::Row& output, IPv6 value) {
    std::array<char, IPV6_MAX_TEXT_LENGTH + 1> buffer {};
    char* end = buffer.data();
    format_ipv6(reinterpret_cast<unsigned char*>(&value), end);
    output.add_string({buffer.data(), static_cast<size_t>(end - buffer.data())});
}

// Keep the exhaustive storage FieldType decoder together so validation and byte consumption for
// every wire tag remain auditable in one dispatch table.
// NOLINTNEXTLINE(readability-function-size)
void decode_storage_value(CellCursor& cursor, VariantBatchBuilder::Row& output, uint32_t depth) {
    validate_depth(depth);
    const auto type = static_cast<FieldType>(cursor.read<uint8_t>("field type"));
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_NONE:
        output.add_null();
        return;
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        const uint8_t value = cursor.read<uint8_t>("boolean");
        if (value > 1) {
            throw Exception(ErrorCode::CORRUPTION, "Invalid Variant storage boolean byte {}",
                            value);
        }
        output.add_bool(value != 0);
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        output.add_int(cursor.read<int8_t>("tinyint"));
        return;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        output.add_int(cursor.read<int16_t>("smallint"));
        return;
    case FieldType::OLAP_FIELD_TYPE_INT:
        output.add_int(cursor.read<int32_t>("int"));
        return;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        output.add_int(cursor.read<int64_t>("bigint"));
        return;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        add_largeint(output, cursor.read<__int128>("largeint"));
        return;
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        output.add_float(cursor.read<float>("float"));
        return;
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        output.add_double(cursor.read<double>("double"));
        return;
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        const size_t size = cursor.read<size_t>("string size");
        output.add_string(cursor.read_bytes(size, "string payload"));
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_JSONB: {
        const size_t size = cursor.read<size_t>("JSONB size");
        jsonb_to_variant(cursor.read_bytes(size, "JSONB payload"), output, depth);
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_ARRAY: {
        validate_container_depth(depth);
        const size_t count = cursor.read<size_t>("array element count");
        if (count > cursor.remaining()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant storage array count {} exceeds remaining {} bytes", count,
                            cursor.remaining());
        }
        auto array = output.start_array();
        for (size_t index = 0; index < count; ++index) {
            decode_storage_value(cursor, output, depth + 1);
        }
        array.finish();
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        add_ipv4(output, cursor.read<IPv4>("IPv4"));
        return;
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        add_ipv6(output, cursor.read<IPv6>("IPv6"));
        return;
    case FieldType::OLAP_FIELD_TYPE_DATEV2: {
        const UInt32 raw = cursor.read<UInt32>("DateV2");
        const auto value = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(raw);
        output.add_date(storage_date_days(value, "DATEV2"));
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ: {
        const uint8_t scale = cursor.read<uint8_t>("timestamp scale");
        if (scale > 6) {
            throw Exception(ErrorCode::CORRUPTION, "Variant storage timestamp scale {} exceeds 6",
                            scale);
        }
        const UInt64 raw = cursor.read<UInt64>("timestamp value");
        if (type == FieldType::OLAP_FIELD_TYPE_DATETIMEV2) {
            const auto value = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(raw);
            output.add_timestamp_micros(storage_timestamp_micros(value, "DATETIMEV2"), false);
        } else {
            const auto value = binary_cast<UInt64, TimestampTzValue>(raw);
            output.add_timestamp_micros(storage_timestamp_micros(value, "TIMESTAMPTZ"), true);
        }
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32: {
        const uint8_t precision = cursor.read<uint8_t>("Decimal32 precision");
        const uint8_t scale = cursor.read<uint8_t>("Decimal32 scale");
        const int32_t value = cursor.read<int32_t>("Decimal32 value");
        validate_decimal(value, precision, scale, 9, "Decimal32");
        output.add_decimal(value, scale, 4);
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64: {
        const uint8_t precision = cursor.read<uint8_t>("Decimal64 precision");
        const uint8_t scale = cursor.read<uint8_t>("Decimal64 scale");
        const int64_t value = cursor.read<int64_t>("Decimal64 value");
        validate_decimal(value, precision, scale, 18, "Decimal64");
        output.add_decimal(value, scale, 8);
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
        const uint8_t precision = cursor.read<uint8_t>("Decimal128 precision");
        const uint8_t scale = cursor.read<uint8_t>("Decimal128 scale");
        const __int128 value = cursor.read<__int128>("Decimal128 value");
        validate_decimal(value, precision, scale, 38, "Decimal128");
        output.add_decimal(value, scale, 16);
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256: {
        const uint8_t precision = cursor.read<uint8_t>("Decimal256 precision");
        const uint8_t scale = cursor.read<uint8_t>("Decimal256 scale");
        const wide::Int256 value = cursor.read<wide::Int256>("Decimal256 value");
        validate_decimal(value, precision, scale, 76, "Decimal256");
        add_decimal256(output, value, scale);
        return;
    }
    default:
        throw Exception(ErrorCode::CORRUPTION, "Unknown Variant storage FieldType {}",
                        static_cast<uint8_t>(type));
    }
}

template <typename ColumnType>
bool matches(const IColumn* column) {
    return check_and_get_column<ColumnType>(column) != nullptr;
}

bool supported_column_shape(PrimitiveType type, const IColumn* column) {
    switch (type) {
    case TYPE_BOOLEAN:
        return matches<ColumnUInt8>(column);
    case TYPE_TINYINT:
        return matches<ColumnInt8>(column);
    case TYPE_SMALLINT:
        return matches<ColumnInt16>(column);
    case TYPE_INT:
        return matches<ColumnInt32>(column);
    case TYPE_BIGINT:
        return matches<ColumnInt64>(column);
    case TYPE_LARGEINT:
        return matches<ColumnInt128>(column);
    case TYPE_FLOAT:
        return matches<ColumnFloat32>(column);
    case TYPE_DOUBLE:
        return matches<ColumnFloat64>(column);
    case TYPE_DECIMALV2:
        return matches<ColumnDecimal128V2>(column);
    case TYPE_DECIMAL32:
        return matches<ColumnDecimal32>(column);
    case TYPE_DECIMAL64:
        return matches<ColumnDecimal64>(column);
    case TYPE_DECIMAL128I:
        return matches<ColumnDecimal128V3>(column);
    case TYPE_DECIMAL256:
        return matches<ColumnDecimal256>(column);
    case TYPE_DATE:
        return matches<ColumnDate>(column);
    case TYPE_DATEV2:
        return matches<ColumnDateV2>(column);
    case TYPE_DATETIME:
        return matches<ColumnDateTime>(column);
    case TYPE_DATETIMEV2:
        return matches<ColumnDateTimeV2>(column);
    case TYPE_TIMESTAMPTZ:
        return matches<ColumnTimeStampTz>(column);
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
    case TYPE_JSONB:
        return matches<ColumnString>(column);
    case TYPE_IPV4:
        return matches<ColumnIPv4>(column);
    case TYPE_IPV6:
        return matches<ColumnIPv6>(column);
    case TYPE_ARRAY:
        return matches<ColumnArray>(column);
    default:
        return false;
    }
}

// This is a flat PrimitiveType dispatch; splitting it would scatter the concrete-column contract.
// NOLINTNEXTLINE(readability-function-size)
void append_materialized_scalar(const PreparedColumn& column, size_t row,
                                VariantBatchBuilder::Row& output, uint32_t depth) {
    const uint8_t scale = column.scale;
    switch (column.primitive) {
    case TYPE_BOOLEAN:
        output.add_bool(assert_cast<const ColumnUInt8&, TypeCheckOnRelease::DISABLE>(*column.data)
                                .get_data()[row] != 0);
        return;
    case TYPE_TINYINT:
        output.add_int(assert_cast<const ColumnInt8&, TypeCheckOnRelease::DISABLE>(*column.data)
                               .get_data()[row]);
        return;
    case TYPE_SMALLINT:
        output.add_int(assert_cast<const ColumnInt16&, TypeCheckOnRelease::DISABLE>(*column.data)
                               .get_data()[row]);
        return;
    case TYPE_INT:
        output.add_int(assert_cast<const ColumnInt32&, TypeCheckOnRelease::DISABLE>(*column.data)
                               .get_data()[row]);
        return;
    case TYPE_BIGINT:
        output.add_int(assert_cast<const ColumnInt64&, TypeCheckOnRelease::DISABLE>(*column.data)
                               .get_data()[row]);
        return;
    case TYPE_LARGEINT:
        add_largeint(output,
                     assert_cast<const ColumnInt128&, TypeCheckOnRelease::DISABLE>(*column.data)
                             .get_data()[row]);
        return;
    case TYPE_FLOAT:
        output.add_float(
                assert_cast<const ColumnFloat32&, TypeCheckOnRelease::DISABLE>(*column.data)
                        .get_data()[row]);
        return;
    case TYPE_DOUBLE:
        output.add_double(
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(*column.data)
                        .get_data()[row]);
        return;
    case TYPE_DECIMALV2:
        output.add_decimal(
                assert_cast<const ColumnDecimal128V2&, TypeCheckOnRelease::DISABLE>(*column.data)
                        .get_data()[row]
                        .value(),
                scale, 16);
        return;
    case TYPE_DECIMAL32:
        output.add_decimal(
                assert_cast<const ColumnDecimal32&, TypeCheckOnRelease::DISABLE>(*column.data)
                        .get_data()[row]
                        .value,
                scale, 4);
        return;
    case TYPE_DECIMAL64:
        output.add_decimal(
                assert_cast<const ColumnDecimal64&, TypeCheckOnRelease::DISABLE>(*column.data)
                        .get_data()[row]
                        .value,
                scale, 8);
        return;
    case TYPE_DECIMAL128I:
        output.add_decimal(
                assert_cast<const ColumnDecimal128V3&, TypeCheckOnRelease::DISABLE>(*column.data)
                        .get_data()[row]
                        .value,
                scale, 16);
        return;
    case TYPE_DECIMAL256:
        add_decimal256(
                output,
                assert_cast<const ColumnDecimal256&, TypeCheckOnRelease::DISABLE>(*column.data)
                        .get_data()[row]
                        .value,
                scale);
        return;
    case TYPE_DATE:
        output.add_date(storage_date_days(
                assert_cast<const ColumnDate&, TypeCheckOnRelease::DISABLE>(*column.data)
                        .get_data()[row],
                "DATE"));
        return;
    case TYPE_DATEV2:
        output.add_date(storage_date_days(
                assert_cast<const ColumnDateV2&, TypeCheckOnRelease::DISABLE>(*column.data)
                        .get_data()[row],
                "DATEV2"));
        return;
    case TYPE_DATETIME:
        output.add_timestamp_micros(
                storage_timestamp_micros(
                        assert_cast<const ColumnDateTime&, TypeCheckOnRelease::DISABLE>(
                                *column.data)
                                .get_data()[row],
                        "DATETIME"),
                false);
        return;
    case TYPE_DATETIMEV2:
        output.add_timestamp_micros(
                storage_timestamp_micros(
                        assert_cast<const ColumnDateTimeV2&, TypeCheckOnRelease::DISABLE>(
                                *column.data)
                                .get_data()[row],
                        "DATETIMEV2"),
                false);
        return;
    case TYPE_TIMESTAMPTZ:
        output.add_timestamp_micros(
                storage_timestamp_micros(
                        assert_cast<const ColumnTimeStampTz&, TypeCheckOnRelease::DISABLE>(
                                *column.data)
                                .get_data()[row],
                        "TIMESTAMPTZ"),
                true);
        return;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        output.add_string(
                assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*column.data)
                        .get_data_at(row));
        return;
    case TYPE_JSONB:
        jsonb_to_variant(assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*column.data)
                                 .get_data_at(row),
                         output, depth);
        return;
    case TYPE_IPV4:
        add_ipv4(output, assert_cast<const ColumnIPv4&, TypeCheckOnRelease::DISABLE>(*column.data)
                                 .get_data()[row]);
        return;
    case TYPE_IPV6:
        add_ipv6(output, assert_cast<const ColumnIPv6&, TypeCheckOnRelease::DISABLE>(*column.data)
                                 .get_data()[row]);
        return;
    default:
        throw Exception(ErrorCode::CORRUPTION, "Unsupported materialized Variant type {}",
                        column.type->get_name());
    }
}

size_t fixed_payload_size(FieldType type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return 1;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return 2;
    case FieldType::OLAP_FIELD_TYPE_INT:
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
    case FieldType::OLAP_FIELD_TYPE_IPV4:
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        return 4;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        return 8;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        return 16;
    default:
        return 0;
    }
}

void require_exact(CellCursor& cursor) {
    if (!cursor.empty()) {
        throw Exception(ErrorCode::CORRUPTION, "Variant storage cell has {} trailing bytes",
                        cursor.remaining());
    }
}

// Keep typed eligibility validation adjacent for all storage scalar tags.
// NOLINTNEXTLINE(readability-function-size)
bool inspect_typed_cell(CellCursor& cursor, CellSignature* signature) {
    signature->type = static_cast<FieldType>(cursor.read<uint8_t>("field type"));
    signature->typed = true;
    if (signature->type == FieldType::OLAP_FIELD_TYPE_BOOL) {
        const uint8_t value = cursor.read<uint8_t>("boolean");
        if (value > 1) {
            throw Exception(ErrorCode::CORRUPTION, "Invalid Variant storage boolean byte {}",
                            value);
        }
        require_exact(cursor);
        return true;
    }
    if (signature->type == FieldType::OLAP_FIELD_TYPE_DATEV2) {
        const UInt32 raw = cursor.read<UInt32>("DateV2");
        const auto value = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(raw);
        static_cast<void>(storage_date_days(value, "DATEV2"));
        require_exact(cursor);
        return true;
    }
    const size_t fixed = fixed_payload_size(signature->type);
    if (fixed != 0) {
        static_cast<void>(cursor.read_bytes(fixed, "fixed scalar payload"));
        require_exact(cursor);
        return true;
    }
    switch (signature->type) {
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        const size_t size = cursor.read<size_t>("string size");
        static_cast<void>(cursor.read_bytes(size, "string payload"));
        require_exact(cursor);
        return true;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        signature->scale = cursor.read<uint8_t>("timestamp scale");
        if (signature->scale > 6) {
            throw Exception(ErrorCode::CORRUPTION, "Variant storage timestamp scale {} exceeds 6",
                            signature->scale);
        }
        if (signature->type == FieldType::OLAP_FIELD_TYPE_DATETIMEV2) {
            const auto value = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(
                    cursor.read<UInt64>("timestamp value"));
            static_cast<void>(storage_timestamp_micros(value, "DATETIMEV2"));
        } else {
            const auto value =
                    binary_cast<UInt64, TimestampTzValue>(cursor.read<UInt64>("timestamp value"));
            static_cast<void>(storage_timestamp_micros(value, "TIMESTAMPTZ"));
        }
        require_exact(cursor);
        return true;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256: {
        signature->precision = cursor.read<uint8_t>("decimal precision");
        signature->scale = cursor.read<uint8_t>("decimal scale");
        uint8_t maximum = 0;
        if (signature->type == FieldType::OLAP_FIELD_TYPE_DECIMAL32) {
            maximum = 9;
        } else if (signature->type == FieldType::OLAP_FIELD_TYPE_DECIMAL64) {
            maximum = 18;
        } else if (signature->type == FieldType::OLAP_FIELD_TYPE_DECIMAL128I) {
            maximum = 38;
        } else {
            maximum = 76;
        }
        if (signature->precision == 0 || signature->precision > maximum ||
            signature->scale > signature->precision) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant storage decimal precision/scale {}/{} is invalid",
                            signature->precision, signature->scale);
        }
        if (signature->type == FieldType::OLAP_FIELD_TYPE_DECIMAL32) {
            validate_decimal(cursor.read<int32_t>("Decimal32 value"), signature->precision,
                             signature->scale, maximum, "Decimal32");
        } else if (signature->type == FieldType::OLAP_FIELD_TYPE_DECIMAL64) {
            validate_decimal(cursor.read<int64_t>("Decimal64 value"), signature->precision,
                             signature->scale, maximum, "Decimal64");
        } else if (signature->type == FieldType::OLAP_FIELD_TYPE_DECIMAL128I) {
            validate_decimal(cursor.read<__int128>("Decimal128 value"), signature->precision,
                             signature->scale, maximum, "Decimal128");
        } else {
            validate_decimal(cursor.read<wide::Int256>("Decimal256 value"), signature->precision,
                             signature->scale, maximum, "Decimal256");
        }
        require_exact(cursor);
        return true;
    }
    case FieldType::OLAP_FIELD_TYPE_NONE:
    case FieldType::OLAP_FIELD_TYPE_JSONB:
    case FieldType::OLAP_FIELD_TYPE_ARRAY:
        signature->typed = false;
        return false;
    default:
        throw Exception(ErrorCode::CORRUPTION, "Unknown Variant storage FieldType {}",
                        static_cast<uint8_t>(signature->type));
    }
}

template <typename Column>
Column& typed_output(IColumn* output) {
    return assert_cast<Column&, TypeCheckOnRelease::DISABLE>(*output);
}

// This mirrors inspect_typed_cell as one exhaustive scalar publication table.
// NOLINTNEXTLINE(readability-function-size)
void append_cell_to_typed(StringRef cell, const CellSignature& signature, IColumn* output) {
    CellCursor cursor(cell);
    const auto type = static_cast<FieldType>(cursor.read<uint8_t>("field type"));
    if (type != signature.type) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Variant binary type changed while publishing typed output");
    }
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        const uint8_t value = cursor.read<uint8_t>("boolean");
        if (value > 1) {
            throw Exception(ErrorCode::CORRUPTION, "Invalid Variant storage boolean byte {}",
                            value);
        }
        typed_output<ColumnUInt8>(output).insert_value(value);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        typed_output<ColumnInt8>(output).insert_value(cursor.read<int8_t>("tinyint"));
        break;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        typed_output<ColumnInt16>(output).insert_value(cursor.read<int16_t>("smallint"));
        break;
    case FieldType::OLAP_FIELD_TYPE_INT:
        typed_output<ColumnInt32>(output).insert_value(cursor.read<int32_t>("int"));
        break;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        typed_output<ColumnInt64>(output).insert_value(cursor.read<int64_t>("bigint"));
        break;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        typed_output<ColumnInt128>(output).insert_value(cursor.read<__int128>("largeint"));
        break;
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        typed_output<ColumnFloat32>(output).insert_value(cursor.read<float>("float"));
        break;
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        typed_output<ColumnFloat64>(output).insert_value(cursor.read<double>("double"));
        break;
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        const size_t size = cursor.read<size_t>("string size");
        const StringRef value = cursor.read_bytes(size, "string payload");
        typed_output<ColumnString>(output).insert_data(value.data, value.size);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        typed_output<ColumnIPv4>(output).insert_value(cursor.read<IPv4>("IPv4"));
        break;
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        typed_output<ColumnIPv6>(output).insert_value(cursor.read<IPv6>("IPv6"));
        break;
    case FieldType::OLAP_FIELD_TYPE_DATEV2: {
        const UInt32 raw = cursor.read<UInt32>("DateV2");
        const auto value = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(raw);
        static_cast<void>(storage_date_days(value, "DATEV2"));
        typed_output<ColumnDateV2>(output).insert_value(raw);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
        const uint8_t scale = cursor.read<uint8_t>("DateTimeV2 scale");
        if (scale != signature.scale || scale > 6) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant binary DateTimeV2 scale changed while publishing");
        }
        const UInt64 raw = cursor.read<UInt64>("DateTimeV2");
        const auto value = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(raw);
        static_cast<void>(storage_timestamp_micros(value, "DATETIMEV2"));
        typed_output<ColumnDateTimeV2>(output).insert_value(raw);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ: {
        const uint8_t scale = cursor.read<uint8_t>("TimestampTz scale");
        if (scale != signature.scale || scale > 6) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant binary TimestampTz scale changed while publishing");
        }
        const UInt64 raw = cursor.read<UInt64>("TimestampTz");
        const auto value = binary_cast<UInt64, TimestampTzValue>(raw);
        static_cast<void>(storage_timestamp_micros(value, "TIMESTAMPTZ"));
        typed_output<ColumnTimeStampTz>(output).insert_value(value);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32: {
        const uint8_t precision = cursor.read<uint8_t>("Decimal32 precision");
        const uint8_t scale = cursor.read<uint8_t>("Decimal32 scale");
        const int32_t value = cursor.read<int32_t>("Decimal32");
        if (precision != signature.precision || scale != signature.scale) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant binary Decimal32 signature changed while publishing");
        }
        validate_decimal(value, precision, scale, 9, "Decimal32");
        typed_output<ColumnDecimal32>(output).insert_value(Decimal32 {value});
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64: {
        const uint8_t precision = cursor.read<uint8_t>("Decimal64 precision");
        const uint8_t scale = cursor.read<uint8_t>("Decimal64 scale");
        const int64_t value = cursor.read<int64_t>("Decimal64");
        if (precision != signature.precision || scale != signature.scale) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant binary Decimal64 signature changed while publishing");
        }
        validate_decimal(value, precision, scale, 18, "Decimal64");
        typed_output<ColumnDecimal64>(output).insert_value(Decimal64 {value});
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
        const uint8_t precision = cursor.read<uint8_t>("Decimal128 precision");
        const uint8_t scale = cursor.read<uint8_t>("Decimal128 scale");
        const __int128 value = cursor.read<__int128>("Decimal128");
        if (precision != signature.precision || scale != signature.scale) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant binary Decimal128 signature changed while publishing");
        }
        validate_decimal(value, precision, scale, 38, "Decimal128");
        typed_output<ColumnDecimal128V3>(output).insert_value(Decimal128V3 {value});
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256: {
        const uint8_t precision = cursor.read<uint8_t>("Decimal256 precision");
        const uint8_t scale = cursor.read<uint8_t>("Decimal256 scale");
        const wide::Int256 value = cursor.read<wide::Int256>("Decimal256");
        if (precision != signature.precision || scale != signature.scale) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Variant binary Decimal256 signature changed while publishing");
        }
        validate_decimal(value, precision, scale, 76, "Decimal256");
        typed_output<ColumnDecimal256>(output).insert_value(Decimal256 {value});
        break;
    }
    default:
        throw Exception(ErrorCode::CORRUPTION, "Unsupported typed Variant storage FieldType {}",
                        static_cast<uint8_t>(type));
    }
    require_exact(cursor);
}

void append_default_to_typed(const CellSignature& signature, IColumn* output) {
    switch (signature.type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        typed_output<ColumnUInt8>(output).insert_value(0);
        return;
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        typed_output<ColumnInt8>(output).insert_value(0);
        return;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        typed_output<ColumnInt16>(output).insert_value(0);
        return;
    case FieldType::OLAP_FIELD_TYPE_INT:
        typed_output<ColumnInt32>(output).insert_value(0);
        return;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        typed_output<ColumnInt64>(output).insert_value(0);
        return;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        typed_output<ColumnInt128>(output).insert_value(0);
        return;
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        typed_output<ColumnFloat32>(output).insert_value(0);
        return;
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        typed_output<ColumnFloat64>(output).insert_value(0);
        return;
    case FieldType::OLAP_FIELD_TYPE_STRING:
        typed_output<ColumnString>(output).insert_data("", 0);
        return;
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        typed_output<ColumnIPv4>(output).insert_value(IPv4 {});
        return;
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        typed_output<ColumnIPv6>(output).insert_value(IPv6 {});
        return;
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        typed_output<ColumnDateV2>(output).insert_value(UInt32 {});
        return;
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
        typed_output<ColumnDateTimeV2>(output).insert_value(UInt64 {});
        return;
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        typed_output<ColumnTimeStampTz>(output).insert_value(TimestampTzValue {});
        return;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        typed_output<ColumnDecimal32>(output).insert_value(Decimal32 {});
        return;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        typed_output<ColumnDecimal64>(output).insert_value(Decimal64 {});
        return;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        typed_output<ColumnDecimal128V3>(output).insert_value(Decimal128V3 {});
        return;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        typed_output<ColumnDecimal256>(output).insert_value(Decimal256 {});
        return;
    default:
        throw Exception(ErrorCode::CORRUPTION, "Unsupported typed Variant default FieldType {}",
                        static_cast<uint8_t>(signature.type));
    }
}

} // namespace

Status prepare_root_jsonb(const IColumn* column, size_t rows, RootJsonbView* output) {
    if (column == nullptr || output == nullptr) {
        return Status::InvalidArgument("Variant root JSONB view is null");
    }
    const IColumn* nested = column;
    const uint8_t* nulls = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        nested = &nullable->get_nested_column();
        nulls = nullable->get_null_map_data().data();
    }
    const auto* strings = check_and_get_column<ColumnString>(nested);
    if (strings == nullptr || column->size() != rows) {
        return Status::Corruption("Variant root must be a {}-row String/JSONB column", rows);
    }
    *output = {.values = strings, .nulls = nulls};
    return Status::OK();
}

Status prepare_map(const ColumnMap* column, size_t rows, std::string_view description,
                   MapView* output) {
    if (column == nullptr || output == nullptr || column->size() != rows) {
        return Status::Corruption("Variant {} map must contain {} rows", description, rows);
    }
    const auto* paths = check_and_get_column<ColumnString>(&column->get_keys());
    const auto* values = check_and_get_column<ColumnString>(&column->get_values());
    if (paths == nullptr || values == nullptr || paths->size() != values->size()) {
        return Status::Corruption("Variant {} map must be Map<String,String>", description);
    }
    const auto& offsets = column->get_offsets();
    size_t previous = 0;
    for (size_t row = 0; row < rows; ++row) {
        const size_t current = offsets[row];
        if (current < previous || current > paths->size()) {
            return Status::Corruption("Variant {} row {} has invalid offset {}", description, row,
                                      current);
        }
        previous = current;
    }
    if (previous != paths->size()) {
        return Status::Corruption("Variant {} offsets consume {} of {} cells", description,
                                  previous, paths->size());
    }
    *output = {.offsets = &offsets, .paths = paths, .values = values};
    return Status::OK();
}

Status prepare_column(const DataTypePtr& type, const IColumn* column, size_t rows,
                      PreparedColumn* output) {
    if (type == nullptr || column == nullptr || output == nullptr || column->size() != rows) {
        return Status::Corruption("Variant materialized column shape does not match {} rows", rows);
    }
    const bool nullable_type = type->is_nullable();
    const IColumn* data = column;
    const uint8_t* nulls = nullptr;
    if (nullable_type) {
        const auto* nullable = check_and_get_column<ColumnNullable>(column);
        if (nullable == nullptr) {
            return Status::Corruption("Variant materialized type {} requires ColumnNullable",
                                      type->get_name());
        }
        data = &nullable->get_nested_column();
        nulls = nullable->get_null_map_data().data();
    } else if (check_and_get_column<ColumnNullable>(column) != nullptr) {
        return Status::Corruption("Variant materialized type {} rejects ColumnNullable",
                                  type->get_name());
    }

    const DataTypePtr base = remove_nullable(type);
    const PrimitiveType primitive = base->get_primitive_type();
    if (!supported_column_shape(primitive, data)) {
        return Status::Corruption("Variant materialized column does not match type {}",
                                  type->get_name());
    }
    output->data = data;
    output->nulls = nulls;
    output->type = base;
    output->primitive = primitive;
    const int scale = base->get_scale();
    if (scale < 0 || std::cmp_greater(scale, std::numeric_limits<uint8_t>::max())) {
        return Status::Corruption("Variant materialized type {} has invalid scale {}",
                                  type->get_name(), scale);
    }
    if ((primitive == TYPE_DATETIMEV2 || primitive == TYPE_TIMESTAMPTZ) && scale > 6) {
        return Status::Corruption("Variant materialized type {} has unsupported scale {}",
                                  type->get_name(), scale);
    }
    output->scale = static_cast<uint8_t>(scale);
    if (primitive == TYPE_ARRAY) {
        output->array = assert_cast<const ColumnArray*>(data);
        const auto* array_type = typeid_cast<const DataTypeArray*>(base.get());
        output->nested = std::make_unique<PreparedColumn>();
        RETURN_IF_ERROR(prepare_column(array_type->get_nested_type(), &output->array->get_data(),
                                       output->array->get_data().size(), output->nested.get()));
        output->nested_size = output->nested->data->size();
    }
    return Status::OK();
}

Status append_typed_value(const PreparedColumn& column, size_t row,
                          VariantBatchBuilder::Row& output, uint32_t depth) {
    try {
        validate_depth(depth);
        if (column.is_null_at(row)) {
            output.add_null();
            return Status::OK();
        }
        if (column.primitive != TYPE_ARRAY) {
            append_materialized_scalar(column, row, output, depth);
            return Status::OK();
        }
        validate_container_depth(depth);
        const auto& offsets = column.array->get_offsets();
        const size_t begin = row == 0 ? 0 : offsets[row - 1];
        const size_t end = offsets[row];
        if (end < begin || end > column.nested_size) {
            return Status::Corruption("Variant materialized array row {} has invalid offsets", row);
        }
        auto array = output.start_array();
        for (size_t element = begin; element < end; ++element) {
            RETURN_IF_ERROR(append_typed_value(*column.nested, element, output, depth + 1));
        }
        array.finish();
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

Status append_storage_cell(StringRef cell, VariantBatchBuilder::Row& output, uint32_t depth) {
    try {
        CellCursor cursor(cell);
        decode_storage_value(cursor, output, depth);
        require_exact(cursor);
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

Status inspect_storage_cell(StringRef cell, CellSignature* signature) {
    if (signature == nullptr) {
        return Status::InvalidArgument("Variant storage signature output must not be null");
    }
    try {
        CellCursor cursor(cell);
        static_cast<void>(inspect_typed_cell(cursor, signature));
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

Status try_build_typed_binary(const ColumnString& cells, std::span<const uint8_t> outer_nulls,
                              std::span<const uint8_t> missing, size_t rows,
                              VariantAssembledColumn* output, bool* built) {
    *built = false;
    CellSignature signature;
    bool have_signature = false;
    for (size_t row = 0; row < rows; ++row) {
        if ((!outer_nulls.empty() && outer_nulls[row] != 0) ||
            (!missing.empty() && missing[row] != 0)) {
            continue;
        }
        CellSignature current;
        RETURN_IF_ERROR(inspect_storage_cell(cells.get_data_at(row), &current));
        if (!current.typed) {
            return Status::OK();
        }
        if (!have_signature) {
            signature = current;
            have_signature = true;
        } else if (!(signature == current)) {
            return Status::OK();
        }
    }
    if (!have_signature) {
        return Status::OK();
    }
    DataTypePtr scalar_type = DataTypeFactory::instance().create_data_type(
            signature.type, signature.precision, signature.scale);
    if (scalar_type == nullptr) {
        return Status::Corruption("Cannot create typed Variant output for FieldType {}",
                                  static_cast<uint8_t>(signature.type));
    }
    if (!is_supported_variant_typed_identity(scalar_type->get_primitive_type())) {
        return Status::OK();
    }
    MutableColumnPtr nested = scalar_type->create_column();
    auto nulls = ColumnUInt8::create();
    auto result_outer = ColumnUInt8::create();
    try {
        for (size_t row = 0; row < rows; ++row) {
            const bool is_missing = (!outer_nulls.empty() && outer_nulls[row] != 0) ||
                                    (!missing.empty() && missing[row] != 0);
            result_outer->insert_value(is_missing ? 1 : 0);
            if (is_missing) {
                append_default_to_typed(signature, nested.get());
                nulls->insert_value(1);
                continue;
            }
            append_cell_to_typed(cells.get_data_at(row), signature, nested.get());
            nulls->insert_value(0);
        }
        auto nullable = ColumnNullable::create(std::move(nested), std::move(nulls));
        VariantAssembledColumn result;
        result.values = ColumnVariantV2::create_typed(std::move(nullable), scalar_type);
        result.outer_nulls = std::move(result_outer);
        *output = std::move(result);
        *built = true;
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

int compare_path_parts(std::span<const StringRef> left, std::span<const StringRef> right) noexcept {
    const size_t common = std::min(left.size(), right.size());
    for (size_t index = 0; index < common; ++index) {
        const int compared = left[index].compare(right[index]);
        if (compared != 0) {
            return compared;
        }
    }
    if (left.size() == right.size()) {
        return 0;
    }
    return left.size() < right.size() ? -1 : 1;
}

bool path_is_prefix(std::span<const StringRef> prefix, std::span<const StringRef> path) noexcept {
    if (prefix.size() > path.size()) {
        return false;
    }
    for (size_t index = 0; index < prefix.size(); ++index) {
        if (prefix[index] != path[index]) {
            return false;
        }
    }
    return true;
}

// The output vector is cleared and repopulated; clang-tidy cannot see mutation through the alias.
// NOLINTNEXTLINE(readability-non-const-parameter)
Status split_sparse_path(StringRef path, DorisVector<StringRef>* parts) {
    parts->clear();
    if (path.data == nullptr && path.size != 0) {
        return Status::Corruption("Variant sparse path has a null pointer");
    }
    if (path.size == 0) {
        // The persisted sparse/doc map never carries the logical root. An empty raw key is the
        // structurally non-empty JSON object key "", whose dotted display path is also empty.
        parts->emplace_back("", 0);
        return Status::OK();
    }
    const char* begin = path.data;
    const char* end = path.data + path.size;
    for (const char* cursor = begin; cursor != end; ++cursor) {
        if (*cursor != '.') {
            continue;
        }
        parts->emplace_back(begin, static_cast<size_t>(cursor - begin));
        begin = cursor + 1;
    }
    parts->emplace_back(begin, static_cast<size_t>(end - begin));
    return Status::OK();
}

} // namespace doris::segment_v2::variant_assembler_internal
