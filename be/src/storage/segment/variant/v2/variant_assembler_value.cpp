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
#include <string_view>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_column_utils.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/storage_field_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "core/typeid_cast.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "exec/common/format_ip.h"
#include "exprs/function/parse/variant_jsonb_parse.h"
#include "storage/segment/variant/v2/variant_assembler_internal.h"

namespace doris::segment_v2::variant_v2 {
namespace {

// The legacy storage-cell encoding has no external length argument in DataTypeSerDe. This cursor
// therefore validates framing, tag payload width, nesting depth, and trailing bytes against the
// enclosing StringRef before typed materialization is delegated to the existing column SerDe.
// Value-domain validation is outside this adapter's contract: cells are expected to originate from
// the trusted V1 writer, so bool/date/UTF-8 checks are intentionally not duplicated here.
class BinaryCellCursor {
public:
    explicit BinaryCellCursor(StringRef cell)
            : _current(reinterpret_cast<const uint8_t*>(cell.data)), _remaining(cell.size) {
        if (cell.data == nullptr && cell.size != 0) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Binary storage cell has a null pointer for {} bytes", cell.size);
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

private:
    void require(size_t size, std::string_view description) const {
        if (size > _remaining) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Truncated binary storage cell while reading {}: need {} bytes, "
                            "have {}",
                            description, size, _remaining);
        }
    }

    const uint8_t* _current;
    size_t _remaining;
};

struct TypedCellSignature {
    FieldType type = FieldType::OLAP_FIELD_TYPE_UNKNOWN;
    uint8_t precision = 0;
    uint8_t scale = 0;
    bool typed = false;

    bool operator==(const TypedCellSignature& other) const noexcept {
        return type == other.type && precision == other.precision && scale == other.scale;
    }
};

// Reads the single scalar wire layout shared by generic assembly and the typed fast-path probe.
// ARRAY and JSONB stay outside this helper so probing a non-scalar cell never recursively decodes
// it. The returned payload borrows the caller-owned input cell and is never retained by assembly.
StringRef read_typed_scalar_payload(BinaryCellCursor& cursor, FieldType type,
                                    TypedCellSignature* signature) {
    DORIS_CHECK(signature != nullptr);
    *signature = {};
    signature->type = type;
    signature->typed = true;

    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        return cursor.read_bytes(sizeof(uint8_t), "boolean");
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return cursor.read_bytes(sizeof(int8_t), "tinyint");
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        return cursor.read_bytes(sizeof(int16_t), "smallint");
    case FieldType::OLAP_FIELD_TYPE_INT:
        return cursor.read_bytes(sizeof(int32_t), "int");
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return cursor.read_bytes(sizeof(int64_t), "bigint");
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        return cursor.read_bytes(sizeof(__int128), "largeint");
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        return cursor.read_bytes(sizeof(float), "float");
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        return cursor.read_bytes(sizeof(double), "double");
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        const size_t size = cursor.read<size_t>("string size");
        return cursor.read_bytes(size, "string payload");
    }
    case FieldType::OLAP_FIELD_TYPE_IPV4:
        return cursor.read_bytes(sizeof(IPv4), "IPv4");
    case FieldType::OLAP_FIELD_TYPE_IPV6:
        return cursor.read_bytes(sizeof(IPv6), "IPv6");
    case FieldType::OLAP_FIELD_TYPE_DATE:
        return cursor.read_bytes(sizeof(VecDateTimeValue), "legacy DATE");
    case FieldType::OLAP_FIELD_TYPE_DATETIME:
        return cursor.read_bytes(sizeof(VecDateTimeValue), "legacy DATETIME");
    case FieldType::OLAP_FIELD_TYPE_DATEV2:
        return cursor.read_bytes(sizeof(UInt32), "DateV2");
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ:
        signature->scale = cursor.read<uint8_t>("timestamp scale");
        return cursor.read_bytes(sizeof(UInt64), "timestamp value");
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        signature->precision = cursor.read<uint8_t>("legacy DecimalV2 precision");
        signature->scale = cursor.read<uint8_t>("legacy DecimalV2 scale");
        return cursor.read_bytes(sizeof(__int128), "legacy DecimalV2 value");
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        signature->precision = cursor.read<uint8_t>("decimal precision");
        signature->scale = cursor.read<uint8_t>("decimal scale");
        return cursor.read_bytes(sizeof(int32_t), "Decimal32 value");
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        signature->precision = cursor.read<uint8_t>("decimal precision");
        signature->scale = cursor.read<uint8_t>("decimal scale");
        return cursor.read_bytes(sizeof(int64_t), "Decimal64 value");
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        signature->precision = cursor.read<uint8_t>("decimal precision");
        signature->scale = cursor.read<uint8_t>("decimal scale");
        return cursor.read_bytes(sizeof(__int128), "Decimal128 value");
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256:
        signature->precision = cursor.read<uint8_t>("decimal precision");
        signature->scale = cursor.read<uint8_t>("decimal scale");
        cursor.read_bytes(sizeof(wide::Int256), "Decimal256 value");
        throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                        "Conversion from Decimal256 storage cell to Variant V2 is not supported");
    default:
        throw Exception(ErrorCode::CORRUPTION, "Unknown binary storage FieldType {}",
                        static_cast<uint8_t>(type));
    }
}

template <typename T>
T load_typed_scalar(StringRef payload) {
    DORIS_CHECK_EQ(payload.size, sizeof(T));
    T value;
    std::memcpy(&value, payload.data, sizeof(T));
    return value;
}

template <typename DateValue>
int32_t storage_date_days(DateValue value, std::string_view description) {
    return variant_days_since_epoch(value, 0, description);
}

template <typename DateTimeValue>
int64_t storage_timestamp_micros(DateTimeValue value, std::string_view description) {
    return variant_timestamp_micros(value, 0, description);
}

void append_binary_value(BinaryCellCursor& cursor, VariantBatchBuilder::Row& output, uint32_t depth,
                         bool* is_null) {
    if (depth > VARIANT_MAX_NESTING_DEPTH) {
        throw Exception(ErrorCode::CORRUPTION,
                        "Binary storage cell exceeds maximum nesting depth {}",
                        VARIANT_MAX_NESTING_DEPTH);
    }

    const auto type = static_cast<FieldType>(cursor.read<uint8_t>("field type"));
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_NONE:
        if (is_null != nullptr) {
            *is_null = true;
        }
        output.add_null();
        return;
    case FieldType::OLAP_FIELD_TYPE_JSONB: {
        const size_t size = cursor.read<size_t>("JSONB size");
        jsonb_to_variant(cursor.read_bytes(size, "JSONB payload"), output, depth, is_null);
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_ARRAY: {
        if (depth >= VARIANT_MAX_NESTING_DEPTH) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Binary storage container exceeds maximum nesting depth {}",
                            VARIANT_MAX_NESTING_DEPTH);
        }
        const size_t count = cursor.read<size_t>("array element count");
        if (count > cursor.remaining()) {
            throw Exception(ErrorCode::CORRUPTION,
                            "Binary storage array count {} exceeds remaining {} bytes", count,
                            cursor.remaining());
        }
        auto array = output.start_array();
        for (size_t index = 0; index < count; ++index) {
            append_binary_value(cursor, output, depth + 1, nullptr);
        }
        array.finish();
        return;
    }
    default:
        break;
    }

    TypedCellSignature signature;
    const StringRef payload = read_typed_scalar_payload(cursor, type, &signature);
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_BOOL:
        output.add_bool(load_typed_scalar<uint8_t>(payload) != 0);
        return;
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        output.add_int(load_typed_scalar<int8_t>(payload));
        return;
    case FieldType::OLAP_FIELD_TYPE_SMALLINT:
        output.add_int(load_typed_scalar<int16_t>(payload));
        return;
    case FieldType::OLAP_FIELD_TYPE_INT:
        output.add_int(load_typed_scalar<int32_t>(payload));
        return;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        output.add_int(load_typed_scalar<int64_t>(payload));
        return;
    case FieldType::OLAP_FIELD_TYPE_LARGEINT:
        output.add_largeint(load_typed_scalar<__int128>(payload));
        return;
    case FieldType::OLAP_FIELD_TYPE_FLOAT:
        output.add_float(load_typed_scalar<float>(payload));
        return;
    case FieldType::OLAP_FIELD_TYPE_DOUBLE:
        output.add_double(load_typed_scalar<double>(payload));
        return;
    case FieldType::OLAP_FIELD_TYPE_STRING:
        output.add_string(payload);
        return;
    case FieldType::OLAP_FIELD_TYPE_IPV4: {
        const IPv4 value = load_typed_scalar<IPv4>(payload);
        std::array<char, IPV4_MAX_TEXT_LENGTH + 1> buffer {};
        char* end = buffer.data();
        format_ipv4(reinterpret_cast<const unsigned char*>(&value), end);
        output.add_string({buffer.data(), static_cast<size_t>(end - buffer.data())});
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_IPV6: {
        IPv6 value = load_typed_scalar<IPv6>(payload);
        std::array<char, IPV6_MAX_TEXT_LENGTH + 1> buffer {};
        char* end = buffer.data();
        format_ipv6(reinterpret_cast<unsigned char*>(&value), end);
        output.add_string({buffer.data(), static_cast<size_t>(end - buffer.data())});
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_DATE: {
        const VecDateTimeValue value = load_typed_scalar<VecDateTimeValue>(payload);
        output.add_date(storage_date_days(value, "legacy DATE"));
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIME: {
        const VecDateTimeValue value = load_typed_scalar<VecDateTimeValue>(payload);
        output.add_timestamp_micros(storage_timestamp_micros(value, "legacy DATETIME"), false);
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_DATEV2: {
        const UInt32 raw = load_typed_scalar<UInt32>(payload);
        const auto value = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(raw);
        output.add_date(storage_date_days(value, "DATEV2"));
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2:
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ: {
        const UInt64 raw = load_typed_scalar<UInt64>(payload);
        if (type == FieldType::OLAP_FIELD_TYPE_DATETIMEV2) {
            const auto value = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(raw);
            output.add_timestamp_micros(storage_timestamp_micros(value, "DATETIMEV2"), false);
        } else {
            const auto value = binary_cast<UInt64, TimestampTzValue>(raw);
            output.add_timestamp_micros(storage_timestamp_micros(value, "TIMESTAMPTZ"), true);
        }
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL:
        output.add_decimal(load_typed_scalar<__int128>(payload), DecimalV2Value::SCALE, 16);
        return;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32:
        output.add_decimal(load_typed_scalar<int32_t>(payload), signature.scale, 4);
        return;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64:
        output.add_decimal(load_typed_scalar<int64_t>(payload), signature.scale, 8);
        return;
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I:
        output.add_decimal(load_typed_scalar<__int128>(payload), signature.scale, 16);
        return;
    default:
        throw Exception(ErrorCode::INTERNAL_ERROR, "Unhandled binary scalar FieldType {}",
                        static_cast<uint8_t>(type));
    }
}

void append_materialized_scalar(const variant_assembler_detail::PreparedMaterializedColumn& column,
                                size_t row, VariantBatchBuilder::Row& output, uint32_t depth) {
    if (column.primitive == TYPE_JSONB) {
        jsonb_to_variant(assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*column.data)
                                 .get_data_at(row),
                         output, depth);
        return;
    }
    dispatch_variant_typed_column(
            *column.data, column.primitive, [&]<PrimitiveType Type>(const auto& typed) {
                with_variant_typed_scalar<Type>(
                        typed, row, column.scale,
                        [&](const VariantScalarRef& scalar) { output.add_scalar(scalar); });
            });
}

Status deserialize_typed_storage_cell(StringRef cell, IColumn& output) {
    try {
        const auto* begin = reinterpret_cast<const uint8_t*>(cell.data);
        const uint8_t* end = DataTypeSerDe::deserialize_binary_to_column(begin, output);
        if (end != begin + cell.size) {
            return Status::Corruption("Binary storage cell decoder consumed {} of {} bytes",
                                      end - begin, cell.size);
        }
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

bool is_semantically_empty_materialized_value(
        const variant_assembler_detail::PreparedMaterializedColumn& column, size_t row) {
    DCHECK_LT(row, column.data->size());
    if (column.is_null_at(row)) {
        return true;
    }
    if (column.primitive == TYPE_JSONB) {
        const StringRef jsonb =
                assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(*column.data)
                        .get_data_at(row);
        const JsonbDocument* document = nullptr;
        DORIS_CHECK(JsonbDocument::checkAndCreateDocument(jsonb.data, jsonb.size, &document).ok());
        return is_variant_jsonb_value_semantically_empty(document->getValue());
    }
    if (column.primitive != TYPE_ARRAY) {
        return false;
    }

    const auto& offsets = column.array->get_offsets();
    const size_t begin = row == 0 ? 0 : offsets[row - 1];
    const size_t end = offsets[row];
    DCHECK_GE(end, begin);
    DCHECK_LE(end, column.nested->data->size());
    for (size_t element = begin; element < end; ++element) {
        if (!is_semantically_empty_materialized_value(*column.nested, element)) {
            return false;
        }
    }
    return true;
}

} // namespace

namespace variant_assembler_detail {

PreparedMaterializedColumn prepare_materialized_column(const DataTypePtr& type,
                                                       const IColumn* column, size_t rows) {
    DORIS_CHECK(type != nullptr);
    DORIS_CHECK(column != nullptr);
    DORIS_CHECK_EQ(column->size(), rows);
    const bool nullable_type = type->is_nullable();
    const IColumn* data = column;
    const uint8_t* nulls = nullptr;
    if (nullable_type) {
        const auto* nullable = check_and_get_column<ColumnNullable>(column);
        DORIS_CHECK(nullable != nullptr);
        data = &nullable->get_nested_column();
        nulls = nullable->get_null_map_data().data();
    } else {
        DORIS_CHECK(check_and_get_column<ColumnNullable>(column) == nullptr);
    }

    const DataTypePtr base = remove_nullable(type);
    const PrimitiveType primitive = base->get_primitive_type();
    DORIS_CHECK(primitive == TYPE_ARRAY || primitive == TYPE_JSONB ||
                primitive == TYPE_DECIMAL256 || is_supported_variant_typed_identity(primitive));
    DORIS_CHECK(base->check_column(*data).ok());

    PreparedMaterializedColumn output;
    output.data = data;
    output.nulls = nulls;
    output.primitive = primitive;
    const int scale = base->get_scale();
    DORIS_CHECK_GE(scale, 0);
    DORIS_CHECK(!std::cmp_greater(scale, std::numeric_limits<uint8_t>::max()));
    DORIS_CHECK((primitive != TYPE_DATETIMEV2 && primitive != TYPE_TIMESTAMPTZ) || scale <= 6);
    output.scale = static_cast<uint8_t>(scale);
    if (primitive == TYPE_ARRAY) {
        output.array = assert_cast<const ColumnArray*>(data);
        const auto* array_type = typeid_cast<const DataTypeArray*>(base.get());
        DORIS_CHECK(array_type != nullptr);
        output.nested = std::make_unique<PreparedMaterializedColumn>(prepare_materialized_column(
                array_type->get_nested_type(), &output.array->get_data(),
                output.array->get_data().size()));
    }
    return output;
}

bool is_materialized_value_visible(const PreparedMaterializedColumn& column, size_t row,
                                   bool logical_root) {
    DCHECK_LT(row, column.data->size());
    if (column.is_null_at(row)) {
        return false;
    }
    if (column.primitive != TYPE_ARRAY) {
        return true;
    }
    if (logical_root) {
        // Match ColumnVariant's historical subtree-root rule: [] and [null] are absent, while an
        // empty object/array shell is still the requested array value and must remain visible.
        const auto& offsets = column.array->get_offsets();
        const size_t begin = row == 0 ? 0 : offsets[row - 1];
        const size_t end = offsets[row];
        DCHECK_GE(end, begin);
        DCHECK_LE(end, column.nested->data->size());
        for (size_t element = begin; element < end; ++element) {
            if (!column.nested->is_null_at(element)) {
                return true;
            }
        }
        return false;
    }
    return !is_semantically_empty_materialized_value(column, row);
}

Status append_materialized_value(const PreparedMaterializedColumn& column, size_t row,
                                 VariantBatchBuilder::Row& output, uint32_t depth) {
    try {
        if (depth > VARIANT_MAX_NESTING_DEPTH) {
            return Status::Corruption("Variant value exceeds maximum nesting depth {}",
                                      VARIANT_MAX_NESTING_DEPTH);
        }
        if (column.is_null_at(row)) {
            output.add_null();
            return Status::OK();
        }
        if (column.primitive == TYPE_DECIMAL256) {
            return Status::NotSupported(
                    "Conversion from Decimal256 materialized storage column to Variant V2 is not "
                    "supported");
        }
        if (column.primitive != TYPE_ARRAY) {
            append_materialized_scalar(column, row, output, depth);
            return Status::OK();
        }
        if (depth >= VARIANT_MAX_NESTING_DEPTH) {
            return Status::Corruption("Variant materialized container exceeds maximum depth {}",
                                      VARIANT_MAX_NESTING_DEPTH);
        }
        const auto& offsets = column.array->get_offsets();
        const size_t begin = row == 0 ? 0 : offsets[row - 1];
        const size_t end = offsets[row];
        DCHECK_GE(end, begin);
        DCHECK_LE(end, column.nested->data->size());
        auto array = output.start_array();
        for (size_t element = begin; element < end; ++element) {
            RETURN_IF_ERROR(append_materialized_value(*column.nested, element, output, depth + 1));
        }
        array.finish();
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

Status append_storage_cell(StringRef cell, VariantBatchBuilder::Row& output, uint32_t depth,
                           bool* is_null) {
    try {
        if (is_null != nullptr) {
            *is_null = false;
        }
        BinaryCellCursor cursor(cell);
        append_binary_value(cursor, output, depth, is_null);
        if (cursor.remaining() != 0) {
            return Status::Corruption("Binary storage cell has {} trailing bytes",
                                      cursor.remaining());
        }
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

void publish_encoded(VariantBatchBuilder* builder, ColumnUInt8::MutablePtr outer_nulls,
                     ColumnNullable::MutablePtr* output) {
    VariantBatchBuilder block = builder->finish_batch();
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(block);
    *output = ColumnNullable::create(std::move(values), std::move(outer_nulls));
}

} // namespace variant_assembler_detail

namespace {

// Typed output is possible only when every visible cell has the same scalar signature. This first
// pass reads just enough bytes to prove each scalar cell is bounded and compatible; only after the
// whole batch agrees does the second pass delegate value materialization to DataTypeSerDe.
Status read_typed_cell_signature(StringRef cell, TypedCellSignature* signature) {
    DORIS_CHECK(signature != nullptr);
    *signature = {};
    try {
        BinaryCellCursor cursor(cell);
        signature->type = static_cast<FieldType>(cursor.read<uint8_t>("field type"));
        switch (signature->type) {
        case FieldType::OLAP_FIELD_TYPE_NONE:
        case FieldType::OLAP_FIELD_TYPE_JSONB:
        case FieldType::OLAP_FIELD_TYPE_ARRAY:
            return Status::OK();
        default:
            static_cast<void>(read_typed_scalar_payload(cursor, signature->type, signature));
            break;
        }
        if (cursor.remaining() != 0) {
            return Status::Corruption("Binary storage cell has {} trailing bytes",
                                      cursor.remaining());
        }
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

Status try_build_typed_storage_cells(std::span<const StringRef> cells,
                                     std::span<const uint8_t> outer_nulls,
                                     std::span<const uint8_t> missing,
                                     ColumnNullable::MutablePtr* output, bool* built) {
    DORIS_CHECK(output != nullptr);
    DORIS_CHECK(built != nullptr);
    *built = false;
    TypedCellSignature signature;
    bool has_signature = false;
    const size_t rows = cells.size();
    for (size_t row = 0; row < rows; ++row) {
        if ((!outer_nulls.empty() && outer_nulls[row] != 0) ||
            (!missing.empty() && missing[row] != 0)) {
            continue;
        }
        TypedCellSignature row_signature;
        RETURN_IF_ERROR(read_typed_cell_signature(cells[row], &row_signature));
        if (!row_signature.typed) {
            return Status::OK();
        }
        if (!has_signature) {
            signature = row_signature;
            has_signature = true;
        } else if (!(row_signature == signature)) {
            return Status::OK();
        }
    }
    if (!has_signature) {
        return Status::OK();
    }
    DataTypePtr scalar_type = DataTypeFactory::instance().create_data_type(
            signature.type, signature.precision, signature.scale);
    if (scalar_type == nullptr) {
        return Status::Corruption("Cannot create typed Variant output for FieldType {}",
                                  static_cast<uint8_t>(signature.type));
    }
    // Decimal factories choose the physical column width from precision, while the SerDe below
    // dispatches from the persisted tag. Reject a mismatch before the unchecked typed cast.
    if (scalar_type->get_primitive_type() != storage_field_type_to_primitive_type(signature.type)) {
        return Status::Corruption(
                "Binary storage FieldType {} is incompatible with precision {} and scale {}",
                static_cast<uint8_t>(signature.type), signature.precision, signature.scale);
    }
    if (!is_supported_variant_typed_identity(scalar_type->get_primitive_type())) {
        return Status::OK();
    }
    auto nullable = ColumnNullable::create(scalar_type->create_column(), ColumnUInt8::create());
    auto result_outer = ColumnUInt8::create();
    nullable->reserve(rows);
    result_outer->reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        const bool is_missing = (!outer_nulls.empty() && outer_nulls[row] != 0) ||
                                (!missing.empty() && missing[row] != 0);
        result_outer->insert_value(is_missing ? 1 : 0);
        if (is_missing) {
            nullable->insert_default();
            continue;
        }
        // The complete batch was bounded by read_typed_cell_signature() before this SerDe pass.
        RETURN_IF_ERROR(deserialize_typed_storage_cell(cells[row], *nullable));
    }
    *output =
            ColumnNullable::create(ColumnVariantV2::create_typed(std::move(nullable), scalar_type),
                                   std::move(result_outer));
    *built = true;
    return Status::OK();
}

} // namespace

Status variant_assembler_detail::assemble_storage_cells(std::span<const StringRef> cells,
                                                        std::span<const uint8_t> outer_nulls,
                                                        std::span<const uint8_t> missing,
                                                        ColumnNullable::MutablePtr* output) {
    DORIS_CHECK(output != nullptr);
    DORIS_CHECK(outer_nulls.empty() || outer_nulls.size() == cells.size());
    DORIS_CHECK(missing.empty() || missing.size() == cells.size());

    try {
        ColumnNullable::MutablePtr result;
        bool built_typed = false;
        RETURN_IF_ERROR(
                try_build_typed_storage_cells(cells, outer_nulls, missing, &result, &built_typed));
        if (!built_typed) {
            VariantBatchBuilder builder({.rows = cells.size()});
            auto result_outer = ColumnUInt8::create();
            result_outer->reserve(cells.size());
            // At this storage boundary a missing/SQL NULL cell and an encoded Variant null have
            // the same logical outer-null result.
            for (size_t row_index = 0; row_index < cells.size(); ++row_index) {
                auto row = builder.begin_row();
                const bool is_missing = (!outer_nulls.empty() && outer_nulls[row_index] != 0) ||
                                        (!missing.empty() && missing[row_index] != 0);
                if (is_missing) {
                    result_outer->insert_value(1);
                    row.add_null();
                } else {
                    bool is_null = false;
                    RETURN_IF_ERROR(variant_assembler_detail::append_storage_cell(
                            cells[row_index], row, 0, &is_null));
                    result_outer->insert_value(is_null ? 1 : 0);
                }
                row.finish();
            }
            variant_assembler_detail::publish_encoded(&builder, std::move(result_outer), &result);
        }
        *output = std::move(result);
        return Status::OK();
    } catch (const Exception& exception) {
        return exception.to_status();
    }
}

} // namespace doris::segment_v2::variant_v2
