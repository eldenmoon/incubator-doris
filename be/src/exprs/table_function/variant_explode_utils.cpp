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

#include "exprs/table_function/variant_explode_utils.h"

#include <limits>
#include <string_view>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"

namespace doris::variant_explode_internal {
namespace {

constexpr uint32_t UNMAPPED_METADATA = std::numeric_limits<uint32_t>::max();

struct ExtractedArrayElements {
    DorisVector<char> metadata_bytes;
    DorisVector<uint32_t> metadata_offsets {0};
    DorisVector<uint32_t> metadata_ids;
    DorisVector<char> value_bytes;
    DorisVector<uint32_t> value_offsets {0};

    ColumnVariantV2::EncodedDataView view() const {
        return {.metadata_bytes = {metadata_bytes.data(), metadata_bytes.size()},
                .metadata_offsets = metadata_offsets,
                .meta_ids = metadata_ids,
                .value_bytes = {value_bytes.data(), value_bytes.size()},
                .value_offsets = value_offsets};
    }
};

uint32_t append_bytes(DorisVector<char>& destination, StringRef source,
                      std::string_view description) {
    if (source.size > std::numeric_limits<uint32_t>::max() - destination.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant explode {} exceeds the ColumnString uint32 byte limit",
                        description);
    }
    if (source.size != 0) {
        destination.insert(destination.end(), source.data, source.data + source.size);
    }
    return static_cast<uint32_t>(destination.size());
}

uint32_t append_metadata(ExtractedArrayElements& elements, VariantMetadataRef metadata) {
    elements.metadata_offsets.push_back(
            append_bytes(elements.metadata_bytes, {metadata.data, metadata.size}, "metadata"));
    return static_cast<uint32_t>(elements.metadata_offsets.size() - 2);
}

void append_value(ExtractedArrayElements& elements, VariantRef value, uint32_t metadata_id) {
    elements.value_offsets.push_back(append_bytes(elements.value_bytes, value.value, "value"));
    elements.metadata_ids.push_back(metadata_id);
}

} // namespace

Status materialize_variant_array(const ColumnPtr& source, ColumnPtr* output) {
    if (!source || output == nullptr) {
        return Status::InvalidArgument("Variant explode requires non-null input and output");
    }

    const size_t logical_rows = source->size();
    const auto& [unpacked, is_const] = unpack_if_const(source);
    const IColumn* physical = unpacked.get();
    const UInt8* outer_nulls = nullptr;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
        outer_nulls = nullable->get_null_map_data().data();
        physical = &nullable->get_nested_column();
    }
    if (typeid(*physical) != typeid(ColumnVariantV2)) {
        return Status::InvalidArgument("Variant explode requires an exact ColumnVariantV2, got {}",
                                       physical->get_name());
    }

    const auto& variant = assert_cast<const ColumnVariantV2&>(*physical);
    const size_t physical_rows = is_const ? 1 : logical_rows;
    DORIS_CHECK_EQ(variant.size(), physical_rows)
            << "Variant explode logical and physical row counts differ";
    if (outer_nulls != nullptr) {
        DORIS_CHECK_EQ(unpacked->size(), physical_rows)
                << "Variant explode nullable row counts differ";
    }

    auto offsets = ColumnArray::ColumnOffsets::create();
    auto& offset_data = offsets->get_data();
    offset_data.reserve(logical_rows);
    ExtractedArrayElements elements;
    const auto read_view = variant.read_view();

    if (!read_view.is_typed()) {
        DorisVector<uint32_t> metadata_remap(read_view.metadata_count(), UNMAPPED_METADATA);
        for (size_t logical_row = 0; logical_row < logical_rows; ++logical_row) {
            const size_t physical_row = is_const ? 0 : logical_row;
            if (outer_nulls != nullptr && outer_nulls[physical_row] != 0) {
                offset_data.push_back(elements.metadata_ids.size());
                continue;
            }

            const VariantRef value = read_view.value_at(physical_row);
            if (value.basic_type() != VariantBasicType::ARRAY) {
                offset_data.push_back(elements.metadata_ids.size());
                continue;
            }

            const uint32_t element_count = value.num_elements();
            if (element_count != 0) {
                const uint32_t source_metadata_id = read_view.metadata_id_at(physical_row);
                DORIS_CHECK_LT(source_metadata_id, metadata_remap.size());
                uint32_t output_metadata_id = metadata_remap[source_metadata_id];
                if (output_metadata_id == UNMAPPED_METADATA) {
                    output_metadata_id =
                            append_metadata(elements, read_view.metadata_at(source_metadata_id));
                    metadata_remap[source_metadata_id] = output_metadata_id;
                }
                elements.metadata_ids.reserve(elements.metadata_ids.size() + element_count);
                elements.value_offsets.reserve(elements.value_offsets.size() + element_count);
                for (uint32_t element = 0; element < element_count; ++element) {
                    append_value(elements, value.array_at(element), output_metadata_id);
                }
            }
            offset_data.push_back(elements.metadata_ids.size());
        }
    } else {
        offset_data.resize_fill(logical_rows, 0);
    }

    auto nested_values = ColumnVariantV2::create();
    nested_values->insert_encoded_rows(elements.view());
    auto nested = ColumnNullable::create(std::move(nested_values),
                                         ColumnUInt8::create(elements.metadata_ids.size(), 0));
    ColumnPtr result = ColumnArray::create(std::move(nested), std::move(offsets));
    output->swap(result);
    return Status::OK();
}

} // namespace doris::variant_explode_internal
