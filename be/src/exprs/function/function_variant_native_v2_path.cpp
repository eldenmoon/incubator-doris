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

#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "exprs/function/function_variant_path_v2_internal.h"

namespace doris::variant_native_v2_internal {
namespace {

ColumnPtr make_exists_result(const ColumnVariantV2& source,
                             const ResolvedVariantElementV2Path& path,
                             std::span<const uint8_t> outer_nulls) {
    auto values = ColumnUInt8::create();
    auto nulls = ColumnUInt8::create();
    values->reserve(source.size());
    nulls->reserve(source.size());
    VariantPathV2BatchReader reader(source, path);
    for (size_t row = 0; row < source.size(); ++row) {
        const bool outer_null = is_outer_null(outer_nulls, row);
        VariantRef found;
        values->insert_value(!outer_null && reader.find_at(row, &found));
        nulls->insert_value(outer_null);
    }
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

ColumnPtr make_scalar_exists_result(size_t rows, std::span<const uint8_t> outer_nulls) {
    auto values = ColumnUInt8::create(rows, 0);
    auto nulls = ColumnUInt8::create();
    nulls->reserve(rows);
    for (size_t row = 0; row < rows; ++row) {
        nulls->insert_value(is_outer_null(outer_nulls, row));
    }
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

} // namespace

Status execute_variant_exists_path_v2(const ColumnVariantV2& source,
                                      const ResolvedVariantElementV2Path& path,
                                      std::span<const uint8_t> outer_nulls,
                                      ColumnPtr* const output) {
    ColumnPtr result;
    if (!source.is_typed()) {
        result = make_exists_result(source, path, outer_nulls);
    } else {
        result = make_scalar_exists_result(source.size(), outer_nulls);
    }
    output->swap(result);
    return Status::OK();
}

} // namespace doris::variant_native_v2_internal
