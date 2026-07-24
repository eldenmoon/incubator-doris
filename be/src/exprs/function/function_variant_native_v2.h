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

#pragma once

#include <span>

#include "common/status.h"
#include "core/column/column.h"
#include "exprs/function/function_variant_element_v2.h"

namespace doris {

// Variant V2 native kernels. Sources are materialized ColumnVariantV2 columns; runtime IFunction
// adapters such as variant_type own ColumnConst expansion. Every result is nullable and is
// published only on success.
Status variant_exists_path_v2(const ColumnVariantV2& source,
                              const ResolvedVariantElementV2Path& path,
                              std::span<const uint8_t> outer_nulls, ColumnPtr* output);

Status variant_is_null_v2(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                          ColumnPtr* output);

// Returns the stable semantic word selected by the physical Variant header. Typed rows use the
// same scalar encoding plan as ensure_encoded() without materializing encoded bytes.
Status variant_type_v2(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                       ColumnPtr* output);

// The nested result shape is Array(Nullable(String)), matching the existing JSON keys contract.
Status variant_keys_v2(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                       ColumnPtr* output);

Status variant_length_v2(const ColumnVariantV2& source, std::span<const uint8_t> outer_nulls,
                         ColumnPtr* output);

Status variant_contains_v2(const ColumnVariantV2& target, const ColumnVariantV2& candidate,
                           std::span<const uint8_t> target_outer_nulls,
                           std::span<const uint8_t> candidate_outer_nulls, ColumnPtr* output);

} // namespace doris
