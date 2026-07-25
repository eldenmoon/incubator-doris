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

#include <cstdint>
#include <memory>
#include <span>

#include "common/status.h"
#include "core/column/column.h"
#include "core/string_ref.h"

namespace doris {

class ColumnVariantV2;
class SimpleFunctionFactory;

// Owns a constant JSONPath after parsing. Object keys are copied into the existing resolved
// Variant V2 path, so execution does not retain pointers into the SQL argument column.
class ResolvedVariantGetV2Path {
public:
    ~ResolvedVariantGetV2Path();
    ResolvedVariantGetV2Path(ResolvedVariantGetV2Path&&) noexcept;
    ResolvedVariantGetV2Path& operator=(ResolvedVariantGetV2Path&&) noexcept;

    ResolvedVariantGetV2Path(const ResolvedVariantGetV2Path&) = delete;
    ResolvedVariantGetV2Path& operator=(const ResolvedVariantGetV2Path&) = delete;

    bool is_root() const noexcept;

private:
    struct Impl;
    explicit ResolvedVariantGetV2Path(std::unique_ptr<Impl> impl);

    friend Status resolve_variant_get_v2_path(StringRef path,
                                              std::unique_ptr<ResolvedVariantGetV2Path>* output);
    friend Status variant_get_v2(const ColumnVariantV2& source,
                                 const ResolvedVariantGetV2Path& path,
                                 std::span<const uint8_t> outer_nulls, ColumnPtr* output);

    std::unique_ptr<Impl> _impl;
};

Status resolve_variant_get_v2_path(StringRef path,
                                   std::unique_ptr<ResolvedVariantGetV2Path>* output);

// The source must be materialized. SQL NULLs are supplied separately because an encoded Variant
// null is a value and must remain distinguishable from SQL NULL.
Status variant_get_v2(const ColumnVariantV2& source, const ResolvedVariantGetV2Path& path,
                      std::span<const uint8_t> outer_nulls, ColumnPtr* output);

void register_function_variant_get(SimpleFunctionFactory& factory);

} // namespace doris
