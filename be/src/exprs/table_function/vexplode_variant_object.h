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

#include <optional>
#include <utility>

#include "common/status.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "exprs/table_function/table_function.h"

namespace doris {

class Block;

class VExplodeVariantObjectTableFunction : public TableFunction {
    ENABLE_FACTORY_CREATOR(VExplodeVariantObjectTableFunction);

public:
    VExplodeVariantObjectTableFunction();
    ~VExplodeVariantObjectTableFunction() override = default;

    Status process_init(Block* block, RuntimeState* state) override;
    void process_row(size_t row_idx) override;
    void process_close() override;
    void get_same_many_values(MutableColumnPtr& column, int length) override;
    int get_value(MutableColumnPtr& column, int max_step) override;

private:
    ColumnPtr _source_column;
    const UInt8* _outer_nulls = nullptr;
    const ColumnVariantV2* _variant_column = nullptr;
    std::optional<ColumnVariantV2::ReadView> _read_view;
    std::pair<MutableColumnPtr, MutableColumnPtr> _kv_pairs;
};

} // namespace doris
