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

#include <span>

#include "core/column/column_nullable.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "exprs/function/function_variant_native_v2.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

class FunctionVariantType : public IFunction {
public:
    static constexpr auto name = "variant_type";
    static FunctionPtr create() { return std::make_shared<FunctionVariantType>(); }

    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr materialized =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const IColumn* source = materialized.get();
        std::span<const uint8_t> outer_nulls;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(source)) {
            outer_nulls = nullable->get_null_map_data();
            source = &nullable->get_nested_column();
        }
        const auto* variant = check_and_get_column<ColumnVariantV2>(source);
        if (variant == nullptr) {
            return Status::RuntimeError("function {} requires ColumnVariantV2, got {}", get_name(),
                                        source->get_name());
        }
        if (variant->size() != input_rows_count) {
            return Status::InternalError("function {} received {} Variant rows, expected {}",
                                         get_name(), variant->size(), input_rows_count);
        }

        ColumnPtr output;
        RETURN_IF_ERROR(variant_type_v2(*variant, outer_nulls, &output));
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }
};

void register_function_variant_type(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionVariantType>();
}

} // namespace doris
