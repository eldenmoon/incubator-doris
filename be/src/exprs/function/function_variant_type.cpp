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
#include <map>
#include <span>

#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "exprs/function/simple_function_factory.h"
#include "util/string_util.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

namespace {

using TypeInfo = std::map<std::string, std::string>;

void append_type_info_json(ColumnString& output, const TypeInfo& type_info) {
    VectorBufferWriter writer(output);
    writer.write_char('{');
    bool first = true;
    for (const auto& [key, value] : type_info) {
        if (!first) {
            writer.write_char(',');
        }
        first = false;
        writer.write_json_string(key);
        writer.write_c_string(":");
        writer.write_json_string(value);
    }
    writer.write_char('}');
    writer.commit();
}

} // namespace

// get data type of variant column
class FunctionVariantType : public IFunction {
public:
    static constexpr auto name = "variant_type";
    static FunctionPtr create() { return std::make_shared<FunctionVariantType>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    TypeInfo get_type_info(const Field& field) const {
        TypeInfo result;
        const auto& variant_map = field.get<TYPE_VARIANT>().legacy_map();
        for (const auto& [key, value] : variant_map) {
            if (key.empty() && value.base_scalar_type_id == PrimitiveType::TYPE_JSONB &&
                value.num_dimensions == 0 && value.field.get<TYPE_JSONB>().get_size() == 0) {
                // ignore empty jsonb root, it's tricky here
                continue;
            }
            result[key.get_path()] =
                    to_lower(type_to_string(value.base_scalar_type_id != PrimitiveType::INVALID_TYPE
                                                    ? value.base_scalar_type_id
                                                    : value.field.get_type()));
        }
        return result;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr materialized =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const IColumn* physical = materialized.get();
        std::span<const NullMap::value_type> outer_nulls;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
            outer_nulls = nullable->get_null_map_data();
            physical = &nullable->get_nested_column();
        }
        auto result_column = ColumnString::create();
        auto result_nulls = ColumnUInt8::create();
        result_nulls->reserve(input_rows_count);

        if (check_and_get_column<ColumnVariantV2>(physical) != nullptr) {
            // Encoded V2 values do not retain every V1 declared type distinction (for example,
            // integer widths, LARGEINT/DECIMAL, DATE, and IP types), so a partial implementation
            // would silently change the path-to-type contract.
            return Status::NotSupported(
                    "function variant_type does not support ColumnVariantV2 execution");
        }

        const auto& arg_column = assert_cast<const ColumnVariant&>(*physical);
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (!outer_nulls.empty() && outer_nulls[i] != 0) {
                result_column->insert_default();
                result_nulls->insert_value(1);
                continue;
            }
            const Field field = arg_column[i];
            if (field.is_null()) {
                result_column->insert_default();
                result_nulls->insert_value(1);
                continue;
            }
            auto type_info = get_type_info(field);
            append_type_info_json(*result_column, type_info);
            result_nulls->insert_value(0);
        }
        block.replace_by_position(
                result, ColumnNullable::create(std::move(result_column), std::move(result_nulls)));
        return Status::OK();
    }
};

void register_function_variant_type(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionVariantType>();
}

} // namespace doris
