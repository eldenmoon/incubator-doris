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

#include <benchmark/benchmark.h>

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant_v2.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "core/value/variant/variant_value.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "runtime/runtime_state.h"

namespace doris {
namespace {

// Keep this dataset contract byte-for-byte aligned with the StarRocks benchmark:
// - 4096 rows per invocation.
// - Narrow8 changes "id" to row + 1.
// - Deep8 changes the target leaf to row + 8.
// - Array100 changes element 99 to row + 99.
// - Log32 has four typed fields followed by field0004..field0031 integer padding.
// - Wide1000 changes field0999 to row + 999.
// - Mixed alternates an object root with fields id/double/string/boolean/null/object/array
//   and an array root [row,row.25E0,"row-N",boolean,null,{"leaf":row},[row,true,"value"]].
// BytesProcessed uses the original JSON bytes as a common throughput denominator. The parse
// benchmarks measure each engine's native semi-structured value construction, so their result
// representations are different and the numbers are not a like-for-like Variant parser comparison.
constexpr size_t VARIANT_BENCHMARK_BATCH_SIZE = 4096;

enum class VariantBenchmarkParseCase : uint8_t {
    NARROW_8,
    DEEP_8,
    ARRAY_100,
    WIDE_1000,
    MIXED,
};

enum class VariantBenchmarkGetCase : uint8_t {
    SHALLOW_HIT,
    SHALLOW_MISSING,
    DEEP_8_HIT,
    DEEP_8_MISSING,
    ARRAY_100_FIRST,
    ARRAY_100_MIDDLE,
    ARRAY_100_LAST,
    ARRAY_100_MISSING,
    LOG_32_INT,
    LOG_32_DOUBLE,
    LOG_32_STRING,
    LOG_32_BOOL,
    WIDE_32_LAST,
    WIDE_1000_FIRST,
    WIDE_1000_MIDDLE,
    WIDE_1000_LAST,
    WIDE_1000_MISSING,
};

enum class VariantBenchmarkGetShape : uint8_t {
    SHALLOW,
    DEEP_8,
    ARRAY_100,
    LOG_32,
    WIDE_32,
    WIDE_1000,
};

enum class VariantBenchmarkScalarTarget : uint8_t {
    BIGINT,
    DOUBLE,
    STRING,
    BOOLEAN,
};

enum class VariantBenchmarkExpectedValue : uint8_t {
    INTEGER,
    DOUBLE,
    STRING,
    BOOLEAN,
};

struct VariantBenchmarkGetConfig {
    VariantBenchmarkGetShape shape;
    std::string_view path;
    std::optional<int64_t> expected_base;
    bool add_row_to_expected = false;
    VariantBenchmarkExpectedValue expected_value = VariantBenchmarkExpectedValue::INTEGER;
};

struct VariantBenchmarkInput {
    ColumnString::MutablePtr column;
    size_t logical_bytes = 0;
};

std::string variant_benchmark_array_json(size_t elements, size_t row) {
    std::string json;
    json.reserve(elements * 4 + 2);
    json.push_back('[');
    for (size_t index = 0; index < elements; ++index) {
        if (index != 0) {
            json.push_back(',');
        }
        const size_t value = index + (index + 1 == elements ? row : 0);
        json.append(std::to_string(value));
    }
    json.push_back(']');
    return json;
}

void variant_benchmark_append_wide_field_token(std::string* output, size_t field) {
    DORIS_CHECK_LT(field, 10000);
    output->append("field");
    output->push_back(static_cast<char>('0' + field / 1000));
    output->push_back(static_cast<char>('0' + field / 100 % 10));
    output->push_back(static_cast<char>('0' + field / 10 % 10));
    output->push_back(static_cast<char>('0' + field % 10));
}

void variant_benchmark_append_wide_field_name(std::string* json, size_t field) {
    json->push_back('"');
    variant_benchmark_append_wide_field_token(json, field);
    json->append("\":");
}

std::string variant_benchmark_wide_json(size_t fields, size_t row) {
    std::string json;
    json.reserve(fields * 16 + 2);
    json.push_back('{');
    for (size_t field = 0; field < fields; ++field) {
        if (field != 0) {
            json.push_back(',');
        }
        variant_benchmark_append_wide_field_name(&json, field);
        const size_t value = field + (field + 1 == fields ? row : 0);
        json.append(std::to_string(value));
    }
    json.push_back('}');
    return json;
}

std::string variant_benchmark_log_32_json(size_t row) {
    std::string json;
    json.reserve(32 * 20);
    json.append("{\"int_value\":");
    json.append(std::to_string(row + 7));
    json.append(",\"double_value\":");
    json.append(std::to_string(row));
    json.append(".25,\"string_value\":\"row-");
    json.append(std::to_string(row));
    json.append("\",\"bool_value\":");
    json.append(row % 2 == 0 ? "true" : "false");
    for (size_t field = 4; field < 32; ++field) {
        json.push_back(',');
        variant_benchmark_append_wide_field_name(&json, field);
        json.append(std::to_string(row + field));
    }
    json.push_back('}');
    return json;
}

std::string variant_benchmark_object_array_json(size_t elements, size_t row) {
    std::string json;
    json.reserve(elements * 20 + 2);
    json.push_back('[');
    for (size_t element = 0; element < elements; ++element) {
        if (element != 0) {
            json.push_back(',');
        }
        json.push_back('{');
        variant_benchmark_append_wide_field_name(&json, element);
        json.append(std::to_string(row + element));
        json.append("}");
    }
    json.push_back(']');
    return json;
}

std::string variant_benchmark_deep_json(size_t depth, size_t target) {
    std::string json;
    json.reserve(depth * 14 + 16);
    for (size_t level = 0; level < depth; ++level) {
        json.append("{\"level");
        json.append(std::to_string(level));
        json.append("\":");
    }
    json.append("{\"target\":");
    json.append(std::to_string(target));
    json.push_back('}');
    json.append(depth, '}');
    return json;
}

std::string variant_benchmark_mixed_json(size_t row) {
    const std::string number = std::to_string(row);
    const std::string boolean = row % 4 == 0 ? "true" : "false";
    if (row % 2 == 0) {
        return "{\"id\":" + number + ",\"double\":" + number + ".25E0,\"string\":\"row-" + number +
               "\",\"boolean\":" + boolean + R"(,"null":null,"object":{"leaf":)" + number +
               R"(},"array":[)" + number + R"(,true,"value"]})";
    }
    return "[" + number + "," + number + ".25E0,\"row-" + number + "\"," + boolean +
           ",null,{\"leaf\":" + number + "},[" + number + R"(,true,"value"]])";
}

std::string variant_benchmark_parse_json(VariantBenchmarkParseCase test_case, size_t row) {
    switch (test_case) {
    case VariantBenchmarkParseCase::NARROW_8:
        return "{\"id\":" + std::to_string(row + 1) +
               R"(,"active":true,"score":12.5,"name":"narrow",)"
               R"("city":"hangzhou","empty":null,"tags":["a","b"],"nested":{"x":1}})";
    case VariantBenchmarkParseCase::DEEP_8:
        return variant_benchmark_deep_json(8, row + 8);
    case VariantBenchmarkParseCase::ARRAY_100:
        return variant_benchmark_array_json(100, row);
    case VariantBenchmarkParseCase::WIDE_1000:
        return variant_benchmark_wide_json(1000, row);
    case VariantBenchmarkParseCase::MIXED:
        return variant_benchmark_mixed_json(row);
    }
    __builtin_unreachable();
}

VariantBenchmarkGetConfig variant_benchmark_get_config(VariantBenchmarkGetCase test_case) {
    switch (test_case) {
    case VariantBenchmarkGetCase::SHALLOW_HIT:
        return {.shape = VariantBenchmarkGetShape::SHALLOW,
                .path = "$.target",
                .expected_base = 7,
                .add_row_to_expected = true};
    case VariantBenchmarkGetCase::SHALLOW_MISSING:
        return {.shape = VariantBenchmarkGetShape::SHALLOW,
                .path = "$.absent",
                .expected_base = std::nullopt};
    case VariantBenchmarkGetCase::DEEP_8_HIT:
        return {.shape = VariantBenchmarkGetShape::DEEP_8,
                .path = "$.level0.level1.level2.level3.level4.level5.level6.level7.target",
                .expected_base = 8,
                .add_row_to_expected = true};
    case VariantBenchmarkGetCase::DEEP_8_MISSING:
        return {.shape = VariantBenchmarkGetShape::DEEP_8,
                .path = "$.level0.level1.level2.level3.level4.level5.level6.level7.absent",
                .expected_base = std::nullopt};
    case VariantBenchmarkGetCase::ARRAY_100_FIRST:
        return {.shape = VariantBenchmarkGetShape::ARRAY_100,
                .path = "$.items[0]",
                .expected_base = 0};
    case VariantBenchmarkGetCase::ARRAY_100_MIDDLE:
        return {.shape = VariantBenchmarkGetShape::ARRAY_100,
                .path = "$.items[50]",
                .expected_base = 50};
    case VariantBenchmarkGetCase::ARRAY_100_LAST:
        return {.shape = VariantBenchmarkGetShape::ARRAY_100,
                .path = "$.items[99]",
                .expected_base = 99,
                .add_row_to_expected = true};
    case VariantBenchmarkGetCase::ARRAY_100_MISSING:
        return {.shape = VariantBenchmarkGetShape::ARRAY_100,
                .path = "$.items[100]",
                .expected_base = std::nullopt};
    case VariantBenchmarkGetCase::LOG_32_INT:
        return {.shape = VariantBenchmarkGetShape::LOG_32,
                .path = "$.int_value",
                .expected_base = 7,
                .add_row_to_expected = true};
    case VariantBenchmarkGetCase::LOG_32_DOUBLE:
        return {.shape = VariantBenchmarkGetShape::LOG_32,
                .path = "$.double_value",
                .expected_base = 0,
                .expected_value = VariantBenchmarkExpectedValue::DOUBLE};
    case VariantBenchmarkGetCase::LOG_32_STRING:
        return {.shape = VariantBenchmarkGetShape::LOG_32,
                .path = "$.string_value",
                .expected_base = 0,
                .expected_value = VariantBenchmarkExpectedValue::STRING};
    case VariantBenchmarkGetCase::LOG_32_BOOL:
        return {.shape = VariantBenchmarkGetShape::LOG_32,
                .path = "$.bool_value",
                .expected_base = 0,
                .expected_value = VariantBenchmarkExpectedValue::BOOLEAN};
    case VariantBenchmarkGetCase::WIDE_32_LAST:
        return {.shape = VariantBenchmarkGetShape::WIDE_32,
                .path = "$.field0031",
                .expected_base = 31,
                .add_row_to_expected = true};
    case VariantBenchmarkGetCase::WIDE_1000_FIRST:
        return {.shape = VariantBenchmarkGetShape::WIDE_1000,
                .path = "$.field0000",
                .expected_base = 0};
    case VariantBenchmarkGetCase::WIDE_1000_MIDDLE:
        return {.shape = VariantBenchmarkGetShape::WIDE_1000,
                .path = "$.field0500",
                .expected_base = 500};
    case VariantBenchmarkGetCase::WIDE_1000_LAST:
        return {.shape = VariantBenchmarkGetShape::WIDE_1000,
                .path = "$.field0999",
                .expected_base = 999,
                .add_row_to_expected = true};
    case VariantBenchmarkGetCase::WIDE_1000_MISSING:
        return {.shape = VariantBenchmarkGetShape::WIDE_1000,
                .path = "$.field1000",
                .expected_base = std::nullopt};
    }
    __builtin_unreachable();
}

std::string variant_benchmark_get_json(VariantBenchmarkGetShape shape, size_t row) {
    switch (shape) {
    case VariantBenchmarkGetShape::SHALLOW:
        return "{\"target\":" + std::to_string(row + 7) + R"(,"padding":"value"})";
    case VariantBenchmarkGetShape::DEEP_8:
        return variant_benchmark_deep_json(8, row + 8);
    case VariantBenchmarkGetShape::ARRAY_100:
        return "{\"items\":" + variant_benchmark_array_json(100, row) + "}";
    case VariantBenchmarkGetShape::LOG_32:
        return variant_benchmark_log_32_json(row);
    case VariantBenchmarkGetShape::WIDE_32:
        return variant_benchmark_wide_json(32, row);
    case VariantBenchmarkGetShape::WIDE_1000:
        return variant_benchmark_wide_json(1000, row);
    }
    __builtin_unreachable();
}

VariantBenchmarkInput variant_benchmark_parse_input(VariantBenchmarkParseCase test_case) {
    VariantBenchmarkInput input {.column = ColumnString::create()};
    for (size_t row = 0; row < VARIANT_BENCHMARK_BATCH_SIZE; ++row) {
        const std::string json = variant_benchmark_parse_json(test_case, row);
        input.column->insert_data(json.data(), json.size());
        input.logical_bytes += json.size();
    }
    return input;
}

VariantBenchmarkInput variant_benchmark_get_input(VariantBenchmarkGetShape shape) {
    VariantBenchmarkInput input {.column = ColumnString::create()};
    for (size_t row = 0; row < VARIANT_BENCHMARK_BATCH_SIZE; ++row) {
        const std::string json = variant_benchmark_get_json(shape, row);
        input.column->insert_data(json.data(), json.size());
        input.logical_bytes += json.size();
    }
    return input;
}

VariantBenchmarkInput variant_benchmark_typeof_input() {
    VariantBenchmarkInput input {.column = ColumnString::create()};
    for (size_t row = 0; row < VARIANT_BENCHMARK_BATCH_SIZE; ++row) {
        const std::string json = variant_benchmark_object_array_json(100, row);
        input.column->insert_data(json.data(), json.size());
        input.logical_bytes += json.size();
    }
    return input;
}

ColumnPtr variant_benchmark_constant_path(std::string_view path) {
    auto value = ColumnString::create();
    value->insert_data(path.data(), path.size());
    return ColumnConst::create(std::move(value), VARIANT_BENCHMARK_BATCH_SIZE);
}

class VariantBenchmarkFunctionInvocation {
public:
    VariantBenchmarkFunctionInvocation(std::string_view function_name,
                                       ColumnsWithTypeAndName arguments,
                                       ColumnNumbers argument_positions, DataTypePtr result_type)
            : _block(std::move(arguments)),
              _argument_positions(std::move(argument_positions)),
              _result_type(std::move(result_type)),
              _result_position(_block.columns()) {
        _argument_types.reserve(_block.columns());
        for (const auto& argument : _block) {
            _argument_types.push_back(argument.type);
        }
        _function = SimpleFunctionFactory::instance().get_function(
                std::string(function_name), _block.get_columns_with_type_and_name(), _result_type);
        _block.insert({nullptr, _result_type, "result"});
    }

    ~VariantBenchmarkFunctionInvocation() { static_cast<void>(close()); }

    Status open(std::optional<size_t> constant_argument = std::nullopt) {
        if (_function == nullptr) {
            return Status::NotFound("benchmark function is not registered");
        }
        _context = FunctionContext::create_context(&_runtime_state, _result_type, _argument_types);
        std::vector<std::shared_ptr<ColumnPtrWrapper>> constant_columns(_argument_types.size());
        if (constant_argument.has_value()) {
            DORIS_CHECK_LT(*constant_argument, _argument_types.size());
            constant_columns[*constant_argument] = std::make_shared<ColumnPtrWrapper>(
                    _block.get_by_position(*constant_argument).column);
        }
        _context->set_constant_cols(constant_columns);
        RETURN_IF_ERROR(_function->open(_context.get(), FunctionContext::FRAGMENT_LOCAL));
        _fragment_open = true;
        RETURN_IF_ERROR(_function->open(_context.get(), FunctionContext::THREAD_LOCAL));
        _thread_open = true;
        return Status::OK();
    }

    Status execute() {
        _block.replace_by_position(_result_position, ColumnPtr {});
        return _function->execute(_context.get(), _block, _argument_positions, _result_position,
                                  VARIANT_BENCHMARK_BATCH_SIZE);
    }

    Status close() {
        Status result = Status::OK();
        if (_thread_open) {
            Status status = _function->close(_context.get(), FunctionContext::THREAD_LOCAL);
            _thread_open = false;
            if (!status.ok()) {
                result = std::move(status);
            }
        }
        if (_fragment_open) {
            Status status = _function->close(_context.get(), FunctionContext::FRAGMENT_LOCAL);
            _fragment_open = false;
            if (result.ok() && !status.ok()) {
                result = std::move(status);
            }
        }
        return result;
    }

    void reset_output() { _block.replace_by_position(_result_position, ColumnPtr {}); }

    void replace_argument(size_t position, ColumnPtr column) {
        DORIS_CHECK_LT(position, _result_position);
        _block.replace_by_position(position, std::move(column));
    }

    const ColumnPtr& output() const { return _block.get_by_position(_result_position).column; }

private:
    RuntimeState _runtime_state;
    Block _block;
    ColumnNumbers _argument_positions;
    DataTypePtr _result_type;
    uint32_t _result_position;
    std::vector<DataTypePtr> _argument_types;
    FunctionBasePtr _function;
    std::unique_ptr<FunctionContext> _context;
    bool _fragment_open = false;
    bool _thread_open = false;
};

bool variant_benchmark_is_integer(VariantPrimitiveId id) {
    return id == VariantPrimitiveId::INT8 || id == VariantPrimitiveId::INT16 ||
           id == VariantPrimitiveId::INT32 || id == VariantPrimitiveId::INT64;
}

Status variant_benchmark_expect_integer(const VariantRef& value, int64_t expected) {
    if (value.basic_type() != VariantBasicType::PRIMITIVE ||
        !variant_benchmark_is_integer(value.primitive_id()) || value.get_int() != expected) {
        return Status::InternalError("parse_to_variant returned an unexpected integer leaf");
    }
    return Status::OK();
}

Status variant_benchmark_find_object_field(const VariantRef& object, std::string_view key,
                                           VariantRef* value) {
    if (object.basic_type() != VariantBasicType::OBJECT ||
        !object.object_find({key.data(), key.size()}, value)) {
        return Status::InternalError("parse_to_variant did not return object field {}", key);
    }
    return Status::OK();
}

Status variant_benchmark_validate_mixed_row(const VariantRef& root, size_t row) {
    VariantRef integer;
    VariantRef floating;
    VariantRef string;
    VariantRef boolean;
    VariantRef null;
    VariantRef object;
    VariantRef array;
    if (row % 2 == 0) {
        RETURN_IF_ERROR(variant_benchmark_find_object_field(root, "id", &integer));
        RETURN_IF_ERROR(variant_benchmark_find_object_field(root, "double", &floating));
        RETURN_IF_ERROR(variant_benchmark_find_object_field(root, "string", &string));
        RETURN_IF_ERROR(variant_benchmark_find_object_field(root, "boolean", &boolean));
        RETURN_IF_ERROR(variant_benchmark_find_object_field(root, "null", &null));
        RETURN_IF_ERROR(variant_benchmark_find_object_field(root, "object", &object));
        RETURN_IF_ERROR(variant_benchmark_find_object_field(root, "array", &array));
    } else {
        if (root.basic_type() != VariantBasicType::ARRAY || root.num_elements() != 7) {
            return Status::InternalError("parse_to_variant returned an invalid mixed array");
        }
        integer = root.array_at(0);
        floating = root.array_at(1);
        string = root.array_at(2);
        boolean = root.array_at(3);
        null = root.array_at(4);
        object = root.array_at(5);
        array = root.array_at(6);
    }

    RETURN_IF_ERROR(variant_benchmark_expect_integer(integer, static_cast<int64_t>(row)));
    if (floating.basic_type() != VariantBasicType::PRIMITIVE ||
        floating.primitive_id() != VariantPrimitiveId::DOUBLE ||
        floating.get_double() != static_cast<double>(row) + 0.25) {
        return Status::InternalError("parse_to_variant returned an unexpected double leaf");
    }
    const std::string expected_string = "row-" + std::to_string(row);
    if (string.basic_type() != VariantBasicType::SHORT_STRING ||
        string.get_string() != StringRef(expected_string)) {
        return Status::InternalError("parse_to_variant returned an unexpected string leaf");
    }
    const VariantPrimitiveId expected_boolean =
            row % 4 == 0 ? VariantPrimitiveId::TRUE_VALUE : VariantPrimitiveId::FALSE_VALUE;
    if (boolean.basic_type() != VariantBasicType::PRIMITIVE ||
        boolean.primitive_id() != expected_boolean || boolean.get_bool() != (row % 4 == 0)) {
        return Status::InternalError("parse_to_variant returned an unexpected boolean leaf");
    }
    if (!null.is_null() || object.basic_type() != VariantBasicType::OBJECT ||
        array.basic_type() != VariantBasicType::ARRAY) {
        return Status::InternalError("parse_to_variant returned an unexpected mixed nested value");
    }
    return Status::OK();
}

Status variant_benchmark_validate_parse_output(const ColumnPtr& output,
                                               VariantBenchmarkParseCase test_case) {
    if (!output || output->size() != VARIANT_BENCHMARK_BATCH_SIZE) {
        return Status::InternalError("parse_to_variant returned an invalid row count");
    }
    const auto* values = check_and_get_column<ColumnVariantV2>(output.get());
    if (values == nullptr) {
        return Status::InternalError("parse_to_variant did not return ColumnVariantV2");
    }
    if (test_case == VariantBenchmarkParseCase::MIXED) {
        constexpr std::array<VariantBasicType, 8> expected_types {
                VariantBasicType::OBJECT, VariantBasicType::ARRAY,  VariantBasicType::OBJECT,
                VariantBasicType::ARRAY,  VariantBasicType::OBJECT, VariantBasicType::ARRAY,
                VariantBasicType::OBJECT, VariantBasicType::ARRAY};
        for (size_t row = 0; row < expected_types.size(); ++row) {
            const VariantRef root = values->get_value_ref(row);
            if (root.basic_type() != expected_types[row]) {
                return Status::InternalError(
                        "parse_to_variant mixed row {} returned an unexpected root type", row);
            }
            RETURN_IF_ERROR(variant_benchmark_validate_mixed_row(root, row));
        }
        return Status::OK();
    }

    const VariantBasicType expected_root_type = test_case == VariantBenchmarkParseCase::ARRAY_100
                                                        ? VariantBasicType::ARRAY
                                                        : VariantBasicType::OBJECT;
    if (values->get_value_ref(0).basic_type() != expected_root_type ||
        values->get_value_ref(VARIANT_BENCHMARK_BATCH_SIZE - 1).basic_type() !=
                expected_root_type) {
        return Status::InternalError("parse_to_variant returned an unexpected root type");
    }

    for (const size_t row : {size_t {0}, VARIANT_BENCHMARK_BATCH_SIZE - 1}) {
        VariantRef value = values->get_value_ref(row);
        switch (test_case) {
        case VariantBenchmarkParseCase::NARROW_8: {
            VariantRef id;
            RETURN_IF_ERROR(variant_benchmark_find_object_field(value, "id", &id));
            RETURN_IF_ERROR(variant_benchmark_expect_integer(id, static_cast<int64_t>(row + 1)));
            break;
        }
        case VariantBenchmarkParseCase::DEEP_8:
            for (size_t level = 0; level < 8; ++level) {
                VariantRef nested;
                const std::string key = "level" + std::to_string(level);
                RETURN_IF_ERROR(variant_benchmark_find_object_field(value, key, &nested));
                value = nested;
            }
            {
                VariantRef target;
                RETURN_IF_ERROR(variant_benchmark_find_object_field(value, "target", &target));
                RETURN_IF_ERROR(
                        variant_benchmark_expect_integer(target, static_cast<int64_t>(row + 8)));
            }
            break;
        case VariantBenchmarkParseCase::ARRAY_100:
            if (value.num_elements() != 100) {
                return Status::InternalError("parse_to_variant returned an invalid Array100 row");
            }
            RETURN_IF_ERROR(variant_benchmark_expect_integer(value.array_at(99),
                                                             static_cast<int64_t>(row + 99)));
            break;
        case VariantBenchmarkParseCase::WIDE_1000: {
            VariantRef last;
            RETURN_IF_ERROR(variant_benchmark_find_object_field(value, "field0999", &last));
            RETURN_IF_ERROR(
                    variant_benchmark_expect_integer(last, static_cast<int64_t>(row + 999)));
            break;
        }
        case VariantBenchmarkParseCase::MIXED:
            __builtin_unreachable();
        }
    }
    return Status::OK();
}

Status variant_benchmark_validate_get_output(const ColumnPtr& output,
                                             const VariantBenchmarkGetConfig& config) {
    if (!output || output->size() != VARIANT_BENCHMARK_BATCH_SIZE) {
        return Status::InternalError("variant_get returned an invalid row count");
    }
    const auto* nullable = check_and_get_column<ColumnNullable>(output.get());
    if (nullable == nullptr) {
        return Status::InternalError("variant_get did not return a nullable column");
    }
    const auto* values = check_and_get_column<ColumnVariantV2>(&nullable->get_nested_column());
    if (values == nullptr) {
        return Status::InternalError("variant_get did not return ColumnVariantV2 values");
    }
    for (size_t row = 0; row < VARIANT_BENCHMARK_BATCH_SIZE; ++row) {
        if (!config.expected_base.has_value()) {
            if (!nullable->is_null_at(row)) {
                return Status::InternalError("variant_get missing-path result was not SQL NULL");
            }
            continue;
        }
        if (nullable->is_null_at(row)) {
            return Status::InternalError("variant_get hit result was SQL NULL");
        }
        const VariantRef value = values->get_value_ref(row);
        bool matches = false;
        switch (config.expected_value) {
        case VariantBenchmarkExpectedValue::INTEGER: {
            const int64_t expected =
                    *config.expected_base +
                    (config.add_row_to_expected ? static_cast<int64_t>(row) : int64_t {0});
            matches = value.get_int() == expected;
            break;
        }
        case VariantBenchmarkExpectedValue::DOUBLE:
            matches = value.get_double() == static_cast<double>(row) + 0.25;
            break;
        case VariantBenchmarkExpectedValue::STRING: {
            const std::string expected = "row-" + std::to_string(row);
            matches = value.get_string() == StringRef(expected);
            break;
        }
        case VariantBenchmarkExpectedValue::BOOLEAN:
            matches = value.get_bool() == (row % 2 == 0);
            break;
        }
        if (!matches) {
            return Status::InternalError("variant_get returned an unexpected value");
        }
    }
    return Status::OK();
}

DataTypePtr variant_benchmark_scalar_type(VariantBenchmarkScalarTarget target) {
    switch (target) {
    case VariantBenchmarkScalarTarget::BIGINT:
        return std::make_shared<DataTypeInt64>();
    case VariantBenchmarkScalarTarget::DOUBLE:
        return std::make_shared<DataTypeFloat64>();
    case VariantBenchmarkScalarTarget::STRING:
        return std::make_shared<DataTypeString>();
    case VariantBenchmarkScalarTarget::BOOLEAN:
        return std::make_shared<DataTypeBool>();
    }
    __builtin_unreachable();
}

Status variant_benchmark_validate_get_cast_output(const ColumnPtr& output,
                                                  const VariantBenchmarkGetConfig& config,
                                                  VariantBenchmarkScalarTarget target) {
    if (!output || output->size() != VARIANT_BENCHMARK_BATCH_SIZE) {
        return Status::InternalError("variant_get plus CAST returned an invalid row count");
    }
    const auto* nullable = check_and_get_column<ColumnNullable>(output.get());
    if (nullable == nullptr) {
        return Status::InternalError("variant_get plus CAST did not return a nullable column");
    }

    const auto* integers =
            target == VariantBenchmarkScalarTarget::BIGINT
                    ? check_and_get_column<ColumnInt64>(&nullable->get_nested_column())
                    : nullptr;
    const auto* doubles =
            target == VariantBenchmarkScalarTarget::DOUBLE
                    ? check_and_get_column<ColumnFloat64>(&nullable->get_nested_column())
                    : nullptr;
    const auto* strings =
            target == VariantBenchmarkScalarTarget::STRING
                    ? check_and_get_column<ColumnString>(&nullable->get_nested_column())
                    : nullptr;
    const auto* booleans =
            target == VariantBenchmarkScalarTarget::BOOLEAN
                    ? check_and_get_column<ColumnUInt8>(&nullable->get_nested_column())
                    : nullptr;
    if (integers == nullptr && doubles == nullptr && strings == nullptr && booleans == nullptr) {
        return Status::InternalError("variant_get plus CAST returned an unexpected value column");
    }

    for (size_t row = 0; row < VARIANT_BENCHMARK_BATCH_SIZE; ++row) {
        if (!config.expected_base.has_value()) {
            if (!nullable->is_null_at(row)) {
                return Status::InternalError(
                        "variant_get plus CAST missing-path result was not SQL NULL");
            }
            continue;
        }
        if (nullable->is_null_at(row)) {
            return Status::InternalError("variant_get plus CAST hit result was SQL NULL");
        }
        bool matches = false;
        switch (target) {
        case VariantBenchmarkScalarTarget::BIGINT: {
            const int64_t expected =
                    *config.expected_base +
                    (config.add_row_to_expected ? static_cast<int64_t>(row) : int64_t {0});
            matches = config.expected_value == VariantBenchmarkExpectedValue::INTEGER &&
                      integers->get_data()[row] == expected;
            break;
        }
        case VariantBenchmarkScalarTarget::DOUBLE: {
            const double expected =
                    config.expected_value == VariantBenchmarkExpectedValue::DOUBLE
                            ? static_cast<double>(row) + 0.25
                            : static_cast<double>(*config.expected_base +
                                                  (config.add_row_to_expected
                                                           ? static_cast<int64_t>(row)
                                                           : int64_t {0}));
            matches = (config.expected_value == VariantBenchmarkExpectedValue::INTEGER ||
                       config.expected_value == VariantBenchmarkExpectedValue::DOUBLE) &&
                      doubles->get_data()[row] == expected;
            break;
        }
        case VariantBenchmarkScalarTarget::STRING: {
            const std::string expected = "row-" + std::to_string(row);
            matches = config.expected_value == VariantBenchmarkExpectedValue::STRING &&
                      strings->get_data_at(row) == StringRef(expected);
            break;
        }
        case VariantBenchmarkScalarTarget::BOOLEAN:
            matches = config.expected_value == VariantBenchmarkExpectedValue::BOOLEAN &&
                      booleans->get_data()[row] == static_cast<UInt8>(row % 2 == 0);
            break;
        }
        if (!matches) {
            return Status::InternalError("variant_get plus CAST returned an unexpected value");
        }
    }
    return Status::OK();
}

std::string variant_benchmark_expected_object_array_schema(size_t fields) {
    std::string schema = "ARRAY<OBJECT<";
    for (size_t field = 0; field < fields; ++field) {
        if (field != 0) {
            schema.append(", ");
        }
        variant_benchmark_append_wide_field_token(&schema, field);
        schema.append(": BIGINT");
    }
    schema.append(">>");
    return schema;
}

Status variant_benchmark_validate_typeof_output(const ColumnPtr& output) {
    if (!output || output->size() != VARIANT_BENCHMARK_BATCH_SIZE) {
        return Status::InternalError("variant_typeof returned an invalid row count");
    }
    const auto* nullable = check_and_get_column<ColumnNullable>(output.get());
    if (nullable == nullptr) {
        return Status::InternalError("variant_typeof did not return a nullable column");
    }
    const auto* values = check_and_get_column<ColumnString>(&nullable->get_nested_column());
    if (values == nullptr) {
        return Status::InternalError("variant_typeof did not return String values");
    }
    const std::string expected = variant_benchmark_expected_object_array_schema(100);
    for (const size_t row : {size_t {0}, VARIANT_BENCHMARK_BATCH_SIZE - 1}) {
        if (nullable->is_null_at(row) || values->get_data_at(row) != StringRef(expected)) {
            return Status::InternalError("variant_typeof returned an unexpected schema");
        }
    }
    return Status::OK();
}

Status variant_benchmark_parse_source(ColumnPtr input, ColumnPtr* output) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const DataTypePtr variant_type = std::make_shared<DataTypeVariantV2>();
    VariantBenchmarkFunctionInvocation invocation(
            "parse_to_variant", {{std::move(input), string_type, "json"}}, {0}, variant_type);
    RETURN_IF_ERROR(invocation.open());
    RETURN_IF_ERROR(invocation.execute());
    *output = invocation.output();
    return invocation.close();
}

bool variant_benchmark_check_status(benchmark::State& state, const Status& status) {
    if (status.ok()) {
        return true;
    }
    const std::string error = status.to_string();
    state.SkipWithError(error.c_str());
    return false;
}

void variant_benchmark_set_throughput(benchmark::State& state, size_t logical_bytes_per_batch) {
    const int64_t iterations = state.iterations();
    state.SetItemsProcessed(iterations * VARIANT_BENCHMARK_BATCH_SIZE);
    state.SetBytesProcessed(iterations * static_cast<int64_t>(logical_bytes_per_batch));
    state.counters["batch_rows"] =
            benchmark::Counter(static_cast<double>(VARIANT_BENCHMARK_BATCH_SIZE));
    state.counters["logical_bytes_per_batch"] =
            benchmark::Counter(static_cast<double>(logical_bytes_per_batch));
}

void BM_Doris_ParseToVariant(benchmark::State& state, VariantBenchmarkParseCase test_case) {
    VariantBenchmarkInput input = variant_benchmark_parse_input(test_case);
    const size_t logical_bytes_per_batch = input.logical_bytes;
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const DataTypePtr variant_type = std::make_shared<DataTypeVariantV2>();
    VariantBenchmarkFunctionInvocation invocation("parse_to_variant",
                                                  {{std::move(input.column), string_type, "json"}},
                                                  {0}, variant_type);
    if (!variant_benchmark_check_status(state, invocation.open()) ||
        !variant_benchmark_check_status(state, invocation.execute()) ||
        !variant_benchmark_check_status(
                state, variant_benchmark_validate_parse_output(invocation.output(), test_case))) {
        return;
    }
    invocation.reset_output();

    for (auto _ : state) {
        const Status status = invocation.execute();
        if (!variant_benchmark_check_status(state, status)) {
            break;
        }
        const ColumnPtr& output = invocation.output();
        const IColumn* output_ptr = output.get();
        size_t output_bytes = output->byte_size();
        benchmark::DoNotOptimize(output_ptr);
        benchmark::DoNotOptimize(output_bytes);
        benchmark::ClobberMemory();
        invocation.reset_output();
    }

    const Status close_status = invocation.close();
    if (!state.skipped() && variant_benchmark_check_status(state, close_status)) {
        variant_benchmark_set_throughput(state, logical_bytes_per_batch);
        state.counters["cross_engine_comparable"] = benchmark::Counter(0.0);
    }
}

void BM_Doris_VariantGet(benchmark::State& state, VariantBenchmarkGetCase test_case) {
    const VariantBenchmarkGetConfig config = variant_benchmark_get_config(test_case);
    VariantBenchmarkInput input = variant_benchmark_get_input(config.shape);
    const size_t logical_bytes_per_batch = input.logical_bytes;
    ColumnPtr variant_input;
    if (!variant_benchmark_check_status(
                state, variant_benchmark_parse_source(std::move(input.column), &variant_input))) {
        return;
    }

    const DataTypePtr variant_type = std::make_shared<DataTypeVariantV2>();
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const DataTypePtr result_type = make_nullable(variant_type);
    VariantBenchmarkFunctionInvocation invocation(
            "variant_get",
            {{std::move(variant_input), variant_type, "variant"},
             {variant_benchmark_constant_path(config.path), string_type, "path"}},
            {0, 1}, result_type);
    if (!variant_benchmark_check_status(state, invocation.open(1)) ||
        !variant_benchmark_check_status(state, invocation.execute()) ||
        !variant_benchmark_check_status(
                state, variant_benchmark_validate_get_output(invocation.output(), config))) {
        return;
    }
    invocation.reset_output();

    for (auto _ : state) {
        const Status status = invocation.execute();
        if (!variant_benchmark_check_status(state, status)) {
            break;
        }
        const ColumnPtr& output = invocation.output();
        const IColumn* output_ptr = output.get();
        size_t output_bytes = output->byte_size();
        benchmark::DoNotOptimize(output_ptr);
        benchmark::DoNotOptimize(output_bytes);
        benchmark::ClobberMemory();
        invocation.reset_output();
    }

    const Status close_status = invocation.close();
    if (!state.skipped() && variant_benchmark_check_status(state, close_status)) {
        variant_benchmark_set_throughput(state, logical_bytes_per_batch);
    }
}

// StarRocks exposes get_variant_int/get_variant_double as fused lookup-and-convert functions.
// This benchmark measures the equivalent Doris SQL expression, including the materialized
// Nullable(VariantV2) intermediate produced by variant_get and its release on every iteration.
void BM_Doris_VariantGetCast(benchmark::State& state, VariantBenchmarkGetCase test_case,
                             VariantBenchmarkScalarTarget target) {
    const VariantBenchmarkGetConfig config = variant_benchmark_get_config(test_case);
    VariantBenchmarkInput input = variant_benchmark_get_input(config.shape);
    const size_t logical_bytes_per_batch = input.logical_bytes;
    ColumnPtr variant_input;
    if (!variant_benchmark_check_status(
                state, variant_benchmark_parse_source(std::move(input.column), &variant_input))) {
        return;
    }

    const DataTypePtr variant_type = std::make_shared<DataTypeVariantV2>();
    const DataTypePtr nullable_variant_type = make_nullable(variant_type);
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    VariantBenchmarkFunctionInvocation get_invocation(
            "variant_get",
            {{std::move(variant_input), variant_type, "variant"},
             {variant_benchmark_constant_path(config.path), string_type, "path"}},
            {0, 1}, nullable_variant_type);
    if (!variant_benchmark_check_status(state, get_invocation.open(1)) ||
        !variant_benchmark_check_status(state, get_invocation.execute()) ||
        !variant_benchmark_check_status(
                state, variant_benchmark_validate_get_output(get_invocation.output(), config))) {
        return;
    }

    const DataTypePtr scalar_type = variant_benchmark_scalar_type(target);
    const DataTypePtr result_type = make_nullable(scalar_type);
    VariantBenchmarkFunctionInvocation cast_invocation(
            "CAST",
            {{get_invocation.output(), nullable_variant_type, "value"},
             {nullptr, result_type, "target_type"}},
            {0}, result_type);
    if (!variant_benchmark_check_status(state, cast_invocation.open()) ||
        !variant_benchmark_check_status(state, cast_invocation.execute()) ||
        !variant_benchmark_check_status(state, variant_benchmark_validate_get_cast_output(
                                                       cast_invocation.output(), config, target))) {
        return;
    }
    cast_invocation.reset_output();
    cast_invocation.replace_argument(0, ColumnPtr {});
    get_invocation.reset_output();

    for (auto _ : state) {
        Status status = get_invocation.execute();
        if (!variant_benchmark_check_status(state, status)) {
            break;
        }
        cast_invocation.replace_argument(0, get_invocation.output());
        status = cast_invocation.execute();
        if (!variant_benchmark_check_status(state, status)) {
            cast_invocation.replace_argument(0, ColumnPtr {});
            get_invocation.reset_output();
            break;
        }
        const ColumnPtr& output = cast_invocation.output();
        const IColumn* output_ptr = output.get();
        size_t output_bytes = output->byte_size();
        benchmark::DoNotOptimize(output_ptr);
        benchmark::DoNotOptimize(output_bytes);
        benchmark::ClobberMemory();
        cast_invocation.reset_output();
        cast_invocation.replace_argument(0, ColumnPtr {});
        get_invocation.reset_output();
    }

    const Status cast_close_status = cast_invocation.close();
    const Status get_close_status = get_invocation.close();
    const bool cast_closed = variant_benchmark_check_status(state, cast_close_status);
    const bool get_closed = variant_benchmark_check_status(state, get_close_status);
    if (!state.skipped() && cast_closed && get_closed) {
        variant_benchmark_set_throughput(state, logical_bytes_per_batch);
        state.counters["cross_engine_comparable"] = benchmark::Counter(1.0);
        state.counters["intermediate_variant_materialized"] = benchmark::Counter(1.0);
    }
}

// Doris returns a full recursive Spark-style schema here. StarRocks variant_typeof returns only a
// top-level tag, so the two timings must not be reported as a cross-engine speed ratio.
void BM_Doris_VariantTypeofRecursiveSchemaObjectArray100(benchmark::State& state) {
    VariantBenchmarkInput input = variant_benchmark_typeof_input();
    const size_t logical_bytes_per_batch = input.logical_bytes;
    ColumnPtr variant_input;
    if (!variant_benchmark_check_status(
                state, variant_benchmark_parse_source(std::move(input.column), &variant_input))) {
        return;
    }

    const DataTypePtr variant_type = std::make_shared<DataTypeVariantV2>();
    const DataTypePtr result_type = make_nullable(std::make_shared<DataTypeString>());
    VariantBenchmarkFunctionInvocation invocation(
            "variant_typeof", {{std::move(variant_input), variant_type, "variant"}}, {0},
            result_type);
    if (!variant_benchmark_check_status(state, invocation.open()) ||
        !variant_benchmark_check_status(state, invocation.execute()) ||
        !variant_benchmark_check_status(
                state, variant_benchmark_validate_typeof_output(invocation.output()))) {
        return;
    }
    invocation.reset_output();

    for (auto _ : state) {
        const Status status = invocation.execute();
        if (!variant_benchmark_check_status(state, status)) {
            break;
        }
        const ColumnPtr& output = invocation.output();
        const IColumn* output_ptr = output.get();
        size_t output_bytes = output->byte_size();
        benchmark::DoNotOptimize(output_ptr);
        benchmark::DoNotOptimize(output_bytes);
        benchmark::ClobberMemory();
        invocation.reset_output();
    }

    const Status close_status = invocation.close();
    if (!state.skipped() && variant_benchmark_check_status(state, close_status)) {
        variant_benchmark_set_throughput(state, logical_bytes_per_batch);
        state.counters["cross_engine_comparable"] = 0;
    }
}

BENCHMARK_CAPTURE(BM_Doris_ParseToVariant, Narrow8, VariantBenchmarkParseCase::NARROW_8)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_ParseToVariant, Deep8, VariantBenchmarkParseCase::DEEP_8)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_ParseToVariant, Array100, VariantBenchmarkParseCase::ARRAY_100)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_ParseToVariant, Wide1000, VariantBenchmarkParseCase::WIDE_1000)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_ParseToVariant, Mixed, VariantBenchmarkParseCase::MIXED)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);

BENCHMARK_CAPTURE(BM_Doris_VariantGet, ShallowHit, VariantBenchmarkGetCase::SHALLOW_HIT)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGet, ShallowMissing, VariantBenchmarkGetCase::SHALLOW_MISSING)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGet, Deep8Hit, VariantBenchmarkGetCase::DEEP_8_HIT)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGet, Deep8Missing, VariantBenchmarkGetCase::DEEP_8_MISSING)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGet, Array100First, VariantBenchmarkGetCase::ARRAY_100_FIRST)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGet, Array100Middle, VariantBenchmarkGetCase::ARRAY_100_MIDDLE)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGet, Array100Last, VariantBenchmarkGetCase::ARRAY_100_LAST)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGet, Array100Missing, VariantBenchmarkGetCase::ARRAY_100_MISSING)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGet, Wide1000First, VariantBenchmarkGetCase::WIDE_1000_FIRST)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGet, Wide1000Middle, VariantBenchmarkGetCase::WIDE_1000_MIDDLE)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGet, Wide1000Last, VariantBenchmarkGetCase::WIDE_1000_LAST)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGet, Wide1000Missing, VariantBenchmarkGetCase::WIDE_1000_MISSING)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);

BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, Log32Int, VariantBenchmarkGetCase::LOG_32_INT,
                  VariantBenchmarkScalarTarget::BIGINT)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, Log32Double, VariantBenchmarkGetCase::LOG_32_DOUBLE,
                  VariantBenchmarkScalarTarget::DOUBLE)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, Log32String, VariantBenchmarkGetCase::LOG_32_STRING,
                  VariantBenchmarkScalarTarget::STRING)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, Log32Bool, VariantBenchmarkGetCase::LOG_32_BOOL,
                  VariantBenchmarkScalarTarget::BOOLEAN)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);

BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, BigIntShallowHit, VariantBenchmarkGetCase::SHALLOW_HIT,
                  VariantBenchmarkScalarTarget::BIGINT)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, BigIntDeep8Hit, VariantBenchmarkGetCase::DEEP_8_HIT,
                  VariantBenchmarkScalarTarget::BIGINT)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, BigIntArray100Last,
                  VariantBenchmarkGetCase::ARRAY_100_LAST, VariantBenchmarkScalarTarget::BIGINT)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, BigIntWide32Last, VariantBenchmarkGetCase::WIDE_32_LAST,
                  VariantBenchmarkScalarTarget::BIGINT)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, BigIntWide1000Last,
                  VariantBenchmarkGetCase::WIDE_1000_LAST, VariantBenchmarkScalarTarget::BIGINT)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);

BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, DoubleShallowHit, VariantBenchmarkGetCase::SHALLOW_HIT,
                  VariantBenchmarkScalarTarget::DOUBLE)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, DoubleDeep8Hit, VariantBenchmarkGetCase::DEEP_8_HIT,
                  VariantBenchmarkScalarTarget::DOUBLE)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, DoubleArray100Last,
                  VariantBenchmarkGetCase::ARRAY_100_LAST, VariantBenchmarkScalarTarget::DOUBLE)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, DoubleWide32Last, VariantBenchmarkGetCase::WIDE_32_LAST,
                  VariantBenchmarkScalarTarget::DOUBLE)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);
BENCHMARK_CAPTURE(BM_Doris_VariantGetCast, DoubleWide1000Last,
                  VariantBenchmarkGetCase::WIDE_1000_LAST, VariantBenchmarkScalarTarget::DOUBLE)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);

BENCHMARK(BM_Doris_VariantTypeofRecursiveSchemaObjectArray100)
        ->Unit(benchmark::kMicrosecond)
        ->UseRealTime()
        ->MinTime(1.0);

} // namespace
} // namespace doris
