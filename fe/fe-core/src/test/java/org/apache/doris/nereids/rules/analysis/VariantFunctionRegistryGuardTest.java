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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.BuiltinScalarFunctions;
import org.apache.doris.catalog.FunctionHelper.ScalarFunc;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Guards Variant function registration and the pre-Variant JSON/JSONB overload snapshot. */
public class VariantFunctionRegistryGuardTest {

    @Test
    public void testJsonFunctionOverloadSnapshotDoesNotDrift() {
        Map<String, String> expected = expectedJsonFunctionSnapshot();
        Map<String, String> actual = new TreeMap<>();
        for (ScalarFunc function : BuiltinScalarFunctions.INSTANCE.scalarFunctions) {
            String className = function.functionClass.getSimpleName();
            if (!className.contains("Json")) {
                continue;
            }
            for (String name : function.names) {
                Assertions.assertNull(actual.put(name, className), "duplicate JSON function " + name);
            }
        }
        Assertions.assertEquals(expected, actual);

        FunctionRegistry registry = new FunctionRegistry();
        expected.forEach((name, className) -> assertRegistration(registry, name, className));
    }

    @Test
    public void testVariantP0FunctionsAreRegisteredExactlyOnce() {
        FunctionRegistry registry = new FunctionRegistry();
        Map.of(
                "parse_to_variant", "ParseToVariant",
                "parse_to_variant_error_to_null", "ParseToVariantErrorToNull",
                "try_parse_to_variant", "TryParseToVariant",
                "variant_contains", "VariantContains",
                "variant_exists_path", "VariantExistsPath",
                "variant_get", "VariantGet",
                "variant_is_null", "VariantIsNull",
                "variant_keys", "VariantKeys",
                "variant_length", "VariantLength"
        ).forEach((name, className) -> assertRegistration(registry, name, className));
    }

    private static void assertRegistration(FunctionRegistry registry, String name, String className) {
        List<FunctionBuilder> builders = registry.tryGetBuiltinBuilders(name).orElseThrow();
        int expectedBuilderCount = switch (className) {
            case "JsonbParseErrorToValue", "JsonLength", "JsonContains", "JsonKeys" -> 2;
            default -> 1;
        };
        Assertions.assertEquals(expectedBuilderCount, builders.size(),
                "unexpected overload builder count for " + name);
        builders.forEach(builder -> Assertions.assertEquals(
                className, builder.functionClass().getSimpleName(), name));
    }

    private static Map<String, String> expectedJsonFunctionSnapshot() {
        ImmutableMap.Builder<String, String> snapshot = ImmutableMap.builder();
        add(snapshot, "JsonArray", "json_array", "jsonb_array");
        add(snapshot, "JsonArrayIgnoreNull", "json_array_ignore_null", "jsonb_array_ignore_null");
        add(snapshot, "JsonObject", "json_object", "jsonb_object");
        add(snapshot, "JsonObjectFlatten", "json_object_flatten");
        add(snapshot, "JsonQuote", "json_quote");
        add(snapshot, "JsonUnQuote", "json_unquote");
        add(snapshot, "JsonExtractNoQuotes", "json_extract_no_quotes", "jsonb_extract_no_quotes");
        add(snapshot, "JsonHash", "json_hash", "jsonb_hash");
        add(snapshot, "JsonInsert", "json_insert", "jsonb_insert");
        add(snapshot, "JsonReplace", "json_replace", "jsonb_replace");
        add(snapshot, "JsonSet", "json_set", "jsonb_set");
        add(snapshot, "JsonRemove", "json_remove");
        add(snapshot, "JsonbExistsPath", "json_exists_path", "jsonb_exists_path");
        add(snapshot, "JsonbExtract", "jsonb_extract", "json_extract");
        add(snapshot, "JsonbExtractBigint", "jsonb_extract_bigint", "json_extract_bigint", "get_json_bigint");
        add(snapshot, "JsonbExtractLargeint", "jsonb_extract_largeint", "json_extract_largeint");
        add(snapshot, "JsonbExtractBool", "jsonb_extract_bool", "json_extract_bool");
        add(snapshot, "JsonbExtractDouble", "jsonb_extract_double", "json_extract_double", "get_json_double");
        add(snapshot, "JsonbExtractInt", "jsonb_extract_int", "json_extract_int", "get_json_int");
        add(snapshot, "JsonbExtractIsnull", "json_extract_isnull", "jsonb_extract_isnull");
        add(snapshot, "JsonbExtractString", "jsonb_extract_string", "json_extract_string", "get_json_string");
        add(snapshot, "JsonbParse", "jsonb_parse", "json_parse");
        add(snapshot, "JsonbParseErrorToNull", "jsonb_parse_error_to_null", "json_parse_error_to_null");
        add(snapshot, "JsonbParseErrorToValue", "jsonb_parse_error_to_value", "json_parse_error_to_value");
        add(snapshot, "JsonSearch", "json_search");
        add(snapshot, "JsonbValid", "json_valid", "jsonb_valid");
        add(snapshot, "JsonbType", "json_type", "jsonb_type");
        add(snapshot, "JsonLength", "json_length");
        add(snapshot, "JsonContains", "json_contains");
        add(snapshot, "JsonKeys", "json_keys", "jsonb_keys");
        add(snapshot, "NormalizeJsonNumbersToDouble",
                "normalize_json_numbers_to_double", "normalize_jsonb_numbers_to_double");
        add(snapshot, "SortJsonbObjectKeys", "sort_json_object_keys", "sort_jsonb_object_keys");
        add(snapshot, "ToJson", "to_json");
        return snapshot.build();
    }

    private static void add(ImmutableMap.Builder<String, String> snapshot, String className, String... names) {
        for (String name : names) {
            snapshot.put(name, className);
        }
    }
}
