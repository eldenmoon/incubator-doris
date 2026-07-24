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

suite("variant_function_registry") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"
    sql "SET enable_strict_cast = false"

    order_qt_explicit_jsonb_escape_hatch """
        SELECT
            CAST(CAST(parse_to_variant('{"a":1}') AS JSON) AS STRING),
            CAST(json_set(CAST(parse_to_variant('{"a":1}') AS JSON), '\$.b', 2) AS STRING),
            CAST(json_search(CAST(parse_to_variant('{"a":"x"}') AS JSON), 'one', 'x') AS STRING),
            json_keys(CAST(parse_to_variant('{"b":2,"a":1}') AS JSON)),
            json_length(CAST(parse_to_variant('[1,2]') AS JSON)),
            json_contains(CAST(parse_to_variant('[1,2]') AS JSON), CAST(parse_to_variant('2') AS JSON)),
            CAST(json_object_flatten(CAST(parse_to_variant('{"a":{"b":1}}') AS JSON)) AS STRING),
            CAST(sort_json_object_keys(CAST(parse_to_variant('{"b":2,"a":1}') AS JSON)) AS STRING),
            CAST(normalize_json_numbers_to_double(CAST(parse_to_variant('{"a":1}') AS JSON)) AS STRING)
    """

    // Existing JSONB overloads must keep the same decisions and results after the escape hatch opens.
    order_qt_existing_jsonb_overload_alignment """
        SELECT
            CAST(CAST(parse_to_variant('{"a":1}') AS JSON) AS STRING)
                = CAST(CAST('{"a":1}' AS JSON) AS STRING),
            json_hash(CAST(parse_to_variant('{"a":1}') AS JSON))
                = json_hash(CAST('{"a":1}' AS JSON)),
            CAST(json_set(CAST(parse_to_variant('{"a":1}') AS JSON), '\$.b', 2) AS STRING)
                = CAST(json_set(CAST('{"a":1}' AS JSON), '\$.b', 2) AS STRING),
            CAST(json_search(CAST(parse_to_variant('{"a":"x"}') AS JSON), 'one', 'x') AS STRING)
                = CAST(json_search(CAST('{"a":"x"}' AS JSON), 'one', 'x') AS STRING),
            array_sort(json_keys(CAST(parse_to_variant('{"b":2,"a":1}') AS JSON)))
                = array_sort(json_keys(CAST('{"b":2,"a":1}' AS JSON))),
            json_length(CAST(parse_to_variant('[1,2]') AS JSON))
                = json_length(CAST('[1,2]' AS JSON)),
            json_contains(CAST(parse_to_variant('[1,2]') AS JSON), CAST(parse_to_variant('2') AS JSON))
                = json_contains(CAST('[1,2]' AS JSON), CAST('2' AS JSON)),
            CAST(json_object_flatten(CAST(parse_to_variant('{"a":{"b":1}}') AS JSON)) AS STRING)
                = CAST(json_object_flatten(CAST('{"a":{"b":1}}' AS JSON)) AS STRING),
            CAST(sort_json_object_keys(CAST(parse_to_variant('{"b":2,"a":1}') AS JSON)) AS STRING)
                = CAST(sort_json_object_keys(CAST('{"b":2,"a":1}' AS JSON)) AS STRING),
            CAST(normalize_json_numbers_to_double(CAST(parse_to_variant('{"a":1}') AS JSON)) AS STRING)
                = CAST(normalize_json_numbers_to_double(CAST('{"a":1}' AS JSON)) AS STRING)
    """

    // JSONB-only cold functions may not silently select a JSONB overload for bare Variant input.
    [
        "SELECT json_hash(parse_to_variant('{}'))",
        "SELECT json_object_flatten(parse_to_variant('{}'))",
        "SELECT json_search(parse_to_variant('{\"a\":\"x\"}'), 'one', 'x')",
        "SELECT json_length(parse_to_variant('[]'))",
        "SELECT json_contains(parse_to_variant('[]'), CAST('1' AS JSON))",
        "SELECT json_keys(parse_to_variant('{}'))",
        "SELECT normalize_json_numbers_to_double(parse_to_variant('{}'))",
        "SELECT sort_json_object_keys(parse_to_variant('{}'))"
    ].each { statement ->
        test {
            sql statement
            exception "Can not find the compatibility function signature"
        }
    }

    [
        "SELECT json_insert(parse_to_variant('{}'), '\$.a', 1)",
        "SELECT json_replace(parse_to_variant('{}'), '\$.a', 1)",
        "SELECT json_set(parse_to_variant('{}'), '\$.a', 1)",
        "SELECT json_remove(parse_to_variant('{}'), '\$.a')"
    ].each { statement ->
        test {
            sql statement
            exception "argument 1 requires JSON type"
        }
    }

    sql "SET enable_strict_cast = true"
    order_qt_strict_jsonb_cast """
        SELECT CAST(CAST(parse_to_variant('{"a":1}') AS JSON) AS STRING)
    """
}
