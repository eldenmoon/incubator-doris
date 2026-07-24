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

suite("variant_native_functions") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    // Public routing plus JSONPath root, array, and quoted-member grammar.
    order_qt_variant_get """
        SELECT
            CAST(variant_get(parse_to_variant('{"a":[1,null,3],"a.b":"quoted"}'), '\$.a[0]') AS STRING),
            CAST(variant_get(parse_to_variant('{"a":[1,null,3],"a.b":"quoted"}'), '\$."a.b"') AS STRING),
            CAST(variant_get(parse_to_variant('[10,20]'), '\$') AS STRING)
    """

    order_qt_variant_exists_path """
        SELECT
            variant_exists_path(parse_to_variant('{"a":null}'), '\$.a'),
            variant_exists_path(parse_to_variant('{"a":null}'), '\$.missing')
    """

    order_qt_variant_is_null """
        SELECT
            variant_is_null(parse_to_variant('null')),
            variant_is_null(parse_to_variant('1'))
    """

    // Non-object input is not an execution error: json_keys-compatible semantics return NULL.
    order_qt_variant_keys """
        SELECT
            variant_keys(parse_to_variant('{"b":2,"a":1}')),
            variant_keys(parse_to_variant('1'))
    """

    // Object/array length and scalar=1 follow json_length semantics.
    order_qt_variant_length """
        SELECT
            variant_length(parse_to_variant('{"b":2,"a":1}')),
            variant_length(parse_to_variant('[1,2,null]')),
            variant_length(parse_to_variant('1'))
    """

    // Incompatible containment is false rather than an execution error.
    order_qt_variant_contains """
        SELECT
            variant_contains(parse_to_variant('[1,2,3]'), parse_to_variant('2')),
            variant_contains(parse_to_variant('{"a":1,"b":2}'), parse_to_variant('{"a":1}')),
            variant_contains(parse_to_variant('[1,2]'), parse_to_variant('3'))
    """

    // SQL NULL propagates through every public native function.
    order_qt_null_inputs """
        SELECT
            variant_get(CAST(NULL AS VARIANT), '\$'),
            variant_exists_path(CAST(NULL AS VARIANT), '\$'),
            variant_is_null(CAST(NULL AS VARIANT)),
            variant_keys(CAST(NULL AS VARIANT)),
            variant_length(CAST(NULL AS VARIANT)),
            variant_contains(CAST(NULL AS VARIANT), parse_to_variant('1'))
    """

    // Exercise result coercion for each function without introducing a JSONB conversion.
    order_qt_type_conversions """
        SELECT
            CAST(variant_get(parse_to_variant('{"n":42}'), '\$.n') AS BIGINT),
            CAST(variant_exists_path(parse_to_variant('{"n":42}'), '\$.n') AS INT),
            CAST(variant_is_null(parse_to_variant('null')) AS INT),
            size(variant_keys(parse_to_variant('{"n":42}'))),
            CAST(variant_length(parse_to_variant('[1,2]')) AS BIGINT),
            CAST(variant_contains(parse_to_variant('[1,2]'), parse_to_variant('2')) AS INT)
    """

    // Compare results to the corresponding JSONB functions. Variant object keys are canonical
    // byte-sorted, so normalize the JSONB key order before comparing.
    order_qt_jsonb_alignment """
        SELECT
            CAST(variant_get(parse_to_variant('{"a":[1]}'), '\$.a[0]') AS STRING)
                = CAST(json_extract(CAST('{"a":[1]}' AS JSON), '\$.a[0]') AS STRING),
            variant_exists_path(parse_to_variant('{"a":null}'), '\$.a')
                = json_exists_path(CAST('{"a":null}' AS JSON), '\$.a'),
            variant_is_null(parse_to_variant('null'))
                = json_extract_isnull(CAST('null' AS JSON), '\$'),
            array_sort(variant_keys(parse_to_variant('{"b":2,"a":1}')))
                = array_sort(json_keys(CAST('{"b":2,"a":1}' AS JSON))),
            variant_length(parse_to_variant('[1,2,null]'))
                = json_length(CAST('[1,2,null]' AS JSON)),
            variant_contains(parse_to_variant('[1,2,3]'), parse_to_variant('2'))
                = json_contains(CAST('[1,2,3]' AS JSON), CAST('2' AS JSON))
    """

    // Appendix A P1 aliases are native rewrites over the same Variant kernels.
    order_qt_json_p1_aliases """
        SELECT
            CAST(json_extract(parse_to_variant('{"n":42,"z":null}'), '\$.n') AS STRING),
            CAST(jsonb_extract(parse_to_variant('{"n":42,"z":null}'), '\$.n') AS STRING),
            json_extract_no_quotes(parse_to_variant('{"n":42,"z":null}'), '\$.n'),
            json_extract_isnull(parse_to_variant('{"n":42,"z":null}'), '\$.z'),
            json_extract_int(parse_to_variant('{"n":42}'), '\$.n'),
            get_json_bigint(parse_to_variant('{"n":42}'), '\$.n'),
            json_extract_largeint(parse_to_variant('{"n":42}'), '\$.n'),
            get_json_double(parse_to_variant('{"n":1.5}'), '\$.n'),
            get_json_string(parse_to_variant('{"s":"x"}'), '\$.s'),
            json_extract_bool(parse_to_variant('{"b":true}'), '\$.b'),
            json_exists_path(parse_to_variant('{"n":42}'), '\$.n'),
            json_type(parse_to_variant('{"n":42}'), '\$.n')
    """

    explain {
        sql "SELECT json_extract(parse_to_variant('{\"n\":42}'), '\$.n')"
        contains "variant_get"
        notContains "CAST(parse_to_variant"
    }

    test {
        sql "SELECT variant_get(parse_to_variant('{}'), 'a')"
        exception "Invalid JSON path for function variant_get"
    }

    test {
        sql "SELECT variant_exists_path(parse_to_variant('{}'), '\$.*')"
        exception "may not contain wildcard tokens"
    }

    test {
        sql "SELECT variant_get(parse_to_variant('{}'), '\$.a[last]')"
        exception "does not support last-relative array indexes"
    }

    test {
        sql """
            SELECT json_extract(parse_to_variant('{"a":1}'), concat('\$.', CAST(number AS STRING)))
            FROM numbers("number" = "1")
        """
        exception "variant_get path must be constant"
    }
}
