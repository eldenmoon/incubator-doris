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

suite("variant_get_typeof", "p0,nonConcurrent") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"
    sql "SET enable_variant_v2 = true"

    qt_variant_get_paths """
        SELECT
            CAST(variant_get(
                parse_to_variant('{"a":[{"b":10},null],"a.b":20,"config":{"key":30}}'),
                '\$.a[0].b') AS STRING),
            CAST(variant_get(
                parse_to_variant('{"a":[{"b":10},null],"a.b":20,"config":{"key":30}}'),
                '\$."a.b"') AS STRING),
            CAST(variant_get(
                parse_to_variant('{"a":[{"b":10},null],"a.b":20,"config":{"key":30}}'),
                '\$.config["key"]') AS STRING),
            variant_get(parse_to_variant('{"a":1}'), '\$.missing') IS NULL
    """

    qt_variant_get_nulls """
        SELECT
            CAST(variant_get(parse_to_variant('null'), '\$') AS STRING),
            variant_get(parse_to_variant('null'), '\$') IS NULL,
            variant_get(parse_to_variant(CAST(NULL AS STRING)), '\$') IS NULL,
            variant_get(parse_to_variant('{"a":1}'), CAST(NULL AS STRING)) IS NULL
    """

    qt_variant_typeof_scalars """
        SELECT
            variant_typeof(parse_to_variant('null')),
            variant_typeof(parse_to_variant('true')),
            variant_typeof(parse_to_variant('1')),
            variant_typeof(parse_to_variant('1E0')),
            variant_typeof(parse_to_variant('"x"')),
            variant_typeof(parse_to_variant(CAST(NULL AS STRING))) IS NULL
    """

    qt_variant_typeof_containers """
        SELECT
            variant_typeof(parse_to_variant('[]')),
            variant_typeof(parse_to_variant('[null,1]')),
            variant_typeof(parse_to_variant('[1,"1"]')),
            variant_typeof(parse_to_variant(
                '{"b":{"c":"x"},"a":["x"],"a.b":1,"a`b":true}'))
    """

    qt_variant_typeof_decimal """
        SELECT variant_typeof(CAST(CAST(1.00 AS DECIMAL(10, 2)) AS VARIANT))
    """

    order_qt_variant_get_multiple_rows """
        SELECT number,
               CAST(variant_get(
                   parse_to_variant(concat('{"a":', CAST(number AS STRING), '}')),
                   '\$.a') AS STRING)
        FROM numbers("number" = "3")
        ORDER BY number
    """

    test {
        sql "SELECT variant_get(parse_to_variant('{}'), '\$.a[*]')"
        exception "Invalid JSONPath for variant_get"
    }

    test {
        sql """
            SELECT variant_get(
                parse_to_variant('{"a":1}'), concat('\$.', CAST(number AS STRING)))
            FROM numbers("number" = "1")
        """
        exception "variant_get path must be constant"
    }
}
