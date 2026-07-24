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

suite("test_variant_array_subscript", "p0") {
    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    sql "set default_variant_enable_nested_group = false"
    sql "set default_variant_max_subcolumns_count = 100"

    sql "DROP TABLE IF EXISTS test_variant_array_subscript"
    sql """
        CREATE TABLE test_variant_array_subscript (
            id BIGINT,
            v VARIANT
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        )
    """

    sql """
        INSERT INTO test_variant_array_subscript VALUES
        (1, parse_to_variant('{"items":{"type":["e2e_QC","platform_QC"]}}'))
    """
    sql "sync"

    explain {
        verbose true
        sql """
            SELECT CAST(v['items']['type'] AS ARRAY<STRING>)[1]
            FROM test_variant_array_subscript
        """
        contains "all access paths: [v.items.type]"
        contains "element_at(CAST(v[#"
        contains "col=v"
        contains "subColPath=[items, type]"
        notContains "element_at(CAST(element_at(element_at(v"
    }

    test {
        sql """
            SELECT
                CAST(element_at(parse_to_variant('[10,20,30]'), 1) AS INT),
                CAST(element_at(parse_to_variant('[10,20,30]'), 2) AS INT),
                CAST(element_at(parse_to_variant('[10,20,30]'), -1) AS INT),
                CAST(element_at(parse_to_variant('[10,20,30]'), -2) AS INT),
                element_at(parse_to_variant('[10,20,30]'), 0) IS NULL,
                element_at(parse_to_variant('[10,20,30]'), -4) IS NULL
        """
        result([[10, 20, 30, 20, true, true]])
    }

    // A present JSON null is a VARIANT value, not a missing path or outer SQL NULL.
    test {
        sql """
            SELECT
                element_at(parse_to_variant('[1,null,3]'), 2) IS NULL,
                variant_is_null(element_at(parse_to_variant('[1,null,3]'), 2))
        """
        result([[false, true]])
    }

    // Array conversion keeps positions. A bad element only becomes NULL at that position.
    test {
        sql """
            WITH converted AS (
                SELECT CAST(parse_to_variant('[1,"bad",3]') AS ARRAY<INT>) AS a
            )
            SELECT size(a), a[1], a[2] IS NULL, a[3] FROM converted
        """
        result([[3L, 1, true, 3]])
    }

    // Numeric elements keep their Variant numeric types.
    test {
        sql """
            SELECT
                variant_type(element_at(parse_to_variant('[1,1.5]'), 1)),
                variant_type(element_at(parse_to_variant('[1,1.5]'), 2))
        """
        result([["tinyint", "double"]])
    }

    // VARIANT null follows the requested array element type.
    test {
        sql """
            WITH converted AS (
                SELECT
                    CAST(parse_to_variant('[null]') AS ARRAY<INT>) AS ints,
                    CAST(parse_to_variant('[null]') AS ARRAY<STRING>) AS strings,
                    CAST(parse_to_variant('[null]') AS ARRAY<JSON>) AS json_values
            )
            SELECT
                ints[1] IS NULL,
                strings[1] IS NULL,
                strings[1],
                json_values[1] IS NULL,
                json_type(json_values[1], '\$')
            FROM converted
        """
        result([[true, false, "null", false, "null"]])
    }

    test {
        sql """
            SELECT
                CAST(element_at(CAST(parse_to_variant('[1]') AS ARRAY<VARIANT>), 1) AS STRING),
                variant_type(element_at(CAST(parse_to_variant('[1]') AS ARRAY<VARIANT>), 1))
        """
        result([["1", "tinyint"]])
    }
}
