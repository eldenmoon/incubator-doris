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

suite("test_variant_equality_contexts") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    sql "DROP TABLE IF EXISTS variant_canonical_hash_left"
    sql "DROP TABLE IF EXISTS variant_canonical_hash_right"

    sql """
        CREATE TABLE variant_canonical_hash_left (
            k INT NOT NULL,
            v VARIANT<PROPERTIES("variant_max_subcolumns_count" = "0")> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE variant_canonical_hash_right (
            k INT NOT NULL,
            v VARIANT<PROPERTIES("variant_max_subcolumns_count" = "0")> NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        PROPERTIES("replication_num" = "1")
    """

    // The two tables deliberately build different metadata dictionaries. The matching logical
    // values use different object field order and numeric physical types.
    sql """
        INSERT INTO variant_canonical_hash_left
        SELECT 1, parse_to_variant('{"a":1,"b":2}')
        UNION ALL SELECT 2, CAST(CAST(1 AS TINYINT)
            AS VARIANT<PROPERTIES("variant_max_subcolumns_count" = "0")>)
        UNION ALL SELECT 3, CAST(CAST(1 AS INT)
            AS VARIANT<PROPERTIES("variant_max_subcolumns_count" = "0")>)
        UNION ALL SELECT 4, parse_to_variant('[1,2]')
        UNION ALL SELECT 5, parse_to_variant('null')
        UNION ALL SELECT 6, CAST(NULL
            AS VARIANT<PROPERTIES("variant_max_subcolumns_count" = "0")>)
        UNION ALL SELECT 7, parse_to_variant('{"id":7,"side":"left"}')
        UNION ALL SELECT 8, parse_to_variant('{"z":0}')
    """
    sql """
        INSERT INTO variant_canonical_hash_right
        SELECT 11, parse_to_variant('{"b":2,"a":1}')
        UNION ALL SELECT 12, CAST(CAST(1.0 AS DOUBLE)
            AS VARIANT<PROPERTIES("variant_max_subcolumns_count" = "0")>)
        UNION ALL SELECT 14, parse_to_variant('[2,1]')
        UNION ALL SELECT 15, parse_to_variant('null')
        UNION ALL SELECT 16, CAST(NULL
            AS VARIANT<PROPERTIES("variant_max_subcolumns_count" = "0")>)
        UNION ALL SELECT 17, parse_to_variant('{"other":"right","id":7}')
        UNION ALL SELECT 18, parse_to_variant('{"aa":0}')
    """

    explain {
        sql """
            SELECT v, COUNT(*)
            FROM (SELECT CAST(CAST(number % 2 AS STRING) AS VARIANT) v
                  FROM numbers("number" = "4")) t
            GROUP BY v
        """
        contains "AGGREGATE"
    }

    explain {
        sql """
            SELECT DISTINCT v
            FROM (SELECT CAST(CAST(number % 2 AS STRING) AS VARIANT) v
                  FROM numbers("number" = "4")) t
        """
        contains "AGGREGATE"
    }

    explain {
        sql """
            SELECT COUNT(DISTINCT v)
            FROM (SELECT CAST(CAST(number % 2 AS STRING) AS VARIANT) v
                  FROM numbers("number" = "4")) t
        """
        contains "COUNT(DISTINCT v)"
    }

    explain {
        sql """
            SELECT CAST(CAST(number AS STRING) AS VARIANT) v
            FROM numbers("number" = "2")
            INTERSECT
            SELECT CAST(CAST(number AS STRING) AS VARIANT) v
            FROM numbers("number" = "2")
        """
        contains "INTERSECT"
    }

    explain {
        sql """
            SELECT CAST(CAST(number AS STRING) AS VARIANT) v
            FROM numbers("number" = "2")
            EXCEPT
            SELECT CAST(CAST(number AS STRING) AS VARIANT) v
            FROM numbers("number" = "2")
        """
        contains "EXCEPT"
    }

    // TYPE_VARIANT intentionally selects the serialized canonical-key path. The original
    // fixed-key/root-join matrix is not a supported Variant path after root comparison and join
    // were rejected; extracting and casting a scalar path exercises the supported fixed-key join.
    explain {
        sql """
            SELECT /*+ SET_VAR(enable_local_exchange=false) */ v, COUNT(*)
            FROM variant_canonical_hash_left
            GROUP BY v
        """
        contains "HASH_PARTITIONED"
    }

    test {
        sql "SELECT parse_to_variant('1') = parse_to_variant('1.0')"
        exception "CAST to a concrete type first"
    }

    test {
        sql "SELECT parse_to_variant('1') != parse_to_variant('1.0')"
        exception "CAST to a concrete type first"
    }

    test {
        sql "SELECT parse_to_variant('1') <=> parse_to_variant('1.0')"
        exception "CAST to a concrete type first"
    }

    test {
        sql """
            SELECT *
            FROM (SELECT CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) a
            JOIN (SELECT CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) b
            ON a.v = b.v
        """
        exception "CAST to a concrete type first"
    }

    test {
        sql """
            SELECT *
            FROM (SELECT CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) a
            JOIN (SELECT CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) b
            ON a.v <=> b.v
        """
        exception "CAST to a concrete type first"
    }

    order_qt_group_by """
        SELECT CAST(v AS STRING), COUNT(*)
        FROM (SELECT parse_to_variant(CAST(number % 2 AS STRING)) v
              FROM numbers("number" = "4")) t
        GROUP BY v
        ORDER BY 1
    """

    order_qt_group_by_sql_null_semantics """
        SELECT v IS NULL, CAST(v AS STRING), COUNT(*)
        FROM (
            SELECT CAST(IF(number = 0, NULL, (number - 1) % 2) AS VARIANT) v
            FROM numbers("number" = "6")
        ) t
        GROUP BY v
        ORDER BY 1 DESC, 2
    """

    order_qt_group_by_variant_null_semantics """
        SELECT v IS NULL, CAST(v AS STRING), COUNT(*)
        FROM (
            SELECT parse_to_variant(
                       IF(number = 0, 'null', CAST((number - 1) % 2 AS STRING))) v
            FROM numbers("number" = "6")
        ) t
        GROUP BY v
        ORDER BY 1 DESC, 2
    """

    qt_count_distinct_canonical """
        SELECT COUNT(DISTINCT v)
        FROM (
            SELECT parse_to_variant('{"b":2,"a":1}') v
            UNION ALL
            SELECT parse_to_variant('{"a":1,"b":2}')
            UNION ALL
            SELECT parse_to_variant('[1,2]')
            UNION ALL
            SELECT parse_to_variant('[2,1]')
            UNION ALL
            SELECT parse_to_variant('1')
            UNION ALL
            SELECT parse_to_variant('1.0')
        ) t
    """

    order_qt_intersect_encoded_canonical_numeric """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT parse_to_variant(CAST(number AS STRING)) v
            FROM numbers("number" = "4")
            INTERSECT
            SELECT parse_to_variant(CONCAT(CAST(number AS STRING), '.0')) v
            FROM numbers("number" = "3")
        ) t
        ORDER BY 1
    """

    order_qt_except_encoded_array_order """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT parse_to_variant(IF(number = 0, '[1,2]', '[2,1]')) v
            FROM numbers("number" = "2")
            EXCEPT
            SELECT parse_to_variant('[1,2]') v
        ) t
        ORDER BY 1
    """

    order_qt_intersect_typed_fast_path """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT CAST(number AS VARIANT) v
            FROM numbers("number" = "4")
            INTERSECT
            SELECT CAST(number AS VARIANT) v
            FROM numbers("number" = "3")
        ) t
        ORDER BY 1
    """

    order_qt_union_distinct_canonical """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT parse_to_variant('{"b":2,"a":1}') v
            UNION DISTINCT
            SELECT parse_to_variant('{"a":1,"b":2}')
            UNION DISTINCT
            SELECT parse_to_variant('1')
            UNION DISTINCT
            SELECT parse_to_variant('1.0')
        ) t
        ORDER BY 1
    """

    order_qt_group_by_cross_metadata_shuffle """
        SELECT v IS NULL, CAST(v AS STRING), COUNT(*)
        FROM (
            SELECT v FROM variant_canonical_hash_left WHERE k BETWEEN 1 AND 6
            UNION ALL
            SELECT v FROM variant_canonical_hash_right WHERE k BETWEEN 11 AND 16
        ) t
        GROUP BY v
        ORDER BY 1 DESC, 2
    """

    qt_count_distinct_cross_metadata """
        SELECT COUNT(DISTINCT v)
        FROM (
            SELECT v FROM variant_canonical_hash_left WHERE k BETWEEN 1 AND 6
            UNION ALL
            SELECT v FROM variant_canonical_hash_right WHERE k BETWEEN 11 AND 16
        ) t
    """

    order_qt_intersect_cross_metadata """
        SELECT v IS NULL, CAST(v AS STRING)
        FROM (
            SELECT v FROM variant_canonical_hash_left WHERE k IN (1, 2, 3, 5, 6)
            INTERSECT
            SELECT v FROM variant_canonical_hash_right WHERE k IN (11, 12, 15, 16)
        ) t
        ORDER BY 1 DESC, 2
    """

    order_qt_except_cross_metadata_array_order """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT v FROM variant_canonical_hash_left WHERE k = 4
            EXCEPT
            SELECT v FROM variant_canonical_hash_right WHERE k = 14
        ) t
        ORDER BY 1
    """

    order_qt_union_distinct_cross_metadata """
        SELECT v IS NULL, CAST(v AS STRING)
        FROM (
            SELECT v FROM variant_canonical_hash_left WHERE k BETWEEN 1 AND 6
            UNION DISTINCT
            SELECT v FROM variant_canonical_hash_right WHERE k BETWEEN 11 AND 16
        ) t
        ORDER BY 1 DESC, 2
    """

    order_qt_explicit_path_cast_join """
        SELECT l.k, r.k
        FROM variant_canonical_hash_left l
        JOIN variant_canonical_hash_right r
          ON CAST(l.v['id'] AS INT) = CAST(r.v['id'] AS INT)
        WHERE l.k = 7 AND r.k = 17
        ORDER BY 1, 2
    """

    test {
        sql """
            SELECT *
            FROM variant_canonical_hash_left l
            JOIN variant_canonical_hash_right r ON l.v = r.v
        """
        exception "CAST to a concrete type first"
    }

    test {
        sql "SELECT MAX(CAST('1' AS VARIANT))"
        exception "Doris hll, bitmap"
    }

    order_qt_explicit_cast_equality """
        SELECT CAST(v AS STRING) = CAST(v AS STRING)
        FROM (SELECT CAST(CAST(number AS STRING) AS VARIANT) v
              FROM numbers("number" = "2")) t
        ORDER BY 1
    """
}
