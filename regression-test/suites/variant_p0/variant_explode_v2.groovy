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

suite("variant_explode_v2", "p0,nonConcurrent") {
    sql "DROP TABLE IF EXISTS variant_explode_v2_data"
    sql """
        CREATE TABLE variant_explode_v2_data (
            id INT NOT NULL,
            v1 VARIANT NULL,
            v2 VARIANT NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        INSERT INTO variant_explode_v2_data VALUES
            (1, parse_to_variant('[{"a":1},null]'), parse_to_variant('[2]')),
            (2, parse_to_variant('42'), parse_to_variant('[3]')),
            (3, parse_to_variant('[]'), parse_to_variant('{}')),
            (4, NULL, parse_to_variant('[4]')),
            (5, parse_to_variant('{"b":null,"a":{"x":1}}'), NULL),
            (6, parse_to_variant('{}'), NULL)
    """

    order_qt_array_preserves_variant_null """
        SELECT id, CAST(element AS STRING), element IS NULL
        FROM variant_explode_v2_data
        LATERAL VIEW explode_variant_array(v1) exploded AS element
        WHERE id = 1
        ORDER BY id, CAST(element AS STRING)
    """

    order_qt_multi_array_missing_is_sql_null """
        SELECT
            CAST(left_element AS STRING),
            left_element IS NULL,
            CAST(right_element AS STRING),
            right_element IS NULL
        FROM variant_explode_v2_data
        LATERAL VIEW explode_variant_array(v1, v2) exploded AS left_element, right_element
        WHERE id = 1
        ORDER BY CAST(left_element AS STRING)
    """

    order_qt_outer_non_container_empty_and_sql_null """
        SELECT id, CAST(element AS STRING), element IS NULL
        FROM variant_explode_v2_data
        LATERAL VIEW explode_outer(v1) exploded AS element
        WHERE id IN (1, 2, 3, 4)
        ORDER BY id, CAST(element AS STRING)
    """

    order_qt_const_array """
        SELECT CAST(element AS STRING), element IS NULL
        FROM (SELECT 1 AS id) seed
        LATERAL VIEW explode_variant_array(parse_to_variant('[1,null]')) exploded AS element
        ORDER BY CAST(element AS STRING)
    """

    order_qt_variant_object_jsonb_values """
        SELECT id, object_key, CAST(object_value AS STRING), object_value IS NULL
        FROM variant_explode_v2_data
        LATERAL VIEW explode_variant_object(v1) exploded AS object_key, object_value
        WHERE id = 5
        ORDER BY id, object_key
    """

    order_qt_variant_object_outer """
        SELECT id, object_key, CAST(object_value AS STRING), object_value IS NULL
        FROM variant_explode_v2_data
        LATERAL VIEW explode_variant_object_outer(v1) exploded AS object_key, object_value
        WHERE id IN (2, 5, 6)
        ORDER BY id, object_key
    """
}
