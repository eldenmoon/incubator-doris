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

suite("variant_load_parse_injection", "p0,variant_type,nonConcurrent") {
    // Keep unqualified CAST(... AS VARIANT) aligned with the explicitly typed target columns
    // when fuzzy mode randomizes the session's default Variant layout.
    sql "SET default_variant_enable_doc_mode = false"
    sql "SET default_variant_max_subcolumns_count = 2048"

    sql "DROP TABLE IF EXISTS variant_load_parse_injection"
    sql "DROP TABLE IF EXISTS variant_load_parse_source"
    sql "DROP TABLE IF EXISTS variant_load_parse_generated"

    sql """
        CREATE TABLE variant_load_parse_injection (
            id INT NOT NULL,
            source VARCHAR(20),
            v VARIANT<properties("variant_max_subcolumns_count" = "2048")>,
            INDEX idx_v_tag(v) USING INVERTED
                PROPERTIES("parser" = "unicode")
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE variant_load_parse_source (
            id INT NOT NULL,
            payload STRING
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql """
        CREATE TABLE variant_load_parse_generated (
            id INT NOT NULL,
            payload STRING,
            v VARIANT<properties("variant_max_subcolumns_count" = "2048")>
                GENERATED ALWAYS AS (parse_to_variant(payload))
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """

    // Normal INSERT does not parse JSON-looking strings. Structured values must call
    // parse_to_variant explicitly.
    sql """
        INSERT INTO variant_load_parse_injection VALUES
            (1, 'literal', '{"a":1}'),
            (2, 'explicit', CAST('{"a":2}' AS VARIANT)),
            (3, 'function', concat('{"a":', '3}')),
            (4, 'numeric', 4),
            (5, 'null', NULL),
            (6, 'manual_parse', parse_to_variant('{"a":6}'))
    """
    sql """
        INSERT INTO variant_load_parse_source VALUES
            (10, '{"a":10}'),
            (11, '{"a":11}')
    """
    sql """
        INSERT INTO variant_load_parse_injection
        SELECT id, 'select', payload FROM variant_load_parse_source
    """
    sql """
        INSERT INTO variant_load_parse_injection
        SELECT 12, 'select_parse', parse_to_variant(payload)
        FROM variant_load_parse_source
        WHERE id = 10
    """
    sql """
        INSERT INTO variant_load_parse_generated(id, payload) VALUES
            (20, '{"a":20}'),
            (21, '{"a":21}')
    """

    def checkStreamLoad = { result, exception, startTime, endTime ->
        if (exception != null) {
            throw exception
        }
        def json = parseJson(result)
        assertEquals("Success", json.Status)
        assertEquals(1, json.NumberTotalRows)
        assertEquals(1, json.NumberLoadedRows)
        assertEquals(0, json.NumberFilteredRows)
    }
    streamLoad {
        table "variant_load_parse_injection"
        set "format", "csv"
        set "column_separator", "|"
        set "columns", "id,source,v"
        inputStream new ByteArrayInputStream(
                '60|stream_direct|{"a":60,"tag":"direct"}\n'.getBytes("UTF-8"))
        time 10000
        check checkStreamLoad
    }
    streamLoad {
        table "variant_load_parse_injection"
        set "format", "csv"
        set "column_separator", "|"
        set "columns", "id,source,raw_v,v=raw_v"
        inputStream new ByteArrayInputStream(
                '61|stream_mapping|{"a":61,"tag":"mapping"}\n'.getBytes("UTF-8"))
        time 10000
        check checkStreamLoad
    }

    // Malformed text is still a legal VARIANT string through CAST. Only explicit parsing fails.
    sql """
        INSERT INTO variant_load_parse_injection VALUES
            (40, 'invalid_string', '{')
    """
    test {
        sql """
            INSERT INTO variant_load_parse_injection VALUES
                (41, 'invalid_parse', parse_to_variant('{'))
        """
        exception "Parse json document failed at row 0, error:"
    }

    test {
        sql """
            SELECT id, source, variant_type(v), CAST(v['a'] AS INT), v IS NULL
            FROM variant_load_parse_injection
            ORDER BY id
        """
        result([
                [1, "literal", "string", null, false],
                [2, "explicit", "string", null, false],
                [3, "function", "string", null, false],
                [4, "numeric", "tinyint", null, false],
                [5, "null", null, null, true],
                [6, "manual_parse", "object", 6, false],
                [10, "select", "string", null, false],
                [11, "select", "string", null, false],
                [12, "select_parse", "object", 10, false],
                [40, "invalid_string", "string", null, false],
                [60, "stream_direct", "object", 60, false],
                [61, "stream_mapping", "object", 61, false]
        ])
    }
    test {
        sql """
            SELECT id, variant_type(v), CAST(v['a'] AS INT)
            FROM variant_load_parse_generated
            ORDER BY id
        """
        result([
                [20, "object", 20],
                [21, "object", 21]
        ])
    }

    sql "SET enable_match_without_inverted_index = false"
    sql "SET inverted_index_skip_threshold = 0"
    test {
        sql """
            SELECT id
            FROM variant_load_parse_injection
            WHERE v['tag'] MATCH 'direct'
            ORDER BY id
        """
        result([[60]])
    }
    sql "SET enable_match_without_inverted_index = true"
}
