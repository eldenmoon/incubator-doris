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

suite("test_variant_values_minimal", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "SET enable_match_without_inverted_index = true"
        // Every on/off pair must execute, rather than reuse a cached SQL or predicate result.
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET enable_condition_cache = false"
        sql "SET enable_inverted_index_query_cache = false"
        sql "DROP TABLE IF EXISTS variant_values_minimal"
        sql "DROP TABLE IF EXISTS variant_values_rejected"
        sql """CREATE TABLE variant_values_minimal (
            id BIGINT NOT NULL,
            v VARIANT<PROPERTIES("variant_max_subcolumns_count"="2")> NULL,
            INDEX idx(v) USING INVERTED PROPERTIES(
                "variant_index_scope"="values", "parser"="english")
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
                   "inverted_index_storage_format"="SNII")"""
        sql """INSERT INTO variant_values_minimal VALUES
            (1, parse_to_variant('{"a":[{"b":"hello world"}],"code":42,"ok":true}')),
            (2, parse_to_variant('["hello",["world"],null]')),
            (3, parse_to_variant('{"hello":"other","wrong":42}')),
            (4, parse_to_variant('{}')),
            (5, parse_to_variant('[]')),
            (6, parse_to_variant('{"a":null}')),
            (7, NULL),
            (8, parse_to_variant('{"a":"HELLO","b":"world","x":0.1}')),
            (9, parse_to_variant('{"a":"control\\\\u0001hello world"}')),
            (10, parse_to_variant('{"a":"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx hello world"}'))"""
        sql "SYNC"
        def queries = [
            "SELECT id FROM variant_values_minimal WHERE v MATCH 'hello world' ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE v MATCH_ANY 'hello world' ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE v MATCH_ALL 'hello world' ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE v MATCH_ANY '42 true' ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE NOT(v MATCH_ANY 'hello') ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE (v MATCH_ANY 'hello') OR id=7 ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE (v MATCH_ALL 'hello world') AND id<>1 ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE v MATCH_ANY '' ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE v MATCH_ALL '' ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE v MATCH_ANY NULL ORDER BY id",
            "SELECT count(*) FROM variant_values_minimal WHERE v MATCH_ALL 'hello world'",
            "SELECT id, v MATCH_ANY 'hello' FROM variant_values_minimal ORDER BY id",
            "SELECT id, v MATCH_ANY NULL FROM variant_values_minimal ORDER BY id",
            "SELECT id, v MATCH_ANY '' FROM variant_values_minimal ORDER BY id",
            "SELECT id, v MATCH_ALL '' FROM variant_values_minimal ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE CAST(v['code'] AS BIGINT)=42 ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE CAST(v['code'] AS BIGINT) IN (42,999) ORDER BY id",
            "SELECT id FROM variant_values_minimal WHERE CAST(v['a'] AS STRING)='HELLO' ORDER BY id"
        ]
        queries.eachWithIndex { query, i ->
            sql "SET enable_inverted_index_query = true"
            quickTest("index_${i}", query, true)
            sql "SET enable_inverted_index_query = false"
            quickTest("scan_${i}", query, true)
        }
        sql "SET enable_inverted_index_query = true"
        def point = "segment_iterator.inverted_index.filtered_rows"
        try {
            GetDebugPoint().enableDebugPointForAllBEs(point, [filtered_rows: "5"])
            qt_participation "SELECT id FROM variant_values_minimal WHERE v MATCH_ALL 'hello world' ORDER BY id"
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(point)
        }
        for (String parser : ["none", "chinese"]) {
            test {
                sql """CREATE TABLE variant_values_rejected (
                    id BIGINT, v VARIANT,
                    INDEX idx(v) USING INVERTED PROPERTIES(
                        "variant_index_scope"="values", "parser"="${parser}")
                ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1", "inverted_index_storage_format"="SNII")"""
                exception "requires the built-in english parser"
            }
        }
        test {
            sql """CREATE TABLE variant_values_rejected (
                id BIGINT, v VARIANT,
                INDEX idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values", "parser"="english")
            ) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                       "inverted_index_storage_format"="SNII")"""
            exception "supports only DUP_KEYS"
        }
        test {
            sql "SELECT id FROM variant_values_minimal WHERE v MATCH_ANY 'hello' USING ANALYZER none"
            exception "supports only the english analyzer"
        }
        test {
            sql "SELECT id FROM variant_values_minimal WHERE search('v:hello')"
            exception "has no inverted index"
        }
        test {
            sql "SELECT id FROM variant_values_minimal WHERE search('v.a:hello')"
            exception "has no inverted index"
        }
        test {
            sql "SELECT id FROM variant_values_minimal WHERE v MATCH_PHRASE 'hello world'"
            exception "supports only MATCH"
        }
        test {
            sql """CREATE TABLE variant_values_rejected (
                id BIGINT, v VARIANT,
                INDEX one(v) USING INVERTED PROPERTIES("variant_index_scope"="values", "parser"="english"),
                INDEX two(v) USING INVERTED PROPERTIES("variant_index_scope"="values", "parser"="english")
            ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1", "inverted_index_storage_format"="SNII")"""
            exception "only one values index"
        }
        sql "DROP TABLE IF EXISTS variant_values_with_paths"
        sql """CREATE TABLE variant_values_with_paths (
            id BIGINT NOT NULL,
            v VARIANT<PROPERTIES("variant_max_subcolumns_count"="8")>,
            INDEX root_idx(v) USING INVERTED PROPERTIES("variant_index_scope"="values", "parser"="english"),
            INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="none")
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
                   "inverted_index_storage_format"="SNII")"""
        sql """INSERT INTO variant_values_with_paths VALUES
            (1, parse_to_variant('{"s":"hello"}')),
            (2, parse_to_variant('{"s":"other","t":"hello"}')),
            (3, parse_to_variant('{"s":"world","t":"hello"}'))"""
        sql "SYNC"
        order_qt_root_with_paths "SELECT id FROM variant_values_with_paths WHERE v MATCH_ANY 'hello' ORDER BY id"
        order_qt_search_path_index "SELECT id FROM variant_values_with_paths WHERE search('v.s:hello') ORDER BY id"
        order_qt_match_path_index "SELECT id FROM variant_values_with_paths WHERE v['s'] MATCH_ANY 'hello' ORDER BY id"
        sql "SET enable_inverted_index_query = false"
        order_qt_match_path_scan "SELECT id FROM variant_values_with_paths WHERE v['s'] MATCH_ANY 'hello' ORDER BY id"
        sql "SET enable_inverted_index_query = true"
        sql "DROP TABLE IF EXISTS variant_values_no_root"
        sql """CREATE TABLE variant_values_no_root (
            id BIGINT NOT NULL, v VARIANT,
            INDEX path_idx(v) USING INVERTED PROPERTIES("parser"="none")
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "inverted_index_storage_format"="SNII")"""
        sql """INSERT INTO variant_values_no_root VALUES (1, parse_to_variant('{"s":"hello"}'))"""
        test {
            sql "SELECT id FROM variant_values_no_root WHERE v MATCH_ANY 'hello'"
            exception "requires a values text index"
        }
        test {
            sql "BUILD INDEX idx ON variant_values_minimal"
            exception "BUILD INDEX"
        }
    }
}
