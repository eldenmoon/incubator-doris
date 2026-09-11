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

suite("test_variant_root_typed_values", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "SET enable_match_without_inverted_index = true"
        sql "SET default_variant_enable_doc_mode = false"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET enable_condition_cache = false"
        sql "SET enable_inverted_index_query_cache = false"
        sql "SET inverted_index_skip_threshold = 0"
        ["root", "all_values"].each { mode ->
            [false, true].each { sparse ->
                def table = "variant_typed_${mode}_${sparse}"
                sql "DROP TABLE IF EXISTS ${table}"
                sql """CREATE TABLE ${table} (
                    id INT,
                    v VARIANT<'a':INT, 'b':STRING, 'items':ARRAY<STRING>,
                        PROPERTIES("variant_enable_typed_paths_to_sparse"="${sparse}",
                                   "variant_max_subcolumns_count"="1")>,
                    INDEX exact_idx(v) USING INVERTED PROPERTIES(
                        "parser"="none", "variant_index_mode"="${mode}"),
                    INDEX text_idx(v) USING INVERTED PROPERTIES(
                        "parser"="english", "variant_index_mode"="${mode}")
                ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num"="1", "inverted_index_storage_format"="SNII",
                           "disable_auto_compaction"="true")"""
                sql """INSERT INTO ${table} VALUES
                    (1,parse_to_variant('{"a":"001","b":123,"items":[1,2]}')),
                    (2,parse_to_variant('{"a":"bad","b":"kept"}')),
                    (3,parse_to_variant('{"a":"002","b":456,"other":"leaf"}')),
                    (4,NULL)"""
                sql "SYNC"
                quickTest("${table}_values", "SELECT id,CAST(v AS STRING) FROM ${table} ORDER BY id", true)
                def queries = [integer: "CAST(v['a'] AS INT)=1",
                               string: "CAST(v['b'] AS STRING)='123'",
                               mixed_in: "CAST(v['a'] AS INT) IN (1,2)"]
                if (mode == "all_values") {
                    queries += [removed: "v MATCH_ANY 'bad' USING ANALYZER english",
                                array_leaf: "v MATCH_ANY '2' USING ANALYZER english"]
                }
                queries.each { name, predicate ->
                    def query = "SELECT id FROM ${table} WHERE ${predicate} ORDER BY id"
                    sql "SET enable_inverted_index_query=false"
                    def scan = sql(query)
                    quickTest("${table}_${name}_scan", query, true)
                    sql "SET enable_inverted_index_query=true"
                    assertEquals(scan, sql(query), "${table} ${name} index/scan parity")
                    quickTest("${table}_${name}_index", query, true)
                    quickTest("${table}_${name}_count", "SELECT COUNT(*) FROM ${table} WHERE ${predicate}", false)
                }
                // Materialized typed INT selects Root exact; AllValues whole-root MATCH is exact.
                if (!sparse || mode == "all_values") {
                    def predicate = mode == "root" ? "CAST(v['a'] AS INT)=1" : "v MATCH_ANY 'bad' USING ANALYZER english"
                    def filtered = mode == "root" ? "3" : "4"
                    try {
                        GetDebugPoint().enableDebugPointForAllBEs(
                            "segment_iterator.inverted_index.filtered_rows", [filtered_rows: filtered])
                        quickTest("${table}_postings", "SELECT id FROM ${table} WHERE ${predicate} ORDER BY id", true)
                    } finally {
                        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.inverted_index.filtered_rows")
                    }
                }
            }
        }
    }
}
