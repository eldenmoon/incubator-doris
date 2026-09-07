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

suite("test_variant_root_typed_doc", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "SET enable_match_without_inverted_index=true"
        sql "SET enable_sql_cache=false"
        sql "SET enable_query_cache=false"
        sql "SET enable_condition_cache=false"
        sql "SET enable_inverted_index_query_cache=false"
        sql "SET inverted_index_skip_threshold=0"
        ["root", "all_values"].each { mode ->
            [0, 1000].each { minRows ->
                def table = "variant_typed_doc_${mode}_${minRows}"
                sql "DROP TABLE IF EXISTS ${table}"
                sql """CREATE TABLE ${table}(id INT,
                    v VARIANT<'a':INT, PROPERTIES("variant_enable_doc_mode"="true",
                        "variant_doc_materialization_min_rows"="${minRows}")>,
                    INDEX idx(v) USING INVERTED PROPERTIES("parser"="none", "variant_index_mode"="${mode}"))
                    DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
                    PROPERTIES("replication_num"="1", "inverted_index_storage_format"="SNII",
                        "disable_auto_compaction"="true")"""
                sql """INSERT INTO ${table} VALUES
                    (1,parse_to_variant('{"a":"001"}')),
                    (2,parse_to_variant('{"a":"bad"}'))"""
                quickTest("${table}_values", "SELECT id,CAST(v AS STRING),CAST(v['a'] AS INT) FROM ${table} ORDER BY id", true)
                [false, true].each { enabled ->
                    sql "SET enable_inverted_index_query=${enabled}"
                    quickTest("${table}_eq_${enabled}", "SELECT id FROM ${table} WHERE CAST(v['a'] AS INT)=1 ORDER BY id", true)
                    if (mode == "all_values") {
                        quickTest("${table}_root_${enabled}", "SELECT id FROM ${table} WHERE v MATCH_ANY 'bad' ORDER BY id", true)
                    }
                }
                def checkpoint = "segment_iterator.inverted_index.filtered_rows"
                try {
                    // Typed doc paths may be converted while whole-root doc values stay raw.
                    GetDebugPoint().enableDebugPointForAllBEs(checkpoint, [filtered_rows: "0"])
                    quickTest("${table}_fallback", "SELECT id FROM ${table} WHERE CAST(v['a'] AS INT)=1 ORDER BY id", true)
                } finally {
                    GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
                }
                if (mode == "all_values") {
                    try {
                        GetDebugPoint().enableDebugPointForAllBEs(checkpoint, [filtered_rows: "1"])
                        quickTest("${table}_root_postings", "SELECT id FROM ${table} WHERE v MATCH_ANY 'bad' ORDER BY id", true)
                    } finally {
                        GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
                    }
                }
            }
        }
    }
}
