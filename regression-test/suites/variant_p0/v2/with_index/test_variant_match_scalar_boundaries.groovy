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

suite("test_variant_match_scalar_boundaries", "p0,nonConcurrent") {
    sql "SET enable_match_without_inverted_index=true"
    sql "SET enable_sql_cache=false"
    sql "SET enable_query_cache=false"
    sql "SET enable_condition_cache=false"
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "SET default_variant_enable_doc_mode=false"
        sql "DROP TABLE IF EXISTS variant_match_scalar_boundaries"
        sql """CREATE TABLE variant_match_scalar_boundaries(id INT,v VARIANT<'a':ARRAY<STRING>>,
            INDEX idx(v) USING INVERTED PROPERTIES("parser"="english","support_phrase"="true"))
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1","inverted_index_storage_format"="SNII")"""
        sql """INSERT INTO variant_match_scalar_boundaries VALUES
            (1,parse_to_variant('{"a":["apache","doris"],"s":"apache doris"}')),
            (2,parse_to_variant('{"a":["apache doris"],"s":"other"}')),
            (3,parse_to_variant('{"a":["apache doris","other"],"s":"apache doris"}'))"""
        [false,true].each { enabled ->
            sql "SET enable_inverted_index_query=${enabled}"
            quickTest("path_phrase_${enabled}", "SELECT id FROM variant_match_scalar_boundaries WHERE v['s'] MATCH_PHRASE 'apache doris' ORDER BY id", true)
            quickTest("path_regexp_${enabled}", "SELECT id FROM variant_match_scalar_boundaries WHERE v['s'] MATCH_REGEXP 'apache.*' ORDER BY id", true)
            quickTest("array_phrase_${enabled}", "SELECT id FROM variant_match_scalar_boundaries WHERE v['a'] MATCH_PHRASE 'apache doris' ORDER BY id", true)
            quickTest("array_prefix_${enabled}", "SELECT id FROM variant_match_scalar_boundaries WHERE v['a'] MATCH_PHRASE_PREFIX 'apache dor' ORDER BY id", true)
        }
    }
    setFeConfigTemporary([enable_variant_v2: false]) {
        sql "DROP TABLE IF EXISTS variant_match_v1_scalar"
        sql """CREATE TABLE variant_match_v1_scalar(id INT,v VARIANT<PROPERTIES("variant_max_subcolumns_count"="0")>)
            DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES("replication_num"="1")"""
        sql """INSERT INTO variant_match_v1_scalar VALUES(1,parse_to_variant('{"s":"apache doris"}'))"""
        sql "SET enable_inverted_index_query=false"
        order_qt_v1_any "SELECT id FROM variant_match_v1_scalar WHERE v['s'] MATCH_ANY 'apache doris'"
        order_qt_v1_phrase "SELECT id FROM variant_match_v1_scalar WHERE v['s'] MATCH_PHRASE 'apache doris'"
    }
}
