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

suite("test_variant_all_values_mow_lifecycle", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "SET default_variant_enable_doc_mode = false"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET enable_condition_cache = false"
        sql "SET enable_inverted_index_query_cache = false"
        sql "SET inverted_index_skip_threshold = 0"
        sql "DROP TABLE IF EXISTS variant_all_values_mow_lifecycle"
        sql """CREATE TABLE variant_all_values_mow_lifecycle (
            id BIGINT NOT NULL,
            v VARIANT<PROPERTIES("variant_max_subcolumns_count"="1")>,
            INDEX text_idx(v) USING INVERTED PROPERTIES(
                "parser"="english", "variant_index_mode"="all_values")
        ) UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                   "disable_auto_compaction"="true", "inverted_index_storage_format"="SNII")"""
        sql """INSERT INTO variant_all_values_mow_lifecycle VALUES
            (1, parse_to_variant('{"k":"old"}')), (2, parse_to_variant('{"k":"keep"}')), (3, NULL)"""
        sql """INSERT INTO variant_all_values_mow_lifecycle VALUES
            (1, parse_to_variant('{"k":"new"}')), (4, parse_to_variant('{"k":[{"keyonly":"leaf"}]}'))"""
        sql "DELETE FROM variant_all_values_mow_lifecycle WHERE id = 2"
        sql "SYNC"
        def verify = { stage ->
            ['old', 'keep', 'new', 'leaf', 'keyonly'].each { term ->
                def statement = "SELECT id FROM variant_all_values_mow_lifecycle WHERE v MATCH_ANY '${term}' USING ANALYZER english ORDER BY id"
                sql "SET enable_inverted_index_query = false"
                sql "SET enable_match_without_inverted_index = true"
                def scanned = sql(statement)
                sql "SET enable_inverted_index_query = true"
                // Whole-root MATCH has an exact index; disabling scan requires its participation.
                sql "SET enable_match_without_inverted_index = false"
                assertEquals(scanned, sql(statement), "MOW parity ${stage}: ${term}")
                quickTest("${stage}_${term}", statement, true)
                def countStatement = "SELECT COUNT(*) FROM variant_all_values_mow_lifecycle WHERE v MATCH_ANY '${term}' USING ANALYZER english"
                assertEquals(scanned.size() as long, sql(countStatement)[0][0] as long)
                quickTest("${stage}_${term}_count", countStatement)
            }
            quickTest("${stage}_rows", "SELECT id, CAST(v AS STRING) FROM variant_all_values_mow_lifecycle ORDER BY id", true)
        }
        verify('before_compaction')
        trigger_and_wait_compaction('variant_all_values_mow_lifecycle', 'full', 1800)
        verify('after_compaction')
    }
}
