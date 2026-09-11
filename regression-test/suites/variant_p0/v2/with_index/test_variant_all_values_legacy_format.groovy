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

suite("test_variant_all_values_legacy_format", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "SET default_variant_enable_doc_mode = false"
        sql "SET enable_match_without_inverted_index = true"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET enable_condition_cache = false"
        sql "SET enable_inverted_index_query_cache = false"
        sql "SET inverted_index_skip_threshold = 0"
        sql "DROP TABLE IF EXISTS variant_all_values_legacy_format"
        sql """CREATE TABLE variant_all_values_legacy_format (
            id BIGINT NOT NULL,
            v VARIANT,
            INDEX text_idx(v) USING INVERTED PROPERTIES("parser"="english",
                "variant_index_mode"="all_values", "variant_root_format_version"="1")
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
                   "inverted_index_storage_format"="SNII")"""
        sql """INSERT INTO variant_all_values_legacy_format VALUES
            (1, parse_to_variant('{"a":[{"keyonly":"leaf"}]}')),
            (2, parse_to_variant('{"a":"different"}'))"""
        sql "SYNC"
        def checkpoint = "segment_iterator.inverted_index.filtered_rows"
        try {
            GetDebugPoint().enableDebugPointForAllBEs(checkpoint, [filtered_rows: "0"])
            order_qt_legacy_scan """SELECT id FROM variant_all_values_legacy_format
                WHERE v MATCH_ANY 'leaf' USING ANALYZER english ORDER BY id"""
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
        }
        sql "SET enable_match_without_inverted_index = false"
        test {
            sql "SELECT id FROM variant_all_values_legacy_format WHERE v MATCH_ANY 'leaf' USING ANALYZER english"
            exception "not support execute_match"
        }
    }
}
