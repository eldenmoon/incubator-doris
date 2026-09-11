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

import org.apache.doris.regression.action.ProfileAction

suite("test_variant_all_values_float_equality", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "SET default_variant_enable_doc_mode = false"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET enable_condition_cache = false"
        sql "SET enable_inverted_index_query_cache = false"
        sql "SET inverted_index_skip_threshold = 0"
        sql "DROP TABLE IF EXISTS variant_all_values_float_equality"
        sql """
            CREATE TABLE variant_all_values_float_equality (
                id BIGINT NOT NULL,
                v VARIANT<PROPERTIES("variant_max_subcolumns_count"="16")>,
                INDEX exact_idx(v) USING INVERTED PROPERTIES(
                    "parser"="none", "variant_index_mode"="all_values")
            ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
                       "inverted_index_storage_format"="SNII")
        """
        sql """INSERT INTO variant_all_values_float_equality VALUES
            (1, parse_to_variant('{"f":-0.0}')), (2, parse_to_variant('{"f":0.0}')), (3, parse_to_variant('{"f":1.0}')),
            (4, parse_to_variant('{"f":1.0}')), (5, parse_to_variant('{"f":1.234567890123456}')), (6, NULL)"""
        sql "SYNC"
        order_qt_physical_type "SELECT id, variant_type(v), variant_type(v['f']) FROM variant_all_values_float_equality ORDER BY id"
        sql "SET enable_profile = true"
        sql "SET profile_level = 2"
        def profileTag = "all_values_fixed_double_${System.nanoTime()}"
        sql """/* ${profileTag} */ SELECT id FROM variant_all_values_float_equality
            WHERE CAST(v['f'] AS DOUBLE) = 0.0 ORDER BY id"""
        logger.info(new ProfileAction(context).getProfileBySql(profileTag))
        sql "SET enable_profile = false"
        [zero: "= 0.0", negative_zero: "= -0.0", mixed: "IN (0.0, 1.0)"].each { name, comparison ->
            def statement = "SELECT id FROM variant_all_values_float_equality WHERE CAST(v['f'] AS DOUBLE) ${comparison} ORDER BY id"
            sql "SET enable_inverted_index_query = false"
            def scanned = sql(statement)
            sql "SET enable_inverted_index_query = true"
            def indexed = sql(statement)
            assertEquals(scanned, indexed, "fixed DOUBLE index/scan parity: ${name}")
            quickTest(name, statement, true)
        }
        def checkpoint = "segment_iterator.inverted_index.filtered_rows"
        try {
            // Values are typed and -0.0 folds into 0: the values index answers the equality with
            // one exact term (rows 1 and 2 are candidates, rows 3-6 are filtered) instead of
            // falling back to a scan.
            GetDebugPoint().enableDebugPointForAllBEs(checkpoint, [filtered_rows: "4"])
            order_qt_float_fallback """SELECT id FROM variant_all_values_float_equality
                WHERE CAST(v['f'] AS DOUBLE) = 0.0 ORDER BY id"""
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
        }
        sql "DROP TABLE IF EXISTS variant_root_float_equality"
        sql """
            CREATE TABLE variant_root_float_equality (
                id BIGINT NOT NULL,
                v VARIANT<PROPERTIES("variant_max_subcolumns_count"="16")>,
                INDEX exact_idx(v) USING INVERTED PROPERTIES(
                    "parser"="none", "variant_index_mode"="root")
            ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num"="1", "disable_auto_compaction"="true",
                       "inverted_index_storage_format"="SNII")
        """
        sql "INSERT INTO variant_root_float_equality SELECT * FROM variant_all_values_float_equality"
        sql "SYNC"
        try {
            GetDebugPoint().enableDebugPointForAllBEs(checkpoint, [filtered_rows: "4"])
            order_qt_root_zero """SELECT id FROM variant_root_float_equality
                WHERE CAST(v['f'] AS DOUBLE) = 0.0 ORDER BY id"""
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
        }
        ["= 0.0", "= -0.0", "IN (0, 1.0)"].each { comparison ->
            def statement = "SELECT id FROM variant_root_float_equality WHERE CAST(v['f'] AS DOUBLE) ${comparison} ORDER BY id"
            sql "SET enable_inverted_index_query = false"
            def scanned = sql(statement)
            sql "SET enable_inverted_index_query = true"
            assertEquals(scanned, sql(statement), "Root DOUBLE index/scan parity: ${comparison}")
        }
        order_qt_root_negative_zero """SELECT id FROM variant_root_float_equality
            WHERE CAST(v['f'] AS DOUBLE) = -0.0 ORDER BY id"""
        order_qt_root_mixed_in """SELECT id FROM variant_root_float_equality
            WHERE CAST(v['f'] AS DOUBLE) IN (0, 1.0) ORDER BY id"""
    }
}
