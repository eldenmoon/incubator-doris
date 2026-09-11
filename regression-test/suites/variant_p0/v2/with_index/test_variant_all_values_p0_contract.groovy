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

suite("test_variant_all_values_p0_contract", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "SET default_variant_enable_doc_mode = false"
        sql "SET enable_match_without_inverted_index = true"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET enable_condition_cache = false"
        sql "SET enable_inverted_index_query_cache = false"
        sql "SET inverted_index_skip_threshold = 0"
        qt_the_analyzer """SELECT TOKENIZE('the The THE', '"parser"="english"')"""
        def logicalBaseline
        // The second layout keeps items and s sparse; the logical input stays identical.
        [variant_all_values_p0_contract: 16, variant_all_values_p0_sparse: 1].each {
            tableName, maxSubcolumns ->
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE ${tableName} (
                    id BIGINT NOT NULL,
                    payload VARIANT<PROPERTIES("variant_max_subcolumns_count" = "${maxSubcolumns}")> NULL,
                    INDEX exact_idx(payload) USING INVERTED PROPERTIES(
                        "parser" = "none", "ignore_above" = "3",
                        "variant_index_mode" = "all_values"),
                    INDEX text_idx(payload) USING INVERTED PROPERTIES(
                        "parser" = "english", "variant_index_mode" = "all_values")
                ) DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true",
                           "inverted_index_storage_format" = "SNII")
            """
            sql """
                INSERT INTO ${tableName} VALUES
                (1, parse_to_variant('{"items":[{"secretkey":"leafvalue"}],"s":"abc","n":1234,"article":"the The"}')),
                (2, parse_to_variant('{"items":["leafvalue"],"s":"abcd","n":1234}')),
                (3, parse_to_variant('{"secretkey":"different","s":"abc","n":7,"":"emptyneedle","a.b":"dotneedle","c":{"b":"nestedneedle"},"quote\\\\u0022key":"escapedneedle"}')),
                (4, parse_to_variant('{"items":[],"blank":""}')),
                (5, parse_to_variant('{}')),
                (6, parse_to_variant('null')),
                (7, NULL),
                (8, parse_to_variant('{"f":-0.0}')),
                (9, parse_to_variant('{"f":0.0}')),
                (10, parse_to_variant('{"f":1}')),
                (11, parse_to_variant('{"f":1.0}')),
                (12, parse_to_variant('{"n":9007199254740993}'))
            """
            sql "SYNC"
            def logicalStatement = "SELECT id, CAST(payload AS STRING) FROM ${tableName} ORDER BY id"
            def logicalRows = sql(logicalStatement)
            if (logicalBaseline == null) {
                logicalBaseline = logicalRows
            } else {
                assertEquals(logicalBaseline, logicalRows, "materialized/sparse logical row parity")
            }
            quickTest("${tableName}_logical_rows", logicalStatement, true)

            // Check real postings participation for the whole-root query; independent writer and
            // scanner unit tests specify the leaf-only results, including exclusion of object keys.
            def checkpoint = "segment_iterator.inverted_index.filtered_rows"
            try {
                GetDebugPoint().enableDebugPointForAllBEs(checkpoint, [filtered_rows: "10"])
                quickTest("${tableName}_root_leaf", """
                    SELECT id FROM ${tableName}
                    WHERE payload MATCH_ANY 'leafvalue' USING ANALYZER english ORDER BY id
                """, true)
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
            }

            def predicates = [
                the_root: "payload MATCH_ANY 'the' USING ANALYZER english",
                the_path: "payload['article'] MATCH_ANY 'the' USING ANALYZER english",
                empty_key: "payload[''] MATCH_ANY 'emptyneedle' USING ANALYZER english",
                dotted_key: "payload['a.b'] MATCH_ANY 'dotneedle' USING ANALYZER english",
                nested_key: "payload['c']['b'] MATCH_ANY 'nestedneedle' USING ANALYZER english",
                dotted_isolation: "payload['c']['b'] MATCH_ANY 'dotneedle' USING ANALYZER english",
                escaped_key: """payload['quote"key'] MATCH_ANY 'escapedneedle' USING ANALYZER english""",
                path_collision: "payload['s'] MATCH_ANY 'leafvalue' USING ANALYZER english",
                path_or: "payload['s'] MATCH_ANY 'leafvalue' USING ANALYZER english OR id = 3",
                path_not: "NOT (payload['s'] MATCH_ANY 'leafvalue' USING ANALYZER english)",
                cross_field_all: "payload MATCH_ALL 'abc leafvalue' USING ANALYZER english",
                root_and: "payload MATCH_ANY 'leafvalue' USING ANALYZER english AND CAST(payload['s'] AS STRING) = 'abc'",
                sql_null: "payload IS NULL",
                missing_null: "payload['missing'] IS NULL",
                root_empty_exact: "payload MATCH_ANY '' USING ANALYZER none",
                root_empty_tokens: "payload MATCH_ANY '' USING ANALYZER english",
                root_key: "payload MATCH_ANY 'secretkey' USING ANALYZER english",
                cast_array_key: "CAST(payload['items'] AS STRING) MATCH_ANY 'secretkey' USING ANALYZER english",
                short_eq: "CAST(payload['s'] AS STRING) = 'abc'",
                long_eq: "CAST(payload['s'] AS STRING) = 'abcd'",
                mixed_in: "CAST(payload['s'] AS STRING) IN ('abc', 'abcd')",
                numeric_in: "CAST(payload['n'] AS BIGINT) IN (7, 1234)",
                numeric_ignore_above: "CAST(payload['n'] AS BIGINT) = 1234",
                integer_precision_boundary: "CAST(payload['n'] AS BIGINT) = 9007199254740993",
                root_integral_text: "payload MATCH_ANY '1' USING ANALYZER none",
                root_float_text: "payload MATCH_ANY '1.0' USING ANALYZER none",
                positive_zero: "CAST(payload['f'] AS DOUBLE) = 0.0",
                negative_zero: "CAST(payload['f'] AS DOUBLE) = -0.0",
                mixed_number: "CAST(payload['f'] AS DOUBLE) = 1.0",
                not_in: "CAST(payload['s'] AS STRING) NOT IN ('abc', 'abcd')",
                not_equal: "CAST(payload['s'] AS STRING) != 'abc'",
                null_in: "CAST(payload['s'] AS STRING) IN ('abc', NULL)",
                root_not: "NOT (payload MATCH_ANY 'leafvalue' USING ANALYZER english)",
                root_or: "payload MATCH_ANY 'leafvalue' USING ANALYZER english OR id = 7"
            ]
            predicates.each { name, predicate ->
                def statement = "SELECT id FROM ${tableName} WHERE ${predicate} ORDER BY id"
                sql "SET enable_inverted_index_query = false"
                def scanned = sql(statement)
                sql "SET enable_inverted_index_query = true"
                def indexed
                def mustFallBack = name in ['long_eq', 'mixed_in', 'numeric_in', 'numeric_ignore_above']
                try {
                    if (mustFallBack) {
                        GetDebugPoint().enableDebugPointForAllBEs(checkpoint, [filtered_rows: "0"])
                    }
                    indexed = sql(statement)
                } finally {
                    if (mustFallBack) {
                        GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
                    }
                }
                assertEquals(scanned, indexed, "index/scan parity: ${name}")
                def countStatement = "SELECT COUNT(*) FROM ${tableName} WHERE ${predicate}"
                def indexedCount = sql(countStatement)[0][0] as long
                assertEquals(scanned.size() as long, indexedCount, "COUNT/scan parity: ${name}")
                quickTest("${tableName}_${name}", statement, true)
                quickTest("${tableName}_${name}_count", countStatement)
            }

            sql "SET enable_match_without_inverted_index = false"
            // Sparse paths use the native VARIANT MATCH binding; an explicit STRING cast
            // on that physical VARIANT reader retains scan evaluation.
            def matchSource = maxSubcolumns == 1 ? "payload['s']" : "CAST(payload['s'] AS STRING)"
            try {
                GetDebugPoint().enableDebugPointForAllBEs(checkpoint, [filtered_rows: "10"])
                quickTest("${tableName}_indexed_path_recheck", """
                    SELECT id FROM ${tableName}
                    WHERE ${matchSource} MATCH_ANY 'abc' USING ANALYZER english ORDER BY id
                """, true)
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
            }
            sql "SET enable_inverted_index_query = false"
            test {
                sql """SELECT id FROM ${tableName}
                       WHERE CAST(payload['s'] AS STRING) MATCH_ANY 'abc' USING ANALYZER english"""
                exception "not support execute_match"
            }
            sql "SET enable_inverted_index_query = true"
            sql "SET enable_match_without_inverted_index = true"


        }

        // A selected candidate index still needs its residual when full-scan MATCH is disabled.
        sql "SET enable_match_without_inverted_index = false"
        order_qt_indexed_path_recheck """
            SELECT id FROM variant_all_values_p0_contract
            WHERE payload['s'] MATCH_ANY 'abc' USING ANALYZER english ORDER BY id
        """
    }
}
