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

suite("test_variant_root_index_correctness", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        def rootNone = "variant_root_index_none"
        def childNone = "variant_child_indexes_none"
        def rootEnglish = "variant_root_index_english"
        def childEnglish = "variant_child_indexes_english"
        def rootBoth = "variant_root_indexes_both"
        def childBoth = "variant_child_indexes_both"

        sql "SET default_variant_enable_doc_mode = false"
        sql "SET default_variant_max_subcolumns_count = 0"
        sql "SET default_variant_enable_typed_paths_to_sparse = false"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET enable_condition_cache = false"
        sql "SET enable_inverted_index_query_cache = false"
        sql "SET enable_match_without_inverted_index = true"
        sql "SET inverted_index_skip_threshold = 0"

        [rootNone, childNone, rootEnglish, childEnglish, rootBoth, childBoth].each { tableName ->
            sql "DROP TABLE IF EXISTS ${tableName}"
        }

        def createTable = { String tableName, boolean root, String parser ->
            def rootProperty = root ? ', "variant_index_mode" = "root"' : ''
            def supportPhraseProperty = root ? '' : ', "support_phrase" = "false"'
            def maxSubcolumns = root ? 0 : 10_000
            sql """
                CREATE TABLE ${tableName} (
                    id BIGINT NOT NULL,
                    v VARIANT<PROPERTIES(
                        "variant_max_subcolumns_count" = "${maxSubcolumns}")> NULL,
                    INDEX idx_v(v) USING INVERTED PROPERTIES(
                        "parser" = "${parser}"${supportPhraseProperty}${rootProperty})
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true",
                    "inverted_index_storage_format" = "SNII"
                )
            """
        }

        createTable(rootNone, true, "none")
        createTable(childNone, false, "none")
        createTable(rootEnglish, true, "english")
        createTable(childEnglish, false, "english")

        def createBothTable = { String tableName, boolean root ->
            def rootProperty = root ? ', "variant_index_mode" = "root"' : ''
            def supportPhraseProperty = root ? '' : ', "support_phrase" = "false"'
            def maxSubcolumns = root ? 0 : 10_000
            sql """
                CREATE TABLE ${tableName} (
                    id BIGINT NOT NULL,
                    v VARIANT<PROPERTIES(
                        "variant_max_subcolumns_count" = "${maxSubcolumns}")> NULL,
                    INDEX idx_v_exact(v) USING INVERTED PROPERTIES(
                        "parser" = "none"${supportPhraseProperty}${rootProperty}),
                    INDEX idx_v_token(v) USING INVERTED PROPERTIES(
                        "parser" = "english"${supportPhraseProperty}${rootProperty})
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true",
                    "inverted_index_storage_format" = "SNII"
                )
            """
        }
        createBothTable(rootBoth, true)
        createBothTable(childBoth, false)

        test {
            sql """
                CREATE TABLE variant_root_indexes_duplicate_analyzer (
                    id BIGINT NOT NULL,
                    v VARIANT NULL,
                    INDEX idx_v_exact_1(v) USING INVERTED PROPERTIES(
                        "parser" = "none", "variant_index_mode" = "root"),
                    INDEX idx_v_exact_2(v) USING INVERTED PROPERTIES(
                        "parser" = "none", "variant_index_mode" = "root")
                )
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "inverted_index_storage_format" = "SNII"
                )
            """
            exception "cannot have multiple inverted indexes of the same type"
        }

        test {
            sql """
                CREATE INDEX idx_root_late ON ${childNone}(v) USING INVERTED
                PROPERTIES("variant_index_mode" = "root", "parser" = "none")
            """
            exception "VARIANT root index can only be declared in CREATE TABLE"
        }
        test {
            sql "DROP INDEX idx_v ON ${rootNone}"
            exception "DROP INDEX is not supported for a VARIANT root index"
        }
        test {
            sql "BUILD INDEX idx_v ON ${rootNone}"
            exception "BUILD INDEX is not supported for a VARIANT root index"
        }

        def firstValues = """
            (1, parse_to_variant('{"action":"opened","repo":{"name":"apache/doris"},"number":42,"mixed":42,"numeric_mixed":42,"score":1.5,"active":true,"comment":{"body":"Root index makes variant search fast"},"labels":["bug","storage"],"nullable":null}')),
            (2, parse_to_variant('{"action":"closed","repo":{"name":"apache/doris"},"number":7,"mixed":"42","numeric_mixed":42.0,"score":0.0,"active":false,"comment":{"body":"Variant storage engine benchmark"},"labels":["performance"]}')),
            (3, parse_to_variant('{"action":"opened","repo":{"name":"other/project"},"number":42,"numeric_mixed":1.5,"score":3.25,"active":true,"comment":{"body":"Search the root document"},"labels":[]}')),
            (4, parse_to_variant('{"action":"reopened","repo":{"name":"apache/spark"},"number":-8,"numeric_mixed":-0.0,"score":-0.0,"active":false,"comment":{"body":"Distributed query engine"},"labels":["bug",null]}')),
            (5, parse_to_variant('{"repo":{"name":"apache/doris"},"number":900719925474099,"comment":{"body":null},"labels":["storage"]}'))
        """
        def secondValues = """
            (6, parse_to_variant('{}')),
            (7, parse_to_variant('null')),
            (8, NULL),
            (9, parse_to_variant('{"action":"opened","repo":{"name":"apache/doris"},"number":99,"numeric_boundary":9007199254740993,"score":1.5,"active":true,"comment":{"body":"Variant root search benchmark"},"labels":["correctness","storage"]}')),
            (10, parse_to_variant('{"numeric_boundary":9007199254740992}')),
            (11, parse_to_variant('{"numeric_boundary":9223372036854775807}')),
            (12, parse_to_variant('{"numeric_boundary":-9223372036854775808}'))
        """
        [rootNone, childNone, rootEnglish, childEnglish, rootBoth, childBoth].each { tableName ->
            sql "INSERT INTO ${tableName} VALUES ${firstValues}"
        }

        // A cast-wrapped VARIANT predicate keeps a scalar residual even after index application,
        // so the zero-residual debug point cannot prove index participation. Instead pin the
        // exact rows filtered by the root bitmap. The first five rows form one segment, avoiding
        // the scan-stat counter's intermediate values when several segments share one query.
        def assertRootIndexFilteredRows = { String query, int expectedFilteredRows ->
            def checkpoint = "segment_iterator.inverted_index.filtered_rows"
            try {
                GetDebugPoint().enableDebugPointForAllBEs(
                        checkpoint, [filtered_rows: "${expectedFilteredRows}"])
                sql "SET experimental_enable_parallel_scan = false"
                sql "SYNC"
                sql query
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
            }
        }
        assertRootIndexFilteredRows("""
                SELECT id FROM ${rootNone}
                WHERE CAST(v['action'] AS STRING) = 'opened'
                ORDER BY id
            """, 3)
        assertRootIndexFilteredRows("""
                SELECT id FROM ${rootNone}
                WHERE CAST(v['number'] AS BIGINT) IN (42, -8)
                ORDER BY id
            """, 2)
        assertRootIndexFilteredRows("""
                SELECT id FROM ${rootEnglish}
                WHERE CAST(v['comment']['body'] AS STRING) MATCH_ALL 'variant search'
                ORDER BY id
            """, 4)
        // Add a second source rowset and pin representative scan-oracle fingerprints before
        // compaction. The same fingerprints are checked again after compacting both layouts.
        [rootNone, childNone, rootEnglish, childEnglish, rootBoth, childBoth].each { tableName ->
            sql "INSERT INTO ${tableName} VALUES ${secondValues}"
        }
        def fingerprint = { String tableName, String predicate, boolean enableIndex ->
            def hint = enableIndex ? "" : "/*+ SET_VAR(enable_inverted_index_query=false) */"
            def result = sql """
                SELECT ${hint} COUNT(*), COALESCE(SUM(id), 0),
                       COALESCE(GROUP_BIT_XOR(id), 0)
                FROM ${tableName}
                WHERE ${predicate}
            """
            return result
        }
        def captureBeforeCompaction = { String rootTable, String childTable, String predicate ->
            def scanResult = fingerprint(childTable, predicate, false)
            assertEquals(scanResult, fingerprint(childTable, predicate, true),
                    "all-child index differs from scan before compaction: ${predicate}")
            assertEquals(scanResult, fingerprint(rootTable, predicate, true),
                    "root index differs from scan before compaction: ${predicate}")
            return scanResult
        }
        def noneStringPredicate = "CAST(v['action'] AS STRING) = 'opened'"
        def noneIntegerPredicate = "CAST(v['number'] AS BIGINT) IN (42, -8)"
        // Mixed integral/non-integral values infer a floating path. The current query
        // eligibility gate conservatively keeps an explicit numeric-family cast on the
        // residual scan, so verify its result against the scan oracle without asserting
        // bitmap participation. Codec unit tests separately pin the canonical root terms.
        def mixedNumericPredicate = "CAST(v['numeric_mixed'] AS DOUBLE) = 42.0"
        def boundaryIntegerPredicate = "CAST(v['numeric_boundary'] AS BIGINT) IN " +
                "(9007199254740993, 9223372036854775807, -9223372036854775808)"
        def boundaryDoublePredicate =
                "CAST(v['numeric_boundary'] AS DOUBLE) = 9007199254740992.0"
        def englishPredicate =
                "CAST(v['comment']['body'] AS STRING) MATCH_ALL 'variant search'"
        def bothPredicate = "CAST(v['repo']['name'] AS STRING) = 'apache/doris' " +
                "AND CAST(v['comment']['body'] AS STRING) MATCH_ANY 'root variant'"
        def noneStringBefore =
                captureBeforeCompaction(rootNone, childNone, noneStringPredicate)
        def noneIntegerBefore =
                captureBeforeCompaction(rootNone, childNone, noneIntegerPredicate)
        def mixedNumericBefore =
                captureBeforeCompaction(rootNone, childNone, mixedNumericPredicate)
        def boundaryIntegerBefore =
                captureBeforeCompaction(rootNone, childNone, boundaryIntegerPredicate)
        def boundaryDoubleBefore =
                captureBeforeCompaction(rootNone, childNone, boundaryDoublePredicate)
        def englishBefore =
                captureBeforeCompaction(rootEnglish, childEnglish, englishPredicate)
        def bothBefore = captureBeforeCompaction(rootBoth, childBoth, bothPredicate)

        [rootNone, childNone, rootEnglish, childEnglish, rootBoth, childBoth].each { tableName ->
            trigger_and_wait_compaction(tableName, "full", 1800)
        }
        def assertFullCompactionSucceeded = { String tableName ->
            def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
            tablets.each { tablet ->
                def (code, out, err) = curl("GET", tablet.CompactionStatus)
                assertEquals(0, code, "show compaction status failed for ${tablet.TabletId}: ${err}")
                def status = parseJson(out.trim())
                assertEquals("[OK]", status["last full status"],
                        "full compaction failed for ${tablet.TabletId}: ${out}")
                assertFalse(status["last full success time"].startsWith("1970-01-01"),
                        "full compaction did not produce output for ${tablet.TabletId}: ${out}")
            }
        }
        [rootNone, childNone, rootEnglish, childEnglish, rootBoth, childBoth].each {
            assertFullCompactionSucceeded(it)
        }
        def checkAfterCompaction = { String rootTable, String childTable, String predicate,
                                     def before ->
            def scanResult = fingerprint(childTable, predicate, false)
            assertEquals(before, scanResult,
                    "scan result changed after compaction: ${predicate}")
            assertEquals(scanResult, fingerprint(childTable, predicate, true),
                    "all-child index differs from scan after compaction: ${predicate}")
            assertEquals(scanResult, fingerprint(rootTable, predicate, true),
                    "root index differs from scan after compaction: ${predicate}")
        }
        checkAfterCompaction(rootNone, childNone, noneStringPredicate, noneStringBefore)
        checkAfterCompaction(rootNone, childNone, noneIntegerPredicate, noneIntegerBefore)
        checkAfterCompaction(rootNone, childNone, mixedNumericPredicate, mixedNumericBefore)
        checkAfterCompaction(rootNone, childNone, boundaryIntegerPredicate, boundaryIntegerBefore)
        checkAfterCompaction(rootNone, childNone, boundaryDoublePredicate, boundaryDoubleBefore)
        checkAfterCompaction(rootEnglish, childEnglish, englishPredicate, englishBefore)
        checkAfterCompaction(rootBoth, childBoth, bothPredicate, bothBefore)

        // Each result appears three times: the root index, the ordinary all-child index, and an
        // index-disabled scan oracle over the ordinary table. Count/sum/xor fingerprints fail the
        // suite immediately on a mismatch, while ordered rows keep the exact result in the golden.
        def compareSources = { String rootTable, String childTable, String predicate ->
            def rootResult = fingerprint(rootTable, predicate, true)
            def childResult = fingerprint(childTable, predicate, true)
            def scanResult = fingerprint(childTable, predicate, false)
            assertEquals(scanResult, childResult,
                    "all-child index differs from scan oracle for: ${predicate}")
            assertEquals(scanResult, rootResult,
                    "root index differs from scan oracle for: ${predicate}")
            return """
                SELECT source, id
                FROM (
                    SELECT 'root' AS source, id FROM ${rootTable} WHERE ${predicate}
                    UNION ALL
                    SELECT 'children' AS source, id FROM ${childTable} WHERE ${predicate}
                    UNION ALL
                    SELECT /*+ SET_VAR(enable_inverted_index_query=false) */
                           'scan' AS source, id FROM ${childTable} WHERE ${predicate}
                ) compared
                ORDER BY source, id
            """
        }

        order_qt_root_string_equal compareSources(rootNone, childNone,
                "CAST(v['action'] AS STRING) = 'opened'")
        order_qt_root_string_in compareSources(rootNone, childNone,
                "CAST(v['repo']['name'] AS STRING) IN ('apache/doris', 'apache/spark')")
        order_qt_root_integer_equal compareSources(rootNone, childNone,
                "CAST(v['number'] AS BIGINT) = 42")
        order_qt_root_integer_in compareSources(rootNone, childNone,
                "CAST(v['number'] AS BIGINT) IN (42, -8)")
        order_qt_root_double_equal compareSources(rootNone, childNone,
                "CAST(v['score'] AS DOUBLE) = 1.5")
        order_qt_root_boolean_equal compareSources(rootNone, childNone,
                "CAST(v['active'] AS BOOLEAN) = true")
        order_qt_root_numeric_as_double compareSources(rootNone, childNone,
                "CAST(v['number'] AS DOUBLE) = 42.0")
        order_qt_root_mixed_numeric_as_double compareSources(rootNone, childNone,
                mixedNumericPredicate)
        order_qt_root_numeric_boundary_as_integer compareSources(rootNone, childNone,
                boundaryIntegerPredicate)
        order_qt_root_numeric_boundary_as_double compareSources(rootNone, childNone,
                boundaryDoublePredicate)
        order_qt_root_numeric_as_string compareSources(rootNone, childNone,
                "CAST(v['number'] AS STRING) = '42'")
        order_qt_root_mixed_as_integer compareSources(rootNone, childNone,
                "CAST(v['mixed'] AS BIGINT) = 42")
        order_qt_root_mixed_as_string compareSources(rootNone, childNone,
                "CAST(v['mixed'] AS STRING) = '42'")
        // Unsupported root-index predicates must remain correct through scalar residuals.
        order_qt_root_numeric_range_fallback compareSources(rootNone, childNone,
                "CAST(v['number'] AS BIGINT) > 40")
        order_qt_root_array_fallback compareSources(rootNone, childNone,
                "ARRAY_CONTAINS(CAST(v['labels'] AS ARRAY<TEXT>), 'bug')")
        order_qt_root_json_and_sql_null_fallback compareSources(rootNone, childNone,
                "CAST(v['nullable'] AS STRING) IS NULL")
        order_qt_root_not_equal_fallback compareSources(rootNone, childNone,
                "CAST(v['action'] AS STRING) != 'opened'")
        order_qt_root_mixed_residual compareSources(rootNone, childNone,
                "CAST(v['repo']['name'] AS STRING) = 'apache/doris' "
                + "AND CAST(v['number'] AS BIGINT) > 40 "
                + "AND ARRAY_CONTAINS(CAST(v['labels'] AS ARRAY<TEXT>), 'storage')")
        order_qt_root_candidate_recheck_or compareSources(rootNone, childNone,
                "CAST(v['number'] AS BIGINT) = 42 "
                + "OR CAST(v['action'] AS STRING) = 'closed'")
        order_qt_root_candidate_recheck_not compareSources(rootNone, childNone,
                "NOT (CAST(v['action'] AS STRING) = 'opened')")

        order_qt_root_none_match_whole_value compareSources(rootNone, childNone,
                "CAST(v['comment']['body'] AS STRING) MATCH_ANY "
                + "'Root index makes variant search fast'")
        order_qt_root_english_match_any compareSources(rootEnglish, childEnglish,
                "CAST(v['comment']['body'] AS STRING) MATCH_ANY 'root benchmark'")
        order_qt_root_english_match_all compareSources(rootEnglish, childEnglish,
                "CAST(v['comment']['body'] AS STRING) MATCH_ALL 'variant search'")
        order_qt_root_english_nested_and_exact compareSources(rootEnglish, childEnglish,
                "CAST(v['comment']['body'] AS STRING) MATCH_ALL 'root search' "
                + "AND CAST(v['action'] AS STRING) = 'opened'")

        order_qt_root_both_exact_and_match compareSources(rootBoth, childBoth,
                "CAST(v['repo']['name'] AS STRING) = 'apache/doris' "
                + "AND CAST(v['comment']['body'] AS STRING) MATCH_ANY 'root variant'")
        order_qt_root_both_exact_or_match compareSources(rootBoth, childBoth,
                "CAST(v['number'] AS BIGINT) = 42 "
                + "OR CAST(v['comment']['body'] AS STRING) MATCH_ALL 'query engine'")

        // Populate an all-false condition-cache entry with the exact analyzer, then execute the
        // same MATCH expression with English. Analyzer semantics must be part of the cache key.
        sql "SET enable_condition_cache = true"
        sql """
            SELECT id FROM ${rootBoth}
            WHERE CAST(v['comment']['body'] AS STRING) MATCH_ANY 'root' USING ANALYZER none
            ORDER BY id
        """
        order_qt_root_both_condition_cache_exact """
            SELECT id FROM ${rootBoth}
            WHERE CAST(v['comment']['body'] AS STRING) MATCH_ANY 'root' USING ANALYZER none
            ORDER BY id
        """
        order_qt_root_both_condition_cache_english """
            SELECT id FROM ${rootBoth}
            WHERE CAST(v['comment']['body'] AS STRING) MATCH_ANY 'root' USING ANALYZER english
            ORDER BY id
        """
        sql "SET enable_condition_cache = false"

        qt_root_show_create "SHOW CREATE TABLE ${rootNone}"
        qt_root_both_show_create "SHOW CREATE TABLE ${rootBoth}"
    }
}
