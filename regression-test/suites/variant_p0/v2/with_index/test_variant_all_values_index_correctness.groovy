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

suite("test_variant_all_values_index_correctness", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        def tokenTable = "variant_all_values_token"
        def tokenChildren = "variant_all_values_token_children"
        def tokenOracle = "variant_all_values_token_oracle"
        def exactTable = "variant_all_values_exact"
        def exactChildren = "variant_all_values_exact_children"
        def pathRootTable = "variant_path_root_mode_isolation"
        def tables = [tokenTable, tokenChildren, tokenOracle, exactTable, exactChildren,
                pathRootTable]

        sql "SET default_variant_enable_doc_mode = false"
        sql "SET default_variant_max_subcolumns_count = 0"
        sql "SET default_variant_enable_typed_paths_to_sparse = false"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET enable_condition_cache = false"
        sql "SET enable_inverted_index_query_cache = false"
        sql "SET enable_match_without_inverted_index = true"
        sql "SET inverted_index_skip_threshold = 0"
        sql "SET experimental_enable_parallel_scan = false"

        tables.each { sql "DROP TABLE IF EXISTS ${it}" }

        def createTable = { String tableName, String mode, String parser,
                            int maxSubcolumns, boolean withIndex ->
            def modeProperty = mode == null ? "" : ", \"variant_index_mode\" = \"${mode}\""
            def supportPhraseProperty = mode == null ? ', "support_phrase" = "false"' : ''
            def index = withIndex ? """,
                    INDEX idx_payload(payload) USING INVERTED PROPERTIES(
                        "parser" = "${parser}"${supportPhraseProperty}${modeProperty})""" : ""
            sql """
                CREATE TABLE ${tableName} (
                    id BIGINT NOT NULL,
                    payload VARIANT<PROPERTIES(
                        "variant_max_subcolumns_count" = "${maxSubcolumns}")> NULL
                    ${index}
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

        createTable(tokenTable, "all_values", "english", 0, true)
        // Omitting variant_index_mode retains the ordinary per-materialized-child layout.
        createTable(tokenChildren, null, "english", 10_000, true)
        createTable(tokenOracle, null, "english", 0, false)
        createTable(exactTable, "all_values", "none", 0, true)
        createTable(exactChildren, null, "none", 10_000, true)
        createTable(pathRootTable, "root", "english", 0, true)

        def longMessage = ("x" * 300) + " needle"
        def firstRows = """
            (1,  parse_to_variant('{"repo":"apache/doris","message":"release ready","n":123}')),
            (2,  parse_to_variant('{"repo":"other/repo","message":"apache/doris","n":7}')),
            (3,  parse_to_variant('{"repo":"apache/spark","message":"security fix applied","n":9}')),
            (4,  parse_to_variant('{"repo":"security/tools","message":"fix later","n":11}')),
            (5,  parse_to_variant('{"left":"security","right":"fix","n":13}')),
            (6,  parse_to_variant('{"repo":"other/repo","other":"apache/doris","n":17}')),
            (7,  parse_to_variant('{"repo":"other/repo","n":7,"other":123}')),
            (8,  parse_to_variant('{"tags":["doris","variant"]}')),
            (9,  parse_to_variant('{"message":null}')),
            (10, parse_to_variant('{}')),
            (11, NULL),
            (12, parse_to_variant('{"message":"DoRiS COMMUNITY","n":"123"}')),
            (13, parse_to_variant('{"message":"${longMessage}"}'))
        """
        tables.each { sql "INSERT INTO ${it} VALUES ${firstRows}" }
        sql "SYNC"

        // The pathless all-values postings admit ids 1, 2, 6, 8, and 12 for the repo query,
        // while the path-bound children postings admit only id 1. Both must keep the same scalar
        // result, but their filtered-row counts prove the intended physical indexes participated.
        def assertIndexFilteredRows = { String tableName, int expectedFilteredRows ->
            def checkpoint = "segment_iterator.inverted_index.filtered_rows"
            try {
                GetDebugPoint().enableDebugPointForAllBEs(
                        checkpoint, [filtered_rows: "${expectedFilteredRows}"])
                def result = sql """
                    SELECT id FROM ${tableName}
                    WHERE CAST(payload['repo'] AS STRING) MATCH_ANY 'doris'
                    ORDER BY id
                """
                assertEquals([1L], result.collect { it[0] })
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
            }
        }
        assertIndexFilteredRows(tokenTable, 8)
        assertIndexFilteredRows(tokenChildren, 12)

        sql "SET enable_profile = true"
        sql "SET profile_level = 2"
        def profileTag = "variant_all_values_candidate_profile"
        profile(profileTag) {
            run {
                sql """
                    /* ${profileTag} */ SELECT id FROM ${tokenTable}
                    WHERE CAST(payload['repo'] AS STRING) MATCH_ANY 'doris'
                    ORDER BY id
                """
            }
            check { profileString, exception ->
                if (exception != null) {
                    throw exception
                }
                def matcher = profileString =~ /RowsInvertedIndexFiltered:(?:&nbsp;|\s)*(\d+)/
                def filteredRows = 0
                while (matcher.find()) {
                    filteredRows += matcher.group(1).toInteger()
                }
                assertEquals(8, filteredRows)
                assertTrue(profileString.contains("InvertedIndexQueryTime"))
            }
        }

        // A second rowset covers empty/root-null, non-object root values, and a nested path
        // collision. The scalar roots exercise the dedicated all-values root-value writer path.
        def secondRows = """
            (14, parse_to_variant('{}')),
            (15, parse_to_variant('null')),
            (16, parse_to_variant('"apache/doris"')),
            (17, parse_to_variant('123')),
            (18, parse_to_variant('{"nested":{"repo":"apache/doris"}}'))
        """
        tables.each { sql "INSERT INTO ${it} VALUES ${secondRows}" }
        sql "SYNC"

        def ids = { String tableName, String predicate, boolean enableIndex ->
            def hint = enableIndex ? "" :
                    "/*+ SET_VAR(enable_inverted_index_query=false, enable_match_without_inverted_index=true) */"
            return sql("SELECT ${hint} id FROM ${tableName} WHERE ${predicate} ORDER BY id")
        }
        def countRows = { String tableName, String predicate, boolean enableIndex ->
            def hint = enableIndex ? "" :
                    "/*+ SET_VAR(enable_inverted_index_query=false, " +
                    "enable_count_on_index_pushdown=false, enable_match_without_inverted_index=true) */"
            return sql("SELECT ${hint} COUNT(*) FROM ${tableName} WHERE ${predicate}")
        }
        // Use the same table with its index disabled as the whole-root oracle so analyzer
        // selection cannot drift from the indexed execution.
        def assertAllValuesParity = { String tableName, String predicate ->
            def scan = ids(tableName, predicate, false)
            assertEquals(scan, ids(tableName, predicate, true),
                    "all-values index differs from its residual scan: ${predicate}")
            return scan
        }
        // The children table with its index disabled is the primary oracle. Comparing both
        // disabled scans also catches logical-layout differences independently of index results.
        def assertLayoutParity = { String allValuesTable, String childrenTable, String predicate ->
            def scan = ids(childrenTable, predicate, false)
            assertEquals(scan, ids(allValuesTable, predicate, false),
                    "all-values residual differs from children residual: ${predicate}")
            assertEquals(scan, ids(childrenTable, predicate, true),
                    "children index differs from its scan: ${predicate}")
            assertEquals(scan, ids(allValuesTable, predicate, true),
                    "all-values index differs from children scan: ${predicate}")
            return scan
        }
        def assertCountParity = { String allValuesTable, String childrenTable, String predicate ->
            def scan = countRows(childrenTable, predicate, false)
            assertEquals(scan, countRows(allValuesTable, predicate, false),
                    "all-values count residual differs from children residual: ${predicate}")
            assertEquals(scan, countRows(childrenTable, predicate, true),
                    "children index count differs from its scan: ${predicate}")
            assertEquals(scan, countRows(allValuesTable, predicate, true),
                    "all-values index count differs from children scan: ${predicate}")
            return scan
        }

        // Query parity alone can miss corruption in an unreferenced field. Compare the complete
        // reconstructed logical value across both analyzers and both physical layouts as well.
        def logicalRows = { String tableName ->
            return sql("""
                SELECT id, payload IS NULL, CAST(payload AS STRING)
                FROM ${tableName}
                ORDER BY id
            """)
        }
        def logicalRowsBefore = logicalRows(tokenChildren)
        [tokenTable, exactTable, exactChildren].each { tableName ->
            assertEquals(logicalRowsBefore, logicalRows(tableName),
                    "logical payload differs after write for ${tableName}")
        }

        def rootTokenPredicates = [
                "payload MATCH_ANY 'doris'",
                "payload MATCH_ALL 'security fix'",
                "payload MATCH_ANY 'DORIS'",
                "payload MATCH_ANY 'the'",             // English stop word
                "payload MATCH_ANY ''",                // empty analyzed query
                "payload MATCH_ANY 'needle'"           // value longer than ignore_above default
        ]
        def tokenPathPredicates = [
                "CAST(payload['repo'] AS STRING) MATCH_ANY 'doris'",
                "CAST(payload['nested']['repo'] AS STRING) MATCH_ANY 'doris'",
                "CAST(payload['message'] AS STRING) MATCH_ALL 'security fix'",
                "CAST(payload['repo'] AS STRING) = 'apache/doris'",
                "CAST(payload['n'] AS BIGINT) = 123",
                "CAST(payload['n'] AS BIGINT) IN (7, 123)",
                "CAST(payload['n'] AS BIGINT) IN (123, NULL)",
                "COALESCE(CAST(payload['missing'] AS BIGINT), 0) = 0",
                "CAST(payload['message'] AS STRING) IS NULL",
                "CAST(payload['repo'] AS STRING) MATCH_ANY 'doris' AND id < 10",
                "CAST(payload['repo'] AS STRING) MATCH_ANY 'doris' OR id = 9",
                "NOT (CAST(payload['repo'] AS STRING) MATCH_ANY 'doris')",
                "CAST(payload['tags'] AS ARRAY<TEXT>) MATCH_ANY 'doris'"
        ]
        def rootTokenBefore = [:]
        rootTokenPredicates.each { rootTokenBefore[it] = assertAllValuesParity(tokenTable, it) }
        def tokenPathBefore = [:]
        tokenPathPredicates.each {
            tokenPathBefore[it] = assertLayoutParity(tokenTable, tokenChildren, it)
        }

        assertEquals([1L, 2L, 6L, 8L, 12L, 16L, 18L],
                rootTokenBefore["payload MATCH_ANY 'doris'"].collect { it[0] })
        assertEquals([3L, 4L, 5L],
                rootTokenBefore["payload MATCH_ALL 'security fix'"].collect { it[0] })
        assertEquals([13L], rootTokenBefore["payload MATCH_ANY 'needle'"].collect { it[0] })
        assertEquals([1L], tokenPathBefore[
                "CAST(payload['repo'] AS STRING) MATCH_ANY 'doris'"].collect { it[0] })
        assertEquals([18L], tokenPathBefore[
                "CAST(payload['nested']['repo'] AS STRING) MATCH_ANY 'doris'"].collect { it[0] })
        assertEquals([3L], tokenPathBefore[
                "CAST(payload['message'] AS STRING) MATCH_ALL 'security fix'"].collect { it[0] })
        assertEquals([1L, 9L], tokenPathBefore[
                "CAST(payload['repo'] AS STRING) MATCH_ANY 'doris' OR id = 9"].collect { it[0] })
        assertEquals([8L], tokenPathBefore[
                "CAST(payload['tags'] AS ARRAY<TEXT>) MATCH_ANY 'doris'"].collect { it[0] })

        def countPredicate = "CAST(payload['repo'] AS STRING) MATCH_ANY 'doris'"
        def countBefore = assertCountParity(tokenTable, tokenChildren, countPredicate)
        assertEquals(1L, countBefore[0][0])
        explain {
            sql("SELECT COUNT(*) FROM ${tokenTable} WHERE ${countPredicate}")
            notContains "pushAggOp=COUNT_ON_INDEX"
        }

        test {
            sql "SELECT id FROM ${tokenTable} WHERE payload MATCH_PHRASE 'security fix'"
            exception "VARIANT root column supports only MATCH"
        }

        def exactRootPredicates = [
                "payload MATCH_ANY 'apache/doris'",
                "payload MATCH_ANY '123'"
        ]
        def exactPathPredicates = [
                "CAST(payload['repo'] AS STRING) MATCH_ANY 'apache/doris'",
                "CAST(payload['repo'] AS STRING) = 'apache/doris'",
                "CAST(payload['repo'] AS STRING) IN ('apache/doris', 'other/repo')",
                "CAST(payload['n'] AS BIGINT) = 123",
                "CAST(payload['n'] AS BIGINT) IN (7, 123)",
                "CAST(payload['n'] AS BIGINT) IN (123, NULL)",
                "CAST(payload['message'] AS STRING) IS NULL",
                "CAST(payload['repo'] AS STRING) != 'apache/doris'",
                "NOT (CAST(payload['repo'] AS STRING) MATCH_ANY 'apache/doris')"
        ]
        def exactRootBefore = [:]
        exactRootPredicates.each { exactRootBefore[it] = assertAllValuesParity(exactTable, it) }
        def exactPathBefore = [:]
        exactPathPredicates.each {
            exactPathBefore[it] = assertLayoutParity(exactTable, exactChildren, it)
        }
        assertEquals([1L, 2L, 6L, 16L, 18L], exactRootBefore[
                "payload MATCH_ANY 'apache/doris'"].collect { it[0] })
        assertEquals([1L, 7L, 12L, 17L], exactRootBefore[
                "payload MATCH_ANY '123'"].collect { it[0] })
        assertEquals([1L], exactPathBefore[
                "CAST(payload['repo'] AS STRING) MATCH_ANY 'apache/doris'"].collect { it[0] })
        assertEquals([1L, 12L], exactPathBefore[
                "CAST(payload['n'] AS BIGINT) IN (123, NULL)"].collect { it[0] })

        // With row execution disabled, only an exact whole-root all-values index may admit root
        // MATCH. Children, path-qualified root mode, and an unindexed table retain their boundary.
        sql "SET enable_match_without_inverted_index = false"
        assertEquals([1L, 2L, 6L, 8L, 12L, 16L, 18L],
                ids(tokenTable, "payload MATCH_ANY 'doris'", true).collect { it[0] })
        assertEquals([1L, 2L, 6L, 16L, 18L],
                ids(exactTable, "payload MATCH_ANY 'apache/doris'", true).collect { it[0] })
        [tokenChildren, pathRootTable, tokenOracle].each { tableName ->
            test {
                sql "SELECT id FROM ${tableName} WHERE payload MATCH_ANY 'doris'"
                exception "not support execute_match"
            }
        }
        sql "SET enable_match_without_inverted_index = true"

        def compactedTables = [tokenTable, tokenChildren, exactTable, exactChildren]
        compactedTables.each { trigger_and_wait_compaction(it, "full", 1800) }
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
        compactedTables.each { assertFullCompactionSucceeded(it) }

        // Result parity can still pass if compaction drops an index and falls back to row
        // execution. Re-prove both physical layouts on the compacted 18-row segment: the
        // pathless all-values candidates contain seven rows, while the repo child contains one.
        assertIndexFilteredRows(tokenTable, 11)
        assertIndexFilteredRows(tokenChildren, 17)
        sql "SET enable_match_without_inverted_index = false"
        try {
            assertEquals(rootTokenBefore["payload MATCH_ANY 'doris'"],
                    ids(tokenTable, "payload MATCH_ANY 'doris'", true))
            assertEquals(exactRootBefore["payload MATCH_ANY 'apache/doris'"],
                    ids(exactTable, "payload MATCH_ANY 'apache/doris'", true))
        } finally {
            sql "SET enable_match_without_inverted_index = true"
        }

        compactedTables.each { tableName ->
            assertEquals(logicalRowsBefore, logicalRows(tableName),
                    "logical payload changed after compaction for ${tableName}")
        }

        rootTokenPredicates.each { predicate ->
            assertEquals(rootTokenBefore[predicate], assertAllValuesParity(tokenTable, predicate),
                    "whole-root token result changed after compaction: ${predicate}")
        }
        tokenPathPredicates.each { predicate ->
            assertEquals(tokenPathBefore[predicate],
                    assertLayoutParity(tokenTable, tokenChildren, predicate),
                    "token path result changed after compaction: ${predicate}")
        }
        exactRootPredicates.each { predicate ->
            assertEquals(exactRootBefore[predicate], assertAllValuesParity(exactTable, predicate),
                    "whole-root exact result changed after compaction: ${predicate}")
        }
        exactPathPredicates.each { predicate ->
            assertEquals(exactPathBefore[predicate],
                    assertLayoutParity(exactTable, exactChildren, predicate),
                    "exact path result changed after compaction: ${predicate}")
        }
        assertEquals(countBefore, assertCountParity(tokenTable, tokenChildren, countPredicate),
                "count result changed after compaction")

        // Golden rows make representative all-values/children/scan parity reviewable directly.
        def compareSources = { String allValuesTable, String childrenTable, String predicate ->
            return """
                SELECT source, id
                FROM (
                    SELECT 'all_values' AS source, id
                    FROM ${allValuesTable} WHERE ${predicate}
                    UNION ALL
                    SELECT 'children' AS source, id
                    FROM ${childrenTable} WHERE ${predicate}
                    UNION ALL
                    SELECT /*+ SET_VAR(enable_inverted_index_query=false,
                                      enable_match_without_inverted_index=true) */
                           'scan' AS source, id
                    FROM ${childrenTable} WHERE ${predicate}
                ) compared
                ORDER BY source, id
            """
        }
        order_qt_all_values_children_match_any compareSources(tokenTable, tokenChildren,
                "CAST(payload['repo'] AS STRING) MATCH_ANY 'doris'")
        order_qt_all_values_children_match_all compareSources(tokenTable, tokenChildren,
                "CAST(payload['message'] AS STRING) MATCH_ALL 'security fix'")
        order_qt_all_values_children_numeric_in compareSources(exactTable, exactChildren,
                "CAST(payload['n'] AS BIGINT) IN (7, 123)")
        order_qt_all_values_children_null_fallback compareSources(exactTable, exactChildren,
                "CAST(payload['message'] AS STRING) IS NULL")

        order_qt_all_values_null_domains """
            SELECT source, id, sql_null, root_text, message_null_or_missing
            FROM (
                SELECT 'all_values' AS source, id, payload IS NULL AS sql_null,
                       CAST(payload AS STRING) AS root_text,
                       CAST(payload['message'] AS STRING) IS NULL AS message_null_or_missing
                FROM ${exactTable}
                WHERE id IN (9, 10, 11, 15, 16, 17, 18)
                UNION ALL
                SELECT 'children' AS source, id, payload IS NULL AS sql_null,
                       CAST(payload AS STRING) AS root_text,
                       CAST(payload['message'] AS STRING) IS NULL AS message_null_or_missing
                FROM ${exactChildren}
                WHERE id IN (9, 10, 11, 15, 16, 17, 18)
            ) compared
            ORDER BY source, id
        """

        qt_all_values_show_create "SHOW CREATE TABLE ${tokenTable}"
        qt_all_values_children_show_create "SHOW CREATE TABLE ${tokenChildren}"
    }
}
