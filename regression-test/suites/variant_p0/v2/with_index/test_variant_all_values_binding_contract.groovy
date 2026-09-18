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

// The query bindings of the VARIANT root index (be/src/storage/index/inverted/variant_root_index.h,
// contract 3): the empty key `v['']` is a path binding (candidates + residual), never the whole
// document; a declared FLOAT path is indexed after its storage conversion and binds FLOAT
// literals only; score() is refused on a root MATCH.
suite("test_variant_all_values_binding_contract", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        sql "SET default_variant_enable_doc_mode = false"
        sql "SET default_variant_max_subcolumns_count = 1"
        sql "SET enable_match_without_inverted_index = true"
        sql "SET enable_sql_cache = false"
        sql "SET enable_query_cache = false"
        sql "SET enable_condition_cache = false"
        sql "SET enable_inverted_index_query_cache = false"
        sql "SET inverted_index_skip_threshold = 0"
        sql "DROP TABLE IF EXISTS variant_all_values_binding_contract"
        sql """
            CREATE TABLE variant_all_values_binding_contract (
                id BIGINT NOT NULL,
                v VARIANT<'f':FLOAT> NULL,
                INDEX exact_idx(v) USING INVERTED PROPERTIES(
                    "parser" = "none", "variant_index_scope" = "values"),
                INDEX text_idx(v) USING INVERTED PROPERTIES(
                    "parser" = "english", "variant_index_scope" = "values")
            ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true",
                       "inverted_index_storage_format" = "SNII")
        """
        // Row 2 holds the searched value under another key: the values index lists it as a
        // candidate for `v['']`, the residual drops it.
        sql """INSERT INTO variant_all_values_binding_contract VALUES
            (1, parse_to_variant('{"":"needle","k":"other"}')),
            (2, parse_to_variant('{"k":"needle"}')),
            (3, parse_to_variant('{"":"other"}')),
            (4, parse_to_variant('{"f":0.1,"k":"x"}')),
            (5, parse_to_variant('{"f":1}')),
            (6, parse_to_variant('{"f":2.5}')),
            (7, NULL)"""
        sql "SYNC"
        qt_show_create "SHOW CREATE TABLE variant_all_values_binding_contract"
        qt_rows "SELECT id, CAST(v AS STRING), variant_type(v['f']) FROM variant_all_values_binding_contract ORDER BY id"

        def predicates = [
            empty_key_match: "v[''] MATCH_ANY 'needle' USING ANALYZER english",
            empty_key_match_exact: "v[''] MATCH_ANY 'needle' USING ANALYZER none",
            empty_key_eq: "CAST(v[''] AS STRING) = 'needle'",
            empty_key_in: "CAST(v[''] AS STRING) IN ('needle', 'other')",
            empty_key_not: "NOT (v[''] MATCH_ANY 'needle' USING ANALYZER english)",
            empty_key_or: "v[''] MATCH_ANY 'needle' USING ANALYZER english OR id = 6",
            root_match: "v MATCH_ANY 'needle' USING ANALYZER english",
            // A decimal literal compares as DOUBLE, so 0.1 never equals the float 0.1f on either
            // side; a FLOAT literal reaches the index as the typed term of the declared path.
            float_eq: "CAST(v['f'] AS FLOAT) = 0.1",
            float_eq_float_literal: "CAST(v['f'] AS FLOAT) = CAST(0.1 AS FLOAT)",
            float_in: "CAST(v['f'] AS FLOAT) IN (0.1, 2.5)",
            float_in_float_literals: "CAST(v['f'] AS FLOAT) IN (CAST(0.1 AS FLOAT), CAST(2.5 AS FLOAT))",
            float_integral: "CAST(v['f'] AS FLOAT) = 1",
            float_as_double: "CAST(v['f'] AS DOUBLE) = 0.1",
            float_as_string: "CAST(v['f'] AS STRING) = '0.1'",
            root_float_text: "v MATCH_ANY '0.1' USING ANALYZER none",
            root_float_canonical_text: "v MATCH_ANY '0.10000000149011612' USING ANALYZER none",
            root_float_token: "v MATCH_ANY '0.10000000149011612' USING ANALYZER english"
        ]
        predicates.each { name, predicate ->
            def statement = "SELECT id FROM variant_all_values_binding_contract WHERE ${predicate} ORDER BY id"
            sql "SET enable_inverted_index_query = false"
            def scanned = sql(statement)
            sql "SET enable_inverted_index_query = true"
            assertEquals(scanned, sql(statement), "index/scan parity: ${name}")
            def countStatement = "SELECT COUNT(*) FROM variant_all_values_binding_contract WHERE ${predicate}"
            assertEquals(scanned.size() as long, sql(countStatement)[0][0] as long, "COUNT/scan parity: ${name}")
            quickTest(name, statement, true)
            quickTest("${name}_count", countStatement)
        }

        // The index takes part: 7 rows, the token index lists rows 1 and 2 as candidates and
        // the residual keeps row 1 only.
        def checkpoint = "segment_iterator.inverted_index.filtered_rows"
        try {
            GetDebugPoint().enableDebugPointForAllBEs(checkpoint, [filtered_rows: "5"])
            order_qt_empty_key_candidates """SELECT id FROM variant_all_values_binding_contract
                WHERE v[''] MATCH_ANY 'needle' USING ANALYZER english ORDER BY id"""
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
        }

        // The declared FLOAT path is indexed after its storage conversion: the FLOAT literal is
        // served by the index (rows 1-3, 5-7 filtered, row 4 kept) rather than by the scan.
        try {
            GetDebugPoint().enableDebugPointForAllBEs(checkpoint, [filtered_rows: "6"])
            order_qt_float_typed_path_index """SELECT id FROM variant_all_values_binding_contract
                WHERE CAST(v['f'] AS FLOAT) = CAST(0.1 AS FLOAT) ORDER BY id"""
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(checkpoint)
        }

        // The root index stores neither positions nor norms: no similarity, so no score().
        test {
            sql """SELECT id, score() FROM variant_all_values_binding_contract
                   WHERE v MATCH_ANY 'needle' ORDER BY score() DESC LIMIT 5"""
            exception "VARIANT root index"
        }
    }
}
