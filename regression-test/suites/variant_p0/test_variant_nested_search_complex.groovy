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

suite("test_variant_nested_search_complex", "p0") {
    def assertSearchIds = { String tableName, String dsl, List<Integer> expectedIds ->
        def rows = sql """
            SELECT id
            FROM ${tableName}
            WHERE search('${dsl}')
            ORDER BY id
        """
        def actualIds = rows.collect { it[0] as Integer }
        assertEquals(expectedIds, actualIds, "dsl=" + dsl + ", actual=" + actualIds)
    }

    def mkNestedDsl = { String rootPath, String innerQuery ->
        return "NESTED(${rootPath}, ${innerQuery})"
    }

    sql "set enable_variant_flatten_nested = true"
    sql "set enable_common_expr_pushdown = true"
    sql "set default_variant_max_subcolumns_count = 0"

    def tableName = "variant_nested_search_complex_test"
    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                data VARIANT,
                INDEX idx_data (data) USING INVERTED PROPERTIES("parser" = "english")
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "storage_format" = "V2"
            )
        """

        sql """
            INSERT INTO ${tableName} VALUES
            (1, '{"items":[{"subitems":[{"msg":"hello","title":"news","tags":"java kotlin"},{"msg":"foo","title":"bar","tags":"cpp"}]},{"subitems":[{"msg":"hello","title":"sports","tags":"python"}]}]}'),
            (2, '{"items":[{"subitems":[{"msg":"hello","tags":"java"},{"title":"news","tags":"python"}]}]}'),
            (3, '{"items":[{"subitems":[{"msg":"hello","title":"news","tags":"scala"}]},{"subitems":[]}]}'),
            (4, '{"items":[{"subitems":null}]}'),
            (5, NULL)
        """

        // Thread.sleep(5000)

        def rootPath = "data.items.subitems"

        def cases = [
                [
                        name: "and_same_element",
                        dsl: mkNestedDsl(rootPath,
                                "data.items.subitems.msg:hello AND data.items.subitems.title:news"),
                        expected: [1, 3]
                ],
                [
                        name: "and_with_any",
                        dsl: mkNestedDsl(rootPath,
                                "data.items.subitems.title:news AND data.items.subitems.tags:ANY(java kotlin)"),
                        expected: [1]
                ],
                [
                        name: "or_and_combo",
                        dsl: mkNestedDsl(rootPath,
                                "(data.items.subitems.title:sports OR data.items.subitems.title:news) AND data.items.subitems.msg:hello"),
                        expected: [1, 3]
                ],
                [
                        name: "or_any_combo",
                        dsl: mkNestedDsl(rootPath,
                                "(data.items.subitems.tags:ANY(python) OR data.items.subitems.tags:ANY(scala)) AND data.items.subitems.msg:hello"),
                        expected: [1, 3]
                ]
        ]

        cases.each { c ->
            assertSearchIds(tableName, c.dsl as String, c.expected as List<Integer>)
        }
    } finally {
        // sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
