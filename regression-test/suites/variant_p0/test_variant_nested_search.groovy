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

suite("test_variant_nested_search", "p0") {
    def tableName = "variant_nested_search_test"
    sql "DROP TABLE IF EXISTS ${tableName}"

    sql "set enable_variant_flatten_nested = true"
    sql "set enable_common_expr_pushdown = true"
    sql "set default_variant_max_subcolumns_count = 0"

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
        (1, '[{"msg":"hello","title":"news"},{"msg":"foo","title":"bar"}]'),
        (2, '[{"msg":"hello"},{"title":"news"}]'),
        (3, '[{"msg":"hello","title":"sports"}]'),
        (4, NULL)
    """

    // Thread.sleep(5000)

    qt_nested_and_match """
        SELECT id FROM ${tableName}
        WHERE search('NESTED(data, data.msg:hello AND data.title:news)')
        ORDER BY id
    """

    qt_nested_term_match """
        SELECT id FROM ${tableName}
        WHERE search('NESTED(data, data.msg:hello)')
        ORDER BY id
    """

    def tableName2 = "variant_nested_search_obj_test"
    sql "DROP TABLE IF EXISTS ${tableName2}"

    sql """
        CREATE TABLE ${tableName2} (
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
        INSERT INTO ${tableName2} VALUES
        (1, '{"items":[{"msg":"hello","title":"news"},{"msg":"foo","title":"bar"}]}'),
        (2, '{"items":[{"msg":"hello"},{"title":"news"}]}'),
        (3, '{"items":[{"msg":"hello","title":"sports"}]}'),
        (4, '{"items":null}'),
        (5, NULL)
    """

    qt_nested_path_with_dot_and_match """
        SELECT id FROM ${tableName2}
        WHERE search('NESTED(data.items, data.items.msg:hello AND data.items.title:news)')
        ORDER BY id
    """

    qt_nested_path_with_dot_term_match """
        SELECT id FROM ${tableName2}
        WHERE search('NESTED(data.items, data.items.msg:hello)')
        ORDER BY id
    """
}
