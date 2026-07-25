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

// Variant storage properties do not change its query-time type. Different V2 layouts can be
// passed directly between expressions and columns. String CAST remains a scalar string;
// parse_to_variant is required for structured JSON.
suite("test_variant_cast_strict", "p0") {
    // Use session variables to set variant defaults (column-level properties
    // forbid setting max_subcolumns_count and enable_doc_mode together).
    sql """ set default_variant_enable_doc_mode = true """
    sql """ set default_variant_max_subcolumns_count = 37 """
    sql """ set default_variant_doc_materialization_min_rows = 8 """
    sql """ set default_variant_doc_hash_shard_count = 7 """

    def t = "variant_cast_strict"
    sql """ DROP TABLE IF EXISTS ${t} """
    sql """
        CREATE TABLE IF NOT EXISTS ${t} (
            id bigint,
            v variant
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    def jsonValue = '{"anchors":{"common_int":150025,"phase_marker":"phase_a","present":true,"row_id":15001},"dynamic":{"path_00000":15001000,"path_00001":15001001},"parent":{"child":{"name":"phase_a_15001"}},"phase_a_small":{"leaf":15001}}'

    // ---- Case 1: different storage properties are compatible. The source is still a
    // string root because CAST(string AS VARIANT) does not parse JSON.
    sql """ insert into ${t} values (15001, cast('${jsonValue}' as variant<properties(
            "variant_enable_doc_mode" = "true",
            "variant_doc_materialization_min_rows" = "999",
            "variant_doc_hash_shard_count" = "7"
        )>)); """
    qt_case1 """ select id, variant_type(v), v['anchors'] is null from ${t} where id = 15001; """

    // ---- Case 2: parse JSON explicitly. No JSONB round trip is needed.
    sql """ insert into ${t} values
             (15002, parse_to_variant('${jsonValue}')); """
    qt_case2 """ select id, cast(v['anchors']['row_id'] as bigint) from ${t} where id = 15002; """

    // ---- Case 3: an explicit string-to-Variant cast keeps the complete input as a
    // scalar string, so element access returns SQL NULL.
    sql """ insert into ${t} values (15003, cast('${jsonValue}' as variant)); """
    test {
        sql """ select id, cast(v['anchors']['row_id'] as bigint) from ${t} where id = 15003; """
        result([[15003L, null]])
    }

    // ---- Case 3b: element access on an explicit-cast string scalar returns SQL NULL even
    // when its text is valid JSON and contains either a flat or nested matching key.
    sql """ DROP TABLE IF EXISTS variant_cast_root_collision """
    sql """
        CREATE TABLE variant_cast_root_collision (
            id bigint,
            v variant
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    sql """ insert into variant_cast_root_collision
             values (1, cast('{"a.b":1,"a":{"b":2}}' as variant)); """
    test {
        sql """ select cast(v['a']['b'] as bigint), cast(v['a']['missing'] as bigint)
                  from variant_cast_root_collision where id = 1; """
        result([[null, null]])
    }
    // ---- Case 4: direct cross-table copy works even when storage properties differ.
    def t_src = "variant_cast_strict_src"
    // Create source table with NO doc-mode by clearing session vars first, then restore.
    sql """ set default_variant_enable_doc_mode = false """
    sql """ set default_variant_max_subcolumns_count = 0 """
    sql """ DROP TABLE IF EXISTS ${t_src} """
    sql """
        CREATE TABLE IF NOT EXISTS ${t_src} (
            id bigint,
            v variant
        )
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1", "disable_auto_compaction" = "true");
    """
    sql """ insert into ${t_src} values (15004, parse_to_variant('${jsonValue}')); """
    // Restore session vars so target's column-level config keeps matching.
    sql """ set default_variant_enable_doc_mode = true """
    sql """ set default_variant_max_subcolumns_count = 37 """

    sql """ insert into ${t} select id, v from ${t_src}; """
    qt_case4 """ select id, cast(v['anchors']['row_id'] as bigint) from ${t} where id = 15004; """

    // ---- Case 5: multi-row VALUES follows the same explicit-parse rule.
    sql """
        insert into ${t} values
            (15005, parse_to_variant('${jsonValue}')),
            (15006, cast('${jsonValue}' as variant));
    """
    test {
        sql """ select id, variant_type(v), cast(v['anchors']['row_id'] as bigint)
                 from ${t} where id in (15005, 15006) order by id; """
        result([
                [15005L, "object", 15001L],
                [15006L, "string", null]
        ])
    }
}
