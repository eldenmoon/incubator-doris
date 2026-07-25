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

// Regression test for: compaction fails on no-key duplicate table with only
// variant columns when the first variant column has unique_id=0.
// Root cause: TabletColumn::is_extracted_column() used "_parent_col_unique_id > 0"
// which incorrectly excluded subcolumns whose parent has unique_id=0.

suite("test_compaction_nokey_variant") {
    def tableName = "test_compaction_nokey_variant"

    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            v1 VARIANT,
            v2 VARIANT,
            v3 VARIANT
        )
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "enable_duplicate_without_keys_by_default" = "true"
        );
    """

    // Insert multiple batches to create multiple rowsets
    sql """INSERT INTO ${tableName} VALUES
        (parse_to_variant('{"name":"Alice","age":30}'), parse_to_variant('{"city":"Beijing","zip":100000}'), parse_to_variant('{"score":95.5,"passed":true}')),
        (parse_to_variant('{"name":"Bob","age":25}'), parse_to_variant('{"city":"Shanghai"}'), parse_to_variant('{"score":88.0,"passed":true}')),
        (parse_to_variant('{"name":"Charlie"}'), parse_to_variant('{"city":"Shenzhen","zip":518000}'), parse_to_variant('{"score":72.3,"passed":false}'));"""

    sql """INSERT INTO ${tableName} VALUES (parse_to_variant('{"name":"u1","age":10}'), parse_to_variant('{"city":"c1"}'), parse_to_variant('{"score":10.5}'));"""
    sql """INSERT INTO ${tableName} VALUES (parse_to_variant('{"name":"u2","age":20}'), parse_to_variant('{"city":"c2"}'), parse_to_variant('{"score":20.5}'));"""
    sql """INSERT INTO ${tableName} VALUES (parse_to_variant('{"name":"u3","age":30}'), parse_to_variant('{"city":"c3"}'), parse_to_variant('{"score":30.5}'));"""
    sql """INSERT INTO ${tableName} VALUES (parse_to_variant('{"name":"u4","age":40}'), parse_to_variant('{"city":"c4"}'), parse_to_variant('{"score":40.5}'));"""
    sql """INSERT INTO ${tableName} VALUES (parse_to_variant('{"name":"u5","age":50}'), parse_to_variant('{"city":"c5"}'), parse_to_variant('{"score":50.5}'));"""

    // Verify data before compaction
    qt_before_compaction """SELECT cast(v1 as string) c1, cast(v2 as string) c2, cast(v3 as string) c3
        FROM ${tableName} ORDER BY c1;"""

    def rowCountBefore = sql "SELECT count() FROM ${tableName}"
    assertEquals(8, rowCountBefore[0][0])

    // Trigger cumulative compaction - reproduces the bug when is_extracted_column() is wrong
    trigger_and_wait_compaction(tableName, "cumulative")

    // Verify data after compaction
    qt_after_compaction """SELECT cast(v1 as string) c1, cast(v2 as string) c2, cast(v3 as string) c3
        FROM ${tableName} ORDER BY c1;"""

    def rowCountAfter = sql "SELECT count() FROM ${tableName}"
    assertEquals(8, rowCountAfter[0][0])

    sql "DROP TABLE IF EXISTS ${tableName}"
}
