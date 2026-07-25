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

suite("duplicate_json_path", "p0") {
    def customBeConfig = [
        variant_enable_duplicate_json_path_check: true
    ]
    setBeConfigTemporary(customBeConfig) {
        sql "DROP TABLE IF EXISTS duplicate_json_path"
        sql """
            CREATE TABLE duplicate_json_path (
                k int,
                v variant
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "group_commit_interval_ms" = "2000",
                "disable_auto_compaction" = "true"
            );
        """

        sql """insert into duplicate_json_path values (1, parse_to_variant('{"a":42,"a":{"b":42}}'))"""
        sql """insert into duplicate_json_path values (2, parse_to_variant('{"a" : 123, "a" : "123"}'))"""
        test {
            sql """insert into duplicate_json_path values (3, parse_to_variant('{"a.b":1,"a":{"b":2}}'))"""
            exception "distinct structures collide at dotted path a.b"
        }
        test {
            sql """insert into duplicate_json_path values (4, parse_to_variant('{"a":{"b":3},"a.b":4}'))"""
            exception "distinct structures collide at dotted path a.b"
        }
        sql """insert into duplicate_json_path values (3, parse_to_variant('{"a":{"b":2}}'))"""
        sql """insert into duplicate_json_path values (4, parse_to_variant('{"a":{"b":3}}'))"""
        sql """insert into duplicate_json_path values (5, parse_to_variant('{"a":{"b":5},"a":{"c":6}}'))"""
        sql """insert into duplicate_json_path values (6, parse_to_variant('{"a":[1],"a":2}'))"""
        sql """insert into duplicate_json_path values (7, parse_to_variant('{"a":2,"a":[1]}'))"""

        sql """insert into duplicate_json_path values (99, parse_to_variant('{"a.b":1}'))"""
        sql """delete from duplicate_json_path where k = 99"""

        streamLoad {
            table "duplicate_json_path"
            set 'read_json_by_line', 'true'
            set 'format', 'json'
            set 'group_commit', 'async_mode'
            set 'where', 'k not in (10, 11)'
            unset 'label'
            file 'duplicate_json_path.json'
            time 10000

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(7, json.NumberTotalRows)
                assertEquals(5, json.NumberLoadedRows)
                assertEquals(2, json.NumberUnselectedRows)
            }
        }

        sql """insert into duplicate_json_path values (10, parse_to_variant('{"a":{"b":9}}'))"""
        sql """insert into duplicate_json_path values (11, parse_to_variant('{"a":{"b":10}}'))"""

        for (int i = 0; i < 30; i++) {
            def count = sql "select count(*) from duplicate_json_path"
            if (count[0][0] == 14) {
                break
            }
            sleep(1000)
        }
        qt_duplicate_json_path_row_count """
            select count(*) from duplicate_json_path
        """

        // Duplicate members keep the first complete subtree at the member boundary.
        qt_duplicate_json_path_before_full_compaction """
            select k, cast(v['a'] as string), cast(v['a']['b'] as string), cast(v['a']['c'] as string)
            from duplicate_json_path
            order by k
        """

        trigger_and_wait_compaction("duplicate_json_path", "full")
        qt_duplicate_json_path_after_full_compaction """
            select k, cast(v['a'] as string), cast(v['a']['b'] as string), cast(v['a']['c'] as string)
            from duplicate_json_path
            order by k
        """
    }
}
