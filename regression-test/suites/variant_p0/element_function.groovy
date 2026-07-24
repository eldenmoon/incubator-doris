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

suite("regression_test_variant_element_at", "p0")  {
      sql """
        CREATE TABLE IF NOT EXISTS element_fn_test(
            k bigint,
            v variant,
            v1 variant not null,
        )
        UNIQUE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4
        properties("replication_num" = "1");
    """

    sql """insert into element_fn_test values
            (1, parse_to_variant('{"arr1" : [1, 2, 3]}'), parse_to_variant('{"arr2" : [4, 5, 6]}'))"""
    qt_sql """select array_first((x,y) -> (x - y) < 0, cast(v['arr1'] as array<int>), cast(v1['arr2'] as array<int>)) from element_fn_test"""

    // element_at keeps the Variant type. Cast explicitly when the test needs a scalar string.
    def scalar = sql """select cast(parse_to_variant('{"wsn":"SRFSPXFDVY","uploadTimeValue":"2026-05-20 18:40:02"}')['wsn'] as string)"""
    assertEquals("SRFSPXFDVY", scalar[0][0])

    def sub = sql """select substring(cast(parse_to_variant('{"uploadTimeValue":"2026-05-20 18:40:02"}')['uploadTimeValue'] as string), 1, 10)"""
    assertEquals("2026-05-20", sub[0][0])

    // The scalar cast unescapes JSON string contents.
    def escaped = sql """select cast(parse_to_variant('{"k":"a\\\\"b"}')['k'] as string)"""
    assertEquals("a\"b", escaped[0][0])

    def num = sql """select cast(parse_to_variant('{"n":49.98}')['n'] as string)"""
    assertEquals("49.98", num[0][0])

    // Casting Variant arrays and objects to string preserves their JSON text representation.
    def arr = sql """select cast(parse_to_variant('{"a":[1,2,3]}')['a'] as string)"""
    assertEquals("[1,2,3]", arr[0][0])

    def obj = sql """select cast(parse_to_variant('{"o":{"name":"john"}}')['o'] as string)"""
    assertEquals('{"name":"john"}', obj[0][0])
}
