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

suite("test_string_function") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set batch_size = 4096;"

    qt_sql "select elt(0, \"hello\", \"doris\");"
    qt_sql "select elt(1, \"hello\", \"doris\");"
    qt_sql "select elt(2, \"hello\", \"doris\");"
    qt_sql "select elt(3, \"hello\", \"doris\");"

    qt_sql "select append_trailing_char_if_absent('a','c');"
    qt_sql "select append_trailing_char_if_absent('ac','c');"

    qt_sql "select ascii('1');"
    qt_sql "select ascii('a');"
    qt_sql "select ascii('A');"
    qt_sql "select ascii('!');"

    qt_sql "select bit_length(\"abc\");"

    qt_sql "select char_length(\"abc\");"

    qt_sql "select concat(\"a\", \"b\");"
    qt_sql "select concat(\"a\", \"b\", \"c\");"
    qt_sql "select concat(\"a\", null, \"c\");"

    qt_sql "select concat_ws(\"or\", \"d\", \"is\");"
    qt_sql "select concat_ws(NULL, \"d\", \"is\");"
    qt_sql "select concat_ws(\"or\", \"d\", NULL,\"is\");"
    // Nereids does't support array function
    // qt_sql "select concat_ws(\"or\", [\"d\", \"is\"]);"
    // Nereids does't support array function
    // qt_sql "select concat_ws(NULL, [\"d\", \"is\"]);"
    // Nereids does't support array function
    // qt_sql "select concat_ws(\"or\", [\"d\", NULL,\"is\"]);"
    // Nereids does't support array function
    // qt_sql "select concat_ws(\"or\", [\"d\", \"\",\"is\"]);"

    qt_sql "select ends_with(\"Hello doris\", \"doris\");"
    qt_sql "select ends_with(\"Hello doris\", \"Hello\");"

    qt_sql "select find_in_set(\"b\", \"a,b,c\");"
    qt_sql "select find_in_set(\"d\", \"a,b,c\");"
    qt_sql "select find_in_set(null, \"a,b,c\");"
    qt_sql "select find_in_set(\"a\", null);"

    qt_sql "select hex('1');"
    qt_sql "select hex('12');"
    qt_sql "select hex('@');"
    qt_sql "select hex('A');"
    qt_sql "select hex(12);"
    qt_sql "select hex(-1);"
    qt_sql "select hex('hello,doris')"

    qt_sql "select unhex('@');"
    qt_sql "select unhex('68656C6C6F2C646F726973');"
    qt_sql "select unhex('41');"
    qt_sql "select unhex('4142');"
    qt_sql "select unhex('');"
    qt_sql "select unhex(NULL);"

    qt_sql "select instr(\"abc\", \"b\");"
    qt_sql "select instr(\"abc\", \"d\");"
    qt_sql "select instr(\"abc\", null);"
    qt_sql "select instr(null, \"a\");"

    qt_sql "SELECT lcase(\"AbC123\");"
    qt_sql "SELECT lower(\"AbC123\");"

    qt_sql "SELECT initcap(\"AbC123abc abc.abc,?|abc\");"

    qt_sql "select left(\"Hello doris\",5);"
    qt_sql "select right(\"Hello doris\",5);"

    qt_sql "select length(\"abc\");"

    qt_sql "SELECT LOCATE('bar', 'foobarbar');"
    qt_sql "SELECT LOCATE('xbar', 'foobar');"

    qt_sql "SELECT lpad(\"hi\", 5, \"xy\");"
    qt_sql "SELECT lpad(\"hi\", 1, \"xy\");"
    qt_sql "SELECT rpad(\"hi\", 5, \"xy\");"
    qt_sql "SELECT rpad(\"hi\", 1, \"xy\");"

    qt_sql "SELECT ltrim('   ab d');"

    qt_sql "select money_format(17014116);"
    qt_sql "select money_format(1123.456);"
    qt_sql "select money_format(1123.4);"
    qt_sql "select money_format(1.1249);"
    qt_sql_decimal32 "select money_format(cast(concat('1.124', repeat('9', 5)) as DECIMAL(9, 8)));"
    qt_sql_decimal64 "select money_format(cast(concat('1.124', repeat('9', 6)) as DECIMAL(10, 9)));"
    qt_sql_decimal64 "select money_format(cast(concat('1.124', repeat('9', 14)) as DECIMAL(18, 17)));"
    qt_sql_decimal128 "select money_format(cast(concat('1.124', repeat('9', 15)) as DECIMAL(19, 18)));"
    qt_sql_decimal128 "select money_format(cast(concat('1.124', repeat('9', 34)) as DECIMAL(38, 37)));"
    qt_sql_float64 "select money_format(cast(concat('1.124', repeat('9', 35)) as DOUBLE));"
    qt_sql_float64 "select money_format(cast(concat('1.124', repeat('9', 70)) as DOUBLE));"

    qt_sql "select null_or_empty(null);"
    qt_sql "select null_or_empty(\"\");"
    qt_sql "select null_or_empty(\"a\");"

    qt_sql "select not_null_or_empty(null);"
    qt_sql "select not_null_or_empty(\"\");"
    qt_sql "select not_null_or_empty(\"a\");"

    qt_sql "SELECT repeat(\"a\", 3);"
    qt_sql "SELECT repeat(\"a\", -1);"
    qt_sql "SELECT repeat(\"a\", 0);"
    qt_sql "SELECT repeat(\"a\",null);"
    qt_sql "SELECT repeat(null,1);"

    qt_sql "select replace(\"https://doris.apache.org:9090\", \":9090\", \"\");"
    qt_sql "select replace(\"https://doris.apache.org:9090\", \"\", \"new_str\");"

    qt_sql "SELECT REVERSE('hello');"

    qt_sql "select split_part('hello world', ' ', 1)"
    qt_sql "select split_part('hello world', ' ', 2)"
    qt_sql "select split_part('hello world', ' ', 0)"
    qt_sql "select split_part('hello world', ' ', -1)"
    qt_sql "select split_part('hello world', ' ', -2)"
    qt_sql "select split_part('hello world', ' ', -3)"
    qt_sql "select split_part('abc##123###xyz', '##', 0)"
    qt_sql "select split_part('abc##123###xyz', '##', 1)"
    qt_sql "select split_part('abc##123###xyz', '##', 3)"
    qt_sql "select split_part('abc##123###xyz', '##', 5)"
    qt_sql "select split_part('abc##123###xyz', '##', -1)"
    qt_sql "select split_part('abc##123###xyz', '##', -2)"
    qt_sql "select split_part('abc##123###xyz', '##', -4)"

    qt_sql "select starts_with(\"hello world\",\"hello\");"
    qt_sql "select starts_with(\"hello world\",\"world\");"
    qt_sql "select starts_with(\"hello world\",null);"

    qt_sql "select strleft(\"Hello doris\",5);"
    qt_sql "select strright(\"Hello doris\",5);"

    qt_sql "select substring('abc1', 2);"
    qt_sql "select substring('abc1', -2);"
    qt_sql "select substring('abc1', 5);"
    qt_sql "select substring('abc1def', 2, 2);"

    qt_sql "select substr('a',3,1);"
    qt_sql "select substr('a',2,1);"
    qt_sql "select substr('a',1,1);"
    qt_sql "select substr('a',0,1);"
    qt_sql "select substr('a',-1,1);"
    qt_sql "select substr('a',-2,1);"
    qt_sql "select substr('a',-3,1);"

    qt_sql "select sub_replace(\"this is origin str\",\"NEW-STR\",1);"
    qt_sql "select sub_replace(\"doris\",\"***\",1,2);"

    qt_sql "select substring_index(\"hello world\", \" \", 1);"
    qt_sql "select substring_index(\"hello world\", \" \", 2);"
    qt_sql "select substring_index(\"hello world\", \" \", 3);"
    qt_sql "select substring_index(\"hello world\", \" \", -1);"
    qt_sql "select substring_index(\"hello world\", \" \", -2);"
    qt_sql "select substring_index(\"hello world\", \" \", -3);"
    qt_sql "select substring_index(\"prefix__string2\", \"__\", 2);"
    qt_sql "select substring_index(\"prefix__string2\", \"_\", 2);"
    qt_sql "select substring_index(\"prefix_string2\", \"__\", 1);"
    qt_sql "select substring_index(null, \"__\", 1);"
    qt_sql "select substring_index(\"prefix_string\", null, 1);"
    qt_sql "select substring_index(\"prefix_string\", \"_\", null);"
    qt_sql "select substring_index(\"prefix_string\", \"__\", -1);"


    qt_sql "select elt(0, \"hello\", \"doris\");"
    qt_sql "select elt(1, \"hello\", \"doris\");"
    qt_sql "select elt(2, \"hello\", \"doris\");"
    qt_sql "select elt(3, \"hello\", \"doris\");"

    qt_sql "select sub_replace(\"this is origin str\",\"NEW-STR\",1);"
    qt_sql "select sub_replace(\"doris\",\"***\",1,2);"
    sql """ set debug_skip_fold_constant = true;"""
    qt_sub_replace_utf8_sql1 " select sub_replace('你好世界','a',1);"
    qt_sub_replace_utf8_sql2 " select sub_replace('你好世界','ab',1);"
    qt_sub_replace_utf8_sql3 " select sub_replace('你好世界','ab',1,20);"
    qt_sub_replace_utf8_sql4 " select sub_replace('你好世界','abcd我',1,2);"
    qt_sub_replace_utf8_sql5 " select sub_replace('你好世界','a',6);"
    qt_sub_replace_utf8_sql6 " select sub_replace('你好世界','大家',0);"
    qt_sub_replace_utf8_sql7 " select sub_replace('你好世界','大家114514',1,20);"
    qt_sub_replace_utf8_sql8 " select sub_replace('你好世界','大家114514',6,20);"
    qt_sub_replace_utf8_sql9 " select sub_replace('你好世界','大家',4);"
    qt_sub_replace_utf8_sql10 " select sub_replace('你好世界','大家',-1);"
    sql """ set debug_skip_fold_constant = false;"""
    qt_sub_replace_utf8_sql1 " select sub_replace('你好世界','a',1);"
    qt_sub_replace_utf8_sql2 " select sub_replace('你好世界','ab',1);"
    qt_sub_replace_utf8_sql3 " select sub_replace('你好世界','ab',1,20);"
    qt_sub_replace_utf8_sql4 " select sub_replace('你好世界','abcd我',1,2);"
    qt_sub_replace_utf8_sql5 " select sub_replace('你好世界','a',6);"
    qt_sub_replace_utf8_sql6 " select sub_replace('你好世界','大家',0);"
    qt_sub_replace_utf8_sql7 " select sub_replace('你好世界','大家114514',1,20);"
    qt_sub_replace_utf8_sql8 " select sub_replace('你好世界','大家114514',6,20);"
    qt_sub_replace_utf8_sql9 " select sub_replace('你好世界','大家',4);"
    qt_sub_replace_utf8_sql10 " select sub_replace('你好世界','大家',-1);"

}
