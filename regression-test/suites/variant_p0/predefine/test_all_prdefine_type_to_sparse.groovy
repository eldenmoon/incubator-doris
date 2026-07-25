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

suite("test_all_prdefine_type_to_sparse", "p0"){ 

    sql """ set describe_extend_variant_column = true """
    sql """ set default_variant_enable_doc_mode = false """

    def tableName = "test_all_prdefine_type_to_sparse"
    sql "set enable_decimal256 = true"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
        `id` bigint NOT NULL,
        `var`  variant <
                'boolean_*':boolean,
                'tinyint_*':tinyint,
                'smallint_*':smallint,
                'int_*':int, 
                'bigint_*':bigint,
                'largeint_*':largeint,
                'char_*': text,
                'string_*':string, 
                'float_*':float,
                'double_*':double,
                'decimal32_*':decimalv3(8,2),
                'decimal64_*':decimalv3(16,9),
                'decimal128_*':decimalv3(36,9),
                'decimal256_*':decimalv3(70,60),
                'datetime_*':datetime,
                'date_*':date,
                'ipv4_*':ipv4,
                'ipv6_*':ipv6,
                'array_boolean_*':array<boolean>,
                'array_tinyint_*':array<tinyint>,
                'array_smallint_*':array<smallint>,
                'array_int_*':array<int>,
                'array_bigint_*':array<bigint>,
                'array_largeint_*':array<largeint>,
                'array_char_*':array<text>,
                'array_string_*':array<string>,
                'array_float_*':array<float>,
                'array_double_*':array<double>,
                'array_decimal32_*':array<decimalv3(8,2)>,
                'array_decimal64_*':array<decimalv3(16,9)>,
                'array_decimal128_*':array<decimalv3(36,9)>,
                'array_decimal256_*':array<decimalv3(70,60)>,
                'array_datetime_*':array<datetime>,
                'array_date_*':array<date>,
                'array_ipv4_*':array<ipv4>,
                'array_ipv6_*':array<ipv6>,
                properties (
                    "variant_enable_typed_paths_to_sparse" = "true",
                    "variant_max_subcolumns_count" = "1",
                    "variant_sparse_hash_shard_count" = "3"
                )
            > NOT NULL
    ) ENGINE=OLAP DUPLICATE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1", "disable_auto_compaction" = "true")

    """

    sql """
         INSERT INTO ${tableName} VALUES
        (0,
            parse_to_variant('{
              "boolean_1": true,
              "tinyint_1": 1,
              "smallint_1": 1,
              "int_1": 1,
              "bigint_1": 1,
              "largeint_1": 1,
              "char_1": "1",
              "string_1": "1",
              "float_1": 1.12,
              "double_1": 1.12,
              "decimal32_1": 1.12,
              "decimal64_1": 1.12,
              "decimal128_1": 1.12,
              "decimal256_1": 1.12,
              "datetime_1": "2021-01-01 00:00:00",
              "date_1": "2021-01-01",
              "ipv4_1": "192.168.1.1",
              "ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
              "array_boolean_1": [true],
              "array_tinyint_1": [1, null],
              "array_smallint_1": [1, null],
              "array_int_1": [1, null],
              "array_bigint_1": [1, null],
              "array_largeint_1": [1, null],
              "array_char_1": ["1"],
              "array_string_1": ["1"],
              "array_float_1": [1.12],
              "array_double_1": [1.12],
              "array_decimal32_1": [1.12],
              "array_decimal64_1": [1.12],
              "array_decimal128_1": [1.12],
              "array_decimal256_1": [1.12],
              "array_datetime_1": ["2021-01-01 00:00:00"],
              "array_date_1": ["2021-01-01"],
              "array_ipv4_1": ["192.168.1.1"],
              "array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"],
              "other_1": "1"
            }')
        ),
        (1,
            parse_to_variant('{"other_1": "1"}')
        ); 
    """

    qt_sql """ select variant_type(var) from ${tableName} limit 1"""
    qt_sql """ select var from ${tableName} order by id """


    def check_table = {
        def before_result = sql """ select var from ${tableName} order by id """
        log.info("before_result: ${before_result}")
        qt_sql_compaction_before """ desc ${tableName} """

        trigger_and_wait_compaction(tableName, "full", 1800)

        def after_result = sql """ select var from ${tableName} order by id """
        log.info("after_result: ${after_result}")
        assertTrue(before_result.toString() == after_result.toString())
        
        qt_sql_compaction_after """ desc ${tableName} """
        qt_sql """ select var from ${tableName} order by id """
    }

    sql """ insert into ${tableName} values (2, parse_to_variant('{"tinyint_1": 1}')),(3, parse_to_variant('{"tinyint_1": 2}')); """

    check_table();

    sql """ insert into ${tableName} values (4, parse_to_variant('{"smallint_1": 1}')),(5, parse_to_variant('{"smallint_1": 2}')),(6, parse_to_variant('{"smallint_1": 3}')); """

    check_table();

    sql """ insert into ${tableName}  values (7, parse_to_variant('{"int_1": 1}')),(8, parse_to_variant('{"int_1": 2}')),(9, parse_to_variant('{"int_1": 3}')),(10, parse_to_variant('{"int_1": 4}')); """

    check_table();

    sql """ insert into ${tableName}  values (11, parse_to_variant('{"bigint_1": 1}')),(12, parse_to_variant('{"bigint_1": 2}')),(13, parse_to_variant('{"bigint_1": 3}')),(14, parse_to_variant('{"bigint_1": 4}')),(15, parse_to_variant('{"bigint_1": 5}')); """

    check_table();

    sql """ insert into ${tableName}  values (16, parse_to_variant('{"largeint_1": 1}')),(17, parse_to_variant('{"largeint_1": 2}')),(18, parse_to_variant('{"largeint_1": 3}')),(19, parse_to_variant('{"largeint_1": 4}')),(20, parse_to_variant('{"largeint_1": 5}')),(21, parse_to_variant('{"largeint_1": 6}')); """

    check_table();

    sql """ insert into ${tableName}  values (22, parse_to_variant('{"char_1": "1"}')),(23, parse_to_variant('{"char_1": "2"}')),(24, parse_to_variant('{"char_1": "3"}')),(25, parse_to_variant('{"char_1": "4"}')),(26, parse_to_variant('{"char_1": "5"}')),(27, parse_to_variant('{"char_1": "6"}')),(28, parse_to_variant('{"char_1": "7"}')); """

    check_table();

    sql """ insert into ${tableName}  values (29, parse_to_variant('{"string_1": "1"}')),(30, parse_to_variant('{"string_1": "2"}')),(31, parse_to_variant('{"string_1": "3"}')),(32, parse_to_variant('{"string_1": "4"}')),(33, parse_to_variant('{"string_1": "5"}')),
    (34, parse_to_variant('{"string_1": "6"}')),(35, parse_to_variant('{"string_1": "7"}')),(36, parse_to_variant('{"string_1": "8"}')); """

    check_table();

    sql """ insert into ${tableName}  values (37, parse_to_variant('{"float_1": 1.12}')),(38, parse_to_variant('{"float_1": 2.12}')),(39, parse_to_variant('{"float_1": 3.12}')),(40, parse_to_variant('{"float_1": 4.12}')),(41, parse_to_variant('{"float_1": 5.12}')),
    (42, parse_to_variant('{"float_1": 6.12}')),(43, parse_to_variant('{"float_1": 7.12}')),(44, parse_to_variant('{"float_1": 8.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (45, parse_to_variant('{"double_1": 1.12}')),(46, parse_to_variant('{"double_1": 2.12}')),(47, parse_to_variant('{"double_1": 3.12}')),(48, parse_to_variant('{"double_1": 4.12}')),(49, parse_to_variant('{"double_1": 5.12}')),
    (50, parse_to_variant('{"double_1": 6.12}')),(51, parse_to_variant('{"double_1": 7.12}')),(52, parse_to_variant('{"double_1": 8.12}')),(53, parse_to_variant('{"double_1": 9.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (54, parse_to_variant('{"decimal32_1": 1.12}')),(55, parse_to_variant('{"decimal32_1": 2.12}')),(56, parse_to_variant('{"decimal32_1": 3.12}')),(57, parse_to_variant('{"decimal32_1": 4.12}')),(58, parse_to_variant('{"decimal32_1": 5.12}')),
    (59, parse_to_variant('{"decimal32_1": 6.12}')),(60, parse_to_variant('{"decimal32_1": 7.12}')),(61, parse_to_variant('{"decimal32_1": 8.12}')),(62, parse_to_variant('{"decimal32_1": 9.12}')),(63, parse_to_variant('{"decimal32_1": 10.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (64, parse_to_variant('{"decimal64_1": 1.12}')),(65, parse_to_variant('{"decimal64_1": 2.12}')),(66, parse_to_variant('{"decimal64_1": 3.12}')),(67, parse_to_variant('{"decimal64_1": 4.12}')),(68, parse_to_variant('{"decimal64_1": 5.12}')),
    (69, parse_to_variant('{"decimal64_1": 6.12}')),(70, parse_to_variant('{"decimal64_1": 7.12}')),(71, parse_to_variant('{"decimal64_1": 8.12}')),(72, parse_to_variant('{"decimal64_1": 9.12}')),(73, parse_to_variant('{"decimal64_1": 10.12}')),(74, parse_to_variant('{"decimal64_1": 11.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (75, parse_to_variant('{"decimal128_1": 1.12}')),(76, parse_to_variant('{"decimal128_1": 2.12}')),(77, parse_to_variant('{"decimal128_1": 3.12}')),(78, parse_to_variant('{"decimal128_1": 4.12}')),(79, parse_to_variant('{"decimal128_1": 5.12}')),
    (80, parse_to_variant('{"decimal128_1": 6.12}')),(81, parse_to_variant('{"decimal128_1": 7.12}')),(82, parse_to_variant('{"decimal128_1": 8.12}')),(83, parse_to_variant('{"decimal128_1": 9.12}')),(84, parse_to_variant('{"decimal128_1": 10.12}')),(85, parse_to_variant('{"decimal128_1": 11.12}')),
    (86, parse_to_variant('{"decimal128_1": 12.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (87, parse_to_variant('{"decimal256_1": 1.12}')),(88, parse_to_variant('{"decimal256_1": 2.12}')),(89, parse_to_variant('{"decimal256_1": 3.12}')),(90, parse_to_variant('{"decimal256_1": 4.12}')),(91, parse_to_variant('{"decimal256_1": 5.12}')),
    (92, parse_to_variant('{"decimal256_1": 6.12}')),(93, parse_to_variant('{"decimal256_1": 7.12}')),(94, parse_to_variant('{"decimal256_1": 8.12}')),(95, parse_to_variant('{"decimal256_1": 9.12}')),(96, parse_to_variant('{"decimal256_1": 10.12}')),(97, parse_to_variant('{"decimal256_1": 11.12}')),
    (98, parse_to_variant('{"decimal256_1": 12.12}')),(99, parse_to_variant('{"decimal256_1": 13.12}')); """

    check_table();

    sql """ insert into ${tableName}  values (100, parse_to_variant('{"datetime_1": "2021-01-01 00:00:00"}')),(101, parse_to_variant('{"datetime_1": "2021-01-01 00:00:01"}')),(102, parse_to_variant('{"datetime_1": "2021-01-01 00:00:02"}')),
    (103, parse_to_variant('{"datetime_1": "2021-01-01 00:00:03"}')),(104, parse_to_variant('{"datetime_1": "2021-01-01 00:00:04"}')),(105, parse_to_variant('{"datetime_1": "2021-01-01 00:00:05"}')),(106, parse_to_variant('{"datetime_1": "2021-01-01 00:00:06"}')),
    (107, parse_to_variant('{"datetime_1": "2021-01-01 00:00:07"}')),(108, parse_to_variant('{"datetime_1": "2021-01-01 00:00:08"}')),(109, parse_to_variant('{"datetime_1": "2021-01-01 00:00:09"}')),(110, parse_to_variant('{"datetime_1": "2021-01-01 00:00:10"}')),
    (111, parse_to_variant('{"datetime_1": "2021-01-01 00:00:07"}')),(112, parse_to_variant('{"datetime_1": "2021-01-01 00:00:08"}')); """

    check_table();

    sql """ insert into ${tableName}  values (113, parse_to_variant('{"date_1": "2021-01-01"}')),(114, parse_to_variant('{"date_1": "2021-01-02"}')),(115, parse_to_variant('{"date_1": "2021-01-03"}')),(116, parse_to_variant('{"date_1": "2021-01-04"}')),
    (117, parse_to_variant('{"date_1": "2021-01-05"}')),(118, parse_to_variant('{"date_1": "2021-01-06"}')),(119, parse_to_variant('{"date_1": "2021-01-07"}')),(120, parse_to_variant('{"date_1": "2021-01-08"}')),(121, parse_to_variant('{"date_1": "2021-01-09"}')),(122, parse_to_variant('{"date_1": "2021-01-10"}')),
    (123, parse_to_variant('{"date_1": "2021-01-07"}')),(124, parse_to_variant('{"date_1": "2021-01-08"}')),(125, parse_to_variant('{"date_1": "2021-01-09"}')),(126, parse_to_variant('{"date_1": "2021-01-10"}')); """

    check_table();

    sql """ insert into ${tableName}  values (127, parse_to_variant('{"ipv4_1": "192.168.1.1"}')),(128, parse_to_variant('{"ipv4_1": "192.168.1.2"}')),(129, parse_to_variant('{"ipv4_1": "192.168.1.3"}')),(130, parse_to_variant('{"ipv4_1": "192.168.1.4"}')),
    (131, parse_to_variant('{"ipv4_1": "192.168.1.5"}')),(132, parse_to_variant('{"ipv4_1": "192.168.1.6"}')),(133, parse_to_variant('{"ipv4_1": "192.168.1.7"}')),(134, parse_to_variant('{"ipv4_1": "192.168.1.8"}')),(135, parse_to_variant('{"ipv4_1": "192.168.1.9"}')),(136, parse_to_variant('{"ipv4_1": "192.168.1.10"}')),
    (137, parse_to_variant('{"ipv4_1": "192.168.1.7"}')),(138, parse_to_variant('{"ipv4_1": "192.168.1.8"}')),(139, parse_to_variant('{"ipv4_1": "192.168.1.9"}')),(140, parse_to_variant('{"ipv4_1": "192.168.1.10"}')),(141, parse_to_variant('{"ipv4_1": "192.168.1.11"}')); """

    check_table();

    sql """ insert into ${tableName}  values (142, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7334"}')),(143, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7335"}')),
    (144, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7336"}')),(145, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7337"}')),(146, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7338"}')),
    (147, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7339"}')),(148, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733a"}')),(149, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733b"}')),
    (150, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733c"}')),(151, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733d"}')),(152, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733e"}')),
    (153, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733f"}')),(154, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7340"}')),(155, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7341"}')),
    (156, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:733f"}')),(157, parse_to_variant('{"ipv6_1": "2001:0db8:85a3:0000:0000:8a2e:0370:7340"}')); """

    check_table();

    sql """ insert into ${tableName}  values (158, parse_to_variant('{"array_boolean_1": [true]}')),(159, parse_to_variant('{"array_boolean_1": [false]}')),(160, parse_to_variant('{"array_boolean_1": [true]}')),(161, parse_to_variant('{"array_boolean_1": [false]}')),
    (162, parse_to_variant('{"array_boolean_1": [true]}')),(163, parse_to_variant('{"array_boolean_1": [false]}')),(164, parse_to_variant('{"array_boolean_1": [true]}')),(165, parse_to_variant('{"array_boolean_1": [false]}')),(166, parse_to_variant('{"array_boolean_1": [true]}')),(167, parse_to_variant('{"array_boolean_1": [false]}')),
    (168, parse_to_variant('{"array_boolean_1": [true]}')),(169, parse_to_variant('{"array_boolean_1": [false]}')),(170, parse_to_variant('{"array_boolean_1": [true]}')),(171, parse_to_variant('{"array_boolean_1": [false]}')),(172, parse_to_variant('{"array_boolean_1": [true]}')),(173, parse_to_variant('{"array_boolean_1": [false]}'));"""

    check_table();

    sql """ insert into ${tableName}  values (174, parse_to_variant('{"array_tinyint_1": [1]}')),(175, parse_to_variant('{"array_tinyint_1": [2]}')),(176, parse_to_variant('{"array_tinyint_1": [3]}')),(177, parse_to_variant('{"array_tinyint_1": [4]}')),
    (178, parse_to_variant('{"array_tinyint_1": [5]}')),(179, parse_to_variant('{"array_tinyint_1": [6]}')),(180, parse_to_variant('{"array_tinyint_1": [7]}')),(181, parse_to_variant('{"array_tinyint_1": [8]}')),(182, parse_to_variant('{"array_tinyint_1": [9]}')),(183, parse_to_variant('{"array_tinyint_1": [10]}')),
    (184, parse_to_variant('{"array_tinyint_1": [11]}')),(185, parse_to_variant('{"array_tinyint_1": [12]}')),(186, parse_to_variant('{"array_tinyint_1": [13]}')),(187, parse_to_variant('{"array_tinyint_1": [14]}')),(188, parse_to_variant('{"array_tinyint_1": [15]}')),(189, parse_to_variant('{"array_tinyint_1": [16]}')),
    (190, parse_to_variant('{"array_tinyint_1": [17]}')),(191, parse_to_variant('{"array_tinyint_1": [18]}')); """

    check_table();
    
    sql """ insert into ${tableName}  values (192, parse_to_variant('{"array_smallint_1": [1]}')),(193, parse_to_variant('{"array_smallint_1": [2, null]}')),(194, parse_to_variant('{"array_smallint_1": [3]}')),(195, parse_to_variant('{"array_smallint_1": [4]}')),
    (196, parse_to_variant('{"array_smallint_1": [5]}')),(197, parse_to_variant('{"array_smallint_1": [6]}')),(198, parse_to_variant('{"array_smallint_1": [7]}')),(199, parse_to_variant('{"array_smallint_1": [8]}')),(200, parse_to_variant('{"array_smallint_1": [9]}')),(201, parse_to_variant('{"array_smallint_1": [10]}')),
    (202, parse_to_variant('{"array_smallint_1": [11]}')),(203, parse_to_variant('{"array_smallint_1": [12]}')),(204, parse_to_variant('{"array_smallint_1": [13]}')),(205, parse_to_variant('{"array_smallint_1": [14]}')),(206, parse_to_variant('{"array_smallint_1": [15]}')),(207, parse_to_variant('{"array_smallint_1": [16]}')),
    (208, parse_to_variant('{"array_smallint_1": [17]}')),(209, parse_to_variant('{"array_smallint_1": [18]}')),(210, parse_to_variant('{"array_smallint_1": [19]}')); """

    check_table();

    sql """ insert into ${tableName}  values (211, parse_to_variant('{"array_int_1": [1]}')),(212, parse_to_variant('{"array_int_1": [2]}')),(213, parse_to_variant('{"array_int_1": [3]}')),(214, parse_to_variant('{"array_int_1": [4]}')),
    (215, parse_to_variant('{"array_int_1": [5]}')),(216, parse_to_variant('{"array_int_1": [6]}')),(217, parse_to_variant('{"array_int_1": [7]}')),(218, parse_to_variant('{"array_int_1": [8]}')),(219, parse_to_variant('{"array_int_1": [9]}')),(220, parse_to_variant('{"array_int_1": [10]}')),
    (221, parse_to_variant('{"array_int_1": [11]}')),(222, parse_to_variant('{"array_int_1": [12]}')),(223, parse_to_variant('{"array_int_1": [13]}')),(224, parse_to_variant('{"array_int_1": [14]}')),(225, parse_to_variant('{"array_int_1": [15]}')),(226, parse_to_variant('{"array_int_1": [16]}')),
    (227, parse_to_variant('{"array_int_1": [17]}')),(228, parse_to_variant('{"array_int_1": [18]}')),(229, parse_to_variant('{"array_int_1": [19]}')),(230, parse_to_variant('{"array_int_1": [20]}')); """

    check_table();

    sql """ insert into ${tableName}  values (231, parse_to_variant('{"array_bigint_1": [1]}')),(232, parse_to_variant('{"array_bigint_1": [2]}')),(233, parse_to_variant('{"array_bigint_1": [3]}')),(234, parse_to_variant('{"array_bigint_1": [4]}')),
    (235, parse_to_variant('{"array_bigint_1": [5]}')),(236, parse_to_variant('{"array_bigint_1": [6]}')),(237, parse_to_variant('{"array_bigint_1": [7]}')),(238, parse_to_variant('{"array_bigint_1": [8]}')),(239, parse_to_variant('{"array_bigint_1": [9]}')),(240, parse_to_variant('{"array_bigint_1": [10]}')),
    (241, parse_to_variant('{"array_bigint_1": [11]}')),(242, parse_to_variant('{"array_bigint_1": [12]}')),(243, parse_to_variant('{"array_bigint_1": [13]}')),(244, parse_to_variant('{"array_bigint_1": [14]}')),(245, parse_to_variant('{"array_bigint_1": [15]}')),(246, parse_to_variant('{"array_bigint_1": [16]}')),
    (247, parse_to_variant('{"array_bigint_1": [17]}')),(248, parse_to_variant('{"array_bigint_1": [18]}')),(249, parse_to_variant('{"array_bigint_1": [19]}')),(250, parse_to_variant('{"array_bigint_1": [20]}')),(251, parse_to_variant('{"array_bigint_1": [21]}')); """

    check_table();

    sql """ insert into ${tableName}  values (252, parse_to_variant('{"array_largeint_1": [1, null]}')),(253, parse_to_variant('{"array_largeint_1": [2]}')),(254, parse_to_variant('{"array_largeint_1": [3]}')),(255, parse_to_variant('{"array_largeint_1": [4]}')),
    (256, parse_to_variant('{"array_largeint_1": [5]}')),(257, parse_to_variant('{"array_largeint_1": [6]}')),(258, parse_to_variant('{"array_largeint_1": [7]}')),(259, parse_to_variant('{"array_largeint_1": [8]}')),(260, parse_to_variant('{"array_largeint_1": [9]}')),(261, parse_to_variant('{"array_largeint_1": [10]}')),
    (262, parse_to_variant('{"array_largeint_1": [11]}')),(263, parse_to_variant('{"array_largeint_1": [12]}')),(264, parse_to_variant('{"array_largeint_1": [13]}')),(265, parse_to_variant('{"array_largeint_1": [14]}')),(266, parse_to_variant('{"array_largeint_1": [15]}')),(267, parse_to_variant('{"array_largeint_1": [16]}')),
    (268, parse_to_variant('{"array_largeint_1": [17]}')),(269, parse_to_variant('{"array_largeint_1": [18]}')),(270, parse_to_variant('{"array_largeint_1": [19]}')),(271, parse_to_variant('{"array_largeint_1": [20]}')),(272, parse_to_variant('{"array_largeint_1": [21]}')),(273, parse_to_variant('{"array_largeint_1": [22]}')); """

    check_table();

    sql """ insert into ${tableName}  values (274, parse_to_variant('{"array_char_1": ["1"]}')),(275, parse_to_variant('{"array_char_1": ["2"]}')),(276, parse_to_variant('{"array_char_1": ["3"]}')),(277, parse_to_variant('{"array_char_1": ["4"]}')),
    (278, parse_to_variant('{"array_char_1": ["5"]}')),(279, parse_to_variant('{"array_char_1": ["6"]}')),(280, parse_to_variant('{"array_char_1": ["7"]}')),(281, parse_to_variant('{"array_char_1": ["8"]}')),(282, parse_to_variant('{"array_char_1": ["9"]}')),(283, parse_to_variant('{"array_char_1": ["10"]}')),
    (284, parse_to_variant('{"array_char_1": ["11"]}')),(285, parse_to_variant('{"array_char_1": ["12"]}')),(286, parse_to_variant('{"array_char_1": ["13"]}')),(287, parse_to_variant('{"array_char_1": ["14"]}')),(288, parse_to_variant('{"array_char_1": ["15"]}')),(289, parse_to_variant('{"array_char_1": ["16"]}')),
    (290, parse_to_variant('{"array_char_1": ["17"]}')),(291, parse_to_variant('{"array_char_1": ["18"]}')),(292, parse_to_variant('{"array_char_1": ["19"]}')),(293, parse_to_variant('{"array_char_1": ["20"]}')),(294, parse_to_variant('{"array_char_1": ["21"]}')),(295, parse_to_variant('{"array_char_1": ["22"]}')),
    (296, parse_to_variant('{"array_char_1": ["23"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (297, parse_to_variant('{"array_string_1": ["1"]}')),(298, parse_to_variant('{"array_string_1": ["2"]}')),(299, parse_to_variant('{"array_string_1": ["3"]}')),(300, parse_to_variant('{"array_string_1": ["4"]}')),
    (301, parse_to_variant('{"array_string_1": ["5"]}')),(302, parse_to_variant('{"array_string_1": ["6"]}')),(303, parse_to_variant('{"array_string_1": ["7"]}')),(304, parse_to_variant('{"array_string_1": ["8"]}')),(305, parse_to_variant('{"array_string_1": ["9"]}')),(306, parse_to_variant('{"array_string_1": ["10"]}')),
    (307, parse_to_variant('{"array_string_1": ["11"]}')),(308, parse_to_variant('{"array_string_1": ["12"]}')),(309, parse_to_variant('{"array_string_1": ["13"]}')),(310, parse_to_variant('{"array_string_1": ["14"]}')),(311, parse_to_variant('{"array_string_1": ["15"]}')),(312, parse_to_variant('{"array_string_1": ["16"]}')),
    (313, parse_to_variant('{"array_string_1": ["17"]}')),(314, parse_to_variant('{"array_string_1": ["18"]}')),(315, parse_to_variant('{"array_string_1": ["19"]}')),(316, parse_to_variant('{"array_string_1": ["20"]}')),(317, parse_to_variant('{"array_string_1": ["21"]}')),(318, parse_to_variant('{"array_string_1": ["22"]}')),
    (319, parse_to_variant('{"array_string_1": ["23"]}')),(320, parse_to_variant('{"array_string_1": ["24"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (321, parse_to_variant('{"array_float_1": [1.12]}')),(322, parse_to_variant('{"array_float_1": [2.12]}')),(323, parse_to_variant('{"array_float_1": [3.12]}')),(324, parse_to_variant('{"array_float_1": [4.12]}')),
    (325, parse_to_variant('{"array_float_1": [5.12]}')),(326, parse_to_variant('{"array_float_1": [6.12]}')),(327, parse_to_variant('{"array_float_1": [7.12]}')),(328, parse_to_variant('{"array_float_1": [8.12]}')),(329, parse_to_variant('{"array_float_1": [9.12]}')),(330, parse_to_variant('{"array_float_1": [10.12]}')),
    (331, parse_to_variant('{"array_float_1": [11.12]}')),(332, parse_to_variant('{"array_float_1": [12.12]}')),(333, parse_to_variant('{"array_float_1": [13.12]}')),(334, parse_to_variant('{"array_float_1": [14.12]}')),(335, parse_to_variant('{"array_float_1": [15.12]}')),(336, parse_to_variant('{"array_float_1": [16.12]}')),
    (337, parse_to_variant('{"array_float_1": [17.12]}')),(338, parse_to_variant('{"array_float_1": [18.12]}')),(339, parse_to_variant('{"array_float_1": [19.12]}')),(340, parse_to_variant('{"array_float_1": [20.12]}')),(341, parse_to_variant('{"array_float_1": [21.12]}')),(342, parse_to_variant('{"array_float_1": [22.12]}')),
    (343, parse_to_variant('{"array_float_1": [23.12]}')),(344, parse_to_variant('{"array_float_1": [24.12]}')),(345, parse_to_variant('{"array_float_1": [25.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (346, parse_to_variant('{"array_double_1": [1.12]}')),(347, parse_to_variant('{"array_double_1": [2.12]}')),(348, parse_to_variant('{"array_double_1": [3.12]}')),(349, parse_to_variant('{"array_double_1": [4.12]}')),
    (350, parse_to_variant('{"array_double_1": [5.12]}')),(351, parse_to_variant('{"array_double_1": [6.12]}')),(352, parse_to_variant('{"array_double_1": [7.12]}')),(353, parse_to_variant('{"array_double_1": [8.12]}')),(354, parse_to_variant('{"array_double_1": [9.12]}')),(355, parse_to_variant('{"array_double_1": [10.12]}')),
    (356, parse_to_variant('{"array_double_1": [11.12]}')),(357, parse_to_variant('{"array_double_1": [12.12]}')),(358, parse_to_variant('{"array_double_1": [13.12]}')),(359, parse_to_variant('{"array_double_1": [14.12]}')),(360, parse_to_variant('{"array_double_1": [15.12]}')),(361, parse_to_variant('{"array_double_1": [16.12]}')),
    (362, parse_to_variant('{"array_double_1": [17.12]}')),(363, parse_to_variant('{"array_double_1": [18.12]}')),(364, parse_to_variant('{"array_double_1": [19.12]}')),(365, parse_to_variant('{"array_double_1": [20.12]}')),(366, parse_to_variant('{"array_double_1": [21.12]}')),(367, parse_to_variant('{"array_double_1": [22.12]}')),
    (368, parse_to_variant('{"array_double_1": [23.12]}')),(369, parse_to_variant('{"array_double_1": [24.12]}')),(370, parse_to_variant('{"array_double_1": [25.12]}')),(371, parse_to_variant('{"array_double_1": [26.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (372, parse_to_variant('{"array_decimal32_1": [1.12]}')),(373, parse_to_variant('{"array_decimal32_1": [2.12]}')),(374, parse_to_variant('{"array_decimal32_1": [3.12]}')),(375, parse_to_variant('{"array_decimal32_1": [4.12]}')),
    (376, parse_to_variant('{"array_decimal32_1": [5.12]}')),(377, parse_to_variant('{"array_decimal32_1": [6.12]}')),(378, parse_to_variant('{"array_decimal32_1": [7.12]}')),(379, parse_to_variant('{"array_decimal32_1": [8.12]}')),(380, parse_to_variant('{"array_decimal32_1": [9.12]}')),(381, parse_to_variant('{"array_decimal32_1": [10.12]}')),
    (382, parse_to_variant('{"array_decimal32_1": [11.12]}')),(383, parse_to_variant('{"array_decimal32_1": [12.12]}')),(384, parse_to_variant('{"array_decimal32_1": [13.12]}')),(385, parse_to_variant('{"array_decimal32_1": [14.12]}')),(386, parse_to_variant('{"array_decimal32_1": [15.12]}')),(387, parse_to_variant('{"array_decimal32_1": [16.12]}')),
    (388, parse_to_variant('{"array_decimal32_1": [17.12]}')),(389, parse_to_variant('{"array_decimal32_1": [18.12]}')),(390, parse_to_variant('{"array_decimal32_1": [19.12]}')),(391, parse_to_variant('{"array_decimal32_1": [20.12]}')),(392, parse_to_variant('{"array_decimal32_1": [21.12]}')),(393, parse_to_variant('{"array_decimal32_1": [22.12]}')),
    (394, parse_to_variant('{"array_decimal32_1": [23.12]}')),(395, parse_to_variant('{"array_decimal32_1": [24.12]}')),(396, parse_to_variant('{"array_decimal32_1": [25.12]}')),(397, parse_to_variant('{"array_decimal32_1": [26.12]}')),(398, parse_to_variant('{"array_decimal32_1": [27.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (399, parse_to_variant('{"array_decimal64_1": [1.12]}')),(400, parse_to_variant('{"array_decimal64_1": [2.12]}')),(401, parse_to_variant('{"array_decimal64_1": [3.12]}')),(402, parse_to_variant('{"array_decimal64_1": [4.12]}')),
    (403, parse_to_variant('{"array_decimal64_1": [5.12]}')),(404, parse_to_variant('{"array_decimal64_1": [6.12]}')),(405, parse_to_variant('{"array_decimal64_1": [7.12]}')),(406, parse_to_variant('{"array_decimal64_1": [8.12]}')),(407, parse_to_variant('{"array_decimal64_1": [9.12]}')),(408, parse_to_variant('{"array_decimal64_1": [10.12]}')),
    (409, parse_to_variant('{"array_decimal64_1": [11.12]}')),(410, parse_to_variant('{"array_decimal64_1": [12.12]}')),(411, parse_to_variant('{"array_decimal64_1": [13.12]}')),(412, parse_to_variant('{"array_decimal64_1": [14.12]}')),(413, parse_to_variant('{"array_decimal64_1": [15.12]}')),(414, parse_to_variant('{"array_decimal64_1": [16.12]}')),
    (415, parse_to_variant('{"array_decimal64_1": [17.12]}')),(416, parse_to_variant('{"array_decimal64_1": [18.12]}')),(417, parse_to_variant('{"array_decimal64_1": [19.12]}')),(418, parse_to_variant('{"array_decimal64_1": [20.12]}')),(419, parse_to_variant('{"array_decimal64_1": [21.12]}')),(420, parse_to_variant('{"array_decimal64_1": [22.12]}')),
    (421, parse_to_variant('{"array_decimal64_1": [23.12]}')),(422, parse_to_variant('{"array_decimal64_1": [24.12]}')),(423, parse_to_variant('{"array_decimal64_1": [25.12]}')),(424, parse_to_variant('{"array_decimal64_1": [26.12]}')),(425, parse_to_variant('{"array_decimal64_1": [27.12]}')),(426, parse_to_variant('{"array_decimal64_1": [28.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (427, parse_to_variant('{"array_decimal128_1": [1.12]}')),(428, parse_to_variant('{"array_decimal128_1": [2.12]}')),(429, parse_to_variant('{"array_decimal128_1": [3.12]}')),(430, parse_to_variant('{"array_decimal128_1": [4.12]}')),
    (431, parse_to_variant('{"array_decimal128_1": [5.12]}')),(432, parse_to_variant('{"array_decimal128_1": [6.12]}')),(433, parse_to_variant('{"array_decimal128_1": [7.12]}')),(434, parse_to_variant('{"array_decimal128_1": [8.12]}')),(435, parse_to_variant('{"array_decimal128_1": [9.12]}')),(436, parse_to_variant('{"array_decimal128_1": [10.12]}')),
    (437, parse_to_variant('{"array_decimal128_1": [11.12]}')),(438, parse_to_variant('{"array_decimal128_1": [12.12]}')),(439, parse_to_variant('{"array_decimal128_1": [13.12]}')),(440, parse_to_variant('{"array_decimal128_1": [14.12]}')),(441, parse_to_variant('{"array_decimal128_1": [15.12]}')),(442, parse_to_variant('{"array_decimal128_1": [16.12]}')),
    (443, parse_to_variant('{"array_decimal128_1": [17.12]}')),(444, parse_to_variant('{"array_decimal128_1": [18.12]}')),(445, parse_to_variant('{"array_decimal128_1": [19.12]}')),(446, parse_to_variant('{"array_decimal128_1": [20.12]}')),(447, parse_to_variant('{"array_decimal128_1": [21.12]}')),(448, parse_to_variant('{"array_decimal128_1": [22.12]}')),
    (449, parse_to_variant('{"array_decimal128_1": [23.12]}')),(450, parse_to_variant('{"array_decimal128_1": [24.12]}')),(451, parse_to_variant('{"array_decimal128_1": [25.12]}')),(452, parse_to_variant('{"array_decimal128_1": [26.12]}')),(453, parse_to_variant('{"array_decimal128_1": [27.12]}')),(454, parse_to_variant('{"array_decimal128_1": [28.12]}')),
    (455, parse_to_variant('{"array_decimal128_1": [29.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (456, parse_to_variant('{"array_decimal256_1": [1.12]}')),(457, parse_to_variant('{"array_decimal256_1": [2.12]}')),(458, parse_to_variant('{"array_decimal256_1": [3.12]}')),(459, parse_to_variant('{"array_decimal256_1": [4.12]}')),
    (460, parse_to_variant('{"array_decimal256_1": [5.12]}')),(461, parse_to_variant('{"array_decimal256_1": [6.12]}')),(462, parse_to_variant('{"array_decimal256_1": [7.12]}')),(463, parse_to_variant('{"array_decimal256_1": [8.12]}')),(464, parse_to_variant('{"array_decimal256_1": [9.12]}')),(465, parse_to_variant('{"array_decimal256_1": [10.12]}')),
    (466, parse_to_variant('{"array_decimal256_1": [11.12]}')),(467, parse_to_variant('{"array_decimal256_1": [12.12]}')),(468, parse_to_variant('{"array_decimal256_1": [13.12]}')),(469, parse_to_variant('{"array_decimal256_1": [14.12]}')),(470, parse_to_variant('{"array_decimal256_1": [15.12]}')),(471, parse_to_variant('{"array_decimal256_1": [16.12]}')),
    (472, parse_to_variant('{"array_decimal256_1": [17.12]}')),(473, parse_to_variant('{"array_decimal256_1": [18.12]}')),(474, parse_to_variant('{"array_decimal256_1": [19.12]}')),(475, parse_to_variant('{"array_decimal256_1": [20.12]}')),(476, parse_to_variant('{"array_decimal256_1": [21.12]}')),(477, parse_to_variant('{"array_decimal256_1": [22.12]}')),
    (478, parse_to_variant('{"array_decimal256_1": [23.12]}')),(479, parse_to_variant('{"array_decimal256_1": [24.12]}')),(480, parse_to_variant('{"array_decimal256_1": [25.12]}')),(481, parse_to_variant('{"array_decimal256_1": [26.12]}')),(482, parse_to_variant('{"array_decimal256_1": [27.12]}')),(483, parse_to_variant('{"array_decimal256_1": [28.12]}')),
    (484, parse_to_variant('{"array_decimal256_1": [29.12]}')),(485, parse_to_variant('{"array_decimal256_1": [30.12]}')); """

    check_table();

    sql """ insert into ${tableName}  values (486, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:00"]}')),(487, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:01"]}')),(488, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:02"]}')),
    (489, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:03"]}')),(490, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:04"]}')),(491, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:05"]}')),(492, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:06"]}')),
    (493, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(494, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(495, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(496, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:10"]}')),
    (497, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(498, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(499, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(500, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:10"]}')),
    (501, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(502, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(503, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(504, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:10"]}')),
    (505, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(506, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(507, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(508, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:10"]}')),
    (509, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(510, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(511, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(512, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:10"]}')),
    (513, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:07"]}')),(514, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:08"]}')),(515, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:09"]}')),(516, parse_to_variant('{"array_datetime_1": ["2021-01-01 00:00:10"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (517, parse_to_variant('{"array_date_1": ["2021-01-01"]}')),(518, parse_to_variant('{"array_date_1": ["2021-01-02"]}')),(519, parse_to_variant('{"array_date_1": ["2021-01-03"]}')),(520, parse_to_variant('{"array_date_1": ["2021-01-04"]}')),
    (521, parse_to_variant('{"array_date_1": ["2021-01-05"]}')),(522, parse_to_variant('{"array_date_1": ["2021-01-06"]}')),(523, parse_to_variant('{"array_date_1": ["2021-01-07"]}')),(524, parse_to_variant('{"array_date_1": ["2021-01-08"]}')),(525, parse_to_variant('{"array_date_1": ["2021-01-09"]}')),(526, parse_to_variant('{"array_date_1": ["2021-01-10"]}')),
    (527, parse_to_variant('{"array_date_1": ["2021-01-07"]}')),(528, parse_to_variant('{"array_date_1": ["2021-01-08"]}')),(529, parse_to_variant('{"array_date_1": ["2021-01-09"]}')),(530, parse_to_variant('{"array_date_1": ["2021-01-10"]}')),
    (531, parse_to_variant('{"array_date_1": ["2021-01-07"]}')),(532, parse_to_variant('{"array_date_1": ["2021-01-08"]}')),(533, parse_to_variant('{"array_date_1": ["2021-01-09"]}')),(534, parse_to_variant('{"array_date_1": ["2021-01-10"]}')),
    (535, parse_to_variant('{"array_date_1": ["2021-01-07"]}')),(536, parse_to_variant('{"array_date_1": ["2021-01-08"]}')),(537, parse_to_variant('{"array_date_1": ["2021-01-09"]}')),(538, parse_to_variant('{"array_date_1": ["2021-01-10"]}')),
    (539, parse_to_variant('{"array_date_1": ["2021-01-07"]}')),(540, parse_to_variant('{"array_date_1": ["2021-01-08"]}')),(541, parse_to_variant('{"array_date_1": ["2021-01-09"]}')),(542, parse_to_variant('{"array_date_1": ["2021-01-10"]}')),
    (543, parse_to_variant('{"array_date_1": ["2021-01-07"]}')),(544, parse_to_variant('{"array_date_1": ["2021-01-08"]}')),(545, parse_to_variant('{"array_date_1": ["2021-01-09"]}')),(546, parse_to_variant('{"array_date_1": ["2021-01-10"]}')),
    (547, parse_to_variant('{"array_date_1": ["2021-01-07"]}')),(548, parse_to_variant('{"array_date_1": ["2021-01-08"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (549, parse_to_variant('{"array_ipv4_1": ["192.168.1.1"]}')),(550, parse_to_variant('{"array_ipv4_1": ["192.168.1.2"]}')),(551, parse_to_variant('{"array_ipv4_1": ["192.168.1.3"]}')),(552, parse_to_variant('{"array_ipv4_1": ["192.168.1.4"]}')),
    (553, parse_to_variant('{"array_ipv4_1": ["192.168.1.5"]}')),(554, parse_to_variant('{"array_ipv4_1": ["192.168.1.6"]}')),(555, parse_to_variant('{"array_ipv4_1": ["192.168.1.7"]}')),(556, parse_to_variant('{"array_ipv4_1": ["192.168.1.8"]}')),(557, parse_to_variant('{"array_ipv4_1": ["192.168.1.9"]}')),(558, parse_to_variant('{"array_ipv4_1": ["192.168.1.10"]}')),
    (559, parse_to_variant('{"array_ipv4_1": ["192.168.1.7"]}')),(560, parse_to_variant('{"array_ipv4_1": ["192.168.1.8"]}')),(561, parse_to_variant('{"array_ipv4_1": ["192.168.1.9"]}')),(562, parse_to_variant('{"array_ipv4_1": ["192.168.1.10"]}')),
    (563, parse_to_variant('{"array_ipv4_1": ["192.168.1.7"]}')),(564, parse_to_variant('{"array_ipv4_1": ["192.168.1.8"]}')),(565, parse_to_variant('{"array_ipv4_1": ["192.168.1.9"]}')),(566, parse_to_variant('{"array_ipv4_1": ["192.168.1.10"]}')),
    (567, parse_to_variant('{"array_ipv4_1": ["192.168.1.7"]}')),(568, parse_to_variant('{"array_ipv4_1": ["192.168.1.8"]}')),(569, parse_to_variant('{"array_ipv4_1": ["192.168.1.9"]}')),(570, parse_to_variant('{"array_ipv4_1": ["192.168.1.10"]}')),
    (571, parse_to_variant('{"array_ipv4_1": ["192.168.1.7"]}')),(572, parse_to_variant('{"array_ipv4_1": ["192.168.1.8"]}')),(573, parse_to_variant('{"array_ipv4_1": ["192.168.1.9"]}')),(574, parse_to_variant('{"array_ipv4_1": ["192.168.1.10"]}')),
    (575, parse_to_variant('{"array_ipv4_1": ["192.168.1.7"]}')),(576, parse_to_variant('{"array_ipv4_1": ["192.168.1.8"]}')),(577, parse_to_variant('{"array_ipv4_1": ["192.168.1.9"]}')),(578, parse_to_variant('{"array_ipv4_1": ["192.168.1.10"]}')),
    (579, parse_to_variant('{"array_ipv4_1": ["192.168.1.7"]}')),(580, parse_to_variant('{"array_ipv4_1": ["192.168.1.8"]}')),(581, parse_to_variant('{"array_ipv4_1": ["192.168.1.9"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (582, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7334"]}')),(583, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7335"]}')),
    (584, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7336"]}')),(585, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7337"]}')),(586, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7338"]}')),
    (587, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7339"]}')),(588, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733a"]}')),(589, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733b"]}')),
    (590, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733c"]}')),(591, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733d"]}')),(592, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733e"]}')),
    (593, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(594, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(595, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (596, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(597, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(598, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (599, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(600, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(601, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (602, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(603, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(604, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (605, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(606, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(607, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (608, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(609, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(610, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (611, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(612, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')),(613, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7341"]}')),
    (614, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:733f"]}')),(615, parse_to_variant('{"array_ipv6_1": ["2001:0db8:85a3:0000:0000:8a2e:0370:7340"]}')); """

    check_table();

    sql """ insert into ${tableName}  values (616, parse_to_variant('{"other_1": "1"}')),(617, parse_to_variant('{"other_1": "2"}')),(618, parse_to_variant('{"other_1": "3"}')),(619, parse_to_variant('{"other_1": "4"}')),(620, parse_to_variant('{"other_1": "5"}')),(621, parse_to_variant('{"other_1": "6"}')),(622, parse_to_variant('{"other_1": "7"}')),(623, parse_to_variant('{"other_1": "8"}')),(624, parse_to_variant('{"other_1": "9"}')),(625, parse_to_variant('{"other_1": "10"}')),
    (626, parse_to_variant('{"other_1": "11"}')),(627, parse_to_variant('{"other_1": "12"}')),(628, parse_to_variant('{"other_1": "13"}')),(629, parse_to_variant('{"other_1": "14"}')),(630, parse_to_variant('{"other_1": "15"}')),(631, parse_to_variant('{"other_1": "16"}')),(632, parse_to_variant('{"other_1": "17"}')),(633, parse_to_variant('{"other_1": "18"}')),(634, parse_to_variant('{"other_1": "19"}')),(635, parse_to_variant('{"other_1": "20"}')),
    (636, parse_to_variant('{"other_1": "21"}')),(637, parse_to_variant('{"other_1": "22"}')),(638, parse_to_variant('{"other_1": "23"}')),(639, parse_to_variant('{"other_1": "24"}')),(640, parse_to_variant('{"other_1": "25"}')),(641, parse_to_variant('{"other_1": "26"}')),(642, parse_to_variant('{"other_1": "27"}')),(643, parse_to_variant('{"other_1": "28"}')),(644, parse_to_variant('{"other_1": "29"}')),(645, parse_to_variant('{"other_1": "30"}')),
    (646, parse_to_variant('{"other_1": "31"}')),(647, parse_to_variant('{"other_1": "32"}')),(648, parse_to_variant('{"other_1": "33"}')),(649, parse_to_variant('{"other_1": "34"}')),(650, parse_to_variant('{"other_1": "35"}')); """

    check_table();
}