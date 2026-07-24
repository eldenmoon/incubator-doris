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

suite("variant_semantics_snapshot", "nonConcurrent") {
    sql "set default_variant_enable_doc_mode = false"

    sql "DROP TABLE IF EXISTS variant_semantics_snapshot_default_insert"
    sql "DROP TABLE IF EXISTS variant_semantics_snapshot_default_alter"
    sql "DROP TABLE IF EXISTS variant_semantics_snapshot_values"
    sql "DROP TABLE IF EXISTS variant_semantics_snapshot_cast_source"
    sql "DROP TABLE IF EXISTS variant_semantics_snapshot_largeint"
    sql "DROP TABLE IF EXISTS variant_semantics_snapshot_duplicate"
    sql "DROP TABLE IF EXISTS variant_semantics_snapshot_csv_source"
    sql "DROP TABLE IF EXISTS variant_semantics_snapshot_csv_raw"
    sql "DROP TABLE IF EXISTS variant_semantics_snapshot_csv_reload"

    sql """
        CREATE TABLE variant_semantics_snapshot_default_insert (
            id INT NOT NULL,
            v VARIANT NOT NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    test {
        sql "INSERT INTO variant_semantics_snapshot_default_insert (id) VALUES (1)"
        exception "Column has no default value, column=v"
    }

    sql """
        CREATE TABLE variant_semantics_snapshot_default_alter (
            id INT NOT NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO variant_semantics_snapshot_default_alter VALUES (1)"
    test {
        sql "ALTER TABLE variant_semantics_snapshot_default_alter ADD COLUMN v VARIANT NOT NULL"
        exception "Field 'v' doesn't have a default value"
    }

    sql """
        CREATE TABLE variant_semantics_snapshot_values (
            id INT NOT NULL,
            v VARIANT NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // Structured values are parsed explicitly. Rows 10 and 11 intentionally use the normal
    // String-to-Variant conversion to verify that JSON-looking text remains a string root.
    sql """
        INSERT INTO variant_semantics_snapshot_values VALUES
            (1, NULL),
            (2, parse_to_variant('{"present_null":null,"present":1,"array":[{"k":"v"},null]}')),
            (3, parse_to_variant('42')),
            (4, parse_to_variant(
                    CAST(UNHEX('2271756F7465205C2220736C617368205C5C20756E69636F646520E99BAA22')
                    AS STRING))),
            (5, parse_to_variant('{"scientific":1.25e3,"small":1e-6}')),
            (6, parse_to_variant('null')),
            (7, parse_to_variant('{}')),
            (8, parse_to_variant('[]')),
            (9, parse_to_variant('[1,{"k":2},null]')),
            (10, '{"n":99999999999999999999999999999999999999}'),
            (11, '{"n":100000000000000000000000000000000000000}')
    """

    order_qt_jdbc_text """
        SELECT
            id,
            v,
            CAST(v AS STRING) AS cast_text,
            v IS NULL AS value_is_sql_null,
            CAST(v['present_null'] AS STRING) AS json_null_text,
            v['present_null'] IS NULL AS json_null_is_sql_null,
            CAST(v['missing'] AS STRING) AS missing_text,
            v['missing'] IS NULL AS missing_is_sql_null,
            CAST(v['present'] AS STRING) AS present_text,
            variant_type(v) AS type_map
        FROM variant_semantics_snapshot_values
        ORDER BY id
    """

    order_qt_path_types """
        SELECT
            id,
            variant_type(v) AS root_type,
            variant_type(v['present']) AS present_type,
            variant_type(v['array']) AS array_type,
            variant_type(v['present_null']) AS json_null_type,
            variant_type(v['missing']) AS missing_type
        FROM variant_semantics_snapshot_values
        ORDER BY id
    """

    order_qt_json_length """
        SELECT
            id,
            json_length(CAST(v AS JSON)) AS root_length,
            json_length(CAST(v['array'] AS JSON)) AS array_length,
            json_length(CAST(v['present'] AS JSON)) AS scalar_path_length
        FROM variant_semantics_snapshot_values
        WHERE id <= 9
        ORDER BY id
    """

    sql """
        CREATE TABLE variant_semantics_snapshot_cast_source (
            id INT NOT NULL,
            text_value STRING NOT NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO variant_semantics_snapshot_cast_source VALUES
            (1, '{"looks":"json"}'),
            (2, 'plain text')
    """
    order_qt_cast_string_root """
        SELECT
            id,
            CAST(CAST(text_value AS VARIANT) AS STRING) AS variant_text,
            variant_type(CAST(text_value AS VARIANT)) AS type_map
        FROM variant_semantics_snapshot_cast_source
        ORDER BY id
    """

    sql """
        CREATE TABLE variant_semantics_snapshot_largeint (
            id INT NOT NULL,
            v VARIANT NOT NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO variant_semantics_snapshot_largeint VALUES
            (1, CAST(CAST('99999999999999999999999999999999999999' AS LARGEINT) AS VARIANT)),
            (2, CAST(CAST('-99999999999999999999999999999999999999' AS LARGEINT) AS VARIANT))
    """
    sql """
        INSERT INTO variant_semantics_snapshot_largeint VALUES
            (10, CAST(CAST('100000000000000000000000000000000000000' AS LARGEINT) AS VARIANT)),
            (11, CAST(CAST('-100000000000000000000000000000000000000' AS LARGEINT) AS VARIANT)),
            (12, CAST(CAST('170141183460469231731687303715884105727' AS LARGEINT) AS VARIANT)),
            (13, CAST(CAST('-170141183460469231731687303715884105728' AS LARGEINT) AS VARIANT))
    """
    order_qt_largeint """
        SELECT id, v, CAST(v AS STRING) AS cast_text, variant_type(v) AS type_map
        FROM variant_semantics_snapshot_largeint
        ORDER BY id
    """

    sql """
        CREATE TABLE variant_semantics_snapshot_duplicate (
            id INT NOT NULL,
            v VARIANT NOT NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    setBeConfigTemporary([variant_enable_duplicate_json_path_check: false]) {
        test {
            sql """
                INSERT INTO variant_semantics_snapshot_duplicate
                VALUES (1, parse_to_variant('{"dup":1,"dup":2}'))
            """
            exception "Duplicate Variant object key"
        }
    }
    setBeConfigTemporary([variant_enable_duplicate_json_path_check: true]) {
        sql """
            INSERT INTO variant_semantics_snapshot_duplicate
            VALUES (2, parse_to_variant('{"dup":1,"dup":2}'))
        """
        order_qt_duplicate_first_wins """
            SELECT id, v, CAST(v['dup'] AS STRING) AS dup_text, variant_type(v) AS type_map
            FROM variant_semantics_snapshot_duplicate
            ORDER BY id
        """
    }

    sql """
        CREATE TABLE variant_semantics_snapshot_csv_source (
            id INT NOT NULL,
            v VARIANT NOT NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE variant_semantics_snapshot_csv_raw (
            id INT NOT NULL,
            v_raw STRING NOT NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE variant_semantics_snapshot_csv_reload (
            id INT NOT NULL,
            v VARIANT NOT NULL
        )
        ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO variant_semantics_snapshot_csv_source VALUES
            (1, parse_to_variant('{"csv_object":1}')),
            (2, parse_to_variant('"csv text"')),
            (3, parse_to_variant('null'))
    """
    order_qt_csv_source """
        SELECT
            id,
            CASE id WHEN 1 THEN 'object' WHEN 2 THEN 'string' ELSE 'json_null_input' END,
            v,
            CAST(v AS STRING) AS cast_text,
            variant_type(v) AS type_map,
            v IS NULL AS value_is_sql_null
        FROM variant_semantics_snapshot_csv_source
        ORDER BY id
    """

    assertEquals("true", getFeConfig("enable_outfile_to_local").toLowerCase())
    sql "SET enable_parallel_outfile = false"
    sql "SET parallel_pipeline_task_num = 1"

    File outputDir = new File(
            System.getProperty("java.io.tmpdir"),
            "variant_semantics_snapshot_" + UUID.randomUUID().toString())
    if (outputDir.exists()) {
        outputDir.eachFile { file -> assertTrue(file.delete()) }
        assertTrue(outputDir.delete())
    }
    assertTrue(outputDir.mkdirs())

    sql """
        SELECT id, v
        FROM variant_semantics_snapshot_csv_source
        ORDER BY id
        INTO OUTFILE "file://${outputDir.getAbsolutePath()}/"
        FORMAT AS CSV
        PROPERTIES ("column_separator" = "\\\\x01")
    """

    File[] outputFiles = outputDir.listFiles()
    assertNotNull(outputFiles)
    assertEquals(1, outputFiles.length)
    assertTrue(outputFiles[0].isFile())
    assertTrue(outputFiles[0].getName().endsWith(".csv"))
    File csvFile = outputFiles[0]

    def checkLoad = { result, exception, startTime, endTime ->
        if (exception != null) {
            throw exception
        }
        def json = parseJson(result)
        assertEquals("Success", json.Status)
        assertEquals(3, json.NumberTotalRows)
        assertEquals(3, json.NumberLoadedRows)
        assertEquals(0, json.NumberFilteredRows)
        assertEquals(0, json.NumberUnselectedRows)
    }

    streamLoad {
        table "variant_semantics_snapshot_csv_raw"
        set "format", "csv"
        set "column_separator", '\\x01'
        set "strict_mode", "true"
        file csvFile.getAbsolutePath()
        time 10000
        check checkLoad
    }
    sql "SYNC"
    order_qt_csv_raw """
        SELECT
            id,
            CASE id WHEN 1 THEN 'object' WHEN 2 THEN 'string' ELSE 'json_null_input' END,
            v_raw,
            HEX(v_raw)
        FROM variant_semantics_snapshot_csv_raw
        ORDER BY id
    """

    streamLoad {
        table "variant_semantics_snapshot_csv_reload"
        set "format", "csv"
        set "column_separator", '\\x01'
        set "strict_mode", "true"
        file csvFile.getAbsolutePath()
        time 10000
        check checkLoad
    }
    sql "SYNC"
    order_qt_csv_reload """
        SELECT
            id,
            CASE id WHEN 1 THEN 'object' WHEN 2 THEN 'string' ELSE 'json_null_input' END,
            v,
            CAST(v AS STRING) AS cast_text,
            variant_type(v) AS type_map,
            v IS NULL AS value_is_sql_null
        FROM variant_semantics_snapshot_csv_reload
        ORDER BY id
    """
}
