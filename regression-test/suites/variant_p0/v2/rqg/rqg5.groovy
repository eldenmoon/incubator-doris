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

import org.apache.doris.regression.util.SqlUtils

suite("rqg5", "p0,nonConcurrent") {
    setFeConfigTemporary([enable_variant_v2: true]) {
        assertTrue(getFeConfig("enable_variant_v2").toBoolean())
        String sqlText = new File(context.dataPath, "rqg5.sql").text
        sqlText = sqlText.replace("parse_to_variant_if_v2(", "parse_to_variant(")
                .replace("variant_decimal_type_if_v2", "decimal(32, 6)")
                .replace("decimalv3(76,56)", "decimalv3(32,6)")
        List<String> sqls = SqlUtils.splitAndGetNonEmptySql(sqlText)
        String exceptions = ""
        sqls.eachWithIndex { String statement, int index ->
            String tag = index == 0 ? "rqg5" : "rqg5_${index + 1}"
            try {
                quickTest(tag, statement, false)
            } catch (Throwable throwable) {
                exceptions += "exception : ${throwable.message}\\nsql is :${statement}\\n"
            }
        }
        if (!exceptions.isEmpty()) {
            throw new IllegalStateException("exceptions : ${exceptions}")
        }
    }
}
