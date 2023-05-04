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

import org.codehaus.groovy.runtime.IOGroovyMethods

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_cast") {
    def tableName = "test_cast"
    try {
        sql """ DROP TABLE IF EXISTS ${tableName} """
        qt_select_constant """ select cast (381904456594463916032 as decimal(19, 4)), cast (381904456594463916032 as decimalv3(19, 4)); """
        sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
              `a` decimalv3(38,18),
              `b` decimal(27,9)
            ) ENGINE=OLAP
            DUPLICATE KEY(`a`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
        """
        sql """ insert into ${tableName} values(1234567.12345, 1234567.12345) """
        qt_select_default """ SELECT CAST(`a` AS DECIMALV3(27,9)), CAST(`a` AS DECIMAL(27,9)), CAST(`a` AS DECIMALV3(11,5)), CAST(`a` AS DECIMALV3(12,4)), CAST(`a` AS DECIMAL(11,5)), CAST(`a` AS DECIMAL(12,4)), CAST(`b` AS DECIMALV3(27,9)), CAST(`b` AS DECIMAL(27,9)), CAST(`b` AS DECIMALV3(11,5)), CAST(`b` AS DECIMALV3(12,4)), CAST(`b` AS DECIMAL(11,5)), CAST(`b` AS DECIMAL(12,4)) FROM ${tableName} t ORDER BY a; """
    } finally {
    }
}
