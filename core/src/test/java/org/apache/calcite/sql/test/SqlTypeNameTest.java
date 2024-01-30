/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.test;

import org.apache.calcite.sql.type.ExtraSqlTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import java.sql.Types;

import static org.apache.calcite.sql.type.SqlTypeName.ARRAY;
import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.BINARY;
import static org.apache.calcite.sql.type.SqlTypeName.BOOLEAN;
import static org.apache.calcite.sql.type.SqlTypeName.CHAR;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.calcite.sql.type.SqlTypeName.DISTINCT;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.REAL;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.STRUCTURED;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARBINARY;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests types supported by {@link SqlTypeName}.
 */
class SqlTypeNameTest {
  @Test void testBit() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.BIT);
    assertEquals(BOOLEAN, tn, "BIT did not map to BOOLEAN");
  }

  @Test void testTinyint() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.TINYINT);
    assertEquals(TINYINT, tn, "TINYINT did not map to TINYINT");
  }

  @Test void testSmallint() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.SMALLINT);
    assertEquals(SMALLINT, tn, "SMALLINT did not map to SMALLINT");
  }

  @Test void testInteger() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.INTEGER);
    assertEquals(INTEGER, tn, "INTEGER did not map to INTEGER");
  }

  @Test void testBigint() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.BIGINT);
    assertEquals(BIGINT, tn, "BIGINT did not map to BIGINT");
  }

  @Test void testFloat() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.FLOAT);
    assertEquals(FLOAT, tn, "FLOAT did not map to FLOAT");
  }

  @Test void testReal() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.REAL);
    assertEquals(REAL, tn, "REAL did not map to REAL");
  }

  @Test void testDouble() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.DOUBLE);
    assertEquals(DOUBLE, tn, "DOUBLE did not map to DOUBLE");
  }

  @Test void testNumeric() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.NUMERIC);
    assertEquals(DECIMAL, tn, "NUMERIC did not map to DECIMAL");
  }

  @Test void testDecimal() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.DECIMAL);
    assertEquals(DECIMAL, tn, "DECIMAL did not map to DECIMAL");
  }

  @Test void testChar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.CHAR);
    assertEquals(CHAR, tn, "CHAR did not map to CHAR");
  }

  @Test void testVarchar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.VARCHAR);
    assertEquals(VARCHAR, tn, "VARCHAR did not map to VARCHAR");
  }

  @Test void testLongvarchar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.LONGVARCHAR);
    assertEquals(null, tn, "LONGVARCHAR did not map to null");
  }

  @Test void testDate() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.DATE);
    assertEquals(DATE, tn, "DATE did not map to DATE");
  }

  @Test void testTime() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.TIME);
    assertEquals(TIME, tn, "TIME did not map to TIME");
  }

  @Test void testTimestamp() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.TIMESTAMP);
    assertEquals(TIMESTAMP, tn, "TIMESTAMP did not map to TIMESTAMP");
  }

  @Test void testBinary() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.BINARY);
    assertEquals(BINARY, tn, "BINARY did not map to BINARY");
  }

  @Test void testVarbinary() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.VARBINARY);
    assertEquals(VARBINARY, tn, "VARBINARY did not map to VARBINARY");
  }

  @Test void testLongvarbinary() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.LONGVARBINARY);
    assertEquals(null, tn, "LONGVARBINARY did not map to null");
  }

  @Test void testNull() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.NULL);
    assertEquals(null, tn, "NULL did not map to null");
  }

  @Test void testOther() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.OTHER);
    assertEquals(null, tn, "OTHER did not map to null");
  }

  @Test void testJavaobject() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.JAVA_OBJECT);
    assertEquals(null, tn, "JAVA_OBJECT did not map to null");
  }

  @Test void testDistinct() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.DISTINCT);
    assertEquals(DISTINCT, tn, "DISTINCT did not map to DISTINCT");
  }

  @Test void testStruct() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.STRUCT);
    assertEquals(STRUCTURED, tn, "STRUCT did not map to null");
  }

  @Test void testArray() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.ARRAY);
    assertEquals(ARRAY, tn, "ARRAY did not map to ARRAY");
  }

  @Test void testBlob() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.BLOB);
    assertEquals(null, tn, "BLOB did not map to null");
  }

  @Test void testClob() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.CLOB);
    assertEquals(null, tn, "CLOB did not map to null");
  }

  @Test void testRef() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.REF);
    assertEquals(null, tn, "REF did not map to null");
  }

  @Test void testDatalink() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.DATALINK);
    assertEquals(null, tn, "DATALINK did not map to null");
  }

  @Test void testBoolean() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.BOOLEAN);
    assertEquals(BOOLEAN, tn, "BOOLEAN did not map to BOOLEAN");
  }

  @Test void testRowid() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.ROWID);

    // ROWID not supported yet
    assertEquals(null, tn, "ROWID maps to non-null type");
  }

  @Test void testNchar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.NCHAR);

    // NCHAR not supported yet, currently maps to CHAR
    assertEquals(CHAR, tn, "NCHAR did not map to CHAR");
  }

  @Test void testNvarchar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.NVARCHAR);

    // NVARCHAR not supported yet, currently maps to VARCHAR
    assertEquals(VARCHAR, tn, "NVARCHAR did not map to VARCHAR");
  }

  @Test void testLongnvarchar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.LONGNVARCHAR);

    // LONGNVARCHAR not supported yet
    assertEquals(null, tn, "LONGNVARCHAR maps to non-null type");
  }

  @Test void testNclob() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.NCLOB);

    // NCLOB not supported yet
    assertEquals(null, tn, "NCLOB maps to non-null type");
  }

  @Test void testSqlxml() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.SQLXML);

    // SQLXML not supported yet
    assertEquals(null, tn, "SQLXML maps to non-null type");
  }
}
