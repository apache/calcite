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
import static org.apache.calcite.sql.type.SqlTypeName.UBIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.UINTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.USMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.UTINYINT;
import static org.apache.calcite.sql.type.SqlTypeName.VARBINARY;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests types supported by {@link SqlTypeName}.
 */
class SqlTypeNameTest {
  @Test void testBit() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.BIT);
    assertThat("BIT did not map to BOOLEAN", tn, is(BOOLEAN));
  }

  @Test void testTinyint() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.TINYINT);
    assertThat("TINYINT did not map to TINYINT", tn, is(TINYINT));
  }

  @Test void testSmallint() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.SMALLINT);
    assertThat("SMALLINT did not map to SMALLINT", tn, is(SMALLINT));
  }

  @Test void testInteger() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.INTEGER);
    assertThat("INTEGER did not map to INTEGER", tn, is(INTEGER));
  }

  @Test void testBigint() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.BIGINT);
    assertThat("BIGINT did not map to BIGINT", tn, is(BIGINT));
  }

  @Test void testUnsignedTinyint() {
    SqlTypeName tn =
        SqlTypeName.getNameForUnsignedJdbcType(Types.TINYINT);
    assertThat("TINYINT did not map to UTINYINT", tn, is(UTINYINT));
  }

  @Test void testUnsignedSmallint() {
    SqlTypeName tn =
        SqlTypeName.getNameForUnsignedJdbcType(Types.SMALLINT);
    assertThat("SMALLINT did not map to USMALLINT", tn, is(USMALLINT));
  }

  @Test void testUnsignedInteger() {
    SqlTypeName tn =
        SqlTypeName.getNameForUnsignedJdbcType(Types.INTEGER);
    assertThat("INTEGER did not map to UINTEGER", tn, is(UINTEGER));
  }

  @Test void testUnsignedBigint() {
    SqlTypeName tn =
        SqlTypeName.getNameForUnsignedJdbcType(Types.BIGINT);
    assertThat("BIGINT did not map to UBIGINT", tn, is(UBIGINT));
  }

  @Test void testFloat() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.FLOAT);
    assertThat("FLOAT did not map to FLOAT", tn, is(FLOAT));
  }

  @Test void testReal() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.REAL);
    assertThat("REAL did not map to REAL", tn, is(REAL));
  }

  @Test void testDouble() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.DOUBLE);
    assertThat("DOUBLE did not map to DOUBLE", tn, is(DOUBLE));
  }

  @Test void testNumeric() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.NUMERIC);
    assertThat("NUMERIC did not map to DECIMAL", tn, is(DECIMAL));
  }

  @Test void testDecimal() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.DECIMAL);
    assertThat("DECIMAL did not map to DECIMAL", tn, is(DECIMAL));
  }

  @Test void testChar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.CHAR);
    assertThat("CHAR did not map to CHAR", tn, is(CHAR));
  }

  @Test void testVarchar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.VARCHAR);
    assertThat("VARCHAR did not map to VARCHAR", tn, is(VARCHAR));
  }

  @Test void testLongvarchar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.LONGVARCHAR);
    assertThat("LONGVARCHAR did not map to null", tn, nullValue());
  }

  @Test void testDate() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.DATE);
    assertThat("DATE did not map to DATE", tn, is(DATE));
  }

  @Test void testTime() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.TIME);
    assertThat("TIME did not map to TIME", tn, is(TIME));
  }

  @Test void testTimestamp() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.TIMESTAMP);
    assertThat("TIMESTAMP did not map to TIMESTAMP", tn, is(TIMESTAMP));
  }

  @Test void testBinary() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.BINARY);
    assertThat("BINARY did not map to BINARY", tn, is(BINARY));
  }

  @Test void testVarbinary() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.VARBINARY);
    assertThat("VARBINARY did not map to VARBINARY", tn, is(VARBINARY));
  }

  @Test void testLongvarbinary() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.LONGVARBINARY);
    assertThat("LONGVARBINARY did not map to null", tn, nullValue());
  }

  @Test void testNull() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.NULL);
    assertThat("NULL did not map to null", tn, nullValue());
  }

  @Test void testOther() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.OTHER);
    assertThat("OTHER did not map to null", tn, nullValue());
  }

  @Test void testJavaobject() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.JAVA_OBJECT);
    assertThat("JAVA_OBJECT did not map to null", tn, nullValue());
  }

  @Test void testDistinct() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.DISTINCT);
    assertThat("DISTINCT did not map to DISTINCT", tn, is(DISTINCT));
  }

  @Test void testStruct() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.STRUCT);
    assertThat("STRUCT did not map to null", tn, is(STRUCTURED));
  }

  @Test void testArray() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.ARRAY);
    assertThat("ARRAY did not map to ARRAY", tn, is(ARRAY));
  }

  @Test void testBlob() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.BLOB);
    assertThat("BLOB did not map to null", tn, nullValue());
  }

  @Test void testClob() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.CLOB);
    assertThat("CLOB did not map to null", tn, nullValue());
  }

  @Test void testRef() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.REF);
    assertThat("REF did not map to null", tn, nullValue());
  }

  @Test void testDatalink() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.DATALINK);
    assertThat("DATALINK did not map to null", tn, nullValue());
  }

  @Test void testBoolean() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(Types.BOOLEAN);
    assertThat("BOOLEAN did not map to BOOLEAN", tn, is(BOOLEAN));
  }

  @Test void testRowid() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.ROWID);

    // ROWID not supported yet
    assertThat("ROWID maps to non-null type", tn, nullValue());
  }

  @Test void testNchar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.NCHAR);

    // NCHAR not supported yet, currently maps to CHAR
    assertThat("NCHAR did not map to CHAR", tn, is(CHAR));
  }

  @Test void testNvarchar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.NVARCHAR);

    // NVARCHAR not supported yet, currently maps to VARCHAR
    assertThat("NVARCHAR did not map to VARCHAR", tn, is(VARCHAR));
  }

  @Test void testLongnvarchar() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.LONGNVARCHAR);

    // LONGNVARCHAR not supported yet
    assertThat("LONGNVARCHAR maps to non-null type", tn, nullValue());
  }

  @Test void testNclob() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.NCLOB);

    // NCLOB not supported yet
    assertThat("NCLOB maps to non-null type", tn, nullValue());
  }

  @Test void testSqlxml() {
    SqlTypeName tn =
        SqlTypeName.getNameForJdbcType(ExtraSqlTypes.SQLXML);

    // SQLXML not supported yet
    assertThat("SQLXML maps to non-null type", tn, nullValue());
  }
}
