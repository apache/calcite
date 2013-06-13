/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.sql.test;

import java.sql.Types;

import org.eigenbase.sql.type.*;

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests types supported by {@link SqlTypeName}.
 */
public class SqlTypeNameTest {
    @Test public void testBit() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.BIT);
        assertEquals(
            "BIT did not map to BOOLEAN",
            SqlTypeName.BOOLEAN,
            tn);
    }

    @Test public void testTinyint() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.TINYINT);
        assertEquals(
            "TINYINT did not map to TINYINT",
            SqlTypeName.TINYINT,
            tn);
    }

    @Test public void testSmallint() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.SMALLINT);
        assertEquals(
            "SMALLINT did not map to SMALLINT",
            SqlTypeName.SMALLINT,
            tn);
    }

    @Test public void testInteger() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.INTEGER);
        assertEquals(
            "INTEGER did not map to INTEGER",
            SqlTypeName.INTEGER,
            tn);
    }

    @Test public void testBigint() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.BIGINT);
        assertEquals(
            "BIGINT did not map to BIGINT",
            SqlTypeName.BIGINT,
            tn);
    }

    @Test public void testFloat() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.FLOAT);
        assertEquals(
            "FLOAT did not map to FLOAT",
            SqlTypeName.FLOAT,
            tn);
    }

    @Test public void testReal() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.REAL);
        assertEquals(
            "REAL did not map to REAL",
            SqlTypeName.REAL,
            tn);
    }

    @Test public void testDouble() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.DOUBLE);
        assertEquals(
            "DOUBLE did not map to DOUBLE",
            SqlTypeName.DOUBLE,
            tn);
    }

    @Test public void testNumeric() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.NUMERIC);
        assertEquals(
            "NUMERIC did not map to DECIMAL",
            SqlTypeName.DECIMAL,
            tn);
    }

    @Test public void testDecimal() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.DECIMAL);
        assertEquals(
            "DECIMAL did not map to DECIMAL",
            SqlTypeName.DECIMAL,
            tn);
    }

    @Test public void testChar() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.CHAR);
        assertEquals(
            "CHAR did not map to CHAR",
            SqlTypeName.CHAR,
            tn);
    }

    @Test public void testVarchar() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.VARCHAR);
        assertEquals(
            "VARCHAR did not map to VARCHAR",
            SqlTypeName.VARCHAR,
            tn);
    }

    @Test public void testLongvarchar() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.LONGVARCHAR);
        assertEquals(
            "LONGVARCHAR did not map to null",
            null,
            tn);
    }

    @Test public void testDate() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.DATE);
        assertEquals(
            "DATE did not map to DATE",
            SqlTypeName.DATE,
            tn);
    }

    @Test public void testTime() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.TIME);
        assertEquals(
            "TIME did not map to TIME",
            SqlTypeName.TIME,
            tn);
    }

    @Test public void testTimestamp() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.TIMESTAMP);
        assertEquals(
            "TIMESTAMP did not map to TIMESTAMP",
            SqlTypeName.TIMESTAMP,
            tn);
    }

    @Test public void testBinary() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.BINARY);
        assertEquals(
            "BINARY did not map to BINARY",
            SqlTypeName.BINARY,
            tn);
    }

    @Test public void testVarbinary() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.VARBINARY);
        assertEquals(
            "VARBINARY did not map to VARBINARY",
            SqlTypeName.VARBINARY,
            tn);
    }

    @Test public void testLongvarbinary() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.LONGVARBINARY);
        assertEquals(
            "LONGVARBINARY did not map to null",
            null,
            tn);
    }

    @Test public void testNull() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.NULL);
        assertEquals(
            "NULL did not map to null",
            null,
            tn);
    }

    @Test public void testOther() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.OTHER);
        assertEquals(
            "OTHER did not map to null",
            null,
            tn);
    }

    @Test public void testJavaobject() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.JAVA_OBJECT);
        assertEquals(
            "JAVA_OBJECT did not map to null",
            null,
            tn);
    }

    @Test public void testDistinct() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.DISTINCT);
        assertEquals(
            "DISTINCT did not map to DISTINCT",
            SqlTypeName.DISTINCT,
            tn);
    }

    @Test public void testStruct() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.STRUCT);
        assertEquals(
            "STRUCT did not map to null",
            SqlTypeName.STRUCTURED,
            tn);
    }

    @Test public void testArray() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.ARRAY);
        assertEquals(
            "ARRAY did not map to null",
            null,
            tn);
    }

    @Test public void testBlob() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.BLOB);
        assertEquals(
            "BLOB did not map to null",
            null,
            tn);
    }

    @Test public void testClob() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.CLOB);
        assertEquals(
            "CLOB did not map to null",
            null,
            tn);
    }

    @Test public void testRef() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.REF);
        assertEquals(
            "REF did not map to null",
            null,
            tn);
    }

    @Test public void testDatalink() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.DATALINK);
        assertEquals(
            "DATALINK did not map to null",
            null,
            tn);
    }

    @Test public void testBoolean() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(Types.BOOLEAN);
        assertEquals(
            "BOOLEAN did not map to BOOLEAN",
            SqlTypeName.BOOLEAN,
            tn);
    }

    @Test public void testRowid() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(ExtraSqlTypes.ROWID);

        // ROWID not supported yet
        assertEquals(
            "ROWID maps to non-null type",
            null,
            tn);
    }

    @Test public void testNchar() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(ExtraSqlTypes.NCHAR);

        // NCHAR not supported yet, currently maps to CHAR
        assertEquals(
            "NCHAR did not map to CHAR",
            SqlTypeName.CHAR,
            tn);
    }

    @Test public void testNvarchar() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(ExtraSqlTypes.NVARCHAR);

        // NVARCHAR not supported yet, currently maps to VARCHAR
        assertEquals(
            "NVARCHAR did not map to VARCHAR",
            SqlTypeName.VARCHAR,
            tn);
    }

    @Test public void testLongnvarchar() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(ExtraSqlTypes.LONGNVARCHAR);

        // LONGNVARCHAR not supported yet
        assertEquals(
            "LONGNVARCHAR maps to non-null type",
            null,
            tn);
    }

    @Test public void testNclob() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(ExtraSqlTypes.NCLOB);

        // NCLOB not supported yet
        assertEquals(
            "NCLOB maps to non-null type",
            null,
            tn);
    }

    @Test public void testSqlxml() {
        SqlTypeName tn =
            SqlTypeName.getNameForJdbcType(ExtraSqlTypes.SQLXML);

        // SQLXML not supported yet
        assertEquals(
            "SQLXML maps to non-null type",
            null,
            tn);
    }
}

// End SqlTypeNameTest.java
