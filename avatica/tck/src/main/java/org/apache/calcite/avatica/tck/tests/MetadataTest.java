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
package org.apache.calcite.avatica.tck.tests;

import org.junit.Test;

import java.math.BigDecimal;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class for metadata operations (parameter, resultset).
 */
public class MetadataTest extends BaseTckTest {

  @Test public void parameterMetadata() throws Exception {
    final String tableName = getTableName();
    try (Statement stmt = getConnection().createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName + " (pk integer not null primary key, "
          + "col1 DECIMAL(10, 5), col2 boolean not null)";
      assertFalse(stmt.execute(sql));

      String insertSql = "INSERT INTO " + tableName + " values(?, ?, ?)";
      try (PreparedStatement pstmt = getConnection().prepareStatement(insertSql)) {
        ParameterMetaData params = pstmt.getParameterMetaData();
        assertEquals(3, params.getParameterCount());

        assertEquals(Types.INTEGER, params.getParameterType(1));
        assertTrue(params.isSigned(1));
        assertTrue(ParameterMetaData.parameterNoNulls == params.isNullable(1)
            || ParameterMetaData.parameterNullableUnknown == params.isNullable(1));

        assertEquals(Types.DECIMAL, params.getParameterType(2));
        assertTrue(params.isSigned(2));
        assertTrue(ParameterMetaData.parameterNullable == params.isNullable(2)
            || ParameterMetaData.parameterNullableUnknown == params.isNullable(2));
        assertEquals(10, params.getPrecision(2));
        assertEquals(5, params.getScale(2));

        assertEquals(Types.BOOLEAN, params.getParameterType(3));
        assertFalse(params.isSigned(3));
        assertTrue(ParameterMetaData.parameterNoNulls == params.isNullable(3)
            || ParameterMetaData.parameterNullableUnknown == params.isNullable(3));

        // CALCITE-1103 <1.8.0 server mishandled the protobuf translation from BIG_DECIMAL to NUMBER
        pstmt.setInt(1, Integer.MAX_VALUE);
        pstmt.setBigDecimal(2, new BigDecimal("12345.12345"));
        pstmt.setBoolean(3, true);
        assertEquals(1, pstmt.executeUpdate());

        pstmt.setInt(1, Integer.MIN_VALUE);
        pstmt.setBigDecimal(2, new BigDecimal("54321.54321"));
        pstmt.setBoolean(3, false);
        assertEquals(1, pstmt.executeUpdate());
      }

      ResultSet results = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY pk");
      assertNotNull(results);
      ResultSetMetaData resultMetadata = results.getMetaData();
      // Verify result metadata
      assertEquals(3, resultMetadata.getColumnCount());

      assertTrue(ParameterMetaData.parameterNoNulls == resultMetadata.isNullable(1)
          || ParameterMetaData.parameterNullableUnknown == resultMetadata.isNullable(1));
      assertEquals(Types.INTEGER, resultMetadata.getColumnType(1));
      assertTrue(resultMetadata.isSigned(1));

      assertTrue(ParameterMetaData.parameterNullable == resultMetadata.isNullable(2)
          || ParameterMetaData.parameterNullableUnknown == resultMetadata.isNullable(2));
      assertEquals(Types.DECIMAL, resultMetadata.getColumnType(2));
      assertTrue(resultMetadata.isSigned(2));
      assertEquals(10, resultMetadata.getPrecision(2));
      assertEquals(5, resultMetadata.getScale(2));

      assertTrue(ParameterMetaData.parameterNoNulls == resultMetadata.isNullable(3)
          || ParameterMetaData.parameterNullableUnknown == resultMetadata.isNullable(3));
      assertEquals(Types.BOOLEAN, resultMetadata.getColumnType(3));
      assertFalse(resultMetadata.isSigned(3));

      // Verify the results
      assertTrue(results.next());
      assertEquals(Integer.MIN_VALUE, results.getInt(1));
      // CALCITE-1103 protobuf truncated decimal value
      BigDecimal buggyDecimalValue = new BigDecimal("54321.00000");
      BigDecimal expectedDecimalValue = new BigDecimal("54321.54321");
      BigDecimal actualDecimalValue = results.getBigDecimal(2);
      assertTrue("Unexpected decimal value of " + actualDecimalValue,
          expectedDecimalValue.equals(actualDecimalValue)
          || buggyDecimalValue.equals(actualDecimalValue));
      assertEquals(false, results.getBoolean(3));

      assertTrue(results.next());
      assertEquals(Integer.MAX_VALUE, results.getInt(1));
      // CALCITE-1103 protobuf truncated decimal value
      buggyDecimalValue = new BigDecimal("12345.00000");
      expectedDecimalValue = new BigDecimal("12345.12345");
      actualDecimalValue = results.getBigDecimal(2);
      assertTrue("Unexpected decimal value of " + actualDecimalValue,
          expectedDecimalValue.equals(actualDecimalValue)
          || buggyDecimalValue.equals(actualDecimalValue));
      assertEquals(true, results.getBoolean(3));

      assertFalse(results.next());
    }
  }
}

// End MetadataTest.java
