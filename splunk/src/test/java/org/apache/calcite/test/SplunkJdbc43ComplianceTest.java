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
package org.apache.calcite.test;

import org.apache.calcite.adapter.splunk.SplunkDataConverter;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for JDBC 4.3 compliant type conversions in the Splunk adapter.
 *
 * According to JDBC 4.3 Specification (Section 15.2.3):
 * - VARCHAR/CHAR columns CAN be read using getInt(), getLong(), getDouble(), etc.
 * - The driver SHOULD attempt conversion when numeric getter is called on VARCHAR
 * - Throw SQLException if the string isn't numeric
 */
@Tag("unit")
public class SplunkJdbc43ComplianceTest {

  @Test public void testVarcharToIntegerConversion() {
    // Test valid integer string
    assertEquals(123, SplunkDataConverter.convertValue("123", SqlTypeName.INTEGER));

    // Test integer string with spaces
    assertEquals(456, SplunkDataConverter.convertValue("  456  ", SqlTypeName.INTEGER));

    // Test negative integer
    assertEquals(-789, SplunkDataConverter.convertValue("-789", SqlTypeName.INTEGER));

    // Test decimal string (should truncate per JDBC spec)
    assertEquals(100, SplunkDataConverter.convertValue("100.99", SqlTypeName.INTEGER));

    // Test invalid string (should throw exception)
    assertThrows(RuntimeException.class,
        () -> SplunkDataConverter.convertValue("abc", SqlTypeName.INTEGER));

    // Test empty string (should return null)
    assertNull(SplunkDataConverter.convertValue("", SqlTypeName.INTEGER));

    // Test null string literal (should return null)
    assertNull(SplunkDataConverter.convertValue("null", SqlTypeName.INTEGER));
  }

  @Test public void testVarcharToBigIntConversion() {
    // Test valid long string
    assertEquals(9876543210L, SplunkDataConverter.convertValue("9876543210", SqlTypeName.BIGINT));

    // Test long string with spaces
    assertEquals(1234567890L, SplunkDataConverter.convertValue("  1234567890  ", SqlTypeName.BIGINT));

    // Test negative long
    assertEquals(-9876543210L, SplunkDataConverter.convertValue("-9876543210", SqlTypeName.BIGINT));

    // Test decimal string (should truncate)
    assertEquals(1000L, SplunkDataConverter.convertValue("1000.99", SqlTypeName.BIGINT));

    // Test invalid string
    assertThrows(RuntimeException.class,
        () -> SplunkDataConverter.convertValue("not_a_number", SqlTypeName.BIGINT));
  }

  @Test public void testVarcharToDoubleConversion() {
    // Test valid double string
    assertEquals(123.456, SplunkDataConverter.convertValue("123.456", SqlTypeName.DOUBLE));

    // Test scientific notation
    assertEquals(1.23e10, SplunkDataConverter.convertValue("1.23e10", SqlTypeName.DOUBLE));

    // Test negative double
    assertEquals(-789.012, SplunkDataConverter.convertValue("-789.012", SqlTypeName.DOUBLE));

    // Test integer string (should work as double)
    assertEquals(100.0, SplunkDataConverter.convertValue("100", SqlTypeName.DOUBLE));

    // Test invalid string
    assertThrows(RuntimeException.class,
        () -> SplunkDataConverter.convertValue("invalid", SqlTypeName.DOUBLE));
  }

  @Test public void testVarcharToFloatConversion() {
    // Test valid float string
    assertEquals(12.34f, SplunkDataConverter.convertValue("12.34", SqlTypeName.FLOAT));

    // Test negative float
    assertEquals(-56.78f, SplunkDataConverter.convertValue("-56.78", SqlTypeName.FLOAT));

    // Test integer string as float
    assertEquals(99.0f, SplunkDataConverter.convertValue("99", SqlTypeName.FLOAT));

    // Test REAL type (alias for FLOAT)
    assertEquals(88.88f, SplunkDataConverter.convertValue("88.88", SqlTypeName.REAL));

    // Test invalid string
    assertThrows(RuntimeException.class,
        () -> SplunkDataConverter.convertValue("not_float", SqlTypeName.FLOAT));
  }

  @Test public void testVarcharToDecimalConversion() {
    // Test valid decimal string
    assertEquals(new BigDecimal("123.456789"),
        SplunkDataConverter.convertValue("123.456789", SqlTypeName.DECIMAL));

    // Test high precision decimal
    assertEquals(new BigDecimal("99999999999999999999.123456789"),
        SplunkDataConverter.convertValue("99999999999999999999.123456789", SqlTypeName.DECIMAL));

    // Test negative decimal
    assertEquals(new BigDecimal("-987.654321"),
        SplunkDataConverter.convertValue("-987.654321", SqlTypeName.DECIMAL));

    // Test invalid string
    assertThrows(RuntimeException.class,
        () -> SplunkDataConverter.convertValue("not_decimal", SqlTypeName.DECIMAL));
  }

  @Test public void testVarcharToBooleanConversion() {
    // Test various true values
    assertEquals(Boolean.TRUE, SplunkDataConverter.convertValue("true", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.TRUE, SplunkDataConverter.convertValue("TRUE", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.TRUE, SplunkDataConverter.convertValue("1", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.TRUE, SplunkDataConverter.convertValue("yes", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.TRUE, SplunkDataConverter.convertValue("YES", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.TRUE, SplunkDataConverter.convertValue("y", SqlTypeName.BOOLEAN));

    // Test various false values
    assertEquals(Boolean.FALSE, SplunkDataConverter.convertValue("false", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.FALSE, SplunkDataConverter.convertValue("FALSE", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.FALSE, SplunkDataConverter.convertValue("0", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.FALSE, SplunkDataConverter.convertValue("no", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.FALSE, SplunkDataConverter.convertValue("NO", SqlTypeName.BOOLEAN));
    assertEquals(Boolean.FALSE, SplunkDataConverter.convertValue("n", SqlTypeName.BOOLEAN));

    // Test invalid string
    assertThrows(RuntimeException.class,
        () -> SplunkDataConverter.convertValue("maybe", SqlTypeName.BOOLEAN));
  }

  @Test public void testNumericToNumericConversions() {
    // Test that numeric types still work correctly
    assertEquals(42, SplunkDataConverter.convertValue(42, SqlTypeName.INTEGER));
    assertEquals(42L, SplunkDataConverter.convertValue(42L, SqlTypeName.BIGINT));
    assertEquals(42.5, SplunkDataConverter.convertValue(42.5, SqlTypeName.DOUBLE));
    assertEquals(42.5f, SplunkDataConverter.convertValue(42.5f, SqlTypeName.FLOAT));

    // Test cross-numeric conversions
    assertEquals(100, SplunkDataConverter.convertValue(100L, SqlTypeName.INTEGER));
    assertEquals(100L, SplunkDataConverter.convertValue(100, SqlTypeName.BIGINT));
    assertEquals(100.0, SplunkDataConverter.convertValue(100, SqlTypeName.DOUBLE));
    assertEquals(100.0f, SplunkDataConverter.convertValue(100, SqlTypeName.FLOAT));
  }

  @Test public void testEdgeCases() {
    // Test null handling
    assertNull(SplunkDataConverter.convertValue(null, SqlTypeName.INTEGER));
    assertNull(SplunkDataConverter.convertValue(null, SqlTypeName.DOUBLE));
    assertNull(SplunkDataConverter.convertValue(null, SqlTypeName.VARCHAR));

    // Test empty string
    assertNull(SplunkDataConverter.convertValue("", SqlTypeName.INTEGER));
    assertNull(SplunkDataConverter.convertValue("   ", SqlTypeName.DOUBLE));

    // Test "null" string literal
    assertNull(SplunkDataConverter.convertValue("null", SqlTypeName.INTEGER));
    assertNull(SplunkDataConverter.convertValue("null", SqlTypeName.BIGINT));
    assertNull(SplunkDataConverter.convertValue("null", SqlTypeName.DOUBLE));

    // Test VARCHAR to VARCHAR (should pass through)
    assertEquals("test", SplunkDataConverter.convertValue("test", SqlTypeName.VARCHAR));
    assertEquals("123", SplunkDataConverter.convertValue("123", SqlTypeName.VARCHAR));
  }

  @Test public void testRealWorldSplunkScenarios() {
    // Splunk often returns numeric values as strings in JSON
    // Test common Splunk field conversions

    // bytes field (often returned as string)
    assertEquals(1024L, SplunkDataConverter.convertValue("1024", SqlTypeName.BIGINT));

    // count field
    assertEquals(42, SplunkDataConverter.convertValue("42", SqlTypeName.INTEGER));

    // duration field (often has decimals)
    assertEquals(123.456, SplunkDataConverter.convertValue("123.456", SqlTypeName.DOUBLE));

    // status code (string that should convert to int)
    assertEquals(200, SplunkDataConverter.convertValue("200", SqlTypeName.INTEGER));
    assertEquals(404, SplunkDataConverter.convertValue("404", SqlTypeName.INTEGER));

    // port numbers
    assertEquals(8089, SplunkDataConverter.convertValue("8089", SqlTypeName.INTEGER));

    // percentages (may include decimal)
    assertEquals(95.5, SplunkDataConverter.convertValue("95.5", SqlTypeName.DOUBLE));
  }
}
