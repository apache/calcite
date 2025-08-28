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
package org.apache.calcite.adapter.ops;

import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CloudOpsDataConverter}.
 * Tests comprehensive data type conversion for cloud provider data.
 */
public class CloudOpsDataConverterTest {

  @Test public void testBooleanConversionFromString() {
    // Test string "true" variants
    assertTrue((Boolean) CloudOpsDataConverter.convertValue("true", SqlTypeName.BOOLEAN));
    assertTrue((Boolean) CloudOpsDataConverter.convertValue("True", SqlTypeName.BOOLEAN));
    assertTrue((Boolean) CloudOpsDataConverter.convertValue("TRUE", SqlTypeName.BOOLEAN));
    assertTrue((Boolean) CloudOpsDataConverter.convertValue("1", SqlTypeName.BOOLEAN));
    assertTrue((Boolean) CloudOpsDataConverter.convertValue("yes", SqlTypeName.BOOLEAN));
    assertTrue((Boolean) CloudOpsDataConverter.convertValue("Y", SqlTypeName.BOOLEAN));
    assertTrue((Boolean) CloudOpsDataConverter.convertValue("enabled", SqlTypeName.BOOLEAN));
    assertTrue((Boolean) CloudOpsDataConverter.convertValue("active", SqlTypeName.BOOLEAN));
    assertTrue((Boolean) CloudOpsDataConverter.convertValue("on", SqlTypeName.BOOLEAN));

    // Test string "false" variants
    assertFalse((Boolean) CloudOpsDataConverter.convertValue("false", SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue("False", SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue("FALSE", SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue("0", SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue("no", SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue("n", SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue("disabled", SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue("inactive", SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue("off", SqlTypeName.BOOLEAN));
  }

  @Test public void testBooleanConversionFromNumber() {
    // Test numeric boolean values
    assertTrue((Boolean) CloudOpsDataConverter.convertValue(1, SqlTypeName.BOOLEAN));
    assertTrue((Boolean) CloudOpsDataConverter.convertValue(99, SqlTypeName.BOOLEAN));
    assertTrue((Boolean) CloudOpsDataConverter.convertValue(-1, SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue(0, SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue(0L, SqlTypeName.BOOLEAN));
  }

  @Test public void testBooleanNullHandling() {
    // Test null representations for boolean
    assertNull(CloudOpsDataConverter.convertValue(null, SqlTypeName.BOOLEAN));
    assertNull(CloudOpsDataConverter.convertValue("", SqlTypeName.BOOLEAN));
    assertNull(CloudOpsDataConverter.convertValue("null", SqlTypeName.BOOLEAN));
    assertNull(CloudOpsDataConverter.convertValue("NULL", SqlTypeName.BOOLEAN));
    assertNull(CloudOpsDataConverter.convertValue("nil", SqlTypeName.BOOLEAN));
    assertNull(CloudOpsDataConverter.convertValue("none", SqlTypeName.BOOLEAN));
    assertNull(CloudOpsDataConverter.convertValue("undefined", SqlTypeName.BOOLEAN));
    assertNull(CloudOpsDataConverter.convertValue("N/A", SqlTypeName.BOOLEAN));
    assertNull(CloudOpsDataConverter.convertValue("na", SqlTypeName.BOOLEAN));
    assertNull(CloudOpsDataConverter.convertValue("-", SqlTypeName.BOOLEAN));
  }

  @Test public void testIntegerConversion() {
    // Test integer conversions
    assertEquals(42, CloudOpsDataConverter.convertValue(42, SqlTypeName.INTEGER));
    assertEquals(42, CloudOpsDataConverter.convertValue("42", SqlTypeName.INTEGER));
    assertEquals(42, CloudOpsDataConverter.convertValue(42L, SqlTypeName.INTEGER));
    assertEquals(42, CloudOpsDataConverter.convertValue(42.0, SqlTypeName.INTEGER));
    assertEquals(42, CloudOpsDataConverter.convertValue("42.0", SqlTypeName.INTEGER));

    // Test null representations
    assertNull(CloudOpsDataConverter.convertValue("", SqlTypeName.INTEGER));
    assertNull(CloudOpsDataConverter.convertValue("null", SqlTypeName.INTEGER));
    assertNull(CloudOpsDataConverter.convertValue("N/A", SqlTypeName.INTEGER));
  }

  @Test public void testLongConversion() {
    // Test long conversions
    assertEquals(9999999999L, CloudOpsDataConverter.convertValue(9999999999L, SqlTypeName.BIGINT));
    assertEquals(9999999999L, CloudOpsDataConverter.convertValue("9999999999", SqlTypeName.BIGINT));
    assertEquals(42L, CloudOpsDataConverter.convertValue(42, SqlTypeName.BIGINT));
    assertEquals(42L, CloudOpsDataConverter.convertValue(42.0, SqlTypeName.BIGINT));
  }

  @Test public void testFloatConversion() {
    // Test float conversions
    assertEquals(3.14f, CloudOpsDataConverter.convertValue(3.14f, SqlTypeName.FLOAT));
    assertEquals(3.14f, CloudOpsDataConverter.convertValue("3.14", SqlTypeName.FLOAT));
    assertEquals(42.0f, CloudOpsDataConverter.convertValue(42, SqlTypeName.FLOAT));
    assertEquals(3.14f, (Float) CloudOpsDataConverter.convertValue(3.14, SqlTypeName.FLOAT), 0.001f);
  }

  @Test public void testDoubleConversion() {
    // Test double conversions
    assertEquals(3.14159, CloudOpsDataConverter.convertValue(3.14159, SqlTypeName.DOUBLE));
    assertEquals(3.14159, CloudOpsDataConverter.convertValue("3.14159", SqlTypeName.DOUBLE));
    assertEquals(42.0, CloudOpsDataConverter.convertValue(42, SqlTypeName.DOUBLE));
  }

  @Test public void testDecimalConversion() {
    // Test decimal conversions
    BigDecimal expected = new BigDecimal("123.456");
    assertEquals(expected, CloudOpsDataConverter.convertValue("123.456", SqlTypeName.DECIMAL));
    assertEquals(new BigDecimal("42.0"), CloudOpsDataConverter.convertValue(42, SqlTypeName.DECIMAL));
    assertEquals(new BigDecimal("3.14"),
        CloudOpsDataConverter.convertValue(new BigDecimal("3.14"), SqlTypeName.DECIMAL));
  }

  @Test public void testStringConversion() {
    // Test that VARCHAR preserves all string values (doesn't apply null conversions)
    assertEquals("null", CloudOpsDataConverter.convertValue("null", SqlTypeName.VARCHAR));
    assertEquals("N/A", CloudOpsDataConverter.convertValue("N/A", SqlTypeName.VARCHAR));
    assertEquals("-", CloudOpsDataConverter.convertValue("-", SqlTypeName.VARCHAR));
    assertEquals("none", CloudOpsDataConverter.convertValue("none", SqlTypeName.VARCHAR));
    assertEquals("test", CloudOpsDataConverter.convertValue("test", SqlTypeName.VARCHAR));

    // Only actual null returns null
    assertNull(CloudOpsDataConverter.convertValue(null, SqlTypeName.VARCHAR));
  }

  @Test public void testRowConversion() {
    // Test batch row conversion
    Object[] row = {"true", "42", "3.14", "test", null};
    SqlTypeName[] types = {
        SqlTypeName.BOOLEAN,
        SqlTypeName.INTEGER,
        SqlTypeName.DOUBLE,
        SqlTypeName.VARCHAR,
        SqlTypeName.BOOLEAN
    };

    Object[] converted = CloudOpsDataConverter.convertRow(row, types);

    assertEquals(Boolean.TRUE, converted[0]);
    assertEquals(42, converted[1]);
    assertEquals(3.14, converted[2]);
    assertEquals("test", converted[3]);
    assertNull(converted[4]);
  }

  @Test public void testCloudProviderSpecificValues() {
    // Test cloud provider specific boolean representations
    assertTrue((Boolean) CloudOpsDataConverter.convertValue("Enabled", SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue("Disabled", SqlTypeName.BOOLEAN));
    assertTrue((Boolean) CloudOpsDataConverter.convertValue("Active", SqlTypeName.BOOLEAN));
    assertFalse((Boolean) CloudOpsDataConverter.convertValue("Inactive", SqlTypeName.BOOLEAN));

    // Test that these specific values are preserved as strings for VARCHAR
    assertEquals("Enabled", CloudOpsDataConverter.convertValue("Enabled", SqlTypeName.VARCHAR));
    assertEquals("Disabled", CloudOpsDataConverter.convertValue("Disabled", SqlTypeName.VARCHAR));
  }

  @Test public void testErrorHandling() {
    // Test that invalid conversions return the original value
    Object invalidBoolean = CloudOpsDataConverter.convertValue("invalid", SqlTypeName.BOOLEAN);
    assertEquals("invalid", invalidBoolean); // Falls back to original value on error

    Object invalidNumber = CloudOpsDataConverter.convertValue("not-a-number", SqlTypeName.INTEGER);
    assertEquals("not-a-number", invalidNumber); // Falls back to original value on error
  }
}
