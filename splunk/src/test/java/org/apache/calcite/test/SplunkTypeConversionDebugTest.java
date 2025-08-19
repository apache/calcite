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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Debug test to understand how type conversion flows through the system.
 */
@Tag("unit")
public class SplunkTypeConversionDebugTest {

  @Test
  public void testDirectConversion() {
    // Test that our converter works for basic cases
    Object result = SplunkDataConverter.convertValue("123", SqlTypeName.INTEGER);
    assertEquals(123, result);
    assertTrue(result instanceof Integer);
    
    result = SplunkDataConverter.convertValue("456", SqlTypeName.BIGINT);
    assertEquals(456L, result);
    assertTrue(result instanceof Long);
    
    result = SplunkDataConverter.convertValue("78.9", SqlTypeName.DOUBLE);
    assertEquals(78.9, result);
    assertTrue(result instanceof Double);
  }

  @Test
  public void testStringPassThrough() {
    // Test that strings stay as strings when targeting VARCHAR
    Object result = SplunkDataConverter.convertValue("test", SqlTypeName.VARCHAR);
    assertEquals("test", result);
    assertTrue(result instanceof String);
  }

  @Test
  public void testNullHandling() {
    // Test null handling
    Object result = SplunkDataConverter.convertValue(null, SqlTypeName.INTEGER);
    assertEquals(null, result);
    
    result = SplunkDataConverter.convertValue("", SqlTypeName.INTEGER);
    assertEquals(null, result);
    
    result = SplunkDataConverter.convertValue("null", SqlTypeName.INTEGER);
    assertEquals(null, result);
  }

  @Test
  public void testInvalidConversion() {
    // Test that invalid conversions throw proper exceptions
    try {
      SplunkDataConverter.convertValue("abc", SqlTypeName.INTEGER);
      assertTrue(false, "Should have thrown exception");
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Cannot convert VARCHAR value 'abc' to INTEGER"));
    }
  }

  @Test
  public void testSchemaFieldLookup() {
    // Create a mock schema to test field lookup
    RelDataTypeFactory typeFactory = new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
        org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 100);
    
    // Create a schema with mixed types
    RelDataType schema = typeFactory.builder()
        .add("id", intType)
        .add("name", stringType)
        .add("count", intType)
        .build();
    
    // Test that we can look up field types correctly
    assertEquals(SqlTypeName.INTEGER, schema.getField("id", false, false).getType().getSqlTypeName());
    assertEquals(SqlTypeName.VARCHAR, schema.getField("name", false, false).getType().getSqlTypeName());
    assertEquals(SqlTypeName.INTEGER, schema.getField("count", false, false).getType().getSqlTypeName());
  }

  @Test
  public void testRowConversion() {
    // Test converting a full row
    RelDataTypeFactory typeFactory = new org.apache.calcite.sql.type.SqlTypeFactoryImpl(
        org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    
    RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    RelDataType stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 100);
    
    RelDataType schema = typeFactory.builder()
        .add("id", intType)
        .add("name", stringType)
        .add("count", intType)
        .build();
    
    // Input row with string values (as might come from Splunk JSON)
    Object[] inputRow = {"123", "test_name", "456"};
    
    // Convert the row
    Object[] convertedRow = SplunkDataConverter.convertRow(inputRow, schema);
    
    // Check that conversions happened correctly
    assertEquals(123, convertedRow[0]); // id converted to Integer
    assertTrue(convertedRow[0] instanceof Integer);
    
    assertEquals("test_name", convertedRow[1]); // name stays as String
    assertTrue(convertedRow[1] instanceof String);
    
    assertEquals(456, convertedRow[2]); // count converted to Integer
    assertTrue(convertedRow[2] instanceof Integer);
  }
}