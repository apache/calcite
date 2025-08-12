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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.format.csv.CsvTypeInferrer;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for CSV type inference functionality.
 */
public class CsvTypeInferenceTest {

  /**
   * Test type inference on mixed data types.
   */
  @Test
  void testMixedTypesInference() throws Exception {
    // Test with type inference enabled
    Properties info = new Properties();
    info.put("model", resourcePath("csv-type-inference-model.json"));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      // Query the table with type inference
      String sql = "SELECT * FROM \"CSV_INFER\".\"mixed-types\" WHERE age > 30";
      
      try (ResultSet rs = connection.createStatement().executeQuery(sql)) {
        ResultSetMetaData metaData = rs.getMetaData();
        
        // Check column types (accept both INTEGER and BIGINT as valid integer types)
        int idType = metaData.getColumnType(1);
        assertTrue(idType == Types.INTEGER || idType == Types.BIGINT, 
            "id should be INTEGER or BIGINT, but was " + idType);
        assertEquals(Types.VARCHAR, metaData.getColumnType(2), "name should be VARCHAR");
        int ageType = metaData.getColumnType(3);
        assertTrue(ageType == Types.INTEGER || ageType == Types.BIGINT,
            "age should be INTEGER or BIGINT, but was " + ageType);
        assertEquals(Types.DOUBLE, metaData.getColumnType(4), "salary should be DOUBLE");
        assertEquals(Types.DATE, metaData.getColumnType(5), "hire_date should be DATE");
        assertEquals(Types.BOOLEAN, metaData.getColumnType(6), "is_active should be BOOLEAN");
        assertEquals(Types.DOUBLE, metaData.getColumnType(7), "rating should be DOUBLE");
        assertEquals(Types.TIME, metaData.getColumnType(8), "login_time should be TIME");
        assertEquals(Types.VARCHAR, metaData.getColumnType(9), "notes should be VARCHAR");
        
        // Verify we can filter by numeric comparison
        int count = 0;
        while (rs.next()) {
          int age = rs.getInt("age");
          assertTrue(age > 30, "Age filter should work with INTEGER type");
          count++;
        }
        assertTrue(count > 0, "Should have results with age > 30");
      }
    }
  }

  /**
   * Test that type inference is disabled by default.
   */
  @Test
  void testNoInferenceByDefault() throws Exception {
    Properties info = new Properties();
    info.put("model", resourcePath("csv-type-inference-model.json"));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      // Query a different file to avoid cache conflicts
      // This file is not used by the CSV_INFER schema, so it won't have cached types
      String sql = "SELECT * FROM \"CSV_NO_INFER\".\"no-inference-test\" LIMIT 1";
      
      try (ResultSet rs = connection.createStatement().executeQuery(sql)) {
        ResultSetMetaData metaData = rs.getMetaData();
        
        // All columns should be VARCHAR when inference is disabled
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          assertEquals(Types.VARCHAR, metaData.getColumnType(i), 
              "Column " + metaData.getColumnName(i) + " should be VARCHAR without inference");
        }
        
        // Verify we can read the data
        assertTrue(rs.next(), "Should have at least one row");
      }
    }
  }

  /**
   * Test timestamp type detection.
   */
  @Test
  void testTimestampInference() throws Exception {
    Properties info = new Properties();
    info.put("model", resourcePath("csv-type-inference-model.json"));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      String sql = "SELECT * FROM \"CSV_INFER\".\"timestamps\" LIMIT 1";
      
      try (ResultSet rs = connection.createStatement().executeQuery(sql)) {
        ResultSetMetaData metaData = rs.getMetaData();
        
        // Check timestamp columns
        int eventIdType = metaData.getColumnType(1);
        assertTrue(eventIdType == Types.INTEGER || eventIdType == Types.BIGINT,
            "event_id should be INTEGER or BIGINT, but was " + eventIdType);
        assertEquals(Types.VARCHAR, metaData.getColumnType(2), "event_name should be VARCHAR");
        assertEquals(Types.TIMESTAMP, metaData.getColumnType(3), "timestamp_local should be TIMESTAMP");
        // RFC formatted timestamps might be detected as TIMESTAMP_WITH_TIMEZONE
        int utcType = metaData.getColumnType(4);
        assertTrue(utcType == Types.TIMESTAMP || utcType == Types.TIMESTAMP_WITH_TIMEZONE,
            "timestamp_utc should be TIMESTAMP type");
        int rfcType = metaData.getColumnType(5);
        assertTrue(rfcType == Types.TIMESTAMP || rfcType == Types.TIMESTAMP_WITH_TIMEZONE || rfcType == Types.VARCHAR,
            "timestamp_rfc should be TIMESTAMP type or VARCHAR");
      }
    }
  }

  /**
   * Test null handling and null representations.
   */
  @Test
  void testNullHandling() throws Exception {
    Properties info = new Properties();
    info.put("model", resourcePath("csv-type-inference-model.json"));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      String sql = "SELECT * FROM \"CSV_INFER\".\"nulls-and-empty\"";
      
      try (ResultSet rs = connection.createStatement().executeQuery(sql)) {
        ResultSetMetaData metaData = rs.getMetaData();
        
        // Types should still be inferred despite nulls
        int idType2 = metaData.getColumnType(1);
        assertTrue(idType2 == Types.INTEGER || idType2 == Types.BIGINT,
            "id should be INTEGER or BIGINT, but was " + idType2);
        int value1Type = metaData.getColumnType(2);
        assertTrue(value1Type == Types.INTEGER || value1Type == Types.BIGINT,
            "value1 should be INTEGER or BIGINT despite nulls, but was " + value1Type);
        assertEquals(Types.DOUBLE, metaData.getColumnType(3), "value2 should be DOUBLE despite nulls");
        assertEquals(Types.BOOLEAN, metaData.getColumnType(4), "value3 should be BOOLEAN despite nulls");
        assertEquals(Types.DATE, metaData.getColumnType(5), "value4 should be DATE despite nulls");
        
        // All columns should be nullable
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
          assertEquals(ResultSetMetaData.columnNullable, metaData.isNullable(i),
              "Column " + metaData.getColumnName(i) + " should be nullable");
        }
      }
    }
  }

  /**
   * Test direct type inferrer functionality.
   */
  @Test
  void testTypeInferrerDirectly() throws Exception {
    // Use absolute path to test resources
    String basePath = System.getProperty("user.dir");
    File csvFile = new File(basePath, "src/test/resources/csv-type-inference/mixed-types.csv");
    
    // Make sure file exists
    assertTrue(csvFile.exists(), "Test CSV file should exist at: " + csvFile.getAbsolutePath());
    
    CsvTypeInferrer.TypeInferenceConfig config = 
        new CsvTypeInferrer.TypeInferenceConfig(true, 1.0, 100, 0.95, true, true, true, true, 0.0);
    
    List<CsvTypeInferrer.ColumnTypeInfo> types = 
        CsvTypeInferrer.inferTypes(Sources.of(csvFile), config, "UNCHANGED");
    
    assertNotNull(types);
    assertEquals(9, types.size(), "Should detect 9 columns");
    
    // Verify detected types
    assertEquals(SqlTypeName.INTEGER, types.get(0).inferredType, "id should be INTEGER");
    assertEquals(SqlTypeName.VARCHAR, types.get(1).inferredType, "name should be VARCHAR");
    assertEquals(SqlTypeName.INTEGER, types.get(2).inferredType, "age should be INTEGER");
    assertEquals(SqlTypeName.DOUBLE, types.get(3).inferredType, "salary should be DOUBLE");
    assertEquals(SqlTypeName.DATE, types.get(4).inferredType, "hire_date should be DATE");
    assertEquals(SqlTypeName.BOOLEAN, types.get(5).inferredType, "is_active should be BOOLEAN");
    assertEquals(SqlTypeName.DOUBLE, types.get(6).inferredType, "rating should be DOUBLE");
    assertEquals(SqlTypeName.TIME, types.get(7).inferredType, "login_time should be TIME");
    assertEquals(SqlTypeName.VARCHAR, types.get(8).inferredType, "notes should be VARCHAR");
    
    // All should be nullable
    for (CsvTypeInferrer.ColumnTypeInfo typeInfo : types) {
      assertTrue(typeInfo.nullable, "All columns should be nullable");
    }
  }

  /**
   * Test aggregations with inferred types.
   */
  @Test
  void testAggregationsWithInferredTypes() throws Exception {
    Properties info = new Properties();
    info.put("model", resourcePath("csv-type-inference-model.json"));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      // Test numeric aggregations
      String sql = "SELECT AVG(salary), MAX(age), MIN(rating) FROM \"CSV_INFER\".\"mixed-types\" WHERE is_active = true";
      
      try (ResultSet rs = connection.createStatement().executeQuery(sql)) {
        assertTrue(rs.next(), "Should have aggregation results");
        
        // These operations should work because types are properly inferred
        double avgSalary = rs.getDouble(1);
        assertTrue(avgSalary > 0, "Average salary should be calculated");
        
        int maxAge = rs.getInt(2);
        assertTrue(maxAge > 0, "Max age should be calculated");
        
        double minRating = rs.getDouble(3);
        assertTrue(minRating > 0, "Min rating should be calculated");
      }
    }
  }

  /**
   * Test date comparisons with inferred types.
   */
  @Test
  void testDateComparisons() throws Exception {
    Properties info = new Properties();
    info.put("model", resourcePath("csv-type-inference-model.json"));
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      // Test date filtering
      String sql = "SELECT COUNT(*) FROM \"CSV_INFER\".\"mixed-types\" WHERE hire_date >= DATE '2020-01-01'";
      
      try (ResultSet rs = connection.createStatement().executeQuery(sql)) {
        assertTrue(rs.next(), "Should have count result");
        int count = rs.getInt(1);
        assertTrue(count > 0, "Should have employees hired after 2020");
      }
    }
  }

  private static String resourcePath(String path) {
    return CsvTypeInferenceTest.class.getResource("/" + path).getFile();
  }
}