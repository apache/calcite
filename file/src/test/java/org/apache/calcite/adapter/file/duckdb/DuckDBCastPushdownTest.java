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
package org.apache.calcite.adapter.file.duckdb;

import org.apache.calcite.adapter.file.BaseFileTest;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test to verify CAST operations are properly pushed down to DuckDB.
 * This test only runs when the DUCKDB engine is configured.
 */
@Tag("unit")
@EnabledIfSystemProperty(named = "CALCITE_FILE_ENGINE_TYPE", matches = "DUCKDB")
public class DuckDBCastPushdownTest extends BaseFileTest {
  private File tempDir;
  private File modelFile;

  @BeforeEach
  public void setUp() throws Exception {
    tempDir = new File(System.getProperty("java.io.tmpdir"), "duckdb-cast-test-" + System.currentTimeMillis());
    tempDir.mkdirs();
    tempDir.deleteOnExit();
    
    // Create a JSON file with proper numeric types
    File jsonFile = new File(tempDir, "test_data.json");
    try (FileWriter writer = new FileWriter(jsonFile)) {
      writer.write("[\n");
      writer.write("  {\"id\": 1, \"price\": 10.50, \"name\": \"Widget\"},\n");
      writer.write("  {\"id\": 2, \"price\": 25.75, \"name\": \"Gadget\"},\n");
      writer.write("  {\"id\": 3, \"price\": 15.99, \"name\": \"Tool\"}\n");
      writer.write("]\n");
    }
    
    // Create model file
    modelFile = new File(tempDir, "model.json");
    try (FileWriter writer = new FileWriter(modelFile)) {
      writer.write("{\n");
      writer.write("  \"version\": \"1.0\",\n");
      writer.write("  \"defaultSchema\": \"TEST\",\n");
      writer.write("  \"schemas\": [\n");
      writer.write("    {\n");
      writer.write("      \"name\": \"TEST\",\n");
      writer.write("      \"type\": \"custom\",\n");
      writer.write("      \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n");
      writer.write("      \"operand\": {\n");
      writer.write("        \"directory\": \"" + tempDir.getAbsolutePath() + "\",\n");
      writer.write("        \"ephemeralCache\": true\n");
      writer.write("      }\n");
      writer.write("    }\n");
      writer.write("  ]\n");
      writer.write("}\n");
    }
  }

  @Test
  public void testCastInWhereClause() throws Exception {
    // Set to use DuckDB engine
    System.setProperty("CALCITE_FILE_ENGINE_TYPE", "DUCKDB");
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + modelFile.getAbsolutePath())) {
      try (Statement stmt = connection.createStatement()) {
        // This query uses CAST in WHERE clause
        String sql = "SELECT * FROM \"TEST\".\"test_data\" WHERE CAST(\"price\" AS DECIMAL) > 15.0";
        
        // Enable debug output to see the actual SQL sent to DuckDB
        System.setProperty("calcite.debug", "true");
        
        // Also check what SQL is generated
        try (ResultSet planRs = stmt.executeQuery("EXPLAIN PLAN FOR " + sql)) {
          System.out.println("Query plan:");
          while (planRs.next()) {
            System.out.println(planRs.getString(1));
          }
        }
        
        try {
          ResultSet rs = stmt.executeQuery(sql);
          int count = 0;
          while (rs.next()) {
            count++;
            double price = rs.getDouble("price");
            System.out.println("Row " + count + ": price=" + price + ", id=" + rs.getInt("id") + ", name=" + rs.getString("name"));
            assertTrue(price > 15.0, "Price should be greater than 15.0");
          }
          assertEquals(2, count, "Should have 2 rows with price > 15.0");
        } catch (Exception e) {
          // Log the error for debugging
          System.err.println("Query failed: " + e.getMessage());
          if (e.getMessage().contains("Cannot compare values of type VARCHAR")) {
            System.err.println("CAST was incorrectly removed during pushdown!");
          }
          throw e;
        }
      }
    } finally {
      System.clearProperty("CALCITE_FILE_ENGINE_TYPE");
      System.clearProperty("calcite.debug");
    }
  }

  @Test
  public void testCastWithArithmetic() throws Exception {
    System.setProperty("CALCITE_FILE_ENGINE_TYPE", "DUCKDB");
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + modelFile.getAbsolutePath())) {
      try (Statement stmt = connection.createStatement()) {
        // Test CAST with arithmetic operation - note that we need to explicitly SELECT the price column 
        // to verify the cast was preserved in the filter
        String sql = "SELECT \"name\", \"price\", CAST(\"price\" AS DECIMAL) * 2 AS double_price " +
                     "FROM \"TEST\".\"test_data\" WHERE CAST(\"price\" AS DECIMAL) * 2 > 30";
        
        System.setProperty("calcite.debug", "true");
        
        // Also check what SQL is generated
        try (ResultSet planRs = stmt.executeQuery("EXPLAIN PLAN FOR " + sql)) {
          System.out.println("Arithmetic query plan:");
          while (planRs.next()) {
            System.out.println(planRs.getString(1));
          }
        }
        
        try {
          ResultSet rs = stmt.executeQuery(sql);
          int count = 0;
          while (rs.next()) {
            count++;
            double doublePrice = rs.getDouble("double_price");
            assertTrue(doublePrice > 30, "Double price should be greater than 30");
          }
          assertEquals(2, count, "Should have 2 rows with double_price > 30");
        } catch (Exception e) {
          System.err.println("Query with arithmetic failed: " + e.getMessage());
          throw e;
        }
      }
    } finally {
      System.clearProperty("CALCITE_FILE_ENGINE_TYPE");
      System.clearProperty("calcite.debug");
    }
  }
}