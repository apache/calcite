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

import org.apache.calcite.adapter.file.execution.duckdb.DuckDBExecutionEngine;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for DuckDB execution engine.
 */
@Tag("integration")
public class DuckDBIntegrationTest {

  @TempDir
  Path tempDir;

  @BeforeAll
  public static void checkDuckDB() {
    assumeTrue(DuckDBExecutionEngine.isAvailable(), 
               "DuckDB not available, skipping tests");
  }

  @Test
  public void testSimpleQuery() throws Exception {
    // Create test Parquet file
    createTestParquet(tempDir, "test_data", 100);
    
    // Create model
    String modelJson = createModel(tempDir, "DUCKDB");
    Path modelPath = tempDir.resolve("model.json");
    Files.write(modelPath, modelJson.getBytes());
    
    Properties props = new Properties();
    props.put("model", modelPath.toString());
    props.put("lex", "ORACLE");
    props.put("unquotedCasing", "TO_LOWER");
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_data")) {
      
      assertTrue(rs.next());
      assertEquals(100, rs.getInt(1));
    }
  }

  @Test
  public void testAggregation() throws Exception {
    // Create test Parquet file
    createTestParquet(tempDir, "sales", 1000);
    
    // Create model
    String modelJson = createModel(tempDir, "DUCKDB");
    Path modelPath = tempDir.resolve("model.json");
    Files.write(modelPath, modelJson.getBytes());
    
    Properties props = new Properties();
    props.put("model", modelPath.toString());
    props.put("lex", "ORACLE");
    props.put("unquotedCasing", "TO_LOWER");
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT category, COUNT(*), AVG(amount) " +
             "FROM sales GROUP BY category ORDER BY category")) {
      
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        assertNotNull(rs.getObject(1)); // category
        assertTrue(rs.getInt(2) > 0);   // count
        assertTrue(rs.getDouble(3) >= 0); // avg
      }
      assertTrue(rowCount > 0);
    }
  }

  @Test
  public void testJoinQuery() throws Exception {
    // Create two test Parquet files
    createCustomersParquet(tempDir);
    createOrdersParquet(tempDir);
    
    // Create model
    String modelJson = createModel(tempDir, "DUCKDB");
    Path modelPath = tempDir.resolve("model.json");
    Files.write(modelPath, modelJson.getBytes());
    
    Properties props = new Properties();
    props.put("model", modelPath.toString());
    props.put("lex", "ORACLE");
    props.put("unquotedCasing", "TO_LOWER");
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(
             "SELECT c.name, COUNT(o.order_id) as order_count " +
             "FROM customers c " +
             "JOIN orders o ON c.customer_id = o.customer_id " +
             "GROUP BY c.name " +
             "ORDER BY order_count DESC " +
             "LIMIT 10")) {
      
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        assertNotNull(rs.getString(1)); // name
        assertTrue(rs.getInt(2) >= 0);  // order_count
      }
      assertTrue(rowCount > 0 && rowCount <= 10);
    }
  }

  private void createTestParquet(Path dir, String tableName, int rows) throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {
      
      stmt.execute(String.format(
          "CREATE TABLE %s AS " +
          "SELECT " +
          "  i as id, " +
          "  i %% 10 as category, " +
          "  random() * 100 as amount " +
          "FROM generate_series(1, %d) as t(i)",
          tableName, rows));
      
      stmt.execute(String.format(
          "COPY %s TO '%s/%s.parquet' (FORMAT PARQUET)",
          tableName, dir.toString(), tableName));
    }
  }

  private void createCustomersParquet(Path dir) throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {
      
      stmt.execute(
          "CREATE TABLE customers AS " +
          "SELECT " +
          "  i as customer_id, " +
          "  'Customer_' || i as name " +
          "FROM generate_series(1, 100) as t(i)");
      
      stmt.execute(String.format(
          "COPY customers TO '%s/customers.parquet' (FORMAT PARQUET)",
          dir.toString()));
    }
  }

  private void createOrdersParquet(Path dir) throws Exception {
    Class.forName("org.duckdb.DuckDBDriver");
    
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:");
         Statement stmt = conn.createStatement()) {
      
      stmt.execute(
          "CREATE TABLE orders AS " +
          "SELECT " +
          "  i as order_id, " +
          "  (random() * 100)::INT + 1 as customer_id, " +
          "  random() * 1000 as amount " +
          "FROM generate_series(1, 500) as t(i)");
      
      stmt.execute(String.format(
          "COPY orders TO '%s/orders.parquet' (FORMAT PARQUET)",
          dir.toString()));
    }
  }

  private String createModel(Path dir, String engine) {
    return String.format(
        "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"test\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"test\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n" +
        "    \"operand\": {\n" +
        "      \"directory\": \"%s\",\n" +
        "      \"executionEngine\": \"%s\"\n" +
        "    }\n" +
        "  }]\n" +
        "}", dir.toString(), engine);
  }
}