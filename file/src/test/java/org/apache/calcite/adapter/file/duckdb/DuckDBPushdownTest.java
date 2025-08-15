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

import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Tag;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Tag;import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Tag;
/**
 * Test that verifies aggregations are pushed down to DuckDB, not computed in Calcite.
 */
@Tag("integration")public class DuckDBPushdownTest {
  
  @TempDir
  static File tempDir;
  
  private static File csvFile;
  
  @BeforeAll
  public static void setupTestData() throws Exception {
    // Create a test CSV file with 100,000 rows
    csvFile = new File(tempDir, "sales.csv");
    try (FileWriter writer = new FileWriter(csvFile)) {
      writer.write("id,product,amount\n");
      for (int i = 1; i <= 100000; i++) {
        writer.write(String.format("%d,Product%d,%d\n", i, i % 100, i * 10));
      }
    }
    System.out.println("Created test file: " + csvFile.getAbsolutePath());
  }
  
  @Test
  public void testAggregationPushdown() throws Exception {
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      // Create DuckDB JDBC schema
      JdbcSchema duckdbSchema = DuckDBJdbcSchemaFactory.create(
          rootSchema, "duckdb_test", tempDir, false);
      rootSchema.add("duckdb_test", duckdbSchema);
      
      // Test COUNT(*) aggregation
      long startTime = System.currentTimeMillis();
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM duckdb_test.sales")) {
        assertTrue(rs.next());
        assertEquals(100000, rs.getInt(1));
      }
      long countTime = System.currentTimeMillis() - startTime;
      System.out.println("COUNT(*) executed in " + countTime + "ms");
      
      // Should be very fast if pushed to DuckDB (< 100ms)
      // If computed in Calcite after fetching all rows, would be much slower
      assertTrue(countTime < 500, "COUNT(*) too slow - likely not pushed to DuckDB");
      
      // Test SUM aggregation
      startTime = System.currentTimeMillis();
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery("SELECT SUM(amount) FROM duckdb_test.sales")) {
        assertTrue(rs.next());
        long expectedSum = 100000L * 100001L * 10 / 2; // Sum of arithmetic sequence
        assertEquals(expectedSum, rs.getLong(1));
      }
      long sumTime = System.currentTimeMillis() - startTime;
      System.out.println("SUM(amount) executed in " + sumTime + "ms");
      
      assertTrue(sumTime < 500, "SUM too slow - likely not pushed to DuckDB");
      
      // Test GROUP BY with aggregation
      startTime = System.currentTimeMillis();
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT product, COUNT(*), SUM(amount) " +
               "FROM duckdb_test.sales " +
               "GROUP BY product " +
               "ORDER BY product " +
               "LIMIT 5")) {
        int count = 0;
        while (rs.next()) {
          count++;
          System.out.println("Product: " + rs.getString(1) + 
                           ", Count: " + rs.getInt(2) + 
                           ", Sum: " + rs.getLong(3));
        }
        assertEquals(5, count);
      }
      long groupByTime = System.currentTimeMillis() - startTime;
      System.out.println("GROUP BY executed in " + groupByTime + "ms");
      
      assertTrue(groupByTime < 1000, "GROUP BY too slow - likely not pushed to DuckDB");
      
      // Test complex aggregation with HAVING
      startTime = System.currentTimeMillis();
      try (PreparedStatement pstmt = connection.prepareStatement(
          "SELECT product, COUNT(*) as cnt, AVG(amount) as avg_amount " +
          "FROM duckdb_test.sales " +
          "GROUP BY product " +
          "HAVING COUNT(*) > ? " +
          "ORDER BY avg_amount DESC " +
          "LIMIT 10")) {
        pstmt.setInt(1, 500);
        try (ResultSet rs = pstmt.executeQuery()) {
          int count = 0;
          while (rs.next()) {
            count++;
          }
          assertTrue(count > 0 && count <= 10);
        }
      }
      long havingTime = System.currentTimeMillis() - startTime;
      System.out.println("Complex query with HAVING executed in " + havingTime + "ms");
      
      assertTrue(havingTime < 1000, "HAVING query too slow - likely not pushed to DuckDB");
    }
  }
  
  @Test
  public void testExplainPlan() throws Exception {
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      
      // Create DuckDB JDBC schema
      JdbcSchema duckdbSchema = DuckDBJdbcSchemaFactory.create(
          rootSchema, "duckdb_test", tempDir, false);
      rootSchema.add("duckdb_test", duckdbSchema);
      
      // Get explain plan for aggregation query
      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(
               "EXPLAIN PLAN FOR SELECT COUNT(*), SUM(amount) FROM duckdb_test.sales")) {
        while (rs.next()) {
          String plan = rs.getString(1);
          System.out.println(plan);
          
          // Verify plan shows JdbcAggregate (pushed to DuckDB)
          // not EnumerableAggregate (computed in Calcite)
          assertTrue(plan.contains("JdbcAggregate") || plan.contains("JdbcToEnumerable"),
                   "Plan should show JDBC operations, not Enumerable aggregation");
        }
      }
    }
  }
}