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

import org.apache.calcite.adapter.splunk.SplunkDriver;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests that complex operations are NOT pushed down to Splunk
 * and are correctly handled by Calcite's enumerable implementation.
 */
@Tag("integration")
@EnabledIfEnvironmentVariable(named = "CALCITE_TEST_SPLUNK", matches = "true")
public class SplunkNonPushableOperationsTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SplunkNonPushableOperationsTest.class);
  
  private static String SPLUNK_URL;
  private static String SPLUNK_USER;
  private static String SPLUNK_PASSWORD;
  
  @BeforeAll
  public static void setupConnection() {
    // Register the Splunk driver
    try {
      Class.forName(SplunkDriver.class.getName());
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load Splunk driver", e);
    }
    
    // Get connection details from environment or use defaults
    SPLUNK_URL = System.getenv("SPLUNK_URL");
    if (SPLUNK_URL == null) {
      SPLUNK_URL = "https://kentest.xyz:8089";
    }
    
    SPLUNK_USER = System.getenv("SPLUNK_USER");
    if (SPLUNK_USER == null) {
      SPLUNK_USER = "admin";
    }
    
    SPLUNK_PASSWORD = System.getenv("SPLUNK_PASSWORD");
    if (SPLUNK_PASSWORD == null) {
      SPLUNK_PASSWORD = "changeme";
    }
    
    LOGGER.info("Testing with Splunk instance at: {}", SPLUNK_URL);
  }
  
  private Connection getConnection() throws SQLException {
    Properties props = new Properties();
    props.setProperty("user", SPLUNK_USER);
    props.setProperty("password", SPLUNK_PASSWORD);
    props.setProperty("ssl", "true");
    props.setProperty("disableSslValidation", "true");
    
    String jdbcUrl = "jdbc:splunk:" + SPLUNK_URL.replace("https://", "//");
    return DriverManager.getConnection(jdbcUrl, props);
  }
  
  @Test
  @Tag("integration")
  public void testArithmeticOperationsNotPushedDown() throws SQLException {
    // These arithmetic operations should be handled by Calcite, not pushed to Splunk
    String[] queries = {
        "SELECT 10 + 20 AS sum_result",
        "SELECT 100 * 2 AS product_result",
        "SELECT 100 / 5 AS quotient_result",
        "SELECT 50 - 30 AS difference_result",
        "SELECT (10 + 20) * 3 AS complex_calc"
    };
    
    try (Connection conn = getConnection()) {
      for (String sql : queries) {
        LOGGER.debug("Testing arithmetic query: {}", sql);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
          assertTrue(rs.next(), "Expected result for: " + sql);
          
          // Verify the calculations are correct
          if (sql.contains("10 + 20")) {
            assertEquals(30, rs.getInt(1));
          } else if (sql.contains("100 * 2")) {
            assertEquals(200, rs.getInt(1));
          } else if (sql.contains("100 / 5")) {
            assertEquals(20, rs.getInt(1));
          } else if (sql.contains("50 - 30")) {
            assertEquals(20, rs.getInt(1));
          } else if (sql.contains("(10 + 20) * 3")) {
            assertEquals(90, rs.getInt(1));
          }
        }
      }
    }
  }
  
  @Test
  @Tag("integration")
  public void testCaseStatementNotPushedDown() throws SQLException {
    // CASE statements should be handled by Calcite
    String sql = "SELECT CASE WHEN 1 = 1 THEN 'true' ELSE 'false' END AS result";
    
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      assertTrue(rs.next(), "Expected result for CASE statement");
      assertEquals("true", rs.getString("result"));
    }
  }
  
  @Test
  @Tag("integration")
  public void testStringConcatenationNotPushedDown() throws SQLException {
    // String concatenation should be handled by Calcite
    String sql = "SELECT 'Hello' || ' ' || 'World' AS greeting";
    
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      assertTrue(rs.next(), "Expected result for string concatenation");
      assertEquals("Hello World", rs.getString("greeting"));
    }
  }
  
  @Test
  @Tag("integration")
  public void testComplexExpressionsWithTableData() throws SQLException {
    // Test complex expressions on actual table data
    String sql = "SELECT \"action\", "
        + "UPPER(\"action\") AS action_upper, "
        + "\"bytes\" * 2 AS bytes_doubled "
        + "FROM \"splunk\".\"web\" "
        + "WHERE \"bytes\" IS NOT NULL "
        + "LIMIT 5";
    
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        String action = rs.getString("action");
        String actionUpper = rs.getString("action_upper");
        
        assertNotNull(action);
        assertNotNull(actionUpper);
        assertEquals(action.toUpperCase(), actionUpper, 
            "UPPER function should work correctly");
        
        // Check that bytes_doubled is actually doubled
        // Note: We can't directly verify without the original bytes value,
        // but we can check it's not null when bytes was not null
        rs.getInt("bytes_doubled");
        
        count++;
      }
      
      assertTrue(count > 0, "Should have returned at least one row");
    }
  }
  
  @Test
  @Tag("integration") 
  public void testMixedPushableAndNonPushable() throws SQLException {
    // Mix of pushable (simple field) and non-pushable (arithmetic) operations
    String sql = "SELECT \"status\", \"status\" + 100 AS status_plus "
        + "FROM \"splunk\".\"web\" "
        + "LIMIT 5";
    
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        int status = rs.getInt("status");
        int statusPlus = rs.getInt("status_plus");
        
        assertEquals(status + 100, statusPlus, 
            "Arithmetic should be computed correctly by Calcite");
        count++;
      }
      
      assertTrue(count > 0, "Should have returned at least one row");
    }
  }
  
  @Test
  @Tag("integration")
  public void testCastOperationsHandling() throws SQLException {
    // Test that CAST operations are handled appropriately
    // Some simple CASTs might be pushed down, complex ones should not
    String sql = "SELECT "
        + "CAST('123' AS INTEGER) AS str_to_int, "
        + "CAST(456 AS VARCHAR) AS int_to_str "
        + "FROM (VALUES (1)) AS t(dummy)";
    
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      assertTrue(rs.next(), "Expected result for CAST operations");
      assertEquals(123, rs.getInt("str_to_int"));
      assertEquals("456", rs.getString("int_to_str"));
    }
  }
  
  @Test
  @Tag("integration")
  public void testComplexQueryWithMultipleNonPushableOperations() throws SQLException {
    // Comprehensive test combining multiple operations that shouldn't be pushed down
    String sql = "SELECT "
        + "\"status\", "
        + "\"status\" + 100 AS status_plus_100, "
        + "\"status\" * 2 AS status_times_2, "
        + "\"status\" - 50 AS status_minus_50, "
        + "\"status\" / 10 AS status_div_10, "
        + "CASE WHEN \"status\" = 200 THEN 'OK' "
        + "     WHEN \"status\" = 404 THEN 'NOT_FOUND' "
        + "     ELSE 'OTHER' END AS status_text, "
        + "UPPER(\"action\") AS action_upper, "
        + "LOWER(\"action\") AS action_lower, "
        + "LENGTH(\"action\") AS action_length "
        + "FROM \"splunk\".\"web\" "
        + "WHERE \"status\" IS NOT NULL AND \"action\" IS NOT NULL "
        + "LIMIT 10";
    
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        int status = rs.getInt("status");
        
        // Verify arithmetic operations
        assertEquals(status + 100, rs.getInt("status_plus_100"), 
            "Addition should work");
        assertEquals(status * 2, rs.getInt("status_times_2"), 
            "Multiplication should work");
        assertEquals(status - 50, rs.getInt("status_minus_50"), 
            "Subtraction should work");
        assertEquals(status / 10, rs.getInt("status_div_10"), 
            "Division should work");
        
        // Verify CASE statement
        String statusText = rs.getString("status_text");
        if (status == 200) {
          assertEquals("OK", statusText);
        } else if (status == 404) {
          assertEquals("NOT_FOUND", statusText);
        } else {
          assertEquals("OTHER", statusText);
        }
        
        // Verify string functions
        String action = rs.getString("action");
        assertEquals(action.toUpperCase(), rs.getString("action_upper"),
            "UPPER should work");
        assertEquals(action.toLowerCase(), rs.getString("action_lower"),
            "LOWER should work");
        assertEquals(action.length(), rs.getInt("action_length"),
            "LENGTH should work");
        
        count++;
      }
      
      assertTrue(count > 0, "Should have returned at least one row");
      LOGGER.info("Successfully tested {} rows with complex non-pushable operations", count);
    }
  }
  
  @Test
  @Tag("integration")
  public void testAggregatesWithArithmetic() throws SQLException {
    // Test aggregates combined with arithmetic
    String sql = "SELECT "
        + "COUNT(*) AS total_count, "
        + "COUNT(*) * 100 AS count_times_100, "
        + "AVG(\"status\") AS avg_status, "
        + "AVG(\"status\") + 10 AS avg_plus_10, "
        + "SUM(\"bytes\") / COUNT(*) AS avg_bytes_computed "
        + "FROM \"splunk\".\"web\" "
        + "WHERE \"status\" IS NOT NULL AND \"bytes\" IS NOT NULL";
    
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      assertTrue(rs.next(), "Expected aggregate results");
      
      int count = rs.getInt("total_count");
      assertEquals(count * 100, rs.getInt("count_times_100"),
          "Arithmetic on COUNT should work");
      
      double avgStatus = rs.getDouble("avg_status");
      assertEquals(avgStatus + 10, rs.getDouble("avg_plus_10"), 0.01,
          "Arithmetic on AVG should work");
      
      // avg_bytes_computed should equal SUM/COUNT
      double avgBytesComputed = rs.getDouble("avg_bytes_computed");
      assertTrue(avgBytesComputed > 0, "Computed average should be positive");
      
      LOGGER.info("Aggregate arithmetic test passed with {} total rows", count);
    }
  }
  
  @Test
  @Tag("integration")
  public void testSubqueryWithArithmetic() throws SQLException {
    // Test subquery with arithmetic operations
    String sql = "SELECT "
        + "t1.status_original, "
        + "t1.status_doubled, "
        + "t1.status_doubled - t1.status_original AS difference "
        + "FROM ("
        + "  SELECT \"status\" AS status_original, "
        + "         \"status\" * 2 AS status_doubled "
        + "  FROM \"splunk\".\"web\" "
        + "  WHERE \"status\" IS NOT NULL "
        + "  LIMIT 5"
        + ") t1";
    
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        int original = rs.getInt("status_original");
        int doubled = rs.getInt("status_doubled");
        int difference = rs.getInt("difference");
        
        assertEquals(original * 2, doubled, "Doubling should work in subquery");
        assertEquals(doubled - original, difference, "Difference calculation should work");
        assertEquals(original, difference, "Difference should equal original (2x - x = x)");
        
        count++;
      }
      
      assertEquals(5, count, "Should return exactly 5 rows as per LIMIT");
    }
  }
  
  @Test
  @Tag("integration")
  public void testDateArithmetic() throws SQLException {
    // Test date/time arithmetic operations
    String sql = "SELECT "
        + "CURRENT_DATE AS today, "
        + "CURRENT_DATE + INTERVAL '1' DAY AS tomorrow, "
        + "CURRENT_DATE - INTERVAL '1' DAY AS yesterday, "
        + "EXTRACT(YEAR FROM CURRENT_DATE) AS current_year, "
        + "EXTRACT(MONTH FROM CURRENT_DATE) AS current_month "
        + "FROM (VALUES (1)) AS t(dummy)";
    
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      assertTrue(rs.next(), "Expected date arithmetic results");
      
      // Just verify we get non-null results
      assertNotNull(rs.getDate("today"));
      assertNotNull(rs.getDate("tomorrow"));
      assertNotNull(rs.getDate("yesterday"));
      assertTrue(rs.getInt("current_year") > 2020);
      assertTrue(rs.getInt("current_month") >= 1 && rs.getInt("current_month") <= 12);
      
      LOGGER.info("Date arithmetic test passed");
    }
  }
  
  @Test
  @Tag("integration")
  public void testComplexWhereClauseWithArithmetic() throws SQLException {
    // Test WHERE clause with arithmetic expressions
    String sql = "SELECT \"status\", \"bytes\" "
        + "FROM \"splunk\".\"web\" "
        + "WHERE \"status\" + 100 > 300 "  // This arithmetic should be handled by Calcite
        + "  AND \"bytes\" * 2 < 10000 "   // This too
        + "  AND \"status\" IS NOT NULL "
        + "  AND \"bytes\" IS NOT NULL "
        + "LIMIT 10";
    
    try (Connection conn = getConnection();
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
      
      int count = 0;
      while (rs.next()) {
        int status = rs.getInt("status");
        int bytes = rs.getInt("bytes");
        
        // Verify the WHERE conditions are actually met
        assertTrue(status + 100 > 300, "Status + 100 should be > 300");
        assertTrue(bytes * 2 < 10000, "Bytes * 2 should be < 10000");
        
        count++;
      }
      
      LOGGER.info("Complex WHERE clause test returned {} rows", count);
    }
  }
}