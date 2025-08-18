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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Integration tests for JDBC 4.3 compliant VARCHAR to numeric conversions
 * in the Splunk adapter.
 * 
 * This tests that when Splunk returns numeric values as strings (which is common),
 * we can still read them using numeric getters like getInt(), getLong(), etc.
 */
@Tag("integration")
public class SplunkVarcharNumericIntegrationTest {

  private static boolean splunkAvailable = false;
  
  @BeforeAll
  public static void checkSplunkConnection() {
    // Check if Splunk test environment is available
    String splunkUrl = System.getenv("SPLUNK_URL");
    String splunkToken = System.getenv("SPLUNK_TOKEN");
    String splunkUser = System.getenv("SPLUNK_USER");
    String splunkPassword = System.getenv("SPLUNK_PASSWORD");
    
    splunkAvailable = splunkUrl != null && 
        (splunkToken != null || (splunkUser != null && splunkPassword != null));
    
    if (!splunkAvailable) {
      System.out.println("Splunk connection not configured. Set SPLUNK_URL and either "
          + "SPLUNK_TOKEN or SPLUNK_USER/SPLUNK_PASSWORD to run integration tests.");
    }
  }

  private Connection getConnection() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    
    Properties props = new Properties();
    props.setProperty("url", System.getenv("SPLUNK_URL"));
    
    String token = System.getenv("SPLUNK_TOKEN");
    if (token != null) {
      props.setProperty("token", token);
    } else {
      props.setProperty("user", System.getenv("SPLUNK_USER"));
      props.setProperty("password", System.getenv("SPLUNK_PASSWORD"));
    }
    
    // Disable SSL validation for test environments
    props.setProperty("disableSslValidation", "true");
    
    return DriverManager.getConnection("jdbc:splunk:", props);
  }

  @Test
  public void testNumericStringAsInteger() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    
    try (Connection conn = getConnection()) {
      // Create a query that returns a numeric value as a string
      // Using eval to force string conversion, then reading as integer
      String query = "SELECT CAST(status AS VARCHAR) AS status_str, "
          + "CAST(status AS INTEGER) AS status_int "
          + "FROM (SELECT 200 AS status FROM splunk.\"search index=_internal | head 1\") AS t";
      
      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {
        
        assertTrue(rs.next(), "Should have at least one row");
        
        // Test that we can read a VARCHAR field containing '200' as an integer
        int statusFromString = rs.getInt("status_str");
        assertEquals(200, statusFromString, "Should convert VARCHAR '200' to INTEGER 200");
        
        // Verify the regular integer field still works
        int statusInt = rs.getInt("status_int");
        assertEquals(200, statusInt);
      }
    }
  }

  @Test
  public void testNumericStringAsLong() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    
    try (Connection conn = getConnection()) {
      // Query that returns bytes as string
      String query = "SELECT CAST('1234567890' AS VARCHAR) AS bytes_str "
          + "FROM splunk.\"search index=_internal | head 1\"";
      
      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {
        
        assertTrue(rs.next());
        
        // Test VARCHAR to LONG conversion
        long bytes = rs.getLong("bytes_str");
        assertEquals(1234567890L, bytes);
      }
    }
  }

  @Test
  public void testNumericStringAsDouble() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    
    try (Connection conn = getConnection()) {
      // Query that returns a decimal value as string
      String query = "SELECT CAST('123.456' AS VARCHAR) AS duration_str "
          + "FROM splunk.\"search index=_internal | head 1\"";
      
      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {
        
        assertTrue(rs.next());
        
        // Test VARCHAR to DOUBLE conversion
        double duration = rs.getDouble("duration_str");
        assertEquals(123.456, duration, 0.001);
      }
    }
  }

  @Test
  public void testRealWorldSplunkQuery() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    
    try (Connection conn = getConnection()) {
      // Real-world Splunk query where numeric fields might be returned as strings
      String query = "SELECT "
          + "CAST(COUNT(*) AS VARCHAR) AS count_str, "
          + "COUNT(*) AS count_num "
          + "FROM splunk.\"search index=_internal | stats count\"";
      
      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {
        
        assertTrue(rs.next());
        
        // Test that count_str (VARCHAR) can be read as integer
        int countFromString = rs.getInt("count_str");
        int countNum = rs.getInt("count_num");
        
        // Both should return the same value
        assertEquals(countNum, countFromString);
        assertTrue(countFromString > 0, "Count should be positive");
      }
    }
  }

  @Test
  public void testInvalidConversion() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    
    try (Connection conn = getConnection()) {
      // Query that returns a non-numeric string
      String query = "SELECT 'not_a_number' AS text_field "
          + "FROM splunk.\"search index=_internal | head 1\"";
      
      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {
        
        assertTrue(rs.next());
        
        // This should throw an exception when trying to convert non-numeric string to int
        boolean exceptionThrown = false;
        try {
          rs.getInt("text_field");
        } catch (SQLException e) {
          exceptionThrown = true;
          // Expected exception - VARCHAR 'not_a_number' cannot be converted to INTEGER
        }
        
        assertTrue(exceptionThrown, "Should throw exception for invalid conversion");
      }
    }
  }

  @Test 
  public void testNullHandling() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    
    try (Connection conn = getConnection()) {
      // Query that might return null values
      String query = "SELECT NULL AS null_field "
          + "FROM splunk.\"search index=_internal | head 1\"";
      
      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {
        
        assertTrue(rs.next());
        
        // Test null handling
        int value = rs.getInt("null_field");
        assertTrue(rs.wasNull(), "Should indicate null was read");
        assertEquals(0, value, "getInt() should return 0 for null per JDBC spec");
      }
    }
  }
}