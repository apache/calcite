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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test to reproduce the JDBC 4.3 conversion issue.
 * This test demonstrates the exact problem the user is experiencing.
 */
@Tag("integration") 
public class SplunkJdbc43ReproducerTest extends SplunkTestBase {

  @Override
  protected Connection getConnection() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    return super.getConnection();
  }

  @Test
  public void testVarcharToIntConversionIssue() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    
    try (Connection conn = getConnection()) {
      // Create a query that explicitly returns a string value
      // This should trigger the conversion with our wrapper
      String query = "SELECT '123' AS string_number";
      
      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {
        
        assertTrue(rs.next(), "Should have at least one row");
        
        // First verify we can get it as a string (this should work)
        String stringValue = rs.getString("string_number");
        assertEquals("123", stringValue);
        
        // Now try to get it as an integer - this should work with the wrapper
        int intValue = rs.getInt("string_number");
        assertEquals(123, intValue);
        System.out.println("SUCCESS: VARCHAR '123' converted to INTEGER 123");
      }
    }
  }

  @Test
  public void testLongConversionIssue() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    
    try (Connection conn = getConnection()) {
      String query = "SELECT '456789' AS string_long";
      
      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {
        
        assertTrue(rs.next());
        
        long longValue = rs.getLong("string_long");
        assertEquals(456789L, longValue);
        System.out.println("SUCCESS: VARCHAR '456789' converted to BIGINT 456789");
      }
    }
  }

  @Test
  public void testDoubleConversionIssue() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    
    try (Connection conn = getConnection()) {
      String query = "SELECT '123.45' AS string_double";
      
      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {
        
        assertTrue(rs.next());
        
        double doubleValue = rs.getDouble("string_double");
        assertEquals(123.45, doubleValue, 0.001);
        System.out.println("SUCCESS: VARCHAR '123.45' converted to DOUBLE 123.45");
      }
    }
  }

  @Test
  public void testAlreadyNumericFieldWorks() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    
    try (Connection conn = getConnection()) {
      // Test that actual numeric fields work fine (they should)
      String query = "SELECT 789 AS actual_number";
      
      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {
        
        assertTrue(rs.next());
        
        // This should work fine since it's actually an integer
        int intValue = rs.getInt("actual_number");
        assertEquals(789, intValue);
        System.out.println("SUCCESS: Actual INTEGER 789 accessed correctly");
      }
    }
  }
}