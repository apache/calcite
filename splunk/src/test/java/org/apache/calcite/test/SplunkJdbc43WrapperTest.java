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
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Comprehensive tests for JDBC 4.3 compliant VARCHAR to numeric conversions
 * in the Splunk adapter using the ResultSet wrapper.
 *
 * <p>This test validates that the wrapper correctly intercepts conversion failures
 * and performs JDBC 4.3 compliant string-to-numeric conversions.</p>
 */
@Tag("integration")
public class SplunkJdbc43WrapperTest extends SplunkTestBase {

  @Override protected Connection getConnection() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");
    return super.getConnection();
  }

  /**
   * Gets the first available table from the Splunk schema for testing.
   */
  private static String getFirstAvailableTable(Connection conn) throws SQLException {
    // Try common Splunk tables first
    String[] commonTables = {"web", "authentication", "All_Traffic"};

    for (String tableName : commonTables) {
      try {
        String testQuery = "SELECT COUNT(*) FROM splunk.\"" + tableName + "\" LIMIT 1";
        try (PreparedStatement stmt = conn.prepareStatement(testQuery)) {
          stmt.executeQuery();
          return tableName; // If query works, table exists
        }
      } catch (SQLException e) {
        // Table doesn't exist or isn't accessible, try next
      }
    }

    // Fallback: try to discover tables dynamically
    String query = "SELECT \"TABLE_NAME\" FROM information_schema.\"TABLES\" " +
        "WHERE \"TABLE_SCHEMA\" = 'splunk' LIMIT 1";
    try (PreparedStatement stmt = conn.prepareStatement(query);
         ResultSet rs = stmt.executeQuery()) {
      if (rs.next()) {
        return rs.getString("TABLE_NAME");
      }
    }

    return "web"; // Default fallback table name
  }

  @Test public void testBasicIntegerConversion() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      // Get first available table from Splunk schema
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT '123' AS string_int FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next(), "Should have at least one row");

        // Test string retrieval (should always work)
        String stringValue = rs.getString("string_int");
        assertEquals("123", stringValue);

        // Test integer conversion with JDBC 4.3 wrapper
        int intValue = rs.getInt("string_int");
        assertEquals(123, intValue);
        assertFalse(rs.wasNull());
      }
    }
  }

  @Test public void testLongConversion() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT '9876543210' AS string_long FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        long longValue = rs.getLong("string_long");
        assertEquals(9876543210L, longValue);
        assertFalse(rs.wasNull());
      }
    }
  }

  @Test public void testDoubleConversion() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT '123.456' AS string_double FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        double doubleValue = rs.getDouble("string_double");
        assertEquals(123.456, doubleValue, 0.001);
        assertFalse(rs.wasNull());
      }
    }
  }

  @Test public void testFloatConversion() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT '78.9' AS string_float FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        float floatValue = rs.getFloat("string_float");
        assertEquals(78.9f, floatValue, 0.001f);
        assertFalse(rs.wasNull());
      }
    }
  }

  @Test public void testColumnIndexConversion() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT '42' AS string_num FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        // Test conversion by column index
        int intValue = rs.getInt(1);
        assertEquals(42, intValue);
      }
    }
  }

  @Test public void testDecimalStringToInteger() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT '456.78' AS decimal_string FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        // Should truncate decimal part when converting to integer
        int intValue = rs.getInt("decimal_string");
        assertEquals(456, intValue);
      }
    }
  }

  @Test public void testNullHandling() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT NULL AS null_field FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        // Test that null values are handled correctly
        int intValue = rs.getInt("null_field");
        assertTrue(rs.wasNull());
        assertEquals(0, intValue); // JDBC spec: return 0 for null integers

        long longValue = rs.getLong("null_field");
        assertTrue(rs.wasNull());
        assertEquals(0L, longValue);

        double doubleValue = rs.getDouble("null_field");
        assertTrue(rs.wasNull());
        assertEquals(0.0, doubleValue, 0.001);
      }
    }
  }

  @Test public void testInvalidConversions() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT 'not_a_number' AS invalid_num FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        // These should throw SQLException for invalid conversions
        assertThrows(SQLException.class, () -> rs.getInt("invalid_num"));
        assertThrows(SQLException.class, () -> rs.getLong("invalid_num"));
        assertThrows(SQLException.class, () -> rs.getDouble("invalid_num"));
        assertThrows(SQLException.class, () -> rs.getFloat("invalid_num"));
      }
    }
  }

  @Test public void testEmptyStringConversion() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT '' AS empty_string FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        // Empty strings should throw SQLException
        assertThrows(SQLException.class, () -> rs.getInt("empty_string"));
        assertThrows(SQLException.class, () -> rs.getLong("empty_string"));
        assertThrows(SQLException.class, () -> rs.getDouble("empty_string"));
      }
    }
  }

  @Test public void testWhitespaceHandling() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT '  123  ' AS padded_num FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        // Should handle leading/trailing whitespace correctly
        int intValue = rs.getInt("padded_num");
        assertEquals(123, intValue);
      }
    }
  }

  @Test public void testNegativeNumbers() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT '-456' AS negative_num FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        int intValue = rs.getInt("negative_num");
        assertEquals(-456, intValue);

        long longValue = rs.getLong("negative_num");
        assertEquals(-456L, longValue);

        double doubleValue = rs.getDouble("negative_num");
        assertEquals(-456.0, doubleValue, 0.001);
      }
    }
  }

  @Test public void testStatementWrapper() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT '789' AS stmt_test FROM splunk.\"" + tableName + "\"";

      // Test that Statement (not just PreparedStatement) also works
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {

        assertTrue(rs.next());

        int intValue = rs.getInt("stmt_test");
        assertEquals(789, intValue);
      }
    }
  }

  @Test public void testOverflowHandling() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      // Test integer overflow
      String query = "SELECT '99999999999999999999' AS overflow_num FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        // Should throw SQLException for integer overflow
        assertThrows(SQLException.class, () -> rs.getInt("overflow_num"));

        // Long might also overflow depending on the value
        assertThrows(SQLException.class, () -> rs.getLong("overflow_num"));

        // Double should handle very large numbers
        double doubleValue = rs.getDouble("overflow_num");
        assertTrue(doubleValue > 0); // Should be a valid positive number
      }
    }
  }

  @Test public void testMixedConversions() throws SQLException {
    assumeTrue(splunkAvailable, "Splunk not available");

    try (Connection conn = getConnection()) {
      String tableName = getFirstAvailableTable(conn);
      assumeTrue(tableName != null, "No tables available in Splunk schema");

      String query = "SELECT '123' AS str_int, '456.78' AS str_double, 'invalid' AS str_invalid "
          + "FROM splunk.\"" + tableName + "\"";

      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {

        assertTrue(rs.next());

        // Valid conversions should work
        assertEquals(123, rs.getInt("str_int"));
        assertEquals(456.78, rs.getDouble("str_double"), 0.001);

        // Invalid conversion should still throw exception
        assertThrows(SQLException.class, () -> rs.getInt("str_invalid"));
      }
    }
  }
}
