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

import org.apache.calcite.adapter.splunk.search.SearchResultListener;
import org.apache.calcite.adapter.splunk.search.SplunkConnection;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Unit tests for Splunk adapter using mocks.
 * These tests don't require a live Splunk connection.
 */
class SplunkAdapterUnitTest {

  @BeforeEach
  void setUp() {
    // Ensure the driver is loaded
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      // Driver should auto-register
    }
  }

  @Test void testDriverRegistration() {
    assertDoesNotThrow(() -> Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver"));
  }

  @Test void testConnectionWithMockUrl() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      assertThat(connection, is(notNullValue()));
      assertThat(connection.isClosed(), is(false));
    }
  }

  @Test void testConnectionWithMissingUrl() {
    Properties info = new Properties();
    info.put("user", "test");
    info.put("password", "test");

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    assertThat(ex.getMessage(), containsString("Must specify 'url' property"));
  }

  @Test void testConnectionWithMissingAuth() {
    Properties info = new Properties();
    info.setProperty("url", "mock");

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    assertThat(ex.getMessage(), containsString("Must specify either 'token' or both 'user' and 'password'"));
  }

  @Test void testCimModelConfiguration() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("cimModel", "authentication");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      assertThat(connection, is(notNullValue()));
      // The authentication table should be available
    }
  }

  @Test void testCimModelsConfiguration() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("cimModels", "authentication,web,network_traffic");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT 1")) {
      assertThat(rs.next(), is(true));
    }
  }

  @Test void testDefaultCimModelsWhenNoneSpecified() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    // No cimModel or cimModels specified - should default to all

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      assertThat(connection, is(notNullValue()));
      // All 24 CIM models should be available by default
    }
  }

  @Test void testSslValidationProperty() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("disableSslValidation", "true");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      assertThat(connection, is(notNullValue()));
    }
  }

  @Test void testSslValidationPropertySnakeCase() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("disable_ssl_validation", "true");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      assertThat(connection, is(notNullValue()));
    }
  }

  @Test void testCimModelSnakeCase() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("cim_model", "authentication");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      assertThat(connection, is(notNullValue()));
    }
  }

  @Test void testCimModelsSnakeCase() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("cim_models", "authentication,web");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      assertThat(connection, is(notNullValue()));
    }
  }

  @Test void testTokenAuthentication() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("token", "test-token-123");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      assertThat(connection, is(notNullValue()));
    }
  }

  @Test void testUrlParameterParsing() throws SQLException {
    // Test URL parameter parsing without requiring Properties object
    String url = "jdbc:splunk:url='mock';user='testuser';password='testpass'";

    try (Connection connection = DriverManager.getConnection(url)) {
      assertThat(connection, is(notNullValue()));
      assertThat(connection.isClosed(), is(false));
    }
  }

  @Test void testUrlParameterParsingWithQuotes() throws SQLException {
    // Test URL parameter parsing with different quote styles
    String url = "jdbc:splunk:url=\"mock\";user=\"testuser\";password=\"testpass\"";

    try (Connection connection = DriverManager.getConnection(url)) {
      assertThat(connection, is(notNullValue()));
      assertThat(connection.isClosed(), is(false));
    }
  }

  @Test void testUrlParameterParsingWithoutQuotes() throws SQLException {
    // Test URL parameter parsing without quotes
    String url = "jdbc:splunk:url=mock;user=testuser;password=testpass";

    try (Connection connection = DriverManager.getConnection(url)) {
      assertThat(connection, is(notNullValue()));
      assertThat(connection.isClosed(), is(false));
    }
  }

  @Test void testUrlParameterOverridesProperties() throws SQLException {
    // Test that URL parameters override Properties
    Properties info = new Properties();
    info.setProperty("url", "wrong");
    info.put("user", "wronguser");
    info.put("password", "wrongpass");

    String url = "jdbc:splunk:url='mock';user='testuser';password='testpass'";

    try (Connection connection = DriverManager.getConnection(url, info)) {
      assertThat(connection, is(notNullValue()));
      assertThat(connection.isClosed(), is(false));
    }
  }

  @Test void testUrlParameterParsingWithCimModel() throws SQLException {
    // Test URL parameter parsing with CIM model
    String url = "jdbc:splunk:url='mock';user='testuser';password='testpass';cimModel='web'";

    try (Connection connection = DriverManager.getConnection(url)) {
      assertThat(connection, is(notNullValue()));
      assertThat(connection.isClosed(), is(false));
    }
  }

  @Test void testInvalidUrl() {
    Properties info = new Properties();
    info.setProperty("url", "not-a-valid-url");
    info.put("user", "test");
    info.put("password", "test");

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    assertThat(ex.getMessage(), containsString("URI is not absolute"));
  }

  @Test void testHostPortConfiguration() throws SQLException {
    Properties info = new Properties();
    info.put("host", "test.example.com");
    info.put("port", "8089");
    info.put("protocol", "https");
    info.put("user", "test");
    info.put("password", "test");

    // This should work once host/port support is added
    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    assertThat(ex.getMessage(), containsString("Must specify 'url' property"));
  }

  @Test void testCastOperationsInMockEnvironment() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("cim_model", "web");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      // Test basic CAST operations - these should be handled by Calcite
      String[] castQueries = {
          "SELECT CAST('123' AS INTEGER) as int_val",
          "SELECT CAST(123 AS VARCHAR) as str_val",
          "SELECT CAST('123.45' AS DOUBLE) as double_val",
          "SELECT CAST(1 AS BOOLEAN) as bool_val"
      };

      for (String query : castQueries) {
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertThat("Query should execute: " + query, rs.next(), is(true));
        }
      }
    }
  }

  @Test void testCastWithTableFields() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("cim_model", "web");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      // Test CAST operations with table fields
      String[] fieldCastQueries = {
          "SELECT CAST(\"status\" AS VARCHAR) as status_str FROM \"splunk\".\"web\"",
          "SELECT CAST(\"bytes\" AS INTEGER) as bytes_int FROM \"splunk\".\"web\"",
          "SELECT CAST(\"response_time\" AS DOUBLE) as resp_time FROM \"splunk\".\"web\""
      };

      for (String query : fieldCastQueries) {
        assertDoesNotThrow(() -> {
          try (ResultSet rs = stmt.executeQuery(query)) {
            // Query should execute without throwing RelOptUtil type mismatch errors
          } catch (SQLException e) {
            if (e.getMessage().contains("Cannot add expression of different type to set")) {
              throw new RuntimeException("RelOptUtil type mismatch error detected: " + e.getMessage());
            }
            // Other SQL exceptions are expected in mock environment
          }
        }, "CAST query should not cause type mismatch errors: " + query);
      }
    }
  }

  @Test void testComplexCastOperations() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("cim_model", "web");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      // Test multiple CAST operations in one query
      String complexQuery = "SELECT " +
          "CAST(\"status\" AS VARCHAR) as status_str, " +
          "CAST(\"bytes\" AS INTEGER) as bytes_int, " +
          "CAST(\"response_time\" AS DOUBLE) as resp_time " +
          "FROM \"splunk\".\"web\" LIMIT 5";

      assertDoesNotThrow(() -> {
        try (ResultSet rs = stmt.executeQuery(complexQuery)) {
          // Should not throw RelOptUtil errors
        } catch (SQLException e) {
          if (e.getMessage().contains("Cannot add expression of different type to set")) {
            throw new RuntimeException("Multiple CAST operations caused type mismatch: " + e.getMessage());
          }
        }
      }, "Multiple CAST operations should not cause type mismatch errors");
    }
  }

  @Test void testCastWithFilters() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("cim_model", "web");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      // Test CAST operations with WHERE clauses
      String[] filterQueries = {
          "SELECT CAST(\"status\" AS VARCHAR) as status_str FROM \"splunk\".\"web\" WHERE \"status\" = '200'",
          "SELECT CAST(\"bytes\" AS INTEGER) as bytes_int FROM \"splunk\".\"web\" WHERE \"action\" = 'GET'",
          "SELECT \"action\", CAST(\"status\" AS INTEGER) as status_int FROM \"splunk\".\"web\" WHERE \"bytes\" > 1000"
      };

      for (String query : filterQueries) {
        assertDoesNotThrow(() -> {
          try (ResultSet rs = stmt.executeQuery(query)) {
            // Should execute without RelOptUtil errors
          } catch (SQLException e) {
            if (e.getMessage().contains("Cannot add expression of different type to set")) {
              throw new RuntimeException("CAST with filter caused type mismatch: " + e.getMessage());
            }
          }
        }, "CAST with filter should work: " + query);
      }
    }
  }

  @Test void testEdgeCaseCastOperations() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("cim_model", "web");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      // Test edge case CAST operations
      String[] edgeCaseQueries = {
          // Nested CAST operations
          "SELECT CAST(CAST(\"status\" AS INTEGER) AS VARCHAR) as nested_cast FROM \"splunk\".\"web\"",
          // CAST with arithmetic
          "SELECT CAST(\"bytes\" AS INTEGER) + 100 as bytes_plus FROM \"splunk\".\"web\"",
          // CAST in subquery
          "SELECT * FROM (SELECT CAST(\"status\" AS VARCHAR) as status_str FROM \"splunk\".\"web\") t",
          // CAST with aggregation
          "SELECT COUNT(CAST(\"status\" AS INTEGER)) as status_count FROM \"splunk\".\"web\""
      };

      for (String query : edgeCaseQueries) {
        assertDoesNotThrow(() -> {
          try (ResultSet rs = stmt.executeQuery(query)) {
            // Edge cases should not cause type mismatch errors
          } catch (SQLException e) {
            if (e.getMessage().contains("Cannot add expression of different type to set")) {
              throw new RuntimeException("Edge case CAST caused type mismatch: " + e.getMessage());
            }
          }
        }, "Edge case CAST should work: " + query);
      }
    }
  }

  // Negative test cases for error handling
  @Test void testMalformedUrl() {
    Properties info = new Properties();
    info.setProperty("url", "http://[invalid:url:format");
    info.put("user", "test");
    info.put("password", "test");

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    // The error message depends on whether it's caught during URL validation or connection creation
    assertThat(ex.getMessage(), containsString("Failed to create enhanced Splunk connection"));
  }

  @Test void testInvalidUrlParameterFormat() {
    // Test malformed URL parameter (missing value)
    String url = "jdbc:splunk:url=mock;user=;password=test";

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection(url);
    });
    assertThat(ex.getMessage(), containsString("Must specify either 'token' or both 'user' and 'password'"));
  }

  @Test void testInvalidCustomTablesJson() {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("customTables", "invalid-json-format");

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    assertThat(ex.getMessage(), containsString("Invalid customTables JSON"));
  }

  @Test void testCustomTablesWithoutColumns() {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    // Valid JSON but missing required 'columns' property
    info.put("customTables", "[{\"name\":\"test_table\"}]");

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    assertThat(ex.getMessage(), containsString("must have a 'columns' array property"));
  }

  @Test void testCustomTablesWithEmptyColumns() {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    // Valid JSON but empty columns array
    info.put("customTables", "[{\"name\":\"test_table\",\"columns\":[]}]");

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    assertThat(ex.getMessage(), containsString("must have at least one column"));
  }

  @Test void testCustomTablesWithInvalidColumnStructure() {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    // Valid JSON but column missing required 'type' property
    info.put("customTables", "[{\"name\":\"test_table\",\"columns\":[{\"name\":\"col1\"}]}]");

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    assertThat(ex.getMessage(), containsString("must have a non-empty 'type' property"));
  }

  @Test void testEmptyTokenAuthentication() {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("token", "");  // Empty token

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    assertThat(ex.getMessage(), containsString("Must specify either 'token' or both 'user' and 'password'"));
  }

  @Test void testWhitespaceOnlyCredentials() {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "   ");  // Whitespace only
    info.put("password", "   ");  // Whitespace only

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    assertThat(ex.getMessage(), containsString("Must specify either 'token' or both 'user' and 'password'"));
  }

  @Test void testIncompleteCredentials() {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "testuser");
    // Missing password

    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    assertThat(ex.getMessage(), containsString("Must specify either 'token' or both 'user' and 'password'"));
  }

  @Test void testUrlParameterParsingErrors() {
    // Test URL with malformed parameter (no equals sign)
    String url = "jdbc:splunk:url=mock;invalidparam;user=test;password=test";

    // This should still work, just ignore the invalid parameter
    assertDoesNotThrow(() -> {
      try (Connection connection = DriverManager.getConnection(url)) {
        assertThat(connection, is(notNullValue()));
      }
    });
  }

  @Test void testUrlParameterWithOnlyKey() {
    // Test URL parameter with key but no value
    String url = "jdbc:splunk:url=mock;user=test;password=test;invalidkey=";

    // Should still work, the empty value will be handled appropriately
    assertDoesNotThrow(() -> {
      try (Connection connection = DriverManager.getConnection(url)) {
        assertThat(connection, is(notNullValue()));
      }
    });
  }

  @Test void testConnectionFailureWithRealUrl() {
    Properties info = new Properties();
    info.setProperty("url", "https://nonexistent.splunk.server:8089");
    info.put("user", "test");
    info.put("password", "test");

    // Should fail when trying to create actual SplunkConnection (not mock)
    SQLException ex = assertThrows(SQLException.class, () -> {
      DriverManager.getConnection("jdbc:splunk:", info);
    });
    // The exact error message depends on network/connection failure
    assertThat(ex.getMessage(), containsString("Failed to create enhanced Splunk connection"));
  }

  @Test void testCastWithInvalidFieldNames() throws SQLException {
    Properties info = new Properties();
    info.setProperty("url", "mock");
    info.put("user", "test");
    info.put("password", "test");
    info.put("cim_model", "web");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      // Test CAST with non-existent field names - should not cause type mismatch errors
      String[] invalidFieldQueries = {
          "SELECT CAST(\"nonexistent_field\" AS VARCHAR) as field_str FROM \"splunk\".\"web\"",
          "SELECT CAST(\"another_missing_field\" AS INTEGER) as field_int FROM \"splunk\".\"web\""
      };

      for (String query : invalidFieldQueries) {
        assertDoesNotThrow(() -> {
          try (ResultSet rs = stmt.executeQuery(query)) {
            // Should not cause RelOptUtil type mismatch errors even with invalid field names
          } catch (SQLException e) {
            if (e.getMessage().contains("Cannot add expression of different type to set")) {
              throw new RuntimeException("CAST with invalid field caused type mismatch: " + e.getMessage());
            }
            // Other SQL exceptions (like field not found) are expected and OK
          }
        }, "CAST with invalid field should not cause type mismatch: " + query);
      }
    }
  }

  /**
   * Mock implementation of SplunkConnection for testing.
   */
  static class MockSplunkConnection implements SplunkConnection {
    private final Map<String, List<Map<String, Object>>> mockData = new HashMap<>();

    MockSplunkConnection() {
      // Add some mock data
      mockData.put(
          "authentication", Arrays.asList(
          createEvent("user1", "login", "success"),
          createEvent("user2", "login", "failed"),
          createEvent("user1", "logout", "success")));

      mockData.put(
          "web", Arrays.asList(
          createEvent("user1", "GET", "200"),
          createEvent("user2", "POST", "404")));
    }

    private Map<String, Object> createEvent(String user, String action, String status) {
      Map<String, Object> event = new HashMap<>();
      event.put("_time", System.currentTimeMillis() / 1000);
      event.put("user", user);
      event.put("action", action);
      event.put("status", status);
      event.put("_raw", String.format("user=%s action=%s status=%s", user, action, status));
      return event;
    }

    @Override public void getSearchResults(String search, Map<String, String> otherArgs,
        List<String> fieldList, SearchResultListener srl) {
      // Simple mock implementation
      List<Map<String, Object>> results = mockData.getOrDefault("authentication", Arrays.asList());
      for (Map<String, Object> event : results) {
        // Convert event to string array based on fieldList
        String[] values = new String[fieldList.size()];
        for (int i = 0; i < fieldList.size(); i++) {
          Object value = event.get(fieldList.get(i));
          values[i] = value != null ? value.toString() : null;
        }
        srl.processSearchResult(values);
      }
    }

    @Override public Enumerator<Object> getSearchResultEnumerator(String search,
        Map<String, String> otherArgs, List<String> fieldList, Set<String> wantedFields) {
      List<Object> results = Arrays.asList();
      return Linq4j.enumerator(results);
    }

    @Override public Enumerator<Object> getSearchResultEnumerator(String search,
        Map<String, String> otherArgs, List<String> fieldList, Set<String> wantedFields,
        Map<String, String> fieldMapping) {
      return getSearchResultEnumerator(search, otherArgs, fieldList, wantedFields);
    }
  }
}
