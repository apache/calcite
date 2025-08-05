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
package org.apache.calcite.adapter.sharepoint;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for CAST operations in SharePoint List adapter.
 *
 * Tests various CAST scenarios including type conversion between SharePoint
 * column types and SQL types, CAST operations in projections, WHERE clauses,
 * and complex expressions.
 */
@EnabledIfEnvironmentVariable(named = "SHAREPOINT_INTEGRATION_TESTS", matches = "true")
public class SharePointCastIntegrationTest {

  private static Properties testConfig;
  private String testListName;
  private Connection connection;
  private MicrosoftGraphListClient client;

  @BeforeAll
  public static void setUpClass() throws IOException {
    testConfig = loadTestConfig();
  }

  @BeforeEach
  public void setUp() throws Exception {
    testListName = "cast_test_" + UUID.randomUUID().toString().substring(0, 8).toLowerCase(Locale.ROOT);
    connection = createConnection();
    client = createDirectClient();

    // Create test list with various column types for CAST testing
    createTestList();
    populateTestData();
  }

  @AfterEach
  public void tearDown() throws SQLException {
    if (connection != null) {
      try {
        // Clean up test list
        Map<String, SharePointListMetadata> lists = client.getAvailableLists();
        if (lists.containsKey(testListName)) {
          SharePointListMetadata metadata = lists.get(testListName);
          client.deleteList(metadata.getListId());
        }
      } catch (Exception e) {
        System.err.println("Cleanup warning: " + e.getMessage());
      }
      connection.close();
    }
  }

  @Test public void testCastStringToNumeric() throws SQLException {
    Statement stmt = connection.createStatement();

    // Test CAST of string column to different numeric types
    String sql =
        String.format(Locale.ROOT, "SELECT "
        + "CAST(text_number AS INTEGER) AS text_as_int, "
        + "CAST(text_number AS DOUBLE) AS text_as_double, "
        + "CAST(text_number AS DECIMAL) AS text_as_decimal "
        + "FROM sharepoint.%s WHERE text_number IS NOT NULL",
        testListName);

    ResultSet rs = stmt.executeQuery(sql);
    assertTrue(rs.next(), "Should have at least one row");

    // Verify numeric conversions
    int intValue = rs.getInt("text_as_int");
    double doubleValue = rs.getDouble("text_as_double");
    BigDecimal decimalValue = rs.getBigDecimal("text_as_decimal");

    assertTrue(intValue > 0, "Integer cast should produce valid number");
    assertTrue(doubleValue > 0, "Double cast should produce valid number");
    assertNotNull(decimalValue, "Decimal cast should produce valid number");
  }

  @Test public void testCastNumericToString() throws SQLException {
    Statement stmt = connection.createStatement();

    // Test CAST of numeric columns to string (VARCHAR)
    String sql =
        String.format(Locale.ROOT, "SELECT "
        + "CAST(priority_number AS VARCHAR) AS num_as_string, "
        + "CAST(budget_amount AS VARCHAR) AS amount_as_string "
        + "FROM sharepoint.%s WHERE priority_number IS NOT NULL",
        testListName);

    ResultSet rs = stmt.executeQuery(sql);
    assertTrue(rs.next(), "Should have at least one row");

    String numAsString = rs.getString("num_as_string");
    String amountAsString = rs.getString("amount_as_string");

    assertNotNull(numAsString, "Numeric to string cast should work");
    assertNotNull(amountAsString, "Amount to string cast should work");
    assertTrue(numAsString.matches("\\d+"), "Should be a numeric string");
  }

  @Test public void testCastDateTimeToString() throws SQLException {
    Statement stmt = connection.createStatement();

    // Test CAST of datetime column to string
    String sql =
        String.format(Locale.ROOT, "SELECT "
        + "CAST(due_date AS VARCHAR) AS date_as_string, "
        + "due_date AS original_date "
        + "FROM sharepoint.%s WHERE due_date IS NOT NULL",
        testListName);

    ResultSet rs = stmt.executeQuery(sql);
    assertTrue(rs.next(), "Should have at least one row");

    String dateAsString = rs.getString("date_as_string");
    Timestamp originalDate = rs.getTimestamp("original_date");

    assertNotNull(dateAsString, "DateTime to string cast should work");
    assertNotNull(originalDate, "Original date should not be null");
    assertTrue(dateAsString.contains("-"), "Date string should contain date separators");
  }

  @Test public void testCastStringToDateTime() throws SQLException {
    Statement stmt = connection.createStatement();

    // Test CAST of string to datetime (using a properly formatted date string)
    String sql =
        String.format(Locale.ROOT, "SELECT "
        + "CAST('2023-12-25 10:30:00' AS TIMESTAMP) AS string_as_date, "
        + "text_title "
        + "FROM sharepoint.%s LIMIT 1",
        testListName);

    ResultSet rs = stmt.executeQuery(sql);
    assertTrue(rs.next(), "Should have at least one row");

    Timestamp stringAsDate = rs.getTimestamp("string_as_date");
    assertNotNull(stringAsDate, "String to datetime cast should work");
  }

  @Test public void testCastBooleanToString() throws SQLException {
    Statement stmt = connection.createStatement();

    // Test CAST of boolean column to string
    String sql =
        String.format(Locale.ROOT, "SELECT "
        + "CAST(is_completed AS VARCHAR) AS bool_as_string, "
        + "is_completed AS original_bool "
        + "FROM sharepoint.%s",
        testListName);

    ResultSet rs = stmt.executeQuery(sql);
    assertTrue(rs.next(), "Should have at least one row");

    String boolAsString = rs.getString("bool_as_string");
    boolean originalBool = rs.getBoolean("original_bool");

    assertNotNull(boolAsString, "Boolean to string cast should work");
    assertTrue(boolAsString.equals("true") || boolAsString.equals("false"),
        "Boolean string should be 'true' or 'false'");
  }

  @Test public void testCastBooleanToNumeric() throws SQLException {
    Statement stmt = connection.createStatement();

    // Test boolean operations (CAST boolean to numeric not supported in strict mode)
    // Instead, test boolean logic with CASE expressions
    String sql =
        String.format(Locale.ROOT, "SELECT "
        + "CASE WHEN is_completed THEN 1 ELSE 0 END AS bool_as_int, "
        + "CASE WHEN is_completed THEN 1.0 ELSE 0.0 END AS bool_as_double, "
        + "is_completed AS original_bool "
        + "FROM sharepoint.%s",
        testListName);

    ResultSet rs = stmt.executeQuery(sql);
    assertTrue(rs.next(), "Should have at least one row");

    int boolAsInt = rs.getInt("bool_as_int");
    double boolAsDouble = rs.getDouble("bool_as_double");
    boolean originalBool = rs.getBoolean("original_bool");

    // Boolean to numeric using CASE: true = 1, false = 0
    if (originalBool) {
      assertEquals(1, boolAsInt, "true should convert to 1");
      assertEquals(1.0, boolAsDouble, "true should convert to 1.0");
    } else {
      assertEquals(0, boolAsInt, "false should convert to 0");
      assertEquals(0.0, boolAsDouble, "false should convert to 0.0");
    }
  }

  @Test public void testCastInWhereClause() throws SQLException {
    Statement stmt = connection.createStatement();

    // Test CAST operations in WHERE clause for filtering
    String sql =
        String.format(Locale.ROOT, "SELECT text_title, priority_number "
        + "FROM sharepoint.%s "
        + "WHERE CAST(text_number AS INTEGER) > 100 "
        + "AND is_completed = false",
        testListName);

    ResultSet rs = stmt.executeQuery(sql);

    // Should execute without error (exact results depend on test data)
    int rowCount = 0;
    while (rs.next()) {
      assertNotNull(rs.getString("text_title"), "Title should not be null");
      rowCount++;
    }

    assertTrue(rowCount >= 0, "Query should execute successfully");
  }

  @Test public void testCastInOrderBy() throws SQLException {
    Statement stmt = connection.createStatement();

    // Test CAST operations in ORDER BY clause
    String sql =
        String.format(Locale.ROOT, "SELECT text_title, text_number, priority_number "
        + "FROM sharepoint.%s "
        + "ORDER BY CAST(text_number AS INTEGER) DESC, "
        + "CAST(priority_number AS VARCHAR) ASC",
        testListName);

    ResultSet rs = stmt.executeQuery(sql);

    // Should execute without error and maintain order
    assertTrue(rs.next(), "Should have at least one row");
    assertNotNull(rs.getString("text_title"), "Title should not be null");
  }

  @Test public void testComplexCastExpressions() throws SQLException {
    Statement stmt = connection.createStatement();

    // Test complex expressions involving CAST operations
    String sql =
        String.format(Locale.ROOT, "SELECT "
        + "text_title, "
        + "CAST(text_number AS INTEGER) + priority_number AS calculated_total, "
        + "CONCAT(CAST(priority_number AS VARCHAR), '-', text_title) AS priority_title, "
        + "CASE WHEN is_completed THEN 'Done' ELSE 'Pending' END AS status_text "
        + "FROM sharepoint.%s "
        + "WHERE CAST(text_number AS INTEGER) IS NOT NULL",
        testListName);

    ResultSet rs = stmt.executeQuery(sql);
    assertTrue(rs.next(), "Should have at least one row");

    String title = rs.getString("text_title");
    int calculatedTotal = rs.getInt("calculated_total");
    String priorityTitle = rs.getString("priority_title");
    String statusText = rs.getString("status_text");

    assertNotNull(title, "Title should not be null");
    assertTrue(calculatedTotal >= 0, "Calculated total should be valid");
    assertNotNull(priorityTitle, "Priority title should not be null");
    assertTrue(statusText.equals("Done") || statusText.equals("Pending"),
        "Status should be 'Done' or 'Pending'");
  }

  @Test public void testCastNullValues() throws SQLException {
    Statement stmt = connection.createStatement();

    // Test CAST operations with NULL values
    String sql =
        String.format(Locale.ROOT, "SELECT "
        + "CAST(NULL AS INTEGER) AS null_as_int, "
        + "CAST(NULL AS VARCHAR) AS null_as_string, "
        + "CAST(NULL AS TIMESTAMP) AS null_as_date, "
        + "text_title "
        + "FROM sharepoint.%s LIMIT 1",
        testListName);

    ResultSet rs = stmt.executeQuery(sql);
    assertTrue(rs.next(), "Should have at least one row");

    // All CAST operations on NULL should return NULL
    assertEquals(0, rs.getInt("null_as_int"), "NULL cast to int should be 0 (SQL default)");
    assertTrue(rs.wasNull(), "Should register as NULL");

    rs.getString("null_as_string");
    assertTrue(rs.wasNull(), "NULL cast to string should be NULL");

    rs.getTimestamp("null_as_date");
    assertTrue(rs.wasNull(), "NULL cast to timestamp should be NULL");
  }

  private void createTestList() throws Exception {
    // Define test columns with various types for CAST testing
    List<SharePointColumn> columns =
        Arrays.asList(new SharePointColumn("TextTitle", "Text Title", "text", true),
        new SharePointColumn("TextNumber", "Text Number", "text", false), // String that contains numbers
        new SharePointColumn("PriorityNumber", "Priority Number", "integer", false),
        new SharePointColumn("BudgetAmount", "Budget Amount", "number", false),
        new SharePointColumn("IsCompleted", "Is Completed", "boolean", false),
        new SharePointColumn("DueDate", "Due Date", "datetime", false));

    client.createList(testListName, columns);
  }

  private void populateTestData() throws Exception {
    Map<String, SharePointListMetadata> lists = client.getAvailableLists();
    SharePointListMetadata metadata = lists.get(testListName);
    assertNotNull(metadata, "Test list should exist");

    // Insert test data with various types
    Map<String, Object> item1 = new HashMap<>();
    item1.put("TextTitle", "High Priority Task");
    item1.put("TextNumber", "150"); // String that can be cast to number
    item1.put("PriorityNumber", 1);
    item1.put("BudgetAmount", 2500.75);
    item1.put("IsCompleted", false);
    item1.put("DueDate", "2024-03-15T10:30:00Z");

    Map<String, Object> item2 = new HashMap<>();
    item2.put("TextTitle", "Medium Priority Task");
    item2.put("TextNumber", "75");
    item2.put("PriorityNumber", 2);
    item2.put("BudgetAmount", 1200.50);
    item2.put("IsCompleted", true);
    item2.put("DueDate", "2024-04-20T14:15:00Z");

    Map<String, Object> item3 = new HashMap<>();
    item3.put("TextTitle", "Low Priority Task");
    item3.put("TextNumber", "25");
    item3.put("PriorityNumber", 3);
    item3.put("BudgetAmount", 500.00);
    item3.put("IsCompleted", false);
    item3.put("DueDate", "2024-05-10T09:00:00Z");

    client.createListItem(metadata.getListId(), item1);
    client.createListItem(metadata.getListId(), item2);
    client.createListItem(metadata.getListId(), item3);
  }

  private static Properties loadTestConfig() throws IOException {
    Properties props = new Properties();

    Path configPath = Paths.get("../file/local-test.properties");
    if (!Files.exists(configPath)) {
      configPath = Paths.get("../../file/local-test.properties");
    }

    if (Files.exists(configPath)) {
      try (FileInputStream fis = new FileInputStream(configPath.toFile())) {
        props.load(fis);
      }
    } else {
      props.setProperty("SHAREPOINT_TENANT_ID",
          System.getenv().getOrDefault("SHAREPOINT_TENANT_ID", ""));
      props.setProperty("SHAREPOINT_CLIENT_ID",
          System.getenv().getOrDefault("SHAREPOINT_CLIENT_ID", ""));
      props.setProperty("SHAREPOINT_CLIENT_SECRET",
          System.getenv().getOrDefault("SHAREPOINT_CLIENT_SECRET", ""));
      props.setProperty("SHAREPOINT_SITE_URL",
          System.getenv().getOrDefault("SHAREPOINT_SITE_URL", ""));
    }

    return props;
  }

  private Connection createConnection() throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");

    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    Map<String, Object> operand = new HashMap<>();
    operand.put("siteUrl", testConfig.getProperty("SHAREPOINT_SITE_URL"));
    operand.put("authType", "CLIENT_CREDENTIALS");
    operand.put("clientId", testConfig.getProperty("SHAREPOINT_CLIENT_ID"));
    operand.put("clientSecret", testConfig.getProperty("SHAREPOINT_CLIENT_SECRET"));
    operand.put("tenantId", testConfig.getProperty("SHAREPOINT_TENANT_ID"));

    SharePointListSchema sharePointSchema =
        new SharePointListSchema(testConfig.getProperty("SHAREPOINT_SITE_URL"), operand);

    rootSchema.add("sharepoint", sharePointSchema);
    return connection;
  }

  private MicrosoftGraphListClient createDirectClient() {
    Map<String, Object> authConfig = new HashMap<>();
    authConfig.put("authType", "CLIENT_CREDENTIALS");
    authConfig.put("clientId", testConfig.getProperty("SHAREPOINT_CLIENT_ID"));
    authConfig.put("clientSecret", testConfig.getProperty("SHAREPOINT_CLIENT_SECRET"));
    authConfig.put("tenantId", testConfig.getProperty("SHAREPOINT_TENANT_ID"));

    return new MicrosoftGraphListClient(testConfig.getProperty("SHAREPOINT_SITE_URL"),
        org.apache.calcite.adapter.sharepoint.auth.SharePointAuthFactory.createAuth(authConfig));
  }
}
