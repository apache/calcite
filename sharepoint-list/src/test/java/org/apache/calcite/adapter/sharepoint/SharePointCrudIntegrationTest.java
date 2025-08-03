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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive CRUD integration tests for SharePoint List adapter.
 *
 * Tests all CREATE, READ, UPDATE, DELETE operations plus table management.
 */
@EnabledIfEnvironmentVariable(named = "SHAREPOINT_INTEGRATION_TESTS", matches = "true")
public class SharePointCrudIntegrationTest {

  private static Properties testConfig;
  private String testListName;
  private Connection connection;

  @BeforeAll
  public static void setUpClass() throws IOException {
    testConfig = loadTestConfig();
  }

  @BeforeEach
  public void setUp() throws SQLException {
    // Generate unique test list name
    testListName = "crud_test_" + UUID.randomUUID().toString().substring(0, 8).toLowerCase(Locale.ROOT);
    connection = createConnection();
  }

  @AfterEach
  public void tearDown() throws SQLException {
    if (connection != null) {
      // Clean up test list if it exists
      try {
        MicrosoftGraphListClient client = createDirectClient();
        // Try to find and delete the test list
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

  @Test public void testCreateList() throws Exception {
    // Create a test list programmatically using the client
    MicrosoftGraphListClient client = createDirectClient();

    // Define columns for our test list (avoid SharePoint system column names)
    java.util.List<SharePointColumn> columns =
        java.util.Arrays.asList(new SharePointColumn("TaskTitle", "Task Title", "text", true),
        new SharePointColumn("TaskDescription", "Task Description", "text", false),
        new SharePointColumn("TaskPriority", "Task Priority", "number", false),
        new SharePointColumn("TaskCompleted", "Task Completed", "boolean", false),
        new SharePointColumn("TaskDueDate", "Task Due Date", "datetime", false));

    // Create the list
    SharePointListMetadata metadata = client.createList(testListName, columns);

    assertNotNull(metadata);
    assertEquals(testListName, SharePointNameConverter.toSqlName(metadata.getDisplayName()));

    // Verify the list appears in schema discovery
    // Note: May need to wait a moment for SharePoint to index the new list
    Thread.sleep(2000);

    // Reconnect to pick up new schema
    connection.close();
    connection = createConnection();

    java.sql.DatabaseMetaData metaData = connection.getMetaData();
    ResultSet rs = metaData.getTables(null, "sharepoint", testListName, new String[]{"TABLE"});
    assertTrue(rs.next(), "Created list should appear in schema");
  }

  @Test public void testInsertSingleRecord() throws Exception {
    createTestList();

    // Insert a single record
    String insertSql =
        String.format(Locale.ROOT, "INSERT INTO sharepoint.%s (task_title, task_description, task_priority, task_completed) "
        + "VALUES (?, ?, ?, ?)", testListName);

    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setString(1, "Test Task 1");
      pstmt.setString(2, "This is a test task");
      pstmt.setInt(3, 1);
      pstmt.setBoolean(4, false);

      int rowsInserted = pstmt.executeUpdate();
      assertEquals(1, rowsInserted, "Should insert exactly 1 row");
    }

    // Verify the record was inserted
    String selectSql =
        String.format(Locale.ROOT, "SELECT id, task_title, task_description, task_priority, task_completed FROM sharepoint.%s",
            testListName);

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(selectSql)) {

      assertTrue(rs.next(), "Should find the inserted record");

      String id = rs.getString("id");
      assertNotNull(id, "ID should not be null");
      assertEquals("Test Task 1", rs.getString("task_title"));
      assertEquals("This is a test task", rs.getString("task_description"));
      assertEquals(1, rs.getInt("task_priority"));
      assertFalse(rs.getBoolean("task_completed"));

      assertFalse(rs.next(), "Should only have one record");
    }
  }

  @Test public void testBatchInsert() throws Exception {
    createTestList();

    String insertSql =
        String.format(Locale.ROOT, "INSERT INTO sharepoint.%s (task_title, task_description, task_priority, task_completed) "
        + "VALUES (?, ?, ?, ?)", testListName);

    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      // Insert multiple records in a batch
      for (int i = 1; i <= 10; i++) {
        pstmt.setString(1, "Batch Task " + i);
        pstmt.setString(2, "Batch insert test item " + i);
        pstmt.setInt(3, i % 3 + 1); // Priority 1-3
        pstmt.setBoolean(4, i % 2 == 0); // Alternate completed status
        pstmt.addBatch();
      }

      int[] results = pstmt.executeBatch();
      assertEquals(10, results.length, "Should have 10 batch results");

      // Verify all were successful (each should return 1)
      for (int result : results) {
        assertEquals(1, result, "Each batch item should insert 1 row");
      }
    }

    // Verify all records were inserted
    String countSql = String.format(Locale.ROOT, "SELECT COUNT(*) FROM sharepoint.%s", testListName);
    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(countSql)) {

      assertTrue(rs.next());
      assertEquals(10, rs.getInt(1), "Should have 10 records total");
    }
  }

  @Test public void testSelectWithWhere() throws Exception {
    createTestList();
    insertTestData();

    // Test WHERE clause with different conditions
    String selectSql =
        String.format(Locale.ROOT, "SELECT task_title, task_priority FROM sharepoint.%s WHERE priority = ? AND is_completed = ?",
            testListName);

    try (PreparedStatement pstmt = connection.prepareStatement(selectSql)) {
      pstmt.setInt(1, 1);
      pstmt.setBoolean(2, false);

      try (ResultSet rs = pstmt.executeQuery()) {
        int count = 0;
        while (rs.next()) {
          String title = rs.getString("task_title");
          int priority = rs.getInt("task_priority");

          assertEquals(1, priority, "All results should have priority 1");
          assertNotNull(title, "Title should not be null");
          count++;
        }

        assertTrue(count > 0, "Should find at least one matching record");
      }
    }
  }

  @Test public void testDelete() throws Exception {
    createTestList();
    insertTestData();

    // First, get an ID to delete
    String selectSql = String.format(Locale.ROOT, "SELECT id FROM sharepoint.%s LIMIT 1", testListName);
    String itemId;

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(selectSql)) {

      assertTrue(rs.next(), "Should have at least one record to delete");
      itemId = rs.getString("id");
      assertNotNull(itemId, "Item ID should not be null");
    }

    // Delete the record
    String deleteSql = String.format(Locale.ROOT, "DELETE FROM sharepoint.%s WHERE id = ?", testListName);

    try (PreparedStatement pstmt = connection.prepareStatement(deleteSql)) {
      pstmt.setString(1, itemId);

      int rowsDeleted = pstmt.executeUpdate();
      assertEquals(1, rowsDeleted, "Should delete exactly 1 row");
    }

    // Verify the record was deleted
    String verifySql = String.format(Locale.ROOT, "SELECT COUNT(*) FROM sharepoint.%s WHERE id = ?", testListName);

    try (PreparedStatement pstmt = connection.prepareStatement(verifySql)) {
      pstmt.setString(1, itemId);

      try (ResultSet rs = pstmt.executeQuery()) {
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1), "Deleted record should not be found");
      }
    }
  }

  @Test public void testDeleteMultiple() throws Exception {
    createTestList();
    insertTestData();

    // Delete all completed tasks
    String deleteSql =
        String.format(Locale.ROOT, "DELETE FROM sharepoint.%s WHERE task_completed = true", testListName);

    try (Statement stmt = connection.createStatement()) {
      int rowsDeleted = stmt.executeUpdate(deleteSql);
      assertTrue(rowsDeleted > 0, "Should delete at least one completed task");
    }

    // Verify no completed tasks remain
    String verifySql =
        String.format(Locale.ROOT, "SELECT COUNT(*) FROM sharepoint.%s WHERE task_completed = true", testListName);

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(verifySql)) {

      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1), "Should have no completed tasks remaining");
    }
  }

  @Test public void testDropList() throws Exception {
    createTestList();
    insertTestData();

    // Get the list ID for direct deletion
    MicrosoftGraphListClient client = createDirectClient();
    Map<String, SharePointListMetadata> lists = client.getAvailableLists();
    assertTrue(lists.containsKey(testListName), "Test list should exist");

    SharePointListMetadata metadata = lists.get(testListName);
    String listId = metadata.getListId();

    // Delete the list
    client.deleteList(listId);

    // Wait for SharePoint to process the deletion
    Thread.sleep(2000);

    // Verify the list is gone
    lists = client.getAvailableLists();
    assertFalse(lists.containsKey(testListName), "Test list should be deleted");
  }

  @Test public void testTypeConversions() throws Exception {
    createTestList();

    // Insert record with various data types
    String insertSql =
        String.format(Locale.ROOT, "INSERT INTO sharepoint.%s (title, description, priority, is_completed, task_due_date) "
        + "VALUES (?, ?, ?, ?, ?)", testListName);

    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      pstmt.setString(1, "Type Test");
      pstmt.setString(2, "Testing data type conversions");
      pstmt.setDouble(3, 2.5); // Test number type
      pstmt.setBoolean(4, true);
      pstmt.setTimestamp(5, java.sql.Timestamp.valueOf("2024-12-31 23:59:59"));

      int rowsInserted = pstmt.executeUpdate();
      assertEquals(1, rowsInserted);
    }

    // Retrieve and verify the data types
    String selectSql =
        String.format(Locale.ROOT, "SELECT title, description, priority, is_completed, task_due_date FROM sharepoint.%s",
            testListName);

    try (Statement stmt = connection.createStatement();
         ResultSet rs = stmt.executeQuery(selectSql)) {

      assertTrue(rs.next());

      assertEquals("Type Test", rs.getString("task_title"));
      assertEquals("Testing data type conversions", rs.getString("task_description"));
      assertEquals(2.5, rs.getDouble("task_priority"), 0.01);
      assertTrue(rs.getBoolean("task_completed"));

      java.sql.Timestamp retrievedDate = rs.getTimestamp("task_due_date");
      assertNotNull(retrievedDate, "Due date should not be null");
    }
  }

  // Helper methods

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
      throw new RuntimeException("Test configuration file not found: local-test.properties");
    }

    return props;
  }

  private Connection createConnection() throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");

    Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);
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

    return conn;
  }

  private MicrosoftGraphListClient createDirectClient() throws Exception {
    Map<String, Object> authConfig = new HashMap<>();
    authConfig.put("authType", "CLIENT_CREDENTIALS");
    authConfig.put("clientId", testConfig.getProperty("SHAREPOINT_CLIENT_ID"));
    authConfig.put("clientSecret", testConfig.getProperty("SHAREPOINT_CLIENT_SECRET"));
    authConfig.put("tenantId", testConfig.getProperty("SHAREPOINT_TENANT_ID"));

    org.apache.calcite.adapter.sharepoint.auth.SharePointAuth auth =
        org.apache.calcite.adapter.sharepoint.auth.SharePointAuthFactory.createAuth(authConfig);

    return new MicrosoftGraphListClient(testConfig.getProperty("SHAREPOINT_SITE_URL"), auth);
  }

  private void createTestList() throws Exception {
    MicrosoftGraphListClient client = createDirectClient();

    java.util.List<SharePointColumn> columns =
        java.util.Arrays.asList(new SharePointColumn("TaskTitle", "Task Title", "text", true),
        new SharePointColumn("TaskDescription", "Task Description", "text", false),
        new SharePointColumn("TaskPriority", "Task Priority", "number", false),
        new SharePointColumn("TaskCompleted", "Task Completed", "boolean", false),
        new SharePointColumn("TaskDueDate", "Task Due Date", "datetime", false));

    client.createList(testListName, columns);

    // Wait for SharePoint to index the new list
    Thread.sleep(3000);

    // Reconnect to pick up new schema
    connection.close();
    connection = createConnection();
  }

  private void insertTestData() throws Exception {
    String insertSql =
        String.format(Locale.ROOT, "INSERT INTO sharepoint.%s (task_title, task_description, task_priority, task_completed) "
        + "VALUES (?, ?, ?, ?)", testListName);

    try (PreparedStatement pstmt = connection.prepareStatement(insertSql)) {
      // Insert 5 test records with varying data
      String[] titles = {"Task A", "Task B", "Task C", "Task D", "Task E"};
      String[] descriptions = {"First task", "Second task", "Third task", "Fourth task", "Fifth task"};
      int[] priorities = {1, 2, 1, 3, 2};
      boolean[] completed = {false, true, false, true, false};

      for (int i = 0; i < titles.length; i++) {
        pstmt.setString(1, titles[i]);
        pstmt.setString(2, descriptions[i]);
        pstmt.setInt(3, priorities[i]);
        pstmt.setBoolean(4, completed[i]);
        pstmt.addBatch();
      }

      pstmt.executeBatch();
    }
  }
}
