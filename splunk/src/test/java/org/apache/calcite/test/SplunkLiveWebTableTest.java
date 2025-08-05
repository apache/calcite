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
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live test for web table structure and metadata against actual Splunk instance.
 * These tests require a live Splunk connection configured in local-properties.settings.
 */
@Tag("integration")
@EnabledIf("splunkTestEnabled")
class SplunkLiveWebTableTest {
  // Connection properties loaded from local-properties.settings
  private static String SPLUNK_URL = "https://localhost:8089";
  private static String SPLUNK_USER = "admin";
  private static String SPLUNK_PASSWORD = "changeme";
  private static boolean DISABLE_SSL_VALIDATION = false;

  @BeforeAll
  static void loadConnectionProperties() {
    // Try multiple possible locations for the properties file
    File[] possibleLocations = {
        new File("local-properties.settings"),
        new File("splunk/local-properties.settings"),
        new File("../splunk/local-properties.settings")
    };

    File propsFile = null;
    for (File location : possibleLocations) {
      if (location.exists()) {
        propsFile = location;
        break;
      }
    }

    if (propsFile != null) {
      Properties props = new Properties();
      try (FileInputStream fis = new FileInputStream(propsFile)) {
        props.load(fis);

        if (props.containsKey("splunk.url")) {
          SPLUNK_URL = props.getProperty("splunk.url");
        }
        if (props.containsKey("splunk.username")) {
          SPLUNK_USER = props.getProperty("splunk.username");
        }
        if (props.containsKey("splunk.password")) {
          SPLUNK_PASSWORD = props.getProperty("splunk.password");
        }
        if (props.containsKey("splunk.ssl.insecure")) {
          DISABLE_SSL_VALIDATION = Boolean.parseBoolean(props.getProperty("splunk.ssl.insecure"));
        }

        System.out.println("Loaded Splunk connection from " + propsFile.getPath() + ": " + SPLUNK_URL);
      } catch (IOException e) {
        System.err.println("Warning: Could not load local-properties.settings: " + e.getMessage());
      }
    } else {
      System.out.println("Using default Splunk connection: " + SPLUNK_URL);
    }
  }

  private static boolean splunkTestEnabled() {
    return System.getProperty("CALCITE_TEST_SPLUNK", "false").equals("true") ||
           System.getenv("CALCITE_TEST_SPLUNK") != null;  }

  private void loadDriverClass() {
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("driver not found", e);
    }
  }

  private Connection createConnection() throws SQLException {
    loadDriverClass();
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.setProperty("user", SPLUNK_USER);
    info.setProperty("password", SPLUNK_PASSWORD);
    if (DISABLE_SSL_VALIDATION) {
      info.setProperty("ssl_insecure", "true");
    }
    return DriverManager.getConnection("jdbc:splunk:", info);
  }

  @Test void testDiscoverWebTableStructure() throws SQLException {
    try (Connection connection = createConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      // Get all tables in splunk schema
      ResultSet tables = metaData.getTables(null, "splunk", "%", null);
      List<String> tableNames = new ArrayList<>();
      while (tables.next()) {
        tableNames.add(tables.getString("TABLE_NAME"));
      }

      System.out.println("Found " + tableNames.size() + " tables in splunk schema:");
      for (String tableName : tableNames) {
        System.out.println("  - " + tableName);
      }

      // Check if web table exists
      assertTrue(tableNames.contains("web"), "Web table should exist");

      // Get columns for web table
      ResultSet columns = metaData.getColumns(null, "splunk", "web", null);
      List<String> columnNames = new ArrayList<>();
      System.out.println("\nWeb table columns:");
      while (columns.next()) {
        String columnName = columns.getString("COLUMN_NAME");
        String dataType = columns.getString("TYPE_NAME");
        columnNames.add(columnName);
        System.out.println("  - " + columnName + " (" + dataType + ")");
      }

      assertTrue(columnNames.size() > 0, "Web table should have columns");
    }
  }

  @Test void testQueryWebTableData() throws SQLException {
    try (Connection connection = createConnection();
         Statement stmt = connection.createStatement()) {

      // Try a basic SELECT * to see what columns are actually available
      String sql = "SELECT * FROM \"splunk\".\"web\" LIMIT 5";
      ResultSet rs = stmt.executeQuery(sql);

      int columnCount = rs.getMetaData().getColumnCount();
      System.out.println("Web table has " + columnCount + " columns:");

      for (int i = 1; i <= columnCount; i++) {
        String columnName = rs.getMetaData().getColumnName(i);
        String columnType = rs.getMetaData().getColumnTypeName(i);
        System.out.println("  " + i + ". " + columnName + " (" + columnType + ")");
      }

      // Count rows
      int rowCount = 0;
      while (rs.next() && rowCount < 5) {
        rowCount++;
        System.out.println("Row " + rowCount + ":");
        for (int i = 1; i <= columnCount; i++) {
          String columnName = rs.getMetaData().getColumnName(i);
          String value = rs.getString(i);
          System.out.println("  " + columnName + " = " + (value == null ? "NULL" : value));
        }
      }

      System.out.println("Retrieved " + rowCount + " rows from web table");
      assertTrue(columnCount > 0, "Web table should have columns");
    }
  }

  @Test void testMetadataSchemaWithLiveData() throws SQLException {
    try (Connection connection = createConnection();
         Statement stmt = connection.createStatement()) {

      // Test information_schema with live data
      String sql = "SELECT \"TABLE_SCHEMA\", \"TABLE_NAME\", \"TABLE_TYPE\" " +
                   "FROM \"information_schema\".\"TABLES\" " +
                   "WHERE \"TABLE_SCHEMA\" = 'splunk' " +
                   "ORDER BY \"TABLE_NAME\"";

      ResultSet rs = stmt.executeQuery(sql);

      List<String> tableNames = new ArrayList<>();
      while (rs.next()) {
        String tableName = rs.getString("TABLE_NAME");
        tableNames.add(tableName);
      }

      System.out.println("Found " + tableNames.size() + " tables via information_schema:");
      for (String tableName : tableNames) {
        System.out.println("  - " + tableName);
      }

      assertTrue(tableNames.contains("web"), "Web table should be discoverable via information_schema");
      assertEquals(29, tableNames.size(), "Should find 29 tables as mentioned in the integration test");
    }
  }

  @Test void testPgCatalogWithLiveData() throws SQLException {
    try (Connection connection = createConnection();
         Statement stmt = connection.createStatement()) {

      // Test pg_catalog with live data
      String sql = "SELECT schemaname, tablename FROM pg_catalog.pg_tables " +
                   "WHERE schemaname = 'splunk' " +
                   "ORDER BY tablename";

      ResultSet rs = stmt.executeQuery(sql);

      List<String> tableNames = new ArrayList<>();
      while (rs.next()) {
        String tableName = rs.getString("tablename");
        tableNames.add(tableName);
      }

      System.out.println("Found " + tableNames.size() + " tables via pg_catalog.pg_tables:");
      for (String tableName : tableNames) {
        System.out.println("  - " + tableName);
      }

      assertTrue(tableNames.contains("web"), "Web table should be discoverable via pg_catalog");
      assertEquals(29, tableNames.size(), "Should find 29 tables as mentioned in the integration test");
    }
  }

  @Test void testWebTableColumnsMetadata() throws SQLException {
    try (Connection connection = createConnection();
         Statement stmt = connection.createStatement()) {

      // Get web table column information via information_schema
      String sql = "SELECT \"COLUMN_NAME\", \"DATA_TYPE\", \"IS_NULLABLE\", \"ORDINAL_POSITION\" " +
                   "FROM \"information_schema\".\"COLUMNS\" " +
                   "WHERE \"TABLE_SCHEMA\" = 'splunk' AND \"TABLE_NAME\" = 'web' " +
                   "ORDER BY \"ORDINAL_POSITION\"";

      ResultSet rs = stmt.executeQuery(sql);

      List<String> columnNames = new ArrayList<>();
      System.out.println("Web table columns from information_schema:");
      while (rs.next()) {
        String columnName = rs.getString("COLUMN_NAME");
        String dataType = rs.getString("DATA_TYPE");
        String nullable = rs.getString("IS_NULLABLE");
        int position = rs.getInt("ORDINAL_POSITION");
        columnNames.add(columnName);
        System.out.println("  " + position + ". " + columnName + " (" + dataType + ", nullable=" + nullable + ")");
      }

      assertTrue(columnNames.size() > 0, "Web table should have discoverable columns");

      // Test that we can query using the actual column names
      if (!columnNames.isEmpty()) {
        String firstColumn = columnNames.get(0);
        String testSql = "SELECT \"" + firstColumn + "\" FROM \"splunk\".\"web\" LIMIT 3";
        System.out.println("Testing query: " + testSql);

        ResultSet testRs = stmt.executeQuery(testSql);
        int testRowCount = 0;
        while (testRs.next() && testRowCount < 3) {
          testRowCount++;
          String value = testRs.getString(1);
          System.out.println("  Row " + testRowCount + ": " + (value == null ? "NULL" : value));
        }

        System.out.println("Successfully queried web table with actual column names");
      }
    }
  }
}
