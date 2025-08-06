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
 * Simple test to discover web table structure against live Splunk.
 */
@Tag("integration")
class SplunkWebTableDiscoveryTest {
  private static String SPLUNK_URL = null;
  private static String SPLUNK_USER = null;
  private static String SPLUNK_PASSWORD = null;
  private static boolean DISABLE_SSL_VALIDATION = false;
  private static boolean PROPERTIES_LOADED = false;

  private void loadDriverClass() {
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("driver not found", e);
    }
  }

  @BeforeAll
  static void loadConnectionProperties() {
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
    }
  }


  private Connection createConnection() throws SQLException {
    loadDriverClass();
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.put("user", SPLUNK_USER);
    info.put("password", SPLUNK_PASSWORD);
    if (DISABLE_SSL_VALIDATION) {
      info.put("disableSslValidation", "true");
    }
    return DriverManager.getConnection("jdbc:splunk:", info);
  }

  @Test void testDiscoverWebTableStructure() throws SQLException {
    try (Connection connection = createConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      // List all tables
      ResultSet tables = metaData.getTables(null, "splunk", "%", null);
      List<String> tableNames = new ArrayList<>();
      while (tables.next()) {
        tableNames.add(tables.getString("TABLE_NAME"));
      }

      System.out.println("Found " + tableNames.size() + " tables:");
      for (String table : tableNames) {
        System.out.println("  - " + table);
      }

      assertTrue(tableNames.contains("web"), "Web table should exist");

      // Get web table columns
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

      // Test basic query
      try (Statement stmt = connection.createStatement()) {
        String sql = "SELECT * FROM \"splunk\".\"web\" LIMIT 3";
        ResultSet rs = stmt.executeQuery(sql);

        int columnCount = rs.getMetaData().getColumnCount();
        System.out.println("\nActual table structure (" + columnCount + " columns):");
        for (int i = 1; i <= columnCount; i++) {
          String name = rs.getMetaData().getColumnName(i);
          String type = rs.getMetaData().getColumnTypeName(i);
          System.out.println("  " + i + ". " + name + " (" + type + ")");
        }

        int rowCount = 0;
        while (rs.next() && rowCount < 3) {
          rowCount++;
          System.out.println("\nRow " + rowCount + ":");
          for (int i = 1; i <= columnCount; i++) {
            String name = rs.getMetaData().getColumnName(i);
            String value = rs.getString(i);
            System.out.println("  " + name + " = " + (value == null ? "NULL" : "'" + value + "'"));
          }
        }

        System.out.println("\nSuccessfully queried web table with " + rowCount + " rows");
      }
    }
  }

  @Test void testMetadataQueries() throws SQLException {
    try (Connection connection = createConnection();
         Statement stmt = connection.createStatement()) {

      // Test information_schema
      String sql = "SELECT COUNT(*) as table_count FROM \"information_schema\".\"TABLES\" " +
                   "WHERE \"TABLE_SCHEMA\" = 'splunk'";

      ResultSet rs = stmt.executeQuery(sql);
      if (rs.next()) {
        int tableCount = rs.getInt("table_count");
        System.out.println("Found " + tableCount + " tables via information_schema");
        assertEquals(29, tableCount, "Should find 29 tables via information_schema");
      }

      // Test pg_catalog
      sql = "SELECT COUNT(*) as table_count FROM pg_catalog.pg_tables " +
            "WHERE schemaname = 'splunk'";

      rs = stmt.executeQuery(sql);
      if (rs.next()) {
        int tableCount = rs.getInt("table_count");
        System.out.println("Found " + tableCount + " tables via pg_catalog");
        assertEquals(29, tableCount, "Should find 29 tables via pg_catalog");
      }

      System.out.println("✅ Metadata queries work correctly with live Splunk data!");
    }
  }
}
