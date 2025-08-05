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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Test to find and display the actual web data rows.
 */
@Tag("integration")
@EnabledIf("splunkTestEnabled")
class SplunkWebDataTest {
  private static String SPLUNK_URL = "https://localhost:8089";
  private static String SPLUNK_USER = "admin";
  private static String SPLUNK_PASSWORD = "changeme";
  private static boolean DISABLE_SSL_VALIDATION = false;

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
    info.put("user", SPLUNK_USER);
    info.put("password", SPLUNK_PASSWORD);
    if (DISABLE_SSL_VALIDATION) {
      info.put("disableSslValidation", "true");
    }
    return DriverManager.getConnection("jdbc:splunk:", info);
  }

  @Test void testFindWebData() throws SQLException {
    try (Connection connection = createConnection();
         Statement stmt = connection.createStatement()) {

      // Count total rows first
      String countSql = "SELECT COUNT(*) as row_count FROM \"splunk\".\"web\"";
      ResultSet countRs = stmt.executeQuery(countSql);
      int totalRows = 0;
      if (countRs.next()) {
        totalRows = countRs.getInt("row_count");
      }
      System.out.println("Total rows in web table: " + totalRows);

      // Get all the data
      String sql = "SELECT * FROM \"splunk\".\"web\"";
      ResultSet rs = stmt.executeQuery(sql);

      int columnCount = rs.getMetaData().getColumnCount();
      System.out.println("Columns: " + columnCount);

      int actualRowCount = 0;
      while (rs.next()) {
        actualRowCount++;
        System.out.println("\n=== ROW " + actualRowCount + " ===");

        for (int i = 1; i <= columnCount; i++) {
          String columnName = rs.getMetaData().getColumnName(i);
          String value = rs.getString(i);
          if (value != null && !value.trim().isEmpty()) {
            System.out.println(columnName + " = " + value);
          }
        }
      }

      System.out.println("\nActual rows retrieved: " + actualRowCount);
      System.out.println("Expected 5 rows, found: " + actualRowCount);
    }
  }

  @Test void testWebDataWithSpecificColumns() throws SQLException {
    try (Connection connection = createConnection();
         Statement stmt = connection.createStatement()) {

      // Query with key columns only
      String sql = "SELECT \"time\", \"action\", \"user\", \"src\", \"dest\", \"uri_path\" " +
                   "FROM \"splunk\".\"web\" " +
                   "ORDER BY \"time\" DESC";

      ResultSet rs = stmt.executeQuery(sql);

      int rowCount = 0;
      System.out.println("Web traffic data:");
      System.out.println("Time | Action | User | Source | Destination | URI Path");
      System.out.println("-----|--------|------|--------|-------------|----------");

      while (rs.next()) {
        rowCount++;
        String time = rs.getString("time");
        String action = rs.getString("action");
        String user = rs.getString("user");
        String src = rs.getString("src");
        String dest = rs.getString("dest");
        String uriPath = rs.getString("uri_path");

        System.out.printf("%-20s | %-6s | %-8s | %-10s | %-11s | %s%n",
            time != null ? time : "NULL",
            action != null ? action : "NULL",
            user != null ? user : "NULL",
            src != null ? src : "NULL",
            dest != null ? dest : "NULL",
            uriPath != null ? uriPath : "NULL");
      }

      System.out.println("\nFound " + rowCount + " rows of web data");
    }
  }
}
