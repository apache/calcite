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
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Live server test for Splunk adapter.
 */
@Tag("integration")
class SplunkLiveServerTest {

  private static boolean splunkTestEnabled() {
    return System.getProperty("CALCITE_TEST_SPLUNK", "false").equals("true") ||
           System.getenv("CALCITE_TEST_SPLUNK") != null;
  }

  private void loadDriverClass() {
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("driver not found", e);
    }
  }

  @Test @EnabledIf("splunkTestEnabled")
  void testDataModelDiscovery() throws SQLException {
    loadDriverClass();

    Properties props = new Properties();

    // Load from local-properties.settings file
    File propsFile = new File("local-properties.settings");
    if (!propsFile.exists()) {
      // Try parent directory if running from submodule
      propsFile = new File("../local-properties.settings");
    }

    try (FileInputStream fis = new FileInputStream(propsFile)) {
      props.load(fis);
    } catch (Exception e) {
      throw new RuntimeException("Could not load local-properties.settings from " + propsFile.getAbsolutePath() + ": " + e.getMessage(), e);
    }

    // Verify required properties exist
    if (props.getProperty("splunk.url") == null ||
        props.getProperty("splunk.username") == null ||
        props.getProperty("splunk.password") == null) {
      throw new RuntimeException("Required Splunk connection properties not found in local-properties.settings");
    }

    Properties info = new Properties();
    info.setProperty("url", props.getProperty("splunk.url"));
    info.setProperty("user", props.getProperty("splunk.username"));
    info.setProperty("password", props.getProperty("splunk.password"));
    info.setProperty("app", "Splunk_SA_CIM");
    info.setProperty("datamodelCacheTtl", "0");

    if (props.containsKey("splunk.ssl.insecure")) {
      info.setProperty("disableSslValidation", props.getProperty("splunk.ssl.insecure"));
    }

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      System.out.println("=== Native Data Model Discovery from Splunk API ===\n");

      // Query all discovered tables using metadata
      try (Statement stmt = connection.createStatement()) {
        String query = "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname = 'splunk' ORDER BY tablename";
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Discovered Data Models (Tables):");
          System.out.println("Schema\t\tTable Name");
          System.out.println("------\t\t----------");

          while (rs.next()) {
            String schema = rs.getString("schemaname");
            String table = rs.getString("tablename");
            System.out.printf("%-15s %s%n", schema, table);
          }
        }
      }

      System.out.println("\n=== Data Model Details ===\n");

      // Get detailed information about each data model
      try (Statement stmt = connection.createStatement()) {
        String tablesQuery = "SELECT \"TABLE_NAME\" FROM \"information_schema\".\"TABLES\" WHERE \"TABLE_SCHEMA\" = 'splunk' ORDER BY \"TABLE_NAME\"";
        try (ResultSet tablesRs = stmt.executeQuery(tablesQuery)) {

          while (tablesRs.next()) {
            String tableName = tablesRs.getString("TABLE_NAME");
            System.out.println("Data Model: " + tableName);
            System.out.println("Columns:");
            System.out.println("Name\t\t\tType\t\tNullable");
            System.out.println("----\t\t\t----\t\t--------");

            // Get column information for this table
            String columnsQuery = "SELECT \"COLUMN_NAME\", \"DATA_TYPE\", \"IS_NULLABLE\" " +
                "FROM \"information_schema\".\"COLUMNS\" " +
                "WHERE \"TABLE_SCHEMA\" = 'splunk' AND \"TABLE_NAME\" = '" + tableName + "' " +
                "ORDER BY \"ORDINAL_POSITION\"";

            try (Statement colStmt = connection.createStatement();
                 ResultSet colRs = colStmt.executeQuery(columnsQuery)) {

              while (colRs.next()) {
                String colName = colRs.getString("COLUMN_NAME");
                String dataType = colRs.getString("DATA_TYPE");
                String nullable = colRs.getString("IS_NULLABLE");
                System.out.printf("%-23s %-15s %s%n", colName, dataType, nullable);
              }
            }

            System.out.println("\n"
  + "=".repeat(80) + "\n");
          }
        }
      }

      System.out.println("\n=== Splunk-Specific Metadata ===\n");

      // Query Splunk indexes
      try (Statement stmt = connection.createStatement()) {
        String query = "SELECT index_name, is_internal FROM pg_catalog.splunk_indexes ORDER BY index_name";
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Splunk Indexes:");
          System.out.println("Index Name\t\tInternal");
          System.out.println("----------\t\t--------");

          while (rs.next()) {
            String indexName = rs.getString("index_name");
            boolean isInternal = rs.getBoolean("is_internal");
            System.out.printf("%-23s %s%n", indexName, isInternal ? "Yes" : "No");
          }
        }
      }
    }
  }

  @Test @EnabledIf("splunkTestEnabled")
  void testBasicConnection() throws SQLException {
    loadDriverClass();

    Properties props = loadTestProperties();
    Properties info = new Properties();
    info.setProperty("url", props.getProperty("splunk.url"));
    info.put("user", props.getProperty("splunk.username"));
    info.put("password", props.getProperty("splunk.password"));

    if (props.containsKey("splunk.ssl.insecure")) {
      info.put("disableSslValidation", props.getProperty("splunk.ssl.insecure"));
    }

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      assertThat(connection.isClosed(), is(false));
      System.out.println("Successfully connected to Splunk at " + props.getProperty("splunk.url"));
    }
  }

  private Properties loadTestProperties() {
    Properties props = new Properties();

    // Load from local-properties.settings file
    File propsFile = new File("local-properties.settings");
    if (!propsFile.exists()) {
      // Try parent directory if running from submodule
      propsFile = new File("../local-properties.settings");
    }

    try (FileInputStream fis = new FileInputStream(propsFile)) {
      props.load(fis);
    } catch (Exception e) {
      throw new RuntimeException("Could not load local-properties.settings from " + propsFile.getAbsolutePath() + ": " + e.getMessage(), e);
    }

    // Verify required properties exist
    if (props.getProperty("splunk.url") == null ||
        props.getProperty("splunk.username") == null ||
        props.getProperty("splunk.password") == null) {
      throw new RuntimeException("Required Splunk connection properties not found in local-properties.settings");
    }

    return props;
  }

  // Now re-enabled - metadata schemas are available
  @Test @EnabledIf("splunkTestEnabled")
  void testMetadataSchemas() throws SQLException {
    loadDriverClass();

    Properties props = loadTestProperties();
    Properties info = new Properties();
    info.setProperty("url", props.getProperty("splunk.url"));
    info.put("user", props.getProperty("splunk.username"));
    info.put("password", props.getProperty("splunk.password"));

    if (props.containsKey("splunk.ssl.insecure")) {
      info.put("disableSslValidation", props.getProperty("splunk.ssl.insecure"));
    }

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      // Test pg_catalog schema
      System.out.println("\n=== Testing pg_catalog.pg_tables ===");
      try (ResultSet rs =
          stmt.executeQuery("SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname = 'splunk' LIMIT 5")) {
        int count = 0;
        while (rs.next()) {
          System.out.printf("Schema: %s, Table: %s%n",
              rs.getString("schemaname"),
              rs.getString("tablename"));
          count++;
        }
        System.out.println("Found " + count + " tables in pg_catalog.pg_tables");
      }

      // Test information_schema
      System.out.println("\n=== Testing information_schema.TABLES ===");
      try (ResultSet rs =
          stmt.executeQuery("SELECT \"TABLE_SCHEMA\", \"TABLE_NAME\", \"TABLE_TYPE\" FROM information_schema.\"TABLES\" " +
          "WHERE \"TABLE_SCHEMA\" = 'splunk' LIMIT 5")) {
        int count = 0;
        while (rs.next()) {
          System.out.printf("Schema: %s, Table: %s, Type: %s%n",
              rs.getString("TABLE_SCHEMA"),
              rs.getString("TABLE_NAME"),
              rs.getString("TABLE_TYPE"));
          count++;
        }
        System.out.println("Found " + count + " tables in information_schema.tables");
      }

      // Test splunk indexes
      System.out.println("\n=== Testing pg_catalog.splunk_indexes ===");
      try (ResultSet rs =
          stmt.executeQuery("SELECT index_name, is_internal FROM pg_catalog.splunk_indexes LIMIT 10")) {
        int count = 0;
        while (rs.next()) {
          System.out.printf("Index: %s, Internal: %s%n",
              rs.getString("index_name"),
              rs.getString("is_internal"));
          count++;
        }
        System.out.println("Found " + count + " indexes");
      }
    }
  }

  @Test @EnabledIf("splunkTestEnabled")
  void testDataQuery() throws SQLException {
    loadDriverClass();

    Properties props = loadTestProperties();
    Properties info = new Properties();
    info.setProperty("url", props.getProperty("splunk.url"));
    info.put("user", props.getProperty("splunk.username"));
    info.put("password", props.getProperty("splunk.password"));

    if (props.containsKey("splunk.ssl.insecure")) {
      info.put("disableSslValidation", props.getProperty("splunk.ssl.insecure"));
    }

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      // Test simple query - using the 'authentication' CIM model table
      // Using common CIM authentication fields: user, src, dest, time
      System.out.println("\n=== Testing data query ===");
      try (ResultSet rs =
          stmt.executeQuery("SELECT \"user\", src, dest, \"time\" FROM authentication LIMIT 5")) {
        int count = 0;
        while (rs.next()) {
          System.out.printf("User: %s, Source: %s, Dest: %s, Time: %s%n",
              rs.getString("user"),
              rs.getString("src"),
              rs.getString("dest"),
              rs.getTimestamp("time"));
          count++;
        }
        System.out.println("Found " + count + " events");
      }
    }
  }
}
