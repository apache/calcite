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
import java.sql.Statement;
import java.util.Properties;

/**
 * Tests for Splunk data model discovery on live connections.
 */
@Tag("integration")
public class SplunkDataModelDiscoveryTest {

  private static boolean splunkTestEnabled() {
    return System.getProperty("CALCITE_TEST_SPLUNK", "false").equals("true") ||
           System.getenv("CALCITE_TEST_SPLUNK") != null;
  }

  @Test @EnabledIf("splunkTestEnabled")
  public void testDataModelDiscoveryWithMetadata() throws Exception {
    // Load driver class
    Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");

    Properties props = new Properties();

    // Load from local-properties.settings file
    File propsFile = new File("local-properties.settings");
    if (!propsFile.exists()) {
      // Try parent directory if running from submodule
      propsFile = new File("../local-properties.settings");
    }

    if (!propsFile.exists()) {
      System.out.println("Skipping test: Could not find local-properties.settings at " + propsFile.getAbsolutePath());
      return;
    }

    try (FileInputStream fis = new FileInputStream(propsFile)) {
      props.load(fis);
    } catch (Exception e) {
      System.out.println("Skipping test: Could not load local-properties.settings: " + e.getMessage());
      return;
    }

    // Verify required properties exist
    if (props.getProperty("splunk.url") == null ||
        props.getProperty("splunk.username") == null ||
        props.getProperty("splunk.password") == null) {
      System.out.println("Skipping test: Required Splunk connection properties not found");
      return;
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

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", info)) {
      System.out.println("=== Data Model Discovery Results ===\n");

      // Query all discovered tables using metadata
      try (Statement stmt = conn.createStatement()) {
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
      try (Statement stmt = conn.createStatement()) {
        String tablesQuery = "SELECT \"TABLE_NAME\" FROM information_schema.\"TABLES\" WHERE \"TABLE_SCHEMA\" = 'splunk' ORDER BY \"TABLE_NAME\"";
        try (ResultSet tablesRs = stmt.executeQuery(tablesQuery)) {

          while (tablesRs.next()) {
            String tableName = tablesRs.getString("TABLE_NAME");
            System.out.println("Data Model: " + tableName);
            System.out.println("Columns:");
            System.out.println("Name\t\t\tType\t\tNullable");
            System.out.println("----\t\t\t----\t\t--------");

            // Get column information for this table
            String columnsQuery = "SELECT \"COLUMN_NAME\", \"DATA_TYPE\", \"IS_NULLABLE\" " +
                "FROM information_schema.\"COLUMNS\" " +
                "WHERE \"TABLE_SCHEMA\" = 'splunk' AND \"TABLE_NAME\" = '" + tableName + "' " +
                "ORDER BY \"ORDINAL_POSITION\"";

            try (Statement colStmt = conn.createStatement();
                 ResultSet colRs = colStmt.executeQuery(columnsQuery)) {

              while (colRs.next()) {
                String colName = colRs.getString("COLUMN_NAME");
                String dataType = colRs.getString("DATA_TYPE");
                String nullable = colRs.getString("IS_NULLABLE");
                System.out.printf("%-23s %-15s %s%n", colName, dataType, nullable);
              }
            }

            // Get sample data from the table
            System.out.println("\nSample Data (first 3 rows):");
            String sampleQuery = "SELECT * FROM " + tableName + " LIMIT 3";

            try (Statement sampleStmt = conn.createStatement();
                 ResultSet sampleRs = sampleStmt.executeQuery(sampleQuery)) {

              int columnCount = sampleRs.getMetaData().getColumnCount();

              // Print column headers
              for (int i = 1; i <= columnCount; i++) {
                System.out.print(sampleRs.getMetaData().getColumnName(i));
                if (i < columnCount) System.out.print("\t");
              }
              System.out.println();

              // Print sample rows
              int rowCount = 0;
              while (sampleRs.next() && rowCount < 3) {
                for (int i = 1; i <= columnCount; i++) {
                  String value = sampleRs.getString(i);
                  if (value != null && value.length() > 20) {
                    value = value.substring(0, 17) + "...";
                  }
                  System.out.print(value != null ? value : "NULL");
                  if (i < columnCount) System.out.print("\t");
                }
                System.out.println();
                rowCount++;
              }
            } catch (Exception e) {
              System.out.println("(No sample data available: " + e.getMessage() + ")");
            }

            System.out.println("\n"
  + "=".repeat(80) + "\n");
          }
        }
      }
    }
  }

  @Test @EnabledIf("splunkTestEnabled")
  public void testDataModelDiscoveryWithDifferentApps() throws Exception {
    Properties props = new Properties();
    props.load(SplunkDataModelDiscoveryTest.class.getResourceAsStream("/splunk-connection.properties"));

    String[] apps = {"Splunk_SA_CIM", "search", "SplunkEnterpriseSecuritySuite"};

    for (String app : apps) {
      System.out.println("=== App Context: " + app + " ===\n");

      String url = "jdbc:splunk:" +
          "url='" + props.getProperty("splunk.url") + "';" +
          "user='" + props.getProperty("splunk.username") + "';" +
          "password='" + props.getProperty("splunk.password") + "';" +
          "app='" + app + "';" +
          "datamodelCacheTtl=0";

      try (Connection conn = DriverManager.getConnection(url)) {
        try (Statement stmt = conn.createStatement()) {
          String query = "SELECT COUNT(*) as table_count FROM pg_catalog.pg_tables WHERE schemaname = 'splunk'";
          try (ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
              int count = rs.getInt("table_count");
              System.out.println("Data models discovered: " + count);
            }
          }

          // List the actual tables
          String tablesQuery = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'splunk' ORDER BY tablename";
          try (ResultSet rs = stmt.executeQuery(tablesQuery)) {
            System.out.println("Available data models:");
            while (rs.next()) {
              System.out.println("  - " + rs.getString("tablename"));
            }
          }
        }
      } catch (Exception e) {
        System.out.println("Error connecting to app '" + app + "': " + e.getMessage());
      }

      System.out.println();
    }
  }

  @Test @EnabledIf("splunkTestEnabled")
  public void testDataModelFiltering() throws Exception {
    Properties props = new Properties();
    props.load(SplunkDataModelDiscoveryTest.class.getResourceAsStream("/splunk-connection.properties"));

    String[] filters = {"auth*", "web", "/^(auth|web|network)/"};

    for (String filter : filters) {
      System.out.println("=== Filter: " + filter + " ===\n");

      String url = "jdbc:splunk:" +
          "url='" + props.getProperty("splunk.url") + "';" +
          "user='" + props.getProperty("splunk.username") + "';" +
          "password='" + props.getProperty("splunk.password") + "';" +
          "app='Splunk_SA_CIM';" +
          "datamodelFilter='" + filter + "';" +
          "datamodelCacheTtl=0";

      try (Connection conn = DriverManager.getConnection(url)) {
        try (Statement stmt = conn.createStatement()) {
          String query = "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'splunk' ORDER BY tablename";
          try (ResultSet rs = stmt.executeQuery(query)) {
            System.out.println("Filtered data models:");
            while (rs.next()) {
              System.out.println("  - " + rs.getString("tablename"));
            }
          }
        }
      } catch (Exception e) {
        System.out.println("Error with filter '" + filter + "': " + e.getMessage());
      }

      System.out.println();
    }
  }

  @Test @EnabledIf("splunkTestEnabled")
  public void testSplunkSpecificMetadata() throws Exception {
    // Load driver class
    Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");

    Properties props = new Properties();

    // Load from local-properties.settings file
    File propsFile = new File("local-properties.settings");
    if (!propsFile.exists()) {
      // Try parent directory if running from submodule
      propsFile = new File("../local-properties.settings");
    }

    if (!propsFile.exists()) {
      System.out.println("Skipping test: Could not find local-properties.settings at " + propsFile.getAbsolutePath());
      return;
    }

    try (FileInputStream fis = new FileInputStream(propsFile)) {
      props.load(fis);
    } catch (Exception e) {
      System.out.println("Skipping test: Could not load local-properties.settings: " + e.getMessage());
      return;
    }

    // Verify required properties exist
    if (props.getProperty("splunk.url") == null ||
        props.getProperty("splunk.username") == null ||
        props.getProperty("splunk.password") == null) {
      System.out.println("Skipping test: Required Splunk connection properties not found");
      return;
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

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", info)) {
      System.out.println("=== Splunk-Specific Metadata ===\n");

      // Query Splunk indexes
      try (Statement stmt = conn.createStatement()) {
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

      System.out.println();

      // Query Splunk sources
      try (Statement stmt = conn.createStatement()) {
        String query = "SELECT source, sourcetype, host, index FROM pg_catalog.splunk_sources ORDER BY source";
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Splunk Data Sources:");
          System.out.println("Source\t\t\tSourcetype\t\tHost\t\tIndex");
          System.out.println("------\t\t\t----------\t\t----\t\t-----");

          while (rs.next()) {
            String source = rs.getString("source");
            String sourcetype = rs.getString("sourcetype");
            String host = rs.getString("host");
            String index = rs.getString("index");
            System.out.printf("%-23s %-23s %-15s %s%n",
                source != null ? source : "N/A",
                sourcetype != null ? sourcetype : "N/A",
                host != null ? host : "N/A",
                index != null ? index : "N/A");
          }
        }
      }
    }
  }
}
