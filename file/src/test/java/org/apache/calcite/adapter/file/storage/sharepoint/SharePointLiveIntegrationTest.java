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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.file.storage.MicrosoftGraphStorageProvider;
import org.apache.calcite.adapter.file.storage.MicrosoftGraphTokenManager;
import org.apache.calcite.adapter.file.storage.StorageProvider;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Live integration test for SharePoint with Calcite using Microsoft Graph API.
 * Tests the full stack: Azure AD auth → Microsoft Graph → Calcite queries.
 */
@EnabledIfSystemProperty(named = "runSharePointAutoTests", matches = "true")
public class SharePointLiveIntegrationTest {

  private static String tenantId;
  private static String clientId;
  private static String clientSecret;
  private static String siteUrl;
  private static MicrosoftGraphTokenManager tokenManager;

  @BeforeAll
  static void setup() {
    tenantId = getRequiredConfig("SHAREPOINT_TENANT_ID");
    clientId = getRequiredConfig("SHAREPOINT_CLIENT_ID");
    clientSecret = getRequiredConfig("SHAREPOINT_CLIENT_SECRET");
    siteUrl = getRequiredConfig("SHAREPOINT_SITE_URL");

    // Create token manager
    tokenManager = new MicrosoftGraphTokenManager(tenantId, clientId, clientSecret, siteUrl);

    System.out.println("SharePoint Integration Test Setup:");
    System.out.println("  Site: " + siteUrl);
    System.out.println("  Client ID: " + clientId);
  }

  private static String getRequiredConfig(String key) {
    String value = System.getenv(key);
    if (value == null || value.isEmpty()) {
      value = System.getProperty(key);
    }
    if (value == null || value.isEmpty()) {
      throw new IllegalStateException(key + " must be set");
    }
    return value;
  }

  @Test void testSharePointDirectAccess() throws Exception {
    // Test direct SharePoint access
    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    // List documents
    List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents", false);
    assertNotNull(entries);
    System.out.println("Found " + entries.size() + " items in Shared Documents");

    // Find a CSV file if available
    StorageProvider.FileEntry csvFile = entries.stream()
        .filter(e -> !e.isDirectory() && e.getName().toLowerCase(Locale.ROOT).endsWith(".csv"))
        .findFirst()
        .orElse(null);

    if (csvFile != null) {
      System.out.println("Found CSV file: " + csvFile.getName());
      assertTrue(provider.exists(csvFile.getPath()));
    }
  }

  @Test void testCalciteSharePointQuery() throws Exception {
    // Create Calcite connection
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      // Create SharePoint-backed schema
      Map<String, Object> operand = new HashMap<>();
      operand.put("directory", "/Shared Documents");
      operand.put("storageType", "sharepoint");

      Map<String, Object> storageConfig = new HashMap<>();
      storageConfig.put("siteUrl", siteUrl);
      storageConfig.put("tenantId", tenantId);
      storageConfig.put("clientId", clientId);
      storageConfig.put("clientSecret", clientSecret);
      operand.put("storageConfig", storageConfig);

      operand.put("tableNameCasing", "LOWER");
      operand.put("columnNameCasing", "LOWER");

      // Create schema
      FileSchemaFactory factory = FileSchemaFactory.INSTANCE;
      rootSchema.add("sharepoint", factory.create(rootSchema, "sharepoint", operand));

      // List available tables
      try (Statement stmt = conn.createStatement()) {
        ResultSet rs = conn.getMetaData().getTables(null, "SHAREPOINT", "%", null);

        System.out.println("\nAvailable tables in SharePoint schema:");
        int tableCount = 0;
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          System.out.println("  - " + tableName);
          tableCount++;

          // If it's a CSV file, try to query it
          if (tableName.toLowerCase(Locale.ROOT).endsWith("_csv")) {
            testQueryTable(conn, tableName);
          }
        }

        assertTrue(tableCount > 0, "Should find at least one table");
      }
    }
  }

  private void testQueryTable(Connection conn, String tableName) {
    try (Statement stmt = conn.createStatement()) {
      System.out.println("\nQuerying table: " + tableName);

      // Get row count
      ResultSet rs =
          stmt.executeQuery("SELECT COUNT(*) as cnt FROM sharepoint." + tableName);

      if (rs.next()) {
        int count = rs.getInt("cnt");
        System.out.println("  Row count: " + count);
      }

      // Get first few rows
      rs =
          stmt.executeQuery("SELECT * FROM sharepoint." + tableName + " LIMIT 5");

      // Print column info
      int columnCount = rs.getMetaData().getColumnCount();
      System.out.print("  Columns: ");
      for (int i = 1; i <= columnCount; i++) {
        if (i > 1) System.out.print(", ");
        System.out.print(rs.getMetaData().getColumnName(i));
      }
      System.out.println();

    } catch (Exception e) {
      System.out.println("  Error querying table: " + e.getMessage());
    }
  }

  @Test void testTokenRefreshDuringQuery() throws Exception {
    // Force token refresh
    System.out.println("\nTesting token refresh mechanism...");

    MicrosoftGraphStorageProvider provider = new MicrosoftGraphStorageProvider(tokenManager);

    // Initial access
    provider.listFiles("/Shared Documents", false);
    System.out.println("Initial access successful");

    // Invalidate token
    tokenManager.invalidateToken();
    System.out.println("Token invalidated");

    // Access again (should trigger refresh)
    List<StorageProvider.FileEntry> entries = provider.listFiles("/Shared Documents", false);
    assertNotNull(entries);
    System.out.println("Access after invalidation successful - token refreshed!");
  }

  @Test void testSharePointWithSQLQuery() throws Exception {
    // Create a test CSV file in SharePoint first (manual step)
    System.out.println("\n=== SQL Query Test ===");
    System.out.println("This test assumes you have a CSV file in SharePoint.");
    System.out.println("If not, upload a simple CSV file to Shared Documents first.");

    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    info.setProperty("unquotedCasing", "UNCHANGED");
    info.setProperty("caseSensitive", "false");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConn.getRootSchema();

      // Setup SharePoint schema
      Map<String, Object> operand = createSharePointOperand();
      FileSchemaFactory factory = FileSchemaFactory.INSTANCE;
      rootSchema.add("sp", factory.create(rootSchema, "sp", operand));

      // Example queries
      try (Statement stmt = conn.createStatement()) {
        // Show tables
        System.out.println("\nSHOW TABLES:");
        ResultSet rs =
            stmt.executeQuery("SELECT table_name FROM information_schema.tables " +
            "WHERE table_schema = 'sp'");

        while (rs.next()) {
          String table = rs.getString(1);
          System.out.println("  " + table);

          // For each CSV table, show sample data
          if (table.toLowerCase(Locale.ROOT).contains("csv")) {
            System.out.println("\n  Sample from " + table + ":");
            try {
              ResultSet data =
                  stmt.executeQuery("SELECT * FROM sp." + table + " LIMIT 3");

              int cols = data.getMetaData().getColumnCount();
              while (data.next()) {
                System.out.print("    ");
                for (int i = 1; i <= cols; i++) {
                  if (i > 1) System.out.print(" | ");
                  System.out.print(data.getString(i));
                }
                System.out.println();
              }
            } catch (Exception e) {
              System.out.println("    (Unable to query: " + e.getMessage() + ")");
            }
          }
        }
      }
    }
  }

  private Map<String, Object> createSharePointOperand() {
    Map<String, Object> operand = new HashMap<>();
    operand.put("directory", "/Shared Documents");
    operand.put("storageType", "sharepoint");
    operand.put("recursive", true);

    Map<String, Object> storageConfig = new HashMap<>();
    storageConfig.put("siteUrl", siteUrl);
    storageConfig.put("tenantId", tenantId);
    storageConfig.put("clientId", clientId);
    storageConfig.put("clientSecret", clientSecret);
    operand.put("storageConfig", storageConfig);

    return operand;
  }
}
