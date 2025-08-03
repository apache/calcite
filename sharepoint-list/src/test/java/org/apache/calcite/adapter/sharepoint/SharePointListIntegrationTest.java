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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for SharePoint List adapter using Microsoft Graph API.
 *
 * <p>These tests require SharePoint credentials to be configured.
 * Set up sharepoint-test-config.properties in the file module with:
 * <ul>
 *   <li>SHAREPOINT_TENANT_ID</li>
 *   <li>SHAREPOINT_CLIENT_ID</li>
 *   <li>SHAREPOINT_CLIENT_SECRET</li>
 *   <li>SHAREPOINT_SITE_URL</li>
 * </ul>
 */
@EnabledIfEnvironmentVariable(named = "SHAREPOINT_INTEGRATION_TESTS", matches = "true")
public class SharePointListIntegrationTest {

  private static Properties testConfig;
  private static String testListName;

  @BeforeAll
  public static void setUp() throws IOException {
    // Load test configuration
    testConfig = loadTestConfig();

    // Generate unique test list name to avoid conflicts (using lowercase)
    testListName = "test_list_" + UUID.randomUUID().toString().substring(0, 8).toLowerCase(Locale.ROOT);
  }

  private static Properties loadTestConfig() throws IOException {
    Properties props = new Properties();

    // Try to load from file module's local-test.properties
    Path configPath = Paths.get("../file/local-test.properties");
    if (!Files.exists(configPath)) {
      // Try alternate path
      configPath = Paths.get("../../file/local-test.properties");
    }

    if (Files.exists(configPath)) {
      try (FileInputStream fis = new FileInputStream(configPath.toFile())) {
        props.load(fis);
      }
    } else {
      // Fall back to environment variables
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

  @Test public void testListDiscovery() throws SQLException {
    try (Connection connection = createConnection()) {
      // Try to access the SharePoint schema and execute a simple query
      // This tests that the schema is created and lists are discovered
      Statement stmt = connection.createStatement();

      // First, let's see if we can access the schema at all
      // We'll try to get metadata about available tables
      java.sql.DatabaseMetaData metaData = connection.getMetaData();
      ResultSet rs = metaData.getTables(null, "sharepoint", "%", new String[]{"TABLE"});

      boolean foundTables = false;
      System.out.println("Available SharePoint tables:");
      while (rs.next()) {
        String tableName = rs.getString("TABLE_NAME");
        System.out.println("  - " + tableName);
        foundTables = true;
      }

      assertTrue(foundTables, "Should discover at least one SharePoint list");
    }
  }


  @Test public void testQuerySharePointList() throws SQLException {
    try (Connection connection = createConnection()) {
      Statement stmt = connection.createStatement();

      // Query the Documents list (which should exist in most SharePoint sites)
      ResultSet rs = stmt.executeQuery("SELECT id, title FROM sharepoint.documents LIMIT 5");

      System.out.println("Documents in SharePoint:");
      boolean hasData = false;
      while (rs.next()) {
        String id = rs.getString("id");
        String title = rs.getString("title");
        System.out.println("  ID: " + id + ", Title: " + title);
        hasData = true;
      }

      // Note: It's okay if the list is empty, we just want to make sure the query executes
      System.out.println("Query completed successfully" + (hasData ? " with data" : " (empty list)"));

      // The fact that we got here without exception means the integration is working
      assertTrue(true, "SharePoint query executed successfully");
    }
  }

  // Note: For now, we'll focus on read-only tests
  // Write operations would require implementing ModifiableTable properly


  private Connection createConnection() throws SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");

    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // Create SharePoint schema with auth config
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
}
