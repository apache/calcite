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

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for querying SharePoint metadata through PostgreSQL-compatible system catalogs.
 * This test verifies that real SharePoint metadata is returned by the metadata queries.
 */
@EnabledIfEnvironmentVariable(named = "SHAREPOINT_INTEGRATION_TESTS", matches = "true")
class SharePointMetadataQueryTest {

  @Test void testPgTablesQuery() {
    Map<String, Object> authConfig = createAuthConfig();

    SharePointListSchema schema =
        new SharePointListSchema(loadTestConfig().getProperty("SHAREPOINT_SITE_URL"), authConfig);

    // Get the pg_catalog schema and pg_tables table
    Schema pgCatalog = schema.subSchemas().get("pg_catalog");
    assertNotNull(pgCatalog);

    Table pgTablesTable = pgCatalog.tables().get("pg_tables");
    assertNotNull(pgTablesTable);
    assertTrue(pgTablesTable instanceof ScannableTable);

    // Execute a scan to get actual data
    ScannableTable scannableTable = (ScannableTable) pgTablesTable;
    Enumerable<Object[]> results = scannableTable.scan(null);

    // Verify we get results
    int rowCount = 0;
    for (Object[] row : results) {
      rowCount++;
      assertNotNull(row);
      assertTrue(row.length >= 7); // pg_tables has 7 columns

      String schemaName = (String) row[0];
      String tableName = (String) row[1];
      String tableOwner = (String) row[2];

      assertNotNull(schemaName);
      assertNotNull(tableName);
      assertNotNull(tableOwner);

      System.out.println("Found table: " + schemaName + "." + tableName + " owned by " + tableOwner);
    }

    assertTrue(rowCount > 0, "Should find at least one SharePoint list");
  }

  @Test void testInformationSchemaTablesQuery() {
    Map<String, Object> authConfig = createAuthConfig();

    SharePointListSchema schema =
        new SharePointListSchema(loadTestConfig().getProperty("SHAREPOINT_SITE_URL"), authConfig);

    // Get the information_schema and tables table
    Schema informationSchema = schema.subSchemas().get("information_schema");
    assertNotNull(informationSchema);

    Table tablesTable = informationSchema.tables().get("tables");
    assertNotNull(tablesTable);
    assertTrue(tablesTable instanceof ScannableTable);

    // Execute a scan to get actual data
    ScannableTable scannableTable = (ScannableTable) tablesTable;
    Enumerable<Object[]> results = scannableTable.scan(null);

    // Verify we get results
    int rowCount = 0;
    for (Object[] row : results) {
      rowCount++;
      assertNotNull(row);
      assertTrue(row.length >= 7); // information_schema.tables has 7 columns

      String catalogName = (String) row[0];
      String schemaName = (String) row[1];
      String tableName = (String) row[2];
      String tableType = (String) row[3];

      assertNotNull(catalogName);
      assertNotNull(schemaName);
      assertNotNull(tableName);
      assertNotNull(tableType);

      System.out.println("Found table: " + catalogName + "." + schemaName + "." + tableName + " (" + tableType + ")");
    }

    assertTrue(rowCount > 0, "Should find at least one SharePoint list");
  }

  @Test void testInformationSchemaColumnsQuery() {
    Map<String, Object> authConfig = createAuthConfig();

    SharePointListSchema schema =
        new SharePointListSchema(loadTestConfig().getProperty("SHAREPOINT_SITE_URL"), authConfig);

    // Get the information_schema and columns table
    Schema informationSchema = schema.subSchemas().get("information_schema");
    assertNotNull(informationSchema);

    Table columnsTable = informationSchema.tables().get("columns");
    assertNotNull(columnsTable);
    assertTrue(columnsTable instanceof ScannableTable);

    // Execute a scan to get actual data
    ScannableTable scannableTable = (ScannableTable) columnsTable;
    Enumerable<Object[]> results = scannableTable.scan(null);

    // Verify we get results
    int rowCount = 0;
    for (Object[] row : results) {
      rowCount++;
      assertNotNull(row);
      assertTrue(row.length >= 13); // information_schema.columns has 13 columns

      String catalogName = (String) row[0];
      String schemaName = (String) row[1];
      String tableName = (String) row[2];
      String columnName = (String) row[3];
      Integer ordinalPosition = (Integer) row[4];
      String dataType = (String) row[7];

      assertNotNull(catalogName);
      assertNotNull(schemaName);
      assertNotNull(tableName);
      assertNotNull(columnName);
      assertNotNull(ordinalPosition);
      assertNotNull(dataType);

      System.out.println("Found column: " + tableName + "." + columnName + " (" + dataType + ") position " + ordinalPosition);
    }

    assertTrue(rowCount > 0, "Should find at least one SharePoint list column");
  }

  @Test void testSharePointListsQuery() {
    Map<String, Object> authConfig = createAuthConfig();

    SharePointListSchema schema =
        new SharePointListSchema(loadTestConfig().getProperty("SHAREPOINT_SITE_URL"), authConfig);

    // Get the pg_catalog schema and sharepoint_lists table
    Schema pgCatalog = schema.subSchemas().get("pg_catalog");
    assertNotNull(pgCatalog);

    Table sharePointListsTable = pgCatalog.tables().get("sharepoint_lists");
    assertNotNull(sharePointListsTable);
    assertTrue(sharePointListsTable instanceof ScannableTable);

    // Execute a scan to get actual data
    ScannableTable scannableTable = (ScannableTable) sharePointListsTable;
    Enumerable<Object[]> results = scannableTable.scan(null);

    // Verify we get results
    int rowCount = 0;
    for (Object[] row : results) {
      rowCount++;
      assertNotNull(row);
      assertTrue(row.length >= 10); // sharepoint_lists has 10 columns

      String listId = (String) row[0];
      String listName = (String) row[1];
      String displayName = (String) row[2];
      String entityTypeName = (String) row[3];
      String templateType = (String) row[4];
      String baseType = (String) row[5];
      String siteUrl = (String) row[9];

      assertNotNull(listId);
      assertNotNull(listName);
      assertNotNull(displayName);
      assertNotNull(entityTypeName);
      assertNotNull(templateType);
      assertNotNull(baseType);
      assertNotNull(siteUrl);

      System.out.println("Found SharePoint list: " + displayName + " (" + listId + ") type: " + templateType);
    }

    assertTrue(rowCount > 0, "Should find at least one SharePoint list");
  }

  private Map<String, Object> createAuthConfig() {
    // Load from local-test.properties file
    Properties props = loadTestConfig();

    Map<String, Object> authConfig = new HashMap<>();
    authConfig.put("authType", "CLIENT_CREDENTIALS");
    authConfig.put("tenantId", props.getProperty("SHAREPOINT_TENANT_ID"));
    authConfig.put("clientId", props.getProperty("SHAREPOINT_CLIENT_ID"));
    authConfig.put("clientSecret", props.getProperty("SHAREPOINT_CLIENT_SECRET"));
    return authConfig;
  }

  private Properties loadTestConfig() {
    Properties props = new Properties();
    try {
      // Try to load from file module's local-test.properties
      java.nio.file.Path configPath = java.nio.file.Paths.get("../file/local-test.properties");
      if (!java.nio.file.Files.exists(configPath)) {
        configPath = java.nio.file.Paths.get("../../file/local-test.properties");
      }

      if (java.nio.file.Files.exists(configPath)) {
        try (java.io.FileInputStream fis = new java.io.FileInputStream(configPath.toFile())) {
          props.load(fis);
        }
      } else {
        // Fall back to environment variables
        props.setProperty("SHAREPOINT_TENANT_ID", System.getenv("SHAREPOINT_TENANT_ID"));
        props.setProperty("SHAREPOINT_CLIENT_ID", System.getenv("SHAREPOINT_CLIENT_ID"));
        props.setProperty("SHAREPOINT_CLIENT_SECRET", System.getenv("SHAREPOINT_CLIENT_SECRET"));
        props.setProperty("SHAREPOINT_SITE_URL", System.getenv("SHAREPOINT_SITE_URL"));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to load test configuration", e);
    }
    return props;
  }

}
