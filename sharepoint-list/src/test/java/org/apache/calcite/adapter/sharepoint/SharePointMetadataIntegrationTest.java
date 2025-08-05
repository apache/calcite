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

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.lookup.Lookup;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for SharePoint metadata schema functionality.
 * Tests the integration of PostgreSQL-compatible system catalogs.
 */
@EnabledIfEnvironmentVariable(named = "SHAREPOINT_INTEGRATION_TESTS", matches = "true")
class SharePointMetadataIntegrationTest {

  @Test void testMetadataSchemaIntegration() {
    Map<String, Object> authConfig = createAuthConfig();

    SharePointListSchema schema =
        new SharePointListSchema(loadTestConfig().getProperty("SHAREPOINT_SITE_URL"), authConfig);

    // Test that metadata sub-schemas are available using the new Lookup API
    Lookup<? extends Schema> subSchemaLookup = schema.subSchemas();
    assertTrue(subSchemaLookup.get("pg_catalog") != null);
    assertTrue(subSchemaLookup.get("information_schema") != null);

    // Test accessing pg_catalog schema
    Schema pgCatalog = subSchemaLookup.get("pg_catalog");
    assertNotNull(pgCatalog);

    // Test accessing information_schema
    Schema informationSchema = subSchemaLookup.get("information_schema");
    assertNotNull(informationSchema);

    // Test that metadata tables are available
    Table pgTables = pgCatalog.tables().get("pg_tables");
    assertNotNull(pgTables);

    Table infoTables = informationSchema.tables().get("TABLES");
    assertNotNull(infoTables);

    Table infoColumns = informationSchema.tables().get("COLUMNS");
    assertNotNull(infoColumns);

    Table infoSchemata = informationSchema.tables().get("SCHEMATA");
    assertNotNull(infoSchemata);

    Table spLists = pgCatalog.tables().get("sharepoint_lists");
    assertNotNull(spLists);
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
