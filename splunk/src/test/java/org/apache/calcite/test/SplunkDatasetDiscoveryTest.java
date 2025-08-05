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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Test that shows native Splunk data model and dataset discovery.
 */
@Tag("integration")
class SplunkDatasetDiscoveryTest {
  public static final String SPLUNK_URL = "https://kentest.xyz:8089";
  public static final String SPLUNK_USER = "admin";
  public static final String SPLUNK_PASSWORD = "admin123";

  private void loadDriverClass() {
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("driver not found", e);
    }
  }

  @Test void testNativeDataModelAndDatasetDiscovery() throws Exception {
    loadDriverClass();
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.setProperty("user", SPLUNK_USER);
    info.setProperty("password", SPLUNK_PASSWORD);
    info.setProperty("disableSslValidation", "true");
    info.setProperty("app", "Splunk_SA_CIM");
    info.setProperty("datamodelCacheTtl", "0");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      System.out.println("=== Native Splunk Data Model & Dataset Discovery ===\n");

      // Query discovered data models (this triggers the discovery process)
      try (Statement stmt = connection.createStatement()) {
        String query = "SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname = 'splunk' ORDER BY tablename";
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Data Models Discovered from Splunk REST API:");
          System.out.println("(Each corresponds to a root dataset in a CIM data model)\n");

          int count = 0;
          while (rs.next()) {
            count++;
            String schema = rs.getString("schemaname");
            String table = rs.getString("tablename");
            System.out.printf("%2d. %-25s (Schema: %s)%n", count, table, schema);
          }

          System.out.println("\nTotal: " + count + " data models with root datasets discovered");
        }
      }

      System.out.println("\n=== Native Discovery Process Explanation ===\n");
      System.out.println("The Splunk adapter uses dynamic discovery via REST API calls:");
      System.out.println("1. Calls /servicesNS/-/Splunk_SA_CIM/data/models to list all data models");
      System.out.println("2. For each data model, extracts the model definition JSON");
      System.out.println("3. Parses the 'objects' array which contains datasets");
      System.out.println("4. Each dataset has:");
      System.out.println("   - objectName: The dataset identifier");
      System.out.println("   - displayName: Human-readable name");
      System.out.println("   - parentName: Parent dataset (for hierarchical models)");
      System.out.println("   - fields: Array of field definitions with types");
      System.out.println("   - constraints: SPL search constraints");
      System.out.println("   - calculations: Calculated field definitions");
      System.out.println("5. Creates a SQL table for the root dataset of each model");
      System.out.println("6. Maps Splunk field types to SQL types (string->VARCHAR, number->INTEGER, etc.)");
      System.out.println("7. Adds standard Splunk fields (_time, host, source, sourcetype, index)");
      System.out.println("8. Adds CIM-specific calculated fields for known models");
      System.out.println("\nThis allows SQL queries against live Splunk data using standard CIM field names.");

      System.out.println("\n=== Sample Data Model Structure ===\n");
      System.out.println("Example: Authentication data model");
      System.out.println("Root Dataset: 'Authentication'");
      System.out.println("Fields from SPL discovery:");
      System.out.println("  - _time (timestamp) -> TIMESTAMP");
      System.out.println("  - host (string) -> VARCHAR");
      System.out.println("  - user (string) -> VARCHAR");
      System.out.println("  - src (string) -> VARCHAR");
      System.out.println("  - dest (string) -> VARCHAR");
      System.out.println("  - action (string) -> VARCHAR");
      System.out.println("CIM Calculated Fields Added:");
      System.out.println("  - is_success (BOOLEAN) = action=\"success\"");
      System.out.println("  - is_failure (BOOLEAN) = action=\"failure\"");
      System.out.println("Constraints (SPL search):");
      System.out.println("  - tag=authentication OR eventtype=authentication");
      System.out.println("\nThis enables SQL like:");
      System.out.println("SELECT user, src, dest, action FROM authentication WHERE is_failure = true");
    }
  }
}
