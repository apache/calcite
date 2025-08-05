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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test dynamic discovery of Splunk datamodels.
 * Run with: -Dcalcite.test.splunk=true
 */
@Tag("integration")
@EnabledIf("splunkTestEnabled")
class SplunkDynamicDiscoveryTest {

  @BeforeAll
  static void setUp() {
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Splunk driver not found", e);
    }
  }

  private static boolean splunkTestEnabled() {
    return System.getProperty("CALCITE_TEST_SPLUNK", "false").equals("true") ||
           System.getenv("CALCITE_TEST_SPLUNK") != null;
  }

  @Test void testDynamicDiscovery() throws Exception {
    System.out.println("\n=== Testing Dynamic Discovery ===");

    Properties props = new Properties();
    props.setProperty("url", "https://kentest.xyz:8089");
    props.setProperty("user", "admin");
    props.setProperty("password", "admin123");
    props.setProperty("disableSslValidation", "true");
    props.setProperty("app", "Splunk_SA_CIM");
    props.setProperty("datamodelFilter", "/^(Authentication|Web)$/");

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props)) {
      DatabaseMetaData metaData = conn.getMetaData();

      System.out.println("\nDiscovered tables:");
      int count = 0;
      boolean foundAuth = false;
      boolean foundWeb = false;

      try (ResultSet rs = metaData.getTables(null, "splunk", "%", null)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          System.out.println("  - " + tableName);
          count++;

          if ("authentication".equals(tableName.toLowerCase())) {
            foundAuth = true;
          }
          if ("web".equals(tableName.toLowerCase())) {
            foundWeb = true;
          }
        }
      }

      System.out.println("\nTotal tables discovered: " + count);
      System.out.println("Found authentication table: " + foundAuth);
      System.out.println("Found web table: " + foundWeb);

      // With filter applied, we should find authentication and web tables
      assertTrue(count >= 2, "Should find at least 2 tables with filter");
      assertTrue(foundAuth, "Should find authentication table");
      assertTrue(foundWeb, "Should find web table");

      // Try a simple query on authentication if found
      if (foundAuth) {
        try (Statement stmt = conn.createStatement()) {
          String query = "SELECT COUNT(*) FROM splunk.Authentication LIMIT 1";
          System.out.println("\nExecuting test query: " + query);
          try (ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
              System.out.println("Query succeeded!");
            }
          }
        } catch (Exception e) {
          System.out.println("Query failed (expected if no data): " + e.getMessage());
        }
      }
    }
  }

  @Test void testDynamicDiscoveryWithFilter() throws Exception {
    System.out.println("\n=== Testing Dynamic Discovery with Filter ===");

    Properties props = new Properties();
    props.setProperty("url", "https://kentest.xyz:8089");
    props.setProperty("user", "admin");
    props.setProperty("password", "admin123");
    props.setProperty("disableSslValidation", "true");
    props.setProperty("app", "Splunk_SA_CIM");
    props.setProperty("datamodelFilter", "/^(Authentication|Web|Email|Endpoint)$/");

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props)) {
      DatabaseMetaData metaData = conn.getMetaData();

      System.out.println("\nDiscovered tables with filter:");
      int count = 0;
      boolean foundAuth = false;
      boolean foundWeb = false;

      try (ResultSet rs = metaData.getTables(null, "splunk", "%", null)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          System.out.println("  - " + tableName);
          count++;

          if ("authentication".equals(tableName.toLowerCase())) {
            foundAuth = true;
          }
          if ("web".equals(tableName.toLowerCase())) {
            foundWeb = true;
          }
        }
      }

      System.out.println("\nTotal tables discovered: " + count);
      assertTrue(count >= 2, "Should discover at least 2 tables");
      assertTrue(foundAuth || foundWeb, "Should find at least one of auth or web tables");
    }
  }
}
