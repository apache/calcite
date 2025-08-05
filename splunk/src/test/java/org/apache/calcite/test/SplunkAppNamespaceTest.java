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

import org.apache.calcite.adapter.splunk.search.SplunkConnectionImpl;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Test to understand Splunk app namespaces and CIM model visibility.
 * Run with: -Dcalcite.test.splunk=true
 */
@Tag("integration")
@EnabledIf("splunkTestEnabled")
class SplunkAppNamespaceTest {

  private static boolean splunkTestEnabled() {
    return System.getProperty("CALCITE_TEST_SPLUNK", "false").equals("true") ||
           System.getenv("CALCITE_TEST_SPLUNK") != null;
  }

  @Test void testDataModelVisibilityInSplunk() throws Exception {
    System.out.println("\n=== Testing Data Model Visibility in Splunk ===");

    Properties props = new Properties();
    props.setProperty("url", "https://kentest.xyz:8089");
    props.setProperty("user", "admin");
    props.setProperty("password", "admin123");
    props.setProperty("disableSslValidation", "true");
    props.setProperty("cimModel", "authentication");

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props);
         Statement stmt = conn.createStatement()) {

      // Test 1: Try basic datamodel query (what we currently generate)
      System.out.println("\n1. Testing basic datamodel query:");
      try {
        String query = "SELECT * FROM splunk.authentication LIMIT 1";
        System.out.println("SQL: " + query);

        // This should work and show us what SPL is generated
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            System.out.println("✓ Query succeeded");
          }
        }
      } catch (Exception e) {
        System.out.println("✗ Query failed: " + e.getMessage());
      }
    }
  }

  @Test void testActualSplunkSearches() throws Exception {
    System.out.println("\n=== Testing Direct Splunk Searches ===");

    // Connect directly to Splunk to test searches
    SplunkConnectionImpl splunkConn =
        new SplunkConnectionImpl("https://kentest.xyz:8089", "admin", "admin123", true);

    // Test different ways to access datamodels
    String[] testSearches = {
        // 1. Basic datamodel command (what we use now)
        "| datamodel Authentication Authentication search | head 1",

        // 2. With explicit app context
        "| datamodel Splunk_SA_CIM.Authentication Authentication search | head 1",

        // 3. Check what datamodels are visible
        "| rest /services/datamodel/model | table title eai:appName eai:acl.app | head 10",

        // 4. Check apps
        "| rest /services/apps/local | search visible=1 | table title label | head 10"
    };

    for (String search : testSearches) {
      System.out.println("\nTesting: " + search);
      try {
        // Execute search and print results
        System.out.println("(Would execute search and show results)");
        // Note: Can't easily execute without refactoring SearchResultListener
      } catch (Exception e) {
        System.out.println("Error: " + e.getMessage());
      }
    }
  }

  @Test void testWhatSplunkSchemaReallySees() throws Exception {
    System.out.println("\n=== Understanding SplunkSchema Behavior ===");

    Properties props = new Properties();
    props.setProperty("url", "https://kentest.xyz:8089");
    props.setProperty("user", "admin");
    props.setProperty("password", "admin123");
    props.setProperty("disableSslValidation", "true");

    // Test with different CIM model configurations
    String[] configs = {
        "authentication",
        "authentication,web",
        "Splunk_SA_CIM.authentication",  // Does this work?
        "Splunk_SA_CIM:authentication"   // Or this?
    };

    for (String config : configs) {
      System.out.println("\nTesting cimModel config: " + config);
      props.setProperty("cimModel", config);

      try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props)) {
        // List what tables we see
        try (ResultSet rs = conn.getMetaData().getTables(null, "splunk", "%", null)) {
          System.out.println("Tables found:");
          while (rs.next()) {
            System.out.println("  - " + rs.getString("TABLE_NAME"));
          }
        }
      } catch (Exception e) {
        System.out.println("  Error: " + e.getMessage());
      }
    }
  }
}
