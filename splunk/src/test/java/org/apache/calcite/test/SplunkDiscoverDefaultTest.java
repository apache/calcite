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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Test dynamic discovery with default app context.
 * Run with: -Dcalcite.test.splunk=true
 */
@Tag("integration")
@EnabledIf("splunkTestEnabled")
class SplunkDiscoverDefaultTest {

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

  @Test void testDiscoverWithDefaultAppContext() throws Exception {
    System.out.println("\n=== Testing Dynamic Discovery with DEFAULT App Context ===");

    Properties props = new Properties();
    props.setProperty("url", "https://kentest.xyz:8089");
    props.setProperty("user", "admin");
    props.setProperty("password", "admin123");
    props.setProperty("disableSslValidation", "true");
    // NO app parameter - use default

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props)) {
      DatabaseMetaData metaData = conn.getMetaData();

      System.out.println("\nDiscovered tables in default app context:");
      List<String> tables = new ArrayList<>();

      try (ResultSet rs = metaData.getTables(null, "splunk", "%", null)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          tables.add(tableName);
          System.out.println("  - " + tableName);
        }
      }

      System.out.println("\nTotal tables discovered: " + tables.size());

      // Check if we found any CIM-like models
      System.out.println("\nChecking for CIM-like models:");
      String[] possibleCIMModels = {
          "authentication", "web", "network_traffic", "email",
          "endpoint", "malware", "vulnerabilities"
      };

      for (String model : possibleCIMModels) {
        if (tables.contains(model)) {
          System.out.println("  ✓ Found: " + model);
        }
      }
    }
  }

  @Test void testDiscoverWithSearchApp() throws Exception {
    System.out.println("\n=== Testing Dynamic Discovery with 'search' App ===");

    Properties props = new Properties();
    props.setProperty("url", "https://kentest.xyz:8089");
    props.setProperty("user", "admin");
    props.setProperty("password", "admin123");
    props.setProperty("disableSslValidation", "true");
    props.setProperty("app", "search"); // Try search app

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props)) {
      DatabaseMetaData metaData = conn.getMetaData();

      System.out.println("\nDiscovered tables in 'search' app:");
      List<String> tables = new ArrayList<>();

      try (ResultSet rs = metaData.getTables(null, "splunk", "%", null)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          tables.add(tableName);
          System.out.println("  - " + tableName);
        }
      }

      System.out.println("\nTotal tables discovered: " + tables.size());
    }
  }

  @Test void testListAllApps() throws Exception {
    System.out.println("\n=== Testing List of Available Apps ===");

    // We can't directly list apps via JDBC, but we can try common app names
    String[] commonApps = {
        "search",           // Default search app
        "Splunk_SA_CIM",   // Common Information Model
        "SA-CIM",          // Alternative CIM name
        "launcher",        // Launcher app
        "splunk_instrumentation", // Instrumentation
        "user-prefs"       // User preferences
    };

    Properties baseProps = new Properties();
    baseProps.setProperty("url", "https://kentest.xyz:8089");
    baseProps.setProperty("user", "admin");
    baseProps.setProperty("password", "admin123");
    baseProps.setProperty("disableSslValidation", "true");

    for (String app : commonApps) {
      System.out.println("\nTrying app: " + app);
      Properties props = new Properties();
      props.putAll(baseProps);
      props.setProperty("app", app);

      try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props)) {
        DatabaseMetaData metaData = conn.getMetaData();
        int count = 0;

        try (ResultSet rs = metaData.getTables(null, "splunk", "%", null)) {
          while (rs.next()) {
            count++;
          }
        }

        if (count > 0) {
          System.out.println("  ✓ App exists - found " + count + " tables");
        } else {
          System.out.println("  ✗ No tables found (app might not exist or have no models)");
        }
      } catch (Exception e) {
        System.out.println("  ✗ Error: " + e.getMessage());
      }
    }
  }
}
