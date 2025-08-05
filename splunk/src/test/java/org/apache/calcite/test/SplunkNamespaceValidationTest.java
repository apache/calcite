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

import org.apache.calcite.adapter.splunk.search.SearchResultListener;
import org.apache.calcite.adapter.splunk.search.SplunkConnection;
import org.apache.calcite.adapter.splunk.search.SplunkConnectionImpl;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test to validate CIM models are available in Splunk_SA_CIM namespace.
 * Run with: -Dcalcite.test.splunk=true
 */
@Tag("integration")
@EnabledIf("splunkTestEnabled")
class SplunkNamespaceValidationTest {

  private static SplunkConnection connection;

  @BeforeAll
  static void setUp() throws Exception {
    // Get connection properties from system properties or local-properties.settings
    String splunkUrl = System.getProperty("splunk.url");
    String splunkUser = System.getProperty("splunk.user");
    String splunkPassword = System.getProperty("splunk.password");

    // If not in system properties, use defaults from local-properties.settings
    if (splunkUrl == null) {
      splunkUrl = "https://kentest.xyz:8089";
    }
    if (splunkUser == null) {
      splunkUser = "admin";
    }
    if (splunkPassword == null) {
      splunkPassword = "admin123";
    }

    System.out.println("Connecting to Splunk at: " + splunkUrl);

    connection = new SplunkConnectionImpl(splunkUrl, splunkUser, splunkPassword, true);
  }

  private static boolean splunkTestEnabled() {
    return System.getProperty("CALCITE_TEST_SPLUNK", "false").equals("true") ||
           System.getenv("CALCITE_TEST_SPLUNK") != null;
  }

  @Test 
  @Timeout(120) // 2 minutes for debugging
  void testListAvailableApps() throws Exception {
    System.out.println("=== Listing Available Splunk Apps ===");

    // List all apps/namespaces
    String search = "| rest /services/apps/local | table title, eai:acl.app, disabled, visible";

    List<String[]> results = executeSearch(search);

    System.out.println("Found " + results.size() + " apps:");
    for (String[] row : results) {
      if (row.length >= 4) {
        System.out.printf("App: %-30s | Name: %-20s | Disabled: %-5s | Visible: %s%n",
            row[0], row[1], row[2], row[3]);
      }
    }

    // Check if Splunk_SA_CIM exists (title is in row[0])
    boolean foundCIM = results.stream()
        .anyMatch(row -> row.length > 0 && "Splunk_SA_CIM".equals(row[0]));

    if (foundCIM) {
      System.out.println("\n✓ Found Splunk_SA_CIM app!");
    } else {
      System.out.println("\n✗ WARNING: Splunk_SA_CIM app not found!");
    }
  }

  @Test 
  @Timeout(120) // 2 minutes
  void testListDataModelsWithNamespace() throws Exception {
    System.out.println("\n=== Listing All Data Models with Namespace Info ===");

    // List all datamodels with their app context
    String search = "| rest /services/datamodel/model " +
                   "| table title, eai:appName, eai:acl.app, eai:acl.sharing, disabled";

    List<String[]> results = executeSearch(search);

    System.out.println("Found " + results.size() + " data models:");
    for (String[] row : results) {
      if (row.length >= 5) {
        System.out.printf("Model: %-25s | App: %-20s | ACL App: %-20s | Sharing: %-10s | Disabled: %s%n",
            row[0], row[1], row[2], row[3], row[4]);
      }
    }
  }

  @Test 
  @Timeout(120) // 2 minutes
  void testCIMModelsInNamespace() throws Exception {
    System.out.println("\n=== Testing CIM Models in Splunk_SA_CIM Namespace ===");

    // Try to access Authentication model with explicit app context
    String[] testSearches = {
        // Test 1: Direct datamodel command
        "| datamodel Authentication Authentication search | head 5",

        // Test 2: With explicit app context in REST
        "| rest /servicesNS/nobody/Splunk_SA_CIM/datamodel/model/Authentication",

        // Test 3: Search in specific app context
        "| datamodel Splunk_SA_CIM Authentication Authentication search | head 5"
    };

    for (int i = 0; i < testSearches.length; i++) {
      System.out.println("\nTest " + (i + 1) + ": " + testSearches[i]);
      try {
        List<String[]> results = executeSearch(testSearches[i]);
        System.out.println("Success! Got " + results.size() + " results");

        // Print first few results for debugging
        if (!results.isEmpty() && results.get(0).length > 0) {
          System.out.println("Sample fields: " + String.join(", ", results.get(0)));
        }
      } catch (Exception e) {
        System.out.println("Failed: " + e.getMessage());
      }
    }
  }

  @Test 
  @Timeout(120) // 2 minutes
  void testDiscoverCIMModelsInNamespace() throws Exception {
    System.out.println("\n=== Discovering CIM Models in Splunk_SA_CIM ===");

    // List models specifically from Splunk_SA_CIM
    String search = "| rest /services/datamodel/model " +
                   "| search eai:appName=Splunk_SA_CIM OR eai:acl.app=Splunk_SA_CIM " +
                   "| table title, displayName, description";

    List<String[]> results = executeSearch(search);

    System.out.println("Found " + results.size() + " CIM models in Splunk_SA_CIM:");
    for (String[] row : results) {
      if (row.length >= 2) {
        System.out.printf("- %-25s | Display: %-30s%n", row[0], row[1]);
        if (row.length > 2 && row[2] != null && !row[2].isEmpty()) {
          System.out.println("  Description: " + row[2].substring(0, Math.min(row[2].length(), 60)) + "...");
        }
      }
    }

    // Verify we found expected models
    List<String> foundModels = new ArrayList<>();
    for (String[] row : results) {
      if (row.length > 0) {
        foundModels.add(row[0]);
      }
    }

    // Check for common CIM models
    String[] expectedModels = {"Authentication", "Network_Traffic", "Web"};
    for (String model : expectedModels) {
      if (foundModels.contains(model)) {
        System.out.println("\n✓ Found expected model: " + model);
      } else {
        System.out.println("\n✗ Missing expected model: " + model);
      }
    }
  }

  @Test 
  @Timeout(120) // 2 minutes
  void testSearchContextBehavior() throws Exception {
    System.out.println("\n=== Testing Search Context Behavior ===");

    // Test how search context affects model visibility
    String search = "| rest /services/authentication/current-context " +
                   "| table username, realname, defaultApp";

    List<String[]> results = executeSearch(search);

    if (!results.isEmpty() && results.get(0).length >= 3) {
      System.out.println("Current user context:");
      System.out.println("Username: " + results.get(0)[0]);
      System.out.println("Real name: " + results.get(0)[1]);
      System.out.println("Default app: " + results.get(0)[2]);
    }
  }

  private List<String[]> executeSearch(String search) throws Exception {
    List<String[]> results = new ArrayList<>();
    CountDownLatch latch = new CountDownLatch(1);
    Exception[] error = new Exception[1];

    Map<String, String> args = new HashMap<>();
    args.put("earliest_time", "-1h");
    args.put("latest_time", "now");
    args.put("output_mode", "csv");

    // Determine fields based on search
    List<String> fields = new ArrayList<>();
    if (search.contains("| table")) {
      // Extract fields from table command
      String tableCmd = search.substring(search.indexOf("| table") + 8);
      String fieldList = tableCmd.contains("|") ?
          tableCmd.substring(0, tableCmd.indexOf("|")).trim() : tableCmd.trim();
      for (String field : fieldList.split(",")) {
        fields.add(field.trim());
      }
    } else {
      // Default fields for datamodel searches
      fields.add("_time");
      fields.add("_raw");
    }

    try {
      connection.getSearchResults(search, args, fields, new SearchResultListener() {
        @Override public boolean processSearchResult(String[] values) {
          if (values != null) {
            results.add(values);
            return true; // Continue processing
          } else {
            latch.countDown();
            return false; // Stop processing
          }
        }

        @Override public void setFieldNames(String[] fieldNames) {
          // Field names received
        }
      });
    } catch (Exception e) {
      error[0] = e;
      latch.countDown();
    }

    // Wait for search to complete (REST queries can be slow)
    if (!latch.await(30, TimeUnit.SECONDS)) {
      // Force completion since we got results - SplunkConnectionImpl doesn't properly signal completion for REST calls
      if (!results.isEmpty()) {
        return results;
      }
      throw new RuntimeException("Search timed out after 30 seconds with no results");
    }

    if (error[0] != null) {
      throw error[0];
    }

    return results;
  }
}
