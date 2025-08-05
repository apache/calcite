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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.util.Sources;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Compare dynamically discovered models with hardcoded CIM models.
 * Run with: -Dcalcite.test.splunk=true
 */
@Tag("integration")
@EnabledIf("splunkTestEnabled")
class SplunkModelComparisonTest {

  private static boolean splunkTestEnabled() {
    return System.getProperty("CALCITE_TEST_SPLUNK", "false").equals("true") ||
           System.getenv("CALCITE_TEST_SPLUNK") != null;
  }

  @Test void compareModels() throws Exception {
    System.out.println("\n=== Comparing Dynamic Discovery with Hardcoded CIM Models ===");

    // Define the hardcoded CIM models and their aliases from CimModelBuilder
    Map<String, Set<String>> hardcodedModels = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    // Add all CIM models with their aliases
    hardcodedModels.put("alerts", Set.of("alerts"));
    hardcodedModels.put("application_state", Set.of("application_state", "applicationstate"));
    hardcodedModels.put("authentication", Set.of("authentication"));
    hardcodedModels.put("certificates", Set.of("certificates"));
    hardcodedModels.put("change", Set.of("change"));
    hardcodedModels.put("compute_inventory", Set.of("compute_inventory", "computeinventory"));
    hardcodedModels.put("data_access", Set.of("data_access", "dataaccess"));
    hardcodedModels.put("databases", Set.of("databases", "database"));
    hardcodedModels.put("data_loss_prevention", Set.of("data_loss_prevention", "dlp"));
    hardcodedModels.put("email", Set.of("email"));
    hardcodedModels.put("endpoint", Set.of("endpoint"));
    hardcodedModels.put("event_signatures", Set.of("event_signatures", "eventsignatures"));
    hardcodedModels.put("interprocess_messaging", Set.of("interprocess_messaging", "messaging"));
    hardcodedModels.put("intrusion_detection", Set.of("intrusion_detection", "ids"));
    hardcodedModels.put("inventory", Set.of("inventory"));
    hardcodedModels.put("jvm", Set.of("jvm"));
    hardcodedModels.put("malware", Set.of("malware"));
    hardcodedModels.put("network_resolution", Set.of("network_resolution", "dns"));
    hardcodedModels.put("network_sessions", Set.of("network_sessions", "networksessions"));
    hardcodedModels.put("network_traffic", Set.of("network_traffic", "network"));
    hardcodedModels.put("performance", Set.of("performance"));
    hardcodedModels.put("ticket_management", Set.of("ticket_management", "ticketing"));
    hardcodedModels.put("updates", Set.of("updates"));
    hardcodedModels.put("vulnerabilities", Set.of("vulnerabilities", "vulnerability"));
    hardcodedModels.put("web", Set.of("web"));

    // Get dynamically discovered models using model file
    Properties info = new Properties();
    info.put("model", Sources.of(SplunkModelComparisonTest.class.getResource("/test-splunk-model.json")).file().getAbsolutePath());

    Set<String> discoveredModels = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      CalciteConnection calciteConn = conn.unwrap(CalciteConnection.class);
      DatabaseMetaData metaData = calciteConn.getMetaData();

      try (ResultSet rs = metaData.getTables(null, "splunk", "%", null)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          discoveredModels.add(tableName);
        }
      }
    }

    // Compare the sets
    System.out.println("\n--- Hardcoded CIM Models (" + hardcodedModels.size() + " total) ---");
    for (String model : hardcodedModels.keySet()) {
      System.out.println("  - " + model);
    }

    System.out.println("\n--- Dynamically Discovered Models (" + discoveredModels.size() + " total) ---");
    for (String model : discoveredModels) {
      System.out.println("  - " + model);
    }

    // Find differences
    Set<String> onlyInHardcoded = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    onlyInHardcoded.addAll(hardcodedModels.keySet());
    onlyInHardcoded.removeAll(discoveredModels);

    Set<String> onlyInDiscovered = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    onlyInDiscovered.addAll(discoveredModels);
    onlyInDiscovered.removeAll(hardcodedModels.keySet());

    System.out.println("\n--- Models Only in Hardcoded (" + onlyInHardcoded.size() + ") ---");
    if (onlyInHardcoded.isEmpty()) {
      System.out.println("  (none)");
    } else {
      for (String model : onlyInHardcoded) {
        System.out.println("  - " + model);
      }
    }

    System.out.println("\n--- Models Only in Dynamic Discovery (" + onlyInDiscovered.size() + ") ---");
    if (onlyInDiscovered.isEmpty()) {
      System.out.println("  (none)");
    } else {
      for (String model : onlyInDiscovered) {
        System.out.println("  - " + model);
      }
    }

    // Check naming patterns
    System.out.println("\n--- Naming Pattern Analysis ---");
    System.out.println("Checking if discovered models would match hardcoded aliases:");

    for (String discovered : onlyInDiscovered) {
      System.out.println("\nDiscovered: " + discovered);
      // Check if it matches any hardcoded model alias
      for (Map.Entry<String, Set<String>> entry : hardcodedModels.entrySet()) {
        for (String alias : entry.getValue()) {
          if (alias.equalsIgnoreCase(discovered)) {
            System.out.println("  → Would match hardcoded model '" + entry.getKey() + "' via alias '" + alias + "'");
          }
        }
      }

      // Check if removing underscores would match
      String noUnderscore = discovered.replace("_", "");
      for (Map.Entry<String, Set<String>> entry : hardcodedModels.entrySet()) {
        for (String alias : entry.getValue()) {
          if (alias.equalsIgnoreCase(noUnderscore)) {
            System.out.println("  → Would match hardcoded model '" + entry.getKey() + "' if underscores removed");
          }
        }
      }
    }

    // Show which hardcoded models are missing
    System.out.println("\n--- Missing Standard CIM Models ---");
    for (String missing : onlyInHardcoded) {
      System.out.println("Model '" + missing + "' is not in dynamic discovery");
      System.out.println("  CimModelBuilder accepts: " + hardcodedModels.get(missing));
    }

    // Summary
    System.out.println("\n--- Summary ---");
    System.out.println("Hardcoded CIM models: " + hardcodedModels.size());
    System.out.println("Dynamically discovered: " + discoveredModels.size());
    System.out.println("Missing from discovery: " + onlyInHardcoded.size());
    System.out.println("Extra in discovery: " + onlyInDiscovered.size());

    // List all discovered models that are not standard CIM
    System.out.println("\n--- Non-Standard Models in Discovery ---");
    for (String model : onlyInDiscovered) {
      boolean isStandardCim = false;
      // Check against all aliases
      for (Set<String> aliases : hardcodedModels.values()) {
        if (aliases.stream().anyMatch(alias -> alias.equalsIgnoreCase(model))) {
          isStandardCim = true;
          break;
        }
      }
      if (!isStandardCim) {
        System.out.println("  - " + model + " (not in CIM 6.0 standard 24 models)");
      }
    }
  }
}
