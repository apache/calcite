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

import org.apache.calcite.config.CalciteSystemProperty;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Test to check which CIM models have data in the Splunk instance.
 */
@EnabledIf("isSplunkEnabled")
class CheckAllCimModels {

  private static String SPLUNK_URL = "https://localhost:8089";
  private static String SPLUNK_USER = "admin";
  private static String SPLUNK_PASSWORD = "changeme";
  private static boolean DISABLE_SSL_VALIDATION = false;

  static {
    loadConnectionProperties();
  }

  private static void loadConnectionProperties() {
    File[] possibleLocations = {
        new File("local-properties.settings"),
        new File("splunk/local-properties.settings"),
        new File("../splunk/local-properties.settings")
    };

    File propsFile = null;
    for (File location : possibleLocations) {
      if (location.exists()) {
        propsFile = location;
        break;
      }
    }

    if (propsFile != null) {
      Properties props = new Properties();
      try (FileInputStream fis = new FileInputStream(propsFile)) {
        props.load(fis);

        if (props.containsKey("splunk.url")) {
          SPLUNK_URL = props.getProperty("splunk.url");
        }
        if (props.containsKey("splunk.username")) {
          SPLUNK_USER = props.getProperty("splunk.username");
        }
        if (props.containsKey("splunk.password")) {
          SPLUNK_PASSWORD = props.getProperty("splunk.password");
        }
        if (props.containsKey("splunk.ssl.insecure")) {
          DISABLE_SSL_VALIDATION = Boolean.parseBoolean(props.getProperty("splunk.ssl.insecure"));
        }
      } catch (IOException e) {
        System.err.println("Warning: Could not load local-properties.settings: " + e.getMessage());
      }
    }
  }

  static boolean isSplunkEnabled() {
    return CalciteSystemProperty.TEST_SPLUNK.value();
  }

  @Test void checkAllCimModelsForData() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");

    // All 24 CIM models from CIM 6.0
    List<String> allCimModels =
        Arrays.asList("alerts", "application_state", "authentication", "certificates",
        "change", "compute_inventory", "data_access", "databases",
        "data_loss_prevention", "email", "endpoint", "event_signatures",
        "interprocess_messaging", "intrusion_detection", "jvm", "malware",
        "network_resolution", "network_sessions", "network_traffic",
        "performance", "ticket_management", "updates", "vulnerabilities", "web");

    System.out.println("\nChecking all CIM models for data...\n");
    System.out.println("Model Name                    | Row Count | Sample Fields");
    System.out.println("------------------------------|-----------|--------------------------------------------------");

    for (String model : allCimModels) {
      checkModel(model);
    }
  }

  private void checkModel(String modelName) {
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.put("user", SPLUNK_USER);
    info.put("password", SPLUNK_PASSWORD);
    if (DISABLE_SSL_VALIDATION) {
      info.put("disableSslValidation", "true");
    }
    info.put("cimModel", modelName);

    System.out.println("\n=== Checking model: " + modelName + " ===");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      System.out.println("Connection established successfully");

      try (Statement stmt = connection.createStatement()) {
        // Count rows
        String countSql = String.format("SELECT COUNT(*) as cnt FROM \"splunk\".\"%s\"", modelName);
        System.out.println("Executing SQL: " + countSql);
        int rowCount = 0;

        try (ResultSet rs = stmt.executeQuery(countSql)) {
          if (rs.next()) {
            rowCount = rs.getInt("cnt");
          }
          System.out.println("Row count: " + rowCount);
        } catch (SQLException sqlEx) {
          System.err.println("SQL execution error:");
          System.err.println("  SQL State: " + sqlEx.getSQLState());
          System.err.println("  Error Code: " + sqlEx.getErrorCode());
          System.err.println("  Message: " + sqlEx.getMessage());

          // Check if this is a Splunk-specific error
          Throwable cause = sqlEx.getCause();
          if (cause != null) {
            System.err.println("  Underlying cause: " + cause.getClass().getName());
            System.err.println("  Cause message: " + cause.getMessage());

            // Try to get more details from the exception chain
            if (cause.getCause() != null) {
              System.err.println("  Root cause: " + cause.getCause().getClass().getName());
              System.err.println("  Root message: " + cause.getCause().getMessage());
            }
          }

          // Try to understand if this is a Splunk SPL error
          if (sqlEx.getMessage() != null && sqlEx.getMessage().contains("400")) {
            System.err.println("  This appears to be an HTTP 400 error from Splunk REST API");
            System.err.println("  Likely cause: The CIM model '" + modelName + "' is not installed or configured in Splunk");
          }

          throw sqlEx; // Re-throw to be caught by outer catch
        }

        // Get sample fields if data exists
        String sampleFields = "";
        if (rowCount > 0) {
          String sampleSql = String.format("SELECT * FROM \"splunk\".\"%s\" LIMIT 1", modelName);
          try (ResultSet rs = stmt.executeQuery(sampleSql)) {
            if (rs.next()) {
              int columnCount = rs.getMetaData().getColumnCount();
              StringBuilder fields = new StringBuilder();
              int fieldsShown = 0;
              for (int i = 1; i <= columnCount && fieldsShown < 5; i++) {
                String columnName = rs.getMetaData().getColumnName(i);
                if (!columnName.startsWith("_") && !columnName.equals("host")
                    && !columnName.equals("source") && !columnName.equals("sourcetype")) {
                  if (fields.length() > 0) fields.append(", ");
                  fields.append(columnName);
                  fieldsShown++;
                }
              }
              sampleFields = fields.toString();
            }
          }
        }

        // Format output
        System.out.printf("%-30s| %9d | %s%n", modelName, rowCount, sampleFields);
      }

    } catch (SQLException e) {
      System.err.println("\nConnection or high-level error for model '" + modelName + "':");
      System.err.println("  Error type: " + e.getClass().getName());
      System.err.println("  Message: " + e.getMessage());
      System.err.println("  SQL State: " + e.getSQLState());
      System.err.println("  Error Code: " + e.getErrorCode());

      // Summary for table
      System.err.printf("%-30s| ERROR: %s%n", modelName, e.getMessage());
    }
  }
}
