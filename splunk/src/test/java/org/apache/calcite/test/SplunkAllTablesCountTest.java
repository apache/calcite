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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Get row counts for all tables in Splunk.
 */
@Tag("integration")
class SplunkAllTablesCountTest {
  private static String SPLUNK_URL = "https://kentest.xyz:8089";
  private static String SPLUNK_USER = "admin";
  private static String SPLUNK_PASSWORD = "admin123";
  private static boolean DISABLE_SSL_VALIDATION = true;

  @BeforeAll
  static void loadConnectionProperties() {
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

  private void loadDriverClass() {
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("driver not found", e);
    }
  }

  private Connection createConnection() throws SQLException {
    loadDriverClass();
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.put("user", SPLUNK_USER);
    info.put("password", SPLUNK_PASSWORD);
    if (DISABLE_SSL_VALIDATION) {
      info.put("disableSslValidation", "true");
    }
    // Test with app context to show improved error messages
    info.put("app", "Splunk_SA_CIM");
    info.put("datamodelCacheTtl", "0");
    return DriverManager.getConnection("jdbc:splunk:", info);
  }

  @Test void testCountAllTables() throws SQLException {
    try (Connection connection = createConnection()) {
      DatabaseMetaData metaData = connection.getMetaData();

      // Get all tables
      ResultSet tables = metaData.getTables(null, "splunk", "%", null);
      List<String> tableNames = new ArrayList<>();
      while (tables.next()) {
        tableNames.add(tables.getString("TABLE_NAME"));
      }

      System.out.println("Counting rows in " + tableNames.size() + " tables:");
      System.out.println("=".repeat(60));

      int totalRows = 0;
      int tablesWithData = 0;

      try (Statement stmt = connection.createStatement()) {
        for (String tableName : tableNames) {
          try {
            String sql = "SELECT COUNT(*) as row_count FROM \"splunk\".\"" + tableName + "\"";
            ResultSet rs = stmt.executeQuery(sql);

            if (rs.next()) {
              int rowCount = rs.getInt("row_count");
              totalRows += rowCount;

              if (rowCount > 0) {
                tablesWithData++;
                System.out.printf("%-30s: %,8d rows\n", tableName, rowCount);
              } else {
                System.out.printf("%-30s: %8s\n", tableName, "empty");
              }
            }
          } catch (SQLException e) {
            System.out.printf("%-30s: %8s\n", tableName, "ERROR");
            // Print the full error message to see our improved error handling
            System.out.println("  → " + e.getMessage());
          }
        }
      }

      System.out.println("=".repeat(60));
      System.out.printf("Total tables: %d\n", tableNames.size());
      System.out.printf("Tables with data: %d\n", tablesWithData);
      System.out.printf("Empty tables: %d\n", tableNames.size() - tablesWithData);
      System.out.printf("Total rows across all tables: %,d\n", totalRows);

      if (tablesWithData > 0) {
        System.out.println("\nTables with data:");
        try (Statement stmt = connection.createStatement()) {
          for (String tableName : tableNames) {
            try {
              String sql = "SELECT COUNT(*) as row_count FROM \"splunk\".\"" + tableName + "\"";
              ResultSet rs = stmt.executeQuery(sql);

              if (rs.next()) {
                int rowCount = rs.getInt("row_count");
                if (rowCount > 0) {
                  System.out.printf("  %-25s: %,8d rows\n", tableName, rowCount);
                }
              }
            } catch (SQLException e) {
              // Skip errors
            }
          }
        }
      }
    }
  }
}
