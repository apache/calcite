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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Test to inspect sample data for all 24 CIM models showing complete field details.
 */
@Tag("integration")
class SampleDataInspector {

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


  private void inspectModelSampleData(String modelName) {
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.put("user", SPLUNK_USER);
    info.put("password", SPLUNK_PASSWORD);
    if (DISABLE_SSL_VALIDATION) {
      info.put("disableSslValidation", "true");
    }
    info.put("cimModel", modelName);

    System.out.println("================================================================================");
    System.out.println("MODEL: " + modelName.toUpperCase());
    System.out.println("================================================================================");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      // Query all fields, limit to 10 results
      String sql = String.format("SELECT * FROM \"splunk\".\"%s\" LIMIT 10", modelName);
      System.out.println("SQL: " + sql);
      System.out.println();

      try (ResultSet rs = stmt.executeQuery(sql)) {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Print column headers
        System.out.print("COLUMNS: ");
        for (int i = 1; i <= columnCount; i++) {
          System.out.print(metaData.getColumnName(i));
          if (i < columnCount) System.out.print(" | ");
        }
        System.out.println();
        System.out.println("------------------------------------------------------------------------");

        int rowCount = 0;
        while (rs.next() && rowCount < 10) {
          System.out.printf("ROW %d:\n", rowCount + 1);
          for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            String value = rs.getString(i);

            // Show full _extra field to see complete messages, truncate others
            if (value != null && value.length() > 500 && !columnName.equals("_extra")) {
              value = value.substring(0, 497) + "...";
            }

            System.out.printf("  %-25s: %s\n", columnName, value);
          }
          System.out.println();
          rowCount++;
        }

        if (rowCount == 0) {
          System.out.println("NO DATA FOUND");
        } else {
          System.out.printf("TOTAL ROWS SHOWN: %d\n", rowCount);
        }

      } catch (SQLException sqlEx) {
        System.err.println("ERROR executing query:");
        System.err.println("  Message: " + sqlEx.getMessage());
        System.err.println("  SQL State: " + sqlEx.getSQLState());
        System.err.println("  Error Code: " + sqlEx.getErrorCode());
      }

    } catch (SQLException e) {
      System.err.println("ERROR connecting to model '" + modelName + "':");
      System.err.println("  Message: " + e.getMessage());
      System.err.println("  SQL State: " + e.getSQLState());
      System.err.println("  Error Code: " + e.getErrorCode());
    }

    System.out.println();
  }
}
