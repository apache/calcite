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
import java.util.Properties;

/**
 * Test to show the full warning message from empty CIM models.
 */
@EnabledIf("splunkTestEnabled")
class ShowWarningMessage {

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

  private static boolean splunkTestEnabled() {
    return System.getProperty("CALCITE_TEST_SPLUNK", "false").equals("true") ||
           System.getenv("CALCITE_TEST_SPLUNK") != null;  }

  @Test void showFullWarningMessage() throws SQLException, ClassNotFoundException {
    Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");

    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.put("user", SPLUNK_USER);
    info.put("password", SPLUNK_PASSWORD);
    if (DISABLE_SSL_VALIDATION) {
      info.put("disableSslValidation", "true");
    }
    info.put("cimModel", "certificates"); // Use certificates as example of empty model

    System.out.println("Checking full warning message from certificates model...\n");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      String sql = "SELECT * FROM \"splunk\".\"certificates\" LIMIT 1";

      try (ResultSet rs = stmt.executeQuery(sql)) {
        if (rs.next()) {
          // Get all columns and find _extra
          for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
            String columnName = rs.getMetaData().getColumnName(i);
            if ("_extra".equals(columnName)) {
              String extraField = rs.getString(i);
              System.out.println("Full _extra field content:");
              System.out.println("=" + "=".repeat(80));
              System.out.println(extraField);
              System.out.println("=" + "=".repeat(80));
              break;
            }
          }
        } else {
          System.out.println("No data returned");
        }
      }

    } catch (SQLException e) {
      System.err.println("Error: " + e.getMessage());
    }
  }
}
