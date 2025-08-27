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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Base class for Splunk adapter tests that require connection to a Splunk instance.
 * Loads connection properties from local-properties.settings file.
 */
public abstract class SplunkTestBase {
  protected static String SPLUNK_URL = null;
  protected static String SPLUNK_USER = null;
  protected static String SPLUNK_PASSWORD = null;
  protected static boolean DISABLE_SSL_VALIDATION = false;
  protected static boolean splunkAvailable = false;

  static {
    // Register the Splunk driver
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load Splunk driver", e);
    }
  }

  @BeforeAll
  public static void loadConnectionProperties() {
    // Try to load from local-properties.settings
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

        splunkAvailable = SPLUNK_URL != null && SPLUNK_USER != null && SPLUNK_PASSWORD != null;

        if (splunkAvailable) {
          System.out.println("Loaded Splunk connection from " + propsFile.getPath());
        }
      } catch (IOException e) {
        System.err.println("Failed to load local-properties.settings: " + e.getMessage());
      }
    }

    // Fall back to environment variables if not loaded from file
    if (!splunkAvailable) {
      String envUrl = System.getenv("SPLUNK_URL");
      String envUser = System.getenv("SPLUNK_USER");
      String envPassword = System.getenv("SPLUNK_PASSWORD");

      if (envUrl != null && envUser != null && envPassword != null) {
        SPLUNK_URL = envUrl;
        SPLUNK_USER = envUser;
        SPLUNK_PASSWORD = envPassword;
        DISABLE_SSL_VALIDATION = "true".equals(System.getenv("SPLUNK_SSL_INSECURE"));
        splunkAvailable = true;
        System.out.println("Loaded Splunk connection from environment variables");
      }
    }

    if (!splunkAvailable) {
      System.out.println("Splunk not configured. Create local-properties.settings with " +
          "splunk.url, splunk.username, and splunk.password properties.");
    }
  }

  protected Connection getConnection() throws SQLException {
    if (!splunkAvailable) {
      throw new IllegalStateException("Splunk connection not configured");
    }

    Properties props = new Properties();
    props.setProperty("url", SPLUNK_URL);
    props.setProperty("user", SPLUNK_USER);
    props.setProperty("password", SPLUNK_PASSWORD);
    if (DISABLE_SSL_VALIDATION) {
      props.setProperty("disableSslValidation", "true");
    }
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    return DriverManager.getConnection("jdbc:splunk:", props);
  }
}
