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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Live server test for Splunk adapter using kentest.xyz configuration.
 */
class SplunkLiveServerTest {
  // Live server configuration from local-properties.settings
  public static final String SPLUNK_URL = "https://kentest.xyz:8089";
  public static final String SPLUNK_USER = "admin";
  public static final String SPLUNK_PASSWORD = "admin123";

  private void loadDriverClass() {
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("driver not found", e);
    }
  }

  @Test void testBasicConnection() throws SQLException {
    loadDriverClass();
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.put("user", SPLUNK_USER);
    info.put("password", SPLUNK_PASSWORD);
    info.put("disableSslValidation", "true");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info)) {
      assertThat(connection.isClosed(), is(false));
      System.out.println("Successfully connected to Splunk at " + SPLUNK_URL);
    }
  }

  // Disabled for now - metadata schemas not available via JDBC driver
  // @Test void testMetadataSchemas() throws SQLException {
    loadDriverClass();
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.put("user", SPLUNK_USER);
    info.put("password", SPLUNK_PASSWORD);
    info.put("disableSslValidation", "true");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      // Test pg_catalog schema
      System.out.println("\n=== Testing pg_catalog.pg_tables ===");
      try (ResultSet rs =
          stmt.executeQuery("SELECT schemaname, tablename FROM pg_catalog.pg_tables WHERE schemaname = 'splunk' LIMIT 5")) {
        int count = 0;
        while (rs.next()) {
          System.out.printf("Schema: %s, Table: %s%n",
              rs.getString("schemaname"),
              rs.getString("tablename"));
          count++;
        }
        System.out.println("Found " + count + " tables in pg_catalog.pg_tables");
      }

      // Test information_schema
      System.out.println("\n=== Testing information_schema.tables ===");
      try (ResultSet rs =
          stmt.executeQuery("SELECT table_schema, table_name, table_type FROM information_schema.tables " +
          "WHERE table_schema = 'splunk' LIMIT 5")) {
        int count = 0;
        while (rs.next()) {
          System.out.printf("Schema: %s, Table: %s, Type: %s%n",
              rs.getString("table_schema"),
              rs.getString("table_name"),
              rs.getString("table_type"));
          count++;
        }
        System.out.println("Found " + count + " tables in information_schema.tables");
      }

      // Test splunk indexes
      System.out.println("\n=== Testing pg_catalog.splunk_indexes ===");
      try (ResultSet rs =
          stmt.executeQuery("SELECT index_name, is_internal FROM pg_catalog.splunk_indexes LIMIT 10")) {
        int count = 0;
        while (rs.next()) {
          System.out.printf("Index: %s, Internal: %s%n",
              rs.getString("index_name"),
              rs.getString("is_internal"));
          count++;
        }
        System.out.println("Found " + count + " indexes");
      }
    }
  }

  @Test void testDataQuery() throws SQLException {
    loadDriverClass();
    Properties info = new Properties();
    info.setProperty("url", SPLUNK_URL);
    info.put("user", SPLUNK_USER);
    info.put("password", SPLUNK_PASSWORD);
    info.put("disableSslValidation", "true");

    try (Connection connection = DriverManager.getConnection("jdbc:splunk:", info);
         Statement stmt = connection.createStatement()) {

      // Test simple query - using the 'web' CIM model table
      System.out.println("\n=== Testing data query ===");
      try (ResultSet rs =
          stmt.executeQuery("SELECT \"action\", \"status\", \"user\" FROM \"splunk\".\"web\" LIMIT 5")) {
        int count = 0;
        while (rs.next()) {
          System.out.printf("Action: %s, Status: %s, User: %s%n",
              rs.getString("action"),
              rs.getString("status"),
              rs.getString("user"));
          count++;
        }
        System.out.println("Found " + count + " events");
      }
    }
  }
}
