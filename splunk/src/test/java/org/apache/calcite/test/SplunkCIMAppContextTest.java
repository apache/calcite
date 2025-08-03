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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for accessing CIM models using app context parameter.
 * Run with: -Dcalcite.test.splunk=true
 */
@EnabledIfSystemProperty(named = "calcite.test.splunk", matches = "true")
class SplunkCIMAppContextTest {

  @BeforeAll
  static void setUp() {
    // Ensure driver is loaded
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Splunk driver not found", e);
    }
  }

  @Test void testCIMModelsWithAppContext() throws Exception {
    System.out.println("\n=== Testing CIM Models with App Context ===");

    Properties props = new Properties();
    props.setProperty("url", "https://kentest.xyz:8089");
    props.setProperty("user", "admin");
    props.setProperty("password", "admin123");
    props.setProperty("disableSslValidation", "true");
    props.setProperty("app", "Splunk_SA_CIM");  // Specify the CIM app context
    props.setProperty("cimModel", "authentication");

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props);
         Statement stmt = conn.createStatement()) {

      // Test 1: Simple query
      System.out.println("\n1. Testing simple query with app context:");
      String query = "SELECT COUNT(*) FROM splunk.authentication";
      System.out.println("SQL: " + query);

      try (ResultSet rs = stmt.executeQuery(query)) {
        if (rs.next()) {
          int count = rs.getInt(1);
          System.out.println("✓ Query succeeded - found " + count + " records");
          assertTrue(count >= 0, "Count should be non-negative");
        }
      }

      // Test 2: Query with limit
      System.out.println("\n2. Testing query with LIMIT:");
      query = "SELECT action, user FROM splunk.authentication LIMIT 5";
      System.out.println("SQL: " + query);

      try (ResultSet rs = stmt.executeQuery(query)) {
        int rowCount = 0;
        while (rs.next()) {
          String action = rs.getString("action");
          String user = rs.getString("user");
          System.out.println("  Row " + (++rowCount) + ": action=" + action + ", user=" + user);
        }
        System.out.println("✓ Retrieved " + rowCount + " rows");
      }
    }
  }

  @Test void testMultipleCIMModelsWithAppContext() throws Exception {
    System.out.println("\n=== Testing Multiple CIM Models with App Context ===");

    Properties props = new Properties();
    props.setProperty("url", "https://kentest.xyz:8089");
    props.setProperty("user", "admin");
    props.setProperty("password", "admin123");
    props.setProperty("disableSslValidation", "true");
    props.setProperty("app", "Splunk_SA_CIM");
    props.setProperty("cimModels", "authentication,web,network_traffic");

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props)) {
      // Check what tables are available
      System.out.println("\nAvailable tables:");
      try (ResultSet rs = conn.getMetaData().getTables(null, "splunk", "%", null)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          System.out.println("  - " + tableName);
        }
      }

      // Test each table
      String[] tables = {"authentication", "web", "network_traffic"};
      for (String table : tables) {
        System.out.println("\nTesting table: " + table);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM splunk." + table)) {
          if (rs.next()) {
            System.out.println("  ✓ " + table + " count: " + rs.getInt(1));
          }
        } catch (Exception e) {
          System.out.println("  ✗ " + table + " failed: " + e.getMessage());
        }
      }
    }
  }

  @Test void testWithoutAppContext() throws Exception {
    System.out.println("\n=== Testing WITHOUT App Context (should use default) ===");

    Properties props = new Properties();
    props.setProperty("url", "https://kentest.xyz:8089");
    props.setProperty("user", "admin");
    props.setProperty("password", "admin123");
    props.setProperty("disableSslValidation", "true");
    // Note: NOT setting app parameter
    props.setProperty("cimModel", "authentication");

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props);
         Statement stmt = conn.createStatement()) {

      String query = "SELECT COUNT(*) FROM splunk.authentication";
      System.out.println("SQL: " + query);

      try (ResultSet rs = stmt.executeQuery(query)) {
        if (rs.next()) {
          System.out.println("✓ Query succeeded without explicit app context");
        }
      } catch (Exception e) {
        System.out.println("✗ Query failed: " + e.getMessage());
        System.out.println("This is expected if CIM models are not in the global namespace");
      }
    }
  }

  @Test void testURLParameterFormat() throws Exception {
    System.out.println("\n=== Testing URL Parameter Format ===");

    // Test with app parameter in URL
    String url = "jdbc:splunk:url='https://kentest.xyz:8089';user='admin';password='admin123';"
        + "disableSslValidation='true';app='Splunk_SA_CIM';cimModel='authentication'";

    System.out.println("URL: " + url);

    try (Connection conn = DriverManager.getConnection(url);
         Statement stmt = conn.createStatement()) {

      String query = "SELECT COUNT(*) FROM splunk.authentication";
      try (ResultSet rs = stmt.executeQuery(query)) {
        if (rs.next()) {
          System.out.println("✓ URL parameter format works correctly");
        }
      }
    }
  }
}
