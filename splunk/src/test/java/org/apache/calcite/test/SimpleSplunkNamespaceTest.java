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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Simple test to check dynamic data model discovery through JDBC.
 * Run with: -Dcalcite.test.splunk=true
 */
@Tag("integration")
class SimpleSplunkNamespaceTest {

  @BeforeAll
  static void setUp() {
    // Ensure driver is loaded
    try {
      Class.forName("org.apache.calcite.adapter.splunk.SplunkDriver");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Splunk driver not found", e);
    }
  }


  @Test void testDataModelAccessibility() throws Exception {
    Properties props = new Properties();
    props.setProperty("url", "https://kentest.xyz:8089");
    props.setProperty("user", "admin");
    props.setProperty("password", "admin123");
    props.setProperty("disableSslValidation", "true");

    // Test 1: Connect with dynamic discovery (all models)
    System.out.println("\n=== Test 1: Connect with dynamic discovery ===");
    props.setProperty("app", "Splunk_SA_CIM");
    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props)) {
      System.out.println("Connected successfully");

      // Try to see what schemas are available
      try (ResultSet rs = conn.getMetaData().getSchemas()) {
        System.out.println("Available schemas:");
        while (rs.next()) {
          System.out.println("- " + rs.getString("TABLE_SCHEM"));
        }
      }
    }

    // Test 2: Connect with filtered discovery
    System.out.println("\n=== Test 2: Connect with authentication filter ===");
    props.setProperty("datamodelFilter", "authentication");

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props)) {
      System.out.println("Connected with authentication filter");

      // List tables
      try (ResultSet rs = conn.getMetaData().getTables(null, null, "%", null)) {
        System.out.println("Available tables:");
        while (rs.next()) {
          System.out.println("- Schema: " + rs.getString("TABLE_SCHEM") +
                           ", Table: " + rs.getString("TABLE_NAME"));
        }
      }
    }

    // Test 3: Try to query authentication table
    System.out.println("\n=== Test 3: Query authentication table ===");
    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props);
         Statement stmt = conn.createStatement()) {

      try {
        // Simple count query
        String query = "SELECT COUNT(*) FROM authentication";
        System.out.println("Executing: " + query);

        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            System.out.println("Count: " + rs.getInt(1));
          }
        }
      } catch (Exception e) {
        System.out.println("Query failed: " + e.getMessage());

        // Try with schema qualifier
        String query2 = "SELECT COUNT(*) FROM splunk.authentication";
        System.out.println("\nTrying with schema: " + query2);

        try (ResultSet rs = stmt.executeQuery(query2)) {
          if (rs.next()) {
            System.out.println("Count: " + rs.getInt(1));
          }
        }
      }
    }
  }

  @Test void testNamespaceVisibility() throws Exception {
    System.out.println("\n=== Testing Namespace Visibility ===");

    Properties props = new Properties();
    props.setProperty("url", "https://kentest.xyz:8089");
    props.setProperty("user", "admin");
    props.setProperty("password", "admin123");
    props.setProperty("disableSslValidation", "true");

    // Connect with multiple models via filter
    props.setProperty("app", "Splunk_SA_CIM");
    props.setProperty("datamodelFilter", "/^(authentication|web|network_traffic)$/");

    try (Connection conn = DriverManager.getConnection("jdbc:splunk:", props)) {
      System.out.println("Connected with filtered data models");

      // Check what we can see
      System.out.println("\nChecking table accessibility:");
      String[] tables = {"authentication", "web", "network_traffic"};

      for (String table : tables) {
        // Try to get table metadata
        try (ResultSet rs = conn.getMetaData().getTables(null, null, table, null)) {
          if (rs.next()) {
            System.out.println("✓ Found table: " + table + " in schema: " +
                             rs.getString("TABLE_SCHEM"));
          } else {
            System.out.println("✗ Table not found: " + table);
          }
        }
      }
    }
  }
}
