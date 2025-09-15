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
package org.apache.calcite.adapter.govdata.geo;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verification test to confirm GEO tables contain queryable data.
 */
@Tag("integration")
public class GeoDataVerificationTest {

  @BeforeAll
  public static void setUp() {
    // Load environment variables from .env.test if available
    try {
      java.nio.file.Path envFile = java.nio.file.Paths.get(".env.test");
      if (java.nio.file.Files.exists(envFile)) {
        java.nio.file.Files.lines(envFile)
            .filter(line -> !line.startsWith("#") && line.contains("="))
            .forEach(line -> {
              String[] parts = line.split("=", 2);
              if (parts.length == 2) {
                System.setProperty(parts[0].trim(), parts[1].trim().replaceAll("^\"|\"$", ""));
              }
            });
        System.out.println("Loaded " + 
            java.nio.file.Files.lines(envFile)
                .filter(line -> !line.startsWith("#") && line.contains("="))
                .count() + " environment variables from .env.test");
      }
    } catch (Exception e) {
      System.err.println("Could not load .env.test: " + e.getMessage());
    }
  }

  @Test
  public void testGeoTablesContainData() throws Exception {
    URL modelUrl = GeoDataVerificationTest.class.getClassLoader()
        .getResource("govdata-geo-test-model.json");
    if (modelUrl == null) {
      throw new RuntimeException("Could not find govdata-geo-test-model.json in test resources");
    }
    String modelPath = modelUrl.getPath();
    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {
      System.out.println("\n========================================");
      System.out.println("VERIFYING GEO TABLES CONTAIN DATA");
      System.out.println("========================================\n");

      String[] tables = {
          "tiger_states",
          "tiger_counties",
          "tiger_places",
          "tiger_zctas"
          // Note: cbsa, census_tracts, and block_groups data not yet converted to parquet
      };

      for (String table : tables) {
        System.out.println("Testing table: geo." + table);
        System.out.println("-----------------------------------------");

        // Count records
        String countQuery = "SELECT COUNT(*) as total FROM geo." + table;
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(countQuery)) {
          assertTrue(rs.next(), "Count query should return a result");
          int count = rs.getInt("total");
          System.out.println("  Record count: " + count);
          assertTrue(count > 0, "Table " + table + " should contain data");
        }

        // Get sample data
        String sampleQuery = "SELECT * FROM geo." + table + " LIMIT 3";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sampleQuery)) {
          
          int rowCount = 0;
          while (rs.next()) {
            rowCount++;
            if (rowCount == 1) {
              // Print first row details
              System.out.println("  Sample row 1:");
              for (int i = 1; i <= Math.min(rs.getMetaData().getColumnCount(), 5); i++) {
                System.out.println("    " + rs.getMetaData().getColumnName(i) + ": " + 
                    truncate(rs.getString(i), 50));
              }
            }
          }
          assertTrue(rowCount > 0, "Should be able to retrieve sample data from " + table);
          System.out.println("  ✓ Table is queryable with " + rowCount + " sample rows\n");
        }
      }

      // Test complex queries
      System.out.println("========================================");
      System.out.println("TESTING COMPLEX QUERIES");
      System.out.println("========================================\n");

      // Query states
      System.out.println("Query: States with specific names");
      String stateQuery = "SELECT state_name, state_code, state_fips " +
                         "FROM geo.tiger_states " +
                         "WHERE state_name IN ('California', 'New York', 'Texas') " +
                         "ORDER BY state_name";
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(stateQuery)) {
        int stateCount = 0;
        while (rs.next()) {
          stateCount++;
          System.out.printf("  %s (%s) - FIPS: %s\n",
              rs.getString("state_name"),
              rs.getString("state_code"),
              rs.getString("state_fips"));
        }
        assertTrue(stateCount > 0, "Should find states matching the criteria");
        System.out.println("  ✓ Found " + stateCount + " states\n");
      }

      // Test filtering by state
      System.out.println("Query: Counties in California (FIPS 06)");
      String countyQuery = "SELECT county_name, county_fips " +
                          "FROM geo.tiger_counties " +
                          "WHERE state_fips = '06' " +
                          "LIMIT 5";
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(countyQuery)) {
        int countyCount = 0;
        while (rs.next()) {
          countyCount++;
          System.out.printf("  %s - FIPS: %s\n",
              rs.getString("county_name"),
              rs.getString("county_fips"));
        }
        assertTrue(countyCount > 0, "Should find counties in California");
        System.out.println("  ✓ Found " + countyCount + " California counties\n");
      }

      // Test partition columns
      System.out.println("Query: Verify partition columns work");
      String partitionQuery = "SELECT DISTINCT source, type, year " +
                             "FROM geo.tiger_states " +
                             "ORDER BY year";
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(partitionQuery)) {
        while (rs.next()) {
          System.out.printf("  Partition: source=%s, type=%s, year=%d\n",
              rs.getString("source"),
              rs.getString("type"),
              rs.getInt("year"));
        }
        System.out.println("  ✓ Partition columns are accessible\n");
      }

      System.out.println("========================================");
      System.out.println("ALL DATA VERIFICATION TESTS PASSED!");
      System.out.println("========================================\n");
    }
  }

  private static String truncate(String str, int maxLen) {
    if (str == null) return "null";
    if (str.length() <= maxLen) return str;
    return str.substring(0, maxLen - 3) + "...";
  }
}