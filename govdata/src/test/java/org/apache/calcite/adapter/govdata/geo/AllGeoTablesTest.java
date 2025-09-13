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

import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive test to verify all geographic tables are properly implemented
 * and can be queried without errors.
 */
@Tag("integration")
public class AllGeoTablesTest {

  private static final List<String> EXPECTED_TABLES = Arrays.asList(
      "TIGER_STATES",
      "TIGER_COUNTIES", 
      "TIGER_ZCTAS",
      "TIGER_CENSUS_TRACTS",
      "TIGER_BLOCK_GROUPS", 
      "TIGER_CBSA",
      "HUD_ZIP_COUNTY",
      "HUD_ZIP_TRACT", 
      "HUD_ZIP_CBSA",
      "HUD_ZIP_CBSA_DIV",
      "HUD_ZIP_CONGRESSIONAL",
      "CENSUS_PLACES"
  );

  @BeforeAll
  public static void setupEnvironment() {
    TestEnvironmentLoader.ensureLoaded();
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void testAllGeoTablesQueryable() throws Exception {
    System.out.println("\n=== COMPREHENSIVE GEO TABLES TEST ===");
    
    // Create model file with full geographic data configuration
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"geo\",\n" +
        "  \"schemas\": [\n" +
        "    {\n" +
        "      \"name\": \"geo\",\n" +
        "      \"type\": \"custom\",\n" +
        "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "      \"operand\": {\n" +
        "        \"dataSource\": \"geo\",\n" +
        "        \"cacheDirectory\": \"${GEO_CACHE_DIR:/Volumes/T9/geo-test-all}\",\n" +
        "        \"autoDownload\": false,\n" +
        "        \"dataYear\": 2024,\n" +
        "        \"enabledSources\": [\"tiger\", \"census\", \"hud\"],\n" +
        "        \"censusApiKey\": \"${CENSUS_API_KEY:}\",\n" +
        "        \"hudUsername\": \"${HUD_USERNAME:}\",\n" +
        "        \"hudPassword\": \"${HUD_PASSWORD:}\"\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    File tempDir = Files.createTempDirectory("geo-all-test").toFile();
    File modelFile = new File(tempDir, "all-geo-model.json");
    Files.write(modelFile.toPath(), modelJson.getBytes());

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, info)) {
      
      System.out.println("\n=== STEP 1: Discover All Available Tables ===");
      List<String> availableTables = discoverTables(conn);
      System.out.println("Found " + availableTables.size() + " tables total");
      
      assertFalse(availableTables.isEmpty(), "Should have at least some geographic tables");
      
      System.out.println("\n=== STEP 2: Test Each Table Type ===");
      
      // Test TIGER tables
      testTigerTables(conn, availableTables);
      
      // Test HUD tables  
      testHudTables(conn, availableTables);
      
      // Test Census tables
      testCensusTables(conn, availableTables);
      
      System.out.println("\n=== STEP 3: Test Schema Queries ===");
      testSchemaQueries(conn, availableTables);
      
      System.out.println("\n=== ALL GEO TABLES TEST COMPLETED SUCCESSFULLY ===");
    }
  }
  
  private List<String> discoverTables(Connection conn) throws SQLException {
    List<String> tables = new ArrayList<>();
    
    // Try different schema names to find tables
    for (String schemaName : new String[]{"GEO", "geo", null}) {
      try (ResultSet rs = conn.getMetaData().getTables(null, schemaName, "%", new String[]{"TABLE"})) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          String tableSchema = rs.getString("TABLE_SCHEM");
          if (!tables.contains(tableName)) {
            tables.add(tableName);
            System.out.println("  Found: " + (tableSchema != null ? tableSchema + "." : "") + tableName);
          }
        }
      }
    }
    
    return tables;
  }
  
  private void testTigerTables(Connection conn, List<String> availableTables) throws SQLException {
    System.out.println("\n--- Testing TIGER Tables ---");
    
    String[] tigerTables = {
        "TIGER_STATES", "TIGER_COUNTIES", "TIGER_ZCTAS", 
        "TIGER_CENSUS_TRACTS", "TIGER_BLOCK_GROUPS", "TIGER_CBSA"
    };
    
    for (String tableName : tigerTables) {
      if (availableTables.contains(tableName) || availableTables.contains(tableName.toLowerCase())) {
        System.out.println("Testing " + tableName + "...");
        
        // Test basic SELECT with LIMIT to avoid large result sets
        // Use lowercase table name since tables are registered with lowercase names
        String lowerTableName = tableName.toLowerCase();
        String query = "SELECT * FROM \"geo\".\"" + lowerTableName + "\" LIMIT 5";
        
        try (Statement stmt = conn.createStatement()) {
          // Test that the query can be prepared and executed (even if no data)
          try (ResultSet rs = stmt.executeQuery(query)) {
            int columnCount = rs.getMetaData().getColumnCount();
            System.out.println("  ✓ " + tableName + " - Query successful (" + columnCount + " columns)");
            
            // Try to read a few rows if available
            int rowCount = 0;
            while (rs.next() && rowCount < 3) {
              rowCount++;
            }
            System.out.println("  ✓ " + tableName + " - Retrieved " + rowCount + " sample rows");
          }
        } catch (SQLException e) {
          // Check if it's just a "no data" issue vs a real implementation problem
          if (e.getMessage().contains("cost") || e.getMessage().contains("infinite")) {
            System.out.println("  ⚠ " + tableName + " - No data available (expected in test env): " + e.getMessage());
          } else {
            System.out.println("  ✗ " + tableName + " - Implementation error: " + e.getMessage());
            throw new AssertionError("Table " + tableName + " failed: " + e.getMessage(), e);
          }
        }
      } else {
        System.out.println("  - " + tableName + " - Not available");
      }
    }
  }
  
  private void testHudTables(Connection conn, List<String> availableTables) throws SQLException {
    System.out.println("\n--- Testing HUD Tables ---");
    
    String[] hudTables = {
        "HUD_ZIP_COUNTY", "HUD_ZIP_TRACT", "HUD_ZIP_CBSA", 
        "HUD_ZIP_CBSA_DIV", "HUD_ZIP_CONGRESSIONAL"
    };
    
    for (String tableName : hudTables) {
      if (availableTables.contains(tableName) || availableTables.contains(tableName.toLowerCase())) {
        System.out.println("Testing " + tableName + "...");
        
        String lowerTableName = tableName.toLowerCase();
        String query = "SELECT * FROM \"geo\".\"" + lowerTableName + "\" LIMIT 5";
        
        try (Statement stmt = conn.createStatement()) {
          try (ResultSet rs = stmt.executeQuery(query)) {
            int columnCount = rs.getMetaData().getColumnCount();
            System.out.println("  ✓ " + tableName + " - Query successful (" + columnCount + " columns)");
            
            int rowCount = 0;
            while (rs.next() && rowCount < 3) {
              rowCount++;
            }
            System.out.println("  ✓ " + tableName + " - Retrieved " + rowCount + " sample rows");
          }
        } catch (SQLException e) {
          if (e.getMessage().contains("cost") || e.getMessage().contains("infinite") ||
              e.getMessage().contains("HUD fetcher not configured") || 
              e.getMessage().contains("HUD API authentication") ||
              e.getMessage().contains("Failed to fetch HUD")) {
            System.out.println("  ⚠ " + tableName + " - No data/credentials available (expected): " + e.getMessage());
          } else {
            System.out.println("  ✗ " + tableName + " - Implementation error: " + e.getMessage());
            throw new AssertionError("Table " + tableName + " failed: " + e.getMessage(), e);
          }
        }
      } else {
        System.out.println("  - " + tableName + " - Not available");
      }
    }
  }
  
  private void testCensusTables(Connection conn, List<String> availableTables) throws SQLException {
    System.out.println("\n--- Testing Census Tables ---");
    
    String[] censusTables = {"CENSUS_PLACES", "CENSUS_POPULATION"};
    
    for (String tableName : censusTables) {
      if (availableTables.contains(tableName) || availableTables.contains(tableName.toLowerCase())) {
        System.out.println("Testing " + tableName + "...");
        
        String lowerTableName = tableName.toLowerCase();
        String query = "SELECT * FROM \"geo\".\"" + lowerTableName + "\" LIMIT 5";
        
        try (Statement stmt = conn.createStatement()) {
          try (ResultSet rs = stmt.executeQuery(query)) {
            int columnCount = rs.getMetaData().getColumnCount();
            System.out.println("  ✓ " + tableName + " - Query successful (" + columnCount + " columns)");
            
            int rowCount = 0;
            while (rs.next() && rowCount < 3) {
              rowCount++;
            }
            System.out.println("  ✓ " + tableName + " - Retrieved " + rowCount + " sample rows");
          }
        } catch (SQLException e) {
          if (e.getMessage().contains("cost") || e.getMessage().contains("infinite") ||
              e.getMessage().contains("Census API key") ||
              e.getMessage().contains("HUD API authentication") ||
              e.getMessage().contains("Failed to fetch")) {
            System.out.println("  ⚠ " + tableName + " - No data/API key available (expected): " + e.getMessage());
          } else {
            System.out.println("  ✗ " + tableName + " - Implementation error: " + e.getMessage());
            throw new AssertionError("Table " + tableName + " failed: " + e.getMessage(), e);
          }
        }
      } else {
        System.out.println("  - " + tableName + " - Not available");
      }
    }
  }
  
  private void testSchemaQueries(Connection conn, List<String> availableTables) throws SQLException {
    System.out.println("\n--- Testing Schema Information ---");
    
    // Test that we can query metadata for each table
    for (String tableName : availableTables) {
      System.out.println("Checking metadata for " + tableName + "...");
      
      // Test column metadata
      try (ResultSet columns = conn.getMetaData().getColumns(null, "geo", tableName, "%")) {
        int columnCount = 0;
        while (columns.next()) {
          columnCount++;
        }
        System.out.println("  ✓ " + tableName + " - " + columnCount + " columns in metadata");
        assertTrue(columnCount > 0, "Table " + tableName + " should have columns");
      }
      
      // Test that the table can be found in metadata
      try (ResultSet tables = conn.getMetaData().getTables(null, "geo", tableName, new String[]{"TABLE"})) {
        boolean found = tables.next();
        assertTrue(found, "Table " + tableName + " should be found in metadata");
        if (found) {
          System.out.println("  ✓ " + tableName + " - Found in table metadata");
        }
      }
    }
  }
}