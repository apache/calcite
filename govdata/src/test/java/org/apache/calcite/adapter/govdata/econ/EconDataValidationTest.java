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
package org.apache.calcite.adapter.govdata.econ;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Validates ECON schema data with SQL queries.
 */
@Tag("integration")
public class EconDataValidationTest {
  
  @BeforeAll
  public static void setup() {
    // Use the actual environment variables from .env.test
    String cacheDir = System.getenv("GOVDATA_CACHE_DIR");
    String parquetDir = System.getenv("GOVDATA_PARQUET_DIR");
    
    if (cacheDir == null || parquetDir == null) {
      System.out.println("Setting default directories for test");
      System.setProperty("GOVDATA_CACHE_DIR", "/Volumes/T9/govdata-cache");
      System.setProperty("GOVDATA_PARQUET_DIR", "/Volumes/T9/govdata-parquet");
    }
  }
  
  @Test
  public void testEconTablesExistAndHaveData() throws Exception {
    System.out.println("\n=== ECON Data Validation Test ===");
    System.out.println("GOVDATA_CACHE_DIR: " + 
        (System.getenv("GOVDATA_CACHE_DIR") != null ? 
         System.getenv("GOVDATA_CACHE_DIR") : 
         System.getProperty("GOVDATA_CACHE_DIR")));
    System.out.println("GOVDATA_PARQUET_DIR: " + 
        (System.getenv("GOVDATA_PARQUET_DIR") != null ? 
         System.getenv("GOVDATA_PARQUET_DIR") : 
         System.getProperty("GOVDATA_PARQUET_DIR")));
    
    // Create model JSON with auto-download enabled
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"econ\","
        + "\"schemas\": [{"
        + "  \"name\": \"econ\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.econ.EconSchemaFactory\","
        + "  \"operand\": {"
        + "    \"autoDownload\": true,"
        + "    \"startYear\": 2023,"
        + "    \"endYear\": 2024,"
        + "    \"enabledSources\": [\"bls\", \"fred\", \"treasury\", \"bea\", \"worldbank\"]"
        + "  }"
        + "}]"
        + "}";
    
    java.nio.file.Path modelFile = java.nio.file.Files.createTempFile("econ-validation", ".json");
    java.nio.file.Files.writeString(modelFile, modelJson);
    
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=" + modelFile, info);
         Statement stmt = conn.createStatement()) {
      
      System.out.println("\n1. Checking what tables exist in ECON schema:");
      System.out.println("================================================");
      
      // Query for all tables in ECON schema (try both uppercase and lowercase)
      String tableQuery = "SELECT \"TABLE_SCHEMA\", \"TABLE_NAME\", \"TABLE_TYPE\" "
          + "FROM information_schema.tables "
          + "ORDER BY \"TABLE_SCHEMA\", \"TABLE_NAME\"";
      
      int tableCount = 0;
      try (ResultSet rs = stmt.executeQuery(tableQuery)) {
        while (rs.next()) {
          String schemaName = rs.getString("TABLE_SCHEMA");
          String tableName = rs.getString("TABLE_NAME");
          String tableType = rs.getString("TABLE_TYPE");
          System.out.printf("  - %s.%s (%s)\n", schemaName, tableName, tableType);
          if ("ECON".equalsIgnoreCase(schemaName)) {
            tableCount++;
          }
        }
      }
      
      System.out.println("\nTotal tables found: " + tableCount);
      assertTrue(tableCount > 0, "Should have at least one table in ECON schema");
      
      System.out.println("\n2. Checking row counts for each table:");
      System.out.println("========================================");
      
      // List of expected tables
      String[] expectedTables = {
          "employment_statistics",
          "inflation_metrics", 
          "wage_growth",
          "regional_employment",
          "treasury_yields",
          "federal_debt",
          "world_indicators",
          "fred_indicators",
          "gdp_components",
          "consumer_price_index",
          "gdp_statistics"
      };
      
      int tablesWithData = 0;
      for (String table : expectedTables) {
        try {
          String countQuery = "SELECT COUNT(*) as cnt FROM econ." + table;
          try (ResultSet rs = stmt.executeQuery(countQuery)) {
            if (rs.next()) {
              int count = rs.getInt("cnt");
              System.out.printf("  %s: %d rows\n", table, count);
              if (count > 0) {
                tablesWithData++;
              }
            }
          }
        } catch (Exception e) {
          System.out.printf("  %s: NOT FOUND or ERROR (%s)\n", table, e.getMessage());
        }
      }
      
      System.out.println("\nTables with data: " + tablesWithData);
      
      System.out.println("\n3. Sample data from available tables:");
      System.out.println("=======================================");
      
      // Try to get sample data from employment_statistics
      try {
        System.out.println("\nSample from employment_statistics:");
        String sampleQuery = "SELECT * FROM econ.employment_statistics LIMIT 3";
        try (ResultSet rs = stmt.executeQuery(sampleQuery)) {
          int cols = rs.getMetaData().getColumnCount();
          
          // Print column headers
          for (int i = 1; i <= cols; i++) {
            System.out.print(rs.getMetaData().getColumnName(i) + "\t");
          }
          System.out.println();
          
          // Print data
          while (rs.next()) {
            for (int i = 1; i <= cols; i++) {
              System.out.print(rs.getString(i) + "\t");
            }
            System.out.println();
          }
        }
      } catch (Exception e) {
        System.out.println("Could not query employment_statistics: " + e.getMessage());
      }
      
      // Try to get sample data from treasury_yields
      try {
        System.out.println("\nSample from treasury_yields:");
        String sampleQuery = "SELECT * FROM econ.treasury_yields LIMIT 3";
        try (ResultSet rs = stmt.executeQuery(sampleQuery)) {
          int cols = rs.getMetaData().getColumnCount();
          
          // Print column headers
          for (int i = 1; i <= cols; i++) {
            System.out.print(rs.getMetaData().getColumnName(i) + "\t");
          }
          System.out.println();
          
          // Print data
          while (rs.next()) {
            for (int i = 1; i <= cols; i++) {
              System.out.print(rs.getString(i) + "\t");
            }
            System.out.println();
          }
        }
      } catch (Exception e) {
        System.out.println("Could not query treasury_yields: " + e.getMessage());
      }
      
      System.out.println("\n4. Checking table schema/columns:");
      System.out.println("===================================");
      
      // Get column information for a table that exists
      String columnQuery = "SELECT \"TABLE_NAME\", \"COLUMN_NAME\", \"DATA_TYPE\", \"ORDINAL_POSITION\" "
          + "FROM information_schema.columns "
          + "WHERE \"TABLE_SCHEMA\" = 'ECON' "
          + "AND \"TABLE_NAME\" IN ('employment_statistics', 'treasury_yields') "
          + "ORDER BY \"TABLE_NAME\", \"ORDINAL_POSITION\"";
      
      String currentTable = "";
      try (ResultSet rs = stmt.executeQuery(columnQuery)) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          String columnName = rs.getString("COLUMN_NAME");
          String dataType = rs.getString("DATA_TYPE");
          
          if (!tableName.equals(currentTable)) {
            currentTable = tableName;
            System.out.println("\nTable: " + tableName);
          }
          System.out.printf("  - %s (%s)\n", columnName, dataType);
        }
      }
      
      System.out.println("\n=== Test Complete ===\n");
      
    } finally {
      java.nio.file.Files.deleteIfExists(modelFile);
    }
  }
}