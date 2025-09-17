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

import org.apache.calcite.adapter.govdata.TestEnvironmentLoader;
import org.apache.calcite.adapter.govdata.econ.BeaDataDownloader;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Simple test to verify all ECON tables are exposed and queryable.
 * Requires GOVDATA_CACHE_DIR and GOVDATA_PARQUET_DIR environment variables.
 */
@Tag("integration")
public class EconSchemaTableExposureTest {
  
  @BeforeAll
  static void setUp() {
    TestEnvironmentLoader.ensureLoaded();
  }
  
  @Test
  public void testAllTablesAreQueryable() throws Exception {
    // Create model JSON using GovDataSchemaFactory
    String modelJson = 
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"econ\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"econ\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "      \"operand\": {"
        + "        \"dataSource\": \"econ\","
        + "        \"executionEngine\": \"DUCKDB\""
        + "      }"
        + "    }"
        + "  ]"
        + "}";
    
    // Write model file
    Path modelFile = Files.createTempFile("model", ".json");
    Files.write(modelFile, modelJson.getBytes());
    
    // Connect and query
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    
    Set<String> expectedTables = new HashSet<>();
    expectedTables.add("employment_statistics");
    expectedTables.add("inflation_metrics");
    expectedTables.add("wage_growth");
    expectedTables.add("regional_employment");
    expectedTables.add("treasury_yields");
    expectedTables.add("federal_debt");
    expectedTables.add("world_indicators");
    expectedTables.add("fred_indicators");
    expectedTables.add("gdp_components");
    expectedTables.add("regional_income");
    expectedTables.add("state_gdp");
    expectedTables.add("trade_statistics");
    expectedTables.add("ita_data");
    expectedTables.add("industry_gdp");
    
    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=" + modelFile, props);
         Statement stmt = conn.createStatement()) {
      
      // Test each table by sampling data and checking for rows
      for (String table : expectedTables) {
        String query = "SELECT COUNT(*) as row_count FROM econ." + table;
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next(), "COUNT query should work for " + table);
          int rowCount = rs.getInt("row_count");
          System.out.println("Table " + table + " has " + rowCount + " rows");
          
          // Verify the table has actual data
          if (rowCount > 0) {
            // Sample a few rows to ensure data is accessible
            String sampleQuery = "SELECT * FROM econ." + table + " LIMIT 3";
            try (ResultSet sampleRs = stmt.executeQuery(sampleQuery)) {
              assertTrue(sampleRs.next(), "Table " + table + " should have accessible sample data");
              System.out.println("✅ Table " + table + " is queryable with " + rowCount + " rows");
            }
          } else {
            System.out.println("⚠️  Table " + table + " exists but has no data (" + rowCount + " rows)");
          }
        } catch (Exception e) {
          fail("Table " + table + " is not queryable: " + e.getMessage());
        }
      }
      
      System.out.println("✅ All " + expectedTables.size() + " ECON tables are exposed and testable!");
    }
  }
  
  @Test
  public void testStateGdpTableExists() throws Exception {
    // Create model JSON
    String modelJson = 
        "{"
        + "  \"version\": \"1.0\","
        + "  \"defaultSchema\": \"econ\","
        + "  \"schemas\": ["
        + "    {"
        + "      \"name\": \"econ\","
        + "      \"type\": \"custom\","
        + "      \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "      \"operand\": {"
        + "        \"dataSource\": \"econ\","
        + "        \"executionEngine\": \"DUCKDB\""
        + "      }"
        + "    }"
        + "  ]"
        + "}";
    
    Path modelFile = Files.createTempFile("model", ".json");
    Files.write(modelFile, modelJson.getBytes());
    
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=" + modelFile, props);
         Statement stmt = conn.createStatement()) {
      
      // Simple test - just try to query state_gdp table
      String query = "SELECT COUNT(*) as row_count FROM econ.state_gdp";
      
      try (ResultSet rs = stmt.executeQuery(query)) {
        assertTrue(rs.next(), "Query should return a result");
        int rowCount = rs.getInt("row_count");
        System.out.println("✅ state_gdp table contains " + rowCount + " rows");
        
        if (rowCount > 0) {
          // If data exists, show a sample
          query = "SELECT geo_fips, geo_name, line_code, line_description, year, value, units "
              + "FROM econ.state_gdp LIMIT 3";
          
          try (ResultSet rs2 = stmt.executeQuery(query)) {
            while (rs2.next()) {
              System.out.println("✅ Sample: " + rs2.getString("geo_fips") + " (" + rs2.getString("geo_name") + "), " 
                  + rs2.getString("line_description") + ", " + rs2.getInt("year") + ": " + rs2.getDouble("value"));
            }
          }
        }
      } catch (Exception e) {
        System.out.println("❌ state_gdp table query failed: " + e.getMessage());
        throw e;
      }
    }
  }
}