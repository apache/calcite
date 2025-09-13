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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test that downloads limited geographic data for each type:
 * - TIGER boundary data (states and selected counties)
 * - Census demographic data (population for selected areas)
 * - HUD crosswalk data (ZIP to county mappings)
 */
@Tag("integration")
public class LimitedGeoDataTest {

  @BeforeAll
  public static void setupEnvironment() {
    TestEnvironmentLoader.ensureLoaded();
  }

  @Test
  @Timeout(value = 10, unit = TimeUnit.MINUTES)
  public void testLimitedGeoDataDownload() throws Exception {
    System.out.println("\n=== LIMITED GEOGRAPHIC DATA DOWNLOAD TEST ===");
    
    // Create model file with limited geographic data configuration
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
        "        \"cacheDirectory\": \"${GEO_CACHE_DIR:/Volumes/T9/geo-test-limited}\",\n" +
        "        \"autoDownload\": true,\n" +
        "        \"dataYear\": 2024,\n" +
        "        \"enabledSources\": [\"tiger\", \"census\", \"hud\"],\n" +
        "        \"limitedMode\": true,\n" +
        "        \"states\": [\"CA\", \"NY\", \"TX\"],\n" +
        "        \"censusApiKey\": \"${CENSUS_API_KEY:}\",\n" +
        "        \"hudUsername\": \"${HUD_USERNAME:}\",\n" +
        "        \"hudPassword\": \"${HUD_PASSWORD:}\"\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";

    File tempDir = Files.createTempDirectory("geo-test").toFile();
    File modelFile = new File(tempDir, "limited-geo-model.json");
    Files.write(modelFile.toPath(), modelJson.getBytes());

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, info)) {
      System.out.println("\n=== STEP 1: Check Available Tables ===");
      
      List<String> tables = new ArrayList<>();
      try (ResultSet rs = conn.getMetaData().getTables(null, "GEO", "%", new String[]{"TABLE"})) {
        while (rs.next()) {
          String tableName = rs.getString("TABLE_NAME");
          tables.add(tableName);
          System.out.println("Found table: " + tableName);
        }
      }
      
      // Verify expected tables exist
      assertTrue(tables.contains("tiger_states") || tables.contains("TIGER_STATES"), 
          "Should have tiger_states table");
      assertTrue(tables.contains("tiger_counties") || tables.contains("TIGER_COUNTIES"), 
          "Should have tiger_counties table");
      
      System.out.println("\n=== STEP 2: Query TIGER States Data ===");
      String statesQuery = "SELECT state_fips, state_code, state_name, state_abbr " +
                          "FROM geo.tiger_states " +
                          "WHERE state_abbr IN ('CA', 'NY', 'TX') " +
                          "ORDER BY state_abbr";
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(statesQuery)) {
        
        System.out.println("\nStates data:");
        System.out.println("FIPS\tCode\tName\t\tAbbr");
        System.out.println("----\t----\t----\t\t----");
        
        int stateCount = 0;
        while (rs.next()) {
          stateCount++;
          System.out.printf("%s\t%s\t%-20s\t%s\n",
              rs.getString("state_fips"),
              rs.getString("state_code"),
              rs.getString("state_name"),
              rs.getString("state_abbr"));
        }
        
        assertTrue(stateCount > 0, "Should have retrieved state data");
        assertTrue(stateCount <= 3, "Should have at most 3 states");
      }
      
      System.out.println("\n=== STEP 3: Query TIGER Counties Data ===");
      String countiesQuery = "SELECT state_fips, county_fips, county_name " +
                            "FROM geo.tiger_counties " +
                            "WHERE state_fips IN ('06', '36', '48') " + // CA, NY, TX FIPS codes
                            "ORDER BY state_fips, county_name " +
                            "LIMIT 10";
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(countiesQuery)) {
        
        System.out.println("\nCounties data (first 10):");
        System.out.println("State\tCounty\tName");
        System.out.println("-----\t------\t----");
        
        int countyCount = 0;
        while (rs.next()) {
          countyCount++;
          System.out.printf("%s\t%s\t%s\n",
              rs.getString("state_fips"),
              rs.getString("county_fips"),
              rs.getString("county_name"));
        }
        
        assertTrue(countyCount > 0, "Should have retrieved county data");
      }
      
      // Check if Census tables exist (may require API key)
      boolean hasCensusData = tables.contains("census_population") || 
                              tables.contains("CENSUS_POPULATION");
      
      if (hasCensusData) {
        System.out.println("\n=== STEP 4: Query Census Population Data ===");
        String popQuery = "SELECT state_code, county_code, total_population " +
                         "FROM geo.census_population " +
                         "WHERE state_code IN ('06', '36', '48') " +
                         "ORDER BY total_population DESC " +
                         "LIMIT 5";
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(popQuery)) {
          
          System.out.println("\nTop 5 counties by population:");
          System.out.println("State\tCounty\tPopulation");
          System.out.println("-----\t------\t----------");
          
          while (rs.next()) {
            System.out.printf("%s\t%s\t%,d\n",
                rs.getString("state_code"),
                rs.getString("county_code"),
                rs.getLong("total_population"));
          }
        }
      } else {
        System.out.println("\n=== STEP 4: Census Data Not Available ===");
        System.out.println("Census data requires CENSUS_API_KEY environment variable");
      }
      
      // Check if HUD tables exist (may require credentials)
      boolean hasHudData = tables.contains("hud_zip_county") || 
                           tables.contains("HUD_ZIP_COUNTY");
      
      if (hasHudData) {
        System.out.println("\n=== STEP 5: Query HUD Crosswalk Data ===");
        String hudQuery = "SELECT zip, county_code, res_ratio " +
                         "FROM geo.hud_zip_county " +
                         "WHERE zip LIKE '900%' " + // LA area ZIPs
                         "ORDER BY zip " +
                         "LIMIT 5";
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(hudQuery)) {
          
          System.out.println("\nHUD ZIP to County mappings:");
          System.out.println("ZIP\tCounty\tRatio");
          System.out.println("-----\t------\t-----");
          
          while (rs.next()) {
            System.out.printf("%s\t%s\t%.2f\n",
                rs.getString("zip"),
                rs.getString("county_code"),
                rs.getDouble("res_ratio"));
          }
        }
      } else {
        System.out.println("\n=== STEP 5: HUD Data Not Available ===");
        System.out.println("HUD data requires HUD_USERNAME and HUD_PASSWORD environment variables");
      }
      
      System.out.println("\n=== STEP 6: Verify Parquet Files Created ===");
      
      // Check cache directory for parquet files
      String cacheDir = System.getenv("GEO_CACHE_DIR");
      if (cacheDir == null) {
        cacheDir = "/Volumes/T9/geo-test-limited";
      }
      
      File cacheRoot = new File(cacheDir);
      if (cacheRoot.exists()) {
        // Check for boundary data
        File boundaryDir = new File(cacheRoot, "source=geo/type=boundary");
        if (boundaryDir.exists()) {
          File[] boundaryFiles = boundaryDir.listFiles((dir, name) -> name.endsWith(".parquet"));
          if (boundaryFiles != null && boundaryFiles.length > 0) {
            System.out.println("Found " + boundaryFiles.length + " boundary parquet files");
            for (File f : boundaryFiles) {
              System.out.println("  - " + f.getName() + " (" + f.length() + " bytes)");
            }
          }
        }
        
        // Check for demographic data
        File demoDir = new File(cacheRoot, "source=geo/type=demographic");
        if (demoDir.exists()) {
          File[] demoFiles = demoDir.listFiles((dir, name) -> name.endsWith(".parquet"));
          if (demoFiles != null && demoFiles.length > 0) {
            System.out.println("Found " + demoFiles.length + " demographic parquet files");
          }
        }
        
        // Check for crosswalk data
        File crosswalkDir = new File(cacheRoot, "source=geo/type=crosswalk");
        if (crosswalkDir.exists()) {
          File[] crosswalkFiles = crosswalkDir.listFiles((dir, name) -> name.endsWith(".parquet"));
          if (crosswalkFiles != null && crosswalkFiles.length > 0) {
            System.out.println("Found " + crosswalkFiles.length + " crosswalk parquet files");
          }
        }
      }
      
      System.out.println("\n=== TEST COMPLETED SUCCESSFULLY ===");
    }
  }
  
  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void testGeoSchemaWithConstraints() throws Exception {
    System.out.println("\n=== GEO SCHEMA CONSTRAINT TEST ===");
    
    // Create model with just basic configuration
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
        "        \"autoDownload\": false\n" +
        "      }\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    
    File tempDir = Files.createTempDirectory("geo-constraint-test").toFile();
    File modelFile = new File(tempDir, "geo-constraint-model.json");
    Files.write(modelFile.toPath(), modelJson.getBytes());
    
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    
    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();
    
    try (Connection conn = DriverManager.getConnection(jdbcUrl, info)) {
      // Check for primary keys on tiger_states
      try (ResultSet pks = conn.getMetaData().getPrimaryKeys(null, "GEO", "tiger_states")) {
        List<String> pkColumns = new ArrayList<>();
        while (pks.next()) {
          pkColumns.add(pks.getString("COLUMN_NAME"));
        }
        System.out.println("Primary key columns on tiger_states: " + pkColumns);
      }
      
      // Check for foreign keys from tiger_counties to tiger_states
      try (ResultSet fks = conn.getMetaData().getImportedKeys(null, "GEO", "tiger_counties")) {
        System.out.println("\nForeign keys on tiger_counties:");
        while (fks.next()) {
          System.out.printf("  %s -> %s.%s\n",
              fks.getString("FKCOLUMN_NAME"),
              fks.getString("PKTABLE_NAME"),
              fks.getString("PKCOLUMN_NAME"));
        }
      }
      
      System.out.println("\n=== CONSTRAINT TEST COMPLETED ===");
    }
  }
}