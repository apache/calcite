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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for geographic data download functionality.
 * This test requires environment variables to be set for Census API and HUD credentials.
 */
@Tag("integration")
public class GeoDataDownloadTest {

  @TempDir
  File tempDir;

  @BeforeAll
  public static void checkEnvironment() {
    // Check if credentials are available
    String censusKey = System.getenv("CENSUS_API_KEY");
    String hudToken = System.getenv("HUD_TOKEN");
    
    System.out.println("Geographic Data Download Test Environment:");
    System.out.println("  CENSUS_API_KEY: " + (censusKey != null ? "Set" : "Not set"));
    System.out.println("  HUD_TOKEN: " + (hudToken != null ? "Set" : "Not set"));
  }

  @Test
  public void testTigerDataDownload() throws Exception {
    System.out.println("\n=== Testing TIGER Data Download ===");
    
    // Create a simple model that only enables TIGER data (no credentials needed)
    String modelJson = "{\n" +
        "  \"version\": \"1.0\",\n" +
        "  \"defaultSchema\": \"GEO\",\n" +
        "  \"schemas\": [{\n" +
        "    \"name\": \"GEO\",\n" +
        "    \"type\": \"custom\",\n" +
        "    \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\",\n" +
        "    \"operand\": {\n" +
        "      \"dataSource\": \"geo\",\n" +
        "      \"cacheDir\": \"" + tempDir.getAbsolutePath() + "\",\n" +
        "      \"enabledSources\": [\"tiger\"],\n" +
        "      \"dataYear\": 2024,\n" +
        "      \"autoDownload\": true\n" +
        "    }\n" +
        "  }]\n" +
        "}";
    
    // Write model to temp file
    File modelFile = new File(tempDir, "tiger-model.json");
    java.nio.file.Files.writeString(modelFile.toPath(), modelJson);
    
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    
    // Load the JDBC driver
    Class.forName("org.apache.calcite.jdbc.Driver");
    
    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelFile.getAbsolutePath(), props)) {
      
      System.out.println("Connected to Calcite with TIGER-only schema");
      
      // List tables
      DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet tables = meta.getTables(null, "GEO", "%", null)) {
        System.out.println("Available tables:");
        while (tables.next()) {
          System.out.println("  - " + tables.getString("TABLE_NAME"));
        }
      }
      
      // Download states data
      System.out.println("\nDownloading states data...");
      TigerDataDownloader downloader = new TigerDataDownloader(
          new File(tempDir, "tiger-data"), 2024, true);
      
      // Download states shapefile
      File statesDir = downloader.downloadStatesFirstYear();
      
      assertNotNull(statesDir, "States directory should be created");
      assertTrue(statesDir.exists(), "States directory should exist");
      System.out.println("✓ States data downloaded to: " + statesDir.getAbsolutePath());
      
      // Check for shapefile components
      File[] stateFiles = statesDir.listFiles();
      assertTrue(stateFiles != null && stateFiles.length > 0, "States directory should contain files");
      System.out.println("  Downloaded " + stateFiles.length + " state files");
      
      // Download counties shapefile
      System.out.println("\nDownloading counties data...");
      File countiesDir = downloader.downloadCountiesFirstYear();
      
      assertNotNull(countiesDir, "Counties directory should be created");
      assertTrue(countiesDir.exists(), "Counties directory should exist");
      System.out.println("✓ Counties data downloaded to: " + countiesDir.getAbsolutePath());
      
      File[] countyFiles = countiesDir.listFiles();
      assertTrue(countyFiles != null && countyFiles.length > 0, "Counties directory should contain files");
      System.out.println("  Downloaded " + countyFiles.length + " county files");
    }
  }

  @Test
  public void testCensusApiWithCredentials() throws Exception {
    String censusKey = System.getenv("CENSUS_API_KEY");
    if (censusKey == null || censusKey.isEmpty()) {
      System.out.println("⚠ Skipping Census API test - CENSUS_API_KEY not set");
      return;
    }
    
    System.out.println("\n=== Testing Census API with Credentials ===");
    
    CensusApiClient client = new CensusApiClient(
        censusKey,
        new File(tempDir, "census-cache")
    );
    
    // Test fetching population data for California
    System.out.println("Fetching population data for California...");
    
    // Get ACS data for California
    com.fasterxml.jackson.databind.JsonNode result = client.getAcsData(
        2022,  // Year (2022 = 2018-2022 5-year estimates)
        "NAME,B01003_001E",  // Variables: Name and total population
        "state:06"  // California FIPS code
    );
    
    assertNotNull(result, "Census API should return data");
    assertTrue(result.isArray() && result.size() > 1, "Result should contain data rows");
    
    System.out.println("✓ Census API returned data:");
    System.out.println("  " + result.toString().substring(0, Math.min(200, result.toString().length())) + "...");
  }

  @Test
  public void testHudCrosswalkWithToken() throws Exception {
    String hudToken = System.getenv("HUD_TOKEN");
    String hudUsername = System.getenv("HUD_USERNAME");
    String hudPassword = System.getenv("HUD_PASSWORD");
    
    if (hudToken == null && (hudUsername == null || hudPassword == null)) {
      System.out.println("⚠ Skipping HUD test - credentials not set");
      return;
    }
    
    System.out.println("\n=== Testing HUD Crosswalk API ===");
    
    HudCrosswalkFetcher fetcher = new HudCrosswalkFetcher(
        hudUsername,
        hudPassword,
        hudToken,
        new File(tempDir, "hud-crosswalk")
    );
    
    // Download ZIP to County crosswalk
    System.out.println("Downloading ZIP to County crosswalk for Q3 2024...");
    File crosswalkFile = fetcher.downloadZipToCounty("3", 2024);
    
    assertNotNull(crosswalkFile, "Crosswalk file should be downloaded");
    assertTrue(crosswalkFile.exists(), "Crosswalk file should exist");
    System.out.println("✓ HUD crosswalk downloaded: " + crosswalkFile.getAbsolutePath());
    System.out.println("  File size: " + (crosswalkFile.length() / 1024) + " KB");
  }
}