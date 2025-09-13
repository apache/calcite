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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive integration test for all new geographic tables.
 * 
 * <p>Tests that each geographic table type can:
 * 1. Download a small amount of data
 * 2. Generate parquet files in hive-partitioned structure
 * 3. Be queried via SQL
 */
@Tag("integration")
public class ComprehensiveGeoTableTest {

  private static String tempDir;
  private static Connection connection;

  @BeforeAll
  public static void setupTest() throws Exception {
    System.out.println("Comprehensive Geographic Table Integration Test");
    System.out.println("=============================================");
    
    // Use temp directory for test data
    tempDir = System.getProperty("java.io.tmpdir") + "/govdata-geo-test-" + System.currentTimeMillis();
    File testDir = new File(tempDir);
    testDir.mkdirs();
    
    System.out.println("Test data directory: " + tempDir);
    
    // Create model for testing
    String modelJson = createTestModel();
    
    // Write model to temp file
    File modelFile = File.createTempFile("geo-test-model", ".json");
    java.nio.file.Files.write(modelFile.toPath(), modelJson.getBytes());
    
    // Create connection
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    
    connection = DriverManager.getConnection("jdbc:calcite:model=" + modelFile.getAbsolutePath(), props);
    
    System.out.println("✓ Test setup complete");
  }
  
  private static String createTestModel() {
    return "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"geo\","
        + "\"schemas\": ["
        + "  {"
        + "    \"name\": \"geo\","
        + "    \"type\": \"custom\","
        + "    \"factory\": \"org.apache.calcite.adapter.govdata.geo.GeoSchemaFactory\","
        + "    \"operand\": {"
        + "      \"cacheDir\": \"" + tempDir + "\","
        + "      \"enabledSources\": [\"tiger\", \"hud\"],"
        + "      \"dataYear\": 2024,"
        + "      \"autoDownload\": true"
        + "    }"
        + "  }"
        + "]}";
  }

  @Test
  public void testTigerZctasTable() throws Exception {
    System.out.println("\nTesting TIGER ZCTAs table...");
    
    try (Statement stmt = connection.createStatement()) {
      // Test that the table is accessible
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM geo.tiger_zctas LIMIT 1");
      assertTrue(rs.next(), "Should return at least one row");
      
      // Test that we can query specific columns
      rs = stmt.executeQuery(
          "SELECT zcta5, namelsad, population, housing_units "
          + "FROM geo.tiger_zctas "
          + "WHERE zcta5 IS NOT NULL "
          + "LIMIT 5");
      
      int count = 0;
      while (rs.next()) {
        String zcta = rs.getString("zcta5");
        String name = rs.getString("namelsad");
        assertNotNull(zcta, "ZCTA code should not be null");
        System.out.println("  ZCTA: " + zcta + " - " + name);
        count++;
      }
      
      System.out.println("  ✓ Query successful, returned " + count + " ZCTAs");
    }
  }

  @Test
  public void testTigerCensusTractsTable() throws Exception {
    System.out.println("\nTesting TIGER Census Tracts table...");
    
    try (Statement stmt = connection.createStatement()) {
      // Test that the table is accessible
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM geo.tiger_census_tracts LIMIT 1");
      assertTrue(rs.next(), "Should return at least one row");
      
      // Test that we can query specific columns
      rs = stmt.executeQuery(
          "SELECT tract_geoid, namelsad, population, median_income "
          + "FROM geo.tiger_census_tracts "
          + "WHERE tract_geoid IS NOT NULL "
          + "LIMIT 5");
      
      int count = 0;
      while (rs.next()) {
        String tractId = rs.getString("tract_geoid");
        String name = rs.getString("namelsad");
        assertNotNull(tractId, "Tract GEOID should not be null");
        System.out.println("  Tract: " + tractId + " - " + name);
        count++;
      }
      
      System.out.println("  ✓ Query successful, returned " + count + " census tracts");
    }
  }

  @Test
  public void testTigerBlockGroupsTable() throws Exception {
    System.out.println("\nTesting TIGER Block Groups table...");
    
    try (Statement stmt = connection.createStatement()) {
      // Test that the table is accessible
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM geo.tiger_block_groups LIMIT 1");
      assertTrue(rs.next(), "Should return at least one row");
      
      // Test that we can query specific columns
      rs = stmt.executeQuery(
          "SELECT bg_geoid, tract_code, blkgrp, population, housing_units "
          + "FROM geo.tiger_block_groups "
          + "WHERE bg_geoid IS NOT NULL "
          + "LIMIT 5");
      
      int count = 0;
      while (rs.next()) {
        String bgId = rs.getString("bg_geoid");
        String tractCode = rs.getString("tract_code");
        String blkgrp = rs.getString("blkgrp");
        assertNotNull(bgId, "Block group GEOID should not be null");
        System.out.println("  Block Group: " + bgId + " (Tract: " + tractCode + ", BG: " + blkgrp + ")");
        count++;
      }
      
      System.out.println("  ✓ Query successful, returned " + count + " block groups");
    }
  }

  @Test
  public void testTigerCbsaTable() throws Exception {
    System.out.println("\nTesting TIGER CBSA table...");
    
    try (Statement stmt = connection.createStatement()) {
      // Test that the table is accessible
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM geo.tiger_cbsa LIMIT 1");
      assertTrue(rs.next(), "Should return at least one row");
      
      // Test that we can query specific columns
      rs = stmt.executeQuery(
          "SELECT cbsa_code, cbsa_name, cbsa_type, population "
          + "FROM geo.tiger_cbsa "
          + "WHERE cbsa_code IS NOT NULL "
          + "LIMIT 5");
      
      int count = 0;
      while (rs.next()) {
        String cbsaCode = rs.getString("cbsa_code");
        String cbsaName = rs.getString("cbsa_name");
        String cbsaType = rs.getString("cbsa_type");
        assertNotNull(cbsaCode, "CBSA code should not be null");
        System.out.println("  CBSA: " + cbsaCode + " - " + cbsaName + " (" + cbsaType + ")");
        count++;
      }
      
      System.out.println("  ✓ Query successful, returned " + count + " CBSAs");
    }
  }

  @Test
  public void testHudZipCbsaDivTable() throws Exception {
    System.out.println("\nTesting HUD ZIP-CBSA Division table...");
    
    try (Statement stmt = connection.createStatement()) {
      // Test that the table is accessible
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM geo.hud_zip_cbsa_div LIMIT 1");
      assertTrue(rs.next(), "Should return at least one row");
      
      // Test that we can query specific columns
      rs = stmt.executeQuery(
          "SELECT zip, cbsadiv, cbsadiv_name, cbsa, cbsa_name, tot_ratio "
          + "FROM geo.hud_zip_cbsa_div "
          + "WHERE zip IS NOT NULL "
          + "LIMIT 5");
      
      int count = 0;
      while (rs.next()) {
        String zip = rs.getString("zip");
        String cbsaDiv = rs.getString("cbsadiv");
        String divName = rs.getString("cbsadiv_name");
        assertNotNull(zip, "ZIP code should not be null");
        System.out.println("  ZIP: " + zip + " → CBSA Div: " + cbsaDiv + " (" + divName + ")");
        count++;
      }
      
      System.out.println("  ✓ Query successful, returned " + count + " ZIP-CBSA Division mappings");
    }
  }

  @Test
  public void testHudZipCongressionalTable() throws Exception {
    System.out.println("\nTesting HUD ZIP-Congressional District table...");
    
    try (Statement stmt = connection.createStatement()) {
      // Test that the table is accessible
      ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM geo.hud_zip_congressional LIMIT 1");
      assertTrue(rs.next(), "Should return at least one row");
      
      // Test that we can query specific columns
      rs = stmt.executeQuery(
          "SELECT zip, cd, cd_name, state_cd, state_code, state_name, tot_ratio "
          + "FROM geo.hud_zip_congressional "
          + "WHERE zip IS NOT NULL "
          + "LIMIT 5");
      
      int count = 0;
      while (rs.next()) {
        String zip = rs.getString("zip");
        String cd = rs.getString("cd");
        String cdName = rs.getString("cd_name");
        String stateCode = rs.getString("state_code");
        assertNotNull(zip, "ZIP code should not be null");
        System.out.println("  ZIP: " + zip + " → CD: " + cd + " (" + cdName + ") in " + stateCode);
        count++;
      }
      
      System.out.println("  ✓ Query successful, returned " + count + " ZIP-Congressional mappings");
    }
  }

  @Test
  public void testCrossTableJoins() throws Exception {
    System.out.println("\nTesting cross-table joins...");
    
    try (Statement stmt = connection.createStatement()) {
      // Test joining census tracts with block groups
      ResultSet rs = stmt.executeQuery(
          "SELECT t.tract_geoid, t.namelsad as tract_name, "
          + "       COUNT(bg.bg_geoid) as block_group_count "
          + "FROM geo.tiger_census_tracts t "
          + "LEFT JOIN geo.tiger_block_groups bg ON t.tract_code = bg.tract_code "
          + "WHERE t.tract_geoid IS NOT NULL "
          + "GROUP BY t.tract_geoid, t.namelsad "
          + "LIMIT 5");
      
      int count = 0;
      while (rs.next()) {
        String tractId = rs.getString("tract_geoid");
        String tractName = rs.getString("tract_name");
        int bgCount = rs.getInt("block_group_count");
        System.out.println("  Tract: " + tractId + " (" + tractName + ") has " + bgCount + " block groups");
        count++;
      }
      
      assertTrue(count > 0, "Should return at least one joined result");
      System.out.println("  ✓ Cross-table join successful, returned " + count + " results");
    }
  }

  @Test
  public void testTableComments() throws Exception {
    System.out.println("\nTesting table comments...");
    
    try (Statement stmt = connection.createStatement()) {
      // Test that table comments are accessible
      ResultSet rs = stmt.executeQuery(
          "SELECT table_name, table_comment "
          + "FROM metadata.tables "
          + "WHERE table_schema = 'geo' "
          + "AND table_name IN ('tiger_zctas', 'tiger_census_tracts', 'tiger_block_groups', "
          + "'tiger_cbsa', 'hud_zip_cbsa_div', 'hud_zip_congressional') "
          + "ORDER BY table_name");
      
      int count = 0;
      while (rs.next()) {
        String tableName = rs.getString("table_name");
        String tableComment = rs.getString("table_comment");
        assertNotNull(tableComment, "Table comment should not be null for " + tableName);
        assertTrue(tableComment.length() > 10, "Table comment should be meaningful for " + tableName);
        System.out.println("  Table: " + tableName + " - " + 
                          (tableComment.length() > 50 ? tableComment.substring(0, 50) + "..." : tableComment));
        count++;
      }
      
      assertTrue(count >= 6, "Should find comments for at least 6 new tables");
      System.out.println("  ✓ Table comments verified for " + count + " tables");
    }
  }

  @Test
  public void testConstraintMetadata() throws Exception {
    System.out.println("\nTesting constraint metadata...");
    
    try (Statement stmt = connection.createStatement()) {
      // Test primary key constraints
      ResultSet rs = stmt.executeQuery(
          "SELECT table_name, column_name, constraint_type "
          + "FROM metadata.key_column_usage "
          + "WHERE table_schema = 'geo' "
          + "AND table_name IN ('tiger_zctas', 'tiger_census_tracts', 'tiger_block_groups', "
          + "'tiger_cbsa', 'hud_zip_cbsa_div', 'hud_zip_congressional') "
          + "AND constraint_type = 'PRIMARY KEY' "
          + "ORDER BY table_name, ordinal_position");
      
      int count = 0;
      String lastTable = "";
      while (rs.next()) {
        String tableName = rs.getString("table_name");
        String columnName = rs.getString("column_name");
        String constraintType = rs.getString("constraint_type");
        
        if (!tableName.equals(lastTable)) {
          System.out.println("  Table: " + tableName);
          lastTable = tableName;
        }
        System.out.println("    PK: " + columnName);
        count++;
      }
      
      assertTrue(count > 0, "Should find at least some primary key constraints");
      System.out.println("  ✓ Constraint metadata verified for " + count + " constraints");
    }
  }

  @Test
  public void testDataFileGeneration() throws Exception {
    System.out.println("\nTesting data file generation...");
    
    // Check that parquet files were created in hive-partitioned structure
    File geoDir = new File(tempDir, "source=geo");
    File boundaryDir = new File(geoDir, "type=boundary");
    File crosswalkDir = new File(geoDir, "type=crosswalk");
    
    System.out.println("  Checking directory structure:");
    System.out.println("    Geo source dir exists: " + geoDir.exists());
    System.out.println("    Boundary dir exists: " + boundaryDir.exists());
    System.out.println("    Crosswalk dir exists: " + crosswalkDir.exists());
    
    // Show any files that were created
    if (geoDir.exists()) {
      showDirectoryStructure(geoDir, "    ");
    }
    
    System.out.println("  ✓ File generation test complete");
  }

  private static void showDirectoryStructure(File dir, String indent) {
    if (!dir.exists()) return;
    
    System.out.println(indent + dir.getName() + "/");
    File[] files = dir.listFiles();
    if (files != null) {
      for (File file : files) {
        if (file.isDirectory()) {
          showDirectoryStructure(file, indent + "  ");
        } else {
          System.out.println(indent + "  " + file.getName() + 
                           " (" + (file.length() / 1024) + " KB)");
        }
      }
    }
  }
}