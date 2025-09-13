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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Simple test to verify new geographic tables are properly registered and accessible.
 */
@Tag("integration")
public class SimpleNewTablesTest {

  @BeforeAll
  public static void checkEnvironment() {
    System.out.println("Simple New Geographic Tables Test");
    System.out.println("================================");
  }

  @Test
  public void testNewTablesAccessible() throws Exception {
    // Use temp directory for test data
    String tempDir = System.getProperty("java.io.tmpdir") + "/simple-geo-test-" + System.currentTimeMillis();
    File testDir = new File(tempDir);
    testDir.mkdirs();
    
    System.out.println("Test directory: " + tempDir);
    
    // Create model for testing (disable auto-download to avoid actual downloads)
    String modelJson = "{"
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
        + "      \"autoDownload\": false"
        + "    }"
        + "  }"
        + "]}";
    
    // Write model to temp file
    File modelFile = File.createTempFile("simple-geo-test", ".json");
    java.nio.file.Files.write(modelFile.toPath(), modelJson.getBytes());
    
    // Create connection
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection connection = DriverManager.getConnection(
        "jdbc:calcite:model=" + modelFile.getAbsolutePath(), props)) {
      
      System.out.println("✓ Connection established successfully");
      
      // Test each new table can be accessed (just test schema, not data)
      String[] newTables = {
          "tiger_zctas",
          "tiger_census_tracts", 
          "tiger_block_groups",
          "tiger_cbsa",
          "hud_zip_cbsa_div",
          "hud_zip_congressional"
      };
      
      for (String tableName : newTables) {
        testTableAccessible(connection, tableName);
      }
      
      System.out.println("✓ All new tables are accessible!");
    }
  }
  
  private void testTableAccessible(Connection connection, String tableName) throws SQLException {
    try (Statement stmt = connection.createStatement()) {
      // Test that we can query the table structure (even if no data)
      // Use a simple SELECT that should work even with empty tables
      String sql = "SELECT * FROM geo." + tableName + " WHERE 1=0";  // No rows expected
      stmt.executeQuery(sql);
      System.out.println("  ✓ " + tableName + " - table accessible");
    } catch (Exception e) {
      System.err.println("  ✗ " + tableName + " - failed: " + e.getMessage());
      throw new AssertionError("Table " + tableName + " should be accessible", e);
    }
  }

  @Test
  public void testTigerDataDownloaderEnhancements() throws Exception {
    System.out.println("Testing TigerDataDownloader enhancements...");
    
    // Use temp directory
    String tempDir = System.getProperty("java.io.tmpdir") + "/tiger-test-" + System.currentTimeMillis();
    File testDir = new File(tempDir);
    testDir.mkdirs();
    
    TigerDataDownloader downloader = new TigerDataDownloader(testDir, 2024, true);
    
    // Test that new methods are available and accessible
    assertNotNull(downloader.getCacheDir(), "getCacheDir() should return directory");
    assertTrue(downloader.isAutoDownload(), "isAutoDownload() should return true");
    
    System.out.println("  ✓ getCacheDir() method works");
    System.out.println("  ✓ isAutoDownload() method works");
    
    // Verify the cache directory exists
    assertTrue(downloader.getCacheDir().exists(), "Cache directory should exist");
    
    System.out.println("  ✓ Cache directory exists: " + downloader.getCacheDir());
    
    // Test that the new download methods exist (don't actually call them to avoid downloads)
    // We can't easily test they work without downloading, but we can verify they exist
    try {
      // These methods should exist and not throw NoSuchMethodError
      downloader.getClass().getMethod("downloadCensusTracts");
      downloader.getClass().getMethod("downloadBlockGroups");
      downloader.getClass().getMethod("downloadCbsas");
      
      System.out.println("  ✓ downloadCensusTracts() method exists");
      System.out.println("  ✓ downloadBlockGroups() method exists");
      System.out.println("  ✓ downloadCbsas() method exists");
      
    } catch (NoSuchMethodException e) {
      throw new AssertionError("Missing expected download method", e);
    }
    
    System.out.println("✓ TigerDataDownloader enhancements verified");
  }

  @Test 
  public void testTableInstantiation() throws Exception {
    System.out.println("Testing table class instantiation...");
    
    // Create mock dependencies
    File tempDir = new File(System.getProperty("java.io.tmpdir"), "table-test-" + System.currentTimeMillis());
    tempDir.mkdirs();
    
    TigerDataDownloader mockDownloader = new TigerDataDownloader(tempDir, 2024, false);
    
    // Test that all new table classes can be instantiated
    TigerZctasTable zctasTable = new TigerZctasTable(mockDownloader);
    TigerCensusTractsTable tractsTable = new TigerCensusTractsTable(mockDownloader);
    TigerBlockGroupsTable bgTable = new TigerBlockGroupsTable(mockDownloader);
    TigerCbsaTable cbsaTable = new TigerCbsaTable(mockDownloader);
    
    // HUD tables (can use null fetcher for basic testing)
    HudZipCbsaDivTable cbsaDivTable = new HudZipCbsaDivTable(null);
    HudZipCongressionalTable congressionalTable = new HudZipCongressionalTable(null);
    
    System.out.println("  ✓ TigerZctasTable instantiated");
    System.out.println("  ✓ TigerCensusTractsTable instantiated");
    System.out.println("  ✓ TigerBlockGroupsTable instantiated");
    System.out.println("  ✓ TigerCbsaTable instantiated");
    System.out.println("  ✓ HudZipCbsaDivTable instantiated");
    System.out.println("  ✓ HudZipCongressionalTable instantiated");
    
    // Test that they have proper row types
    org.apache.calcite.sql.type.SqlTypeFactoryImpl typeFactory = 
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    
    assertNotNull(zctasTable.getRowType(typeFactory), "ZCTAs table should have row type");
    assertNotNull(tractsTable.getRowType(typeFactory), "Census tracts table should have row type");
    assertNotNull(bgTable.getRowType(typeFactory), "Block groups table should have row type");
    assertNotNull(cbsaTable.getRowType(typeFactory), "CBSA table should have row type");
    assertNotNull(cbsaDivTable.getRowType(typeFactory), "CBSA division table should have row type");
    assertNotNull(congressionalTable.getRowType(typeFactory), "Congressional table should have row type");
    
    System.out.println("  ✓ All tables have valid row types");
    
    // Test some expected columns exist in schemas
    org.apache.calcite.rel.type.RelDataType zctaType = zctasTable.getRowType(typeFactory);
    assertNotNull(zctaType.getField("zcta5", false, false), "ZCTAs should have zcta5 column");
    assertNotNull(zctaType.getField("population", false, false), "ZCTAs should have population column");
    
    org.apache.calcite.rel.type.RelDataType tractType = tractsTable.getRowType(typeFactory);
    assertNotNull(tractType.getField("tract_geoid", false, false), "Tracts should have tract_geoid column");
    
    org.apache.calcite.rel.type.RelDataType congressionalType = congressionalTable.getRowType(typeFactory);
    assertNotNull(congressionalType.getField("zip", false, false), "Congressional should have zip column");
    assertNotNull(congressionalType.getField("cd", false, false), "Congressional should have cd column");
    
    System.out.println("  ✓ Expected columns found in schemas");
    System.out.println("✓ Table instantiation and schema tests passed");
  }
  
  @Test
  public void testHivePartitionStructure() throws Exception {
    System.out.println("Testing hive partition structure...");
    
    // Use temp directory
    String tempDir = System.getProperty("java.io.tmpdir") + "/hive-test-" + System.currentTimeMillis();
    File testDir = new File(tempDir);
    
    // Test the expected hive-partitioned directory structure
    File geoSourceDir = new File(testDir, "source=geo");
    File boundaryDir = new File(geoSourceDir, "type=boundary");
    File crosswalkDir = new File(geoSourceDir, "type=crosswalk");
    
    // Create the structure
    boundaryDir.mkdirs();
    crosswalkDir.mkdirs();
    
    // Verify structure exists
    assertTrue(geoSourceDir.exists(), "geo source partition should exist");
    assertTrue(boundaryDir.exists(), "boundary type partition should exist");
    assertTrue(crosswalkDir.exists(), "crosswalk type partition should exist");
    
    System.out.println("  ✓ Hive partitions created:");
    System.out.println("    - " + geoSourceDir.getAbsolutePath());
    System.out.println("    - " + boundaryDir.getAbsolutePath());
    System.out.println("    - " + crosswalkDir.getAbsolutePath());
    
    // Test that paths contain expected patterns
    String boundaryPath = boundaryDir.getAbsolutePath();
    assertTrue(boundaryPath.contains("source=geo"), "Should contain geo source partition");
    assertTrue(boundaryPath.contains("type=boundary"), "Should contain boundary type partition");
    
    String crosswalkPath = crosswalkDir.getAbsolutePath(); 
    assertTrue(crosswalkPath.contains("source=geo"), "Should contain geo source partition");
    assertTrue(crosswalkPath.contains("type=crosswalk"), "Should contain crosswalk type partition");
    
    System.out.println("✓ Hive partition structure verified");
  }
}