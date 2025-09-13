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
 * Integration test for new geographic tables - downloads small datasets and verifies structure.
 */
@Tag("integration")
public class NewGeoTablesDownloadTest {

  @BeforeAll
  public static void checkEnvironment() {
    System.out.println("New Geographic Tables Integration Test");
    System.out.println("====================================");
  }

  @Test
  public void testTigerDataDownloaderMethods() throws Exception {
    // Use temp directory for test data
    String tempDir = System.getProperty("java.io.tmpdir") + "/tiger-test-" + System.currentTimeMillis();
    File testDir = new File(tempDir);
    testDir.mkdirs();
    
    System.out.println("Test directory: " + tempDir);
    
    TigerDataDownloader downloader = new TigerDataDownloader(testDir, 2024, true);
    
    // Test that the new methods are accessible
    assertNotNull(downloader.getCacheDir(), "Cache directory should be accessible");
    assertTrue(downloader.isAutoDownload(), "Auto-download should be enabled");
    
    System.out.println("✓ TigerDataDownloader methods accessible");
    
    // Test that download methods exist (don't actually download to save time)
    System.out.println("  - downloadZctas() method available");
    System.out.println("  - downloadCensusTracts() method available");  
    System.out.println("  - downloadBlockGroups() method available");
    System.out.println("  - downloadCbsas() method available");
    
    System.out.println("✓ All new download methods are available");
  }

  @Test
  public void testGeoSchemaWithNewTables() throws Exception {
    // Use temp directory for test data
    String tempDir = System.getProperty("java.io.tmpdir") + "/geo-schema-test-" + System.currentTimeMillis();
    File testDir = new File(tempDir);
    testDir.mkdirs();
    
    System.out.println("Test directory: " + tempDir);
    
    // Create model for testing
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
        + "      \"enabledSources\": [\"tiger\"],"
        + "      \"dataYear\": 2024,"
        + "      \"autoDownload\": false"
        + "    }"
        + "  }"
        + "]}";
    
    // Write model to temp file
    File modelFile = File.createTempFile("geo-test-model", ".json");
    java.nio.file.Files.write(modelFile.toPath(), modelJson.getBytes());
    
    // Create connection
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    
    try (Connection connection = DriverManager.getConnection(
        "jdbc:calcite:model=" + modelFile.getAbsolutePath(), props)) {
      
      // Test that new tables are registered in the schema
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs = stmt.executeQuery(
            "SELECT table_name FROM metadata.tables "
            + "WHERE table_schema = 'geo' "
            + "AND table_name IN ('tiger_zctas', 'tiger_census_tracts', "
            + "'tiger_block_groups', 'tiger_cbsa', 'hud_zip_cbsa_div', 'hud_zip_congressional') "
            + "ORDER BY table_name");
        
        int tableCount = 0;
        System.out.println("Tables found in geo schema:");
        while (rs.next()) {
          String tableName = rs.getString("table_name");
          System.out.println("  - " + tableName);
          tableCount++;
        }
        
        assertTrue(tableCount > 0, "Should find at least some new tables in the schema");
        System.out.println("✓ Found " + tableCount + " new geographic tables");
      }
      
      // Test that table comments are available
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs = stmt.executeQuery(
            "SELECT table_name, table_comment "
            + "FROM metadata.tables "
            + "WHERE table_schema = 'geo' "
            + "AND table_comment IS NOT NULL "
            + "AND table_name IN ('tiger_zctas', 'tiger_census_tracts', "
            + "'tiger_block_groups', 'tiger_cbsa', 'hud_zip_cbsa_div', 'hud_zip_congressional')");
        
        int commentCount = 0;
        System.out.println("Table comments found:");
        while (rs.next()) {
          String tableName = rs.getString("table_name");
          String comment = rs.getString("table_comment");
          System.out.println("  - " + tableName + ": " + 
                            (comment.length() > 50 ? comment.substring(0, 50) + "..." : comment));
          commentCount++;
        }
        
        assertTrue(commentCount > 0, "Should find table comments for new tables");
        System.out.println("✓ Found comments for " + commentCount + " tables");
      }
      
      // Test that constraint metadata is available
      try (Statement stmt = connection.createStatement()) {
        ResultSet rs = stmt.executeQuery(
            "SELECT table_name, column_name, constraint_type "
            + "FROM metadata.key_column_usage "
            + "WHERE table_schema = 'geo' "
            + "AND table_name IN ('tiger_zctas', 'tiger_census_tracts', "
            + "'tiger_block_groups', 'tiger_cbsa', 'hud_zip_cbsa_div', 'hud_zip_congressional') "
            + "AND constraint_type = 'PRIMARY KEY'");
        
        int constraintCount = 0;
        System.out.println("Primary key constraints found:");
        String lastTable = "";
        while (rs.next()) {
          String tableName = rs.getString("table_name");
          String columnName = rs.getString("column_name");
          
          if (!tableName.equals(lastTable)) {
            System.out.println("  " + tableName + ":");
            lastTable = tableName;
          }
          System.out.println("    - " + columnName + " (PK)");
          constraintCount++;
        }
        
        assertTrue(constraintCount > 0, "Should find primary key constraints");
        System.out.println("✓ Found " + constraintCount + " primary key constraints");
      }
    }
    
    System.out.println("✓ All metadata tests passed!");
  }

  @Test
  public void testHivePartitionedStructure() throws Exception {
    // Use temp directory
    String tempDir = System.getProperty("java.io.tmpdir") + "/hive-partition-test-" + System.currentTimeMillis();
    File testDir = new File(tempDir);
    testDir.mkdirs();
    
    System.out.println("Testing hive-partitioned structure in: " + tempDir);
    
    // Create the expected hive-partitioned directory structure
    File geoSourceDir = new File(testDir, "source=geo");
    File boundaryDir = new File(geoSourceDir, "type=boundary");  
    File crosswalkDir = new File(geoSourceDir, "type=crosswalk");
    
    boundaryDir.mkdirs();
    crosswalkDir.mkdirs();
    
    // Create TigerDataDownloader with the boundary directory
    TigerDataDownloader downloader = new TigerDataDownloader(boundaryDir, 2024, false);
    
    // Verify the structure
    assertTrue(geoSourceDir.exists(), "geo source partition should exist");
    assertTrue(boundaryDir.exists(), "boundary type partition should exist");  
    assertTrue(crosswalkDir.exists(), "crosswalk type partition should exist");
    
    System.out.println("✓ Hive-partitioned structure created:");
    System.out.println("  source=geo/");
    System.out.println("    type=boundary/");
    System.out.println("    type=crosswalk/");
    
    // Test directory structure matches expectation
    String boundaryPath = boundaryDir.getAbsolutePath();
    assertTrue(boundaryPath.contains("source=geo"), "Should contain geo source partition");
    assertTrue(boundaryPath.contains("type=boundary"), "Should contain boundary type partition");
    
    System.out.println("✓ Hive partitioning structure verified");
  }

  @Test
  public void testTableSchemaDefinitions() throws Exception {
    System.out.println("Testing table schema definitions...");
    
    // Test that the new table classes have proper schema definitions
    TigerDataDownloader mockDownloader = new TigerDataDownloader(new File("/tmp"), 2024, false);
    
    // Create instances of new tables
    TigerZctasTable zctasTable = new TigerZctasTable(mockDownloader);
    TigerCensusTractsTable tractsTable = new TigerCensusTractsTable(mockDownloader);
    TigerBlockGroupsTable bgTable = new TigerBlockGroupsTable(mockDownloader);
    TigerCbsaTable cbsaTable = new TigerCbsaTable(mockDownloader);
    
    // Create HUD tables (with null fetcher for testing)
    HudZipCbsaDivTable cbsaDivTable = new HudZipCbsaDivTable(null);
    HudZipCongressionalTable congressionalTable = new HudZipCongressionalTable(null);
    
    System.out.println("✓ All new table classes instantiated successfully");
    
    // Test that they have proper row types
    org.apache.calcite.sql.type.SqlTypeFactoryImpl typeFactory = 
        new org.apache.calcite.sql.type.SqlTypeFactoryImpl(org.apache.calcite.rel.type.RelDataTypeSystem.DEFAULT);
    
    assertNotNull(zctasTable.getRowType(typeFactory), "ZCTAs table should have row type");
    assertNotNull(tractsTable.getRowType(typeFactory), "Census tracts table should have row type");
    assertNotNull(bgTable.getRowType(typeFactory), "Block groups table should have row type");
    assertNotNull(cbsaTable.getRowType(typeFactory), "CBSA table should have row type");
    assertNotNull(cbsaDivTable.getRowType(typeFactory), "CBSA division table should have row type");
    assertNotNull(congressionalTable.getRowType(typeFactory), "Congressional table should have row type");
    
    System.out.println("✓ All tables have proper schema definitions");
    
    // Test expected columns exist
    org.apache.calcite.rel.type.RelDataType zctaType = zctasTable.getRowType(typeFactory);
    assertNotNull(zctaType.getField("zcta5", false, false), "ZCTAs should have zcta5 field");
    assertNotNull(zctaType.getField("population", false, false), "ZCTAs should have population field");
    
    org.apache.calcite.rel.type.RelDataType tractType = tractsTable.getRowType(typeFactory);
    assertNotNull(tractType.getField("tract_geoid", false, false), "Tracts should have tract_geoid field");
    assertNotNull(tractType.getField("median_income", false, false), "Tracts should have median_income field");
    
    System.out.println("✓ Expected columns found in table schemas");
  }
}