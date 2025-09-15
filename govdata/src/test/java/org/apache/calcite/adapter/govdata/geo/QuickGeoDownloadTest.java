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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Quick integration test for geographic data download to unified structure.
 */
@Tag("integration")
public class QuickGeoDownloadTest {

  @BeforeAll
  public static void checkEnvironment() {
    System.out.println("Quick Geographic Data Download Test");
    System.out.println("===================================");
  }

  @Test
  public void testStatesDownloadToUnifiedStructure() throws Exception {
    // Use the unified directory on T9
    String baseDir = System.getenv("GOVDATA_PARQUET_DIR");
    if (baseDir == null) {
      baseDir = "/Volumes/T9/govdata-parquet";
    }
    
    System.out.println("Base directory: " + baseDir);
    
    // Create hive-partitioned structure
    File geoDir = new File(baseDir, "source=geo");
    File boundaryDir = new File(geoDir, "type=boundary");
    boundaryDir.mkdirs();
    
    System.out.println("Target boundary directory: " + boundaryDir);
    
    // Download just states (small file ~5MB)
    System.out.println("Downloading states shapefile...");
    TigerDataDownloader tiger = new TigerDataDownloader(boundaryDir, 2024, true);
    File statesDir = tiger.downloadStatesFirstYear();
    
    // Verify download
    assertNotNull(statesDir, "States directory should be created");
    assertTrue(statesDir.exists(), "States directory should exist");
    
    File[] stateFiles = statesDir.listFiles();
    assertTrue(stateFiles != null && stateFiles.length > 0, "States directory should contain files");
    
    System.out.println("✓ States downloaded to: " + statesDir);
    System.out.println("  Files downloaded: " + stateFiles.length);
    
    // Show the structure
    System.out.println("\nHive-partitioned structure created:");
    showStructure(geoDir, "");
    
    // Verify the hive partitioning
    assertTrue(statesDir.getAbsolutePath().contains("source=geo"), "Should be in geo partition");
    assertTrue(statesDir.getAbsolutePath().contains("type=boundary"), "Should be in boundary partition");
    
    System.out.println("\n✓ SUCCESS: Geographic data downloaded into unified hive-partitioned structure!");
  }
  
  private static void showStructure(File dir, String indent) {
    if (!dir.exists()) return;
    System.out.println(indent + dir.getName() + "/");
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.isDirectory()) {
          showStructure(f, indent + "  ");
        } else {
          System.out.println(indent + "  " + f.getName() + " (" + (f.length()/1024) + " KB)");
        }
      }
    }
  }
}