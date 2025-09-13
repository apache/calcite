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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test data download into unified hive-partitioned structure.
 */
@Tag("integration")
public class UnifiedStructureTest {

  @Test
  public void testCensusDataToUnifiedStructure() throws Exception {
    String censusKey = System.getenv("CENSUS_API_KEY");
    if (censusKey == null || censusKey.isEmpty()) {
      System.out.println("⚠ Skipping Census test - CENSUS_API_KEY not set");
      return;
    }
    
    System.out.println("Testing Census Data Download to Unified Structure");
    System.out.println("================================================");
    
    // Use the unified directory on T9
    String baseDir = "/Volumes/T9/govdata-parquet";
    File geoDir = new File(baseDir, "source=geo");
    File demographicDir = new File(geoDir, "type=demographic");
    demographicDir.mkdirs();
    
    System.out.println("Target demographic directory: " + demographicDir);
    
    // Download Census data
    CensusApiClient census = new CensusApiClient(censusKey, demographicDir);
    
    // Get data for multiple states
    String[] states = {"06", "36", "48"}; // CA, NY, TX
    for (String state : states) {
      System.out.println("Downloading data for state FIPS: " + state);
      
      com.fasterxml.jackson.databind.JsonNode data = census.getAcsData(
          2022, "NAME,B01003_001E,B19013_001E", "state:" + state
      );
      
      assertNotNull(data, "Census data should be returned for state " + state);
      System.out.println("  ✓ Data received for state " + state);
    }
    
    // Check the cache files were created
    File[] cacheFiles = demographicDir.listFiles((dir, name) -> name.endsWith(".json"));
    assertTrue(cacheFiles != null && cacheFiles.length > 0, "Cache files should be created");
    
    System.out.println("\nCache files created:");
    for (File f : cacheFiles) {
      System.out.println("  " + f.getName() + " (" + (f.length()/1024) + " KB)");
    }
    
    System.out.println("\n✓ SUCCESS: Census data cached in unified structure!");
    showUnifiedStructure();
  }
  
  private void showUnifiedStructure() {
    System.out.println("\nUnified Government Data Structure:");
    System.out.println("==================================");
    File base = new File("/Volumes/T9/govdata-parquet");
    if (base.exists()) {
      showStructure(base, "");
    }
  }
  
  private void showStructure(File dir, String indent) {
    System.out.println(indent + dir.getName() + "/");
    File[] files = dir.listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.getName().startsWith("._")) continue; // Skip macOS files
        if (f.isDirectory()) {
          showStructure(f, indent + "  ");
        } else {
          System.out.println(indent + "  " + f.getName() + " (" + (f.length()/1024) + " KB)");
        }
      }
    }
  }
}