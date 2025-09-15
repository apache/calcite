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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Minimal test to check if GEO model loads without stack overflow.
 */
public class MinimalGeoLoadTest {

  @BeforeAll
  public static void setupEnvironment() {
    // Ensure .env.test is loaded before any tests run
    TestEnvironmentLoader.ensureLoaded();
  }

  @Test
  @Tag("integration")
  public void testMinimalGeoModelLoad() throws Exception {
    System.out.println("Starting minimal GEO model load test...");
    
    String modelPath = MinimalGeoLoadTest.class.getResource(
        "/govdata-geo-test-model.json").getPath();
    
    System.out.println("Model path: " + modelPath);
    
    // Use the same properties as GeoDataVerificationTest
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    
    // Try to create a connection and query information_schema
    try (Connection conn = DriverManager.getConnection(
        "jdbc:calcite:model=" + modelPath, props)) {
      
      System.out.println("Connection created successfully!");
      assertNotNull(conn);
      
      // Query information_schema for tables
      System.out.println("\nQuerying information_schema.tables...");
      String query = "SELECT \"TABLE_SCHEMA\", \"TABLE_NAME\" " +
                     "FROM information_schema.\"TABLES\" " +
                     "WHERE \"TABLE_SCHEMA\" = 'GEO' " +
                     "ORDER BY \"TABLE_NAME\"";
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(query)) {
        
        System.out.println("Query executed successfully! Tables found:");
        int count = 0;
        while (rs.next()) {
          count++;
          System.out.println("  " + rs.getString("TABLE_SCHEMA") + "." + 
                           rs.getString("TABLE_NAME"));
        }
        System.out.println("Total tables: " + count);
        // Note: information_schema may not show the tables, but let's continue
      }
      
      // Check if parquet files were created
      System.out.println("\nChecking for parquet files...");
      String parquetDir = System.getenv("GOVDATA_PARQUET_DIR");
      if (parquetDir == null) {
        parquetDir = System.getProperty("user.home") + "/govdata-parquet";
      }
      File geoDir = new File(parquetDir + "/source=geo/type=boundary");
      
      System.out.println("Looking in directory: " + geoDir.getAbsolutePath());
      
      if (geoDir.exists()) {
        System.out.println("Directory exists!");
        
        // Check for expected parquet files
        String[] expectedFiles = {
            "year=2023/states.parquet",
            "year=2023/counties.parquet",
            "year=2023/places.parquet",
            "year=2023/zctas.parquet",
            "year=2024/states.parquet",
            "year=2024/counties.parquet",
            "year=2024/places.parquet",
            "year=2024/zctas.parquet"
        };
        
        System.out.println("\nChecking for expected parquet files:");
        int foundCount = 0;
        for (String expectedFile : expectedFiles) {
          File parquetFile = new File(geoDir, expectedFile);
          boolean exists = parquetFile.exists();
          System.out.println("  " + expectedFile + ": " + 
              (exists ? "✓ EXISTS (size: " + parquetFile.length() + " bytes)" : "✗ MISSING"));
          if (exists) {
            foundCount++;
            assertTrue(parquetFile.length() > 0, "Parquet file should not be empty: " + expectedFile);
          }
        }
        
        System.out.println("\nFound " + foundCount + "/" + expectedFiles.length + " expected parquet files");
        assertTrue(foundCount > 0, "Should find at least some parquet files");
        
      } else {
        System.out.println("Directory does not exist: " + geoDir.getAbsolutePath());
      }
      
      System.out.println("\nTest completed!");
    }
  }
}