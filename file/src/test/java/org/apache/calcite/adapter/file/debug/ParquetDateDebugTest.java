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
package org.apache.calcite.adapter.file.debug;

import org.apache.calcite.adapter.file.FileAdapterTests;

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Debug test to trace where DATE values get corrupted in PARQUET engine
 */
public class ParquetDateDebugTest {

  @Test public void debugDateCorruption() throws Exception {
    System.out.println("\n=== PARQUET Date Corruption Debug ===\n");
    
    // Step 1: Verify CSV content
    System.out.println("Step 1: Raw CSV Content");
    String csvPath = "src/test/resources/bug/DATE_FORMATS.csv";
    Files.lines(Paths.get(csvPath))
        .limit(6)
        .forEach(line -> System.out.println("  " + line));
    
    // Step 2: Delete any cached parquet file
    System.out.println("\nStep 2: Clear Parquet Cache");
    File parquetCache = new File("build/resources/test/bug/.parquet_cache/DATE_FORMATS.parquet");
    if (parquetCache.exists()) {
      parquetCache.delete();
      System.out.println("  Deleted: " + parquetCache.getAbsolutePath());
    }
    
    // Step 3: Query with LINQ4J first
    System.out.println("\nStep 3: LINQ4J Engine Results");
    Properties linq4jProps = new Properties();
    linq4jProps.put("model", FileAdapterTests.jsonPath("bug-linq4j"));
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", linq4jProps);
         Statement stmt = conn.createStatement()) {
      
      String sql = "SELECT ID, DATE_VALUE FROM \"DATE_FORMATS\" WHERE ID IN (4, 5, 6)";
      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          int id = rs.getInt(1);
          Date date = rs.getDate(2);
          int epochDay = Math.toIntExact(Math.floorDiv(date.getTime(), 86400000L));
          System.out.printf("  ID %d: %s (epoch day: %d)%n", id, date, epochDay);
        }
      }
    }
    
    // Step 4: Query with PARQUET engine
    System.out.println("\nStep 4: PARQUET Engine Results");
    Properties parquetProps = new Properties();
    parquetProps.put("model", FileAdapterTests.jsonPath("bug"));
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", parquetProps);
         Statement stmt = conn.createStatement()) {
      
      // First query to trigger parquet file creation
      String sql = "SELECT ID, DATE_VALUE FROM \"DATE_FORMATS\" ORDER BY ID";
      try (ResultSet rs = stmt.executeQuery(sql)) {
        while (rs.next()) {
          int id = rs.getInt(1);
          
          // Debug all possible ways to get the value
          Object rawObj = rs.getObject(2);
          String strVal = rs.getString(2);
          Date dateVal = rs.getDate(2);
          
          System.out.printf("  ID %d:%n", id);
          System.out.printf("    Raw object: %s (type: %s)%n", 
              rawObj, rawObj != null ? rawObj.getClass().getName() : "null");
          System.out.printf("    String: %s%n", strVal);
          System.out.printf("    Date: %s%n", dateVal);
          
          if (dateVal != null) {
            int epochDay = (int) (dateVal.getTime() / 86400000L);
            int epochDayFloor = Math.toIntExact(Math.floorDiv(dateVal.getTime(), 86400000L));
            System.out.printf("    Epoch day (simple): %d%n", epochDay);
            System.out.printf("    Epoch day (floor): %d%n", epochDayFloor);
          }
        }
      }
    }
    
    // Step 5: Check if parquet file was created
    System.out.println("\nStep 5: Parquet File Status");
    if (parquetCache.exists()) {
      System.out.println("  Parquet file created: " + parquetCache.length() + " bytes");
      
      // Try to examine the parquet file content using parquet-tools if available
      try {
        ProcessBuilder pb = new ProcessBuilder("parquet-tools", "cat", parquetCache.getAbsolutePath());
        Process p = pb.start();
        p.waitFor();
        
        System.out.println("\n  Parquet file content:");
        java.io.BufferedReader reader = new java.io.BufferedReader(
            new java.io.InputStreamReader(p.getInputStream()));
        String line;
        while ((line = reader.readLine()) != null) {
          if (line.contains("ID = 5") || line.contains("DATE_VALUE")) {
            System.out.println("    " + line);
          }
        }
      } catch (Exception e) {
        System.out.println("  Could not read parquet file with parquet-tools: " + e.getMessage());
      }
    }
    
    // Step 6: Trace through the conversion process
    System.out.println("\nStep 6: Conversion Process Analysis");
    System.out.println("  ParquetConversionUtil should use:");
    System.out.println("    - rs.getString(i) to get original value");
    System.out.println("    - LocalDate.parse() for ISO dates");
    System.out.println("    - Math.floorDiv() for negative epoch days");
    
    // The issue is likely in one of these places:
    System.out.println("\n  Potential corruption points:");
    System.out.println("    1. CsvEnumerator parsing (both engines use this)");
    System.out.println("    2. ParquetConversionUtil.convertToParquet()");
    System.out.println("    3. ParquetEnumerableFactory date retrieval");
    System.out.println("    4. Null handling converting null -> Date(0)");
  }
}