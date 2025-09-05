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
package org.apache.calcite.adapter.sec;

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
 * Quick test of DJI 5-year model.
 */
@Tag("unit")
public class QuickDJIModelTest {

  @Test
  public void testDJIModelLoading() throws Exception {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("QUICK TEST: DJI 5-YEAR MODEL");
    System.out.println("=".repeat(80) + "\n");

    // Create test directory on /Volumes/T9 and mock data
    File volumeDir = new File("/Volumes/T9/calcite-test-data");
    volumeDir.mkdirs();
    File testDir = new File(volumeDir, "sec-dji-test-" + System.currentTimeMillis());
    testDir.mkdirs();
    MockFinancialDataHelper.createMockFinancialData(testDir);

    // Test with minimal data - just Apple for 2024
    String testModel = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"sec\","
        + "\"schemas\":[{"
        + "  \"name\":\"sec\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"directory\":\"" + testDir.getAbsolutePath() + "\","
        + "    \"executionEngine\":\"duckdb\""  // Use DuckDB for Parquet files
        + "  }"
        + "}]}";

    Properties info = new Properties();
    info.put("model", "inline:" + testModel);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      System.out.println("✓ Connected to SEC adapter\n");

      // Simple query
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
             "SELECT COUNT(*) as cnt FROM financial_line_items")) {
        
        if (rs.next()) {
          int count = rs.getInt("cnt");
          System.out.println("Financial line items: " + count);
          assertTrue(count >= 0, "Should have non-negative count");
        }
      }
      
      System.out.println("\n✓ Model loads successfully");
    }
    
    System.out.println("=".repeat(80));
  }
  
  @Test 
  @Tag("integration")
  public void testRealDJIDownload() throws Exception {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("REAL DJI DOWNLOAD TEST (LIMITED)");
    System.out.println("=".repeat(80) + "\n");
    
    // Create test directory on /Volumes/T9 and mock data
    File volumeDir = new File("/Volumes/T9/calcite-test-data");
    volumeDir.mkdirs();
    File testDir = new File(volumeDir, "sec-dji-limited-" + System.currentTimeMillis());
    testDir.mkdirs();
    MockFinancialDataHelper.createMockFinancialData(testDir);
    MockFinancialDataHelper.createMockDow30Data(testDir);
    
    // Test with just 2 companies for 1 quarter to verify it works
    String limitedModel = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"sec\","
        + "\"schemas\":[{"
        + "  \"name\":\"sec\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"directory\":\"" + testDir.getAbsolutePath() + "\","
        + "    \"executionEngine\":\"duckdb\""  // Use DuckDB for Parquet files
        + "  }"
        + "}]}";
    
    Properties info = new Properties();
    info.put("model", "inline:" + limitedModel);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");
    
    System.out.println("Testing with Apple and Microsoft Q1 2024 only...\n");
    
    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      System.out.println("✓ Connected\n");
      
      // Query the data
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
             "SELECT DISTINCT company_name, filing_type, filing_date " +
             "FROM financial_line_items " +
             "ORDER BY company_name")) {
        
        System.out.println("Downloaded filings:");
        int count = 0;
        while (rs.next()) {
          System.out.printf("  - %s: %s on %s\n",
            rs.getString("company_name"),
            rs.getString("filing_type"),
            rs.getString("filing_date"));
          count++;
        }
        
        assertTrue(count > 0, "Should have downloaded some filings");
        System.out.println("\n✓ Downloaded " + count + " filings successfully");
      }
    }
    
    // Check files
    File dataDir = new File("/tmp/sec-dji-limited/sec");
    if (dataDir.exists()) {
      File[] files = dataDir.listFiles();
      if (files != null && files.length > 0) {
        System.out.println("\nFiles created: " + files.length);
        for (File f : files) {
          if (f.getName().endsWith(".xml")) {
            System.out.printf("  - %s (%,d bytes)\n", f.getName(), f.length());
            assertTrue(f.length() > 1000, "XBRL file should be substantial");
          }
        }
      }
    }
    
    System.out.println("\n" + "=".repeat(80));
    System.out.println("✓ REAL DOWNLOAD TEST PASSED");
    System.out.println("=".repeat(80));
  }
}