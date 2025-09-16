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
package org.apache.calcite.adapter.govdata.econ;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test for BEA GDP by Industry data integration.
 * Tests value added by NAICS industry classification with both
 * annual and quarterly frequency data.
 */
@Tag("integration")
public class IndustryGdpTest {
  
  @TempDir
  Path tempDir;
  
  @Test
  public void testIndustryGdpDownload() throws Exception {
    String apiKey = System.getenv("BEA_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(), 
        "BEA_API_KEY not set, skipping industry GDP test");
    
    BeaDataDownloader downloader = new BeaDataDownloader(tempDir.toString(), apiKey);
    
    // Test industry GDP data for a 2-year period
    File parquetFile = downloader.downloadIndustryGdp(2022, 2023);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    System.out.println("Industry GDP Parquet file: " + parquetFile.getAbsolutePath());
    System.out.println("File size: " + parquetFile.length() + " bytes");
    
    // Verify the Parquet file contains industry GDP data
    verifyIndustryGdpParquet(parquetFile);
  }
  
  /**
   * Verifies that the industry GDP Parquet file contains expected data structure.
   */
  private void verifyIndustryGdpParquet(File parquetFile) throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        // Check row count
        String query = String.format(
            "SELECT COUNT(*) as row_count FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          int rowCount = rs.getInt("row_count");
          assertTrue(rowCount > 0, "Industry GDP Parquet should contain data");
          System.out.println("Industry GDP: Found " + rowCount + " rows");
        }
        
        // Verify schema has expected columns
        query = String.format(
            "DESCRIBE SELECT * FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        boolean foundTableId = false;
        boolean foundFrequency = false;
        boolean foundYear = false;
        boolean foundQuarter = false;
        boolean foundIndustryCode = false;
        boolean foundIndustryDesc = false;
        boolean foundValue = false;
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Industry GDP schema:");
          while (rs.next()) {
            String columnName = rs.getString("column_name");
            String columnType = rs.getString("column_type");
            System.out.printf("  %s: %s%n", columnName, columnType);
            
            if ("table_id".equals(columnName)) foundTableId = true;
            else if ("frequency".equals(columnName)) foundFrequency = true;
            else if ("year".equals(columnName)) foundYear = true;
            else if ("quarter".equals(columnName)) foundQuarter = true;
            else if ("industry_code".equals(columnName)) foundIndustryCode = true;
            else if ("industry_description".equals(columnName)) foundIndustryDesc = true;
            else if ("value".equals(columnName)) foundValue = true;
          }
        }
        
        assertTrue(foundTableId, "Expected table_id column");
        assertTrue(foundFrequency, "Expected frequency column");
        assertTrue(foundYear, "Expected year column");
        assertTrue(foundQuarter, "Expected quarter column");
        assertTrue(foundIndustryCode, "Expected industry_code column");
        assertTrue(foundIndustryDesc, "Expected industry_description column");
        assertTrue(foundValue, "Expected value column");
        
        // Check frequency distribution
        query = String.format(
            "SELECT frequency, COUNT(*) as count FROM read_parquet('%s') GROUP BY frequency ORDER BY count DESC",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Data by frequency:");
          boolean foundAnnual = false;
          boolean foundQuarterly = false;
          while (rs.next()) {
            String frequency = rs.getString("frequency");
            int count = rs.getInt("count");
            System.out.printf("  %s: %d records%n", frequency, count);
            
            if ("A".equals(frequency)) foundAnnual = true;
            else if ("Q".equals(frequency)) foundQuarterly = true;
          }
          assertTrue(foundAnnual, "Expected annual frequency data");
          // Quarterly data may not always be available
        }
        
        // Check industry distribution
        query = String.format(
            "SELECT industry_code, industry_description, COUNT(*) as count FROM read_parquet('%s') " +
            "GROUP BY industry_code, industry_description ORDER BY industry_code LIMIT 10",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Sample industries:");
          boolean foundNAICS = false;
          while (rs.next()) {
            String code = rs.getString("industry_code");
            String desc = rs.getString("industry_description");
            int count = rs.getInt("count");
            System.out.printf("  %s: %s (%d records)%n", code, desc, count);
            
            // Check for NAICS codes
            if (code.matches("\\d+.*") || code.matches("[A-Z0-9]+")) {
              foundNAICS = true;
            }
          }
          assertTrue(foundNAICS, "Expected NAICS industry codes");
        }
        
        // Verify value ranges
        query = String.format(
            "SELECT MIN(value) as min_value, MAX(value) as max_value, AVG(value) as avg_value " +
            "FROM read_parquet('%s') WHERE value > 0",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          if (rs.next()) {
            double minValue = rs.getDouble("min_value");
            double maxValue = rs.getDouble("max_value");
            double avgValue = rs.getDouble("avg_value");
            System.out.printf("Value statistics: min=%.2f, max=%.2f, avg=%.2f (billions)%n", 
                minValue, maxValue, avgValue);
            
            assertTrue(minValue > 0, "Should have positive GDP values");
            assertTrue(maxValue > minValue, "Should have value range");
          }
        }
        
        // Check year coverage
        query = String.format(
            "SELECT year, COUNT(DISTINCT industry_code) as industries FROM read_parquet('%s') " +
            "GROUP BY year ORDER BY year",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Year coverage:");
          while (rs.next()) {
            int year = rs.getInt("year");
            int industries = rs.getInt("industries");
            System.out.printf("  %d: %d industries%n", year, industries);
          }
        }
        
        // Check for key sectors
        query = String.format(
            "SELECT DISTINCT industry_description FROM read_parquet('%s') " +
            "WHERE industry_description LIKE '%%Manufacturing%%' " +
            "OR industry_description LIKE '%%Finance%%' " +
            "OR industry_description LIKE '%%Technology%%' " +
            "OR industry_description LIKE '%%Information%%' " +
            "LIMIT 5",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Key sectors found:");
          int sectorCount = 0;
          while (rs.next()) {
            String desc = rs.getString("industry_description");
            System.out.println("  - " + desc);
            sectorCount++;
          }
          assertTrue(sectorCount > 0, "Should find key economic sectors");
        }
      }
    }
  }
}