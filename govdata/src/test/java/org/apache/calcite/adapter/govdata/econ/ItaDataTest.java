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
 * Test for BEA International Transactions Accounts (ITA) data integration.
 * Tests comprehensive trade balance and current account data.
 */
@Tag("integration")
public class ItaDataTest {
  
  @TempDir
  Path tempDir;
  
  @Test
  public void testItaDataDownload() throws Exception {
    String apiKey = System.getenv("BEA_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(), 
        "BEA_API_KEY not set, skipping ITA data test");
    
    BeaDataDownloader downloader = new BeaDataDownloader(tempDir.toString(), apiKey);
    
    // Test ITA data for a 2-year period for faster testing
    File parquetFile = downloader.downloadItaData(2022, 2023);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    System.out.println("ITA data Parquet file: " + parquetFile.getAbsolutePath());
    System.out.println("File size: " + parquetFile.length() + " bytes");
    
    // Verify the Parquet file contains ITA data
    verifyItaDataParquet(parquetFile);
  }
  
  /**
   * Verifies that the ITA data Parquet file contains expected data structure.
   */
  private void verifyItaDataParquet(File parquetFile) throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        // Check row count
        String query = String.format(
            "SELECT COUNT(*) as row_count FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          int rowCount = rs.getInt("row_count");
          assertTrue(rowCount > 0, "ITA data Parquet should contain data");
          System.out.println("ITA data: Found " + rowCount + " rows");
        }
        
        // Verify schema has expected columns
        query = String.format(
            "DESCRIBE SELECT * FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        boolean foundIndicator = false;
        boolean foundIndicatorDesc = false;
        boolean foundValue = false;
        boolean foundYear = false;
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("ITA data schema:");
          while (rs.next()) {
            String columnName = rs.getString("column_name");
            String columnType = rs.getString("column_type");
            System.out.printf("  %s: %s%n", columnName, columnType);
            
            if ("indicator".equals(columnName)) foundIndicator = true;
            else if ("indicator_description".equals(columnName)) foundIndicatorDesc = true;
            else if ("value".equals(columnName)) foundValue = true;
            else if ("year".equals(columnName)) foundYear = true;
          }
        }
        
        assertTrue(foundIndicator, "Expected indicator column");
        assertTrue(foundIndicatorDesc, "Expected indicator_description column");
        assertTrue(foundValue, "Expected value column");
        assertTrue(foundYear, "Expected year column");
        
        // Check indicators distribution
        query = String.format(
            "SELECT indicator, indicator_description, COUNT(*) as count FROM read_parquet('%s') GROUP BY indicator, indicator_description ORDER BY count DESC",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("ITA indicators:");
          boolean foundBalanceIndicators = false;
          while (rs.next()) {
            String indicator = rs.getString("indicator");
            String description = rs.getString("indicator_description");
            int count = rs.getInt("count");
            System.out.printf("  %s (%s): %d records%n", indicator, description, count);
            
            // Check for key balance indicators
            if (indicator.startsWith("Bal")) {
              foundBalanceIndicators = true;
            }
          }
          assertTrue(foundBalanceIndicators, "Expected balance indicators in ITA data");
        }
        
        // Verify data quality - check for reasonable trade balance values
        query = String.format(
            "SELECT indicator, AVG(value) as avg_value, MIN(value) as min_value, MAX(value) as max_value " +
            "FROM read_parquet('%s') WHERE indicator = 'BalGdsServ' GROUP BY indicator",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Trade balance statistics:");
          while (rs.next()) {
            String indicator = rs.getString("indicator");
            double avgValue = rs.getDouble("avg_value");
            double minValue = rs.getDouble("min_value");
            double maxValue = rs.getDouble("max_value");
            System.out.printf("  %s: avg=%.0f, min=%.0f, max=%.0f (USD millions)%n", 
                indicator, avgValue, minValue, maxValue);
          }
        }
        
        // Check time series coverage
        query = String.format(
            "SELECT year, COUNT(DISTINCT indicator) as indicators FROM read_parquet('%s') GROUP BY year ORDER BY year",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Year coverage:");
          while (rs.next()) {
            int year = rs.getInt("year");
            int indicators = rs.getInt("indicators");
            System.out.printf("  %d: %d indicators%n", year, indicators);
          }
        }
      }
    }
  }
}