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
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test specifically for the new banking indicators added to FRED integration.
 */
@Tag("integration")
public class BankingIndicatorsTest {
  
  @TempDir
  Path tempDir;
  
  @Test
  public void testBankingIndicatorsDownload() throws Exception {
    String apiKey = System.getenv("FRED_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(), 
        "FRED_API_KEY not set, skipping banking indicators test");
    
    FredDataDownloader downloader = new FredDataDownloader(tempDir.toString(), apiKey);
    
    // Test all 4 banking indicators
    File parquetFile = downloader.downloadEconomicIndicators(
        Arrays.asList(
            FredDataDownloader.Series.COMMERCIAL_BANK_DEPOSITS,
            FredDataDownloader.Series.BANK_CREDIT,
            FredDataDownloader.Series.BANK_LENDING_STANDARDS,
            FredDataDownloader.Series.MORTGAGE_DELINQUENCY_RATE
        ),
        "2023-01-01", "2023-06-01"); // Just 6 months for faster testing
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    System.out.println("Banking indicators Parquet file: " + parquetFile.getAbsolutePath());
    System.out.println("File size: " + parquetFile.length() + " bytes");
    
    // Verify the Parquet file contains banking data
    verifyBankingIndicatorsParquet(parquetFile);
  }
  
  /**
   * Verifies that the banking indicators Parquet file contains expected data.
   */
  private void verifyBankingIndicatorsParquet(File parquetFile) throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        // Check row count
        String query = String.format(
            "SELECT COUNT(*) as row_count FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          int rowCount = rs.getInt("row_count");
          assertTrue(rowCount > 0, "Banking indicators Parquet should contain data");
          System.out.println("Banking indicators: Found " + rowCount + " rows");
        }
        
        // Verify schema has expected columns
        query = String.format(
            "DESCRIBE SELECT * FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        boolean foundSeriesId = false;
        boolean foundValue = false;
        boolean foundDate = false;
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Banking indicators schema:");
          while (rs.next()) {
            String columnName = rs.getString("column_name");
            String columnType = rs.getString("column_type");
            System.out.printf("  %s: %s%n", columnName, columnType);
            
            if ("series_id".equals(columnName)) foundSeriesId = true;
            else if ("value".equals(columnName)) foundValue = true;
            else if ("date".equals(columnName)) foundDate = true;
          }
        }
        
        assertTrue(foundSeriesId, "Expected series_id column");
        assertTrue(foundValue, "Expected value column"); 
        assertTrue(foundDate, "Expected date column");
        
        // Check that we have data for our banking indicators
        query = String.format(
            "SELECT series_id, COUNT(*) as count FROM read_parquet('%s') GROUP BY series_id",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Banking indicators by series:");
          boolean foundBankingIndicator = false;
          while (rs.next()) {
            String seriesId = rs.getString("series_id");
            int count = rs.getInt("count");
            System.out.printf("  %s: %d observations%n", seriesId, count);
            
            // Check if this is one of our banking indicators
            if ("DPSACBW027SBOG".equals(seriesId) || "TOTBKCR".equals(seriesId) || 
                "DRTSCILM".equals(seriesId) || "DRSFRMACBS".equals(seriesId)) {
              foundBankingIndicator = true;
            }
          }
          assertTrue(foundBankingIndicator, "Expected at least one banking indicator in the data");
        }
      }
    }
  }
}