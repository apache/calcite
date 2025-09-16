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
 * Test specifically for the new consumer sentiment indices added to FRED integration.
 */
@Tag("integration")
public class ConsumerSentimentTest {
  
  @TempDir
  Path tempDir;
  
  @Test
  public void testConsumerSentimentDownload() throws Exception {
    String apiKey = System.getenv("FRED_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(), 
        "FRED_API_KEY not set, skipping consumer sentiment test");
    
    FredDataDownloader downloader = new FredDataDownloader(tempDir.toString(), apiKey);
    
    // Test all 4 consumer sentiment indices
    File parquetFile = downloader.downloadEconomicIndicators(
        Arrays.asList(
            FredDataDownloader.Series.CONSUMER_SENTIMENT,
            FredDataDownloader.Series.REAL_DISPOSABLE_INCOME,
            FredDataDownloader.Series.CONSUMER_CONFIDENCE,
            FredDataDownloader.Series.PERSONAL_SAVING_RATE
        ),
        "2023-01-01", "2023-06-01"); // Just 6 months for faster testing
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    System.out.println("Consumer sentiment Parquet file: " + parquetFile.getAbsolutePath());
    System.out.println("File size: " + parquetFile.length() + " bytes");
    
    // Verify the Parquet file contains consumer sentiment data
    verifyConsumerSentimentParquet(parquetFile);
  }
  
  /**
   * Verifies that the consumer sentiment Parquet file contains expected data.
   */
  private void verifyConsumerSentimentParquet(File parquetFile) throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        // Check row count
        String query = String.format(
            "SELECT COUNT(*) as row_count FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          int rowCount = rs.getInt("row_count");
          assertTrue(rowCount > 0, "Consumer sentiment Parquet should contain data");
          System.out.println("Consumer sentiment: Found " + rowCount + " rows");
        }
        
        // Verify schema has expected columns
        query = String.format(
            "DESCRIBE SELECT * FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        boolean foundSeriesId = false;
        boolean foundValue = false;
        boolean foundDate = false;
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Consumer sentiment schema:");
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
        
        // Check that we have data for our consumer sentiment indices
        query = String.format(
            "SELECT series_id, COUNT(*) as count FROM read_parquet('%s') GROUP BY series_id",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Consumer sentiment by series:");
          boolean foundConsumerSentimentMetric = false;
          while (rs.next()) {
            String seriesId = rs.getString("series_id");
            int count = rs.getInt("count");
            System.out.printf("  %s: %d observations%n", seriesId, count);
            
            // Check if this is one of our consumer sentiment indices
            if ("UMCSENT".equals(seriesId) || "DSPIC96".equals(seriesId) || 
                "CSCICP03USM665S".equals(seriesId) || "PSAVERT".equals(seriesId)) {
              foundConsumerSentimentMetric = true;
            }
          }
          assertTrue(foundConsumerSentimentMetric, "Expected at least one consumer sentiment index in the data");
        }
      }
    }
  }
}