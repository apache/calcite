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
 * Test specifically for the new real estate metrics added to FRED integration.
 */
@Tag("integration")
public class RealEstateMetricsTest {
  
  @TempDir
  Path tempDir;
  
  @Test
  public void testRealEstateMetricsDownload() throws Exception {
    String apiKey = System.getenv("FRED_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(), 
        "FRED_API_KEY not set, skipping real estate metrics test");
    
    FredDataDownloader downloader = new FredDataDownloader(tempDir.toString(), apiKey);
    
    // Test all 4 real estate metrics
    File parquetFile = downloader.downloadEconomicIndicators(
        Arrays.asList(
            FredDataDownloader.Series.BUILDING_PERMITS,
            FredDataDownloader.Series.MEDIAN_HOME_PRICE,
            FredDataDownloader.Series.RENTAL_VACANCY_RATE,
            FredDataDownloader.Series.SINGLE_UNIT_HOUSING_STARTS
        ),
        "2023-01-01", "2023-06-01"); // Just 6 months for faster testing
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    System.out.println("Real estate metrics Parquet file: " + parquetFile.getAbsolutePath());
    System.out.println("File size: " + parquetFile.length() + " bytes");
    
    // Verify the Parquet file contains real estate data
    verifyRealEstateMetricsParquet(parquetFile);
  }
  
  /**
   * Verifies that the real estate metrics Parquet file contains expected data.
   */
  private void verifyRealEstateMetricsParquet(File parquetFile) throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        // Check row count
        String query = String.format(
            "SELECT COUNT(*) as row_count FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          int rowCount = rs.getInt("row_count");
          assertTrue(rowCount > 0, "Real estate metrics Parquet should contain data");
          System.out.println("Real estate metrics: Found " + rowCount + " rows");
        }
        
        // Verify schema has expected columns
        query = String.format(
            "DESCRIBE SELECT * FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        boolean foundSeriesId = false;
        boolean foundValue = false;
        boolean foundDate = false;
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Real estate metrics schema:");
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
        
        // Check that we have data for our real estate metrics
        query = String.format(
            "SELECT series_id, COUNT(*) as count FROM read_parquet('%s') GROUP BY series_id",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Real estate metrics by series:");
          boolean foundRealEstateMetric = false;
          while (rs.next()) {
            String seriesId = rs.getString("series_id");
            int count = rs.getInt("count");
            System.out.printf("  %s: %d observations%n", seriesId, count);
            
            // Check if this is one of our real estate metrics
            if ("PERMIT".equals(seriesId) || "MSPUS".equals(seriesId) || 
                "RRVRUSQ156N".equals(seriesId) || "HOUST1F".equals(seriesId)) {
              foundRealEstateMetric = true;
            }
          }
          assertTrue(foundRealEstateMetric, "Expected at least one real estate metric in the data");
        }
      }
    }
  }
}