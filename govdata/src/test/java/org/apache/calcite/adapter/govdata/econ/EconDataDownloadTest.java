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
 * Comprehensive test for all economic data downloaders.
 * Tests BLS, Treasury, World Bank, FRED, and BEA data downloads.
 */
@Tag("integration")
public class EconDataDownloadTest {
  
  @TempDir
  Path tempDir;
  
  @Test
  public void testBlsEmploymentStatistics() throws Exception {
    String apiKey = System.getenv("BLS_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(), 
        "BLS_API_KEY not set, skipping BLS test");
    
    BlsDataDownloader downloader = new BlsDataDownloader(tempDir.toString(), apiKey);
    
    // Download just 1 year of employment data for testing
    File parquetFile = downloader.downloadEmploymentStatistics(2023, 2024);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    verifyParquetReadable(parquetFile, "employment_statistics");
  }
  
  @Test
  public void testTreasuryYields() throws Exception {
    TreasuryDataDownloader downloader = new TreasuryDataDownloader(tempDir.toString());
    
    // Download just 1 year of data for testing
    File parquetFile = downloader.downloadTreasuryYields(2023, 2024);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    verifyParquetReadable(parquetFile, "treasury_yields");
  }
  
  @Test
  public void testFederalDebt() throws Exception {
    TreasuryDataDownloader downloader = new TreasuryDataDownloader(tempDir.toString());
    
    File parquetFile = downloader.downloadFederalDebt(2023, 2024);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    verifyParquetReadable(parquetFile, "federal_debt");
  }
  
  @Test
  public void testWorldBankIndicators() throws Exception {
    WorldBankDataDownloader downloader = new WorldBankDataDownloader(tempDir.toString());
    
    // Download just 2 years for G7 countries
    File parquetFile = downloader.downloadWorldIndicators(2022, 2023);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    verifyParquetReadable(parquetFile, "world_indicators");
  }
  
  @Test
  public void testWorldBankGlobalGDP() throws Exception {
    WorldBankDataDownloader downloader = new WorldBankDataDownloader(tempDir.toString());
    
    // Download just 1 year of GDP data
    File parquetFile = downloader.downloadGlobalGDP(2023, 2023);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    verifyParquetReadable(parquetFile, "global_gdp");
  }
  
  @Test
  public void testFredEconomicIndicators() throws Exception {
    String apiKey = System.getenv("FRED_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(), 
        "FRED_API_KEY not set, skipping FRED test");
    
    FredDataDownloader downloader = new FredDataDownloader(tempDir.toString(), apiKey);
    
    // Download just a few key indicators for 1 year
    File parquetFile = downloader.downloadEconomicIndicators(
        Arrays.asList(FredDataDownloader.Series.FED_FUNDS_RATE, 
                     FredDataDownloader.Series.UNEMPLOYMENT_RATE,
                     FredDataDownloader.Series.CPI_ALL_URBAN),
        "2023-01-01", "2024-01-01");
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    verifyParquetReadable(parquetFile, "fred_indicators");
  }
  
  @Test
  public void testBeaGdpComponents() throws Exception {
    String apiKey = System.getenv("BEA_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(), 
        "BEA_API_KEY not set, skipping BEA test");
    
    BeaDataDownloader downloader = new BeaDataDownloader(tempDir.toString(), apiKey);
    
    // Download just 1 year of GDP components
    File parquetFile = downloader.downloadGdpComponents(2023, 2023);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    verifyParquetReadable(parquetFile, "gdp_components");
  }
  
  @Test
  public void testBeaRegionalIncome() throws Exception {
    String apiKey = System.getenv("BEA_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(), 
        "BEA_API_KEY not set, skipping BEA regional test");
    
    BeaDataDownloader downloader = new BeaDataDownloader(tempDir.toString(), apiKey);
    
    // Download just 1 year of regional income data
    File parquetFile = downloader.downloadRegionalIncome(2023, 2023);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    verifyParquetReadable(parquetFile, "regional_income");
  }
  
  /**
   * Verifies that a Parquet file can be read using DuckDB.
   */
  private void verifyParquetReadable(File parquetFile, String expectedTable) throws Exception {
    // Use DuckDB to verify the Parquet file is readable
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        // Query the Parquet file
        String query = String.format(
            "SELECT COUNT(*) as row_count FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          int rowCount = rs.getInt("row_count");
          assertTrue(rowCount > 0, "Parquet file should contain data");
          System.out.printf("%s: Found %d rows in Parquet file%n", expectedTable, rowCount);
        }
        
        // Verify schema
        query = String.format(
            "DESCRIBE SELECT * FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.printf("%s schema:%n", expectedTable);
          boolean foundExpectedColumns = false;
          while (rs.next()) {
            String columnName = rs.getString("column_name");
            String columnType = rs.getString("column_type");
            System.out.printf("  %s: %s%n", columnName, columnType);
            
            // Check for expected columns based on table type
            if ("employment_statistics".equals(expectedTable) && "unemployment_rate".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("treasury_yields".equals(expectedTable) && "yield_percent".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("federal_debt".equals(expectedTable) && "amount_billions".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("world_indicators".equals(expectedTable) && "country_code".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("fred_indicators".equals(expectedTable) && "series_id".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("gdp_components".equals(expectedTable) && "line_description".equals(columnName)) {
              foundExpectedColumns = true;
            } else if ("regional_income".equals(expectedTable) && "geo_fips".equals(columnName)) {
              foundExpectedColumns = true;
            }
          }
          assertTrue(foundExpectedColumns, "Expected columns not found in " + expectedTable);
        }
      }
    }
  }
}