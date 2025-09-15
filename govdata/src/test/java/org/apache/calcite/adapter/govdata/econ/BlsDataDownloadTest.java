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

import org.junit.jupiter.api.BeforeAll;
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

/**
 * Test BLS data download and ETL functionality.
 */
@Tag("integration")
public class BlsDataDownloadTest {
  
  private static String blsApiKey;
  
  @TempDir
  Path tempDir;
  
  @BeforeAll
  public static void setUp() {
    // Get API key from environment
    blsApiKey = System.getenv("BLS_API_KEY");
    if (blsApiKey == null) {
      blsApiKey = System.getProperty("bls.api.key");
    }
  }
  
  @Test
  public void testDownloadEmploymentStatistics() throws Exception {
    if (blsApiKey == null) {
      System.out.println("Skipping test - no BLS API key configured");
      return;
    }
    
    BlsDataDownloader downloader = new BlsDataDownloader(blsApiKey, tempDir.toString());
    
    // Download just 2 years of data for testing
    File parquetFile = downloader.downloadEmploymentStatistics(2023, 2024);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    // Verify we can query the Parquet file
    verifyParquetReadable(parquetFile, "employment_statistics");
  }
  
  @Test
  public void testDownloadInflationMetrics() throws Exception {
    if (blsApiKey == null) {
      System.out.println("Skipping test - no BLS API key configured");
      return;
    }
    
    BlsDataDownloader downloader = new BlsDataDownloader(blsApiKey, tempDir.toString());
    
    File parquetFile = downloader.downloadInflationMetrics(2023, 2024);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    verifyParquetReadable(parquetFile, "inflation_metrics");
  }
  
  @Test
  public void testDownloadWageGrowth() throws Exception {
    if (blsApiKey == null) {
      System.out.println("Skipping test - no BLS API key configured");
      return;
    }
    
    BlsDataDownloader downloader = new BlsDataDownloader(blsApiKey, tempDir.toString());
    
    File parquetFile = downloader.downloadWageGrowth(2023, 2024);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    verifyParquetReadable(parquetFile, "wage_growth");
  }
  
  @Test
  public void testDownloadRegionalEmployment() throws Exception {
    if (blsApiKey == null) {
      System.out.println("Skipping test - no BLS API key configured");
      return;
    }
    
    BlsDataDownloader downloader = new BlsDataDownloader(blsApiKey, tempDir.toString());
    
    File parquetFile = downloader.downloadRegionalEmployment(2023, 2024);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    verifyParquetReadable(parquetFile, "regional_employment");
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
          while (rs.next()) {
            String columnName = rs.getString("column_name");
            String columnType = rs.getString("column_type");
            System.out.printf("  %s: %s%n", columnName, columnType);
          }
        }
      }
    }
  }
}