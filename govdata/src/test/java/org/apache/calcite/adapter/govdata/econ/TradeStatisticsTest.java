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
 * Test for BEA trade statistics (Table 125) integration.
 * Tests exports and imports data with category breakdown and trade balance calculations.
 */
@Tag("integration")
public class TradeStatisticsTest {
  
  @TempDir
  Path tempDir;
  
  @Test
  public void testTradeStatisticsDownload() throws Exception {
    String apiKey = System.getenv("BEA_API_KEY");
    assumeTrue(apiKey != null && !apiKey.isEmpty(), 
        "BEA_API_KEY not set, skipping trade statistics test");
    
    BeaDataDownloader downloader = new BeaDataDownloader(tempDir.toString(), apiKey);
    
    // Test trade statistics for a 3-year period for faster testing
    File parquetFile = downloader.downloadTradeStatistics(2021, 2023);
    
    assertNotNull(parquetFile);
    assertTrue(parquetFile.exists());
    assertTrue(parquetFile.length() > 0);
    
    System.out.println("Trade statistics Parquet file: " + parquetFile.getAbsolutePath());
    System.out.println("File size: " + parquetFile.length() + " bytes");
    
    // Verify the Parquet file contains trade statistics data
    verifyTradeStatisticsParquet(parquetFile);
  }
  
  /**
   * Verifies that the trade statistics Parquet file contains expected data structure.
   */
  private void verifyTradeStatisticsParquet(File parquetFile) throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:duckdb:")) {
      try (Statement stmt = conn.createStatement()) {
        // Check row count
        String query = String.format(
            "SELECT COUNT(*) as row_count FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          int rowCount = rs.getInt("row_count");
          assertTrue(rowCount > 0, "Trade statistics Parquet should contain data");
          System.out.println("Trade statistics: Found " + rowCount + " rows");
        }
        
        // Verify schema has expected columns
        query = String.format(
            "DESCRIBE SELECT * FROM read_parquet('%s')",
            parquetFile.getAbsolutePath());
        
        boolean foundTableId = false;
        boolean foundTradeType = false;
        boolean foundCategory = false;
        boolean foundTradeBalance = false;
        boolean foundValue = false;
        
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Trade statistics schema:");
          while (rs.next()) {
            String columnName = rs.getString("column_name");
            String columnType = rs.getString("column_type");
            System.out.printf("  %s: %s%n", columnName, columnType);
            
            if ("table_id".equals(columnName)) foundTableId = true;
            else if ("trade_type".equals(columnName)) foundTradeType = true;
            else if ("category".equals(columnName)) foundCategory = true;
            else if ("trade_balance".equals(columnName)) foundTradeBalance = true;
            else if ("value".equals(columnName)) foundValue = true;
          }
        }
        
        assertTrue(foundTableId, "Expected table_id column");
        assertTrue(foundTradeType, "Expected trade_type column");
        assertTrue(foundCategory, "Expected category column");
        assertTrue(foundTradeBalance, "Expected trade_balance column");
        assertTrue(foundValue, "Expected value column");
        
        // Check trade types distribution
        query = String.format(
            "SELECT trade_type, COUNT(*) as count FROM read_parquet('%s') GROUP BY trade_type ORDER BY count DESC",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Trade statistics by type:");
          boolean foundExports = false;
          boolean foundImports = false;
          while (rs.next()) {
            String tradeType = rs.getString("trade_type");
            int count = rs.getInt("count");
            System.out.printf("  %s: %d records%n", tradeType, count);
            
            if ("Exports".equals(tradeType)) foundExports = true;
            else if ("Imports".equals(tradeType)) foundImports = true;
          }
          assertTrue(foundExports, "Expected exports data in trade statistics");
          assertTrue(foundImports, "Expected imports data in trade statistics");
        }
        
        // Check categories distribution
        query = String.format(
            "SELECT category, COUNT(*) as count FROM read_parquet('%s') GROUP BY category ORDER BY count DESC LIMIT 10",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Top trade categories:");
          boolean foundTradeCategories = false;
          while (rs.next()) {
            String category = rs.getString("category");
            int count = rs.getInt("count");
            System.out.printf("  %s: %d records%n", category, count);
            
            // Check for common trade categories
            if ("Goods".equals(category) || "Services".equals(category) || 
                "Food".equals(category) || "Capital Goods".equals(category)) {
              foundTradeCategories = true;
            }
          }
          assertTrue(foundTradeCategories, "Expected recognizable trade categories in the data");
        }
        
        // Verify trade balance calculations
        query = String.format(
            "SELECT trade_type, AVG(trade_balance) as avg_balance, " +
            "MIN(trade_balance) as min_balance, MAX(trade_balance) as max_balance " +
            "FROM read_parquet('%s') GROUP BY trade_type",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          System.out.println("Trade balance statistics:");
          while (rs.next()) {
            String tradeType = rs.getString("trade_type");
            double avgBalance = rs.getDouble("avg_balance");
            double minBalance = rs.getDouble("min_balance");
            double maxBalance = rs.getDouble("max_balance");
            System.out.printf("  %s: avg=%.2f, min=%.2f, max=%.2f%n", 
                tradeType, avgBalance, minBalance, maxBalance);
          }
        }
        
        // Check data quality - ensure we have reasonable values
        query = String.format(
            "SELECT COUNT(*) as valid_values FROM read_parquet('%s') WHERE value > 0",
            parquetFile.getAbsolutePath());
            
        try (ResultSet rs = stmt.executeQuery(query)) {
          assertTrue(rs.next());
          int validValues = rs.getInt("valid_values");
          assertTrue(validValues > 0, "Trade statistics should have positive values");
          System.out.println("Valid positive trade values: " + validValues);
        }
      }
    }
  }
}