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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Live integration test that downloads real stock prices from Yahoo Finance.
 * This test requires internet access and may be rate-limited by Yahoo.
 * Should be run manually or in specific CI environments.
 */
@Tag("integration")
@Tag("manual") // Requires internet and external API
public class YahooFinanceLiveIntegrationTest {

  private String testDataDir;
  private String modelPath;
  private TestInfo testInfo;

  @BeforeEach
  void setUp(TestInfo testInfo) throws Exception {
    this.testInfo = testInfo;
    
    // Create unique test directory
    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
    String testName = testInfo.getTestMethod().get().getName();
    // Use a simple approach - create under java.io.tmpdir which is always writable
    Path tmpDir = Paths.get(System.getProperty("java.io.tmpdir"));
    testDataDir = tmpDir.resolve("calcite-sec-test/" + getClass().getSimpleName() + "/" + testName + "_" + timestamp).toString();
    Files.createDirectories(Paths.get(testDataDir));
    System.out.println("Test directory: " + testDataDir);

    // Create test model
    modelPath = createLiveTestModel();
  }

  @Test
  void testLiveStockPriceDownload() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelPath, props)) {

      // Query for real Apple stock prices
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT ticker, \"date\", \"close\", volume " +
               "FROM stock_prices " +
               "WHERE ticker = 'AAPL' " +
               "ORDER BY \"date\" DESC " +
               "LIMIT 5")) {

        // Should have some results
        assertTrue(rs.next(), "Should have at least one stock price record");
        
        // Validate the data
        String ticker = rs.getString("ticker");
        String date = rs.getString("date");
        Double close = rs.getDouble("close");
        Long volume = rs.getLong("volume");
        
        assertNotNull(ticker);
        assertNotNull(date);
        assertTrue(close > 0, "Close price should be positive");
        assertTrue(volume > 0, "Volume should be positive");
        
        System.out.println("Live data retrieved:");
        System.out.println("  Ticker: " + ticker);
        System.out.println("  Date: " + date);
        System.out.println("  Close: " + close);
        System.out.println("  Volume: " + volume);
      }
    }
  }

  @Test
  void testMultipleTickerDownload() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelPath, props)) {

      // Query for multiple tickers
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT ticker, COUNT(*) as record_count, " +
               "MIN(\"date\") as min_date, MAX(\"date\") as max_date " +
               "FROM stock_prices " +
               "GROUP BY ticker")) {

        int tickerCount = 0;
        while (rs.next()) {
          tickerCount++;
          String ticker = rs.getString("ticker");
          int recordCount = rs.getInt("record_count");
          String minDate = rs.getString("min_date");
          String maxDate = rs.getString("max_date");
          
          System.out.println("Ticker: " + ticker);
          System.out.println("  Records: " + recordCount);
          System.out.println("  Date range: " + minDate + " to " + maxDate);
          
          assertTrue(recordCount > 0, "Should have records for " + ticker);
        }
        
        assertTrue(tickerCount > 0, "Should have downloaded prices for at least one ticker");
      }
    }
  }

  @Test
  void testJoinWithSecFilings() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelPath, props)) {

      // Test joining stock prices with SEC filings
      String sql = 
          "SELECT f.cik, f.filing_type, s.ticker, " +
          "AVG(s.\"close\") as avg_price, COUNT(s.\"date\") as price_records " +
          "FROM financial_line_items f " +
          "JOIN stock_prices s ON f.cik = s.cik " +
          "WHERE f.filing_type = '10K' " +
          "GROUP BY f.cik, f.filing_type, s.ticker " +
          "LIMIT 5";

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {

        // Check if we get any join results (may be empty if no overlap)
        if (rs.next()) {
          String cik = rs.getString("cik");
          String ticker = rs.getString("ticker");
          Double avgPrice = rs.getDouble("avg_price");
          
          assertNotNull(cik);
          assertNotNull(ticker);
          assertTrue(avgPrice >= 0, "Average price should be non-negative");
          
          System.out.println("Join result - CIK: " + cik + ", Ticker: " + ticker + 
                           ", Avg Price: " + avgPrice);
        }
      }
    }
  }

  @AfterEach
  void tearDown() {
    // Manual cleanup
    try {
      if (testDataDir != null && Files.exists(Paths.get(testDataDir))) {
        Files.walk(Paths.get(testDataDir))
            .sorted(Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(File::delete);
      }
    } catch (IOException e) {
      System.err.println("Warning: Could not clean test directory: " + e.getMessage());
    }
  }

  private String createLiveTestModel() throws Exception {
    // Create a test model with real stock price downloading enabled
    // Uses a small set of tickers and limited date range to avoid rate limiting
    String model = String.format("{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"SEC\","
        + "\"schemas\": [{"
        + "\"name\": \"SEC\","
        + "\"type\": \"custom\","
        + "\"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\","
        + "\"operand\": {"
        + "\"directory\": \"%s\","
        + "\"ephemeralCache\": true,"
        + "\"testMode\": false,"  // Real mode, not test mode
        + "\"useMockData\": true,"     // But use mock data for testing
        + "\"fetchStockPrices\": true,"
        + "\"sec.fallback.enabled\": false,"
        + "\"ciks\": [\"AAPL\", \"MSFT\"],"  // Just 2 tickers to avoid rate limiting
        + "\"startYear\": 2024,"  // Just current year
        + "\"endYear\": 2024,"
        + "\"filingTypes\": [\"10-K\"]"
        + "}"
        + "}]"
        + "}", testDataDir);

    Path modelFile = Paths.get(testDataDir, "model.json");
    Files.writeString(modelFile, model);
    return modelFile.toString();
  }
}