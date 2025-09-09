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
import org.junit.jupiter.api.Disabled;
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
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for SEC adapter stock price functionality.
 */
@Tag("integration")
public class StockPriceIntegrationTest {

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
    modelPath = createTestModel();
  }

  @Test
  void testStockPriceTableExists() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelPath, props)) {

      // Query information schema to check if stock_prices table exists
      // Using a simpler query to avoid case sensitivity issues
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT * FROM stock_prices WHERE 1=0")) {
        // If we get here without exception, the table exists
        assertNotNull(rs.getMetaData());
      }
    }
  }

  @Test
  void testStockPriceSchema() throws Exception {
    // This test uses mock data created by SecSchemaFactory when testMode=true
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelPath, props)) {

      // First check what columns are in the information schema
      // Note: information_schema columns are uppercase due to unquotedCasing=TO_LOWER
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT \"COLUMN_NAME\", \"DATA_TYPE\" " +
               "FROM information_schema.\"columns\" " +
               "WHERE \"TABLE_NAME\" = 'stock_prices'")) {
        System.out.println("Columns from information_schema:");
        while (rs.next()) {
          System.out.println("  - " + rs.getString("COLUMN_NAME") + " (" + rs.getString("DATA_TYPE") + ")");
        }
      }
      
      // First test with specific columns to verify they work
      // Note: 'date' and 'close' are reserved words so we need to quote them
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT ticker, cik, \"date\", \"close\", volume FROM stock_prices LIMIT 1")) {
        
        // Check if we got any rows and print values
        assertTrue(rs.next(), "Should get at least one row");
        System.out.println("Got a row of data:");
        System.out.println("  ticker: " + rs.getString("ticker"));
        System.out.println("  cik: " + rs.getString("cik"));
        System.out.println("  date: " + rs.getString("date"));
        System.out.println("  close: " + rs.getDouble("close"));
        System.out.println("  volume: " + rs.getLong("volume"));
      }
      
      // Now test with SELECT * to get all columns
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT * FROM stock_prices LIMIT 1")) {

        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        
        System.out.println("\nAll columns from SELECT *:");
        for (int i = 1; i <= columnCount; i++) {
          System.out.println("  Column " + i + ": " + meta.getColumnName(i) + " (" + meta.getColumnTypeName(i) + ")");
        }
        
        // Check we have the expected columns (8 data columns + 2 partition columns = 10)
        assertTrue(columnCount >= 10, "Expected at least 10 columns, got " + columnCount);
        
        // Check column names (may be in different order)
        Set<String> columnNames = new HashSet<>();
        for (int i = 1; i <= columnCount; i++) {
          columnNames.add(meta.getColumnName(i).toLowerCase());
        }
        
        assertTrue(columnNames.contains("ticker"));
        assertTrue(columnNames.contains("cik"));
        assertTrue(columnNames.contains("date"));
        assertTrue(columnNames.contains("open"));
        assertTrue(columnNames.contains("high"));
        assertTrue(columnNames.contains("low"));
        assertTrue(columnNames.contains("close"));
        assertTrue(columnNames.contains("adj_close"));
        assertTrue(columnNames.contains("volume"));
        assertTrue(columnNames.contains("year"));
      }
    }
  }

  @Test
  void testSimpleStockPriceQuery() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelPath, props)) {

      // Just try to count rows
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT COUNT(*) FROM stock_prices")) {
        
        assertTrue(rs.next(), "Should get a count result");
        int count = rs.getInt(1);
        System.out.println("Stock prices row count: " + count);
        assertTrue(count > 0, "Should have at least one row");
      }
    }
  }
  
  @Test
  void testQueryStockPrices() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelPath, props)) {

      // Query stock prices (may be empty if download fails or in test mode)
      // Note: 'date' and 'close' are reserved words
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT ticker, \"date\", \"close\", volume " +
               "FROM stock_prices " +
               "WHERE ticker = 'AAPL' " +
               "LIMIT 10")) {

        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(4, meta.getColumnCount());
        assertEquals("ticker", meta.getColumnName(1).toLowerCase());
        assertEquals("date", meta.getColumnName(2).toLowerCase());
        assertEquals("close", meta.getColumnName(3).toLowerCase());
        assertEquals("volume", meta.getColumnName(4).toLowerCase());
        
        // Note: Results may be empty in test mode
        while (rs.next()) {
          assertNotNull(rs.getString("ticker"));
          assertNotNull(rs.getString("date"));
          // Price and volume can be null
        }
      }
    }
  }

  @Test
  @Disabled("Requires actual Parquet files with data")
  void testJoinStockPricesWithFilings() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection(
            "jdbc:calcite:model=" + modelPath, props)) {

      // Test join query structure (may return empty in test mode)
      String sql = "SELECT f.cik, f.filing_type, s.ticker, s.close " +
          "FROM financial_line_items f " +
          "LEFT JOIN stock_prices s ON f.cik = s.cik " +
          "WHERE f.filing_type = '10K' " +
          "LIMIT 5";

      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(sql)) {

        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(4, meta.getColumnCount());
        
        // Verify column names
        assertEquals("cik", meta.getColumnName(1).toLowerCase());
        assertEquals("filing_type", meta.getColumnName(2).toLowerCase());
        assertEquals("ticker", meta.getColumnName(3).toLowerCase());
        assertEquals("close", meta.getColumnName(4).toLowerCase());
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

  private String createTestModel() throws Exception {
    // Create a test model that uses the real SEC adapter functionality
    // The adapter should download stock prices when fetchStockPrices=true
    // For testing, we can use testMode=true to use mock data
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
        + "\"testMode\": true,"
        + "\"useMockData\": true,"
        + "\"fetchStockPrices\": true,"
        + "\"sec.fallback.enabled\": false,"
        + "\"ciks\": [\"AAPL\"],"
        + "\"startYear\": 2023,"
        + "\"endYear\": 2023,"
        + "\"filingTypes\": [\"10-K\"]"
        + "}"
        + "}]"
        + "}", testDataDir);

    Path modelFile = Paths.get(testDataDir, "model.json");
    Files.writeString(modelFile, model);
    return modelFile.toString();
  }
}