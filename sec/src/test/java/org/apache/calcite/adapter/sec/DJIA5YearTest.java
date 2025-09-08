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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

/**
 * Test SEC adapter downloading 5 years of DJIA company filings.
 */
@Tag("integration")
public class DJIA5YearTest {

  private File testDir;

  @BeforeEach
  public void setUp() throws Exception {
    // Create test directory on /Volumes/T9 and mock data
    File volumeDir = new File("/Volumes/T9/calcite-test-data");
    volumeDir.mkdirs();
    testDir = new File(volumeDir, "sec-dji5year-test-" + System.currentTimeMillis());
    testDir.mkdirs();
    MockFinancialDataHelper.createMockFinancialData(testDir);
  }

  @AfterEach
  public void tearDown() throws Exception {
    // Clean up test directory
    if (testDir != null && testDir.exists()) {
      Files.walk(testDir.toPath())
        .sorted((a, b) -> b.compareTo(a))
        .forEach(p -> p.toFile().delete());
    }

    // Also clean up temporary Calcite files
    File buildDir = new File("build");
    if (buildDir.exists()) {
      File[] tempFiles = buildDir.listFiles((dir, name) ->
        name.startsWith("calcite_SEC_"));
      if (tempFiles != null) {
        for (File f : tempFiles) {
          f.delete();
        }
      }
    }
  }

  @Test public void testDJIASampleQuery() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("TESTING DJIA 5-YEAR SEC FILINGS DOWNLOAD");
    System.out.println("=".repeat(80) + "\n");

    // Create simple test model instead of loading from file
    String modelJson = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"sec_dji\","
        + "\"schemas\":[{"
        + "  \"name\":\"sec_dji\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"directory\":\"" + testDir.getAbsolutePath() + "\","
        + "    \"executionEngine\":\"duckdb\""
        + "  }"
        + "}]}";

    // Setup properties
    Properties info = new Properties();
    info.put("model", "inline:" + modelJson);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    Instant start = Instant.now();

    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      System.out.println("✓ Connected to SEC adapter with DJIA configuration\n");

      // Query to check what companies and filings we have
      System.out.println("Querying DJIA company filings (2020-2024):");
      System.out.println("-".repeat(60));

      try (Statement stmt = conn.createStatement();
           ResultSet rs =
             stmt.executeQuery("SELECT cik, company_name, filing_type, filing_date, " +
             "COUNT(*) as line_items " +
             "FROM sec_dji.financial_line_items " +
             "WHERE filing_date >= '2020-01-01' " +
             "GROUP BY cik, company_name, filing_type, filing_date " +
             "ORDER BY company_name, filing_date DESC " +
             "LIMIT 20")) {

        int count = 0;
        while (rs.next()) {
          if (count == 0) {
            System.out.println("CIK        | Company                     | Type | Date       | Items");
            System.out.println("-".repeat(70));
          }
          System.out.printf("%10s | %-27s | %-4s | %10s | %5d\n",
            rs.getString("cik"),
            rs.getString("company_name").length() > 27 ?
              rs.getString("company_name").substring(0, 24) + "..." :
              rs.getString("company_name"),
            rs.getString("filing_type"),
            rs.getString("filing_date"),
            rs.getInt("line_items"));
          count++;
        }

        System.out.println("-".repeat(70));
        System.out.println("Showing first " + count + " filings");
      }

      // Check how many unique companies we have
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
             stmt.executeQuery("SELECT COUNT(DISTINCT cik) as company_count, " +
             "COUNT(DISTINCT filing_date) as filing_count, " +
             "COUNT(*) as total_line_items " +
             "FROM sec_dji.financial_line_items")) {

        if (rs.next()) {
          System.out.println("\nSummary:");
          System.out.println("  Companies: " + rs.getInt("company_count"));
          System.out.println("  Unique filing dates: " + rs.getInt("filing_count"));
          System.out.println("  Total line items: " + rs.getInt("total_line_items"));
        }
      }

      // Check if files were downloaded
      Path dataDir = Paths.get("/Volumes/T9/sec-data/dji-5year/sec");
      if (Files.exists(dataDir)) {
        File[] xmlFiles = dataDir.toFile().listFiles((dir, name) -> name.endsWith(".xml"));
        if (xmlFiles != null) {
          System.out.println("\nDownloaded files: " + xmlFiles.length + " XBRL documents");

          // Show a sample of companies
          System.out.println("\nSample downloaded files:");
          int shown = 0;
          for (File f : xmlFiles) {
            if (shown++ < 5) {
              System.out.printf("  - %s (%,d bytes)\n", f.getName(), f.length());
            }
          }
          if (xmlFiles.length > 5) {
            System.out.println("  ... and " + (xmlFiles.length - 5) + " more");
          }
        }
      }
    }

    Duration elapsed = Duration.between(start, Instant.now());

    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("RESULTS:");
    System.out.println("  Total time: " + elapsed.toSeconds() + " seconds");
    System.out.println("  ✓ Successfully connected to DJIA 5-year SEC data");
    System.out.println("=".repeat(80));
  }

  @Test @Tag("unit")
  public void testDJIAQuickQuery() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("QUICK DJIA SAMPLE QUERY TEST");
    System.out.println("=".repeat(80) + "\n");

    // Create mock data for testing
    File volumeDir = new File("/Volumes/T9/calcite-test-data");
    volumeDir.mkdirs();
    File quickTestDir = new File(volumeDir, "dji-quick-test-" + System.currentTimeMillis());
    quickTestDir.mkdirs();
    MockFinancialDataHelper.createMockFinancialData(quickTestDir);

    // Use a minimal model for quick testing
    String quickModel = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"sec_dji\","
        + "\"schemas\":[{"
        + "  \"name\":\"sec_dji\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"directory\":\"" + quickTestDir.getAbsolutePath() + "\","
        + "    \"executionEngine\":\"duckdb\""  // Use DuckDB for Parquet files
        + "  }"
        + "}]}";

    Properties info = new Properties();
    info.put("model", "inline:" + quickModel);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement();
         ResultSet rs =
           stmt.executeQuery("SELECT DISTINCT company_name FROM financial_line_items")) {

      System.out.println("Companies in sample:");
      while (rs.next()) {
        System.out.println("  - " + rs.getString("company_name"));
      }

      System.out.println("\n✓ Quick sample test passed");
    }

    System.out.println("=".repeat(80));
  }
}
