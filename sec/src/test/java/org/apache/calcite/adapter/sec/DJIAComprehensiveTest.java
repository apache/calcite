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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Comprehensive integration test for DJIA companies.
 * Downloads 10 years of filings and runs cross-company queries.
 */
@Tag("integration")
public class DJIAComprehensiveTest {

  private static String testDataDir;
  private static String modelPath;

  @BeforeAll
  public static void setUp() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("DJIA COMPREHENSIVE INTEGRATION TEST");
    System.out.println("=".repeat(80));

    // Create test directory with timestamp
    String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
    testDataDir = "build/test-data/DJIAComprehensive_" + timestamp;
    new File(testDataDir).mkdirs();

    System.out.println("Test data directory: " + testDataDir);
    System.out.println("Start time: " + LocalDateTime.now());

    // Create model file using _DJIA group - minimal config with only required/non-default values
    String modelJson = String.format("{\n"
  +
      "  \"version\": \"1.0\",\n"
  +
      "  \"defaultSchema\": \"SEC\",\n"
  +
      "  \"schemas\": [{\n"
  +
      "    \"name\": \"SEC\",\n"
  +
      "    \"type\": \"custom\",\n"
  +
      "    \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\",\n"
  +
      "    \"operand\": {\n"
  +
      "      \"directory\": \"%s\",\n"
  +
      "      \"ciks\": [\"_DJIA\"],\n"
  +
      "      \"startYear\": 2014,\n"
  +
      "      \"endYear\": 2024,\n"
  +
      "      \"autoDownload\": true\n"
  +
      "    }\n"
  +
      "  }]\n"
  +
      "}", testDataDir);

    modelPath = testDataDir + "/model.json";
    try (FileWriter writer = new FileWriter(modelPath)) {
      writer.write(modelJson);
    }

    // Ensure absolute path
    File modelFile = new File(modelPath);
    modelPath = modelFile.getAbsolutePath();

    System.out.println("Model file created: " + modelPath);
    System.out.println("Configured for:");
    System.out.println("  - Companies: _DJIA group (Dow Jones Industrial Average - 30 companies)");
    System.out.println("  - Filing types: 10-K, 10-Q, 8-K (default)");
    System.out.println("  - Date range: 2014-2024 (10 years)");
    System.out.println("  - Auto download: enabled");
    System.out.println();
  }

  @Test public void testComprehensiveDJIAAnalysis() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("PHASE 1: ESTABLISHING CONNECTION AND DOWNLOADING DATA");
    System.out.println("=".repeat(80));

    Properties props = new Properties();
    // Apply engine defaults for proper casing configuration
    props.put("lex", "ORACLE");
    props.put("unquotedCasing", "TO_LOWER");

    long startTime = System.currentTimeMillis();

    try (Connection conn =
            DriverManager.getConnection("jdbc:calcite:model=" + modelPath, props)) {

      System.out.println("Connection established successfully");

      // The download happens during connection establishment due to autoDownload=true
      // Let's verify the schema is available
      try (Statement stmt = conn.createStatement()) {

        System.out.println("\n"
  + "=".repeat(80));
        System.out.println("PHASE 2: VERIFYING DATA AVAILABILITY");
        System.out.println("=".repeat(80));

        // Check what tables are available
        ResultSet rs =
            stmt.executeQuery("SELECT \"TABLE_NAME\" FROM information_schema.tables " +
            "WHERE \"TABLE_SCHEMA\" = 'SEC' ORDER BY \"TABLE_NAME\"");

        System.out.println("\nAvailable tables:");
        int tableCount = 0;
        while (rs.next()) {
          System.out.println("  - " + rs.getString("TABLE_NAME"));
          tableCount++;
        }
        rs.close();
        System.out.println("Total tables: " + tableCount);

        System.out.println("\n"
  + "=".repeat(80));
        System.out.println("PHASE 3: RUNNING ANALYTICS QUERIES");
        System.out.println("=".repeat(80));

        // Query 1: Count total filings by company
        System.out.println("\n1. Total filings by company:");
        rs =
            stmt.executeQuery("SELECT cik, COUNT(*) as filing_count " +
            "FROM sec_filings " +
            "GROUP BY cik " +
            "ORDER BY filing_count DESC " +
            "LIMIT 10");

        while (rs.next()) {
          System.out.printf("   CIK %s: %d filings\n",
              rs.getString("cik"), rs.getInt("filing_count"));
        }
        rs.close();

        // Query 2: Filings by type
        System.out.println("\n2. Filing distribution by type:");
        rs =
            stmt.executeQuery("SELECT filing_type, COUNT(*) as count " +
            "FROM sec_filings " +
            "GROUP BY filing_type " +
            "ORDER BY count DESC");

        while (rs.next()) {
          System.out.printf("   %s: %d filings\n",
              rs.getString("filing_type"), rs.getInt("count"));
        }
        rs.close();

        // Query 3: Filings by year
        System.out.println("\n3. Filing distribution by year:");
        rs =
            stmt.executeQuery("SELECT EXTRACT(YEAR FROM filing_date) as year, COUNT(*) as count " +
            "FROM sec_filings " +
            "GROUP BY EXTRACT(YEAR FROM filing_date) " +
            "ORDER BY year DESC");

        while (rs.next()) {
          System.out.printf("   %d: %d filings\n",
              rs.getInt("year"), rs.getInt("count"));
        }
        rs.close();

        // Query 4: Recent 10-K filings
        System.out.println("\n4. Most recent 10-K filings:");
        rs =
            stmt.executeQuery("SELECT cik, filing_date, accession_number " +
            "FROM sec_filings " +
            "WHERE filing_type = '10K' " +
            "ORDER BY filing_date DESC " +
            "LIMIT 10");

        while (rs.next()) {
          System.out.printf("   CIK %s: %s (Accession: %s)\n",
              rs.getString("cik"),
              rs.getDate("filing_date"),
              rs.getString("accession_number"));
        }
        rs.close();

        // Query 5: Cross-company analysis - companies with most 8-Ks
        System.out.println("\n5. Companies with most 8-K filings (material events):");
        rs =
            stmt.executeQuery("SELECT cik, COUNT(*) as event_count " +
            "FROM sec_filings " +
            "WHERE filing_type = '8K' " +
            "GROUP BY cik " +
            "ORDER BY event_count DESC " +
            "LIMIT 10");

        while (rs.next()) {
          System.out.printf("   CIK %s: %d material events\n",
              rs.getString("cik"), rs.getInt("event_count"));
        }
        rs.close();

        // Query 6: Financial facts if available
        System.out.println("\n6. Attempting to query financial facts:");
        try {
          rs =
              stmt.executeQuery("SELECT COUNT(*) as fact_count FROM xbrl_facts");
          if (rs.next()) {
            System.out.println("   Total XBRL facts: " + rs.getInt("fact_count"));
          }
          rs.close();

          // Sample financial metrics
          rs =
              stmt.executeQuery("SELECT cik, concept, COUNT(*) as occurrences " +
              "FROM xbrl_facts " +
              "WHERE concept LIKE '%Revenue%' OR concept LIKE '%NetIncome%' " +
              "GROUP BY cik, concept " +
              "ORDER BY occurrences DESC " +
              "LIMIT 10");

          System.out.println("\n   Top financial metrics found:");
          while (rs.next()) {
            System.out.printf("   CIK %s - %s: %d occurrences\n",
                rs.getString("cik"),
                rs.getString("concept"),
                rs.getInt("occurrences"));
          }
          rs.close();

        } catch (Exception e) {
          System.out.println("   Financial facts table not available or empty");
        }

        System.out.println("\n"
  + "=".repeat(80));
        System.out.println("PHASE 4: TEST SUMMARY");
        System.out.println("=".repeat(80));

        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime) / 1000; // seconds

        System.out.println("Test completed successfully!");
        System.out.println("Total execution time: " + duration + " seconds (" + (duration/60) + " minutes)");
        System.out.println("End time: " + LocalDateTime.now());

        // Final verification query
        rs = stmt.executeQuery("SELECT COUNT(*) as total FROM sec_filings");
        if (rs.next()) {
          int totalFilings = rs.getInt("total");
          System.out.println("Total filings processed: " + totalFilings);

          // Assert we got a meaningful amount of data
          assertTrue(totalFilings > 0, "Should have downloaded filings");

          // For 29 companies over 10 years with 3 filing types,
          // we expect at least several hundred filings
          assertTrue(totalFilings > 100,
              "Should have substantial number of filings for DJIA companies");
        }
        rs.close();
      }

      System.out.println("\n✅ All queries executed successfully!");
      System.out.println("✅ Cross-company analysis completed!");

    } catch (Exception e) {
      System.err.println("Error during test execution: " + e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  @AfterAll
  public static void tearDown() {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("TEST CLEANUP");
    System.out.println("=".repeat(80));
    System.out.println("Test data preserved in: " + testDataDir);
    System.out.println("To clean up manually, run: rm -rf " + testDataDir);
  }
}
