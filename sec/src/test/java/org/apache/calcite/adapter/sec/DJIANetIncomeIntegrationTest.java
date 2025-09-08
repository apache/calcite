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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for DJIA net income analysis using complete DJIA companies.
 */
@Tag("integration")
public class DJIANetIncomeIntegrationTest {

  @Test @Timeout(value = 30, unit = TimeUnit.MINUTES) // 30 companies for 2 years
  public void testDJIANetIncomeByYear() throws Exception {
    System.out.println("\n=== DJIA NET INCOME INTEGRATION TEST ===");
    System.out.println("Testing complete DJIA companies for prior 2 years (2023-2024)");

    // Create inline model for DJIA companies
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"sec\","
        + "\"schemas\": [{"
        + "  \"name\": \"sec\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\","
        + "  \"operand\": {"
        + "    \"directory\": \"/Volumes/T9/calcite-sec-cache\","
        + "    \"ciks\": \"_DJIA_CONSTITUENTS\","
        + "    \"filingTypes\": [\"10-K\"],"
        + "    \"autoDownload\": false,"  // Use existing cached data
        + "    \"startYear\": 2023,"
        + "    \"endYear\": 2024,"
        + "    \"executionEngine\": \"parquet\","
        + "    \"testMode\": false"
        + "  }"
        + "}]"
        + "}";

    // Write model to temp file
    java.io.File modelFile = java.io.File.createTempFile("djia-integration-test", ".json");
    modelFile.deleteOnExit();
    java.nio.file.Files.writeString(modelFile.toPath(), modelJson);

    // Connection properties
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection connection = DriverManager.getConnection(jdbcUrl, info)) {

      System.out.println("\n=== STEP 1: Check Available Tables ===");
      try (ResultSet tables = connection.getMetaData().getTables(null, "sec", "%", new String[]{"TABLE"})) {
        while (tables.next()) {
          System.out.println("Found table: " + tables.getString("TABLE_NAME"));
        }
      }

      // First check what data is available
      System.out.println("\n=== STEP 1.5: Check Available Data ===");

      // Debug: Check a sample row to see data types
      String sampleQuery = "SELECT * FROM sec.financial_line_items LIMIT 1";
      try (PreparedStatement sampleStmt = connection.prepareStatement(sampleQuery);
           ResultSet sampleRs = sampleStmt.executeQuery()) {
        if (sampleRs.next()) {
          System.out.println("\nSample row:");
          ResultSetMetaData meta = sampleRs.getMetaData();
          for (int i = 1; i <= meta.getColumnCount(); i++) {
            Object val = sampleRs.getObject(i);
            System.out.println("  [" + i + "] " + meta.getColumnName(i) + " = " + val +
                             " (type: " + (val != null ? val.getClass().getName() : "null") + ")");
          }
        }
      }

      // Debug: Check rows with NetIncomeLoss
      String debugQuery = "SELECT cik, filing_type, \"year\", concept FROM sec.financial_line_items WHERE concept = 'NetIncomeLoss' LIMIT 5";
      System.out.println("\n=== Looking for NetIncomeLoss rows ===");
      System.out.println("Query: " + debugQuery);
      try (PreparedStatement debugStmt = connection.prepareStatement(debugQuery);
           ResultSet debugRs = debugStmt.executeQuery()) {
        int count = 0;
        while (debugRs.next()) {
          count++;
          System.out.println("Row " + count + ": cik=" + debugRs.getString("cik") +
                           ", filing_type=" + debugRs.getString("filing_type") +
                           ", year=" + debugRs.getString("year") +
                           ", concept=" + debugRs.getString("concept"));
        }
        System.out.println("Found " + count + " NetIncomeLoss rows");
      }

      String dataCheckQuery =
          "SELECT " +
          "    COUNT(*) as total_rows, " +
          "    COUNT(DISTINCT cik) as unique_ciks, " +
          "    COUNT(DISTINCT \"year\") as unique_years, " +
          "    MIN(\"year\") as min_year, " +
          "    MAX(\"year\") as max_year " +
          "FROM sec.financial_line_items " +
          "WHERE filing_type = '10K'";

      try (PreparedStatement dataStmt = connection.prepareStatement(dataCheckQuery);
           ResultSet dataRs = dataStmt.executeQuery()) {
        if (dataRs.next()) {
          System.out.println("\nAvailable 10-K Data:");
          System.out.println("  Total rows: " + dataRs.getInt("total_rows"));
          System.out.println("  Unique CIKs: " + dataRs.getInt("unique_ciks"));
          System.out.println("  Unique years: " + dataRs.getInt("unique_years"));
          System.out.println("  Year range: " + dataRs.getObject("min_year") +
                           " to " + dataRs.getObject("max_year"));
        }
      }

      System.out.println("\n=== STEP 2: Query NetIncomeLoss for 10-K Filings ===");

      // Debug: First query without WHERE clause to see all data
      System.out.println("\n=== Debug: Checking all records first ===");
      String allRecordsQuery =
          "SELECT COUNT(*) as total_count " +
          "FROM sec.financial_line_items";

      try (PreparedStatement allStmt = connection.prepareStatement(allRecordsQuery);
           ResultSet allRs = allStmt.executeQuery()) {
        if (allRs.next()) {
          System.out.println("Total records in financial_line_items: " + allRs.getInt("total_count"));
        }
      }

      // Debug: Check distinct concepts
      System.out.println("\n=== Debug: Checking NetIncomeLoss concept presence ===");
      String conceptQuery =
          "SELECT COUNT(*) as concept_count " +
          "FROM sec.financial_line_items " +
          "WHERE concept = 'NetIncomeLoss'";

      try (PreparedStatement conceptStmt = connection.prepareStatement(conceptQuery);
           ResultSet conceptRs = conceptStmt.executeQuery()) {
        if (conceptRs.next()) {
          System.out.println("Records with concept='NetIncomeLoss': " + conceptRs.getInt("concept_count"));
        }
      }

      // Debug: Check filing_type values
      System.out.println("\n=== Debug: Checking filing_type values for NetIncomeLoss ===");
      String filingTypeQuery =
          "SELECT filing_type, COUNT(*) as type_count " +
          "FROM sec.financial_line_items " +
          "WHERE concept = 'NetIncomeLoss' " +
          "GROUP BY filing_type";

      try (PreparedStatement filingTypeStmt = connection.prepareStatement(filingTypeQuery);
           ResultSet filingTypeRs = filingTypeStmt.executeQuery()) {
        while (filingTypeRs.next()) {
          System.out.println("filing_type='" + filingTypeRs.getString("filing_type") +
                           "' count=" + filingTypeRs.getInt("type_count"));
        }
      }

      // First check what the metadata says about the year column
      System.out.println("\n=== Checking column metadata ===");
      try (ResultSet columns = connection.getMetaData().getColumns(null, "sec", "financial_line_items", "%")) {
        while (columns.next()) {
          String colName = columns.getString("COLUMN_NAME");
          String typeName = columns.getString("TYPE_NAME");
          int dataType = columns.getInt("DATA_TYPE");
          System.out.println("Column: " + colName + ", Type: " + typeName + " (JDBC type: " + dataType + ")");
        }
      }

      String netIncomeLossQuery =
          "SELECT " +
          "    cik, " +
          "    filing_type, " +
          "    \"year\", " +
          "    concept, " +
          "    numeric_value " +
          "FROM sec.financial_line_items " +
          "WHERE concept = 'NetIncomeLoss' " +
          "  AND filing_type = '10K' " +
          "ORDER BY cik, \"year\" " +
          "LIMIT 100";

      System.out.println("Executing query: " + netIncomeLossQuery);

      try (PreparedStatement stmt = connection.prepareStatement(netIncomeLossQuery);
           ResultSet rs = stmt.executeQuery()) {

        System.out.println("\nNetIncomeLoss Data for DJIA Companies (10-K only):");
        System.out.println("CIK\t\tFiling\tYear\tConcept\t\t\tValue");
        System.out.println("----------\t------\t----\t-------\t\t\t-----");

        int recordCount = 0;
        while (rs.next()) {
          recordCount++;
          String cik = rs.getString("cik");
          String filingType = rs.getString("filing_type");
          Object year = rs.getObject("year");
          String concept = rs.getString("concept");
          Object value = rs.getObject("numeric_value");

          System.out.printf("%s\t%s\t%s\t%s\t%,d\n",
              cik, filingType, year, concept,
              value != null ? ((Number)value).longValue() : 0);
        }

        System.out.printf("\nTotal NetIncomeLoss records found: %d\n", recordCount);

        System.out.println("\n=== STEP 3: Summary by Company ===");
        String summaryQuery =
            "SELECT " +
            "    cik, " +
            "    COUNT(*) as record_count, " +
            "    COUNT(DISTINCT \"year\") as year_count " +
            "FROM sec.financial_line_items " +
            "WHERE concept = 'NetIncomeLoss' " +
            "  AND filing_type = '10K' " +
            "GROUP BY cik " +
            "ORDER BY cik " +
            "LIMIT 50";

        try (PreparedStatement summaryStmt = connection.prepareStatement(summaryQuery);
             ResultSet summaryRs = summaryStmt.executeQuery()) {

          System.out.println("\nCompany Summary:");
          System.out.println("CIK\t\tRecords\tYears");
          System.out.println("----------\t-------\t-----");

          int companyCount = 0;
          int totalRecords = 0;
          while (summaryRs.next()) {
            companyCount++;
            String cik = summaryRs.getString("cik");
            int records = summaryRs.getInt("record_count");
            int years = summaryRs.getInt("year_count");
            totalRecords += records;

            System.out.printf("%s\t%d\t%d\n", cik, records, years);
          }

          System.out.printf("\nTotal companies with NetIncomeLoss: %d\n", companyCount);
          System.out.printf("Total NetIncomeLoss records across all companies: %d\n", totalRecords);

          assertTrue(recordCount > 0, "Should have at least some NetIncomeLoss records");
          assertTrue(companyCount > 0, "Should have at least some companies with NetIncomeLoss");
        }
      }
    }
  }
}
