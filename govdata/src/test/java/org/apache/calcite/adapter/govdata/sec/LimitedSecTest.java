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
package org.apache.calcite.adapter.govdata.sec;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Limited integration test for SEC adapter with only 2 CIKs, 2 years, 10-K filings.
 */
@Tag("integration")
public class LimitedSecTest {

  @Test @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void testLimitedSecData() throws Exception {
    System.out.println("\n=== LIMITED SEC DATA TEST ===");
    System.out.println("Testing with Apple (AAPL) and Microsoft (MSFT) for 2022-2023, 10-K only");

    // Create inline model for limited test
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"sec\","
        + "\"schemas\": [{"
        + "  \"name\": \"sec\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\": {"
        + "    \"directory\": \"/Volumes/T9/calcite-sec-cache-limited\","
        + "    \"ciks\": [\"AAPL\", \"MSFT\"],"  // Just Apple and Microsoft
        + "    \"filingTypes\": [\"10-K\"],"      // Just 10-K filings
        + "    \"autoDownload\": true,"
        + "    \"startYear\": 2022,"
        + "    \"endYear\": 2023,"
        + "    \"executionEngine\": \"parquet\","
        + "    \"testMode\": false"
        + "  }"
        + "}]"
        + "}";

    // Write model to temp file
    java.io.File modelFile = java.io.File.createTempFile("limited-sec-test", ".json");
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
        int tableCount = 0;
        while (tables.next()) {
          String tableName = tables.getString("TABLE_NAME");
          System.out.println("Found table: " + tableName);
          tableCount++;
        }
        assertTrue(tableCount > 0, "Should have at least one table");
      }

      System.out.println("\n=== STEP 2: Check Schema of financial_line_items ===");
      try (ResultSet columns = connection.getMetaData().getColumns(null, "sec", "financial_line_items", "%")) {
        System.out.println("Columns in financial_line_items:");
        while (columns.next()) {
          String colName = columns.getString("COLUMN_NAME");
          String typeName = columns.getString("TYPE_NAME");
          System.out.println("  " + colName + " (" + typeName + ")");
        }
      }

      System.out.println("\n=== STEP 3: Count Total Records ===");
      String countQuery = "SELECT COUNT(*) as total_count FROM sec.financial_line_items";
      try (PreparedStatement stmt = connection.prepareStatement(countQuery);
           ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          int count = rs.getInt("total_count");
          System.out.println("Total records in financial_line_items: " + count);
          assertTrue(count > 0, "Should have some records");
        }
      }

      System.out.println("\n=== STEP 4: Check NetIncomeLoss Data ===");
      String netIncomeQuery =
          "SELECT " +
          "    cik, " +
          "    filing_type, " +
          "    \"year\", " +
          "    concept, " +
          "    numeric_value " +
          "FROM sec.financial_line_items " +
          "WHERE concept = 'NetIncomeLoss' " +
          "  AND filing_type = '10K' " +
          "ORDER BY cik, \"year\"";

      System.out.println("Executing query: " + netIncomeQuery);

      try (PreparedStatement stmt = connection.prepareStatement(netIncomeQuery);
           ResultSet rs = stmt.executeQuery()) {

        System.out.println("\nNetIncomeLoss Data:");
        System.out.println("CIK\t\tFiling\tYear\tValue");
        System.out.println("---\t\t------\t----\t-----");

        int recordCount = 0;
        while (rs.next()) {
          recordCount++;
          String cik = rs.getString("cik");
          String filingType = rs.getString("filing_type");
          Object year = rs.getObject("year");
          Double value = rs.getDouble("numeric_value");

          System.out.printf("%s\t%s\t%s\t%,.0f\n",
              cik, filingType, year, value);
        }

        System.out.printf("\nTotal NetIncomeLoss records found: %d\n", recordCount);
        assertTrue(recordCount > 0, "Should have NetIncomeLoss records for Apple and Microsoft");

        // We expect at least 4 records (2 companies x 2 years)
        assertTrue(recordCount >= 4, "Should have at least 4 NetIncomeLoss records (2 companies x 2 years)");
      }

      System.out.println("\n=== STEP 5: Verify Partition Values ===");
      String partitionQuery =
          "SELECT DISTINCT cik, filing_type, \"year\" " +
          "FROM sec.financial_line_items " +
          "ORDER BY cik, \"year\"";

      try (PreparedStatement stmt = connection.prepareStatement(partitionQuery);
           ResultSet rs = stmt.executeQuery()) {

        System.out.println("\nDistinct partition values:");
        while (rs.next()) {
          String cik = rs.getString("cik");
          String filingType = rs.getString("filing_type");
          Object year = rs.getObject("year");
          System.out.printf("  cik=%s, filing_type=%s, year=%s\n", cik, filingType, year);
        }
      }
    }
  }
}
