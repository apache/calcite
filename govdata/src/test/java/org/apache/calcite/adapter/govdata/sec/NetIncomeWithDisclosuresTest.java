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
 * Test joining NetIncomeLoss with related disclosure text blocks.
 */
@Tag("integration")
public class NetIncomeWithDisclosuresTest {

  @Test @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void testNetIncomeWithDisclosures() throws Exception {
    System.out.println("\n=== NET INCOME WITH DISCLOSURES TEST ===");

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
        + "    \"ciks\": [\"AAPL\", \"MSFT\"],"
        + "    \"filingTypes\": [\"10-K\"],"
        + "    \"autoDownload\": true,"
        + "    \"startYear\": 2022,"
        + "    \"endYear\": 2023,"
        + "    \"executionEngine\": \"parquet\","
        + "    \"testMode\": false"
        + "  }"
        + "}]"
        + "}";

    // Write model to temp file
    java.io.File modelFile = java.io.File.createTempFile("netincome-disclosures-test", ".json");
    modelFile.deleteOnExit();
    java.nio.file.Files.writeString(modelFile.toPath(), modelJson);

    // Connection properties
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection connection = DriverManager.getConnection(jdbcUrl, info)) {

      System.out.println("\n=== Find Related Disclosure TextBlocks ===");

      // Query to find disclosure TextBlocks that share context with NetIncomeLoss
      String disclosureQuery =
          "SELECT DISTINCT " +
          "    ni.cik, " +
          "    ni.\"year\", " +
          "    ni.concept as financial_item, " +
          "    ni.numeric_value, " +
          "    disc.concept as disclosure_section, " +
          "    LENGTH(disc.value) as text_length, " +
          "    disc.value as disclosure_text " +
          "FROM sec.financial_line_items ni " +
          "JOIN sec.financial_line_items disc " +
          "  ON ni.context_ref = disc.context_ref " +
          "  AND ni.cik = disc.cik " +
          "  AND ni.filing_type = disc.filing_type " +
          "  AND ni.\"year\" = disc.\"year\" " +
          "WHERE ni.concept = 'NetIncomeLoss' " +
          "  AND disc.concept LIKE '%TextBlock%' " +
          "  AND disc.value IS NOT NULL " +
          "ORDER BY ni.cik, ni.\"year\", disc.concept " +
          "LIMIT 20";

      System.out.println("Executing query to find related disclosures...");

      try (PreparedStatement stmt = connection.prepareStatement(disclosureQuery);
           ResultSet rs = stmt.executeQuery()) {

        System.out.println("\nNetIncomeLoss with Related Disclosure Sections:");
        System.out.println("=================================================");

        int recordCount = 0;
        String lastCik = "";
        while (rs.next()) {
          recordCount++;
          String cik = rs.getString("cik");
          String year = rs.getString("year");
          Double netIncome = rs.getDouble("numeric_value");
          String disclosureConcept = rs.getString("disclosure_section");
          int textLength = rs.getInt("text_length");
          String disclosureText = rs.getString("disclosure_text");

          // Print header for new company
          if (!cik.equals(lastCik)) {
            System.out.println("\n"
  + (cik.equals("0000320193") ? "Apple" : "Microsoft") +
                " (CIK: " + cik + ")");
            System.out.println("----------------------------------------");
            lastCik = cik;
          }

          // Format disclosure concept name for readability
          String readableConcept = disclosureConcept
              .replace("TextBlock", "")
              .replaceAll("([A-Z])", " $1")
              .trim();

          System.out.printf("Year %s - Net Income: $%,.0f million\n", year, netIncome);
          System.out.printf("  Related Disclosure: %s\n", readableConcept);
          System.out.printf("  Text Preview: %s%s\n",
              disclosureText.length() > 100 ?
                  disclosureText.substring(0, 100) + "..." :
                  disclosureText,
              textLength > 100 ? " [" + textLength + " chars total]" : "");
        }

        System.out.printf("\nTotal disclosure relationships found: %d\n", recordCount);
        assertTrue(recordCount > 0, "Should have found disclosure TextBlocks related to NetIncomeLoss");
      }

      System.out.println("\n=== Income-Related Disclosures Summary ===");

      // Find all income-related TextBlocks
      String incomeDisclosuresQuery =
          "SELECT DISTINCT concept, COUNT(*) as occurrences " +
          "FROM sec.financial_line_items " +
          "WHERE (concept LIKE '%Income%TextBlock%' " +
          "    OR concept LIKE '%Earnings%TextBlock%' " +
          "    OR concept LIKE '%Tax%TextBlock%') " +
          "GROUP BY concept " +
          "ORDER BY concept";

      try (PreparedStatement stmt = connection.prepareStatement(incomeDisclosuresQuery);
           ResultSet rs = stmt.executeQuery()) {

        System.out.println("\nIncome-Related Disclosure Sections Available:");
        while (rs.next()) {
          String concept = rs.getString("concept");
          int count = rs.getInt("occurrences");

          String readable = concept
              .replace("TextBlock", "")
              .replaceAll("([A-Z])", " $1")
              .trim();

          System.out.printf("  - %s (%d instances)\n", readable, count);
        }
      }
    }
  }
}
