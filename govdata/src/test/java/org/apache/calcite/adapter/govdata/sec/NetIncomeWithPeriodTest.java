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
 * Test joining NetIncomeLoss with context periods.
 */
@Tag("integration")
public class NetIncomeWithPeriodTest {

  @Test @Timeout(value = 5, unit = TimeUnit.MINUTES)
  public void testNetIncomeWithPeriod() throws Exception {
    System.out.println("\n=== NET INCOME WITH PERIOD TEST ===");

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
    java.io.File modelFile = java.io.File.createTempFile("netincome-period-test", ".json");
    modelFile.deleteOnExit();
    java.nio.file.Files.writeString(modelFile.toPath(), modelJson);

    // Connection properties
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection connection = DriverManager.getConnection(jdbcUrl, info)) {

      System.out.println("\n=== Query NetIncomeLoss with Period Information ===");

      // Join facts with contexts to get period information
      String joinQuery =
          "SELECT DISTINCT " +
          "    f.cik, " +
          "    f.filing_type, " +
          "    f.\"year\", " +
          "    f.concept, " +
          "    f.numeric_value, " +
          "    c.period_start, " +
          "    c.period_end, " +
          "    c.period_instant " +
          "FROM sec.financial_line_items f " +
          "JOIN sec.filing_contexts c " +
          "  ON f.context_ref = c.context_id " +
          "  AND f.cik = c.cik " +
          "  AND f.filing_type = c.filing_type " +
          "  AND f.\"year\" = c.\"year\" " +
          "WHERE f.concept = 'NetIncomeLoss' " +
          "  AND f.filing_type = '10K' " +
          "ORDER BY f.cik, f.\"year\", c.period_end";

      System.out.println("Executing query: " + joinQuery);

      try (PreparedStatement stmt = connection.prepareStatement(joinQuery);
           ResultSet rs = stmt.executeQuery()) {

        System.out.println("\nNetIncomeLoss with Period Dates:");
        System.out.println("CIK\t\tYear\tValue\t\tPeriod Start\tPeriod End");
        System.out.println("---\t\t----\t-----\t\t------------\t----------");

        int recordCount = 0;
        while (rs.next()) {
          recordCount++;
          String cik = rs.getString("cik");
          String year = rs.getString("year");  // year is stored as VARCHAR
          Double value = rs.getDouble("numeric_value");
          String periodStart = rs.getString("period_start");
          String periodEnd = rs.getString("period_end");
          String periodInstant = rs.getString("period_instant");

          // Use period_instant if it's a point-in-time value, otherwise use period range
          String periodDisplay = periodInstant != null ?
              "As of " + periodInstant :
              periodStart + " to " + periodEnd;

          System.out.printf("%s\t%s\t%,.0f\t%s\n",
              cik, year, value, periodDisplay);
        }

        System.out.printf("\nTotal records found: %d\n", recordCount);
        assertTrue(recordCount > 0, "Should have NetIncomeLoss records with period information");
      }

      // Also show aggregated annual net income by company
      System.out.println("\n=== Annual Net Income Summary ===");
      String summaryQuery =
          "SELECT " +
          "    f.cik, " +
          "    f.\"year\", " +
          "    MAX(c.period_end) as fiscal_year_end, " +
          "    SUM(DISTINCT f.numeric_value) as total_net_income " +
          "FROM sec.financial_line_items f " +
          "JOIN sec.filing_contexts c " +
          "  ON f.context_ref = c.context_id " +
          "  AND f.cik = c.cik " +
          "  AND f.filing_type = c.filing_type " +
          "  AND f.\"year\" = c.\"year\" " +
          "WHERE f.concept = 'NetIncomeLoss' " +
          "  AND f.filing_type = '10K' " +
          "  AND c.period_start IS NOT NULL " +
          "  AND c.period_end IS NOT NULL " +
          "GROUP BY f.cik, f.\"year\" " +
          "ORDER BY f.cik, f.\"year\"";

      try (PreparedStatement stmt = connection.prepareStatement(summaryQuery);
           ResultSet rs = stmt.executeQuery()) {

        System.out.println("Company\t\tYear\tFiscal Year End\tNet Income");
        System.out.println("-------\t\t----\t---------------\t----------");

        while (rs.next()) {
          String cik = rs.getString("cik");
          String year = rs.getString("year");  // year is stored as VARCHAR
          String fiscalYearEnd = rs.getString("fiscal_year_end");
          Double netIncome = rs.getDouble("total_net_income");

          // Map CIK to company name for display
          String company = cik.equals("0000320193") ? "Apple" : "Microsoft";

          System.out.printf("%s\t\t%s\t%s\t$%,.0f million\n",
              company, year, fiscalYearEnd, netIncome);
        }
      }
    }
  }
}
