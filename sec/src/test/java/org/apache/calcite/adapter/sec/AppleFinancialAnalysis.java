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
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Query Apple's financial data from the last 5 years of 10-K filings.
 *
 * <p>This demonstrates the declarative model approach where:
 * - The model.json declares what data to fetch (Apple 10-Ks from 2019-2024)
 * - The XBRL adapter automatically downloads and converts to Parquet
 * - We can query using standard SQL with partition pruning
 */
@Tag("integration")
public class AppleFinancialAnalysis {

  @TempDir
  Path tempDir;

  @Test public void testAppleFinancialData() throws Exception {
    // Use temp directory for data
    File dataDir = tempDir.toFile();

    // Create the declarative model
    File modelFile = new File(dataDir, "model.json");
    try (FileWriter writer = new FileWriter(modelFile, StandardCharsets.UTF_8)) {
      writer.write("{\n");
      writer.write("  \"version\": \"1.0\",\n");
      writer.write("  \"defaultSchema\": \"EDGAR\",\n");
      writer.write("  \"schemas\": [{\n");
      writer.write("    \"name\": \"EDGAR\",\n");
      writer.write("    \"type\": \"custom\",\n");
      writer.write("    \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\",\n");
      writer.write("    \"operand\": {\n");
      writer.write("      \"directory\": \"" + dataDir.getAbsolutePath().replace("\\", "\\\\") + "\",\n");
      writer.write("      \"executionEngine\": \"linq4j\",\n");
      writer.write("      \"enableSecProcessing\": true,\n");
      writer.write("      \"edgarSource\": {\n");
      writer.write("        \"cik\": \"0000320193\",\n");
      writer.write("        \"filingTypes\": [\"10-K\"],\n");
      writer.write("        \"startYear\": 2019,\n");
      writer.write("        \"endYear\": 2024,\n");
      writer.write("        \"autoDownload\": true\n");
      writer.write("      }\n");
      writer.write("    }\n");
      writer.write("  }]\n");
      writer.write("}\n");
    }

    // Connect using the declarative model
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String url = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    System.out.println("Connecting to Apple financial data via declarative model...");
    System.out.println("Model: " + modelFile.getAbsolutePath());
    System.out.println();

    try (Connection connection = DriverManager.getConnection(url, info)) {

      // Query 1: Revenue trend over 5 years
      System.out.println("=== Apple Revenue Trend (2019-2024) ===");
      String revenueSql =
          "SELECT SUBSTR(filing_date, 1, 4) as year, " +
          "       concept, " +
          "       \"value\" / 1000000000 as billions_usd " +
          "FROM financial_line_items " +
          "WHERE cik = '0000320193' " +
          "  AND filing_type = '10-K' " +
          "  AND concept = 'RevenueFromContractWithCustomerExcludingAssessedTax' " +
          "ORDER BY filing_date";

      executeAndPrintQuery(connection, revenueSql);

      // Query 2: Net Income trend
      System.out.println("\n=== Apple Net Income Trend (2019-2024) ===");
      String incomeSql =
          "SELECT SUBSTR(filing_date, 1, 4) as year, " +
          "       \"value\" / 1000000000 as net_income_billions " +
          "FROM financial_line_items " +
          "WHERE cik = '0000320193' " +
          "  AND filing_type = '10-K' " +
          "  AND concept = 'NetIncomeLoss' " +
          "ORDER BY filing_date";

      executeAndPrintQuery(connection, incomeSql);

      // Query 3: Key financial metrics for latest year
      System.out.println("\n=== Apple Key Metrics - Latest Year (2024) ===");
      String metricsSql =
          "SELECT concept, " +
          "       \"value\" / 1000000000 as billions_usd " +
          "FROM financial_line_items " +
          "WHERE cik = '0000320193' " +
          "  AND filing_type = '10-K' " +
          "  AND filing_date LIKE '2024%' " +
          "  AND concept IN ('RevenueFromContractWithCustomerExcludingAssessedTax', " +
          "                  'GrossProfit', " +
          "                  'OperatingIncomeLoss', " +
          "                  'NetIncomeLoss', " +
          "                  'Assets', " +
          "                  'Liabilities', " +
          "                  'StockholdersEquity') " +
          "ORDER BY " +
          "  CASE concept " +
          "    WHEN 'RevenueFromContractWithCustomerExcludingAssessedTax' THEN 1 " +
          "    WHEN 'GrossProfit' THEN 2 " +
          "    WHEN 'OperatingIncomeLoss' THEN 3 " +
          "    WHEN 'NetIncomeLoss' THEN 4 " +
          "    WHEN 'Assets' THEN 5 " +
          "    WHEN 'Liabilities' THEN 6 " +
          "    WHEN 'StockholdersEquity' THEN 7 " +
          "  END";

      executeAndPrintQuery(connection, metricsSql);

      // Query 4: Year-over-Year growth rates
      System.out.println("\n=== Apple YoY Growth Rates ===");
      String growthSql =
          "WITH yearly_data AS (" +
          "  SELECT SUBSTR(filing_date, 1, 4) as year, " +
          "         MAX(CASE WHEN concept = 'RevenueFromContractWithCustomerExcludingAssessedTax' " +
          "             THEN \"value\" END) as revenue, " +
          "         MAX(CASE WHEN concept = 'NetIncomeLoss' " +
          "             THEN \"value\" END) as net_income " +
          "  FROM financial_line_items " +
          "  WHERE cik = '0000320193' " +
          "    AND filing_type = '10-K' " +
          "  GROUP BY SUBSTR(filing_date, 1, 4) " +
          "), " +
          "growth_calc AS (" +
          "  SELECT year, " +
          "         revenue / 1000000000 as revenue_billions, " +
          "         net_income / 1000000000 as income_billions, " +
          "         LAG(revenue) OVER (ORDER BY year) as prev_revenue, " +
          "         LAG(net_income) OVER (ORDER BY year) as prev_income " +
          "  FROM yearly_data " +
          ") " +
          "SELECT year, " +
          "       revenue_billions, " +
          "       CASE WHEN prev_revenue IS NOT NULL " +
          "            THEN ROUND((revenue_billions - prev_revenue/1000000000) / (prev_revenue/1000000000) * 100, 1) " +
          "            ELSE NULL END as revenue_growth_pct, " +
          "       income_billions, " +
          "       CASE WHEN prev_income IS NOT NULL " +
          "            THEN ROUND((income_billions - prev_income/1000000000) / (prev_income/1000000000) * 100, 1) " +
          "            ELSE NULL END as income_growth_pct " +
          "FROM growth_calc " +
          "ORDER BY year";

      executeAndPrintQuery(connection, growthSql);

      // Query 5: Profitability margins
      System.out.println("\n=== Apple Profitability Margins ===");
      String marginsSql =
          "WITH metrics AS (" +
          "  SELECT SUBSTR(filing_date, 1, 4) as year, " +
          "         MAX(CASE WHEN concept = 'RevenueFromContractWithCustomerExcludingAssessedTax' " +
          "             THEN \"value\" END) as revenue, " +
          "         MAX(CASE WHEN concept = 'GrossProfit' " +
          "             THEN \"value\" END) as gross_profit, " +
          "         MAX(CASE WHEN concept = 'OperatingIncomeLoss' " +
          "             THEN \"value\" END) as operating_income, " +
          "         MAX(CASE WHEN concept = 'NetIncomeLoss' " +
          "             THEN \"value\" END) as net_income " +
          "  FROM financial_line_items " +
          "  WHERE cik = '0000320193' " +
          "    AND filing_type = '10-K' " +
          "  GROUP BY SUBSTR(filing_date, 1, 4) " +
          ") " +
          "SELECT year, " +
          "       ROUND(gross_profit / revenue * 100, 1) as gross_margin_pct, " +
          "       ROUND(operating_income / revenue * 100, 1) as operating_margin_pct, " +
          "       ROUND(net_income / revenue * 100, 1) as net_margin_pct " +
          "FROM metrics " +
          "ORDER BY year";

      executeAndPrintQuery(connection, marginsSql);

    } catch (SQLException e) {
      System.err.println("SQL Error: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private static void executeAndPrintQuery(Connection conn, String sql) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(sql);
         ResultSet rs = stmt.executeQuery()) {

      // Print column headers
      int columnCount = rs.getMetaData().getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        System.out.printf("%-30s", rs.getMetaData().getColumnLabel(i));
      }
      System.out.println();
      for (int i = 0; i < 30 * columnCount; i++) {
        System.out.print("-");
      }
      System.out.println();

      // Print rows
      while (rs.next()) {
        for (int i = 1; i <= columnCount; i++) {
          Object value = rs.getObject(i);
          if (value instanceof Number) {
            System.out.printf("%-30.2f", ((Number) value).doubleValue());
          } else {
            System.out.printf("%-30s", value != null ? value.toString() : "NULL");
          }
        }
        System.out.println();
      }
    }
  }
}
