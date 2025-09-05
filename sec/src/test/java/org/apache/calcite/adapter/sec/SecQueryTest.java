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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test querying SEC EDGAR data through XBRL adapter.
 */
@Tag("integration")
public class SecQueryTest {

  private static final String BASE_DIR = "build/apple-edgar-test-data";

  @BeforeAll
  public static void ensureDataExists() throws Exception {
    // This test requires data from EdgarDownloadTest to be run first
    File dataDir = new File(BASE_DIR);
    if (!dataDir.exists() || !new File(dataDir, "parquet").exists()) {
      throw new IllegalStateException("Test data not found. Run EdgarDownloadTest first to download test data.");
    }
  }

  @Test public void testQueryAppleRevenue() throws Exception {
    System.out.println("\n=== Testing Apple Revenue Query ===");

    // Create model for querying the Parquet data
    File modelFile = createQueryModel();

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String url = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(url, info)) {
      System.out.println("Connected to Parquet data via Calcite\n");

      // Query 1: Apple's annual revenue (10-K filings)
      String sql1 = "SELECT fiscal_year, fiscal_period, concept, \"value\" " +
                    "FROM financial_line_items " +
                    "WHERE cik = '0000320193' " +
                    "  AND filing_type = '10-K' " +
                    "  AND concept LIKE '%Revenue%' " +
                    "ORDER BY fiscal_year DESC " +
                    "LIMIT 10";

      System.out.println("Query 1: Annual Revenue from 10-K filings");
      System.out.println("==========================================");
      executeAndPrintQuery(conn, sql1);

      // Query 2: Compare quarterly vs annual filings
      String sql2 = "SELECT filing_type, fiscal_year, fiscal_period, " +
                    "       COUNT(*) as fact_count, " +
                    "       COUNT(DISTINCT concept) as unique_concepts " +
                    "FROM financial_line_items " +
                    "WHERE cik = '0000320193' " +
                    "  AND filing_type IN ('10-K', '10-Q') " +
                    "GROUP BY filing_type, fiscal_year, fiscal_period " +
                    "ORDER BY fiscal_year DESC, filing_type, fiscal_period " +
                    "LIMIT 15";

      System.out.println("\nQuery 2: Filing Statistics");
      System.out.println("===========================");
      executeAndPrintQuery(conn, sql2);

      // Query 3: Event-driven filings (8-K)
      String sql3 = "SELECT filing_type, filing_year, filing_month, " +
                    "       COUNT(*) as filing_count " +
                    "FROM financial_line_items " +
                    "WHERE cik = '0000320193' " +
                    "  AND filing_type = '8-K' " +
                    "GROUP BY filing_type, filing_year, filing_month " +
                    "ORDER BY filing_year DESC, filing_month DESC " +
                    "LIMIT 10";

      System.out.println("\nQuery 3: 8-K Filing Distribution");
      System.out.println("=================================");
      executeAndPrintQuery(conn, sql3);

      // Query 4: Company information
      String sql4 = "SELECT DISTINCT company_name, fiscal_year, fiscal_period " +
                    "FROM company_info " +
                    "WHERE cik = '0000320193' " +
                    "  AND filing_type = '10-K' " +
                    "ORDER BY fiscal_year DESC " +
                    "LIMIT 5";

      System.out.println("\nQuery 4: Company Information");
      System.out.println("=============================");
      executeAndPrintQuery(conn, sql4);

      // Query 5: Test partition pruning - specific fiscal year
      String sql5 = "SELECT concept, \"value\" " +
                    "FROM financial_line_items " +
                    "WHERE cik = '0000320193' " +
                    "  AND filing_type = '10-K' " +
                    "  AND fiscal_year = '2023' " +
                    "  AND fiscal_period = 'FY' " +
                    "  AND concept IN ('NetIncomeLoss', 'Assets', 'GrossProfit') " +
                    "ORDER BY concept";

      System.out.println("\nQuery 5: 2023 Key Metrics (tests partition pruning)");
      System.out.println("====================================================");
      executeAndPrintQuery(conn, sql5);
    }
  }

  private File createQueryModel() throws Exception {
    File modelFile = new File(BASE_DIR, "query-model.json");
    String modelJson =
        String.format("{\n"
  +
        "  \"version\": \"1.0\",\n"
  +
        "  \"defaultSchema\": \"XBRL\",\n"
  +
        "  \"schemas\": [{\n"
  +
        "    \"name\": \"XBRL\",\n"
  +
        "    \"type\": \"custom\",\n"
  +
        "    \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\",\n"
  +
        "    \"operand\": {\n"
  +
        "      \"directory\": \"%s/parquet\",\n"
  +
        "      \"executionEngine\": \"duckdb\"\n"
  +
        "    }\n"
  +
        "  }]\n"
  +
        "}", new File(BASE_DIR).getAbsolutePath());

    java.nio.file.Files.write(modelFile.toPath(), modelJson.getBytes());
    return modelFile;
  }

  private void executeAndPrintQuery(Connection conn, String sql) throws Exception {
    try (Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {

      // Print column headers
      int columnCount = rs.getMetaData().getColumnCount();
      for (int i = 1; i <= columnCount; i++) {
        System.out.printf("%-20s ", rs.getMetaData().getColumnName(i));
      }
      System.out.println();
      for (int i = 1; i <= columnCount; i++) {
        System.out.print("-------------------- ");
      }
      System.out.println();

      // Print rows
      int rowCount = 0;
      while (rs.next()) {
        for (int i = 1; i <= columnCount; i++) {
          Object value = rs.getObject(i);
          if (value instanceof Number) {
            System.out.printf("%-20.0f ", ((Number) value).doubleValue());
          } else {
            System.out.printf("%-20s ", value != null ? value.toString() : "NULL");
          }
        }
        System.out.println();
        rowCount++;
      }

      System.out.println("(" + rowCount + " rows)");
      assertTrue(rowCount > 0, "Query should return results");
    }
  }
}
