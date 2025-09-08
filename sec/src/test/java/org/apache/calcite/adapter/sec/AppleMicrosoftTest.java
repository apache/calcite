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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test SEC adapter with Apple and Microsoft 10-K data for 2022-2023.
 */
@Tag("integration")
public class AppleMicrosoftTest {

  private String modelPath;

  @BeforeEach
  void setUp(TestInfo testInfo) throws Exception {
    // Use the test model with Apple and Microsoft
    modelPath = AppleMicrosoftTest.class.getResource("/test-aapl-msft-model.json").getPath();
    assertTrue(Files.exists(Paths.get(modelPath)), "Model file should exist");
  }

  @Test void testFinancialLineItemsQuery() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");
    props.setProperty("conformance", "DEFAULT");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      // First, check if the table exists
      System.out.println("Testing financial_line_items table access...");

      String query =
        "SELECT cik, filing_type, year, concept, \"value\", numeric_value " +
        "FROM sec.financial_line_items " +
        "WHERE cik IN ('0000320193', '0000789019') " +
        "  AND filing_type = '10K' " +
        "  AND LOWER(concept) LIKE '%netincome%' " +
        "  AND year >= 2022 " +
        "ORDER BY cik, year " +
        "LIMIT 10";

      System.out.println("Executing query: " + query);

      try (ResultSet rs = stmt.executeQuery(query)) {
        ResultSetMetaData meta = rs.getMetaData();
        System.out.println("Column count: " + meta.getColumnCount());

        for (int i = 1; i <= meta.getColumnCount(); i++) {
          System.out.println("Column " + i + ": " + meta.getColumnName(i) + " (" + meta.getColumnTypeName(i) + ")");
        }

        int rowCount = 0;
        while (rs.next()) {
          String cik = rs.getString("cik");
          String filingType = rs.getString("filing_type");
          int year = rs.getInt("year");
          String concept = rs.getString("concept");
          String valueStr = rs.getString(5);  // "value" column
          double numericValue = rs.getDouble("numeric_value");

          System.out.printf("Row %d: CIK=%s, Filing=%s, Year=%d, Concept=%s, Value=%s, NumericValue=%,.0f%n",
              ++rowCount, cik, filingType, year, concept, valueStr, numericValue);

          // Validate data
          assertTrue(cik.equals("0000320193") || cik.equals("0000789019"), "Should be Apple or Microsoft");
          assertTrue(year >= 2022 && year <= 2023, "Should be 2022 or 2023");
          assertTrue(numericValue > 0, "Net income should be positive");
        }

        assertTrue(rowCount > 0, "Should have found some net income data");
        System.out.println("Successfully queried " + rowCount + " rows");
      }
    }
  }

  @Test void testTableSchema() throws Exception {
    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      // Check table metadata
      String query =
        "SELECT * " +
        "FROM sec.financial_line_items " +
        "WHERE 1=0";

      try (ResultSet rs = stmt.executeQuery(query)) {
        ResultSetMetaData meta = rs.getMetaData();

        System.out.println("Table schema for financial_line_items:");
        System.out.println("Column count: " + meta.getColumnCount());

        // Verify expected columns exist
        boolean hasCik = false;
        boolean hasFilingType = false;
        boolean hasYear = false;
        boolean hasLineItem = false;
        boolean hasValue = false;

        for (int i = 1; i <= meta.getColumnCount(); i++) {
          String colName = meta.getColumnName(i).toLowerCase();
          System.out.println("  Column " + i + ": " + colName + " - " + meta.getColumnTypeName(i));

          if (colName.equals("cik")) hasCik = true;
          if (colName.equals("filing_type")) hasFilingType = true;
          if (colName.contains("year")) hasYear = true;
          if (colName.contains("line_item") || colName.contains("lineitem") || colName.contains("metric") || colName.contains("concept")) hasLineItem = true;
          if (colName.equals("value") || colName.contains("amount")) hasValue = true;
        }

        assertTrue(hasCik, "Should have cik column");
        assertTrue(hasFilingType, "Should have filing_type column");
        assertTrue(hasYear, "Should have year column");
        assertTrue(hasLineItem, "Should have line_item column");
        assertTrue(hasValue, "Should have value column");
      }
    }
  }
}
