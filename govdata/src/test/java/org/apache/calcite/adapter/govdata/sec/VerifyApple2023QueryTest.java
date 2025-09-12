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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verify Apple 2023 10-K query works.
 */
@Tag("unit")
public class VerifyApple2023QueryTest {

  @Test void testApple2023Query() throws Exception {
    System.out.println("\n=== VERIFYING APPLE 2023 10-K QUERY ===");

    // Check that Parquet files exist
    File factsFile = new File("/Volumes/T9/calcite-sec-cache/sec-parquet/cik=0000320193/filing_type=10K/year=2023/0000320193_2023-09-30_facts.parquet");
    System.out.println("Facts file exists: " + factsFile.exists());
    System.out.println("Facts file size: " + factsFile.length() + " bytes");

    if (!factsFile.exists()) {
      System.out.println("SKIPPING: Parquet file not found");
      return;
    }

    // Create model JSON
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"sec\","
        + "\"schemas\": [{"
        + "  \"name\": \"sec\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\": {"
        + "    \"directory\": \"/Volumes/T9/calcite-sec-cache\","
        + "    \"ciks\": [\"0000320193\"],"
        + "    \"filingTypes\": [\"10-K\"],"
        + "    \"autoDownload\": false,"  // Don't download, just use cached
        + "    \"startYear\": 2023,"
        + "    \"endYear\": 2023,"
        + "    \"executionEngine\": \"duckdb\","
        + "    \"testMode\": false,"
        + "    \"ephemeralCache\": false,"
        + "    \"sec.fallback.enabled\": false"
        + "  }"
        + "}]"
        + "}";

    // Write model to temp file
    File modelFile = File.createTempFile("apple-query-test", ".json");
    modelFile.deleteOnExit();
    java.nio.file.Files.writeString(modelFile.toPath(), modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    System.out.println("\nConnecting to: " + jdbcUrl);

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      // First, query for all data to see what's actually in there
      String query =
        "SELECT " +
        "  cik, " +
        "  filing_type, " +
        "  \"year\", " +
        "  concept, " +
        "  numeric_value " +
        "FROM sec.financial_line_items " +
        "LIMIT 20";

      System.out.println("\nExecuting query:\n"
  + query);
      System.out.println("\n=== QUERY RESULTS ===");

      try (ResultSet rs = stmt.executeQuery(query)) {
        int rowCount = 0;

        while (rs.next()) {
          rowCount++;
          String cik = rs.getString("cik");
          String filingType = rs.getString("filing_type");
          Object year = rs.getObject("year");
          String concept = rs.getString("concept");
          Object numericValue = rs.getObject("numeric_value");

          System.out.printf("Row %d:\n", rowCount);
          System.out.printf("  CIK: %s\n", cik);
          System.out.printf("  Filing Type: %s\n", filingType);
          System.out.printf("  Year: %s\n", year);
          System.out.printf("  Concept: %s\n", concept);
          System.out.printf("  Numeric Value: %s\n", numericValue);
          System.out.println();
        }

        if (rowCount == 0) {
          System.out.println("NO DATA FOUND!");
          fail("Query returned no results - check if SEC adapter is working correctly");
        } else {
          System.out.println("=== SUCCESS ===");
          System.out.printf("Found %d rows of data in financial_line_items table\n", rowCount);
          System.out.println("The SEC adapter is working correctly with autoDownload=false!");
        }
      }
    }
  }
}
