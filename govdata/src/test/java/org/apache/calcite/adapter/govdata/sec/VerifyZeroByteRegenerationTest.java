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

import org.junit.jupiter.api.BeforeEach;
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
 * Very simple test to verify zero-byte file regeneration.
 * Uses Apple (0000320193) for years 2022-2024, all filing types.
 */
@Tag("integration")
public class VerifyZeroByteRegenerationTest {

  private File zeroByteFile;

  @BeforeEach
  void checkFile() {
    // The problematic zero-byte file
    zeroByteFile = new File("/Volumes/T9/calcite-sec-cache/sec-parquet/cik=0000320193/filing_type=10K/year=2023/0000320193_2023-09-30_facts.parquet");

    System.out.println("\n=== BEFORE TEST ===");
    if (zeroByteFile.exists()) {
      System.out.println("File exists: " + zeroByteFile.getAbsolutePath());
      System.out.println("File size: " + zeroByteFile.length() + " bytes");
      if (zeroByteFile.length() == 0) {
        System.out.println("WARNING: File is ZERO bytes!");
      }
    } else {
      System.out.println("File does NOT exist");
    }
  }

  @Test void testSimpleAppleQuery() throws Exception {
    // Create model pointing to the same cache directory
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"sec\","
        + "\"schemas\": [{"
        + "  \"name\": \"sec\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "  \"operand\": {"
        + "    \"directory\": \"/Volumes/T9/calcite-sec-cache\","  // Same cache directory
        + "    \"ciks\": [\"0000320193\"],"  // Just Apple
        + "    \"filingTypes\": [\"10-K\", \"10-Q\", \"8-K\"],"  // All filing types
        + "    \"autoDownload\": true,"
        + "    \"startYear\": 2022,"
        + "    \"endYear\": 2024,"  // 2022-2024
        + "    \"executionEngine\": \"duckdb\","
        + "    \"testMode\": false,"
        + "    \"ephemeralCache\": false,"  // Use persistent cache
        + "    \"sec.fallback.enabled\": false"
        + "  }"
        + "}]"
        + "}";

    // Write model to temp file
    File modelFile = File.createTempFile("sec-apple-test", ".json");
    modelFile.deleteOnExit();
    java.nio.file.Files.writeString(modelFile.toPath(), modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    System.out.println("\n=== CONNECTING TO SEC ADAPTER ===");
    System.out.println("Model: " + modelFile.getAbsolutePath());

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      System.out.println("\n=== EXECUTING QUERY ===");

      // Simple query for Apple data across all years
      String query =
        "SELECT " +
        "  \"year\", " +
        "  filing_type, " +
        "  COUNT(*) as fact_count, " +
        "  COUNT(DISTINCT concept) as unique_concepts " +
        "FROM sec.financial_line_items " +
        "WHERE cik = '0000320193' " +
        "  AND filing_type = '10K' " +
        "GROUP BY \"year\", filing_type " +
        "ORDER BY \"year\"";

      System.out.println("Query: " + query);

      try (ResultSet rs = stmt.executeQuery(query)) {
        System.out.println("\n=== QUERY RESULTS ===");

        boolean found2023 = false;
        while (rs.next()) {
          int year = rs.getInt(1);
          String filingType = rs.getString("filing_type");
          int factCount = rs.getInt("fact_count");
          int uniqueConcepts = rs.getInt("unique_concepts");

          System.out.printf("Year=%d, Type=%s, Facts=%d, UniqueConcepts=%d%n",
              year, filingType, factCount, uniqueConcepts);

          if (year == 2023) {
            found2023 = true;
            assertTrue(factCount > 0, "2023 should have facts after regeneration");
          }
        }

        System.out.println("\nFound 2023 data: " + found2023);
      }
    }

    // Check file after test
    System.out.println("\n=== AFTER TEST ===");
    if (zeroByteFile.exists()) {
      System.out.println("File exists: " + zeroByteFile.getAbsolutePath());
      System.out.println("File size: " + zeroByteFile.length() + " bytes");

      if (zeroByteFile.length() == 0) {
        System.out.println("ERROR: File is STILL zero bytes - NOT regenerated!");
        fail("Zero-byte file was not regenerated!");
      } else {
        System.out.println("SUCCESS: File has been regenerated with " + zeroByteFile.length() + " bytes!");
      }
    } else {
      System.out.println("File does NOT exist - may need to be created");
    }
  }
}
