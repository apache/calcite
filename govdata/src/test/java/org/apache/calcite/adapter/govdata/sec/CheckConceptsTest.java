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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * Check exact concept names in the data.
 */
@Tag("integration")
public class CheckConceptsTest {

  @Test void checkNetIncomeConcepts() throws Exception {
    System.out.println("\n=== CHECKING EXACT CONCEPT NAMES ===");

    // Create model JSON
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"sec\","
        + "\"schemas\": [{"
        + "  \"name\": \"sec\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\","
        + "  \"operand\": {"
        + "    \"directory\": \"/Volumes/T9/calcite-sec-cache\","
        + "    \"ciks\": [\"0000320193\"],"  // Just Apple
        + "    \"filingTypes\": [\"10-K\"],"
        + "    \"autoDownload\": false,"
        + "    \"startYear\": 2022,"
        + "    \"endYear\": 2023,"
        + "    \"executionEngine\": \"parquet\","
        + "    \"testMode\": false"
        + "  }"
        + "}]"
        + "}";

    // Write model to temp file
    java.io.File modelFile = java.io.File.createTempFile("concept-check", ".json");
    modelFile.deleteOnExit();
    java.nio.file.Files.writeString(modelFile.toPath(), modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props)) {

      // Just get first 5 concepts to see the actual case
      String query =
        "SELECT DISTINCT concept " +
        "FROM sec.financial_line_items " +
        "WHERE filing_type = '10K' " +
        "  AND cik = '0000320193' " +
        "  AND \"year\" = '2023' " +
        "LIMIT 5";

      System.out.println("\nFirst 5 concepts from Apple 2023 10-K:");
      try (PreparedStatement stmt = conn.prepareStatement(query);
           ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          String concept = rs.getString("concept");
          System.out.println("  Concept: '" + concept + "'");
        }
      }

      // Check what's in the actual data
      String netIncomeQuery =
        "SELECT concept, numeric_value " +
        "FROM sec.financial_line_items " +
        "WHERE LOWER(concept) LIKE '%netincome%' " +
        "  AND filing_type = '10K' " +
        "  AND cik = '0000320193' " +
        "  AND \"year\" = '2023' " +
        "LIMIT 5";

      System.out.println("\nNet income concepts from Apple 2023:");
      try (PreparedStatement stmt = conn.prepareStatement(netIncomeQuery);
           ResultSet rs = stmt.executeQuery()) {
        int count = 0;
        while (rs.next()) {
          count++;
          System.out.println("  Concept: '" + rs.getString("concept") +
                           "', Value: " + rs.getObject("numeric_value"));
        }
        System.out.println("  Total found: " + count);
      }
    }
  }
}
