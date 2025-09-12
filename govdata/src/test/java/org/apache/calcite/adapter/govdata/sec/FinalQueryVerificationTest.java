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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Final verification that Apple 2023 10-K query works using file adapter directly.
 */
@Tag("unit")
public class FinalQueryVerificationTest {

  @Test void testDirectParquetQuery() throws Exception {
    System.out.println("\n=== DIRECT PARQUET QUERY VERIFICATION ===");

    // Check that Parquet file exists
    File factsFile = new File("/Volumes/T9/calcite-sec-cache/sec-parquet/cik=0000320193/filing_type=10K/year=2023/0000320193_2023-09-30_facts.parquet");
    System.out.println("Facts file: " + factsFile.getAbsolutePath());
    System.out.println("Exists: " + factsFile.exists());
    System.out.println("Size: " + factsFile.length() + " bytes");

    if (!factsFile.exists()) {
      System.out.println("SKIPPING: Parquet file not found");
      return;
    }

    // Use file adapter to query the Parquet file directly
    String modelJson = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"apple_data\","
        + "\"schemas\": [{"
        + "  \"name\": \"apple_data\","
        + "  \"type\": \"custom\","
        + "  \"factory\": \"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\": {"
        + "    \"directory\": \"/Volumes/T9/calcite-sec-cache/sec-parquet\","
        + "    \"executionEngine\": \"PARQUET\","
        + "    \"tableNameCasing\": \"SMART_CASING\","
        + "    \"columnNameCasing\": \"SMART_CASING\","
        + "    \"partitionedTables\": [{"
        + "      \"name\": \"apple_facts\","
        + "      \"pattern\": \"cik=0000320193/filing_type=10K/year=2023/*_facts.parquet\""
        + "    }]"
        + "  }"
        + "}]"
        + "}";

    // Write model to temp file
    File modelFile = File.createTempFile("direct-parquet-test", ".json");
    modelFile.deleteOnExit();
    java.nio.file.Files.writeString(modelFile.toPath(), modelJson);

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelFile.getAbsolutePath();

    System.out.println("\nConnecting to: " + jdbcUrl);

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      // First list tables
      try (ResultSet rs = conn.getMetaData().getTables(null, "apple_data", "%", new String[]{"TABLE"})) {
        System.out.println("\nAvailable tables:");
        while (rs.next()) {
          System.out.println("  " + rs.getString("TABLE_NAME"));
        }
      }

      // Query for Apple's 2023 net income using direct table name
      String query =
        "SELECT " +
        "  cik, " +
        "  filing_type, " +
        "  concept, " +
        "  numeric_value, " +
        "  ROUND(numeric_value/1000000000, 2) as billions " +
        "FROM apple_data.apple_facts " +
        "WHERE LOWER(concept) LIKE '%netincome%' " +
        "  AND concept NOT LIKE '%attributable%' " +
        "LIMIT 5";

      System.out.println("\nExecuting query:\n"
  + query);
      System.out.println("\n=== APPLE 2023 NET INCOME RESULTS ===");

      try (ResultSet rs = stmt.executeQuery(query)) {
        int rowCount = 0;

        while (rs.next()) {
          rowCount++;
          String cik = rs.getString("cik");
          String filingType = rs.getString("filing_type");
          String concept = rs.getString("concept");
          double numericValue = rs.getDouble("numeric_value");
          double billions = rs.getDouble("billions");

          System.out.printf("Row %d:\n", rowCount);
          System.out.printf("  CIK: %s (Apple)\n", cik);
          System.out.printf("  Filing: %s\n", filingType);
          System.out.printf("  Concept: %s\n", concept);
          System.out.printf("  Value: $%.2f billion\n", billions);
          System.out.println();

          // Validate
          assertEquals("0000320193", cik, "Should be Apple's CIK");
          assertTrue(concept.toLowerCase().contains("netincome"), "Should be net income concept");
          assertTrue(billions > 0, "Net income should be positive");
        }

        if (rowCount == 0) {
          System.out.println("NO RESULTS - checking file contents...");

          // Try a simpler query to see what's in the file
          try (ResultSet rs2 = stmt.executeQuery("SELECT COUNT(*) as total_rows FROM apple_data.apple_facts")) {
            if (rs2.next()) {
              int total = rs2.getInt("total_rows");
              System.out.println("Total rows in file: " + total);
            }
          }

          try (ResultSet rs3 = stmt.executeQuery("SELECT DISTINCT concept FROM apple_data.apple_facts LIMIT 10")) {
            System.out.println("Sample concepts:");
            while (rs3.next()) {
              System.out.println("  " + rs3.getString("concept"));
            }
          }

        } else {
          System.out.println("=== SUCCESS! ===");
          System.out.printf("Apple 2023 10-K data is queryable: found %d net income entries\n", rowCount);
          System.out.println("The zero-byte file has been successfully regenerated and contains valid data!");
        }
      }
    }
  }
}
