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

import org.apache.calcite.adapter.govdata.GovDataTestModels;
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
 * Simple test of SEC adapter with Apple 2023 data to check zero-byte file handling.
 */
@Tag("integration")
public class SimpleSecAdapterTest {

  @BeforeEach
  void checkZeroByteFile() {
    // Check if the problematic file exists and its size
    File zeroByteFile = new File("/Volumes/T9/calcite-sec-cache/sec-parquet/cik=0000320193/filing_type=10K/year=2023/0000320193_2023-09-30_facts.parquet");
    if (zeroByteFile.exists()) {
      System.out.println("File exists with size: " + zeroByteFile.length() + " bytes");
      if (zeroByteFile.length() == 0) {
        System.out.println("WARNING: Zero-byte file detected, SEC adapter should regenerate it");
      }
    } else {
      System.out.println("File does not exist, SEC adapter should create it");
    }
  }

  @Test void testApple2023NetIncome() throws Exception {
    // Load Apple integration model from resources
    String modelPath = GovDataTestModels.loadTestModel("apple-integration-model");

    Properties props = new Properties();
    props.setProperty("lex", "ORACLE");
    props.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    System.out.println("Connecting with JDBC URL: " + jdbcUrl);

    try (Connection conn = DriverManager.getConnection(jdbcUrl, props);
         Statement stmt = conn.createStatement()) {

      // Query for Apple's 2023 net income (year is a reserved word, must be quoted)
      String query =
        "SELECT " +
        "  cik, " +
        "  filing_type, " +
        "  \"year\", " +
        "  concept, " +
        "  ROUND(numeric_value/1000000000, 1) as billions " +
        "FROM sec.financial_line_items " +
        "WHERE cik = '0000320193' " +
        "  AND filing_type = '10K' " +
        "  AND \"year\" = 2023 " +
        "  AND LOWER(concept) LIKE '%netincome%' " +
        "LIMIT 5";

      System.out.println("\nExecuting query: " + query);

      try (ResultSet rs = stmt.executeQuery(query)) {
        int rowCount = 0;
        while (rs.next()) {
          String cik = rs.getString("cik");
          String filingType = rs.getString("filing_type");
          int year = rs.getInt(3);  // year is column 3
          String concept = rs.getString("concept");
          double billions = rs.getDouble("billions");

          System.out.printf("Row %d: CIK=%s, Filing=%s, Year=%d, Concept=%s, NetIncome=$%.1fB%n",
              ++rowCount, cik, filingType, year, concept, billions);

          // Validate
          assertEquals("0000320193", cik);
          assertEquals("10K", filingType);
          assertEquals(2023, year);
          assertTrue(billions > 0, "Apple should have positive net income");
        }

        if (rowCount == 0) {
          System.out.println("No data found - checking if file was regenerated...");

          // Check if file was regenerated
          File parquetFile = new File("/Volumes/T9/calcite-sec-cache/sec-parquet/cik=0000320193/filing_type=10K/year=2023/0000320193_2023-09-30_facts.parquet");
          if (parquetFile.exists()) {
            System.out.println("File exists with size: " + parquetFile.length() + " bytes");
            if (parquetFile.length() == 0) {
              fail("File still zero bytes - regeneration failed!");
            }
          } else {
            fail("File not found!");
          }
        } else {
          System.out.println("\nSuccess! Found " + rowCount + " rows of Apple 2023 net income data");

          // Verify file was fixed
          File parquetFile = new File("/Volumes/T9/calcite-sec-cache/sec-parquet/cik=0000320193/filing_type=10K/year=2023/0000320193_2023-09-30_facts.parquet");
          assertTrue(parquetFile.exists(), "Parquet file should exist");
          assertTrue(parquetFile.length() > 0, "Parquet file should not be zero bytes");
          System.out.println("File successfully regenerated with size: " + parquetFile.length() + " bytes");
        }
      }
    }
  }
}
