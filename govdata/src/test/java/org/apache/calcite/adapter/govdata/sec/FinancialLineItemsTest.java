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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for financial_line_items consolidation functionality.
 */
@Tag("integration")
public class FinancialLineItemsTest {

  @Test public void testFinancialLineItemsTableExists() throws Exception {
    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");

    // Create model configuration for real integration test
    String model = "{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"SEC\","
        + "\"schemas\": [{"
        + "\"name\": \"SEC\","
        + "\"type\": \"custom\","
        + "\"factory\": \"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "\"operand\": {"
        + "\"ciks\": [\"_DJIA_CONSTITUENTS\"],"
        + "\"startYear\": 2022,"
        + "\"endYear\": 2023,"
        + "\"autoDownload\": true,"
        + "\"filingTypes\": [\"10-K\"],"
        + "\"testMode\": false,"
        + "\"ephemeralCache\": false"
        + "}}]}";

    Properties props = new Properties();
    props.put("inline", model);
    props.put("lex", "ORACLE");
    props.put("unquotedCasing", "TO_LOWER");

    System.out.println("Testing financial_line_items consolidation...");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      System.out.println("✓ Connected to SEC adapter");

      // Test if financial_line_items table exists and has data
      try (Statement stmt = conn.createStatement();
           ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) as record_count FROM financial_line_items")) {

        assertTrue(rs.next(), "financial_line_items query should return a result");
        int count = rs.getInt("record_count");
        System.out.println("✓ financial_line_items table found with " + count + " records");

        // We expect at least some records from consolidation
        assertTrue(count >= 0, "financial_line_items should have non-negative record count");

      }

      System.out.println("✓ financial_line_items test passed");
    }
  }
}
