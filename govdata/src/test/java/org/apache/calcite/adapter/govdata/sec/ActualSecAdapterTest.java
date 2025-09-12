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

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test actual SEC adapter with real EDGAR downloads.
 */
@Tag("integration")
public class ActualSecAdapterTest {
    @Test public void testActualSecAdapterWithDownloads() throws Exception {
        System.out.println("\n"
  + "=".repeat(80));
        System.out.println("TESTING ACTUAL SEC ADAPTER - REAL DOWNLOADS VIA JDBC");
        System.out.println("=".repeat(80) + "\n");

        Instant start = Instant.now();

        // Create inline model that will trigger SEC downloads
        String model = "{"
            + "\"version\":\"1.0\","
            + "\"defaultSchema\":\"sec\","
            + "\"schemas\":[{"
            + "\"name\":\"sec\","
            + "\"type\":\"custom\","
            + "\"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
            + "\"operand\":{"
            + "\"directory\":\"/Volumes/T9/sec-data/real-test\","
            + "\"enableSecProcessing\":true,"
            + "\"processSecOnInit\":true,"
            + "\"secSourceDirectory\":\"/Volumes/T9/sec-data/real-test/sec\","
            + "\"edgarSource\":{"
            + "\"autoDownload\":true,"
            + "\"ciks\":[\"0000320193\",\"0000789019\",\"0001018724\"],"  // Apple, Microsoft, Amazon
            + "\"startDate\":\"2024-01-01\","
            + "\"endDate\":\"2024-03-31\","
            + "\"filingTypes\":[\"10-K\",\"10-Q\",\"8-K\"]"
            + "}}}]}";

        Properties props = new Properties();
        props.put("model", model);

        System.out.println("Connecting to SEC adapter with model:");
        System.out.println("  Companies: Apple, Microsoft, Amazon");
        System.out.println("  Period: Q1 2024");
        System.out.println("  Directory: /Volumes/T9/sec-data/real-test");
        System.out.println("");
        System.out.println("This will download from SEC EDGAR and convert to Parquet...");
        System.out.println("");

        // Register driver
        Class.forName("org.apache.calcite.jdbc.Driver");

        // Connect - this triggers downloads
        Connection conn = DriverManager.getConnection("jdbc:calcite:", props);

        System.out.println("Connected! Running query to verify data...");

        // Query the data
        Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery("SELECT cik, filing_type, filing_date, COUNT(*) as line_items " +
            "FROM sec.financial_line_items " +
            "GROUP BY cik, filing_type, filing_date " +
            "ORDER BY filing_date DESC " +
            "LIMIT 10");

        System.out.println("\nQuery Results:");
        System.out.println("-".repeat(60));
        while (rs.next()) {
            System.out.printf("CIK: %s | Type: %s | Date: %s | Items: %d\n",
                rs.getString("cik"),
                rs.getString("filing_type"),
                rs.getString("filing_date"),
                rs.getInt("line_items"));
        }

        rs.close();
        stmt.close();
        conn.close();

        Duration elapsed = Duration.between(start, Instant.now());

        System.out.println("\n"
  + "=".repeat(80));
        System.out.println("PERFORMANCE METRICS:");
        System.out.println("  Total time: " + elapsed.toSeconds() + " seconds");
        System.out.println("  Per company (3 months): " + (elapsed.toSeconds() / 3.0) + " seconds");
        System.out.println("  Estimated per company-year: " + (elapsed.toSeconds() / 3.0 * 4) + " seconds");
        System.out.println("=".repeat(80));
    }
}
