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

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test Dow 30 SEC adapter with JDBC.
 */
@Tag("integration")
public class Dow30SecAdapterTest {

    @Test public void testDow30ViaJDBC() throws Exception {
        System.out.println("\n"
  + "=".repeat(80));
        System.out.println("TESTING DOW 30 SEC ADAPTER - REAL DOWNLOADS VIA JDBC");
        System.out.println("=".repeat(80) + "\n");

        Instant start = Instant.now();

        // Build model following file adapter pattern - just change the factory
        StringBuilder model = new StringBuilder();
        model.append("{\n");
        model.append("  \"version\": \"1.0\",\n");
        model.append("  \"defaultSchema\": \"sec\",\n");
        model.append("  \"schemas\": [\n");
        model.append("    {\n");
        model.append("      \"name\": \"sec\",\n");
        model.append("      \"type\": \"custom\",\n");
        model.append("      \"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\",\n");
        model.append("      \"operand\": {\n");
        model.append("        \"directory\": \"/Volumes/T9/sec-data/dow30-full\",\n");
        model.append("        \"enableSecProcessing\": true,\n");
        model.append("        \"processSecOnInit\": true,\n");
        model.append("        \"secSourceDirectory\": \"/Volumes/T9/sec-data/dow30-full/sec\",\n");
        model.append("        \"edgarSource\": {\n");
        model.append("          \"autoDownload\": true,\n");
        model.append("          \"ciks\": [\n");
        model.append("            \"0000320193\",\"0000018230\",\"0000012927\",\"0000004962\",\"0000886982\",\n");
        model.append("            \"0000021344\",\"0000354950\",\"0000773840\",\"0000093410\",\"0001744489\",\n");
        model.append("            \"0000789019\",\"0001018724\",\"0001067983\",\"0000030554\",\"0000866787\",\n");
        model.append("            \"0001403161\",\"0000732717\",\"0001045810\",\"0000051143\",\"0000200406\",\n");
        model.append("            \"0000078003\",\"0001326801\",\"0000080424\",\"0000034088\",\"0000019617\",\n");
        model.append("            \"0000040545\",\"0001137774\",\"0001170010\",\"0001410636\",\"0001467373\"\n");
        model.append("          ],\n");
        model.append("          \"startDate\": \"2019-01-01\",\n");
        model.append("          \"endDate\": \"2024-12-31\",\n");
        model.append("          \"filingTypes\": [\"10-K\",\"10-Q\",\"8-K\"]\n");
        model.append("        }\n");
        model.append("      }\n");
        model.append("    }\n");
        model.append("  ]\n");
        model.append("}\n");

        Properties props = new Properties();
        props.put("model", model.toString());

        System.out.println("Connecting to SEC adapter with model:");
        System.out.println("  Companies: Dow 30 (30 companies)");
        System.out.println("  Period: 2019-2024 (5 years)");
        System.out.println("  Directory: /Volumes/T9/sec-data/dow30-full");
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
            "LIMIT 20");

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
        System.out.println("  Total company-years: 150 (30 companies × 5 years)");
        System.out.println("  Seconds per company-year: " + (elapsed.toSeconds() / 150.0));
        System.out.println("=".repeat(80));
    }
}
