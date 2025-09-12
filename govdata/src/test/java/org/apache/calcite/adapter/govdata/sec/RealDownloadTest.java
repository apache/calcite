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
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for REAL SEC EDGAR downloads.
 */
@Tag("integration")
public class RealDownloadTest {

  @Test public void testRealSecDownloads() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("TESTING REAL SEC EDGAR DOWNLOADS");
    System.out.println("=".repeat(80) + "\n");

    Instant start = Instant.now();

    // Create model with REAL downloads enabled
    String model = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"sec\","
        + "\"schemas\":[{"
        + "\"name\":\"sec\","
        + "\"type\":\"custom\","
        + "\"factory\":\"org.apache.calcite.adapter.govdata.GovDataSchemaFactory\","
        + "\"operand\":{"
        + "\"directory\":\"/Volumes/T9/sec-data/test-real\","
        + "\"enableSecProcessing\":true,"
        + "\"processSecOnInit\":true,"
        + "\"secSourceDirectory\":\"/Volumes/T9/sec-data/test-real/sec\","
        + "\"edgarSource\":{"
        + "\"autoDownload\":true,"
        + "\"useRealDownloads\":true,"  // ENABLE REAL DOWNLOADS
        + "\"ciks\":[\"0000320193\"],"  // Just Apple for quick test
        + "\"startDate\":\"2024-06-01\","
        + "\"endDate\":\"2024-06-30\","  // Just one month
        + "\"filingTypes\":[\"10-Q\"]"  // Just quarterly report
        + "}}}]}";

    Properties props = new Properties();
    props.put("model", model);

    System.out.println("Testing REAL SEC download for:");
    System.out.println("  Company: Apple (CIK: 0000320193)");
    System.out.println("  Period: June 2024");
    System.out.println("  Filing Type: 10-Q");
    System.out.println("  Directory: /Volumes/T9/sec-data/test-real");
    System.out.println("");

    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");

    // Connect - this triggers downloads
    Connection conn = DriverManager.getConnection("jdbc:calcite:", props);
    assertNotNull(conn, "Connection should not be null");

    System.out.println("Connected! Checking downloaded files...");

    // Check if files were actually downloaded
    File secDir = new File("/Volumes/T9/sec-data/test-real/sec");
    assertTrue(secDir.exists(), "SEC directory should exist");

    File[] xmlFiles = secDir.listFiles((dir, name) -> name.endsWith(".xml"));
    assertNotNull(xmlFiles, "Should have XML files");
    assertTrue(xmlFiles.length > 0, "Should have at least one XML file");

    // Check file size to ensure it's real data (not synthetic)
    for (File xmlFile : xmlFiles) {
      System.out.println("Downloaded: " + xmlFile.getName() + " (" + xmlFile.length() + " bytes)");
      assertTrue(xmlFile.length() > 10000,
                 "File should be larger than synthetic test data (>10KB)");
    }

    // Try to query the data
    Statement stmt = conn.createStatement();
    ResultSet rs =
        stmt.executeQuery("SELECT cik, filing_type, filing_date, COUNT(*) as line_items " +
        "FROM sec.financial_line_items " +
        "WHERE cik = '0000320193' " +
        "GROUP BY cik, filing_type, filing_date");

    System.out.println("\nQuery Results:");
    System.out.println("-".repeat(60));
    int rowCount = 0;
    while (rs.next()) {
      System.out.printf("CIK: %s | Type: %s | Date: %s | Items: %d\n",
          rs.getString("cik"),
          rs.getString("filing_type"),
          rs.getString("filing_date"),
          rs.getInt("line_items"));
      rowCount++;
    }

    assertTrue(rowCount > 0, "Should have query results");

    rs.close();
    stmt.close();
    conn.close();

    Duration elapsed = Duration.between(start, Instant.now());

    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("TEST RESULTS:");
    System.out.println("  Total time: " + elapsed.toSeconds() + " seconds");
    System.out.println("  Files downloaded: " + xmlFiles.length);
    System.out.println("  Query rows: " + rowCount);
    System.out.println("  ✓ SUCCESS: Real SEC data downloaded and processed");
    System.out.println("=".repeat(80));
  }
}
