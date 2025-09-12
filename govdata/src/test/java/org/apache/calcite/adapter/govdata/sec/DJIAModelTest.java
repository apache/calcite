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
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test DJIA 5-year model.
 */
@Tag("integration")
public class DJIAModelTest {
  @Test public void testDJIA5YearModel() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("TESTING DJIA 5-YEAR MODEL");
    System.out.println("=".repeat(80) + "\n");

    // Register the Calcite driver
    Class.forName("org.apache.calcite.jdbc.Driver");

      // Point to the model file
      String modelPath = "/Users/kennethstott/calcite/sec/src/main/resources/dji-5year-model.json";

      Properties info = new Properties();
      info.put("model", modelPath);
      info.put("lex", "ORACLE");
      info.put("unquotedCasing", "TO_LOWER");

      System.out.println("Connecting to SEC adapter with DJIA 5-year model...");
      System.out.println("This will download filings for all DJIA companies (2020-2024)");
      System.out.println("Data directory: /Volumes/T9/sec-data/dji-5year\n");

      try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
        System.out.println("✓ Connected successfully\n");

        // Get metadata to trigger download if needed
        DatabaseMetaData meta = conn.getMetaData();
        System.out.println("Database: " + meta.getDatabaseProductName());
        System.out.println("Schema: sec_dji\n");

        // Simple query to test the connection
        System.out.println("Running test query...");
        try (Statement stmt = conn.createStatement();
             ResultSet rs =
               stmt.executeQuery("SELECT COUNT(*) as total FROM financial_line_items")) {

          if (rs.next()) {
            int count = rs.getInt("total");
            System.out.println("Total financial line items: " + count);

            if (count > 0) {
              System.out.println("✓ Data loaded successfully!");
            } else {
              System.out.println("⚠ No data found - downloads may be in progress");
            }
          }
        }

        // Check if files were downloaded
        File dataDir = new File("/Volumes/T9/sec-data/dji-5year/sec");
        if (dataDir.exists()) {
          File[] xmlFiles = dataDir.listFiles((dir, name) -> name.endsWith(".xml"));
          if (xmlFiles != null && xmlFiles.length > 0) {
            System.out.println("\nDownloaded " + xmlFiles.length + " XBRL files");
            System.out.println("Sample files:");
            for (int i = 0; i < Math.min(3, xmlFiles.length); i++) {
              System.out.printf("  - %s (%,d bytes)\n",
                xmlFiles[i].getName(), xmlFiles[i].length());
            }
          }
        } else {
          System.out.println("\nData directory not found: " + dataDir);
        }

      }

      System.out.println("\n"
  + "=".repeat(80));
      System.out.println("✓ TEST COMPLETE");
      System.out.println("=".repeat(80));

  }
}
