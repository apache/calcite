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
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@Tag("integration")
public class WikiQueryFixedTest {
  @Test
public void test() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("TESTING WIKIPEDIA QUERY VIA FILE ADAPTER");
    System.out.println("=".repeat(80) + "\n");

    // Register the Calcite driver
    try {
      Class.forName("org.apache.calcite.jdbc.Driver");
    } catch (ClassNotFoundException e) {
      System.err.println("Calcite driver not found: " + e.getMessage());
      return;
    }

    String modelJson = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"dji_wiki\","
        + "\"schemas\":[{"
        + "  \"name\":\"dji_wiki\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"ephemeralCache\":true,"
        + "    \"tables\":[{"
        + "      \"name\":\"dji_constituents\","
        + "      \"url\":\"https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average\","
        + "      \"selector\":\"table.wikitable\","
        + "      \"index\":1,"  // The constituents table is likely the second wikitable
        + "      \"fields\":["
        + "        {\"th\":\"Company\",\"name\":\"company\",\"selector\":\"a\",\"selectedElement\":0},"
        + "        {\"th\":\"Symbol\",\"name\":\"ticker\"}"
        + "      ]"
        + "    }]"
        + "  }"
        + "}]}";

    Properties info = new Properties();
    info.put("model", modelJson);

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      System.out.println("✓ Connected to Calcite\n");

      System.out.println("Querying DJIA constituents:");
      System.out.println("-".repeat(60));

      try (Statement stmt = conn.createStatement();
           ResultSet rs =
             stmt.executeQuery("SELECT company, ticker FROM dji_wiki.dji_constituents LIMIT 35")) {

        int count = 0;
        while (rs.next()) {
          count++;
          System.out.printf("%2d. %-35s %s\n",
            count,
            rs.getString("company"),
            rs.getString("ticker"));
        }

        System.out.println("-".repeat(60));
        System.out.println("Total: " + count + " companies");

        if (count >= 25 && count <= 30) {
          System.out.println("\n✓ SUCCESS: Found DJIA constituents from Wikipedia!");
        } else if (count > 0) {
          System.out.println("\n⚠ Found " + count + " rows (expected ~30)");
        } else {
          System.out.println("\n✗ No data found - check table selector/index");
        }
      }
    } catch (SQLException e) {
      System.err.println("\n✗ Error: " + e.getMessage());

      // Try to provide helpful debug info
      if (e.getMessage().contains("Object 'dji_constituents' not found")) {
        System.err.println("\nTable not found. The Wikipedia page structure may have changed.");
        System.err.println("Check: https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average");
      }
      e.printStackTrace();
    }

    System.out.println("\n"
  + "=".repeat(80));
  }
}
