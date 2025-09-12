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
public class DirectWikiQueryTest {
  @Test
public void test() throws Exception {
    System.out.println("\n"
  + "=".repeat(80));
    System.out.println("TESTING DIRECT WIKIPEDIA QUERY VIA FILE ADAPTER");
    System.out.println("=".repeat(80) + "\n");

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
        + "      \"index\":1,"  // Try index 1 (second table)
        + "      \"fields\":["
        + "        {\"th\":\"Company\",\"name\":\"company\",\"selector\":\"a\",\"selectedElement\":0},"
        + "        {\"th\":\"Exchange\",\"name\":\"exchange\"},"
        + "        {\"th\":\"Symbol\",\"name\":\"ticker\"},"
        + "        {\"th\":\"Industry\",\"name\":\"industry\"},"
        + "        {\"th\":\"Date added\",\"name\":\"date_added\"}"
        + "      ]"
        + "    }]"
        + "  }"
        + "}]}";

    Properties info = new Properties();
    info.put("model", modelJson);

    // Register driver
    Class.forName("org.apache.calcite.jdbc.Driver");
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info)) {
      System.out.println("✓ Connected to Calcite with Wikipedia model\n");

      // First, let's see what tables are available
      DatabaseMetaData meta = conn.getMetaData();
      try (ResultSet tables = meta.getTables(null, "dji_wiki", "%", null)) {
        System.out.println("Available tables in dji_wiki schema:");
        while (tables.next()) {
          System.out.println("  - " + tables.getString("TABLE_NAME"));
        }
      }

      // Now query the DJIA constituents
      System.out.println("\nQuerying DJIA constituents from Wikipedia:");
      System.out.println("-".repeat(60));

      try (Statement stmt = conn.createStatement();
           ResultSet rs =
             stmt.executeQuery("SELECT company, ticker, exchange, industry FROM dji_wiki.dji_constituents")) {

        int count = 0;
        while (rs.next()) {
          count++;
          System.out.printf("%2d. %-30s %6s %8s %s\n",
            count,
            rs.getString("company"),
            rs.getString("ticker"),
            rs.getString("exchange"),
            rs.getString("industry"));
        }

        System.out.println("-".repeat(60));
        System.out.println("Total companies found: " + count);

        if (count == 30) {
          System.out.println("\n✓ SUCCESS: Found all 30 DJIA constituents!");
        } else if (count > 0) {
          System.out.println("\n✓ PARTIAL: Found " + count + " constituents (expected 30)");
        } else {
          System.out.println("\n✗ FAILURE: No constituents found");
        }
      }
    } catch (SQLException e) {
      System.err.println("\n✗ SQL Error: " + e.getMessage());
      e.printStackTrace();
    } catch (Exception e) {
      System.err.println("\n✗ Error: " + e.getMessage());
      e.printStackTrace();
    }

    System.out.println("\n"
  + "=".repeat(80));
  }
}
