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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Wikipedia DJI table directly via file adapter.
 */
@Tag("unit")
public class DirectWikiTest {

  @Test
  public void testWikipediaDirectQuery() throws Exception {
    System.out.println("\n" + "=".repeat(80));
    System.out.println("TESTING DIRECT WIKIPEDIA QUERY");
    System.out.println("=".repeat(80) + "\n");

    // Register the Calcite driver
    Class.forName("org.apache.calcite.jdbc.Driver");

    String modelJson = "{"
        + "\"version\":\"1.0\","
        + "\"defaultSchema\":\"wiki\","
        + "\"schemas\":[{"
        + "  \"name\":\"wiki\","
        + "  \"type\":\"custom\","
        + "  \"factory\":\"org.apache.calcite.adapter.file.FileSchemaFactory\","
        + "  \"operand\":{"
        + "    \"ephemeralCache\":true,"
        + "    \"tables\":[{"
        + "      \"name\":\"dow30\","
        + "      \"url\":\"https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average\","
        + "      \"selector\":\"table.wikitable\","
        + "      \"index\":0,"
        + "      \"fields\":["
        + "        {\"th\":\"Company\",\"name\":\"company\",\"selector\":\"a\",\"selectedElement\":0},"
        + "        {\"th\":\"Symbol\",\"name\":\"ticker\"}"
        + "      ]"
        + "    }]"
        + "  }"
        + "}]}";

    Properties info = new Properties();
    info.put("model", "inline:" + modelJson);
    info.put("lex", "ORACLE");
    info.put("unquotedCasing", "TO_LOWER");

    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
         Statement stmt = conn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT ticker, company FROM wiki.dow30")) {

      System.out.println("Ticker | Company");
      System.out.println("-".repeat(50));
      
      int count = 0;
      while (rs.next() && count < 35) {
        String ticker = rs.getString("ticker");
        String company = rs.getString("company");
        System.out.printf("%-6s | %s\n", ticker, company);
        count++;
      }

      System.out.println("-".repeat(50));
      System.out.println("Found " + count + " companies");
      
      assertTrue(count > 0, "Should find at least one company");
      assertTrue(count >= 25 && count <= 35, 
          "Should find approximately 30 companies, found " + count);

      System.out.println("\n✓ SUCCESS: Wikipedia query returned " + count + " DJI constituents");
    }
    
    System.out.println("=".repeat(80));
  }
}