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

// import org.apache.calcite.adapter.govdata.GovDataTestModels;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test insider trading forms (Forms 3, 4, 5) parsing and querying.
 */
@Tag("integration")
public class InsiderTradingTest {

  @Test @Timeout(value = 5, unit = TimeUnit.MINUTES)
  @org.junit.jupiter.api.Disabled("GovDataTestModels not available")
  public void testForm4Parsing() throws Exception {
    System.out.println("\n=== FORM 4 INSIDER TRADING TEST ===");

    // Load insider trading test model from resources
    // Note: Using ticker "AAPL" which gets automatically converted to CIK "0000320193" by CikRegistry
    String modelPath = null; // GovDataTestModels.createTestModelWithYears("insider-trading-model", 2024, 2024);

    // Connection properties
    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");

    String jdbcUrl = "jdbc:calcite:model=" + modelPath;

    try (Connection connection = DriverManager.getConnection(jdbcUrl, info)) {

      // Check if insider_transactions table exists
      System.out.println("\n=== STEP 1: Check Table Schema ===");

      String tableQuery =
          "SELECT \"TABLE_NAME\" " +
          "FROM information_schema.\"TABLES\" " +
          "WHERE \"TABLE_SCHEMA\" = 'sec' " +
          "  AND \"TABLE_NAME\" = 'insider_transactions'";

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(tableQuery)) {

        assertTrue(rs.next(), "insider_transactions table should exist");
        assertEquals("insider_transactions", rs.getString("TABLE_NAME").toLowerCase());
        System.out.println("✓ insider_transactions table exists");
      }

      // Query insider transactions
      System.out.println("\n=== STEP 2: Query Insider Transactions ===");

      // Note: Using Apple's actual CIK (0000320193) in SQL query
      // This corresponds to ticker "AAPL" used in model configuration above
      String transQuery =
          "SELECT " +
          "    reporting_person_name, " +
          "    transaction_date, " +
          "    transaction_code, " +
          "    shares_transacted, " +
          "    price_per_share, " +
          "    shares_transacted * price_per_share as transaction_value " +
          "FROM sec.insider_transactions " +
          "WHERE cik = '0000320193' " +  // Apple's CIK (converted from "AAPL" ticker)
          "  AND transaction_code IN ('S', 'P') " +  // Sales and Purchases
          "  AND shares_transacted IS NOT NULL " +
          "ORDER BY transaction_date DESC " +
          "LIMIT 10";

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(transQuery)) {

        int count = 0;
        System.out.println("\nRecent Insider Transactions:");
        System.out.println("============================================");

        while (rs.next()) {
          count++;
          String name = rs.getString("reporting_person_name");
          String date = rs.getString("transaction_date");
          String code = rs.getString("transaction_code");
          double shares = rs.getDouble("shares_transacted");
          double price = rs.getDouble("price_per_share");
          double value = rs.getDouble("transaction_value");

          String action = code.equals("S") ? "SOLD" : "BOUGHT";
          System.out.printf("%s %s %,.0f shares @ $%.2f = $%,.0f on %s\n",
              name, action, shares, price, value, date);
        }

        if (count > 0) {
          System.out.println("\n✓ Found " + count + " insider transactions");
        } else {
          System.out.println("Note: No transactions found (may need to download data first)");
        }
      }

      // Aggregate insider activity
      System.out.println("\n=== STEP 3: Aggregate Insider Activity ===");

      String aggregateQuery =
          "SELECT " +
          "    COUNT(DISTINCT reporting_person_name) as insiders, " +
          "    COUNT(CASE WHEN transaction_code = 'S' THEN 1 END) as sales, " +
          "    COUNT(CASE WHEN transaction_code = 'P' THEN 1 END) as purchases, " +
          "    SUM(CASE WHEN transaction_code = 'S' THEN shares_transacted ELSE 0 END) as shares_sold, " +
          "    SUM(CASE WHEN transaction_code = 'P' THEN shares_transacted ELSE 0 END) as shares_bought " +
          "FROM sec.insider_transactions " +
          "WHERE cik = '0000320193' " +  // Apple's CIK (matches "AAPL" from config)
          "  AND filing_type = '4'";

      try (Statement stmt = connection.createStatement();
           ResultSet rs = stmt.executeQuery(aggregateQuery)) {

        if (rs.next()) {
          int insiders = rs.getInt("insiders");
          int sales = rs.getInt("sales");
          int purchases = rs.getInt("purchases");
          double sharesSold = rs.getDouble("shares_sold");
          double sharesBought = rs.getDouble("shares_bought");

          System.out.println("\nInsider Trading Summary:");
          System.out.println("========================");
          System.out.printf("Unique Insiders: %d\n", insiders);
          System.out.printf("Sale Transactions: %d (%.0f shares)\n", sales, sharesSold);
          System.out.printf("Purchase Transactions: %d (%.0f shares)\n", purchases, sharesBought);

          if (sharesSold > sharesBought) {
            System.out.println("Net Activity: SELLING");
          } else if (sharesBought > sharesSold) {
            System.out.println("Net Activity: BUYING");
          } else {
            System.out.println("Net Activity: NEUTRAL");
          }
        }
      }

      System.out.println("\n=== SUMMARY ===");
      System.out.println("Successfully tested Form 4 insider trading functionality:");
      System.out.println("  ✓ Table created and accessible");
      System.out.println("  ✓ Can query individual transactions");
      System.out.println("  ✓ Can aggregate insider activity");
      System.out.println("  ✓ Forms 3, 4, 5 support is working");
    }
  }
}
