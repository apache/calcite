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

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for DJI Net Income Analysis using XBRL financial facts.
 */
@Tag("integration")
public class DJINetIncomeAnalysisTest {

  @Test
  public void testDJINetIncomeAnalysis() throws Exception {
    System.out.println("================================================================================");
    System.out.println("DJI AVERAGE NET INCOME ANALYSIS");
    System.out.println("================================================================================");
    System.out.println();
    
    // Create model configuration for DJI companies
    String model = String.format("{"
        + "\"version\": \"1.0\","
        + "\"defaultSchema\": \"SEC\","
        + "\"schemas\": [{"
        + "\"name\": \"SEC\","
        + "\"type\": \"custom\","
        + "\"factory\": \"org.apache.calcite.adapter.sec.SecSchemaFactory\","
        + "\"operand\": {"
        + "\"directory\": \"/Volumes/T9/calcite-test-data/DJINetIncome_%d\","
        + "\"ciks\": [\"_DJI\"],"
        + "\"startYear\": 2022,"
        + "\"endYear\": 2024,"
        + "\"autoDownload\": true,"
        + "\"filingTypes\": [\"10-K\"]"
        + "}}]}", System.currentTimeMillis());
    
    Properties props = new Properties();
    props.put("model", model);
    props.put("lex", "ORACLE");
    props.put("unquotedCasing", "TO_LOWER");
    
    System.out.println("Connecting to SEC adapter for DJI companies...");
    System.out.println("Configuration:");
    System.out.println("  - Companies: _DJI group (30 Dow Jones companies)");
    System.out.println("  - Years: 2022-2024");
    System.out.println("  - Filing Types: 10-K (annual reports)");
    System.out.println("  - Auto Download: enabled");
    System.out.println();
    
    Class.forName("org.apache.calcite.jdbc.Driver");
    
    try (Connection conn = DriverManager.getConnection("jdbc:calcite:", props)) {
      System.out.println("✓ Connected to SEC adapter");
      System.out.println();
      
      // First, show what tables are available
      System.out.println("Available tables:");
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(
               "SELECT \"TABLE_NAME\" FROM information_schema.tables " +
               "WHERE \"TABLE_SCHEMA\" = 'SEC' ORDER BY \"TABLE_NAME\"")) {
        
        while (rs.next()) {
          System.out.println("  - " + rs.getString("TABLE_NAME"));
        }
      }
      System.out.println();
      
      // Execute net income analysis
      System.out.println("================================================================================");
      System.out.println("NET INCOME ANALYSIS BY FISCAL YEAR");
      System.out.println("================================================================================");
      
      String netIncomeQuery = 
          "SELECT " +
          "    fiscal_year, " +
          "    COUNT(DISTINCT cik) as company_count, " +
          "    ROUND(AVG(net_income_value), 2) as avg_net_income_millions, " +
          "    ROUND(MIN(net_income_value), 2) as min_net_income_millions, " +
          "    ROUND(MAX(net_income_value), 2) as max_net_income_millions " +
          "FROM ( " +
          "    SELECT DISTINCT " +
          "        f.cik, " +
          "        f.fiscal_year, " +
          "        CASE  " +
          "            WHEN f.numeric_value IS NOT NULL THEN f.numeric_value / 1000000.0 " +
          "            ELSE NULL " +
          "        END as net_income_value " +
          "    FROM xbrl_facts f " +
          "    WHERE  " +
          "        (LOWER(f.concept) LIKE '%netincome%'  " +
          "         OR LOWER(f.concept) LIKE '%net_income%' " +
          "         OR f.concept IN ('NetIncomeLoss', 'ProfitLoss')) " +
          "        AND f.fiscal_year >= 2022 " +
          "        AND f.numeric_value IS NOT NULL " +
          "        AND f.numeric_value != 0 " +
          "        AND f.numeric_value > 100000000 " +
          ") net_income_data " +
          "WHERE net_income_value IS NOT NULL " +
          "GROUP BY fiscal_year " +
          "HAVING COUNT(DISTINCT cik) >= 2 " +
          "ORDER BY fiscal_year DESC";
      
      try (Statement stmt = conn.createStatement();
           ResultSet rs = stmt.executeQuery(netIncomeQuery)) {
        
        System.out.printf("%-12s %-10s %-20s %-20s %-20s%n", 
            "Fiscal Year", "Companies", "Avg Net Income ($M)", "Min Net Income ($M)", "Max Net Income ($M)");
        System.out.println("-".repeat(90));
        
        boolean hasResults = false;
        while (rs.next()) {
          hasResults = true;
          System.out.printf("%-12d %-10d $%-19.2f $%-19.2f $%-19.2f%n",
              rs.getInt("fiscal_year"),
              rs.getInt("company_count"),
              rs.getDouble("avg_net_income_millions"),
              rs.getDouble("min_net_income_millions"),
              rs.getDouble("max_net_income_millions"));
        }
        
        if (!hasResults) {
          // Check if xbrl_facts table exists but has no data
          try (Statement checkStmt = conn.createStatement();
               ResultSet checkRs = checkStmt.executeQuery("SELECT COUNT(*) FROM xbrl_facts")) {
            if (checkRs.next()) {
              System.out.println("XBRL facts table exists but contains no net income data matching criteria");
              System.out.println("Total XBRL facts: " + checkRs.getInt(1));
            }
          }
        }
        
        assertTrue(hasResults, "Should find net income data for DJI companies");
      }
      
      System.out.println();
      System.out.println("✓ Net income analysis completed successfully");
      
    } catch (SQLException e) {
      System.err.println("SQL Error: " + e.getMessage());
      throw e;
    }
  }
}