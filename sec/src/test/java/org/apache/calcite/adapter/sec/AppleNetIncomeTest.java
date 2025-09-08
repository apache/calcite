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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test SEC adapter with just Apple to verify XBRL processing works.
 */
@Tag("integration")
public class AppleNetIncomeTest {

  @Test public void testAppleNetIncomeQuery() throws Exception {
    // Load JDBC driver
    Class.forName("org.apache.calcite.jdbc.Driver");

    // Create model for just Apple
    String modelPath = getClass().getClassLoader().getResource("apple-test-model.json").getPath();

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("model", modelPath);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      // Query for Apple's net income data from converted XBRL
      String sql = "SELECT concept, numeric_value, filing_date " +
          "FROM sec.xbrl_facts " +
          "WHERE cik = '0000320193' " +
          "AND concept LIKE '%NetIncome%' " +
          "ORDER BY filing_date DESC " +
          "LIMIT 10";

      try (PreparedStatement stmt = connection.prepareStatement(sql);
           ResultSet rs = stmt.executeQuery()) {

        System.out.println("Apple Net Income data from XBRL:");
        int count = 0;
        while (rs.next()) {
          count++;
          System.out.println("  " + rs.getString("filing_date") +
              " - " + rs.getString("concept") +
              ": $" + rs.getDouble("numeric_value") / 1000000.0 + "M");
        }

        assertTrue(count > 0, "Should find net income data for Apple");
      }
    }
  }
}
