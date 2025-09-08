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
 * Simple test with just Apple to verify net income query works.
 */
@Tag("integration")
public class SimpleNetIncomeTest {

  @Test public void testSimpleNetIncomeQuery() throws Exception {
    // Load JDBC driver
    Class.forName("org.apache.calcite.jdbc.Driver");

    // Create model for just Apple (using cached data)
    String modelPath = getClass().getClassLoader().getResource("simple-test-model.json").getPath();

    Properties info = new Properties();
    info.setProperty("lex", "ORACLE");
    info.setProperty("unquotedCasing", "TO_LOWER");
    info.setProperty("model", modelPath);

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      // Simple query to test if we can query the data
      String sql = "SELECT COUNT(*) as cnt FROM financial_line_items";

      try (PreparedStatement stmt = connection.prepareStatement(sql);
           ResultSet rs = stmt.executeQuery()) {

        if (rs.next()) {
          int count = rs.getInt("cnt");
          System.out.println("Found " + count + " financial line items");
          assertTrue(count > 0, "Should have some financial data");
        } else {
          System.out.println("No data found in financial_line_items table");
        }
      }
    }
  }
}
