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
package org.apache.calcite.adapter.file.temporal;

import org.apache.calcite.adapter.file.FileAdapterTests;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

/**
 * Test what SQL SELECT returns for DATE values.
 */
@Tag("unit")
public class DateSelectTest {

  @Test public void testDateSelectOutput() throws Exception {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug-linq4j"));
    info.put("lex", "ORACLE");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Test key dates
      String sql = "SELECT \"id\", \"format_desc\", \"date_value\", "
          + "CAST(\"date_value\" AS VARCHAR) AS STRING_VALUE "
          + "FROM \"date_formats\" "
          + "WHERE \"id\" IN (1, 4, 5, 6, 7) "
          + "ORDER BY \"id\"";

      System.out.println("\n=== SQL SELECT Date Output ===");
      System.out.println("Testing what SQL SELECT returns for DATE values\n");

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          int id = resultSet.getInt(1);
          String desc = resultSet.getString(2);

          // Get date using different methods
          Date dateValue = resultSet.getDate(3);
          String stringValue = resultSet.getString(4);
          int daysSinceEpoch = resultSet.getInt(3); // Get as int directly from DATE column

          System.out.println("Test " + id + ": " + desc);
          System.out.println("  getDate(): " + dateValue);
          System.out.println("  CAST AS VARCHAR: " + stringValue);
          System.out.println("  Days since epoch (getInt): " + daysSinceEpoch);
          System.out.println();
        }
      }

      // Test date comparison
      System.out.println("=== Date Comparison Test ===");
      sql = "SELECT COUNT(*) FROM \"date_formats\" "
          + "WHERE \"date_value\" = DATE '2024-03-15'";

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        if (resultSet.next()) {
          int count = resultSet.getInt(1);
          System.out.println("Rows with date 2024-03-15: " + count);
        }
      }

      // Test epoch date
      sql = "SELECT COUNT(*) FROM \"date_formats\" "
          + "WHERE \"date_value\" = DATE '1970-01-01'";

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        if (resultSet.next()) {
          int count = resultSet.getInt(1);
          System.out.println("Rows with epoch date: " + count);
        }
      }
    }
  }
}
