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

import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test TIME type handling to verify times are stored as milliseconds since midnight
 * without any timezone component.
 */
public class TimeFormatTest {

  @Test public void testTimeFormats() throws Exception {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug-linq4j"));

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      String sql = "SELECT ID, FORMAT_DESC, TIME_VALUE FROM \"TIME_FORMATS\" ORDER BY ID";

      System.out.println("\n=== TIME Format Test ===");
      System.out.println("Verifying times are stored as milliseconds since midnight\n");

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          int id = resultSet.getInt(1);
          String desc = resultSet.getString(2);

          // Get time as milliseconds (int)
          int millisSinceMidnight = resultSet.getInt(3);

          // Also get as Time object for display
          Time timeValue = resultSet.getTime(3);

          System.out.println("Test " + id + ": " + desc);
          System.out.println("  Millis since midnight: " + millisSinceMidnight);
          System.out.println("  Time value: " + timeValue);

          // Calculate hours, minutes, seconds, millis from the integer
          int hours = millisSinceMidnight / (60 * 60 * 1000);
          int remainingMillis = millisSinceMidnight % (60 * 60 * 1000);
          int minutes = remainingMillis / (60 * 1000);
          remainingMillis = remainingMillis % (60 * 1000);
          int seconds = remainingMillis / 1000;
          int millis = remainingMillis % 1000;

          System.out.println(
              "  Calculated: " + String.format("%02d:%02d:%02d.%03d",
                                                              hours, minutes, seconds, millis));

          // Verify specific test cases
          switch (id) {
            case 1: // Midnight
              assertThat("Midnight should be 0 millis", millisSinceMidnight, is(0));
              break;
            case 2: // 00:00:01
              assertThat("One second past midnight", millisSinceMidnight, is(1000));
              break;
            case 3: // 00:01:00
              assertThat("One minute past midnight", millisSinceMidnight, is(60 * 1000));
              break;
            case 4: // 01:00:00
              assertThat("One hour past midnight", millisSinceMidnight, is(60 * 60 * 1000));
              break;
            case 5: // 02:00:00
              assertThat("Two hours past midnight", millisSinceMidnight, is(2 * 60 * 60 * 1000));
              break;
            case 6: // 00:10:00
              assertThat("Ten minutes past midnight", millisSinceMidnight, is(10 * 60 * 1000));
              break;
            case 7: // 12:00:00 (noon)
              assertThat("Noon", millisSinceMidnight, is(12 * 60 * 60 * 1000));
              break;
            case 8: // 13:00:00 (1 PM)
              assertThat("1 PM", millisSinceMidnight, is(13 * 60 * 60 * 1000));
              break;
            case 9: // 23:59:59
              assertThat("11:59:59 PM", millisSinceMidnight,
                        is(23 * 60 * 60 * 1000 + 59 * 60 * 1000 + 59 * 1000));
              break;
            case 10: // 10:30:45.123
              assertThat("With milliseconds", millisSinceMidnight,
                        is(10 * 60 * 60 * 1000 + 30 * 60 * 1000 + 45 * 1000 + 123));
              break;
          }

          System.out.println();
        }
      }

      // Test SQL operations on times
      System.out.println("=== SQL Time Operations ===");

      // Test time comparison
      sql = "SELECT COUNT(*) FROM \"TIME_FORMATS\" "
          + "WHERE TIME_VALUE = TIME '12:00:00'";

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        if (resultSet.next()) {
          int count = resultSet.getInt(1);
          System.out.println("Rows with time 12:00:00 (noon): " + count);
        }
      }

      // Test time arithmetic
      sql = "SELECT "
          + "TIME_VALUE, "
          + "CAST(TIME_VALUE AS VARCHAR) AS STRING_VALUE "
          + "FROM \"TIME_FORMATS\" WHERE ID = 7";

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        if (resultSet.next()) {
          Time original = resultSet.getTime(1);
          String stringValue = resultSet.getString(2);

          System.out.println("Original time (noon): " + original);
          System.out.println("As string: " + stringValue);
        }
      }
    }
  }

  @Test public void testTimeSelectOutput() throws Exception {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug-linq4j"));

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Test what SQL SELECT actually returns
      String sql = "SELECT ID, FORMAT_DESC, TIME_VALUE, "
          + "CAST(TIME_VALUE AS VARCHAR) AS STRING_VALUE "
          + "FROM \"TIME_FORMATS\" "
          + "WHERE ID IN (1, 4, 7, 9, 10) "
          + "ORDER BY ID";

      System.out.println("\n=== SQL SELECT Time Output ===");
      System.out.println("Testing what SQL SELECT returns for TIME values\n");

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          int id = resultSet.getInt(1);
          String desc = resultSet.getString(2);

          // Get time using different methods
          Time timeValue = resultSet.getTime(3);
          String stringValue = resultSet.getString(4);
          int millisSinceMidnight = resultSet.getInt(3);

          System.out.println("Test " + id + ": " + desc);
          System.out.println("  getTime(): " + timeValue);
          System.out.println("  CAST AS VARCHAR: " + stringValue);
          System.out.println("  Millis since midnight (getInt): " + millisSinceMidnight);
          System.out.println();
        }
      }
    }
  }
}
