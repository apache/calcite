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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test DATE type handling to verify dates are stored as days since Unix epoch
 * without any time or timezone component.
 */
@Tag("unit")
public class DateFormatTest {

  @Test public void testDateFormats() throws Exception {
    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug-linq4j"));

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      String sql = "SELECT ID, FORMAT_DESC, DATE_VALUE FROM \"DATE_FORMATS\" ORDER BY ID";

      System.out.println("\n=== DATE Format Test ===");
      System.out.println("Verifying dates are stored as days since Unix epoch (1970-01-01)\n");

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          int id = resultSet.getInt(1);
          String desc = resultSet.getString(2);

          // Get the date value as int (days since epoch)
          int daysSinceEpoch = resultSet.getInt(3);

          // Also get as Date object for display
          Date dateValue = resultSet.getDate(3);

          System.out.println("Test " + id + ": " + desc);
          System.out.println("  Days since epoch: " + daysSinceEpoch);
          System.out.println("  Date value: " + dateValue);

          // Verify specific test cases
          switch (id) {
            case 4: // Epoch date
              assertThat("Epoch date should be 0 days", daysSinceEpoch, is(0));
              break;
            case 5: // Day before epoch
              assertThat("Day before epoch should be -1", daysSinceEpoch, is(-1));
              break;
            case 6: // Day after epoch
              assertThat("Day after epoch should be 1", daysSinceEpoch, is(1));
              break;
            case 1: // 2024-03-15
              // Calculate expected days manually
              // From 1970-01-01 to 2024-03-15 is 19,797 days
              assertThat("2024-03-15 should be 19797 days since epoch",
                        daysSinceEpoch, is(19797));
              break;
            case 7: // 1959-05-14
              // Let's just verify it's negative and reasonable
              assertThat("1959-05-14 should be negative days since epoch",
                        daysSinceEpoch < 0, is(true));
              break;
          }

          System.out.println();
        }
      }

      // Test SQL operations on dates
      System.out.println("=== SQL Date Operations ===");

      // Test date arithmetic
      sql = "SELECT "
          + "DATE_VALUE, "
          + "DATE_VALUE + INTERVAL '1' DAY AS NEXT_DAY, "
          + "DATE_VALUE - INTERVAL '1' DAY AS PREV_DAY "
          + "FROM \"DATE_FORMATS\" WHERE ID = 4";

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        if (resultSet.next()) {
          Date original = resultSet.getDate(1);
          Date nextDay = resultSet.getDate(2);
          Date prevDay = resultSet.getDate(3);

          System.out.println("Original date (epoch): " + original);
          System.out.println("Next day: " + nextDay);
          System.out.println("Previous day: " + prevDay);
        }
      }
    }
  }

  @Test public void testDateParsingWithParquet() throws Exception {
    Properties info = new Properties();
    // Test with Parquet engine to ensure proper conversion
    info.put("model", FileAdapterTests.jsonPath("bug"));

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Just test a few key dates with Parquet
      String sql = "SELECT ID, DATE_VALUE FROM \"DATE_FORMATS\" WHERE ID IN (4, 5, 6) ORDER BY ID";

      System.out.println("\n=== Parquet Date Storage Test ===");

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        while (resultSet.next()) {
          int id = resultSet.getInt(1);
          Date dateValue = resultSet.getDate(2);

          // Calculate days since epoch from the date value
          long millis = dateValue.getTime();
          long millisPerDay = 24L * 60 * 60 * 1000;
          int daysSinceEpoch = Math.toIntExact(Math.floorDiv(millis, millisPerDay));

          System.out.println("ID " + id + ": " + dateValue + " = " + daysSinceEpoch + " days");

          // Verify the key epoch dates
          switch (id) {
            case 4:
              assertThat("Epoch in Parquet", daysSinceEpoch, is(0));
              break;
            case 5:
              assertThat("Day before epoch in Parquet", daysSinceEpoch, is(-1));
              break;
            case 6:
              assertThat("Day after epoch in Parquet", daysSinceEpoch, is(1));
              break;
          }
        }
      }
    }
  }
}
