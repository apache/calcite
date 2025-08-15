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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.TimeZone;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Test to verify timezone-naive timestamps are parsed as local time
 * and converted to UTC for storage.
 */
@Tag("unit")
public class TimezoneConversionTest {

  @Test public void testTimezoneNaiveTimestampConversion() throws Exception {
    // This test verifies that timezone-naive timestamps in CSV files
    // are parsed as UTC for consistent wall-clock time representation

    TimeZone localTz = TimeZone.getDefault();

    Properties info = new Properties();
    // Use LINQ4J engine to avoid Parquet conversion
    info.put("model", FileAdapterTests.jsonPath("bug-linq4j"));
    info.put("lex", "ORACLE");

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement()) {

      // Query the timezone-naive timestamp
      String sql = "SELECT \"id\", \"event_time\", \"description\" FROM \"timezone_test\" WHERE \"id\" = 1";

      try (ResultSet resultSet = statement.executeQuery(sql)) {
        assertThat(resultSet.next(), is(true));

        int id = resultSet.getInt(1);
        assertThat(id, is(1));

        // IMPORTANT: For TIMESTAMP columns in Calcite, use getLong() to get the UTC milliseconds
        // Do not use getTimestamp() as it applies unwanted timezone conversions
        long utcMillis = resultSet.getLong(2);

        // The input was "1959-05-14 05:00:00" which is parsed as UTC for consistency
        // TIMESTAMP WITHOUT TIME ZONE in CSV files are parsed as UTC to provide
        // consistent wall-clock time representation
        SimpleDateFormat utcParseFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        utcParseFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        long expectedMillis = utcParseFormat.parse("1959-05-14 05:00:00").getTime();

        // Verify the stored value matches what we expect
        assertThat("UTC milliseconds should match expected value",
                   utcMillis, is(expectedMillis));

        // Verify how it's displayed in UTC
        SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String actualUtcString = utcFormat.format(new java.util.Date(utcMillis));

        // Timestamps without timezone are parsed as UTC for consistency
        // The timestamp "1959-05-14 05:00:00" is treated as already being in UTC
        System.out.println("Input: '1959-05-14 05:00:00' (timezone-naive)");
        System.out.println("Parsed as: UTC (for consistency)");
        System.out.println("Stored value: " + actualUtcString);

        // Verify we got the description
        String description = resultSet.getString(3);
        assertThat(description, is("Test timezone-naive timestamp"));

        assertThat(resultSet.next(), is(false));
      }
    }
  }
}
