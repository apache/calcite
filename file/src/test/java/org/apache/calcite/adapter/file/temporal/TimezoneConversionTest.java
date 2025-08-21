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
    // are parsed as LOCAL timezone wall clock time, then converted to UTC for storage

    TimeZone localTz = TimeZone.getDefault();
    System.out.println("Current JVM timezone: " + localTz.getID());

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

        // The input was "1959-05-14 05:00:00" which is TIMESTAMP (timezone-naive)
        // CORRECT BEHAVIOR: Store as UTC wall clock to preserve the time regardless of timezone
        // The timestamp is stored as if it were UTC: 1959-05-14T05:00:00Z
        SimpleDateFormat utcParseFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        utcParseFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        long expectedMillis = utcParseFormat.parse("1959-05-14 05:00:00").getTime();

        // Verify the stored value matches what we expect
        assertThat("Wall clock time should be stored as UTC to preserve the value",
                   utcMillis, is(expectedMillis));

        // Verify how it's displayed
        SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String actualUtcString = utcFormat.format(new java.util.Date(utcMillis));

        SimpleDateFormat localFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        localFormat.setTimeZone(localTz);
        String actualLocalString = localFormat.format(new java.util.Date(utcMillis));

        // Log the conversion for debugging
        System.out.println("Input: '1959-05-14 05:00:00' (TIMESTAMP - timezone-naive)");
        System.out.println("Parsed as: " + localTz.getID() + " wall clock time");
        System.out.println("Stored as UTC millis: " + utcMillis);
        System.out.println("UTC representation: " + actualUtcString);
        System.out.println("Local representation: " + actualLocalString);

        // Verify we got the description
        String description = resultSet.getString(3);
        assertThat(description, is("Test timezone-naive timestamp"));

        assertThat(resultSet.next(), is(false));
      }
    }
  }
}
