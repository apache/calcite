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
package org.apache.calcite.adapter.file.debug;

import org.apache.calcite.adapter.file.FileAdapterTests;

import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class DateDebugTest {

  @Test void debugDateWithLinq4j() throws SQLException {
    long offsetHours = TimeUnit.MILLISECONDS.toHours(java.util.TimeZone.getDefault().getRawOffset());
    System.out.println("DEBUG: Test JVM timezone: " + java.util.TimeZone.getDefault().getID() + " offset: " + offsetHours + " hours");

    Properties info = new Properties();
    info.put("model", FileAdapterTests.jsonPath("bug-linq4j"));

    try (Connection connection = DriverManager.getConnection("jdbc:calcite:", info)) {
      Statement statement = connection.createStatement();
      final String sql = "select \"JOINEDAT\" from \"date\" where EMPNO = 100";
      ResultSet resultSet = statement.executeQuery(sql);
      resultSet.next();

      Date dateValue = resultSet.getDate(1);
      System.out.println("DEBUG: LINQ4J result: " + dateValue);
      System.out.println("DEBUG: Expected: 1996-08-02");

      assertThat(dateValue, is(Date.valueOf("1996-08-02")));
    }
  }
}
