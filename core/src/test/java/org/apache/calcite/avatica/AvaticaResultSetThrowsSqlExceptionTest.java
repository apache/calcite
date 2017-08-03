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
package org.apache.calcite.avatica;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;

/**
 * Test class for AvaticaResultSet, make sure we drop SQLException
 * for non supported function: previous and testUpdateNull, for example
 */
public class AvaticaResultSetThrowsSqlExceptionTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  /**
   * A fake test driver for test.
   */
  private static final class TestDriver extends UnregisteredDriver {

    @Override protected DriverVersion createDriverVersion() {
      return new DriverVersion("test", "test 0.0.0", "test", "test 0.0.0", false, 0, 0, 0, 0);
    }

    @Override protected String getConnectStringPrefix() {
      return "jdbc:test";
    }

    @Override public Meta createMeta(AvaticaConnection connection) {
      return new AvaticaResultSetConversionsTest.TestMetaImpl(connection);
    }
  }

  @Test
  public void testPrevious() throws SQLException {
    Properties properties = new Properties();
    properties.setProperty("timeZone", "GMT");

    final TestDriver driver = new TestDriver();
    try (Connection connection = driver.connect("jdbc:test", properties);
         ResultSet resultSet =
             connection.createStatement().executeQuery("SELECT * FROM TABLE")) {
      thrown.expect(SQLFeatureNotSupportedException.class);
      resultSet.previous();
    }
  }

  @Test
  public void testUpdateNull() throws SQLException {
    Properties properties = new Properties();
    properties.setProperty("timeZone", "GMT");

    final TestDriver driver = new TestDriver();
    try (Connection connection = driver.connect("jdbc:test", properties);
         ResultSet resultSet =
             connection.createStatement().executeQuery("SELECT * FROM TABLE")) {
      thrown.expect(SQLFeatureNotSupportedException.class);
      resultSet.updateNull(1);
    }
  }
}

// End AvaticaResultSetThrowsSqlExceptionTest.java
