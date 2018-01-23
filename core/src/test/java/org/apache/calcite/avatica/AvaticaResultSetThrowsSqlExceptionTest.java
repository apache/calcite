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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link AvaticaResultSet}, to make sure we drop
 * {@link SQLException} for non supported function: {@link ResultSet#previous}
 * and {@link ResultSet#updateNull}, for example.
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

  /**
   * Auxiliary method returning a result set on a test table.
   *
   * @return a result set on a test table.
   * @throws SQLException in case of database error
   */
  private ResultSet getResultSet() throws SQLException {
    Properties properties = new Properties();
    properties.setProperty("timeZone", "GMT");

    final TestDriver driver = new TestDriver();
    final Connection connection = driver.connect("jdbc:test", properties);

    return connection.createStatement().executeQuery("SELECT * FROM TABLE");
  }

  @Test public void testPrevious() throws SQLException {
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

  @Test public void testUpdateNull() throws SQLException {
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

  @Test public void testCommonCursorStates() throws SQLException {
    final ResultSet resultSet = getResultSet();

    // right after statement execution, result set is before first row
    assertTrue(resultSet.isBeforeFirst());

    // checking that return values of next and isAfterLast are coherent
    for (int c = 0; c < 3 && !resultSet.isAfterLast(); ++c) {
      assertTrue(resultSet.next() != resultSet.isAfterLast());
    }

    // result set is not closed yet, despite fully consumed
    assertFalse(resultSet.isClosed());

    resultSet.close();

    // result set is now closed
    assertTrue(resultSet.isClosed());

    // once closed, next should fail
    thrown.expect(SQLException.class);
    resultSet.next();
  }

  /**
   * Auxiliary method for testing column access.
   *
   * @param resultSet Result set
   * @param index Index of the column to be accessed
   * @param shouldThrow Whether the column access should throw an exception
   * @return Whether the method invocation succeeded
   * @throws SQLException in case of database error
   */
  private boolean getColumn(final ResultSet resultSet, final int index,
      final boolean shouldThrow) throws SQLException {
    try {
      switch (index) {
      case 1: // BOOLEAN
        resultSet.getBoolean(index);
        break;
      case 2: // TINYINT
        resultSet.getByte(index);
        break;
      case 3: // SMALLINT
        resultSet.getShort(index);
        break;
      case 4: // INTEGER
        resultSet.getInt(index);
        break;
      case 5: // BIGINT
        resultSet.getLong(index);
        break;
      case 6: // REAL
        resultSet.getFloat(index);
        break;
      case 7: // FLOAT
        resultSet.getDouble(index);
        break;
      case 8: // VARCHAR
        resultSet.getString(index);
        break;
      case 9: // DATE
        resultSet.getDate(index);
        break;
      case 10: // TIME
        resultSet.getTime(index);
        break;
      case 11: // TIMESTAMP
        resultSet.getTimestamp(index);
        break;
      default:
        resultSet.getObject(index);
      }
    } catch (SQLException e) {
      if (!shouldThrow) {
        throw e;
      }
      return true;
    }

    return !shouldThrow;
  }

  @Test public void testGetColumnsBeforeNext() throws SQLException {
    try (ResultSet resultSet = getResultSet()) {
      // we have not called next, so each column getter should throw SQLException
      for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
        assertTrue(getColumn(resultSet, i, true));
      }
    }
  }

  @Test public void testGetColumnsAfterNext() throws SQLException {
    try (ResultSet resultSet = getResultSet()) {
      // result set is composed by 1 row, we call next before accessing columns
      resultSet.next();

      // after calling next, column getters should succeed
      for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
        assertTrue(getColumn(resultSet, i, false));
      }
    }
  }

  @Test public void testGetColumnsAfterLast() throws SQLException {
    try (ResultSet resultSet = getResultSet()) {
      // these two steps move the cursor after the last row
      resultSet.next();
      resultSet.next();

      // the cursor being after the last row, column getters should fail
      for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); ++i) {
        assertTrue(getColumn(resultSet, i, true));
      }
    }
  }

}

// End AvaticaResultSetThrowsSqlExceptionTest.java
