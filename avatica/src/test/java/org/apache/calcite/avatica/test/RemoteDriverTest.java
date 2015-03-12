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
package org.apache.calcite.avatica.test;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.MockJsonService;
import org.apache.calcite.avatica.remote.Service;

import net.hydromatic.scott.data.hsqldb.ScottHsqldb;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for Avatica Remote JDBC driver.
 */
public class RemoteDriverTest {
  public static final String MJS =
      MockJsonService.Factory.class.getName();

  public static final String LJS =
      LocalJdbcServiceFactory.class.getName();

  public static final String QRJS =
      QuasiRemoteJdbcServiceFactory.class.getName();

  private static final ConnectionSpec CONNECTION_SPEC = ConnectionSpec.HSQLDB;

  private Connection mjs() throws SQLException {
    return DriverManager.getConnection("jdbc:avatica:remote:factory=" + MJS);
  }

  private Connection ljs() throws SQLException {
    return DriverManager.getConnection("jdbc:avatica:remote:factory=" + QRJS);
  }

  @Test public void testRegister() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:");
    assertThat(connection.isClosed(), is(false));
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test public void testSchemas() throws Exception {
    final Connection connection = mjs();
    final ResultSet resultSet =
        connection.getMetaData().getSchemas(null, null);
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertTrue(metaData.getColumnCount() >= 2);
    assertEquals("TABLE_CATALOG", metaData.getColumnName(1));
    assertEquals("TABLE_SCHEM", metaData.getColumnName(2));
    resultSet.close();
    connection.close();
  }

  @Test public void testTables() throws Exception {
    final Connection connection = mjs();
    final ResultSet resultSet =
        connection.getMetaData().getTables(null, null, null, new String[0]);
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertTrue(metaData.getColumnCount() >= 3);
    assertEquals("TABLE_CAT", metaData.getColumnName(1));
    assertEquals("TABLE_SCHEM", metaData.getColumnName(2));
    assertEquals("TABLE_NAME", metaData.getColumnName(3));
    resultSet.close();
    connection.close();
  }

  @Ignore
  @Test public void testNoFactory() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:");
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getSchemas();
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("TABLE_SCHEM", metaData.getColumnName(1));
    assertEquals("TABLE_CATALOG", metaData.getColumnName(2));
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Ignore
  @Test public void testCatalogsMock() throws Exception {
    final Connection connection = mjs();
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getSchemas();
    assertFalse(resultSet.next());
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("TABLE_SCHEM", metaData.getColumnName(1));
    assertEquals("TABLE_CATALOG", metaData.getColumnName(2));
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test public void testStatementExecuteQueryLocal() throws Exception {
    checkStatementExecuteQuery(ljs(), false);
  }

  @Ignore
  @Test public void testStatementExecuteQueryMock() throws Exception {
    checkStatementExecuteQuery(mjs(), false);
  }

  @Ignore
  @Test public void testPrepareExecuteQueryLocal() throws Exception {
    checkStatementExecuteQuery(ljs(), true);
  }

  @Ignore
  @Test public void testPrepareExecuteQueryMock() throws Exception {
    checkStatementExecuteQuery(mjs(), true);
  }

  private void checkStatementExecuteQuery(Connection connection,
      boolean prepare) throws SQLException {
    final String sql = "select * from (\n"
        + "  values (1, 'a'), (null, 'b'), (3, 'c')) as t (c1, c2)";
    final Statement statement;
    final ResultSet resultSet;
    final ParameterMetaData parameterMetaData;
    if (prepare) {
      final PreparedStatement ps = connection.prepareStatement(sql);
      statement = ps;
      parameterMetaData = ps.getParameterMetaData();
      resultSet = ps.executeQuery();
    } else {
      statement = connection.createStatement();
      parameterMetaData = null;
      resultSet = statement.executeQuery(sql);
    }
    if (parameterMetaData != null) {
      assertThat(parameterMetaData.getParameterCount(), equalTo(2));
    }
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("C1", metaData.getColumnName(1));
    assertEquals("C2", metaData.getColumnName(2));
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertFalse(resultSet.next());
    resultSet.close();
    statement.close();
    connection.close();
  }

  @Test public void testPrepareBindExecuteFetch() throws Exception {
    checkPrepareBindExecuteFetch(ljs());
  }

  private void checkPrepareBindExecuteFetch(Connection connection)
      throws SQLException {
    final String sql = "select cast(? as integer) * 3 as c, 'x' as x\n"
        + "from (values (1, 'a'))";
    final PreparedStatement ps =
        connection.prepareStatement(sql);
    final ResultSetMetaData metaData = ps.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("C", metaData.getColumnName(1));
    assertEquals("X", metaData.getColumnName(2));
    try {
      final ResultSet resultSet = ps.executeQuery();
      fail("expected error, got " + resultSet);
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          equalTo("exception while executing query: unbound parameter"));
    }

    final ParameterMetaData parameterMetaData = ps.getParameterMetaData();
    assertThat(parameterMetaData.getParameterCount(), equalTo(1));

    ps.setInt(1, 10);
    final ResultSet resultSet = ps.executeQuery();
    assertTrue(resultSet.next());
    assertThat(resultSet.getInt(1), equalTo(30));
    assertFalse(resultSet.next());
    resultSet.close();

    ps.setInt(1, 20);
    final ResultSet resultSet2 = ps.executeQuery();
    assertFalse(resultSet2.isClosed());
    assertTrue(resultSet2.next());
    assertThat(resultSet2.getInt(1), equalTo(60));
    assertThat(resultSet2.wasNull(), is(false));
    assertFalse(resultSet2.next());
    resultSet2.close();

    ps.setObject(1, null);
    final ResultSet resultSet3 = ps.executeQuery();
    assertTrue(resultSet3.next());
    assertThat(resultSet3.getInt(1), equalTo(0));
    assertThat(resultSet3.wasNull(), is(true));
    assertFalse(resultSet3.next());
    resultSet3.close();

    ps.close();
    connection.close();
  }

  /** Factory that creates a service based on a local JDBC connection. */
  public static class LocalJdbcServiceFactory implements Service.Factory {
    @Override public Service create(AvaticaConnection connection) {
      try {
        Connection connection1 =
            DriverManager.getConnection(CONNECTION_SPEC.url,
                CONNECTION_SPEC.username, CONNECTION_SPEC.password);
        return new LocalService(new JdbcMeta(connection1));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Factory that creates a service based on a local JDBC connection. */
  public static class QuasiRemoteJdbcServiceFactory implements Service.Factory {
    @Override public Service create(AvaticaConnection connection) {
      try {
        Connection connection1 =
            DriverManager.getConnection(CONNECTION_SPEC.url,
                CONNECTION_SPEC.username, CONNECTION_SPEC.password);
        final JdbcMeta jdbcMeta = new JdbcMeta(connection1);
        final LocalService localService = new LocalService(jdbcMeta);
        final LocalJsonService localJsonService =
            new LocalJsonService(localService);
        return localJsonService;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Information necessary to create a JDBC connection. Specify one to run
   * tests against a different database. (hsqldb is the default.) */
  public static class ConnectionSpec {
    public final String url;
    public final String username;
    public final String password;
    public final String driver;

    public ConnectionSpec(String url, String username, String password,
        String driver) {
      this.url = url;
      this.username = username;
      this.password = password;
      this.driver = driver;
    }

    public static final ConnectionSpec HSQLDB =
        new ConnectionSpec(ScottHsqldb.URI, ScottHsqldb.USER,
            ScottHsqldb.PASSWORD, "org.hsqldb.jdbcDriver");
  }
}

// End RemoteDriverTest.java
