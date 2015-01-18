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
package org.apache.calcite.jdbc;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;
import org.apache.calcite.test.CalciteAssert;

import com.google.common.base.Function;

import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test for Calcite's remote JDBC driver.
 */
public class CalciteRemoteDriverTest {
  public static final String LJS = Factory2.class.getName();

  private static final CalciteAssert.ConnectionFactory
  REMOTE_CONNECTION_FACTORY =
      new CalciteAssert.ConnectionFactory() {
        public Connection createConnection() throws SQLException {
          return remoteConnection;
        }
      };

  private static final Function<Connection, ResultSet> GET_SCHEMAS =
      new Function<Connection, ResultSet>() {
        public ResultSet apply(Connection input) {
          try {
            return input.getMetaData().getSchemas();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      };
  private static final Function<Connection, ResultSet> GET_CATALOGS =
      new Function<Connection, ResultSet>() {
        public ResultSet apply(Connection input) {
          try {
            return input.getMetaData().getCatalogs();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      };

  private static Connection localConnection;
  private static Connection remoteConnection;
  private static HttpServer start;

  @BeforeClass public static void beforeClass() throws Exception {
    localConnection = CalciteAssert.hr().connect();

    start = Main.start(new String[]{Factory.class.getName()});
    final int port = start.getPort();
    remoteConnection = DriverManager.getConnection(
        "jdbc:avatica:remote:url=http://localhost:" + port);
  }

  @AfterClass public static void afterClass() throws Exception {
    if (localConnection != null) {
      localConnection.close();
      localConnection = null;
    }

    if (start != null) {
      start.stop();
    }
  }

  @Test public void testCatalogsLocal() throws Exception {
    final Connection connection = DriverManager.getConnection(
        "jdbc:avatica:remote:factory=" + LJS);
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getCatalogs();
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(1, metaData.getColumnCount());
    assertEquals("TABLE_CATALOG", metaData.getColumnName(1));
    assertTrue(resultSet.next());
    assertFalse(resultSet.next());
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test public void testSchemasLocal() throws Exception {
    final Connection connection = DriverManager.getConnection(
        "jdbc:avatica:remote:factory=" + LJS);
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getSchemas();
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("TABLE_SCHEM", metaData.getColumnName(1));
    assertEquals("TABLE_CATALOG", metaData.getColumnName(2));
    assertTrue(resultSet.next());
    assertThat(resultSet.getString(1), equalTo("POST"));
    assertThat(resultSet.getString(2), CoreMatchers.nullValue());
    assertTrue(resultSet.next());
    assertThat(resultSet.getString(1), equalTo("foodmart"));
    assertThat(resultSet.getString(2), CoreMatchers.nullValue());
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertFalse(resultSet.next());
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test public void testRemoteCatalogs() throws Exception {
    CalciteAssert.hr().with(REMOTE_CONNECTION_FACTORY)
        .metaData(GET_CATALOGS)
        .returns("TABLE_CATALOG=null\n");
  }

  @Test public void testRemoteSchemas() throws Exception {
    CalciteAssert.hr().with(REMOTE_CONNECTION_FACTORY)
        .metaData(GET_SCHEMAS)
        .returns("TABLE_SCHEM=POST; TABLE_CATALOG=null\n"
            + "TABLE_SCHEM=foodmart; TABLE_CATALOG=null\n"
            + "TABLE_SCHEM=hr; TABLE_CATALOG=null\n"
            + "TABLE_SCHEM=metadata; TABLE_CATALOG=null\n");
  }

  @Test public void testRemoteExecuteQuery() throws Exception {
    CalciteAssert.hr().with(REMOTE_CONNECTION_FACTORY)
        .query("values (1, 'a'), (cast(null as integer), 'b')")
        .returnsUnordered("EXPR$0=1; EXPR$1=a", "EXPR$0=null; EXPR$1=b");
  }

  /** Same query as {@link #testRemoteExecuteQuery()}, run without the test
   * infrastructure. */
  @Test public void testRemoteExecuteQuery2() throws Exception {
    final Statement statement = remoteConnection.createStatement();
    final ResultSet resultSet =
        statement.executeQuery("values (1, 'a'), (cast(null as integer), 'b')");
    int n = 0;
    while (resultSet.next()) {
      ++n;
    }
    assertThat(n, equalTo(2));
  }

  /** Creates a {@link Meta} that can see the test databases. */
  public static class Factory implements Meta.Factory {
    public Meta create(List<String> args) {
      try {
        final Connection connection = CalciteAssert.hr().connect();
        return new CalciteMetaImpl((CalciteConnectionImpl) connection);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Factory that creates a {@code LocalJsonService}. */
  public static class Factory2 implements Service.Factory {
    public Service create(AvaticaConnection connection) {
      try {
        Connection localConnection = CalciteAssert.hr().connect();
        final Meta meta = CalciteConnectionImpl.TROJAN
            .getMeta((CalciteConnectionImpl) localConnection);
        return new LocalJsonService(new LocalService(meta));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}

// End CalciteRemoteDriverTest.java
