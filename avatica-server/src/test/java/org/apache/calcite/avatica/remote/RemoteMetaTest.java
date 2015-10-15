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
package org.apache.calcite.avatica.remote;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.ConnectionSpec;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.server.AvaticaHandler;
import org.apache.calcite.avatica.server.AvaticaProtobufHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;
import org.apache.calcite.avatica.server.Main.HandlerFactory;

import com.google.common.cache.Cache;

import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests covering {@link RemoteMeta}. */
@RunWith(Parameterized.class)
public class RemoteMetaTest {
  private static final Random RANDOM = new Random();
  private static final ConnectionSpec CONNECTION_SPEC = ConnectionSpec.HSQLDB;

  // Keep a reference to the servers we start to clean them up after
  private static final List<HttpServer> ACTIVE_SERVERS = new ArrayList<>();

  /** Factory that provides a {@link JdbcMeta}. */
  public static class FullyRemoteJdbcMetaFactory implements Meta.Factory {

    private static JdbcMeta instance = null;

    private static JdbcMeta getInstance() {
      if (instance == null) {
        try {
          instance = new JdbcMeta(CONNECTION_SPEC.url, CONNECTION_SPEC.username,
              CONNECTION_SPEC.password);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }
      return instance;
    }

    @Override public Meta create(List<String> args) {
      return getInstance();
    }
  }

  @Parameters
  public static List<Object[]> parameters() throws Exception {
    List<Object[]> params = new ArrayList<>();

    final String[] mainArgs = new String[] { FullyRemoteJdbcMetaFactory.class.getName() };

    // Bind to '0' to pluck an ephemeral port instead of expecting a certain one to be free

    final HttpServer jsonServer = Main.start(mainArgs, 0, new HandlerFactory() {
      @Override public AbstractHandler createHandler(Service service) {
        return new AvaticaHandler(service);
      }
    });
    params.add(new Object[] {jsonServer, Driver.Serialization.JSON});
    ACTIVE_SERVERS.add(jsonServer);

    final HttpServer protobufServer = Main.start(mainArgs, 0, new HandlerFactory() {
      @Override public AbstractHandler createHandler(Service service) {
        return new AvaticaProtobufHandler(service);
      }
    });
    params.add(new Object[] {protobufServer, Driver.Serialization.PROTOBUF});

    ACTIVE_SERVERS.add(protobufServer);

    return params;
  }

  private final HttpServer server;
  private final String url;

  public RemoteMetaTest(HttpServer server, Driver.Serialization serialization) {
    this.server = server;
    final int port = server.getPort();
    url = "jdbc:avatica:remote:url=http://localhost:" + port + ";serialization="
        + serialization.name();
  }

  @AfterClass public static void afterClass() throws Exception {
    for (HttpServer server : ACTIVE_SERVERS) {
      if (server != null) {
        server.stop();
      }
    }
  }

  private static Meta getMeta(AvaticaConnection conn) throws Exception {
    Field f = AvaticaConnection.class.getDeclaredField("meta");
    f.setAccessible(true);
    return (Meta) f.get(conn);
  }

  private static Meta.ExecuteResult prepareAndExecuteInternal(AvaticaConnection conn,
    final AvaticaStatement statement, String sql, int maxRowCount) throws Exception {
    Method m =
        AvaticaConnection.class.getDeclaredMethod("prepareAndExecuteInternal",
            AvaticaStatement.class, String.class, long.class);
    m.setAccessible(true);
    return (Meta.ExecuteResult) m.invoke(conn, statement, sql, maxRowCount);
  }

  private static Connection getConnection(JdbcMeta m, String id) throws Exception {
    Field f = JdbcMeta.class.getDeclaredField("connectionCache");
    f.setAccessible(true);
    //noinspection unchecked
    Cache<String, Connection> connectionCache = (Cache<String, Connection>) f.get(m);
    return connectionCache.getIfPresent(id);
  }

  @Test public void testRemoteExecuteMaxRowCount() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try (AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url)) {
      final AvaticaStatement statement = conn.createStatement();
      prepareAndExecuteInternal(conn, statement,
        "select * from (values ('a', 1), ('b', 2))", 0);
      ResultSet rs = statement.getResultSet();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertEquals("Check maxRowCount=0 and ResultSets is 0 row", count, 0);
      assertEquals("Check result set meta is still there",
        rs.getMetaData().getColumnCount(), 2);
      rs.close();
      statement.close();
      conn.close();
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-780">[CALCITE-780]
   * HTTP error 413 when sending a long string to the Avatica server</a>. */
  @Test public void testRemoteExecuteVeryLargeQuery() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      // Before the bug was fixed, a value over 7998 caused an HTTP 413.
      // 16K bytes, I guess.
      checkLargeQuery(8);
      checkLargeQuery(240);
      checkLargeQuery(8000);
      checkLargeQuery(240000);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void checkLargeQuery(int n) throws Exception {
    try (AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url)) {
      final AvaticaStatement statement = conn.createStatement();
      final String frenchDisko = "It said human existence is pointless\n"
          + "As acts of rebellious solidarity\n"
          + "Can bring sense in this world\n"
          + "La resistance!\n";
      final String sql = "select '"
          + longString(frenchDisko, n)
          + "' as s from (values 'x')";
      prepareAndExecuteInternal(conn, statement, sql, -1);
      ResultSet rs = statement.getResultSet();
      int count = 0;
      while (rs.next()) {
        count++;
      }
      assertThat(count, is(1));
      rs.close();
      statement.close();
      conn.close();
    }
  }

  /** Creates a string of exactly {@code length} characters by concatenating
   * {@code fragment}. */
  private static String longString(String fragment, int length) {
    assert fragment.length() > 0;
    final StringBuilder buf = new StringBuilder();
    while (buf.length() < length) {
      buf.append(fragment);
    }
    buf.setLength(length);
    return buf.toString();
  }

  @Test public void testRemoteConnectionProperties() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try (AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url)) {
      String id = conn.id;
      final Map<String, ConnectionPropertiesImpl> m = ((RemoteMeta) getMeta(conn)).propsMap;
      assertFalse("remote connection map should start ignorant", m.containsKey(id));
      // force creating a connection object on the remote side.
      try (final Statement stmt = conn.createStatement()) {
        assertTrue("creating a statement starts a local object.", m.containsKey(id));
        assertTrue(stmt.execute("select count(1) from EMP"));
      }
      Connection remoteConn = getConnection(FullyRemoteJdbcMetaFactory.getInstance(), id);
      final boolean defaultRO = remoteConn.isReadOnly();
      final boolean defaultAutoCommit = remoteConn.getAutoCommit();
      final String defaultCatalog = remoteConn.getCatalog();
      final String defaultSchema = remoteConn.getSchema();
      conn.setReadOnly(!defaultRO);
      assertTrue("local changes dirty local state", m.get(id).isDirty());
      assertEquals("remote connection has not been touched", defaultRO, remoteConn.isReadOnly());
      conn.setAutoCommit(!defaultAutoCommit);
      assertEquals("remote connection has not been touched",
          defaultAutoCommit, remoteConn.getAutoCommit());

      // further interaction with the connection will force a sync
      try (final Statement stmt = conn.createStatement()) {
        assertEquals(!defaultAutoCommit, remoteConn.getAutoCommit());
        assertFalse("local values should be clean", m.get(id).isDirty());
      }
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testRemoteStatementInsert() throws Exception {
    System.out.println(url);
    AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url);
    Statement statement = conn.createStatement();
    int status = statement.executeUpdate(
        "create table if not exists "
        + "TEST_TABLE2 (id int not null, msg varchar(255) not null)");
    assertEquals(status, 0);

    statement = conn.createStatement();
    status = statement.executeUpdate("insert into TEST_TABLE2 values ("
        + "'" + RANDOM.nextInt(Integer.MAX_VALUE) + "', '" + UUID.randomUUID() + "')");
    assertEquals(status, 1);
  }

  @Test public void testBigints() throws Exception {
    final String table = "TESTBIGINTS";
    ConnectionSpec.getDatabaseLock().lock();
    try (AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url);
        Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + table));
      assertFalse(stmt.execute("CREATE TABLE " + table + " (id BIGINT)"));
      assertFalse(stmt.execute("INSERT INTO " + table + " values(10)"));
      ResultSet results = conn.getMetaData().getColumns(null, null, table, null);
      assertTrue(results.next());
      assertEquals(table, results.getString(3));
      // ordinal position
      assertEquals(1L, results.getLong(17));
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testOpenConnectionWithProperties() throws Exception {
    // This tests that username and password are used for creating a connection on the
    // server. If this was not the case, it would succeed.
    try {
      DriverManager.getConnection(url, "john", "doe");
      fail("expected exception");
    } catch (RuntimeException e) {
      assertEquals("Remote driver error:"
          + " java.sql.SQLInvalidAuthorizationSpecException: invalid authorization specification"
          + " - not found: john"
          + " -> invalid authorization specification - not found: john"
          + " -> invalid authorization specification - not found: john",
          e.getMessage());
    }
  }

  @Test public void testRemoteConnectionsAreDifferent() throws SQLException {
    Connection conn1 = DriverManager.getConnection(url);
    Statement stmt = conn1.createStatement();
    stmt.execute("DECLARE LOCAL TEMPORARY TABLE"
        + " buffer (id INTEGER PRIMARY KEY, textdata VARCHAR(100))");
    stmt.execute("insert into buffer(id, textdata) values(1, 'abc')");
    stmt.executeQuery("select * from buffer");

    // The local temporary table is local to the connection above, and should
    // not be visible on another connection
    Connection conn2 = DriverManager.getConnection(url);
    Statement stmt2 = conn2.createStatement();
    try {
      stmt2.executeQuery("select * from buffer");
      fail("expected exception");
    } catch (Exception e) {
      assertEquals("Remote driver error: java.sql.SQLSyntaxErrorException: user lacks privilege"
          + " or object not found: BUFFER -> user lacks privilege or object not found: BUFFER"
          + " -> user lacks privilege or object not found: BUFFER", e.getCause().getMessage());
    }
  }

  @Test public void testRemoteConnectionClosing() throws Exception {
    AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url);
    // Verify connection is usable
    conn.createStatement();
    conn.close();

    // After closing the connection, it should not be usable anymore
    try {
      conn.createStatement();
      fail("expected exception");
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), containsString("Remote driver error:"
          + " Connection not found: invalid id, closed, or expired"));
    }
  }
}

// End RemoteMetaTest.java
