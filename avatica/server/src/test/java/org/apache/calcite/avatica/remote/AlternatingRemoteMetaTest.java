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
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.ConnectionSpec;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;
import org.apache.calcite.avatica.server.Main.HandlerFactory;

import com.google.common.cache.Cache;

import org.eclipse.jetty.server.handler.AbstractHandler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests that verify that the Driver still functions when requests are randomly bounced between
 * more than one server.
 */
public class AlternatingRemoteMetaTest {
  private static final ConnectionSpec CONNECTION_SPEC = ConnectionSpec.HSQLDB;

  private static String url;

  static {
    try {
      // Force DriverManager initialization before we hit AlternatingDriver->Driver.<clinit>
      // Otherwise Driver.<clinit> -> DriverManager.registerDriver -> scan service provider files
      // causes a deadlock; see [CALCITE-1060]
      DriverManager.getDrivers();
      DriverManager.registerDriver(new AlternatingDriver());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

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

  /**
   * AvaticaHttpClient implementation that randomly chooses among the provided URLs.
   */
  public static class AlternatingAvaticaHttpClient implements AvaticaHttpClient {
    private final List<AvaticaHttpClientImpl> clients;
    private final Random r = new Random();

    public AlternatingAvaticaHttpClient(List<URL> urls) {
      //System.out.println("Constructing clients for " + urls);
      clients = new ArrayList<>(urls.size());
      for (URL url : urls) {
        clients.add(new AvaticaHttpClientImpl(url));
      }
    }

    public byte[] send(byte[] request) {
      AvaticaHttpClientImpl client = clients.get(r.nextInt(clients.size()));
      //System.out.println("URL: " + client.url);
      return client.send(request);
    }

  }

  /**
   * Driver implementation {@link AlternatingAvaticaHttpClient}.
   */
  public static class AlternatingDriver extends Driver {

    public static final String PREFIX = "jdbc:avatica:remote-alternating:";

    @Override protected String getConnectStringPrefix() {
      return PREFIX;
    }

    @Override public Meta createMeta(AvaticaConnection connection) {
      final ConnectionConfig config = connection.config();
      final Service service = new RemoteService(getHttpClient(connection, config));
      connection.setService(service);
      return new RemoteMeta(connection, service);
    }

    @Override AvaticaHttpClient getHttpClient(AvaticaConnection connection,
        ConnectionConfig config) {
      return new AlternatingAvaticaHttpClient(parseUrls(config.url()));
    }

    List<URL> parseUrls(String urlStr) {
      final List<URL> urls = new ArrayList<>();
      final char comma = ',';

      int prevIndex = 0;
      int index = urlStr.indexOf(comma);
      if (-1 == index) {
        try {
          return Collections.singletonList(new URL(urlStr));
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }
      }

      // String split w/o regex
      while (-1 != index) {
        try {
          urls.add(new URL(urlStr.substring(prevIndex, index)));
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }
        prevIndex = index + 1;
        index = urlStr.indexOf(comma, prevIndex);
      }

      // Get the last one
      try {
        urls.add(new URL(urlStr.substring(prevIndex)));
      } catch (MalformedURLException e) {
        throw new RuntimeException(e);
      }

      return urls;
    }

  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    final String[] mainArgs = new String[] { FullyRemoteJdbcMetaFactory.class.getName() };

    // Bind to '0' to pluck an ephemeral port instead of expecting a certain one to be free

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 2; i++) {
      if (sb.length() > 0) {
        sb.append(",");
      }
      HttpServer jsonServer = Main.start(mainArgs, 0, new HandlerFactory() {
        @Override public AbstractHandler createHandler(Service service) {
          return new AvaticaJsonHandler(service);
        }
      });
      ACTIVE_SERVERS.add(jsonServer);
      sb.append("http://localhost:").append(jsonServer.getPort());
    }

    url = AlternatingDriver.PREFIX + "url=" + sb.toString();
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

  @Test public void testQuery() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try (AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url);
        Statement statement = conn.createStatement()) {
      assertFalse(statement.execute("SET SCHEMA \"SCOTT\""));
      assertFalse(
          statement.execute(
              "CREATE TABLE \"FOO\"(\"KEY\" INTEGER NOT NULL, \"VALUE\" VARCHAR(10))"));
      assertFalse(statement.execute("SET TABLE \"FOO\" READONLY FALSE"));

      final int numRecords = 1000;
      for (int i = 0; i < numRecords; i++) {
        assertFalse(statement.execute("INSERT INTO \"FOO\" VALUES(" + i + ", '" + i + "')"));
      }

      // Make sure all the records are there that we expect
      ResultSet results = statement.executeQuery("SELECT count(KEY) FROM FOO");
      assertTrue(results.next());
      assertEquals(1000, results.getInt(1));
      assertFalse(results.next());

      results = statement.executeQuery("SELECT KEY, VALUE FROM FOO ORDER BY KEY ASC");
      for (int i = 0; i < numRecords; i++) {
        assertTrue(results.next());
        assertEquals(i, results.getInt(1));
        assertEquals(Integer.toString(i), results.getString(2));
      }
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testSingleUrlParsing() throws Exception {
    AlternatingDriver d = new AlternatingDriver();
    List<URL> urls = d.parseUrls("http://localhost:1234");
    assertEquals(Arrays.asList(new URL("http://localhost:1234")), urls);
  }

  @Test public void testMultipleUrlParsing() throws Exception {
    AlternatingDriver d = new AlternatingDriver();
    List<URL> urls = d.parseUrls("http://localhost:1234,http://localhost:2345,"
        + "http://localhost:3456");
    List<URL> expectedUrls = Arrays.asList(new URL("http://localhost:1234"),
        new URL("http://localhost:2345"), new URL("http://localhost:3456"));
    assertEquals(expectedUrls, urls);
  }
}

// End AlternatingRemoteMetaTest.java
