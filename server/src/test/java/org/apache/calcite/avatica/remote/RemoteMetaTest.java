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
import org.apache.calcite.avatica.AvaticaSpecificDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.ConnectionSpec;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.Meta.DatabaseProperty;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.Service.ErrorResponse;
import org.apache.calcite.avatica.remote.Service.Response;
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.AvaticaProtobufHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;
import org.apache.calcite.avatica.server.Main.HandlerFactory;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.avatica.util.FilteredConstants;

import com.google.common.base.Throwables;
import com.google.common.cache.Cache;

import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.DataOutputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
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

  private final HttpServer server;
  private final String url;
  private final int port;
  private final Driver.Serialization serialization;

  @Parameters
  public static List<Object[]> parameters() throws Exception {
    List<Object[]> params = new ArrayList<>();

    final String[] mainArgs = { FullyRemoteJdbcMetaFactory.class.getName() };

    // Bind to '0' to pluck an ephemeral port instead of expecting a certain one to be free

    final HttpServer jsonServer = Main.start(mainArgs, 0, new HandlerFactory() {
      @Override public AvaticaJsonHandler createHandler(Service service) {
        return new AvaticaJsonHandler(service);
      }
    });
    params.add(new Object[] {jsonServer, Driver.Serialization.JSON});
    ACTIVE_SERVERS.add(jsonServer);

    final HttpServer protobufServer = Main.start(mainArgs, 0, new HandlerFactory() {
      @Override public AvaticaProtobufHandler createHandler(Service service) {
        return new AvaticaProtobufHandler(service);
      }
    });
    params.add(new Object[] {protobufServer, Driver.Serialization.PROTOBUF});

    ACTIVE_SERVERS.add(protobufServer);

    return params;
  }

  public RemoteMetaTest(HttpServer server, Driver.Serialization serialization) {
    this.server = server;
    this.port = this.server.getPort();
    this.serialization = serialization;
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
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1301">[CALCITE-1301]
   * Add cancel flag to AvaticaStatement</a>. */
  @Test public void testCancel() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try (AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url)) {
      final AvaticaStatement statement = conn.createStatement();
      final String sql = "select * from (values ('a', 1), ('b', 2))";
      final ResultSet rs = statement.executeQuery(sql);
      int count = 0;
    loop:
      for (;;) {
        switch (count++) {
        case 0:
          assertThat(rs.next(), is(true));
          break;
        case 1:
          rs.getStatement().cancel();
          try {
            boolean x = rs.next();
            fail("expected exception, got " + x);
          } catch (SQLException e) {
            assertThat(e.getMessage(), is("Statement canceled"));
          }
          break loop;
        default:
          fail("count: " + count);
        }
      }
      assertThat(count, is(2));
      assertThat(statement.isClosed(), is(false));
      rs.close();
      assertThat(statement.isClosed(), is(false));
      statement.close();
      assertThat(statement.isClosed(), is(true));
      statement.close();
      assertThat(statement.isClosed(), is(true));
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
    ConnectionSpec.getDatabaseLock().lock();
    try {
      final String t = AvaticaUtils.unique("TEST_TABLE2");
      AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url);
      Statement statement = conn.createStatement();
      final String create =
          String.format(Locale.ROOT, "create table if not exists %s ("
              + "  id int not null, msg varchar(255) not null)", t);
      int status = statement.executeUpdate(create);
      assertEquals(status, 0);

      statement = conn.createStatement();
      final String update =
          String.format(Locale.ROOT, "insert into %s values ('%d', '%s')",
              t, RANDOM.nextInt(Integer.MAX_VALUE), UUID.randomUUID());
      status = statement.executeUpdate(update);
      assertEquals(status, 1);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
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
      assertEquals("Remote driver error: RuntimeException: "
          + "java.sql.SQLInvalidAuthorizationSpecException: invalid authorization specification"
          + " - not found: john"
          + " -> SQLInvalidAuthorizationSpecException: invalid authorization specification - "
          + "not found: john"
          + " -> HsqlException: invalid authorization specification - not found: john",
          e.getMessage());
    }
  }

  @Test public void testRemoteConnectionsAreDifferent() throws SQLException {
    ConnectionSpec.getDatabaseLock().lock();
    try {
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
        assertEquals("Error -1 (00000) : Error while executing SQL \"select * from buffer\": "
            + "Remote driver error: RuntimeException: java.sql.SQLSyntaxErrorException: "
            + "user lacks privilege or object not found: BUFFER -> "
            + "SQLSyntaxErrorException: user lacks privilege or object not found: BUFFER -> "
            + "HsqlException: user lacks privilege or object not found: BUFFER",
            e.getMessage());
      }
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Ignore("[CALCITE-942] AvaticaConnection should fail-fast when closed.")
  @Test public void testRemoteConnectionClosing() throws Exception {
    AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url);
    // Verify connection is usable
    conn.createStatement();
    conn.close();

    // After closing the connection, it should not be usable anymore
    try {
      conn.createStatement();
      fail("expected exception");
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          containsString("Connection is closed"));
    }
  }

  @Test public void testExceptionPropagation() throws Exception {
    final String sql = "SELECT * from EMP LIMIT FOOBARBAZ";
    ConnectionSpec.getDatabaseLock().lock();
    try (final AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url);
        final Statement stmt = conn.createStatement()) {
      try {
        // invalid SQL
        stmt.execute(sql);
        fail("Expected an AvaticaSqlException");
      } catch (AvaticaSqlException e) {
        assertEquals(ErrorResponse.UNKNOWN_ERROR_CODE, e.getErrorCode());
        assertEquals(ErrorResponse.UNKNOWN_SQL_STATE, e.getSQLState());
        assertTrue("Message should contain original SQL, was '" + e.getMessage() + "'",
            e.getMessage().contains(sql));
        assertEquals(1, e.getStackTraces().size());
        final String stacktrace = e.getStackTraces().get(0);
        final String substring = "unexpected token: FOOBARBAZ";
        assertTrue("Message should contain '" + substring + "', was '" + e.getMessage() + ",",
            stacktrace.contains(substring));
      }
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testRemoteColumnsMeta() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      // Verify all columns are retrieved, thus that frame-based fetching works correctly
      // for columns
      int rowCount = 0;
      try (AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url)) {
        ResultSet rs = conn.getMetaData().getColumns(null, null, null, null);
        while (rs.next()) {
          rowCount++;
        }
        rs.close();

        // The implicitly created statement should have been closed
        assertTrue(rs.getStatement().isClosed());
      }
      // default fetch size is 100, we are well beyond it
      assertTrue(rowCount > 900);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testArrays() throws SQLException {
    ConnectionSpec.getDatabaseLock().lock();
    try (AvaticaConnection conn = (AvaticaConnection) DriverManager.getConnection(url);
         Statement stmt = conn.createStatement()) {
      ResultSet resultSet =
          stmt.executeQuery("select * from (values ('a', array['b', 'c']));");

      assertTrue(resultSet.next());
      assertEquals("a", resultSet.getString(1));
      Array arr = resultSet.getArray(2);
      assertTrue(arr instanceof ArrayImpl);
      Object[] values = (Object[]) ((ArrayImpl) arr).getArray();
      assertArrayEquals(new String[]{"b", "c"}, values);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testBinaryAndStrings() throws Exception {
    final String tableName = "testbinaryandstrs";
    final byte[] data = "asdf".getBytes(StandardCharsets.UTF_8);
    ConnectionSpec.getDatabaseLock().lock();
    try (final Connection conn = DriverManager.getConnection(url);
        final Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      assertFalse(stmt.execute("CREATE TABLE " + tableName + "(id int, bin BINARY(4))"));
      try (final PreparedStatement prepStmt = conn.prepareStatement(
          "INSERT INTO " + tableName + " values(1, ?)")) {
        prepStmt.setBytes(1, data);
        assertFalse(prepStmt.execute());
      }
      try (ResultSet results = stmt.executeQuery("SELECT id, bin from " + tableName)) {
        assertTrue(results.next());
        assertEquals(1, results.getInt(1));
        // byte comparison should work
        assertArrayEquals("Bytes were " + Arrays.toString(results.getBytes(2)),
            data, results.getBytes(2));
        // as should string
        assertEquals(new String(data, StandardCharsets.UTF_8), results.getString(2));
        assertFalse(results.next());
      }
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testLocalStackTraceHasServerStackTrace() {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      Statement statement = DriverManager.getConnection(url).createStatement();
      statement.executeQuery("SELECT * FROM BOGUS_TABLE_DEF_DOESNT_EXIST");
    } catch (SQLException e) {
      // Verify that we got the expected exception
      assertThat(e, instanceOf(AvaticaSqlException.class));

      // Attempt to verify that we got a "server-side" class in the stack.
      assertThat(Throwables.getStackTraceAsString(e),
          containsString(JdbcMeta.class.getName()));
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testServerAddressInResponse() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      URL url = new URL("http://localhost:" + this.port);
      AvaticaHttpClient httpClient = new AvaticaHttpClientImpl(url);
      byte[] request;

      Service.OpenConnectionRequest jsonReq = new Service.OpenConnectionRequest(
          UUID.randomUUID().toString(), Collections.<String, String>emptyMap());
      switch (this.serialization) {
      case JSON:
        request = JsonService.MAPPER.writeValueAsBytes(jsonReq);
        break;
      case PROTOBUF:
        ProtobufTranslation pbTranslation = new ProtobufTranslationImpl();
        request = pbTranslation.serializeRequest(jsonReq);
        break;
      default:
        throw new IllegalStateException("Should not reach here");
      }

      byte[] response = httpClient.send(request);
      Service.OpenConnectionResponse openCnxnResp;
      switch (this.serialization) {
      case JSON:
        openCnxnResp = JsonService.MAPPER.readValue(response,
            Service.OpenConnectionResponse.class);
        break;
      case PROTOBUF:
        ProtobufTranslation pbTranslation = new ProtobufTranslationImpl();
        Response genericResp = pbTranslation.parseResponse(response);
        assertTrue("Expected an OpenConnnectionResponse, but got " + genericResp.getClass(),
            genericResp instanceof Service.OpenConnectionResponse);
        openCnxnResp = (Service.OpenConnectionResponse) genericResp;
        break;
      default:
        throw new IllegalStateException("Should not reach here");
      }

      String hostname = InetAddress.getLocalHost().getHostName();

      assertNotNull(openCnxnResp.rpcMetadata);
      assertEquals(hostname + ":" + this.port, openCnxnResp.rpcMetadata.serverAddress);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testCommitRollback() throws Exception {
    final String productTable = "commitrollback_products";
    final String salesTable = "commitrollback_sales";
    ConnectionSpec.getDatabaseLock().lock();
    try (final Connection conn = DriverManager.getConnection(url);
        final Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + productTable));
      assertFalse(
          stmt.execute(
              String.format(Locale.ROOT,
                  "CREATE TABLE %s(id integer, stock integer)",
                  productTable)));
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + salesTable));
      assertFalse(
          stmt.execute(
              String.format(Locale.ROOT,
                  "CREATE TABLE %s(id integer, units_sold integer)",
                  salesTable)));

      final int productId = 1;
      // No products and no sales
      assertFalse(
          stmt.execute(
              String.format(Locale.ROOT, "INSERT INTO %s VALUES(%d, 0)",
                  productTable, productId)));
      assertFalse(
          stmt.execute(
              String.format(Locale.ROOT, "INSERT INTO %s VALUES(%d, 0)",
                  salesTable, productId)));

      conn.setAutoCommit(false);
      PreparedStatement productStmt = conn.prepareStatement(
          String.format(Locale.ROOT,
              "UPDATE %s SET stock = stock + ? WHERE id = ?", productTable));
      PreparedStatement salesStmt = conn.prepareStatement(
          String.format(Locale.ROOT,
              "UPDATE %s SET units_sold = units_sold + ? WHERE id = ?",
              salesTable));

      // No stock
      assertEquals(0, getInventory(conn, productTable, productId));

      // Set a stock of 10 for product 1
      productStmt.setInt(1, 10);
      productStmt.setInt(2, productId);
      productStmt.executeUpdate();

      conn.commit();
      assertEquals(10, getInventory(conn, productTable, productId));

      // Sold 5 items (5 in stock, 5 sold)
      productStmt.setInt(1, -5);
      productStmt.setInt(2, productId);
      productStmt.executeUpdate();
      salesStmt.setInt(1, 5);
      salesStmt.setInt(2, productId);
      salesStmt.executeUpdate();

      conn.commit();
      // We will definitely see the updated values
      assertEquals(5, getInventory(conn, productTable, productId));
      assertEquals(5, getSales(conn, salesTable, productId));

      // Update some "bad" values
      productStmt.setInt(1, -10);
      productStmt.setInt(2, productId);
      productStmt.executeUpdate();
      salesStmt.setInt(1, 10);
      salesStmt.setInt(2, productId);
      salesStmt.executeUpdate();

      // We just went negative, nonsense. Better rollback.
      conn.rollback();

      // Should still have 5 and 5
      assertEquals(5, getInventory(conn, productTable, productId));
      assertEquals(5, getSales(conn, salesTable, productId));
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private int getInventory(Connection conn, String productTable, int productId) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      ResultSet results = stmt.executeQuery(
          String.format(Locale.ROOT, "SELECT stock FROM %s WHERE id = %d",
              productTable, productId));
      assertTrue(results.next());
      return results.getInt(1);
    }
  }

  private int getSales(Connection conn, String salesTable, int productId) throws Exception {
    try (Statement stmt = conn.createStatement()) {
      ResultSet results = stmt.executeQuery(
          String.format(Locale.ROOT, "SELECT units_sold FROM %s WHERE id = %d",
              salesTable, productId));
      assertTrue(results.next());
      return results.getInt(1);
    }
  }

  @Test public void getAvaticaVersion() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try (final Connection conn = DriverManager.getConnection(url)) {
      DatabaseMetaData metadata = conn.getMetaData();
      assertTrue("DatabaseMetaData is not an instance of AvaticaDatabaseMetaData",
          metadata instanceof AvaticaSpecificDatabaseMetaData);
      AvaticaSpecificDatabaseMetaData avaticaMetadata = (AvaticaSpecificDatabaseMetaData) metadata;
      // We should get the same version back from the server
      assertEquals(FilteredConstants.VERSION, avaticaMetadata.getAvaticaServerVersion());

      Properties avaticaProps = avaticaMetadata.unwrap(Properties.class);
      assertNotNull(avaticaProps);
      assertEquals(FilteredConstants.VERSION,
          avaticaProps.get(DatabaseProperty.AVATICA_VERSION.name()));
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testMalformedRequest() throws Exception {
    URL url = new URL("http://localhost:" + this.port);

    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestMethod("POST");
    conn.setDoInput(true);
    conn.setDoOutput(true);

    try (DataOutputStream wr = new DataOutputStream(conn.getOutputStream())) {
      // Write some garbage data
      wr.write(new byte[] {0, 1, 2, 3, 4, 5, 6, 7});
      wr.flush();
      wr.close();
    }
    final int responseCode = conn.getResponseCode();
    assertEquals(500, responseCode);
    final InputStream inputStream = conn.getErrorStream();
    byte[] responseBytes = AvaticaUtils.readFullyToBytes(inputStream);
    ErrorResponse response;
    switch (this.serialization) {
    case JSON:
      response = JsonService.MAPPER.readValue(responseBytes, ErrorResponse.class);
      assertTrue("Unexpected error message: " + response.errorMessage,
          response.errorMessage.contains("Illegal character"));
      break;
    case PROTOBUF:
      ProtobufTranslation pbTranslation = new ProtobufTranslationImpl();
      Response genericResp = pbTranslation.parseResponse(responseBytes);
      assertTrue("Response was not an ErrorResponse, but was " + genericResp.getClass(),
          genericResp instanceof ErrorResponse);
      response = (ErrorResponse) genericResp;
      assertTrue("Unexpected error message: " + response.errorMessage,
          response.errorMessage.contains("contained an invalid tag"));
      break;
    default:
      fail("Unhandled serialization " + this.serialization);
      throw new RuntimeException();
    }
  }

  @Test public void testDriverProperties() throws Exception {
    final Properties props = new Properties();
    props.setProperty("foo", "bar");
    final Properties originalProps = (Properties) props.clone();
    try (final Connection conn = DriverManager.getConnection(url, props)) {
      // The contents of the two properties objects should not have changed after connecting.
      assertEquals(props, originalProps);
    }
  }

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
}

// End RemoteMetaTest.java
