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

import org.apache.calcite.avatica.Meta.DatabaseProperty;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.JsonService;
import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.LocalProtobufService;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.ProtobufTranslation;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.avatica.util.DateTimeUtils;

import com.google.common.cache.Cache;

import net.jcip.annotations.NotThreadSafe;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit test for Avatica Remote JDBC driver.
 */
@RunWith(Parameterized.class)
@NotThreadSafe // for testConnectionIsolation
public class RemoteDriverTest {
  private static final Logger LOG = LoggerFactory.getLogger(RemoteDriverTest.class);

  public static final String LJS =
      LocalJdbcServiceFactory.class.getName();

  public static final String QRJS =
      QuasiRemoteJdbcServiceFactory.class.getName();

  public static final String QRPBS =
      QuasiRemotePBJdbcServiceFactory.class.getName();

  private static final ConnectionSpec CONNECTION_SPEC = ConnectionSpec.HSQLDB;

  private static Connection ljs() throws SQLException {
    return DriverManager.getConnection("jdbc:avatica:remote:factory=" + QRJS);
  }

  private static Connection lpbs() throws SQLException {
    return DriverManager.getConnection("jdbc:avatica:remote:factory=" + QRPBS);
  }

  private Connection canon() throws SQLException {
    return DriverManager.getConnection(CONNECTION_SPEC.url,
        CONNECTION_SPEC.username, CONNECTION_SPEC.password);
  }

  /**
   * Interface that allows for alternate ways to access internals to the Connection for testing
   * purposes.
   */
  interface ConnectionInternals {
    /**
     * Reaches into the guts of a quasi-remote connection and pull out the
     * statement map from the other side.
     *
     * <p>TODO: refactor tests to replace reflection with package-local access
     */
    Cache<Integer, Object> getRemoteStatementMap(AvaticaConnection connection) throws Exception;

    /**
     * Reaches into the guts of a quasi-remote connection and pull out the
     * connection map from the other side.
     *
     * <p>TODO: refactor tests to replace reflection with package-local access
     */
    Cache<String, Connection> getRemoteConnectionMap(AvaticaConnection connection) throws Exception;
  }

  // Run each test with the LocalJsonService and LocalProtobufService
  @Parameters
  public static List<Object[]> parameters() {
    List<Object[]> connections = new ArrayList<>();

    // Json and Protobuf operations should be equivalent -- tests against one work on the other
    // Each test needs to get a fresh Connection and also access some internals on that Connection.

    connections.add(
      new Object[] {
        new Callable<Connection>() {
          public Connection call() {
            try {
              return ljs();
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
        },
        new QuasiRemoteJdbcServiceInternals(),
        new Callable<RequestInspection>() {
          public RequestInspection call() throws Exception {
            assert null != QuasiRemoteJdbcServiceFactory.requestInspection;
            return QuasiRemoteJdbcServiceFactory.requestInspection;
          }
        } });

    // TODO write the ConnectionInternals implementation
    connections.add(
      new Object[] {
        new Callable<Connection>() {
          public Connection call() {
            try {
              return lpbs();
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }
        },
        new QuasiRemoteProtobufJdbcServiceInternals(),
        new Callable<RequestInspection>() {
          public RequestInspection call() throws Exception {
            assert null != QuasiRemotePBJdbcServiceFactory.requestInspection;
            return QuasiRemotePBJdbcServiceFactory.requestInspection;
          }
        } });

    return connections;
  }

  private final Callable<Connection> localConnectionCallable;
  private final ConnectionInternals localConnectionInternals;
  private final Callable<RequestInspection> requestInspectionCallable;

  public RemoteDriverTest(Callable<Connection> localConnectionCallable,
      ConnectionInternals internals, Callable<RequestInspection> requestInspectionCallable) {
    this.localConnectionCallable = localConnectionCallable;
    this.localConnectionInternals = internals;
    this.requestInspectionCallable = requestInspectionCallable;
  }

  private Connection getLocalConnection() {
    try {
      return localConnectionCallable.call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private ConnectionInternals getLocalConnectionInternals() {
    return localConnectionInternals;
  }

  private RequestInspection getRequestInspection() {
    try {
      return requestInspectionCallable.call();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Executes a lambda for the canonical connection and the local
   * connection. */
  public void eachConnection(ConnectionFunction f, Connection localConn) throws Exception {
    for (int i = 0; i < 2; i++) {
      try (Connection connection = i == 0 ? canon() : localConn) {
        f.apply(connection);
      }
    }
  }

  @Before
  public void before() throws Exception {
    QuasiRemoteJdbcServiceFactory.initService();
    QuasiRemotePBJdbcServiceFactory.initService();
  }

  @Test public void testRegister() throws Exception {
    final Connection connection = getLocalConnection();
    assertThat(connection.isClosed(), is(false));
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test public void testDatabaseProperties() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      final Connection connection = getLocalConnection();
      for (Meta.DatabaseProperty p : Meta.DatabaseProperty.values()) {
        switch (p) {
        case GET_NUMERIC_FUNCTIONS:
          assertThat(connection.getMetaData().getNumericFunctions(),
              equalTo("ABS,ACOS,ASIN,ATAN,ATAN2,BITAND,BITOR,BITXOR,"
                  + "CEILING,COS,COT,DEGREES,EXP,FLOOR,LOG,LOG10,MOD,"
                  + "PI,POWER,RADIANS,RAND,ROUND,ROUNDMAGIC,SIGN,SIN,"
                  + "SQRT,TAN,TRUNCATE"));
          break;
        case GET_SYSTEM_FUNCTIONS:
          assertThat(connection.getMetaData().getSystemFunctions(),
              equalTo("DATABASE,IFNULL,USER"));
          break;
        case GET_TIME_DATE_FUNCTIONS:
          assertThat(connection.getMetaData().getTimeDateFunctions(),
              equalTo("CURDATE,CURTIME,DATEDIFF,DAYNAME,DAYOFMONTH,DAYOFWEEK,"
                  + "DAYOFYEAR,HOUR,MINUTE,MONTH,MONTHNAME,NOW,QUARTER,SECOND,"
                  + "SECONDS_SINCE_MIDNIGHT,TIMESTAMPADD,TIMESTAMPDIFF,"
                  + "TO_CHAR,WEEK,YEAR"));
          break;
        case GET_S_Q_L_KEYWORDS:
          assertThat(connection.getMetaData().getSQLKeywords(),
              equalTo("")); // No SQL keywords return for HSQLDB
          break;
        case GET_STRING_FUNCTIONS:
          assertThat(connection.getMetaData().getStringFunctions(),
              equalTo("ASCII,CHAR,CONCAT,DIFFERENCE,HEXTORAW,INSERT,LCASE,"
                  + "LEFT,LENGTH,LOCATE,LTRIM,RAWTOHEX,REPEAT,REPLACE,"
                  + "RIGHT,RTRIM,SOUNDEX,SPACE,SUBSTR,UCASE"));
          break;
        default:
        }
      }
      connection.close();
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testTypeInfo() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      final Connection connection = getLocalConnection();
      final ResultSet resultSet =
          connection.getMetaData().getTypeInfo();
      assertTrue(resultSet.next());
      final ResultSetMetaData metaData = resultSet.getMetaData();
      assertTrue(metaData.getColumnCount() >= 18);
      assertEquals("TYPE_NAME", metaData.getColumnName(1));
      assertEquals("DATA_TYPE", metaData.getColumnName(2));
      assertEquals("PRECISION", metaData.getColumnName(3));
      assertEquals("SQL_DATA_TYPE", metaData.getColumnName(16));
      assertEquals("SQL_DATETIME_SUB", metaData.getColumnName(17));
      assertEquals("NUM_PREC_RADIX", metaData.getColumnName(18));
      resultSet.close();
      connection.close();
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testGetTables() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      final Connection connection = getLocalConnection();
      final ResultSet resultSet =
              connection.getMetaData().getTables(null, "SCOTT", null, null);
      assertEquals(13, resultSet.getMetaData().getColumnCount());
      assertTrue(resultSet.next());
      assertEquals("DEPT", resultSet.getString(3));
      assertTrue(resultSet.next());
      assertEquals("EMP", resultSet.getString(3));
      assertTrue(resultSet.next());
      assertEquals("BONUS", resultSet.getString(3));
      assertTrue(resultSet.next());
      assertEquals("SALGRADE", resultSet.getString(3));
      resultSet.close();
      connection.close();
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
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

  @Test public void testStatementExecuteQueryLocal() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      checkStatementExecuteQuery(getLocalConnection(), false);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testPrepareExecuteQueryLocal() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      checkStatementExecuteQuery(getLocalConnection(), true);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testInsertDrop() throws Exception {
    final String t = AvaticaUtils.unique("TEST_TABLE2");
    final String create =
        String.format(Locale.ROOT, "create table if not exists %s ("
            + "id int not null, "
            + "msg varchar(3) not null)", t);
    final String insert = String.format(Locale.ROOT,
        "insert into %s values(1, 'foo')", t);
    Connection connection = ljs();
    Statement statement = connection.createStatement();
    statement.execute(create);

    Statement stmt = connection.createStatement();
    int count = stmt.executeUpdate(insert);
    assertThat(count, is(1));
    ResultSet resultSet = stmt.getResultSet();
    assertThat(resultSet, nullValue());

    PreparedStatement pstmt = connection.prepareStatement(insert);
    boolean status = pstmt.execute();
    assertThat(status, is(false));
    int updateCount = pstmt.getUpdateCount();
    assertThat(updateCount, is(1));
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
      assertThat(parameterMetaData.getParameterCount(), equalTo(0));
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

  @Test public void testStatementExecuteLocal() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      checkStatementExecute(getLocalConnection(), false);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testStatementExecuteFetch() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      // Creating a > 100 rows queries to enable fetch request
      String sql = "select * from emp cross join emp";
      checkExecuteFetch(getLocalConnection(), sql, false, 1);
      // PreparedStatement needed an extra fetch, as the execute will
      // trigger the 1st fetch. Where statement execute will execute direct
      // with results back.
      // 1 fetch, because execute did the first fetch
      checkExecuteFetch(getLocalConnection(), sql, true, 1);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void checkExecuteFetch(Connection conn, String sql, boolean isPrepare,
    int fetchCountMatch) throws SQLException {
    final Statement exeStatement;
    final ResultSet results;
    getRequestInspection().getRequestLogger().enableAndClear();
    if (isPrepare) {
      PreparedStatement statement = conn.prepareStatement(sql);
      exeStatement = statement;
      results = statement.executeQuery();
    } else {
      Statement statement = conn.createStatement();
      exeStatement = statement;
      results = statement.executeQuery(sql);
    }
    int count = 0;
    int fetchCount = 0;
    while (results.next()) {
      count++;
    }
    results.close();
    exeStatement.close();
    List<String[]> x = getRequestInspection().getRequestLogger().getAndDisable();
    for (String[] pair : x) {
      if (pair[0].contains("\"request\":\"fetch")) {
        fetchCount++;
      }
    }
    assertEquals(count, 196);
    assertEquals(fetchCountMatch, fetchCount);
  }

  @Test public void testStatementExecuteLocalMaxRow() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      checkStatementExecute(getLocalConnection(), false, 2);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testFetchSize() throws Exception {
    Connection connection = ljs();

    Statement statement = connection.createStatement();
    statement.setFetchSize(101);
    assertEquals(statement.getFetchSize(), 101);

    PreparedStatement preparedStatement =
        connection.prepareStatement("select * from (values (1, 'a')) as tbl1 (c1, c2)");
    preparedStatement.setFetchSize(1);
    assertEquals(preparedStatement.getFetchSize(), 1);
  }

  @Ignore("CALCITE-719: Refactor PreparedStatement to support setMaxRows")
  @Test public void testStatementPrepareExecuteLocalMaxRow() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      checkStatementExecute(getLocalConnection(), true, 2);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testPrepareExecuteLocal() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      checkStatementExecute(getLocalConnection(), true);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void checkStatementExecute(Connection connection,
      boolean prepare) throws SQLException {
    checkStatementExecute(connection, prepare, 0);
  }
  private void checkStatementExecute(Connection connection,
      boolean prepare, int maxRowCount) throws SQLException {
    final String sql = "select * from (\n"
        + "  values (1, 'a'), (null, 'b'), (3, 'c')) as t (c1, c2)";
    final Statement statement;
    final ResultSet resultSet;
    final ParameterMetaData parameterMetaData;
    if (prepare) {
      final PreparedStatement ps = connection.prepareStatement(sql);
      statement = ps;
      ps.setMaxRows(maxRowCount);
      parameterMetaData = ps.getParameterMetaData();
      assertTrue(ps.execute());
      resultSet = ps.getResultSet();
    } else {
      statement = connection.createStatement();
      statement.setMaxRows(maxRowCount);
      parameterMetaData = null;
      assertTrue(statement.execute(sql));
      resultSet = statement.getResultSet();
    }
    if (parameterMetaData != null) {
      assertThat(parameterMetaData.getParameterCount(), equalTo(0));
    }
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertEquals(2, metaData.getColumnCount());
    assertEquals("C1", metaData.getColumnName(1));
    assertEquals("C2", metaData.getColumnName(2));
    for (int i = 0; i < maxRowCount || (maxRowCount == 0 && i < 3); i++) {
      assertTrue(resultSet.next());
    }
    assertFalse(resultSet.next());
    resultSet.close();
    statement.close();
    connection.close();
  }

  @Test public void testCreateInsertUpdateDrop() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    final String t = AvaticaUtils.unique("TEST_TABLE");
    final String drop =
        String.format(Locale.ROOT, "drop table %s if exists", t);
    final String create =
        String.format(Locale.ROOT, "create table %s("
                + "id int not null, "
                + "msg varchar(3) not null)",
            t);
    final String insert = String.format(Locale.ROOT,
        "insert into %s values(1, 'foo')", t);
    final String update =
        String.format(Locale.ROOT, "update %s set msg='bar' where id=1", t);
    try (Connection connection = getLocalConnection();
        Statement statement = connection.createStatement();
        PreparedStatement pstmt = connection.prepareStatement("values 1")) {
      // drop
      assertFalse(statement.execute(drop));
      assertEquals(0, statement.getUpdateCount());
      assertNull(statement.getResultSet());
      try {
        final ResultSet rs = statement.executeQuery(drop);
        fail("expected error, got " + rs);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            equalTo("Statement did not return a result set"));
      }
      assertEquals(0, statement.executeUpdate(drop));
      assertEquals(0, statement.getUpdateCount());
      assertNull(statement.getResultSet());

      // create
      assertFalse(statement.execute(create));
      assertEquals(0, statement.getUpdateCount());
      assertNull(statement.getResultSet());
      assertFalse(statement.execute(drop)); // tidy up
      try {
        final ResultSet rs = statement.executeQuery(create);
        fail("expected error, got " + rs);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            equalTo("Statement did not return a result set"));
      }
      assertFalse(statement.execute(drop)); // tidy up
      assertEquals(0, statement.executeUpdate(create));
      assertEquals(0, statement.getUpdateCount());
      assertNull(statement.getResultSet());

      // insert
      assertFalse(statement.execute(insert));
      assertEquals(1, statement.getUpdateCount());
      assertNull(statement.getResultSet());
      try {
        final ResultSet rs = statement.executeQuery(insert);
        fail("expected error, got " + rs);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            equalTo("Statement did not return a result set"));
      }
      assertEquals(1, statement.executeUpdate(insert));
      assertEquals(1, statement.getUpdateCount());
      assertNull(statement.getResultSet());

      // update
      assertFalse(statement.execute(update));
      assertEquals(3, statement.getUpdateCount());
      assertNull(statement.getResultSet());
      try {
        final ResultSet rs = statement.executeQuery(update);
        fail("expected error, got " + rs);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            equalTo("Statement did not return a result set"));
      }
      assertEquals(3, statement.executeUpdate(update));
      assertEquals(3, statement.getUpdateCount());
      assertNull(statement.getResultSet());

      final String[] messages = {
        "Cannot call executeQuery(String) on prepared or callable statement",
        "Cannot call execute(String) on prepared or callable statement",
        "Cannot call executeUpdate(String) on prepared or callable statement",
      };
      for (String sql : new String[]{drop, create, insert, update}) {
        for (int i = 0; i <= 2; i++) {
          try {
            Object o;
            switch (i) {
            case 0:
              o = pstmt.executeQuery(sql);
              break;
            case 1:
              o = pstmt.execute(sql);
              break;
            default:
              o = pstmt.executeUpdate(sql);
            }
            fail("expected error, got " + o);
          } catch (SQLException e) {
            assertThat(e.getMessage(), equalTo(messages[i]));
          }
        }
      }
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testTypeHandling() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      final String query = "select * from EMP";
      try (Connection cannon = canon();
          Connection underTest = getLocalConnection();
          Statement s1 = cannon.createStatement();
          Statement s2 = underTest.createStatement()) {
        assertTrue(s1.execute(query));
        assertTrue(s2.execute(query));
        assertResultSetsEqual(s1, s2);
      }
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void assertResultSetsEqual(Statement s1, Statement s2)
      throws SQLException {
    final TimeZone moscowTz = TimeZone.getTimeZone("Europe/Moscow");
    final Calendar moscowCalendar =
        Calendar.getInstance(moscowTz, Locale.ROOT);
    final TimeZone alaskaTz = TimeZone.getTimeZone("America/Anchorage");
    final Calendar alaskaCalendar =
        Calendar.getInstance(alaskaTz, Locale.ROOT);
    try (ResultSet rs1 = s1.getResultSet();
        ResultSet rs2 = s2.getResultSet()) {
      assertEquals(rs1.getMetaData().getColumnCount(),
          rs2.getMetaData().getColumnCount());
      int colCount = rs1.getMetaData().getColumnCount();
      while (rs1.next() && rs2.next()) {
        for (int i = 0; i < colCount; i++) {
          Object o1 = rs1.getObject(i + 1);
          Object o2 = rs2.getObject(i + 1);
          if (o1 instanceof Integer && o2 instanceof Short) {
            // Hsqldb returns Integer for short columns; we prefer Short
            o1 = ((Number) o1).shortValue();
          }
          if (o1 instanceof Integer && o2 instanceof Byte) {
            // Hsqldb returns Integer for tinyint columns; we prefer Byte
            o1 = ((Number) o1).byteValue();
          }
          if (o1 instanceof Date) {
            Date d1 = rs1.getDate(i + 1, moscowCalendar);
            Date d2 = rs2.getDate(i + 1, moscowCalendar);
            assertEquals(d1, d2);
            d1 = rs1.getDate(i + 1, alaskaCalendar);
            d2 = rs2.getDate(i + 1, alaskaCalendar);
            assertEquals(d1, d2);
            d1 = rs1.getDate(i + 1, null);
            d2 = rs2.getDate(i + 1, null);
            assertEquals(d1, d2);
            d1 = rs1.getDate(i + 1);
            d2 = rs2.getDate(i + 1);
            assertEquals(d1, d2);
          }
          if (o1 instanceof Timestamp) {
            Timestamp d1 = rs1.getTimestamp(i + 1, moscowCalendar);
            Timestamp d2 = rs2.getTimestamp(i + 1, moscowCalendar);
            assertEquals(d1, d2);
            d1 = rs1.getTimestamp(i + 1, alaskaCalendar);
            d2 = rs2.getTimestamp(i + 1, alaskaCalendar);
            assertEquals(d1, d2);
            d1 = rs1.getTimestamp(i + 1, null);
            d2 = rs2.getTimestamp(i + 1, null);
            assertEquals(d1, d2);
            d1 = rs1.getTimestamp(i + 1);
            d2 = rs2.getTimestamp(i + 1);
            assertEquals(d1, d2);
          }
          assertEquals(o1, o2);
        }
      }
      assertEquals(rs1.next(), rs2.next());
    }
  }

  /** Callback to set parameters on each prepared statement before
   * each is executed and the result sets compared. */
  interface PreparedStatementFunction {
    void apply(PreparedStatement s1, PreparedStatement s2)
        throws SQLException;
  }

  /** Callback to execute some code against a connection. */
  interface ConnectionFunction {
    void apply(Connection c1) throws Exception;
  }

  @Test public void testSetParameter() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      checkSetParameter("select ? from (values 1)",
          new PreparedStatementFunction() {
            public void apply(PreparedStatement s1, PreparedStatement s2)
                throws SQLException {
              final Date d = new Date(1234567890);
              s1.setDate(1, d);
              s2.setDate(1, d);
            }
          });
      checkSetParameter("select ? from (values 1)",
          new PreparedStatementFunction() {
            public void apply(PreparedStatement s1, PreparedStatement s2)
                throws SQLException {
              final Timestamp ts = new Timestamp(123456789012L);
              s1.setTimestamp(1, ts);
              s2.setTimestamp(1, ts);
            }
          });
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  void checkSetParameter(String query, PreparedStatementFunction fn)
      throws SQLException {
    try (Connection cannon = canon();
         Connection underTest = ljs();
         PreparedStatement s1 = cannon.prepareStatement(query);
         PreparedStatement s2 = underTest.prepareStatement(query)) {
      fn.apply(s1, s2);
      assertTrue(s1.execute());
      assertTrue(s2.execute());
      assertResultSetsEqual(s1, s2);
    }
  }

  @Test public void testStatementLifecycle() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try (AvaticaConnection connection = (AvaticaConnection) getLocalConnection()) {
      Map<Integer, AvaticaStatement> clientMap = connection.statementMap;
      Cache<Integer, Object> serverMap = getLocalConnectionInternals()
          .getRemoteStatementMap(connection);
      // Other tests being run might leave statements in the cache.
      // The lock guards against more statements being cached during the test.
      serverMap.invalidateAll();
      assertEquals(0, clientMap.size());
      assertEquals(0, serverMap.size());
      Statement stmt = connection.createStatement();
      assertEquals(1, clientMap.size());
      assertEquals(1, serverMap.size());
      stmt.close();
      assertEquals(0, clientMap.size());
      assertEquals(0, serverMap.size());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testConnectionIsolation() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      Cache<String, Connection> connectionMap = getLocalConnectionInternals()
          .getRemoteConnectionMap((AvaticaConnection) getLocalConnection());
      // Other tests being run might leave connections in the cache.
      // The lock guards against more connections being cached during the test.
      connectionMap.invalidateAll();

      final String sql = "select * from (values (1, 'a'))";
      assertEquals("connection cache should start empty",
          0, connectionMap.size());
      Connection conn1 = getLocalConnection();
      Connection conn2 = getLocalConnection();
      assertEquals("we now have two connections open",
          2, connectionMap.size());
      PreparedStatement conn1stmt1 = conn1.prepareStatement(sql);
      assertEquals(
          "creating a statement does not cause new connection",
          2, connectionMap.size());
      PreparedStatement conn2stmt1 = conn2.prepareStatement(sql);
      assertEquals(
          "creating a statement does not cause new connection",
          2, connectionMap.size());
      AvaticaPreparedStatement s1 = (AvaticaPreparedStatement) conn1stmt1;
      AvaticaPreparedStatement s2 = (AvaticaPreparedStatement) conn2stmt1;
      assertFalse("connection id's should be unique",
          s1.handle.connectionId.equalsIgnoreCase(s2.handle.connectionId));
      conn2.close();
      assertEquals("closing a connection closes the server-side connection",
          1, connectionMap.size());
      conn1.close();
      assertEquals("closing a connection closes the server-side connection",
          0, connectionMap.size());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testPrepareBindExecuteFetch() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      getRequestInspection().getRequestLogger().enableAndClear();
      checkPrepareBindExecuteFetch(getLocalConnection());
      List<String[]> x = getRequestInspection().getRequestLogger().getAndDisable();
      for (String[] pair : x) {
        System.out.println(pair[0] + "=" + pair[1]);
      }
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
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
          containsString("exception while executing query: unbound parameter"));
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

  @Test public void testPrepareBindExecuteFetchVarbinary() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      final Connection connection = getLocalConnection();
      final String sql = "select x'de' || ? as c from (values (1, 'a'))";
      final PreparedStatement ps =
          connection.prepareStatement(sql);
      final ParameterMetaData parameterMetaData = ps.getParameterMetaData();
      assertThat(parameterMetaData.getParameterCount(), equalTo(1));

      ps.setBytes(1, new byte[]{65, 0, 66});
      final ResultSet resultSet = ps.executeQuery();
      assertTrue(resultSet.next());
      assertThat(resultSet.getBytes(1),
          equalTo(new byte[]{(byte) 0xDE, 65, 0, 66}));
      resultSet.close();
      ps.close();
      connection.close();
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testPrepareBindExecuteFetchDate() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      eachConnection(
          new ConnectionFunction() {
            public void apply(Connection c1) throws Exception {
              checkPrepareBindExecuteFetchDate(c1);
            }
          }, getLocalConnection());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void checkPrepareBindExecuteFetchDate(Connection connection) throws Exception {
    final String sql0 =
        "select cast(? as varchar(20)) as c\n"
            + "from (values (1, 'a'))";
    final String sql1 = "select ? + interval '2' day as c from (values (1, 'a'))";

    final Date date = Date.valueOf("2015-04-08");
    final long time = date.getTime();

    PreparedStatement ps;
    ParameterMetaData parameterMetaData;
    ResultSet resultSet;

    ps = connection.prepareStatement(sql0);
    parameterMetaData = ps.getParameterMetaData();
    assertThat(parameterMetaData.getParameterCount(), equalTo(1));
    ps.setDate(1, date);
    resultSet = ps.executeQuery();
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getString(1), is("2015-04-08"));

    ps.setTimestamp(1, new Timestamp(time));
    resultSet = ps.executeQuery();
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getString(1), is("2015-04-08 00:00:00.0"));

    ps.setTime(1, new Time(time));
    resultSet = ps.executeQuery();
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getString(1), is("00:00:00"));
    ps.close();

    ps = connection.prepareStatement(sql1);
    parameterMetaData = ps.getParameterMetaData();
    assertThat(parameterMetaData.getParameterCount(), equalTo(1));

    ps.setDate(1, date);
    resultSet = ps.executeQuery();
    assertTrue(resultSet.next());
    assertThat(resultSet.getDate(1),
        equalTo(new Date(time + TimeUnit.DAYS.toMillis(2))));
    assertThat(resultSet.getTimestamp(1),
        equalTo(new Timestamp(time + TimeUnit.DAYS.toMillis(2))));

    ps.setTimestamp(1, new Timestamp(time));
    resultSet = ps.executeQuery();
    assertTrue(resultSet.next());
    assertThat(resultSet.getTimestamp(1),
        equalTo(new Timestamp(time + TimeUnit.DAYS.toMillis(2))));
    assertThat(resultSet.getTimestamp(1),
        equalTo(new Timestamp(time + TimeUnit.DAYS.toMillis(2))));

    ps.setObject(1, new java.util.Date(time));
    resultSet = ps.executeQuery();
    assertTrue(resultSet.next());
    assertThat(resultSet.getDate(1),
        equalTo(new Date(time + TimeUnit.DAYS.toMillis(2))));
    assertThat(resultSet.getTimestamp(1),
        equalTo(new Timestamp(time + TimeUnit.DAYS.toMillis(2))));

    resultSet.close();
    ps.close();
    connection.close();
  }

  @Test public void testDatabaseProperty() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      eachConnection(
          new ConnectionFunction() {
            public void apply(Connection c1) throws Exception {
              checkDatabaseProperty(c1);
            }
          }, getLocalConnection());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void checkDatabaseProperty(Connection connection)
      throws SQLException {
    final DatabaseMetaData metaData = connection.getMetaData();
    assertThat(metaData.getSQLKeywords(), equalTo(""));
    assertThat(metaData.getStringFunctions(),
        equalTo("ASCII,CHAR,CONCAT,DIFFERENCE,HEXTORAW,INSERT,LCASE,LEFT,"
            + "LENGTH,LOCATE,LTRIM,RAWTOHEX,REPEAT,REPLACE,RIGHT,RTRIM,SOUNDEX,"
            + "SPACE,SUBSTR,UCASE"));
    assertThat(metaData.getDefaultTransactionIsolation(),
        equalTo(Connection.TRANSACTION_READ_COMMITTED));
  }

  @Test public void testBatchExecute() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      eachConnection(
          new ConnectionFunction() {
            public void apply(Connection c1) throws Exception {
              executeBatchUpdate(c1);
            }
          }, getLocalConnection());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void executeBatchUpdate(Connection conn) throws Exception {
    final int numRows = 10;
    try (Statement stmt = conn.createStatement()) {
      final String tableName = AvaticaUtils.unique("BATCH_EXECUTE");
      LOG.info("Creating table {}", tableName);
      final String createCommand = String.format(Locale.ROOT,
          "create table if not exists %s ("
              + "id int not null, "
              + "msg varchar(10) not null)", tableName);
      assertFalse("Failed to create table", stmt.execute(createCommand));

      final String updatePrefix =
          String.format(Locale.ROOT, "INSERT INTO %s values(", tableName);
      for (int i = 0; i < numRows;  i++) {
        stmt.addBatch(updatePrefix + i + ", '" + Integer.toString(i) + "')");
      }

      int[] updateCounts = stmt.executeBatch();
      assertEquals("Unexpected number of update counts returned", numRows, updateCounts.length);
      for (int i = 0; i < updateCounts.length; i++) {
        assertEquals("Unexpected update count at index " + i, 1, updateCounts[i]);
      }

      ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id asc");
      assertNotNull("ResultSet was null", rs);
      for (int i = 0; i < numRows; i++) {
        assertTrue("ResultSet should have a result", rs.next());
        assertEquals("Wrong integer value for row " + i, i, rs.getInt(1));
        assertEquals("Wrong string value for row " + i, Integer.toString(i), rs.getString(2));
      }
      assertFalse("ResultSet should have no more records", rs.next());
    }
  }

  @Test public void testPreparedBatches() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      eachConnection(
          new ConnectionFunction() {
            public void apply(Connection c1) throws Exception {
              executePreparedBatchUpdate(c1);
            }
          }, getLocalConnection());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void executePreparedBatchUpdate(Connection conn) throws Exception {
    final int numRows = 10;
    final String tableName = AvaticaUtils.unique("PREPARED_BATCH_EXECUTE");
    LOG.info("Creating table {}", tableName);
    try (Statement stmt = conn.createStatement()) {
      final String createCommand =
          String.format(Locale.ROOT, "create table if not exists %s ("
              + "id int not null, "
              + "msg varchar(10) not null)", tableName);
      assertFalse("Failed to create table", stmt.execute(createCommand));
    }

    final String insertSql =
        String.format(Locale.ROOT, "INSERT INTO %s values(?, ?)", tableName);
    try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
      // Add batches with the prepared statement
      for (int i = 0; i < numRows; i++) {
        pstmt.setInt(1, i);
        pstmt.setString(2, Integer.toString(i));
        pstmt.addBatch();
      }

      int[] updateCounts = pstmt.executeBatch();
      assertEquals("Unexpected number of update counts returned", numRows, updateCounts.length);
      for (int i = 0; i < updateCounts.length; i++) {
        assertEquals("Unexpected update count at index " + i, 1, updateCounts[i]);
      }
    }

    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id asc");
      assertNotNull("ResultSet was null", rs);
      for (int i = 0; i < numRows; i++) {
        assertTrue("ResultSet should have a result", rs.next());
        assertEquals("Wrong integer value for row " + i, i, rs.getInt(1));
        assertEquals("Wrong string value for row " + i, Integer.toString(i), rs.getString(2));
      }
      assertFalse("ResultSet should have no more records", rs.next());
    }
  }

  @Test public void testPreparedInsert() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      eachConnection(
          new ConnectionFunction() {
            public void apply(Connection c1) throws Exception {
              executePreparedInsert(c1);
            }
          }, getLocalConnection());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void executePreparedInsert(Connection conn) throws Exception {
    final int numRows = 10;
    final String tableName = AvaticaUtils.unique("PREPARED_INSERT_EXECUTE");
    LOG.info("Creating table {}", tableName);
    try (Statement stmt = conn.createStatement()) {
      final String createCommand =
          String.format(Locale.ROOT, "create table if not exists %s ("
              + "id int not null, "
              + "msg varchar(10) not null)", tableName);
      assertFalse("Failed to create table", stmt.execute(createCommand));
    }

    final String insertSql =
        String.format(Locale.ROOT, "INSERT INTO %s values(?, ?)", tableName);
    try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
      // Add batches with the prepared statement
      for (int i = 0; i < numRows; i++) {
        pstmt.setInt(1, i);
        pstmt.setString(2, Integer.toString(i));
        assertEquals(1, pstmt.executeUpdate());
      }
    }

    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id asc");
      assertNotNull("ResultSet was null", rs);
      for (int i = 0; i < numRows; i++) {
        assertTrue("ResultSet should have a result", rs.next());
        assertEquals("Wrong integer value for row " + i, i, rs.getInt(1));
        assertEquals("Wrong string value for row " + i, Integer.toString(i), rs.getString(2));
      }
      assertFalse("ResultSet should have no more records", rs.next());
    }
  }

  @Test public void testPreparedInsertWithNulls() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      eachConnection(
          new ConnectionFunction() {
            public void apply(Connection c1) throws Exception {
              executePreparedInsertWithNulls(c1);
            }
          }, getLocalConnection());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void executePreparedInsertWithNulls(Connection conn) throws Exception {
    final int numRows = 10;
    final String tableName = AvaticaUtils.unique("PREPARED_INSERT_EXECUTE_NULLS");
    LOG.info("Creating table {}", tableName);
    try (Statement stmt = conn.createStatement()) {
      final String createCommand =
          String.format(Locale.ROOT, "create table if not exists %s ("
              + "id int not null, "
              + "msg varchar(10))", tableName);
      assertFalse("Failed to create table", stmt.execute(createCommand));
    }

    final String insertSql =
        String.format(Locale.ROOT, "INSERT INTO %s values(?, ?)", tableName);
    try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
      // Add batches with the prepared statement
      for (int i = 0; i < numRows; i++) {
        pstmt.setInt(1, i);
        // Even inserts are non-null, odd are null
        if (0 == i % 2) {
          pstmt.setString(2, Integer.toString(i));
        } else {
          pstmt.setNull(2, Types.VARCHAR);
        }
        assertEquals(1, pstmt.executeUpdate());
      }
    }

    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id asc");
      assertNotNull("ResultSet was null", rs);
      for (int i = 0; i < numRows; i++) {
        assertTrue("ResultSet should have a result", rs.next());
        assertEquals("Wrong integer value for row " + i, i, rs.getInt(1));
        if (0 == i % 2) {
          assertEquals("Wrong string value for row " + i, Integer.toString(i), rs.getString(2));
        } else {
          assertNull("Expected null value for row " + i, rs.getString(2));
        }
      }
      assertFalse("ResultSet should have no more records", rs.next());
    }
  }

  @Test public void testBatchInsertWithNulls() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      eachConnection(
          new ConnectionFunction() {
            public void apply(Connection c1) throws Exception {
              executeBatchInsertWithNulls(c1);
            }
          }, getLocalConnection());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void executeBatchInsertWithNulls(Connection conn) throws Exception {
    final int numRows = 10;
    final String tableName = AvaticaUtils.unique("BATCH_INSERT_EXECUTE_NULLS");
    LOG.info("Creating table {}", tableName);
    try (Statement stmt = conn.createStatement()) {
      final String createCommand =
          String.format(Locale.ROOT, "create table if not exists %s ("
              + "id int not null, "
              + "msg varchar(10))", tableName);
      assertFalse("Failed to create table", stmt.execute(createCommand));
    }

    final String insertSql =
        String.format(Locale.ROOT, "INSERT INTO %s values(?, ?)", tableName);
    try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
      // Add batches with the prepared statement
      for (int i = 0; i < numRows; i++) {
        pstmt.setInt(1, i);
        // Even inserts are non-null, odd are null
        if (0 == i % 2) {
          pstmt.setString(2, Integer.toString(i));
        } else {
          pstmt.setNull(2, Types.VARCHAR);
        }
        pstmt.addBatch();
      }
      // Verify that all updates were successful
      int[] updateCounts = pstmt.executeBatch();
      assertEquals(numRows, updateCounts.length);
      int[] expectedCounts = new int[numRows];
      Arrays.fill(expectedCounts, 1);
      assertArrayEquals(expectedCounts, updateCounts);
    }

    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id asc");
      assertNotNull("ResultSet was null", rs);
      for (int i = 0; i < numRows; i++) {
        assertTrue("ResultSet should have a result", rs.next());
        assertEquals("Wrong integer value for row " + i, i, rs.getInt(1));
        if (0 == i % 2) {
          assertEquals("Wrong string value for row " + i, Integer.toString(i), rs.getString(2));
        } else {
          assertNull("Expected null value for row " + i, rs.getString(2));
        }
      }
      assertFalse("ResultSet should have no more records", rs.next());
    }
  }

  @Test public void preparedStatementParameterCopies() throws Exception {
    // When implementing the JDBC batch APIs, it's important that we are copying the
    // TypedValues and caching them in the AvaticaPreparedStatement. Otherwise, when we submit
    // the batch, the parameter values for the last update added will be reflected in all previous
    // updates added to the batch.
    ConnectionSpec.getDatabaseLock().lock();
    try {
      final String tableName = AvaticaUtils.unique("PREPAREDSTATEMENT_VALUES");
      final Connection conn = getLocalConnection();
      try (Statement stmt = conn.createStatement()) {
        final String sql = "CREATE TABLE " + tableName
            + " (id varchar(1) not null, col1 varchar(1) not null)";
        assertFalse(stmt.execute(sql));
      }
      try (final PreparedStatement pstmt =
          conn.prepareStatement("INSERT INTO " + tableName + " values(?, ?)")) {
        pstmt.setString(1, "a");
        pstmt.setString(2, "b");

        @SuppressWarnings("resource")
        AvaticaPreparedStatement apstmt = (AvaticaPreparedStatement) pstmt;
        TypedValue[] slots = apstmt.slots;

        assertEquals("Unexpected number of values", 2, slots.length);

        List<TypedValue> valuesReference = apstmt.getParameterValues();
        assertEquals(2, valuesReference.size());
        assertEquals(slots[0], valuesReference.get(0));
        assertEquals(slots[1], valuesReference.get(1));
        List<TypedValue> copiedValues = apstmt.copyParameterValues();
        assertEquals(2, valuesReference.size());
        assertEquals(slots[0], copiedValues.get(0));
        assertEquals(slots[1], copiedValues.get(1));

        slots[0] = null;
        slots[1] = null;

        // Modifications to the array are reflected in the List from getParameterValues()
        assertNull(valuesReference.get(0));
        assertNull(valuesReference.get(1));
        // copyParameterValues() copied the underlying array, so updates to slots is not reflected
        assertNotNull(copiedValues.get(0));
        assertNotNull(copiedValues.get(1));
      }
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testBatchInsertWithDates() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      eachConnection(
          new ConnectionFunction() {
            public void apply(Connection c1) throws Exception {
              executeBatchInsertWithDates(c1);
            }
          }, getLocalConnection());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void executeBatchInsertWithDates(Connection conn) throws Exception {
    final Calendar calendar = DateTimeUtils.calendar();
    long now = calendar.getTime().getTime();
    final int numRows = 10;
    final String tableName = AvaticaUtils.unique("BATCH_INSERT_EXECUTE_DATES");
    LOG.info("Creating table {}", tableName);
    try (Statement stmt = conn.createStatement()) {
      final String dropCommand =
          String.format(Locale.ROOT, "drop table if exists %s", tableName);
      assertFalse("Failed to drop table", stmt.execute(dropCommand));
      final String createCommand =
          String.format(Locale.ROOT, "create table %s ("
              + "id char(15) not null, "
              + "created_date date not null, "
              + "val_string varchar)", tableName);
      assertFalse("Failed to create table", stmt.execute(createCommand));
    }

    final String insertSql =
        String.format(Locale.ROOT, "INSERT INTO %s values(?, ?, ?)", tableName);
    try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
      // Add batches with the prepared statement
      for (int i = 0; i < numRows; i++) {
        pstmt.setString(1, Integer.toString(i));
        pstmt.setDate(2, new Date(now + i), calendar);
        pstmt.setString(3, UUID.randomUUID().toString());
        pstmt.addBatch();
      }
      // Verify that all updates were successful
      int[] updateCounts = pstmt.executeBatch();
      assertEquals(numRows, updateCounts.length);
      int[] expectedCounts = new int[numRows];
      Arrays.fill(expectedCounts, 1);
      assertArrayEquals(expectedCounts, updateCounts);
    }

    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id asc");
      assertNotNull("ResultSet was null", rs);
      for (int i = 0; i < numRows; i++) {
        assertTrue("ResultSet should have a result", rs.next());
        assertEquals("Wrong value for row " + i, Integer.toString(i), rs.getString(1).trim());

        Date actual = rs.getDate(2);
        calendar.setTime(actual);
        int actualDay = calendar.get(Calendar.DAY_OF_MONTH);
        int actualMonth = calendar.get(Calendar.MONTH);
        int actualYear = calendar.get(Calendar.YEAR);

        Date expected = new Date(now + i);
        calendar.setTime(expected);
        int expectedDay = calendar.get(Calendar.DAY_OF_MONTH);
        int expectedMonth = calendar.get(Calendar.MONTH);
        int expectedYear = calendar.get(Calendar.YEAR);
        assertEquals("Wrong day for row " + i, expectedDay, actualDay);
        assertEquals("Wrong month for row " + i, expectedMonth, actualMonth);
        assertEquals("Wrong year for row " + i, expectedYear, actualYear);

        assertNotNull("Non-null string for row " + i, rs.getString(3));
      }
      assertFalse("ResultSet should have no more records", rs.next());
    }
  }

  @Test public void testDatabaseMetaData() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try (Connection conn = getLocalConnection()) {
      DatabaseMetaData metadata = conn.getMetaData();
      assertTrue(metadata.isWrapperFor(AvaticaSpecificDatabaseMetaData.class));
      assertTrue(metadata.isWrapperFor(Properties.class));
      Properties props = metadata.unwrap(Properties.class);
      assertNotNull(props);

      final Object productName = props.get(DatabaseProperty.GET_DATABASE_PRODUCT_NAME.name());
      assertThat(productName, instanceOf(String.class));
      assertThat((String) productName, startsWith("HSQL"));

      final Object driverName = props.get(DatabaseProperty.GET_DRIVER_NAME.name());
      assertThat(driverName, instanceOf(String.class));
      assertThat((String) driverName, startsWith("HSQL"));

      final Object driverVersion = props.get(DatabaseProperty.GET_DRIVER_VERSION.name());
      final Object driverMinVersion = props.get(DatabaseProperty.GET_DRIVER_MINOR_VERSION.name());
      final Object driverMajVersion = props.get(DatabaseProperty.GET_DRIVER_MAJOR_VERSION.name());
      assertThat(driverVersion, instanceOf(String.class));
      assertThat(driverMinVersion, instanceOf(String.class));
      assertThat(driverMajVersion, instanceOf(String.class));
      assertThat((String) driverVersion, startsWith(driverMajVersion + "." + driverMinVersion));
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testUnicodeColumnNames() throws Exception {
    final String tableName = "unicodeColumn";
    final String columnName = "\u041d\u043e\u043c\u0435\u0440\u0422\u0435\u043b"
        + "\u0435\u0444\u043e\u043d\u0430"; // PhoneNumber in Russian
    ConnectionSpec.getDatabaseLock().lock();
    try (Connection conn = getLocalConnection();
        Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      final String sql = "CREATE TABLE " + tableName + "(" + columnName + " integer)";
      assertFalse(stmt.execute(sql));
      final ResultSet results = stmt.executeQuery("SELECT * FROM " + tableName);
      assertNotNull(results);
      ResultSetMetaData metadata = results.getMetaData();
      assertNotNull(metadata);
      String actualColumnName = metadata.getColumnName(1);
      // HSQLDB is going to upper-case the column name
      assertEquals(columnName.toUpperCase(Locale.ROOT), actualColumnName);
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  @Test public void testBigDecimalPrecision() throws Exception {
    final String tableName = "decimalPrecision";
    // DECIMAL(25,5), 20 before, 5 after
    BigDecimal decimal = new BigDecimal("12345123451234512345.09876");
    try (Connection conn = getLocalConnection();
        Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      assertFalse(stmt.execute("CREATE TABLE " + tableName + " (col1 DECIMAL(25,5))"));

      // Insert a single decimal
      try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName
          + " values (?)")) {
        pstmt.setBigDecimal(1, decimal);
        assertEquals(1, pstmt.executeUpdate());
      }

      ResultSet results = stmt.executeQuery("SELECT * FROM " + tableName);
      assertNotNull(results);
      assertTrue(results.next());
      BigDecimal actualDecimal = results.getBigDecimal(1);
      assertEquals(decimal, actualDecimal);
    }
  }

  @Test public void testPreparedClearBatches() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      eachConnection(
          new ConnectionFunction() {
            public void apply(Connection c1) throws Exception {
              executePreparedBatchClears(c1);
            }
          }, getLocalConnection());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void executePreparedBatchClears(Connection conn) throws Exception {
    final int numRows = 10;
    final String tableName = AvaticaUtils.unique("BATCH_CLEARS");
    LOG.info("Creating table {}", tableName);
    try (Statement stmt = conn.createStatement()) {
      final String createCommand =
          String.format(Locale.ROOT, "create table if not exists %s ("
              + "id int not null, "
              + "msg varchar(10) not null)", tableName);
      assertFalse("Failed to create table", stmt.execute(createCommand));
    }

    final String insertSql =
        String.format(Locale.ROOT, "INSERT INTO %s values(?, ?)", tableName);
    try (PreparedStatement pstmt = conn.prepareStatement(insertSql)) {
      // Add batches with the prepared statement
      for (int i = 0; i < numRows; i++) {
        pstmt.setInt(1, i);
        pstmt.setString(2, Integer.toString(i));
        pstmt.addBatch();

        if (numRows / 2 - 1 == i) {
          // Clear the first 5 entries in the batch
          pstmt.clearBatch();
        }
      }

      int[] updateCounts = pstmt.executeBatch();
      assertEquals("Unexpected number of update counts returned", numRows / 2, updateCounts.length);
      for (int i = 0; i < updateCounts.length; i++) {
        assertEquals("Unexpected update count at index " + i, 1, updateCounts[i]);
      }
    }

    try (Statement stmt = conn.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id asc");
      assertNotNull("ResultSet was null", rs);
      for (int i = 0 + (numRows / 2); i < numRows; i++) {
        assertTrue("ResultSet should have a result", rs.next());
        assertEquals("Wrong integer value for row " + i, i, rs.getInt(1));
        assertEquals("Wrong string value for row " + i, Integer.toString(i), rs.getString(2));
      }
      assertFalse("ResultSet should have no more records", rs.next());
    }
  }

  @Test public void testBatchClear() throws Exception {
    ConnectionSpec.getDatabaseLock().lock();
    try {
      eachConnection(
          new ConnectionFunction() {
            public void apply(Connection c1) throws Exception {
              executeBatchClear(c1);
            }
          }, getLocalConnection());
    } finally {
      ConnectionSpec.getDatabaseLock().unlock();
    }
  }

  private void executeBatchClear(Connection conn) throws Exception {
    final int numRows = 10;
    try (Statement stmt = conn.createStatement()) {
      final String tableName = AvaticaUtils.unique("BATCH_EXECUTE");
      LOG.info("Creating table {}", tableName);
      final String createCommand =
          String.format(Locale.ROOT, "create table if not exists %s ("
              + "id int not null, "
              + "msg varchar(10) not null)", tableName);
      assertFalse("Failed to create table", stmt.execute(createCommand));

      final String updatePrefix =
          String.format(Locale.ROOT, "INSERT INTO %s values(", tableName);
      for (int i = 0; i < numRows;  i++) {
        stmt.addBatch(updatePrefix + i + ", '" + Integer.toString(i) + "')");
        if (numRows / 2 - 1 == i) {
          stmt.clearBatch();
        }
      }

      int[] updateCounts = stmt.executeBatch();
      assertEquals("Unexpected number of update counts returned", numRows / 2, updateCounts.length);
      for (int i = 0; i < updateCounts.length; i++) {
        assertEquals("Unexpected update count at index " + i, 1, updateCounts[i]);
      }

      ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY id asc");
      assertNotNull("ResultSet was null", rs);
      for (int i = 0 + (numRows / 2); i < numRows; i++) {
        assertTrue("ResultSet should have a result", rs.next());
        assertEquals("Wrong integer value for row " + i, i, rs.getInt(1));
        assertEquals("Wrong string value for row " + i, Integer.toString(i), rs.getString(2));
      }
      assertFalse("ResultSet should have no more records", rs.next());
    }
  }

  @Test public void testDecimalParameters() throws Exception {
    final String tableName = "decimalParameters";
    BigDecimal decimal = new BigDecimal("123451234512345");
    try (Connection conn = getLocalConnection();
        Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName + " (keycolumn VARCHAR(5), column1 DECIMAL(15,0))";
      assertFalse(stmt.execute(sql));

      // Insert a single decimal
      try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName
          + " values (?, ?)")) {
        ParameterMetaData metadata = pstmt.getParameterMetaData();
        assertNotNull(metadata);
        assertEquals(5, metadata.getPrecision(1));
        assertEquals(0, metadata.getScale(1));
        assertEquals(15, metadata.getPrecision(2));
        assertEquals(0, metadata.getScale(2));

        pstmt.setString(1, "asdfg");
        pstmt.setBigDecimal(2, decimal);
        assertEquals(1, pstmt.executeUpdate());
      }

      ResultSet results = stmt.executeQuery("SELECT * FROM " + tableName);
      assertNotNull(results);
      assertTrue(results.next());
      BigDecimal actualDecimal = results.getBigDecimal(2);
      assertEquals(decimal, actualDecimal);

      ResultSetMetaData resultMetadata = results.getMetaData();
      assertNotNull(resultMetadata);
      assertEquals(15, resultMetadata.getPrecision(2));
      assertEquals(0, resultMetadata.getScale(2));
    }
  }

  @Test public void testSignedParameters() throws Exception {
    final String tableName = "signedParameters";
    try (Connection conn = getLocalConnection();
        Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName + " (keycolumn VARCHAR(5), column1 integer)";
      assertFalse(stmt.execute(sql));

      // Insert a single decimal
      try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName
          + " values (?, ?)")) {
        ParameterMetaData metadata = pstmt.getParameterMetaData();
        assertNotNull(metadata);
        assertFalse("Varchar should not be signed", metadata.isSigned(1));
        assertTrue("Integer should be signed", metadata.isSigned(2));

        pstmt.setString(1, "asdfg");
        pstmt.setInt(2, 10);
        assertEquals(1, pstmt.executeUpdate());
      }

      ResultSet results = stmt.executeQuery("SELECT * FROM " + tableName);
      assertNotNull(results);
      assertTrue(results.next());
      assertEquals("asdfg", results.getString(1));
      assertEquals(10, results.getInt(2));

      ResultSetMetaData resultMetadata = results.getMetaData();
      assertNotNull(resultMetadata);
      assertFalse("Varchar should not be signed", resultMetadata.isSigned(1));
      assertTrue("Integer should be signed", resultMetadata.isSigned(2));
    }
  }

  @Test public void testDateParameterWithGMT0() throws Exception {
    final String tableName = "dateParameters";
    try (Connection conn = getLocalConnection();
         Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName + " (keycolumn VARCHAR(5), column1 date)";
      assertFalse(stmt.execute(sql));
      TimeZone tzUtc = TimeZone.getTimeZone("UTC");
      Calendar cUtc = Calendar.getInstance(tzUtc, Locale.ROOT);
      cUtc.set(Calendar.YEAR, 1970);
      cUtc.set(Calendar.MONTH, Calendar.JANUARY);
      cUtc.set(Calendar.DAY_OF_MONTH, 1);
      cUtc.set(Calendar.HOUR_OF_DAY, 0);
      cUtc.set(Calendar.MINUTE, 0);
      cUtc.set(Calendar.SECOND, 0);
      cUtc.set(Calendar.MILLISECOND, 0);
      Date inputDate = new Date(cUtc.getTimeInMillis());
      // Insert a single date
      try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName
              + " values (?, ?)")) {
        ParameterMetaData metadata = pstmt.getParameterMetaData();
        assertNotNull(metadata);

        pstmt.setString(1, "asdfg");
        pstmt.setDate(2, inputDate, cUtc);
        assertEquals(1, pstmt.executeUpdate());
      }

      ResultSet results = stmt.executeQuery("SELECT * FROM " + tableName);
      assertNotNull(results);
      assertTrue(results.next());
      assertEquals("asdfg", results.getString(1));
      Date outputDate = results.getDate(2, cUtc);
      assertEquals(inputDate.getTime(), outputDate.getTime());
      assertEquals(0, outputDate.getTime());
    }
  }

  @Test public void testDateParameterWithGMTN() throws Exception {
    final String tableName = "dateParameters";
    try (Connection conn = getLocalConnection();
         Statement stmt = conn.createStatement()) {
      assertFalse(stmt.execute("DROP TABLE IF EXISTS " + tableName));
      String sql = "CREATE TABLE " + tableName + " (keycolumn VARCHAR(5), column1 date)";
      assertFalse(stmt.execute(sql));
      TimeZone tzUtc = TimeZone.getTimeZone("GMT+8");
      Calendar cUtc = Calendar.getInstance(tzUtc, Locale.ROOT);
      cUtc.set(Calendar.YEAR, 1970);
      cUtc.set(Calendar.MONTH, Calendar.JANUARY);
      cUtc.set(Calendar.DAY_OF_MONTH, 1);
      cUtc.set(Calendar.HOUR_OF_DAY, 0);
      cUtc.set(Calendar.MINUTE, 0);
      cUtc.set(Calendar.SECOND, 0);
      cUtc.set(Calendar.MILLISECOND, 0);
      Date inputDate = new Date(cUtc.getTimeInMillis());
      // Insert a single date
      try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO " + tableName
              + " values (?, ?)")) {
        ParameterMetaData metadata = pstmt.getParameterMetaData();
        assertNotNull(metadata);

        pstmt.setString(1, "gfdsa");
        pstmt.setDate(2, inputDate, cUtc);
        assertEquals(1, pstmt.executeUpdate());
      }

      ResultSet results = stmt.executeQuery("SELECT * FROM " + tableName);
      assertNotNull(results);
      assertTrue(results.next());
      assertEquals("gfdsa", results.getString(1));
      Date outputDate = results.getDate(2, cUtc);
      assertEquals(inputDate.getTime(), outputDate.getTime());
      assertEquals(-28800000, outputDate.getTime());
    }
  }

  /**
   * Factory that creates a service based on a local JDBC connection.
   */
  public static class LocalJdbcServiceFactory implements Service.Factory {
    @Override public Service create(AvaticaConnection connection) {
      try {
        return new LocalService(
            new JdbcMeta(CONNECTION_SPEC.url, CONNECTION_SPEC.username,
                CONNECTION_SPEC.password));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Factory that creates a fully-local Protobuf service.
   */
  public static class QuasiRemotePBJdbcServiceFactory implements Service.Factory {
    private static Service service;

    private static RequestInspection requestInspection;

    static void initService() {
      try {
        final JdbcMeta jdbcMeta = new JdbcMeta(CONNECTION_SPEC.url,
            CONNECTION_SPEC.username, CONNECTION_SPEC.password);
        final LocalService localService = new LocalService(jdbcMeta);
        service = new LoggingLocalProtobufService(localService, new ProtobufTranslationImpl());
        requestInspection = (RequestInspection) service;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override public Service create(AvaticaConnection connection) {
      assert null != service;
      return service;
    }
  }

  /**
   * Proxy that logs all requests passed into the {@link LocalProtobufService}.
   */
  public static class LoggingLocalProtobufService extends LocalProtobufService
      implements RequestInspection {
    private static final ThreadLocal<RequestLogger> THREAD_LOG =
        new ThreadLocal<RequestLogger>() {
          @Override protected RequestLogger initialValue() {
            return new RequestLogger();
          }
        };

    public LoggingLocalProtobufService(Service service, ProtobufTranslation translation) {
      super(service, translation);
    }

    @Override public RequestLogger getRequestLogger() {
      return THREAD_LOG.get();
    }

    @Override public Response _apply(Request request) {
      final RequestLogger logger = THREAD_LOG.get();
      try {
        String jsonRequest = JsonService.MAPPER.writeValueAsString(request);
        logger.requestStart(jsonRequest);

        Response response = super._apply(request);

        String jsonResponse = JsonService.MAPPER.writeValueAsString(response);
        logger.requestEnd(jsonRequest, jsonResponse);

        return response;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Factory that creates a service based on a local JDBC connection.
   */
  public static class QuasiRemoteJdbcServiceFactory implements Service.Factory {

    /** a singleton instance that is recreated for each test */
    private static Service service;

    private static RequestInspection requestInspection;

    static void initService() {
      try {
        final JdbcMeta jdbcMeta = new JdbcMeta(CONNECTION_SPEC.url,
            CONNECTION_SPEC.username, CONNECTION_SPEC.password);
        final LocalService localService = new LocalService(jdbcMeta);
        service = new LoggingLocalJsonService(localService);
        requestInspection = (RequestInspection) service;
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override public Service create(AvaticaConnection connection) {
      assert service != null;
      return service;
    }
  }

  /**
   * Implementation that reaches into current connection state via reflection to extract certain
   * internal information.
   */
  public static class QuasiRemoteJdbcServiceInternals implements ConnectionInternals {

    @Override public Cache<Integer, Object>
    getRemoteStatementMap(AvaticaConnection connection) throws Exception {
      Field metaF = AvaticaConnection.class.getDeclaredField("meta");
      metaF.setAccessible(true);
      Meta clientMeta = (Meta) metaF.get(connection);
      Field remoteMetaServiceF = clientMeta.getClass().getDeclaredField("service");
      remoteMetaServiceF.setAccessible(true);
      LocalJsonService remoteMetaService = (LocalJsonService) remoteMetaServiceF.get(clientMeta);
      // Use the explicitly class to avoid issues with LoggingLocalJsonService
      Field remoteMetaServiceServiceF = LocalJsonService.class.getDeclaredField("service");
      remoteMetaServiceServiceF.setAccessible(true);
      LocalService remoteMetaServiceService =
          (LocalService) remoteMetaServiceServiceF.get(remoteMetaService);
      Field remoteMetaServiceServiceMetaF =
          remoteMetaServiceService.getClass().getDeclaredField("meta");
      remoteMetaServiceServiceMetaF.setAccessible(true);
      JdbcMeta serverMeta = (JdbcMeta) remoteMetaServiceServiceMetaF.get(remoteMetaServiceService);
      Field jdbcMetaStatementMapF = JdbcMeta.class.getDeclaredField("statementCache");
      jdbcMetaStatementMapF.setAccessible(true);
      //noinspection unchecked
      @SuppressWarnings("unchecked")
      Cache<Integer, Object> cache = (Cache<Integer, Object>) jdbcMetaStatementMapF.get(serverMeta);
      return cache;
    }

    @Override public Cache<String, Connection>
    getRemoteConnectionMap(AvaticaConnection connection) throws Exception {
      Field metaF = AvaticaConnection.class.getDeclaredField("meta");
      metaF.setAccessible(true);
      Meta clientMeta = (Meta) metaF.get(connection);
      Field remoteMetaServiceF = clientMeta.getClass().getDeclaredField("service");
      remoteMetaServiceF.setAccessible(true);
      LocalJsonService remoteMetaService = (LocalJsonService) remoteMetaServiceF.get(clientMeta);
      // Get the field explicitly off the correct class to avoid LocalLoggingJsonService.class
      Field remoteMetaServiceServiceF = LocalJsonService.class.getDeclaredField("service");
      remoteMetaServiceServiceF.setAccessible(true);
      LocalService remoteMetaServiceService =
          (LocalService) remoteMetaServiceServiceF.get(remoteMetaService);
      Field remoteMetaServiceServiceMetaF =
          remoteMetaServiceService.getClass().getDeclaredField("meta");
      remoteMetaServiceServiceMetaF.setAccessible(true);
      JdbcMeta serverMeta = (JdbcMeta) remoteMetaServiceServiceMetaF.get(remoteMetaServiceService);
      Field jdbcMetaConnectionCacheF = JdbcMeta.class.getDeclaredField("connectionCache");
      jdbcMetaConnectionCacheF.setAccessible(true);
      //noinspection unchecked
      @SuppressWarnings("unchecked")
      Cache<String, Connection> cache =
          (Cache<String, Connection>) jdbcMetaConnectionCacheF.get(serverMeta);
      return cache;
    }
  }

  /**
   * Implementation that reaches into current connection state via reflection to extract certain
   * internal information.
   */
  public static class QuasiRemoteProtobufJdbcServiceInternals implements ConnectionInternals {

    @Override public Cache<Integer, Object>
    getRemoteStatementMap(AvaticaConnection connection) throws Exception {
      Field metaF = AvaticaConnection.class.getDeclaredField("meta");
      metaF.setAccessible(true);
      Meta clientMeta = (Meta) metaF.get(connection);
      Field remoteMetaServiceF = clientMeta.getClass().getDeclaredField("service");
      remoteMetaServiceF.setAccessible(true);
      LocalProtobufService remoteMetaService =
          (LocalProtobufService) remoteMetaServiceF.get(clientMeta);
      // Use the explicitly class to avoid issues with LoggingLocalJsonService
      Field remoteMetaServiceServiceF = LocalProtobufService.class.getDeclaredField("service");
      remoteMetaServiceServiceF.setAccessible(true);
      LocalService remoteMetaServiceService =
          (LocalService) remoteMetaServiceServiceF.get(remoteMetaService);
      Field remoteMetaServiceServiceMetaF =
          remoteMetaServiceService.getClass().getDeclaredField("meta");
      remoteMetaServiceServiceMetaF.setAccessible(true);
      JdbcMeta serverMeta = (JdbcMeta) remoteMetaServiceServiceMetaF.get(remoteMetaServiceService);
      Field jdbcMetaStatementMapF = JdbcMeta.class.getDeclaredField("statementCache");
      jdbcMetaStatementMapF.setAccessible(true);
      //noinspection unchecked
      @SuppressWarnings("unchecked")
      Cache<Integer, Object> cache = (Cache<Integer, Object>) jdbcMetaStatementMapF.get(serverMeta);
      return cache;
    }

    @Override public Cache<String, Connection>
    getRemoteConnectionMap(AvaticaConnection connection) throws Exception {
      Field metaF = AvaticaConnection.class.getDeclaredField("meta");
      metaF.setAccessible(true);
      Meta clientMeta = (Meta) metaF.get(connection);
      Field remoteMetaServiceF = clientMeta.getClass().getDeclaredField("service");
      remoteMetaServiceF.setAccessible(true);
      LocalProtobufService remoteMetaService =
          (LocalProtobufService) remoteMetaServiceF.get(clientMeta);
      // Get the field explicitly off the correct class to avoid LocalLoggingJsonService.class
      Field remoteMetaServiceServiceF = LocalProtobufService.class.getDeclaredField("service");
      remoteMetaServiceServiceF.setAccessible(true);
      LocalService remoteMetaServiceService =
          (LocalService) remoteMetaServiceServiceF.get(remoteMetaService);
      Field remoteMetaServiceServiceMetaF =
          remoteMetaServiceService.getClass().getDeclaredField("meta");
      remoteMetaServiceServiceMetaF.setAccessible(true);
      JdbcMeta serverMeta = (JdbcMeta) remoteMetaServiceServiceMetaF.get(remoteMetaServiceService);
      Field jdbcMetaConnectionCacheF = JdbcMeta.class.getDeclaredField("connectionCache");
      jdbcMetaConnectionCacheF.setAccessible(true);
      //noinspection unchecked
      @SuppressWarnings("unchecked")
      Cache<String, Connection> cache =
          (Cache<String, Connection>) jdbcMetaConnectionCacheF.get(serverMeta);
      return cache;
    }
  }

  /**
   * Provides access to a log of requests.
   */
  interface RequestInspection {
    RequestLogger getRequestLogger();
  }

  /** Extension to {@link LocalJsonService} that writes requests and responses
   * into a thread-local. */
  private static class LoggingLocalJsonService extends LocalJsonService
      implements RequestInspection {
    private static final ThreadLocal<RequestLogger> THREAD_LOG =
        new ThreadLocal<RequestLogger>() {
          @Override protected RequestLogger initialValue() {
            return new RequestLogger();
          }
        };

    public LoggingLocalJsonService(LocalService localService) {
      super(localService);
    }

    @Override public String apply(String request) {
      final RequestLogger logger = THREAD_LOG.get();
      logger.requestStart(request);
      final String response = super.apply(request);
      logger.requestEnd(request, response);
      return response;
    }

    @Override public RequestLogger getRequestLogger() {
      return THREAD_LOG.get();
    }
  }

  /** Logs request and response strings if enabled. */
  private static class RequestLogger {
    final List<String[]> requestResponses = new ArrayList<>();
    boolean enabled;

    void enableAndClear() {
      enabled = true;
      requestResponses.clear();
    }

    void requestStart(String request) {
      if (enabled) {
        requestResponses.add(new String[]{request, null});
      }
    }

    void requestEnd(String request, String response) {
      if (enabled) {
        String[] last = requestResponses.get(requestResponses.size() - 1);
        if (!request.equals(last[0])) {
          throw new AssertionError();
        }
        last[1] = response;
      }
    }

    List<String[]> getAndDisable() {
      enabled = false;
      return new ArrayList<>(requestResponses);
    }
  }
}

// End RemoteDriverTest.java
