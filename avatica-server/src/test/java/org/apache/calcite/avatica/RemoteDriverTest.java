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

import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.MockJsonService;
import org.apache.calcite.avatica.remote.Service;

import com.google.common.cache.Cache;

import net.hydromatic.scott.data.hsqldb.ScottHsqldb;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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

  // see [CALCITE-687] Make RemoteDriverTest.testStatementLifecycle thread-safe
  private static final boolean JDK17 =
      System.getProperty("java.version").startsWith("1.7");

  private Connection mjs() throws SQLException {
    return DriverManager.getConnection("jdbc:avatica:remote:factory=" + MJS);
  }

  private Connection ljs() throws SQLException {
    return DriverManager.getConnection("jdbc:avatica:remote:factory=" + QRJS);
  }

  private Connection canon() throws SQLException {
    return DriverManager.getConnection(CONNECTION_SPEC.url,
        CONNECTION_SPEC.username, CONNECTION_SPEC.password);
  }

  /** Executes a lambda for the canonical connection and the local
   * connection. */
  public void eachConnection(ConnectionFunction f) throws Exception {
    for (int i = 0; i < 2; i++) {
      try (Connection connection = i == 0 ? canon() : ljs()) {
        f.apply(connection);
      }
    }
  }

  @Before
  public void before() throws Exception {
    QuasiRemoteJdbcServiceFactory.initService();
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

  @Test public void testDatabaseProperties() throws Exception {
    final Connection connection = ljs();
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

  @Test public void testTypeInfo() throws Exception {
    final Connection connection = ljs();
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
    if (JDK17) {
      return;
    }
    checkStatementExecuteQuery(ljs(), false);
  }

  @Ignore
  @Test public void testStatementExecuteQueryMock() throws Exception {
    checkStatementExecuteQuery(mjs(), false);
  }

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
    checkStatementExecute(ljs(), false);
  }

  @Test public void testPrepareExecuteLocal() throws Exception {
    checkStatementExecute(ljs(), true);
  }

  private void checkStatementExecute(Connection connection, boolean prepare) throws SQLException {
    final String sql = "select * from (\n"
        + "  values (1, 'a'), (null, 'b'), (3, 'c')) as t (c1, c2)";
    final Statement statement;
    final ResultSet resultSet;
    final ParameterMetaData parameterMetaData;
    if (prepare) {
      final PreparedStatement ps = connection.prepareStatement(sql);
      statement = ps;
      parameterMetaData = ps.getParameterMetaData();
      assertTrue(ps.execute());
      resultSet = ps.getResultSet();
    } else {
      statement = connection.createStatement();
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
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertTrue(resultSet.next());
    assertFalse(resultSet.next());
    resultSet.close();
    statement.close();
    connection.close();
  }

  @Test public void testCreateInsertUpdateDrop() throws Exception {
    final String drop = "drop table TEST_TABLE if exists";
    final String create = "create table TEST_TABLE("
        + "id int not null, "
        + "msg varchar(3) not null)";
    final String insert = "insert into TEST_TABLE values(1, 'foo')";
    final String update = "update TEST_TABLE set msg='bar' where id=1";
    try (Connection connection = ljs();
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
    }
  }

  @Test public void testTypeHandling() throws Exception {
    final String query = "select * from EMP";
    try (Connection cannon = canon();
        Connection underTest = ljs();
        Statement s1 = cannon.createStatement();
        Statement s2 = underTest.createStatement()) {
      assertTrue(s1.execute(query));
      assertTrue(s2.execute(query));
      assertResultSetsEqual(s1, s2);
    }
  }

  private void assertResultSetsEqual(Statement s1, Statement s2)
      throws SQLException {
    final TimeZone moscowTz = TimeZone.getTimeZone("Europe/Moscow");
    final Calendar moscowCalendar = Calendar.getInstance(moscowTz);
    final TimeZone alaskaTz = TimeZone.getTimeZone("America/Anchorage");
    final Calendar alaskaCalendar = Calendar.getInstance(alaskaTz);
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

  @Ignore("[CALCITE-687] Make RemoteDriverTest.testStatementLifecycle thread-safe")
  @Test public void testStatementLifecycle() throws Exception {
    try (AvaticaConnection connection = (AvaticaConnection) ljs()) {
      Map<Integer, AvaticaStatement> clientMap = connection.statementMap;
      Cache<Integer, Object> serverMap =
          QuasiRemoteJdbcServiceFactory.getRemoteStatementMap(connection);
      assertEquals(0, clientMap.size());
      assertEquals(0, serverMap.size());
      Statement stmt = connection.createStatement();
      assertEquals(1, clientMap.size());
      assertEquals(1, serverMap.size());
      stmt.close();
      assertEquals(0, clientMap.size());
      assertEquals(0, serverMap.size());
    }
  }

  @Ignore("[CALCITE-687] Make RemoteDriverTest.testStatementLifecycle thread-safe")
  @Test public void testConnectionIsolation() throws Exception {
    final String sql = "select * from (values (1, 'a'))";
    Connection conn1 = ljs();
    Connection conn2 = ljs();
    Cache<String, Connection> connectionMap =
        QuasiRemoteJdbcServiceFactory.getRemoteConnectionMap(
            (AvaticaConnection) conn1);
    assertEquals("connection cache should start empty",
        0, connectionMap.size());
    PreparedStatement conn1stmt1 = conn1.prepareStatement(sql);
    assertEquals(
        "statement creation implicitly creates a connection server-side",
        1, connectionMap.size());
    PreparedStatement conn2stmt1 = conn2.prepareStatement(sql);
    assertEquals(
        "statement creation implicitly creates a connection server-side",
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
  }

  @Test public void testPrepareBindExecuteFetch() throws Exception {
    if (JDK17) {
      return;
    }
    LoggingLocalJsonService.THREAD_LOG.get().enableAndClear();
    checkPrepareBindExecuteFetch(ljs());
    List<String[]> x = LoggingLocalJsonService.THREAD_LOG.get().getAndDisable();
    for (String[] pair : x) {
      System.out.println(pair[0] + "=" + pair[1]);
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

  @Test public void testPrepareBindExecuteFetchVarbinary() throws Exception {
    final Connection connection = ljs();
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
  }

  @Test public void testPrepareBindExecuteFetchDate() throws Exception {
    eachConnection(
        new ConnectionFunction() {
          public void apply(Connection c1) throws Exception {
            checkPrepareBindExecuteFetchDate(c1);
          }
        });
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
    eachConnection(
        new ConnectionFunction() {
          public void apply(Connection c1) throws Exception {
            checkDatabaseProperty(c1);
          }
        });
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
   * Factory that creates a service based on a local JDBC connection.
   */
  public static class QuasiRemoteJdbcServiceFactory implements Service.Factory {

    /** a singleton instance that is recreated for each test */
    private static Service service;

    static void initService() {
      try {
        final JdbcMeta jdbcMeta = new JdbcMeta(CONNECTION_SPEC.url,
            CONNECTION_SPEC.username, CONNECTION_SPEC.password);
        final LocalService localService = new LocalService(jdbcMeta);
        service = new LoggingLocalJsonService(localService);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    @Override public Service create(AvaticaConnection connection) {
      assert service != null;
      return service;
    }

    /**
     * Reach into the guts of a quasi-remote connection and pull out the
     * statement map from the other side.
     * TODO: refactor tests to replace reflection with package-local access
     */
    static Cache<Integer, Object>
    getRemoteStatementMap(AvaticaConnection connection) throws Exception {
      Field metaF = AvaticaConnection.class.getDeclaredField("meta");
      metaF.setAccessible(true);
      Meta clientMeta = (Meta) metaF.get(connection);
      Field remoteMetaServiceF = clientMeta.getClass().getDeclaredField("service");
      remoteMetaServiceF.setAccessible(true);
      LocalJsonService remoteMetaService = (LocalJsonService) remoteMetaServiceF.get(clientMeta);
      Field remoteMetaServiceServiceF = remoteMetaService.getClass().getDeclaredField("service");
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
      return (Cache<Integer, Object>) jdbcMetaStatementMapF.get(serverMeta);
    }

    /**
     * Reach into the guts of a quasi-remote connection and pull out the
     * connection map from the other side.
     * TODO: refactor tests to replace reflection with package-local access
     */
    static Cache<String, Connection>
    getRemoteConnectionMap(AvaticaConnection connection) throws Exception {
      Field metaF = AvaticaConnection.class.getDeclaredField("meta");
      metaF.setAccessible(true);
      Meta clientMeta = (Meta) metaF.get(connection);
      Field remoteMetaServiceF = clientMeta.getClass().getDeclaredField("service");
      remoteMetaServiceF.setAccessible(true);
      LocalJsonService remoteMetaService = (LocalJsonService) remoteMetaServiceF.get(clientMeta);
      Field remoteMetaServiceServiceF = remoteMetaService.getClass().getDeclaredField("service");
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
      return (Cache<String, Connection>) jdbcMetaConnectionCacheF.get(serverMeta);
    }
  }

  /** Extension to {@link LocalJsonService} that writes requests and responses
   * into a thread-local. */
  private static class LoggingLocalJsonService extends LocalJsonService {
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
