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
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.remote.LocalJsonService;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.test.CalciteAssert;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.hamcrest.CoreMatchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test for Calcite's remote JDBC driver.
 */
public class CalciteRemoteDriverTest {
  public static final String LJS = Factory2.class.getName();

  private final PrintStream out =
      CalcitePrepareImpl.DEBUG ? System.out
          : new PrintStream(new ByteArrayOutputStream());

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
  private static final Function<Connection, ResultSet> GET_COLUMNS =
      new Function<Connection, ResultSet>() {
        public ResultSet apply(Connection input) {
          try {
            return input.getMetaData().getColumns(null, null, null, null);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      };
  private static final Function<Connection, ResultSet> GET_TYPEINFO =
      new Function<Connection, ResultSet>() {
        public ResultSet apply(Connection input) {
          try {
            return input.getMetaData().getTypeInfo();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      };
  private static final Function<Connection, ResultSet> GET_TABLE_TYPES =
      new Function<Connection, ResultSet>() {
        public ResultSet apply(Connection input) {
          try {
            return input.getMetaData().getTableTypes();
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

  @Test public void testMetaFunctionsLocal() throws Exception {
    final Connection connection =
        CalciteAssert.hr().connect();
    assertThat(connection.isClosed(), is(false));
    for (Meta.DatabaseProperty p : Meta.DatabaseProperty.values()) {
      switch (p) {
      case GET_NUMERIC_FUNCTIONS:
        assertThat(connection.getMetaData().getNumericFunctions(),
            not(equalTo("")));
        break;
      case GET_SYSTEM_FUNCTIONS:
        assertThat(connection.getMetaData().getSystemFunctions(),
            CoreMatchers.notNullValue());
        break;
      case GET_TIME_DATE_FUNCTIONS:
        assertThat(connection.getMetaData().getTimeDateFunctions(),
            not(equalTo("")));
        break;
      case GET_S_Q_L_KEYWORDS:
        assertThat(connection.getMetaData().getSQLKeywords(),
            not(equalTo("")));
        break;
      case GET_STRING_FUNCTIONS:
        assertThat(connection.getMetaData().getStringFunctions(),
            not(equalTo("")));
        break;
      default:
      }
    }
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

  @Test public void testRemoteColumns() throws Exception {
    CalciteAssert.hr().with(REMOTE_CONNECTION_FACTORY)
        .metaData(GET_COLUMNS)
        .returns(CalciteAssert.checkResultContains("COLUMN_NAME=EMPNO"));
  }

  @Test public void testRemoteTypeInfo() throws Exception {
    CalciteAssert.hr().with(REMOTE_CONNECTION_FACTORY)
        .metaData(GET_TYPEINFO)
        .returns(CalciteAssert.checkResultCount(30));
  }

  @Test public void testRemoteTableTypes() throws Exception {
    CalciteAssert.hr().with(REMOTE_CONNECTION_FACTORY)
        .metaData(GET_TABLE_TYPES)
        .returns("TABLE_TYPE=TABLE\n"
            + "TABLE_TYPE=VIEW\n");
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

  /** For each (source, destination) type, make sure that we can convert bind
   * variables. */
  @Test public void testParameterConvert() throws Exception {
    final StringBuilder sql = new StringBuilder("select 1");
    final Map<SqlType, Integer> map = Maps.newHashMap();
    for (Map.Entry<Class, SqlType> entry : SqlType.getSetConversions()) {
      final SqlType sqlType = entry.getValue();
      switch (sqlType) {
      case BIT:
      case LONGVARCHAR:
      case LONGVARBINARY:
      case NCHAR:
      case NVARCHAR:
      case LONGNVARCHAR:
      case BLOB:
      case CLOB:
      case NCLOB:
      case ARRAY:
      case REF:
      case STRUCT:
      case DATALINK:
      case ROWID:
      case JAVA_OBJECT:
      case SQLXML:
        continue;
      }
      if (!map.containsKey(sqlType)) {
        sql.append(", cast(? as ").append(sqlType).append(")");
        map.put(sqlType, map.size() + 1);
      }
    }
    sql.append(" from (values 1)");
    final PreparedStatement statement =
        localConnection.prepareStatement(sql.toString());
    for (Map.Entry<SqlType, Integer> entry : map.entrySet()) {
      statement.setNull(entry.getValue(), entry.getKey().id);
    }
    for (Map.Entry<Class, SqlType> entry : SqlType.getSetConversions()) {
      final SqlType sqlType = entry.getValue();
      if (!map.containsKey(sqlType)) {
        continue;
      }
      int param = map.get(sqlType);
      Class clazz = entry.getKey();
      for (Object sampleValue : values(sqlType.boxedClass())) {
        switch (sqlType) {
        case DATE:
        case TIME:
        case TIMESTAMP:
          continue; // FIXME
        }
        if (clazz == Calendar.class) {
          continue; // FIXME
        }
        final Object o;
        try {
          o = convert(sampleValue, clazz);
        } catch (IllegalArgumentException | ParseException e) {
          continue;
        }
        out.println("check " + o + " (originally "
            + sampleValue.getClass() + ", now " + o.getClass()
            + ") converted to " + sqlType);
        if (o instanceof Double && o.equals(Double.POSITIVE_INFINITY)
            || o instanceof Float && o.equals(Float.POSITIVE_INFINITY)) {
          continue;
        }
        statement.setObject(param, o, sqlType.id);
        final ResultSet resultSet = statement.executeQuery();
        assertThat(resultSet.next(), is(true));
        out.println(resultSet.getString(param + 1));
      }
    }
    statement.close();
  }

  /** Check that the "set" conversion table looks like Table B-5 in JDBC 4.1
   * specification */
  @Test public void testTableB5() {
    SqlType[] columns = {
      SqlType.TINYINT, SqlType.SMALLINT, SqlType.INTEGER, SqlType.BIGINT,
      SqlType.REAL, SqlType.FLOAT, SqlType.DOUBLE, SqlType.DECIMAL,
      SqlType.NUMERIC, SqlType.BIT, SqlType.BOOLEAN, SqlType.CHAR,
      SqlType.VARCHAR, SqlType.LONGVARCHAR, SqlType.BINARY, SqlType.VARBINARY,
      SqlType.LONGVARBINARY, SqlType.DATE, SqlType.TIME, SqlType.TIMESTAMP,
      SqlType.ARRAY, SqlType.BLOB, SqlType.CLOB, SqlType.STRUCT, SqlType.REF,
      SqlType.DATALINK, SqlType.JAVA_OBJECT, SqlType.ROWID, SqlType.NCHAR,
      SqlType.NVARCHAR, SqlType.LONGNVARCHAR, SqlType.NCLOB, SqlType.SQLXML
    };
    Class[] rows = {
      String.class, BigDecimal.class, Boolean.class, Byte.class, Short.class,
      Integer.class, Long.class, Float.class, Double.class, byte[].class,
      BigInteger.class, java.sql.Date.class, Time.class, Timestamp.class,
      Array.class, Blob.class, Clob.class, Struct.class, Ref.class,
      URL.class, Class.class, RowId.class, NClob.class, SQLXML.class,
      Calendar.class, java.util.Date.class
    };
    for (Class row : rows) {
      final String s = row == Date.class ? row.getName() : row.getSimpleName();
      out.print(pad(s));
      for (SqlType column : columns) {
        out.print(SqlType.canSet(row, column) ? "x " : ". ");
      }
      out.println();
    }
  }

  private String pad(String x) {
    while (x.length() < 20) {
      x = x + " ";
    }
    return x;
  }

  /** Check that the "get" conversion table looks like Table B-5 in JDBC 4.1
   * specification */
  @Test public void testTableB6() {
    SqlType[] columns = {
      SqlType.TINYINT, SqlType.SMALLINT, SqlType.INTEGER, SqlType.BIGINT,
      SqlType.REAL, SqlType.FLOAT, SqlType.DOUBLE, SqlType.DECIMAL,
      SqlType.NUMERIC, SqlType.BIT, SqlType.BOOLEAN, SqlType.CHAR,
      SqlType.VARCHAR, SqlType.LONGVARCHAR, SqlType.BINARY, SqlType.VARBINARY,
      SqlType.LONGVARBINARY, SqlType.DATE, SqlType.TIME, SqlType.TIMESTAMP,
      SqlType.CLOB, SqlType.BLOB, SqlType.ARRAY, SqlType.REF,
      SqlType.DATALINK, SqlType.STRUCT, SqlType.JAVA_OBJECT, SqlType.ROWID,
      SqlType.NCHAR, SqlType.NVARCHAR, SqlType.LONGNVARCHAR, SqlType.NCLOB,
      SqlType.SQLXML
    };
    final PrintStream out =
        CalcitePrepareImpl.DEBUG ? System.out
            : new PrintStream(new ByteArrayOutputStream());
    for (SqlType.Method row : SqlType.Method.values()) {
      out.print(pad(row.methodName));
      for (SqlType column : columns) {
        out.print(SqlType.canGet(row, column) ? "x " : ". ");
      }
      out.println();
    }
  }

  /** Checks {@link Statement#execute} on a query over a remote connection.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-646">[CALCITE-646]
   * AvaticaStatement execute method broken over remote JDBC</a>. */
  @Test public void testRemoteStatementExecute() throws Exception {
    final Statement statement = remoteConnection.createStatement();
    final boolean status = statement.execute("values (1, 2), (3, 4), (5, 6)");
    final ResultSet resultSet = statement.getResultSet();
    int n = 0;
    while (resultSet.next()) {
      ++n;
    }
    assertThat(n, equalTo(3));

  }

  @Test(expected = SQLException.class)
  public void testAvaticaConnectionException() throws Exception {
    remoteConnection.isValid(0);
  }

  @Test(expected = SQLException.class)
  public void testAvaticaStatementException() throws Exception {
    remoteConnection.createStatement().getMoreResults();
  }

  @Test public void testRemoteExecute() throws Exception {
    ResultSet resultSet =
        remoteConnection.createStatement().executeQuery(
            "select * from \"hr\".\"emps\"");
    int count = 0;
    while (resultSet.next()) {
      ++count;
    }
    assertTrue(count > 0);
  }

  @Test public void testRemoteExecuteMaxRow() throws Exception {
    Statement statement = remoteConnection.createStatement();
    statement.setMaxRows(2);
    ResultSet resultSet = statement.executeQuery(
            "select * from \"hr\".\"emps\"");
    int count = 0;
    while (resultSet.next()) {
      ++count;
    }
    assertThat(count, equalTo(2));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-661">[CALCITE-661]
   * Remote fetch in Calcite JDBC driver</a>. */
  @Test public void testRemotePrepareExecute() throws Exception {
    final PreparedStatement preparedStatement =
        remoteConnection.prepareStatement("select * from \"hr\".\"emps\"");
    ResultSet resultSet = preparedStatement.executeQuery();
    int count = 0;
    while (resultSet.next()) {
      ++count;
    }
    assertTrue(count > 0);
  }

  /** A bunch of sample values of various types. */
  private static final List<Object> SAMPLE_VALUES =
      ImmutableList.<Object>of(false, true,
          // byte
          (byte) 0, (byte) 1, Byte.MIN_VALUE, Byte.MAX_VALUE,
          // short
          (short) 0, (short) 1, Short.MIN_VALUE, Short.MAX_VALUE,
          (short) Byte.MIN_VALUE, (short) Byte.MAX_VALUE,
          // int
          0, 1, -3, Integer.MIN_VALUE, Integer.MAX_VALUE,
          (int) Short.MIN_VALUE, (int) Short.MAX_VALUE,
          (int) Byte.MIN_VALUE, (int) Byte.MAX_VALUE,
          // long
          0L, 1L, -2L, Long.MIN_VALUE, Long.MAX_VALUE,
          (long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE,
          (long) Short.MIN_VALUE, (long) Short.MAX_VALUE,
          (long) Byte.MIN_VALUE, (long) Byte.MAX_VALUE,
          // float
          0F, 1.5F, -10F, Float.MIN_VALUE, Float.MAX_VALUE,
          // double
          0D, Math.PI, Double.MIN_VALUE, Double.MAX_VALUE,
          (double) Float.MIN_VALUE, (double) Float.MAX_VALUE,
          (double) Integer.MIN_VALUE, (double) Integer.MAX_VALUE,
          // BigDecimal
          BigDecimal.ZERO, BigDecimal.ONE, BigDecimal.valueOf(2.5D),
          BigDecimal.valueOf(Double.MAX_VALUE),
          BigDecimal.valueOf(Long.MIN_VALUE),
          // datetime
          new Timestamp(0),
          new java.sql.Date(0),
          new Time(0),
          // string
          "", "foo", " foo! Baz ",
          // byte[]
          new byte[0], "hello".getBytes());

  private static List<Object> values(Class clazz) {
    final List<Object> list = Lists.newArrayList();
    for (Object sampleValue : SAMPLE_VALUES) {
      if (sampleValue.getClass() == clazz) {
        list.add(sampleValue);
      }
    }
    return list;
  }

  private Object convert(Object o, Class clazz) throws ParseException {
    if (o.getClass() == clazz) {
      return o;
    }
    if (clazz == String.class) {
      return o.toString();
    }
    if (clazz == Boolean.class) {
      return o instanceof Number
          && ((Number) o).intValue() != 0
          || o instanceof String
          && ((String) o).equalsIgnoreCase("true");
    }
    if (clazz == byte[].class) {
      if (o instanceof String) {
        return ((String) o).getBytes();
      }
    }
    if (clazz == Timestamp.class) {
      if (o instanceof String) {
        return Timestamp.valueOf((String) o);
      }
    }
    if (clazz == Time.class) {
      if (o instanceof String) {
        return Time.valueOf((String) o);
      }
    }
    if (clazz == java.sql.Date.class) {
      if (o instanceof String) {
        return java.sql.Date.valueOf((String) o);
      }
    }
    if (clazz == java.util.Date.class) {
      if (o instanceof String) {
        return DateFormat.getInstance().parse((String) o);
      }
    }
    if (clazz == Calendar.class) {
      if (o instanceof String) {
        return Calendar.getInstance(); // TODO:
      }
    }
    if (o instanceof Boolean) {
      o = (Boolean) o ? 1 : 0;
    }
    if (o instanceof Number) {
      final Number number = (Number) o;
      if (Number.class.isAssignableFrom(clazz)) {
        if (clazz == BigDecimal.class) {
          if (o instanceof Double || o instanceof Float) {
            return new BigDecimal(number.doubleValue());
          } else {
            return new BigDecimal(number.longValue());
          }
        } else if (clazz == BigInteger.class) {
          return new BigInteger(o.toString());
        } else if (clazz == Byte.class || clazz == byte.class) {
          return number.byteValue();
        } else if (clazz == Short.class || clazz == short.class) {
          return number.shortValue();
        } else if (clazz == Integer.class || clazz == int.class) {
          return number.intValue();
        } else if (clazz == Long.class || clazz == long.class) {
          return number.longValue();
        } else if (clazz == Float.class || clazz == float.class) {
          return number.floatValue();
        } else if (clazz == Double.class || clazz == double.class) {
          return number.doubleValue();
        }
      }
    }
    if (Number.class.isAssignableFrom(clazz)) {
      if (clazz == BigDecimal.class) {
        return new BigDecimal(o.toString());
      } else if (clazz == BigInteger.class) {
        return new BigInteger(o.toString());
      } else if (clazz == Byte.class || clazz == byte.class) {
        return Byte.valueOf(o.toString());
      } else if (clazz == Short.class || clazz == short.class) {
        return Short.valueOf(o.toString());
      } else if (clazz == Integer.class || clazz == int.class) {
        return Integer.valueOf(o.toString());
      } else if (clazz == Long.class || clazz == long.class) {
        return Long.valueOf(o.toString());
      } else if (clazz == Float.class || clazz == float.class) {
        return Float.valueOf(o.toString());
      } else if (clazz == Double.class || clazz == double.class) {
        return Double.valueOf(o.toString());
      }
    }
    throw new AssertionError("cannot convert " + o + "(" + o.getClass()
        + ") to " + clazz);
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
