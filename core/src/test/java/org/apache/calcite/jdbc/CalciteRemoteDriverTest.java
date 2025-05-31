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
import org.apache.calcite.avatica.server.AvaticaJsonHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.server.Main;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.JdbcFrontLinqBackTest;
import org.apache.calcite.test.schemata.hr.Employee;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.apache.calcite.test.Matchers.primitiveArrayWithSize;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static java.util.Objects.requireNonNull;

/**
 * Test for Calcite's remote JDBC driver.
 *
 * <p>Technically, the test is thread-safe, however Calcite/Avatica have
 * thread-safety issues; see
 * <a href="https://issues.apache.org/jira/browse/CALCITE-2853">
 * [CALCITE-2853] avatica.MetaImpl and calcite.jdbc.CalciteMetaImpl are not
 * thread-safe</a>.
 *
 * <p>Under JDK 23 and higher, this test requires
 * "{@code -Djava.security.manager=allow}" command-line arguments due to
 * Avatica's use of deprecated methods in {@link javax.security.auth.Subject}.
 * These arguments are set automatically if you run via Gradle.
 */
@Execution(ExecutionMode.SAME_THREAD)
class CalciteRemoteDriverTest {
  public static final String LJS = Factory2.class.getName();

  private final PrintWriter out =
      CalciteSystemProperty.DEBUG.value() ? Util.printWriter(System.out)
          : new PrintWriter(new StringWriter());

  private static @Nullable Connection localConnection;
  private static @Nullable HttpServer start;

  @BeforeAll public static void beforeClass() throws Exception {
    localConnection = CalciteAssert.hr().connect();

    // Make sure we pick an ephemeral port for the server
    final String[] args = {Factory.class.getName()};
    start = Main.start(args, 0, AvaticaJsonHandler::new);
  }

  protected static Connection getRemoteConnection() throws SQLException {
    final int port = requireNonNull(start, "start").getPort();
    return DriverManager.getConnection(
        "jdbc:avatica:remote:url=http://localhost:" + port);
  }

  @AfterAll public static void afterClass() throws Exception {
    if (localConnection != null) {
      localConnection.close();
      localConnection = null;
    }

    if (start != null) {
      start.stop();
    }
  }

  private static ResultSet getSchemas(Connection connection) {
    try {
      return connection.getMetaData().getSchemas();
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
  }

  private static ResultSet getCatalogs(Connection connection) {
    try {
      return connection.getMetaData().getCatalogs();
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
  }

  private static ResultSet getColumns(Connection connection) {
    try {
      return connection.getMetaData().getColumns(null, null, null, null);
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
  }

  private static ResultSet getTables(Connection connection) {
    try {
      return connection.getMetaData().getTables(null, null, null, null);
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
  }

  private static ResultSet getTypeInfo(Connection connection) {
    try {
      return connection.getMetaData().getTypeInfo();
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
  }

  private static ResultSet getTableTypes(Connection connection) {
    try {
      return connection.getMetaData().getTableTypes();
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
  }

  @Test void testCatalogsLocal() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:factory=" + LJS);
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getCatalogs();
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertThat(metaData.getColumnCount(), is(1));
    assertThat(metaData.getColumnName(1), is("TABLE_CAT"));
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.next(), is(false));
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test void testSchemasLocal() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:factory=" + LJS);
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet = connection.getMetaData().getSchemas();
    final ResultSetMetaData metaData = resultSet.getMetaData();
    assertThat(metaData.getColumnCount(), is(2));
    assertThat(metaData.getColumnName(1), is("TABLE_SCHEM"));
    assertThat(metaData.getColumnName(2), is("TABLE_CATALOG"));
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getString(1), equalTo("POST"));
    assertThat(resultSet.getString(2), nullValue());
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getString(1), equalTo("foodmart"));
    assertThat(resultSet.getString(2), nullValue());
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.next(), is(false));
    resultSet.close();
    connection.close();
    assertThat(connection.isClosed(), is(true));
  }

  @Test void testMetaFunctionsLocal() throws Exception {
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
            notNullValue());
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

  @Test void testMeasureColumnsLocal() throws Exception {
    final Connection connection = makeConnectionWithMeasures();
    assertThat(connection.isClosed(), is(false));
    final ResultSet resultSet =
        connection.getMetaData().getColumns(null, "foo", null, "salary");
    assertThat(resultSet.getMetaData().getColumnCount(), is(24));
    final int typeNameIdx = resultSet.findColumn("TYPE_NAME");
    final int dataTypeIdx = resultSet.findColumn("DATA_TYPE");
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getString(typeNameIdx),
        is("MEASURE<FLOAT NOT NULL> NOT NULL"));
    assertThat(resultSet.getInt(dataTypeIdx), is(6));
  }

  @Test void testRemoteCatalogs() {
    CalciteAssert.hr()
        .with(CalciteRemoteDriverTest::getRemoteConnection)
        .metaData(CalciteRemoteDriverTest::getCatalogs)
        .returns("TABLE_CAT=null\n");
  }

  @Test void testRemoteSchemas() {
    CalciteAssert.hr()
        .with(CalciteRemoteDriverTest::getRemoteConnection)
        .metaData(CalciteRemoteDriverTest::getSchemas)
        .returns("TABLE_SCHEM=POST; TABLE_CATALOG=null\n"
            + "TABLE_SCHEM=foodmart; TABLE_CATALOG=null\n"
            + "TABLE_SCHEM=hr; TABLE_CATALOG=null\n"
            + "TABLE_SCHEM=metadata; TABLE_CATALOG=null\n");
  }

  /** Checks that the default {@code getColumns()} response
   * contains the 24 standard columns specified in the JDBC specification
   * and in the correct order. */
  @Test void testRemoteColumns() {
    CalciteAssert.hr()
        .with(CalciteRemoteDriverTest::getRemoteConnection)
        .metaData(CalciteRemoteDriverTest::getColumns)
        .returns(
            CalciteAssert.checkResultContains("TABLE_CAT=null; "
                + "TABLE_SCHEM=POST; TABLE_NAME=EMPS; COLUMN_NAME=EMPNO; "
                + "DATA_TYPE=4; TYPE_NAME=INTEGER NOT NULL; COLUMN_SIZE=-1; "
                + "BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; "
                + "NULLABLE=0; REMARKS=null; COLUMN_DEF=null; "
                + "SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; "
                + "CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=1; IS_NULLABLE=NO; "
                + "SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; "
                + "SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; "
                + "IS_GENERATEDCOLUMN="));
  }

  /** Checks that the default {@code getTables()} response contains the 10
   * standard columns specified in the JDBC specification and in the correct
   * order. */
  @Test void testRemoteTables() {
    CalciteAssert.hr()
        .with(CalciteRemoteDriverTest::getRemoteConnection)
        .metaData(CalciteRemoteDriverTest::getTables)
        .returns(
            CalciteAssert.checkResultContains("TABLE_CAT=null; "
                + "TABLE_SCHEM=POST; TABLE_NAME=DEPT; TABLE_TYPE=VIEW; "
                + "REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; "
                + "TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; "
                + "REF_GENERATION=null"));
  }

  @Test void testRemoteTypeInfo() {
    // TypeInfo does not include internal types (NULL, SYMBOL, ANY, etc.)
    CalciteAssert.hr()
        .with(CalciteRemoteDriverTest::getRemoteConnection)
        .metaData(CalciteRemoteDriverTest::getTypeInfo)
        .returns(CalciteAssert.checkResultCount(is(49)));
  }

  @Test void testRemoteTableTypes() {
    CalciteAssert.hr()
        .with(CalciteRemoteDriverTest::getRemoteConnection)
        .metaData(CalciteRemoteDriverTest::getTableTypes)
        .returns("TABLE_TYPE=TABLE\n"
            + "TABLE_TYPE=VIEW\n");
  }

  @Test void testRemoteExecuteQuery() {
    CalciteAssert.hr()
        .with(CalciteRemoteDriverTest::getRemoteConnection)
        .query("values (1, 'a'), (cast(null as integer), 'b')")
        .returnsUnordered("EXPR$0=1; EXPR$1=a", "EXPR$0=null; EXPR$1=b");
  }

  /** Same query as {@link #testRemoteExecuteQuery()}, run without the test
   * infrastructure. */
  @Test void testRemoteExecuteQuery2() throws Exception {
    try (Connection remoteConnection = getRemoteConnection()) {
      final Statement statement = remoteConnection.createStatement();
      final String sql = "values (1, 'a'), (cast(null as integer), 'b')";
      final ResultSet resultSet = statement.executeQuery(sql);
      int n = 0;
      while (resultSet.next()) {
        ++n;
      }
      assertThat(n, equalTo(2));
    }
  }

  /** For each (source, destination) type, make sure that we can convert bind
   * variables. */
  @Test void testParameterConvert() throws Exception {
    final StringBuilder sql = new StringBuilder("select 1");
    final Map<SqlType, Integer> map = new HashMap<>();
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
  @Test void testTableB5() {
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
  @Test void testTableB6() {
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
    final PrintWriter out =
        CalciteSystemProperty.DEBUG.value()
            ? Util.printWriter(System.out)
            : new PrintWriter(new StringWriter());
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
  @Test void testRemoteStatementExecute() throws Exception {
    try (Connection remoteConnection = getRemoteConnection()) {
      final Statement statement = remoteConnection.createStatement();
      final boolean status = statement.execute("values (1, 2), (3, 4), (5, 6)");
      assertThat(status, is(true));
      final ResultSet resultSet = statement.getResultSet();
      int n = 0;
      while (resultSet.next()) {
        ++n;
      }
      assertThat(n, equalTo(3));
    }
  }

  @Test void testAvaticaConnectionException() {
    assertThrows(SQLException.class, () -> {
      try (Connection remoteConnection = getRemoteConnection()) {
        remoteConnection.isValid(-1);
      }
    });
  }

  @Test void testAvaticaStatementException() {
    assertThrows(SQLException.class, () -> {
      try (Connection remoteConnection = getRemoteConnection()) {
        try (Statement statement = remoteConnection.createStatement()) {
          statement.setCursorName("foo");
        }
      }
    });
  }

  @Test void testAvaticaStatementGetMoreResults() throws Exception {
    try (Connection remoteConnection = getRemoteConnection()) {
      try (Statement statement = remoteConnection.createStatement()) {
        assertThat(statement.getMoreResults(), is(false));
      }
    }
  }

  @Test void testRemoteExecute() throws Exception {
    try (Connection remoteConnection = getRemoteConnection()) {
      ResultSet resultSet =
          remoteConnection.createStatement().executeQuery(
              "select * from \"hr\".\"emps\"");
      int count = 0;
      while (resultSet.next()) {
        ++count;
      }
      assertThat(count > 0, is(true));
    }
  }

  @Test void testRemoteExecuteMaxRow() throws Exception {
    try (Connection remoteConnection = getRemoteConnection()) {
      Statement statement = remoteConnection.createStatement();
      statement.setMaxRows(2);
      ResultSet resultSet =
          statement.executeQuery("select * from \"hr\".\"emps\"");
      int count = 0;
      while (resultSet.next()) {
        ++count;
      }
      assertThat(count, equalTo(2));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-661">[CALCITE-661]
   * Remote fetch in Calcite JDBC driver</a>. */
  @Test void testRemotePrepareExecute() throws Exception {
    try (Connection remoteConnection = getRemoteConnection()) {
      final PreparedStatement preparedStatement =
          remoteConnection.prepareStatement("select * from \"hr\".\"emps\"");
      ResultSet resultSet = preparedStatement.executeQuery();
      int count = 0;
      while (resultSet.next()) {
        ++count;
      }
      assertThat(count > 0, is(true));
    }
  }

  public static Connection makeConnection(boolean withMeasures)
      throws Exception {
    List<Employee> employees = new ArrayList<>();
    for (int i = 1; i <= 101; i++) {
      employees.add(new Employee(i, 0, "first", 0f, null));
    }
    return JdbcFrontLinqBackTest.makeConnection(employees, withMeasures);
  }

  /** Creates a connection without measures. */
  public static Connection makeConnection() throws Exception {
    return makeConnection(false);
  }

  /** Creates a connection with measures. */
  public static Connection makeConnectionWithMeasures() throws Exception {
    return makeConnection(true);
  }

  @Test void testLocalStatementFetch() throws Exception {
    Connection conn = makeConnection();
    String sql = "select * from \"foo\".\"bar\"";
    Statement statement = conn.createStatement();
    boolean status = statement.execute(sql);
    assertThat(status, is(true));
    ResultSet resultSet = statement.getResultSet();
    int count = 0;
    while (resultSet.next()) {
      count += 1;
    }
    assertThat(count, is(101));
  }

  @Disabled("Cannot yet execute query with virtual measures")
  @Test void testLocalStatementResultSetMeasureMetadata() throws Exception {
    Connection conn = makeConnectionWithMeasures();
    String sql = "select * from \"foo\".\"bar\"";
    Statement statement = conn.createStatement();
    boolean status = statement.execute(sql);
    assertThat(status, is(true));
    ResultSet resultSet = statement.getResultSet();
    String typeName = resultSet.getMetaData().getColumnTypeName(4);
    Integer ordinal = resultSet.getMetaData().getColumnType(4);
    assertThat(typeName, is("MEASURE<FLOAT>"));
    assertThat(ordinal, is(6));
  }

  /** Test that returns all result sets in one go. */
  @Test void testLocalPreparedStatementFetch() throws Exception {
    Connection conn = makeConnection();
    assertThat(conn.isClosed(), is(false));
    String sql = "select * from \"foo\".\"bar\"";
    PreparedStatement preparedStatement = conn.prepareStatement(sql);
    assertThat(conn.isClosed(), is(false));
    boolean status = preparedStatement.execute();
    assertThat(status, is(true));
    ResultSet resultSet = preparedStatement.getResultSet();
    assertThat(resultSet, notNullValue());
    int count = 0;
    while (resultSet.next()) {
      assertThat(resultSet.getObject(1), notNullValue());
      count += 1;
    }
    assertThat(count, is(101));
  }

  @Test void testRemoteStatementFetch() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:factory="
            + LocalServiceMoreFactory.class.getName());
    String sql = "select * from \"foo\".\"bar\"";
    Statement statement = connection.createStatement();
    boolean status = statement.execute(sql);
    assertThat(status, is(true));
    ResultSet resultSet = statement.getResultSet();
    int count = 0;
    while (resultSet.next()) {
      count += 1;
    }
    assertThat(count, is(101));
  }

  @Test void testRemotePreparedStatementFetch() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:factory="
            + LocalServiceMoreFactory.class.getName());
    assertThat(connection.isClosed(), is(false));

    String sql = "select * from \"foo\".\"bar\"";
    PreparedStatement preparedStatement = connection.prepareStatement(sql);
    assertThat(preparedStatement.isClosed(), is(false));

    boolean status = preparedStatement.execute();
    assertThat(status, is(true));
    ResultSet resultSet = preparedStatement.getResultSet();
    assertThat(resultSet, notNullValue());

    int count = 0;
    while (resultSet.next()) {
      assertThat(resultSet.getObject(1), notNullValue());
      count += 1;
    }
    assertThat(count, is(101));
  }

  /** Service factory that creates a Calcite instance with more data. */
  public static class LocalServiceMoreFactory implements Service.Factory {
    @Override public Service create(AvaticaConnection connection) {
      try {
        Connection conn = makeConnection();
        final CalciteMetaImpl meta =
            CalciteMetaImpl.create(conn.unwrap(CalciteConnection.class));
        return new LocalService(meta);
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
      }
    }
  }

  /** A bunch of sample values of various types. */
  private static final List<Object> SAMPLE_VALUES =
      ImmutableList.of(false, true,
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
          // Next one causes problems for most types
          // BigDecimal.valueOf(Double.MAX_VALUE),
          BigDecimal.valueOf(Long.MIN_VALUE),
          // datetime
          new Timestamp(0),
          new java.sql.Date(0),
          new Time(0),
          // string
          "", "foo", " foo! Baz ",
          // byte[]
          new byte[0], "hello".getBytes(StandardCharsets.UTF_8));

  private static List<Object> values(Class clazz) {
    final List<Object> list = new ArrayList<>();
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
        return ((String) o).getBytes(StandardCharsets.UTF_8);
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
        final DateFormat dateFormat =
            DateFormat.getDateTimeInstance(DateFormat.SHORT, DateFormat.SHORT,
                Locale.ROOT);
        return dateFormat.parse((String) o);
      }
    }
    if (clazz == Calendar.class) {
      if (o instanceof String) {
        return Util.calendar(); // TODO:
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

  /** Factory that creates a {@link Meta} that can see the test databases. */
  public static class Factory implements Meta.Factory {
    public Meta create(List<String> args) {
      try {
        final Connection connection = CalciteAssert.hr().connect();
        return CalciteMetaImpl.create((CalciteConnection) connection);
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
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
        throw TestUtil.rethrow(e);
      }
    }
  }

  /** Factory that creates a Service with connection to a modifiable table. */
  public static class LocalServiceModifiableFactory implements Service.Factory {
    @Override public Service create(AvaticaConnection connection) {
      try {
        Connection conn = JdbcFrontLinqBackTest.makeConnection();
        final CalciteMetaImpl meta =
            CalciteMetaImpl.create(conn.unwrap(CalciteConnection.class));
        return new LocalService(meta);
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
      }
    }
  }

  /** Test remote Statement insert. */
  @Test void testInsert() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:factory="
            + LocalServiceModifiableFactory.class.getName());
    assertThat(connection.isClosed(), is(false));
    Statement statement = connection.createStatement();
    assertThat(statement.isClosed(), is(false));

    String sql = "insert into \"foo\".\"bar\" values (1, 1, 'second', 2, 2)";
    boolean status = statement.execute(sql);
    assertThat(status, is(false));
    ResultSet resultSet = statement.getResultSet();
    assertThat(resultSet, nullValue());
    int updateCount = statement.getUpdateCount();
    assertThat(updateCount, is(1));
    connection.close();
  }

  /** Test remote Statement batched insert. */
  @Test void testInsertBatch() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:factory="
            + LocalServiceModifiableFactory.class.getName());
    assertThat(connection.getMetaData().supportsBatchUpdates(), is(true));
    assertThat(connection.isClosed(), is(false));
    Statement statement = connection.createStatement();
    assertThat(statement.isClosed(), is(false));

    String sql = "insert into \"foo\".\"bar\" values (1, 1, 'second', 2, 2)";
    statement.addBatch(sql);
    statement.addBatch(sql);
    int[] updateCounts = statement.executeBatch();
    assertThat(updateCounts, primitiveArrayWithSize(2));
    assertThat(updateCounts[0], is(1));
    assertThat(updateCounts[1], is(1));
    ResultSet resultSet = statement.getResultSet();
    assertThat(resultSet, nullValue());

    // Now empty batch
    statement.clearBatch();
    updateCounts = statement.executeBatch();
    assertThat(updateCounts, primitiveArrayWithSize(0));
    resultSet = statement.getResultSet();
    assertThat(resultSet, nullValue());

    connection.close();
  }

  /**
   * Remote PreparedStatement insert WITHOUT bind variables.
   */
  @Test void testRemotePreparedStatementInsert() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:factory="
            + LocalServiceModifiableFactory.class.getName());
    assertThat(connection.isClosed(), is(false));

    String sql = "insert into \"foo\".\"bar\" values (1, 1, 'second', 2, 2)";
    PreparedStatement preparedStatement = connection.prepareStatement(sql);
    assertThat(preparedStatement.isClosed(), is(false));

    boolean status = preparedStatement.execute();
    assertThat(status, is(false));
    ResultSet resultSet = preparedStatement.getResultSet();
    assertThat(resultSet, nullValue());
    int updateCount = preparedStatement.getUpdateCount();
    assertThat(updateCount, is(1));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3338">[CALCITE-3338]
   * Error with executeBatch and preparedStatement when using RemoteMeta</a>. */
  @Test void testInsertBatchWithPreparedStatement() throws Exception {
    final Connection connection =
        DriverManager.getConnection("jdbc:avatica:remote:factory="
            + LocalServiceModifiableFactory.class.getName());

    PreparedStatement pst =
        connection.prepareStatement("insert into \"foo\".\"bar\"\n"
            + "values (?, ?, ?, ?, ?)");
    pst.setInt(1, 1);
    pst.setInt(2, 1);
    pst.setString(3, "second");
    pst.setInt(4, 1);
    pst.setInt(5, 1);
    pst.addBatch();
    pst.addBatch();

    int[] updateCounts = pst.executeBatch();
    assertThat(updateCounts, primitiveArrayWithSize(2));
    assertThat(updateCounts[0], is(1));
    assertThat(updateCounts[1], is(1));
    ResultSet resultSet = pst.getResultSet();
    assertThat(resultSet, nullValue());

    connection.close();
  }

  /**
   * Remote PreparedStatement insert WITH bind variables.
   */
  @Test void testRemotePreparedStatementInsert2() {
  }
}
