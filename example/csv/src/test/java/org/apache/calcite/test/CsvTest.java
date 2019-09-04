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
package org.apache.calcite.test;

import org.apache.calcite.adapter.csv.CsvSchemaFactory;
import org.apache.calcite.adapter.csv.CsvStreamTableFactory;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Unit test of the Calcite adapter for CSV.
 */
public class CsvTest {
  private void close(Connection connection, Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }


  /** Quotes a string for Java or JSON. */
  private static String escapeString(String s) {
    return escapeString(new StringBuilder(), s).toString();
  }

  /** Quotes a string for Java or JSON, into a builder. */
  private static StringBuilder escapeString(StringBuilder buf, String s) {
    buf.append('"');
    int n = s.length();
    char lastChar = 0;
    for (int i = 0; i < n; ++i) {
      char c = s.charAt(i);
      switch (c) {
      case '\\':
        buf.append("\\\\");
        break;
      case '"':
        buf.append("\\\"");
        break;
      case '\n':
        buf.append("\\n");
        break;
      case '\r':
        if (lastChar != '\n') {
          buf.append("\\r");
        }
        break;
      default:
        buf.append(c);
        break;
      }
      lastChar = c;
    }
    return buf.append('"');
  }

  /**
   * Tests the vanity driver.
   */
  @Ignore
  @Test public void testVanityDriver() throws SQLException {
    Properties info = new Properties();
    Connection connection =
        DriverManager.getConnection("jdbc:csv:", info);
    connection.close();
  }

  /**
   * Tests the vanity driver with properties in the URL.
   */
  @Ignore
  @Test public void testVanityDriverArgsInUrl() throws SQLException {
    Connection connection =
        DriverManager.getConnection("jdbc:csv:"
            + "directory='foo'");
    connection.close();
  }

  /** Tests an inline schema with a non-existent directory. */
  @Test public void testBadDirectory() throws SQLException {
    Properties info = new Properties();
    info.put("model",
        "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "   schemas: [\n"
            + "     {\n"
            + "       type: 'custom',\n"
            + "       name: 'bad',\n"
            + "       factory: 'org.apache.calcite.adapter.csv.CsvSchemaFactory',\n"
            + "       operand: {\n"
            + "         directory: '/does/not/exist'\n"
            + "       }\n"
            + "     }\n"
            + "   ]\n"
            + "}");

    Connection connection =
        DriverManager.getConnection("jdbc:calcite:", info);
    // must print "directory ... not found" to stdout, but not fail
    ResultSet tables =
        connection.getMetaData().getTables(null, null, null, null);
    tables.next();
    tables.close();
    connection.close();
  }

  /**
   * Reads from a table.
   */
  @Test public void testSelect() throws SQLException {
    sql("model", "select * from EMPS").ok();
  }

  @Test public void testSelectSingleProjectGz() throws SQLException {
    sql("smart", "select name from EMPS").ok();
  }

  @Test public void testSelectSingleProject() throws SQLException {
    sql("smart", "select name from DEPTS").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-898">[CALCITE-898]
   * Type inference multiplying Java long by SQL INTEGER</a>. */
  @Test public void testSelectLongMultiplyInteger() throws SQLException {
    final String sql = "select empno * 3 as e3\n"
        + "from long_emps where empno = 100";

    sql("bug", sql).checking(resultSet -> {
      try {
        assertThat(resultSet.next(), is(true));
        Long o = (Long) resultSet.getObject(1);
        assertThat(o, is(300L));
        assertThat(resultSet.next(), is(false));
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    }).ok();
  }

  @Test public void testCustomTable() throws SQLException {
    sql("model-with-custom-table", "select * from CUSTOM_TABLE.EMPS").ok();
  }

  @Test public void testPushDownProjectDumb() throws SQLException {
    // rule does not fire, because we're using 'dumb' tables in simple model
    final String sql = "explain plan for select * from EMPS";
    final String expected = "PLAN=EnumerableInterpreter\n"
        + "  BindableTableScan(table=[[SALES, EMPS]])\n";
    sql("model", sql).returns(expected).ok();
  }

  @Test public void testPushDownProject() throws SQLException {
    final String sql = "explain plan for select * from EMPS";
    final String expected = "PLAN=CsvTableScan(table=[[SALES, EMPS]], "
        + "fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n";
    sql("smart", sql).returns(expected).ok();
  }

  @Test public void testPushDownProject2() throws SQLException {
    sql("smart", "explain plan for select name, empno from EMPS")
        .returns("PLAN=CsvTableScan(table=[[SALES, EMPS]], fields=[[1, 0]])\n")
        .ok();
    // make sure that it works...
    sql("smart", "select name, empno from EMPS")
        .returns("NAME=Fred; EMPNO=100",
            "NAME=Eric; EMPNO=110",
            "NAME=John; EMPNO=110",
            "NAME=Wilma; EMPNO=120",
            "NAME=Alice; EMPNO=130")
        .ok();
  }

  @Test public void testPushDownProjectAggregate() throws SQLException {
    final String sql = "explain plan for\n"
        + "select gender, count(*) from EMPS group by gender";
    final String expected = "PLAN="
        + "EnumerableAggregate(group=[{0}], EXPR$1=[COUNT()])\n"
        + "  CsvTableScan(table=[[SALES, EMPS]], fields=[[3]])\n";
    sql("smart", sql).returns(expected).ok();
  }

  @Test public void testPushDownProjectAggregateWithFilter() throws SQLException {
    final String sql = "explain plan for\n"
        + "select max(empno) from EMPS where gender='F'";
    final String expected = "PLAN="
        + "EnumerableAggregate(group=[{}], EXPR$0=[MAX($0)])\n"
        + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=['F':VARCHAR], "
        + "expr#3=[=($t1, $t2)], proj#0..1=[{exprs}], $condition=[$t3])\n"
        + "    CsvTableScan(table=[[SALES, EMPS]], fields=[[0, 3]])\n";
    sql("smart", sql).returns(expected).ok();
  }

  @Test public void testPushDownProjectAggregateNested() throws SQLException {
    final String sql = "explain plan for\n"
        + "select gender, max(qty)\n"
        + "from (\n"
        + "  select name, gender, count(*) qty\n"
        + "  from EMPS\n"
        + "  group by name, gender) t\n"
        + "group by gender";
    final String expected = "PLAN="
        + "EnumerableAggregate(group=[{1}], EXPR$1=[MAX($2)])\n"
        + "  EnumerableAggregate(group=[{0, 1}], QTY=[COUNT()])\n"
        + "    CsvTableScan(table=[[SALES, EMPS]], fields=[[1, 3]])\n";
    sql("smart", sql).returns(expected).ok();
  }

  @Test public void testFilterableSelect() throws SQLException {
    sql("filterable-model", "select name from EMPS").ok();
  }

  @Test public void testFilterableSelectStar() throws SQLException {
    sql("filterable-model", "select * from EMPS").ok();
  }

  /** Filter that can be fully handled by CsvFilterableTable. */
  @Test public void testFilterableWhere() throws SQLException {
    final String sql =
        "select empno, gender, name from EMPS where name = 'John'";
    sql("filterable-model", sql)
        .returns("EMPNO=110; GENDER=M; NAME=John").ok();
  }

  /** Filter that can be partly handled by CsvFilterableTable. */
  @Test public void testFilterableWhere2() throws SQLException {
    final String sql = "select empno, gender, name from EMPS\n"
        + " where gender = 'F' and empno > 125";
    sql("filterable-model", sql)
        .returns("EMPNO=130; GENDER=F; NAME=Alice").ok();
  }

  /** Filter that can be slightly handled by CsvFilterableTable. */
  @Test public void testFilterableWhere3() throws SQLException {
    final String sql = "select empno, gender, name from EMPS\n"
            + " where gender <> 'M' and empno > 125";
    sql("filterable-model", sql)
        .returns("EMPNO=130; GENDER=F; NAME=Alice")
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2272">[CALCITE-2272]
   * Incorrect result for {@code name like '%E%' and city not like '%W%'}</a>.
   */
  @Test public void testFilterableWhereWithNot1() throws SQLException {
    sql("filterable-model",
        "select name, empno from EMPS "
            + "where name like '%E%' and city not like '%W%' ")
        .returns("NAME=Eric; EMPNO=110")
        .ok();
  }

  /** Similar to {@link #testFilterableWhereWithNot1()};
   * But use the same column. */
  @Test public void testFilterableWhereWithNot2() throws SQLException {
    sql("filterable-model",
        "select name, empno from EMPS "
            + "where name like '%i%' and name not like '%W%' ")
        .returns("NAME=Eric; EMPNO=110",
            "NAME=Alice; EMPNO=130")
        .ok();
  }

  @Test public void testJson() throws SQLException {
    final String sql = "select * from archers\n";
    final String[] lines = {
        "id=19990101; dow=Friday; longDate=New Years Day; title=Tractor trouble.; "
            + "characters=[Alice, Bob, Xavier]; script=Julian Hyde; summary=; "
            + "lines=[Bob's tractor got stuck in a field., "
            + "Alice and Xavier hatch a plan to surprise Charlie.]",
        "id=19990103; dow=Sunday; longDate=Sunday 3rd January; "
            + "title=Charlie's surprise.; characters=[Alice, Zebedee, Charlie, Xavier]; "
            + "script=William Shakespeare; summary=; "
            + "lines=[Charlie is very surprised by Alice and Xavier's surprise plan.]",
    };
    sql("bug", sql)
        .returns(lines)
        .ok();
  }

  private Fluent sql(String model, String sql) {
    return new Fluent(model, sql, this::output);
  }

  /** Returns a function that checks the contents of a result set against an
   * expected string. */
  private static Consumer<ResultSet> expect(final String... expected) {
    return resultSet -> {
      try {
        final List<String> lines = new ArrayList<>();
        CsvTest.collect(lines, resultSet);
        Assert.assertEquals(Arrays.asList(expected), lines);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  /** Returns a function that checks the contents of a result set against an
   * expected string. */
  private static Consumer<ResultSet> expectUnordered(String... expected) {
    final List<String> expectedLines =
        Ordering.natural().immutableSortedCopy(Arrays.asList(expected));
    return resultSet -> {
      try {
        final List<String> lines = new ArrayList<>();
        CsvTest.collect(lines, resultSet);
        Collections.sort(lines);
        Assert.assertEquals(expectedLines, lines);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    };
  }

  private void checkSql(String sql, String model, Consumer<ResultSet> fn)
      throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("model", jsonPath(model));
      connection = DriverManager.getConnection("jdbc:calcite:", info);
      statement = connection.createStatement();
      final ResultSet resultSet =
          statement.executeQuery(
              sql);
      fn.accept(resultSet);
    } finally {
      close(connection, statement);
    }
  }

  private String jsonPath(String model) {
    return resourcePath(model + ".json");
  }

  private String resourcePath(String path) {
    return Sources.of(CsvTest.class.getResource("/" + path)).file().getAbsolutePath();
  }

  private static void collect(List<String> result, ResultSet resultSet)
      throws SQLException {
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      buf.setLength(0);
      int n = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= n; i++) {
        buf.append(sep)
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getString(i));
        sep = "; ";
      }
      result.add(Util.toLinux(buf.toString()));
    }
  }

  private void output(ResultSet resultSet, PrintStream out)
      throws SQLException {
    final ResultSetMetaData metaData = resultSet.getMetaData();
    final int columnCount = metaData.getColumnCount();
    while (resultSet.next()) {
      for (int i = 1;; i++) {
        out.print(resultSet.getString(i));
        if (i < columnCount) {
          out.print(", ");
        } else {
          out.println();
          break;
        }
      }
    }
  }

  @Test public void testJoinOnString() throws SQLException {
    final String sql = "select * from emps\n"
        + "join depts on emps.name = depts.name";
    sql("smart", sql).ok();
  }

  @Test public void testWackyColumns() throws SQLException {
    final String sql = "select * from wacky_column_names where false";
    sql("bug", sql).returns().ok();

    final String sql2 = "select \"joined at\", \"naME\"\n"
        + "from wacky_column_names\n"
        + "where \"2gender\" = 'F'";
    sql("bug", sql2)
        .returns("joined at=2005-09-07; naME=Wilma",
            "joined at=2007-01-01; naME=Alice")
        .ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1754">[CALCITE-1754]
   * In Csv adapter, convert DATE and TIME values to int, and TIMESTAMP values
   * to long</a>. */
  @Test public void testGroupByTimestampAdd() throws SQLException {
    final String sql = "select count(*) as c,\n"
        + "  {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT) } as t\n"
        + "from EMPS group by {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT ) } ";
    sql("model", sql)
        .returnsUnordered("C=1; T=1996-08-04",
            "C=1; T=2002-05-04",
            "C=1; T=2005-09-08",
            "C=1; T=2007-01-02",
            "C=1; T=2001-01-02")
        .ok();

    final String sql2 = "select count(*) as c,\n"
        + "  {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT) } as t\n"
        + "from EMPS group by {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT ) } ";
    sql("model", sql2)
        .returnsUnordered("C=1; T=2002-06-03",
            "C=1; T=2005-10-07",
            "C=1; T=2007-02-01",
            "C=1; T=2001-02-01",
            "C=1; T=1996-09-03")
        .ok();
  }

  @Test public void testUnionGroupByWithoutGroupKey() {
    final String sql = "select count(*) as c1 from EMPS group by NAME\n"
        + "union\n"
        + "select count(*) as c1 from EMPS group by NAME";
    sql("model", sql).ok();
  }

  @Test public void testBoolean() {
    sql("smart", "select empno, slacker from emps where slacker")
        .returns("EMPNO=100; SLACKER=true").ok();
  }

  @Test public void testReadme() throws SQLException {
    final String sql = "SELECT d.name, COUNT(*) cnt"
        + " FROM emps AS e"
        + " JOIN depts AS d ON e.deptno = d.deptno"
        + " GROUP BY d.name";
    sql("smart", sql)
        .returns("NAME=Sales; CNT=1", "NAME=Marketing; CNT=2").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-824">[CALCITE-824]
   * Type inference when converting IN clause to semijoin</a>. */
  @Test public void testInToSemiJoinWithCast() throws SQLException {
    // Note that the IN list needs at least 20 values to trigger the rewrite
    // to a semijoin. Try it both ways.
    final String sql = "SELECT e.name\n"
        + "FROM emps AS e\n"
        + "WHERE cast(e.empno as bigint) in ";
    final int threshold = SqlToRelConverter.DEFAULT_IN_SUB_QUERY_THRESHOLD;
    sql("smart", sql + range(130, threshold - 5))
        .returns("NAME=Alice").ok();
    sql("smart", sql + range(130, threshold))
        .returns("NAME=Alice").ok();
    sql("smart", sql + range(130, threshold + 1000))
        .returns("NAME=Alice").ok();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1051">[CALCITE-1051]
   * Underflow exception due to scaling IN clause literals</a>. */
  @Test public void testInToSemiJoinWithoutCast() throws SQLException {
    final String sql = "SELECT e.name\n"
        + "FROM emps AS e\n"
        + "WHERE e.empno in "
        + range(130, SqlToRelConverter.DEFAULT_IN_SUB_QUERY_THRESHOLD);
    sql("smart", sql).returns("NAME=Alice").ok();
  }

  private String range(int first, int count) {
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < count; i++) {
      sb.append(i == 0 ? "(" : ", ").append(first + i);
    }
    return sb.append(')').toString();
  }

  @Test public void testDateType() throws SQLException {
    Properties info = new Properties();
    info.put("model", jsonPath("bug"));

    try (Connection connection =
        DriverManager.getConnection("jdbc:calcite:", info)) {
      ResultSet res = connection.getMetaData().getColumns(null, null,
          "DATE", "JOINEDAT");
      res.next();
      Assert.assertEquals(res.getInt("DATA_TYPE"), java.sql.Types.DATE);

      res = connection.getMetaData().getColumns(null, null,
          "DATE", "JOINTIME");
      res.next();
      Assert.assertEquals(res.getInt("DATA_TYPE"), java.sql.Types.TIME);

      res = connection.getMetaData().getColumns(null, null,
          "DATE", "JOINTIMES");
      res.next();
      Assert.assertEquals(res.getInt("DATA_TYPE"), java.sql.Types.TIMESTAMP);

      Statement statement = connection.createStatement();
      ResultSet resultSet = statement.executeQuery(
          "select \"JOINEDAT\", \"JOINTIME\", \"JOINTIMES\" from \"DATE\" where EMPNO = 100");
      resultSet.next();

      // date
      Assert.assertEquals(java.sql.Date.class, resultSet.getDate(1).getClass());
      Assert.assertEquals(java.sql.Date.valueOf("1996-08-03"),
          resultSet.getDate(1));

      // time
      Assert.assertEquals(java.sql.Time.class, resultSet.getTime(2).getClass());
      Assert.assertEquals(java.sql.Time.valueOf("00:01:02"),
          resultSet.getTime(2));

      // timestamp
      Assert.assertEquals(java.sql.Timestamp.class,
          resultSet.getTimestamp(3).getClass());
      Assert.assertEquals(java.sql.Timestamp.valueOf("1996-08-03 00:01:02"),
          resultSet.getTimestamp(3));

    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1072">[CALCITE-1072]
   * CSV adapter incorrectly parses TIMESTAMP values after noon</a>. */
  @Test public void testDateType2() throws SQLException {
    Properties info = new Properties();
    info.put("model", jsonPath("bug"));

    try (Connection connection =
        DriverManager.getConnection("jdbc:calcite:", info)) {
      Statement statement = connection.createStatement();
      final String sql = "select * from \"DATE\"\n"
          + "where EMPNO >= 140 and EMPNO < 200";
      ResultSet resultSet = statement.executeQuery(sql);
      int n = 0;
      while (resultSet.next()) {
        ++n;
        final int empId = resultSet.getInt(1);
        final String date = resultSet.getString(2);
        final String time = resultSet.getString(3);
        final String timestamp = resultSet.getString(4);
        assertThat(date, is("2015-12-31"));
        switch (empId) {
        case 140:
          assertThat(time, is("07:15:56"));
          assertThat(timestamp, is("2015-12-31 07:15:56"));
          break;
        case 150:
          assertThat(time, is("13:31:21"));
          assertThat(timestamp, is("2015-12-31 13:31:21"));
          break;
        default:
          throw new AssertionError();
        }
      }
      assertThat(n, is(2));
      resultSet.close();
      statement.close();
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1673">[CALCITE-1673]
   * Query with ORDER BY or GROUP BY on TIMESTAMP column throws
   * CompileException</a>. */
  @Test public void testTimestampGroupBy() throws SQLException {
    Properties info = new Properties();
    info.put("model", jsonPath("bug"));
    // Use LIMIT to ensure that results are deterministic without ORDER BY
    final String sql = "select \"EMPNO\", \"JOINTIMES\"\n"
        + "from (select * from \"DATE\" limit 1)\n"
        + "group by \"EMPNO\",\"JOINTIMES\"";
    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(resultSet.next(), is(true));
      final Timestamp timestamp = resultSet.getTimestamp(2);
      Assert.assertThat(timestamp, isA(java.sql.Timestamp.class));
      // Note: This logic is time zone specific, but the same time zone is
      // used in the CSV adapter and this test, so they should cancel out.
      Assert.assertThat(timestamp,
          is(java.sql.Timestamp.valueOf("1996-08-03 00:01:02.0")));
    }
  }

  /** As {@link #testTimestampGroupBy()} but with ORDER BY. */
  @Test public void testTimestampOrderBy() throws SQLException {
    Properties info = new Properties();
    info.put("model", jsonPath("bug"));
    final String sql = "select \"EMPNO\",\"JOINTIMES\" from \"DATE\"\n"
        + "order by \"JOINTIMES\"";
    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(resultSet.next(), is(true));
      final Timestamp timestamp = resultSet.getTimestamp(2);
      Assert.assertThat(timestamp,
          is(java.sql.Timestamp.valueOf("1996-08-03 00:01:02")));
    }
  }

  /** As {@link #testTimestampGroupBy()} but with ORDER BY as well as GROUP
   * BY. */
  @Test public void testTimestampGroupByAndOrderBy() throws SQLException {
    Properties info = new Properties();
    info.put("model", jsonPath("bug"));
    final String sql = "select \"EMPNO\", \"JOINTIMES\" from \"DATE\"\n"
        + "group by \"EMPNO\",\"JOINTIMES\" order by \"JOINTIMES\"";
    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info);
         Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(resultSet.next(), is(true));
      final Timestamp timestamp = resultSet.getTimestamp(2);
      Assert.assertThat(timestamp,
          is(java.sql.Timestamp.valueOf("1996-08-03 00:01:02")));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1031">[CALCITE-1031]
   * In prepared statement, CsvScannableTable.scan is called twice</a>. To see
   * the bug, place a breakpoint in CsvScannableTable.scan, and note that it is
   * called twice. It should only be called once. */
  @Test public void testPrepared() throws SQLException {
    final Properties properties = new Properties();
    properties.setProperty("caseSensitive", "true");
    try (Connection connection =
        DriverManager.getConnection("jdbc:calcite:", properties)) {
      final CalciteConnection calciteConnection = connection.unwrap(
          CalciteConnection.class);

      final Schema schema =
          CsvSchemaFactory.INSTANCE
              .create(calciteConnection.getRootSchema(), null,
                  ImmutableMap.of("directory",
                      resourcePath("sales"), "flavor", "scannable"));
      calciteConnection.getRootSchema().add("TEST", schema);
      final String sql = "select * from \"TEST\".\"DEPTS\" where \"NAME\" = ?";
      final PreparedStatement statement2 =
          calciteConnection.prepareStatement(sql);

      statement2.setString(1, "Sales");
      final ResultSet resultSet1 = statement2.executeQuery();
      Consumer<ResultSet> expect = expect("DEPTNO=10; NAME=Sales");
      expect.accept(resultSet1);
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1054">[CALCITE-1054]
   * NPE caused by wrong code generation for Timestamp fields</a>. */
  @Test public void testFilterOnNullableTimestamp() throws Exception {
    Properties info = new Properties();
    info.put("model", jsonPath("bug"));

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      final Statement statement = connection.createStatement();

      // date
      final String sql1 = "select JOINEDAT from \"DATE\"\n"
          + "where JOINEDAT < {d '2000-01-01'}\n"
          + "or JOINEDAT >= {d '2017-01-01'}";
      final ResultSet joinedAt = statement.executeQuery(sql1);
      assertThat(joinedAt.next(), is(true));
      assertThat(joinedAt.getDate(1), is(java.sql.Date.valueOf("1996-08-03")));

      // time
      final String sql2 = "select JOINTIME from \"DATE\"\n"
          + "where JOINTIME >= {t '07:00:00'}\n"
          + "and JOINTIME < {t '08:00:00'}";
      final ResultSet joinTime = statement.executeQuery(sql2);
      assertThat(joinTime.next(), is(true));
      assertThat(joinTime.getTime(1), is(java.sql.Time.valueOf("07:15:56")));

      // timestamp
      final String sql3 = "select JOINTIMES,\n"
          + "  {fn timestampadd(SQL_TSI_DAY, 1, JOINTIMES)}\n"
          + "from \"DATE\"\n"
          + "where (JOINTIMES >= {ts '2003-01-01 00:00:00'}\n"
          + "and JOINTIMES < {ts '2006-01-01 00:00:00'})\n"
          + "or (JOINTIMES >= {ts '2003-01-01 00:00:00'}\n"
          + "and JOINTIMES < {ts '2007-01-01 00:00:00'})";
      final ResultSet joinTimes = statement.executeQuery(sql3);
      assertThat(joinTimes.next(), is(true));
      assertThat(joinTimes.getTimestamp(1),
          is(java.sql.Timestamp.valueOf("2005-09-07 00:00:00")));
      assertThat(joinTimes.getTimestamp(2),
          is(java.sql.Timestamp.valueOf("2005-09-08 00:00:00")));

      final String sql4 = "select JOINTIMES, extract(year from JOINTIMES)\n"
          + "from \"DATE\"";
      final ResultSet joinTimes2 = statement.executeQuery(sql4);
      assertThat(joinTimes2.next(), is(true));
      assertThat(joinTimes2.getTimestamp(1),
          is(java.sql.Timestamp.valueOf("1996-08-03 00:01:02")));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1118">[CALCITE-1118]
   * NullPointerException in EXTRACT with WHERE ... IN clause if field has null
   * value</a>. */
  @Test public void testFilterOnNullableTimestamp2() throws Exception {
    Properties info = new Properties();
    info.put("model", jsonPath("bug"));

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      final Statement statement = connection.createStatement();
      final String sql1 = "select extract(year from JOINTIMES)\n"
          + "from \"DATE\"\n"
          + "where extract(year from JOINTIMES) in (2006, 2007)";
      final ResultSet joinTimes = statement.executeQuery(sql1);
      assertThat(joinTimes.next(), is(true));
      assertThat(joinTimes.getInt(1), is(2007));

      final String sql2 = "select extract(year from JOINTIMES),\n"
          + "  count(0) from \"DATE\"\n"
          + "where extract(year from JOINTIMES) between 2007 and 2016\n"
          + "group by extract(year from JOINTIMES)";
      final ResultSet joinTimes2 = statement.executeQuery(sql2);
      assertThat(joinTimes2.next(), is(true));
      assertThat(joinTimes2.getInt(1), is(2007));
      assertThat(joinTimes2.getLong(2), is(1L));
      assertThat(joinTimes2.next(), is(true));
      assertThat(joinTimes2.getInt(1), is(2015));
      assertThat(joinTimes2.getLong(2), is(2L));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1427">[CALCITE-1427]
   * Code generation incorrect (does not compile) for DATE, TIME and TIMESTAMP
   * fields</a>. */
  @Test public void testNonNullFilterOnDateType() throws SQLException {
    Properties info = new Properties();
    info.put("model", jsonPath("bug"));

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      final Statement statement = connection.createStatement();

      // date
      final String sql1 = "select JOINEDAT from \"DATE\"\n"
          + "where JOINEDAT is not null";
      final ResultSet joinedAt = statement.executeQuery(sql1);
      assertThat(joinedAt.next(), is(true));
      assertThat(joinedAt.getDate(1).getClass(), equalTo(java.sql.Date.class));
      assertThat(joinedAt.getDate(1), is(java.sql.Date.valueOf("1996-08-03")));

      // time
      final String sql2 = "select JOINTIME from \"DATE\"\n"
          + "where JOINTIME is not null";
      final ResultSet joinTime = statement.executeQuery(sql2);
      assertThat(joinTime.next(), is(true));
      assertThat(joinTime.getTime(1).getClass(), equalTo(java.sql.Time.class));
      assertThat(joinTime.getTime(1), is(java.sql.Time.valueOf("00:01:02")));

      // timestamp
      final String sql3 = "select JOINTIMES from \"DATE\"\n"
          + "where JOINTIMES is not null";
      final ResultSet joinTimes = statement.executeQuery(sql3);
      assertThat(joinTimes.next(), is(true));
      assertThat(joinTimes.getTimestamp(1).getClass(),
          equalTo(java.sql.Timestamp.class));
      assertThat(joinTimes.getTimestamp(1),
          is(java.sql.Timestamp.valueOf("1996-08-03 00:01:02")));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1427">[CALCITE-1427]
   * Code generation incorrect (does not compile) for DATE, TIME and TIMESTAMP
   * fields</a>. */
  @Test public void testGreaterThanFilterOnDateType() throws SQLException {
    Properties info = new Properties();
    info.put("model", jsonPath("bug"));

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:", info)) {
      final Statement statement = connection.createStatement();

      // date
      final String sql1 = "select JOINEDAT from \"DATE\"\n"
          + "where JOINEDAT > {d '1990-01-01'}";
      final ResultSet joinedAt = statement.executeQuery(sql1);
      assertThat(joinedAt.next(), is(true));
      assertThat(joinedAt.getDate(1).getClass(), equalTo(java.sql.Date.class));
      assertThat(joinedAt.getDate(1), is(java.sql.Date.valueOf("1996-08-03")));

      // time
      final String sql2 = "select JOINTIME from \"DATE\"\n"
          + "where JOINTIME > {t '00:00:00'}";
      final ResultSet joinTime = statement.executeQuery(sql2);
      assertThat(joinTime.next(), is(true));
      assertThat(joinTime.getTime(1).getClass(), equalTo(java.sql.Time.class));
      assertThat(joinTime.getTime(1), is(java.sql.Time.valueOf("00:01:02")));

      // timestamp
      final String sql3 = "select JOINTIMES from \"DATE\"\n"
          + "where JOINTIMES > {ts '1990-01-01 00:00:00'}";
      final ResultSet joinTimes = statement.executeQuery(sql3);
      assertThat(joinTimes.next(), is(true));
      assertThat(joinTimes.getTimestamp(1).getClass(),
          equalTo(java.sql.Timestamp.class));
      assertThat(joinTimes.getTimestamp(1),
          is(java.sql.Timestamp.valueOf("1996-08-03 00:01:02")));
    }
  }

  @Ignore("CALCITE-1894: there's a bug in the test code, so it does not test what it should")
  @Test(timeout = 10000) public void testCsvStream() throws Exception {
    final File file = File.createTempFile("stream", "csv");
    final String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'STREAM',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'SS',\n"
        + "      tables: [\n"
        + "        {\n"
        + "          name: 'DEPTS',\n"
        + "          type: 'custom',\n"
        + "          factory: '" + CsvStreamTableFactory.class.getName()
        + "',\n"
        + "          stream: {\n"
        + "            stream: true\n"
        + "          },\n"
        + "          operand: {\n"
        + "            file: " + escapeString(file.getAbsolutePath()) + ",\n"
        + "            flavor: \"scannable\"\n"
        + "          }\n"
        + "        }\n"
        + "      ]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";
    final String[] strings = {
        "DEPTNO:int,NAME:string",
        "10,\"Sales\"",
        "20,\"Marketing\"",
        "30,\"Engineering\""
    };

    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:model=inline:" + model);
         PrintWriter pw = Util.printWriter(file);
         Worker<Void> worker = new Worker<>()) {
      final Thread thread = new Thread(worker);
      thread.start();

      // Add some rows so that the table can deduce its row type.
      final Iterator<String> lines = Arrays.asList(strings).iterator();
      pw.println(lines.next()); // header
      pw.flush();
      worker.queue.put(writeLine(pw, lines.next())); // first row
      worker.queue.put(writeLine(pw, lines.next())); // second row
      final CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      final String sql = "select stream * from \"SS\".\"DEPTS\"";
      final PreparedStatement statement =
          calciteConnection.prepareStatement(sql);
      final ResultSet resultSet = statement.executeQuery();
      int count = 0;
      try {
        while (resultSet.next()) {
          ++count;
          if (lines.hasNext()) {
            worker.queue.put(sleep(10));
            worker.queue.put(writeLine(pw, lines.next()));
          } else {
            worker.queue.put(cancel(statement));
          }
        }
        fail("expected exception, got end of data");
      } catch (SQLException e) {
        assertThat(e.getMessage(), is("Statement canceled"));
      }
      assertThat(count, anyOf(is(strings.length - 2), is(strings.length - 1)));
      assertThat(worker.e, nullValue());
      assertThat(worker.v, nullValue());
    } finally {
      Util.discard(file.delete());
    }
  }

  /** Creates a command that appends a line to the CSV file. */
  private Callable<Void> writeLine(final PrintWriter pw, final String line) {
    return () -> {
      pw.println(line);
      pw.flush();
      return null;
    };
  }

  /** Creates a command that sleeps. */
  private Callable<Void> sleep(final long millis) {
    return () -> {
      Thread.sleep(millis);
      return null;
    };
  }

  /** Creates a command that cancels a statement. */
  private Callable<Void> cancel(final Statement statement) {
    return () -> {
      statement.cancel();
      return null;
    };
  }

  private Void output(ResultSet resultSet) {
    try {
      output(resultSet, System.out);
    } catch (SQLException e) {
      throw TestUtil.rethrow(e);
    }
    return null;
  }

  /** Receives commands on a queue and executes them on its own thread.
   * Call {@link #close} to terminate.
   *
   * @param <E> Result value of commands
   */
  private static class Worker<E> implements Runnable, AutoCloseable {
    /** Queue of commands. */
    final BlockingQueue<Callable<E>> queue =
        new ArrayBlockingQueue<>(5);

    /** Value returned by the most recent command. */
    private E v;

    /** Exception thrown by a command or queue wait. */
    private Exception e;

    /** The poison pill command. */
    final Callable<E> end = () -> null;

    public void run() {
      try {
        for (;;) {
          final Callable<E> c = queue.take();
          if (c == end) {
            return;
          }
          this.v = c.call();
        }
      } catch (Exception e) {
        this.e = e;
      }
    }

    public void close() {
      try {
        queue.put(end);
      } catch (InterruptedException e) {
        // ignore
      }
    }
  }

  /** Fluent API to perform test actions. */
  private class Fluent {
    private final String model;
    private final String sql;
    private final Consumer<ResultSet> expect;

    Fluent(String model, String sql, Consumer<ResultSet> expect) {
      this.model = model;
      this.sql = sql;
      this.expect = expect;
    }

    /** Runs the test. */
    Fluent ok() {
      try {
        checkSql(sql, model, expect);
        return this;
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
    }

    /** Assigns a function to call to test whether output is correct. */
    Fluent checking(Consumer<ResultSet> expect) {
      return new Fluent(model, sql, expect);
    }

    /** Sets the rows that are expected to be returned from the SQL query. */
    Fluent returns(String... expectedLines) {
      return checking(expect(expectedLines));
    }

    /** Sets the rows that are expected to be returned from the SQL query,
     * in no particular order. */
    Fluent returnsUnordered(String... expectedLines) {
      return checking(expectUnordered(expectedLines));
    }
  }
}

// End CsvTest.java
