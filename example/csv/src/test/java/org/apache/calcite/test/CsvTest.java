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

import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import com.google.common.base.Throwables;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.PrintStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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

  public static String toLinux(String s) {
    return s.replaceAll("\r\n", "\n");
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
    checkSql("model", "select * from EMPS");
  }

  @Test public void testSelectSingleProjectGz() throws SQLException {
    checkSql("smart", "select name from EMPS");
  }

  @Test public void testSelectSingleProject() throws SQLException {
    checkSql("smart", "select name from DEPTS");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-898">[CALCITE-898]
   * Type inference multiplying Java long by SQL INTEGER</a>. */
  @Test public void testSelectLongMultiplyInteger() throws SQLException {
    final String sql = "select empno * 3 as e3\n"
        + "from long_emps where empno = 100";

    checkSql(sql, "bug", new Function1<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          assertThat(resultSet.next(), is(true));
          Long o = (Long) resultSet.getObject(1);
          assertThat(o, is(300L));
          assertThat(resultSet.next(), is(false));
          return null;
        } catch (SQLException e) {
          throw Throwables.propagate(e);
        }
      }
    });
  }

  @Test public void testCustomTable() throws SQLException {
    checkSql("model-with-custom-table", "select * from CUSTOM_TABLE.EMPS");
  }

  @Test public void testPushDownProjectDumb() throws SQLException {
    // rule does not fire, because we're using 'dumb' tables in simple model
    checkSql("model", "explain plan for select * from EMPS",
        "PLAN=EnumerableInterpreter\n"
            + "  BindableTableScan(table=[[SALES, EMPS]])\n");
  }

  @Test public void testPushDownProject() throws SQLException {
    checkSql("smart", "explain plan for select * from EMPS",
        "PLAN=CsvTableScan(table=[[SALES, EMPS]], fields=[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]])\n");
  }

  @Test public void testPushDownProject2() throws SQLException {
    checkSql("smart", "explain plan for select name, empno from EMPS",
        "PLAN=CsvTableScan(table=[[SALES, EMPS]], fields=[[1, 0]])\n");
    // make sure that it works...
    checkSql("smart", "select name, empno from EMPS",
        "NAME=Fred; EMPNO=100",
        "NAME=Eric; EMPNO=110",
        "NAME=John; EMPNO=110",
        "NAME=Wilma; EMPNO=120",
        "NAME=Alice; EMPNO=130");
  }

  @Test public void testFilterableSelect() throws SQLException {
    checkSql("filterable-model", "select name from EMPS");
  }

  @Test public void testFilterableSelectStar() throws SQLException {
    checkSql("filterable-model", "select * from EMPS");
  }

  /** Filter that can be fully handled by CsvFilterableTable. */
  @Test public void testFilterableWhere() throws SQLException {
    checkSql("filterable-model",
        "select empno, gender, name from EMPS where name = 'John'",
        "EMPNO=110; GENDER=M; NAME=John");
  }

  /** Filter that can be partly handled by CsvFilterableTable. */
  @Test public void testFilterableWhere2() throws SQLException {
    checkSql("filterable-model",
        "select empno, gender, name from EMPS where gender = 'F' and empno > 125",
        "EMPNO=130; GENDER=F; NAME=Alice");
  }

  @Test public void testJson() throws SQLException {
    checkSql("bug", "select _MAP['id'] as id,\n"
            + " _MAP['title'] as title,\n"
            + " CHAR_LENGTH(CAST(_MAP['title'] AS VARCHAR(30))) as len\n"
            + " from \"archers\"",
        "ID=19990101; TITLE=Tractor trouble.; LEN=16",
        "ID=19990103; TITLE=Charlie's surprise.; LEN=19");
  }

  private void checkSql(String model, String sql) throws SQLException {
    checkSql(sql, model, output());
  }

  private Function1<ResultSet, Void> output() {
    return new Function1<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          output(resultSet, System.out);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    };
  }

  private void checkSql(String model, String sql, final String... expected)
      throws SQLException {
    checkSql(sql, model, expect(expected));
  }

  /** Returns a function that checks the contents of a result set against an
   * expected string. */
  private static Function1<ResultSet, Void> expect(final String... expected) {
    return new Function1<ResultSet, Void>() {
      public Void apply(ResultSet resultSet) {
        try {
          final List<String> lines = new ArrayList<>();
          CsvTest.collect(lines, resultSet);
          Assert.assertEquals(Arrays.asList(expected), lines);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return null;
      }
    };
  }

  private void checkSql(String sql, String model, Function1<ResultSet, Void> fn)
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
      fn.apply(resultSet);
    } finally {
      close(connection, statement);
    }
  }

  private String jsonPath(String model) {
    final URL url = CsvTest.class.getResource("/" + model + ".json");
    String s = url.toString();
    if (s.startsWith("file:")) {
      s = s.substring("file:".length());
    }
    return s;
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
      result.add(toLinux(buf.toString()));
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
    checkSql("smart",
        "select * from emps join depts on emps.name = depts.name");
  }

  @Test public void testWackyColumns() throws SQLException {
    checkSql("select * from wacky_column_names where false", "bug",
        expect());
    checkSql(
        "select \"joined at\", \"naME\" from wacky_column_names where \"2gender\" = 'F'",
        "bug",
        expect(
            "joined at=2005-09-07; naME=Wilma",
            "joined at=2007-01-01; naME=Alice"));
  }

  @Test public void testBoolean() throws SQLException {
    checkSql("smart",
        "select empno, slacker from emps where slacker",
        "EMPNO=100; SLACKER=true");
  }

  @Test public void testReadme() throws SQLException {
    checkSql("SELECT d.name, COUNT(*) cnt"
            + " FROM emps AS e"
            + " JOIN depts AS d ON e.deptno = d.deptno"
            + " GROUP BY d.name",
        "smart",
        expect("NAME=Sales; CNT=1", "NAME=Marketing; CNT=2"));
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
    checkSql(sql + range(130, SqlToRelConverter.IN_SUBQUERY_THRESHOLD - 5),
        "smart", expect("NAME=Alice"));
    checkSql(sql + range(130, SqlToRelConverter.IN_SUBQUERY_THRESHOLD),
        "smart", expect("NAME=Alice"));
    checkSql(sql + range(130, SqlToRelConverter.IN_SUBQUERY_THRESHOLD + 1000),
        "smart", expect("NAME=Alice"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1051">[CALCITE-1051]
   * Underflow exception due to scaling IN clause literals</a>. */
  @Test public void testInToSemiJoinWithoutCast() throws SQLException {
    final String sql = "SELECT e.name\n"
        + "FROM emps AS e\n"
        + "WHERE e.empno in "
        + range(130, SqlToRelConverter.IN_SUBQUERY_THRESHOLD);
    checkSql(sql, "smart", expect("NAME=Alice"));
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

    try (Connection connection
        = DriverManager.getConnection("jdbc:calcite:", info)) {
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
}

// End CsvTest.java
