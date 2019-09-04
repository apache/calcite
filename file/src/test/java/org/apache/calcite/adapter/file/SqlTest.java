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
package org.apache.calcite.adapter.file;

import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.Ordering;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * System test of the Calcite file adapter, which can also read and parse
 * HTML tables over HTTP.
 */
public class SqlTest {
  // helper functions

  private Fluent sql(String model, String sql) {
    return new Fluent(model, sql, input -> {
      throw new AssertionError();
    });
  }

  private static Function<ResultSet, Void> expect(String... expectedLines) {
    final StringBuilder b = new StringBuilder();
    for (String s : expectedLines) {
      b.append(s).append('\n');
    }
    final String expected = b.toString();
    return resultSet -> {
      try {
        String actual = toString(resultSet);
        if (!expected.equals(actual)) {
          System.out.println("Assertion failure:");
          System.out.println("\tExpected: '" + expected + "'");
          System.out.println("\tActual: '" + actual + "'");
        }
        assertEquals(expected, actual);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
      return null;
    };
  }

  /** Returns a function that checks the contents of a result set against an
   * expected string. */
  private static Function<ResultSet, Void> expectUnordered(String... expected) {
    final List<String> expectedLines =
        Ordering.natural().immutableSortedCopy(Arrays.asList(expected));
    return resultSet -> {
      try {
        final List<String> lines = new ArrayList<>();
        SqlTest.collect(lines, resultSet);
        Collections.sort(lines);
        Assert.assertEquals(expectedLines, lines);
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
      return null;
    };
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

  private void checkSql(String sql, String model, Function<ResultSet, Void> fn)
      throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("model",
          FileReaderTest.file("target/test-classes/" + model + ".json"));
      connection = DriverManager.getConnection("jdbc:calcite:", info);
      statement = connection.createStatement();
      final ResultSet resultSet = statement.executeQuery(sql);
      fn.apply(resultSet);
    } finally {
      close(connection, statement);
    }
  }

  private static String toString(ResultSet resultSet) throws SQLException {
    StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      int n = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= n; i++) {
        buf.append(sep)
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getObject(i));
        sep = "; ";
      }
      buf.append("\n");
    }
    return buf.toString();
  }

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

  // tests

  /** Reads from a local file and checks the result. */
  @Test public void testFileSelect() throws SQLException {
    final String sql = "select H1 from T1 where H0 = 'R1C0'";
    sql("testModel", sql).returns("H1=R1C1").ok();
  }

  /** Reads from a local file without table headers &lt;TH&gt; and checks the
   * result. */
  @Test public void testNoThSelect() throws SQLException {
    Assume.assumeTrue(FileSuite.hazNetwork());
    final String sql = "select \"col1\" from T1_NO_TH where \"col0\" like 'R0%'";
    sql("testModel", sql).returns("col1=R0C1").ok();
  }

  /** Reads from a local file - finds larger table even without &lt;TH&gt;
   * elements. */
  @Test public void testFindBiggerNoTh() throws SQLException {
    final String sql = "select \"col4\" from TABLEX2 where \"col0\" like 'R1%'";
    sql("testModel", sql).returns("col4=R1C4").ok();
  }

  /** Reads from a URL and checks the result. */
  @Ignore("[CALCITE-1789] Wikipedia format change breaks file adapter test")
  @Test public void testUrlSelect() throws SQLException {
    Assume.assumeTrue(FileSuite.hazNetwork());
    final String sql = "select \"State\", \"Statehood\" from \"States_as_of\"\n"
        + "where \"State\" = 'California'";
    sql("wiki", sql).returns("State=California; Statehood=1850-09-09").ok();
  }

  /** Reads the EMPS table. */
  @Test public void testSalesEmps() throws SQLException {
    final String sql = "select * from sales.emps";
    sql("sales", sql)
        .returns("EMPNO=100; NAME=Fred; DEPTNO=30",
            "EMPNO=110; NAME=Eric; DEPTNO=20",
            "EMPNO=110; NAME=John; DEPTNO=40",
            "EMPNO=120; NAME=Wilma; DEPTNO=20",
            "EMPNO=130; NAME=Alice; DEPTNO=40")
        .ok();
  }

  /** Reads the DEPTS table. */
  @Test public void testSalesDepts() throws SQLException {
    final String sql = "select * from sales.depts";
    sql("sales", sql)
        .returns("DEPTNO=10; NAME=Sales",
            "DEPTNO=20; NAME=Marketing",
            "DEPTNO=30; NAME=Accounts")
        .ok();
  }

  /** Reads the DEPTS table from the CSV schema. */
  @Test public void testCsvSalesDepts() throws SQLException {
    final String sql = "select * from sales.depts";
    sql("sales-csv", sql)
        .returns("DEPTNO=10; NAME=Sales",
            "DEPTNO=20; NAME=Marketing",
            "DEPTNO=30; NAME=Accounts")
        .ok();
  }

  /** Reads the EMPS table from the CSV schema. */
  @Test public void testCsvSalesEmps() throws SQLException {
    final String sql = "select * from sales.emps";
    final String[] lines = {
        "EMPNO=100; NAME=Fred; DEPTNO=10; GENDER=; CITY=; EMPID=30; AGE=25; SLACKER=true; MANAGER=false; JOINEDAT=1996-08-03",
        "EMPNO=110; NAME=Eric; DEPTNO=20; GENDER=M; CITY=San Francisco; EMPID=3; AGE=80; SLACKER=null; MANAGER=false; JOINEDAT=2001-01-01",
        "EMPNO=110; NAME=John; DEPTNO=40; GENDER=M; CITY=Vancouver; EMPID=2; AGE=null; SLACKER=false; MANAGER=true; JOINEDAT=2002-05-03",
        "EMPNO=120; NAME=Wilma; DEPTNO=20; GENDER=F; CITY=; EMPID=1; AGE=5; SLACKER=null; MANAGER=true; JOINEDAT=2005-09-07",
        "EMPNO=130; NAME=Alice; DEPTNO=40; GENDER=F; CITY=Vancouver; EMPID=2; AGE=null; SLACKER=false; MANAGER=true; JOINEDAT=2007-01-01",
    };
    sql("sales-csv", sql).returns(lines).ok();
  }

  /** Reads the HEADER_ONLY table from the CSV schema. The CSV file has one
   * line - the column headers - but no rows of data. */
  @Test public void testCsvSalesHeaderOnly() throws SQLException {
    final String sql = "select * from sales.header_only";
    sql("sales-csv", sql).returns().ok();
  }

  /** Reads the EMPTY table from the CSV schema. The CSV file has no lines,
   * therefore the table has a system-generated column called
   * "EmptyFileHasNoColumns". */
  @Test public void testCsvSalesEmpty() throws SQLException {
    final String sql = "select * from sales.\"EMPTY\"";
    checkSql(sql, "sales-csv", resultSet -> {
      try {
        assertThat(resultSet.getMetaData().getColumnCount(), is(1));
        assertThat(resultSet.getMetaData().getColumnName(1),
            is("EmptyFileHasNoColumns"));
        assertThat(resultSet.getMetaData().getColumnType(1),
            is(Types.BOOLEAN));
        String actual = toString(resultSet);
        assertThat(actual, is(""));
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
      return null;
    });
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1754">[CALCITE-1754]
   * In Csv adapter, convert DATE and TIME values to int, and TIMESTAMP values
   * to long</a>. */
  @Test public void testCsvGroupByTimestampAdd() throws SQLException {
    final String sql = "select count(*) as c,\n"
        + "  {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT) } as t\n"
        + "from EMPS group by {fn timestampadd(SQL_TSI_DAY, 1, JOINEDAT ) } ";
    sql("sales-csv", sql)
        .returnsUnordered("C=1; T=1996-08-04",
            "C=1; T=2002-05-04",
            "C=1; T=2005-09-08",
            "C=1; T=2007-01-02",
            "C=1; T=2001-01-02")
        .ok();
    final String sql2 = "select count(*) as c,\n"
        + "  {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT) } as t\n"
        + "from EMPS group by {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT ) } ";
    sql("sales-csv", sql2)
        .returnsUnordered("C=1; T=2002-06-03",
            "C=1; T=2005-10-07",
            "C=1; T=2007-02-01",
            "C=1; T=2001-02-01",
            "C=1; T=1996-09-03").ok();
    final String sql3 = "select\n"
        + " distinct {fn timestampadd(SQL_TSI_MONTH, 1, JOINEDAT) } as t\n"
        + "from EMPS";
    sql("sales-csv", sql3)
        .returnsUnordered("T=2002-06-03",
            "T=2005-10-07",
            "T=2007-02-01",
            "T=2001-02-01",
            "T=1996-09-03").ok();
  }

  /** Fluent API to perform test actions. */
  private class Fluent {
    private final String model;
    private final String sql;
    private final Function<ResultSet, Void> expect;

    Fluent(String model, String sql, Function<ResultSet, Void> expect) {
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
    Fluent checking(Function<ResultSet, Void> expect) {
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

  /** Reads the DEPTS table from the JSON schema. */
  @Test public void testJsonSalesDepts() throws SQLException {
    final String sql = "select * from sales.depts";
    sql("sales-json", sql)
        .returns("DEPTNO=10; NAME=Sales",
            "DEPTNO=20; NAME=Marketing",
            "DEPTNO=30; NAME=Accounts")
        .ok();
  }

  /** Reads the EMPS table from the JSON schema. */
  @Test public void testJsonSalesEmps() throws SQLException {
    final String sql = "select * from sales.emps";
    final String[] lines = {
        "EMPNO=100; NAME=Fred; DEPTNO=10; GENDER=; CITY=; EMPID=30; AGE=25; SLACKER=true; MANAGER=false; JOINEDAT=1996-08-03",
        "EMPNO=110; NAME=Eric; DEPTNO=20; GENDER=M; CITY=San Francisco; EMPID=3; AGE=80; SLACKER=null; MANAGER=false; JOINEDAT=2001-01-01",
        "EMPNO=110; NAME=John; DEPTNO=40; GENDER=M; CITY=Vancouver; EMPID=2; AGE=null; SLACKER=false; MANAGER=true; JOINEDAT=2002-05-03",
        "EMPNO=120; NAME=Wilma; DEPTNO=20; GENDER=F; CITY=; EMPID=1; AGE=5; SLACKER=null; MANAGER=true; JOINEDAT=2005-09-07",
        "EMPNO=130; NAME=Alice; DEPTNO=40; GENDER=F; CITY=Vancouver; EMPID=2; AGE=null; SLACKER=false; MANAGER=true; JOINEDAT=2007-01-01",
    };
    sql("sales-json", sql).returns(lines).ok();
  }

  /** Reads the EMPTY table from the JSON schema. The JSON file has no lines,
   * therefore the table has a system-generated column called
   * "EmptyFileHasNoColumns". */
  @Test public void testJsonSalesEmpty() throws SQLException {
    final String sql = "select * from sales.\"EMPTY\"";
    checkSql(sql, "sales-json", resultSet -> {
      try {
        assertThat(resultSet.getMetaData().getColumnCount(), is(1));
        assertThat(resultSet.getMetaData().getColumnName(1),
            is("EmptyFileHasNoColumns"));
        assertThat(resultSet.getMetaData().getColumnType(1),
            is(Types.BOOLEAN));
        String actual = toString(resultSet);
        assertThat(actual, is(""));
      } catch (SQLException e) {
        throw TestUtil.rethrow(e);
      }
      return null;
    });
  }

  /** Test returns the result of two json file joins. */
  @Test public void testJsonJoinOnString() {
    final String sql = "select emps.EMPNO, emps.NAME, depts.deptno from emps\n"
        + "join depts on emps.deptno = depts.deptno";
    final String[] lines = {
        "EMPNO=100; NAME=Fred; DEPTNO=10",
        "EMPNO=110; NAME=Eric; DEPTNO=20",
        "EMPNO=120; NAME=Wilma; DEPTNO=20",
    };
    sql("sales-json", sql).returns(lines).ok();
  }

  /** The folder contains both JSON files and CSV files joins. */
  @Test public void testJsonWithCsvJoin() {
    final String sql = "select emps.empno,\n"
        + " NAME,\n"
        + " \"DATE\".JOINEDAT\n"
        + " from \"DATE\"\n"
        + "join emps on emps.empno = \"DATE\".EMPNO limit 3";
    final String[] lines = {
        "EMPNO=100; NAME=Fred; JOINEDAT=1996-08-03",
        "EMPNO=110; NAME=Eric; JOINEDAT=2001-01-01",
        "EMPNO=110; NAME=Eric; JOINEDAT=2002-05-03",
    };
    sql("sales-json", sql)
        .returns(lines)
        .ok();
  }
}

// End SqlTest.java
