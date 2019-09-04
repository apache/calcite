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

import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

/**
 * Unit tests for server and DDL.
 */
public class ServerTest {

  static final String URL = "jdbc:calcite:";

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  static Connection connect() throws SQLException {
    return DriverManager.getConnection(URL,
        CalciteAssert.propBuilder()
            .set(CalciteConnectionProperty.PARSER_FACTORY,
                SqlDdlParserImpl.class.getName() + "#FACTORY")
            .set(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED,
                "true")
            .build());
  }

  @Test public void testStatement() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement();
         ResultSet r = s.executeQuery("values 1, 2")) {
      assertThat(r.next(), is(true));
      assertThat(r.getString(1), notNullValue());
      assertThat(r.next(), is(true));
      assertThat(r.next(), is(false));
    }
  }

  @Test public void testCreateSchema() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      boolean b = s.execute("create schema s");
      assertThat(b, is(false));
      b = s.execute("create table s.t (i int not null)");
      assertThat(b, is(false));
      int x = s.executeUpdate("insert into s.t values 1");
      assertThat(x, is(1));
      try (ResultSet r = s.executeQuery("select count(*) from s.t")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(1));
        assertThat(r.next(), is(false));
      }
    }
  }

  @Test public void testCreateType() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      boolean b = s.execute("create type mytype1 as BIGINT");
      assertThat(b, is(false));
      b = s.execute("create or replace type mytype2 as (i int not null, jj mytype1)");
      assertThat(b, is(false));
      b = s.execute("create type mytype3 as (i int not null, jj mytype2)");
      assertThat(b, is(false));
      b = s.execute("create or replace type mytype1 as DOUBLE");
      assertThat(b, is(false));
      b = s.execute("create table t (c mytype1 NOT NULL)");
      assertThat(b, is(false));
      b = s.execute("create type mytype4 as BIGINT");
      assertThat(b, is(false));
      int x = s.executeUpdate("insert into t values 12.0");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t values 3.0");
      assertThat(x, is(1));
      try (ResultSet r = s.executeQuery("select CAST(c AS mytype4) from t")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(12));
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(3));
        assertThat(r.next(), is(false));
      }
    }
  }

  @Test public void testDropType() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      boolean b = s.execute("create type mytype1 as BIGINT");
      assertThat(b, is(false));
      b = s.execute("drop type mytype1");
      assertThat(b, is(false));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3046">[CALCITE-3046]
   * CompileException when inserting casted value of composited user defined type
   * into table</a>. */
  @Test public void testCreateTable() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      boolean b = s.execute("create table t (i int not null)");
      assertThat(b, is(false));
      int x = s.executeUpdate("insert into t values 1");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t values 3");
      assertThat(x, is(1));
      try (ResultSet r = s.executeQuery("select sum(i) from t")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(4));
        assertThat(r.next(), is(false));
      }

      // CALCITE-2464: Allow to set nullability for columns of structured types
      b = s.execute("create type mytype as (i int)");
      assertThat(b, is(false));
      b = s.execute("create table w (i int not null, j mytype)");
      assertThat(b, is(false));
      x = s.executeUpdate("insert into w values (1, NULL)");
      assertThat(x, is(1));

      // Test user defined type name as component identifier.
      b = s.execute("create schema a");
      assertThat(b, is(false));
      b = s.execute("create schema a.b");
      assertThat(b, is(false));
      b = s.execute("create type a.b.mytype as (i varchar(5))");
      assertThat(b, is(false));
      b = s.execute("create table t2 (i int not null, j a.b.mytype)");
      assertThat(b, is(false));
      x = s.executeUpdate("insert into t2 values (1, NULL)");
      assertThat(x, is(1));
    }
  }

  @Test public void testInsertCastedValueOfCompositeUdt() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      boolean b = s.execute("create type mytype as (i int, j int)");
      assertThat(b, is(false));
      b = s.execute("create table w (i int not null, j mytype)");
      assertThat(b, is(false));
      int x = s.executeUpdate("insert into w "
          + "values (1, cast((select j from w limit 1) as mytype))");
      assertThat(x, is(1));
    }
  }

  @Test public void testStoredGeneratedColumn() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      final String sql0 = "create table t (\n"
          + " h int not null,\n"
          + " i int,\n"
          + " j int as (i + 1) stored)";
      boolean b = s.execute(sql0);
      assertThat(b, is(false));

      int x;

      // A successful row.
      x = s.executeUpdate("insert into t (h, i) values (3, 4)");
      assertThat(x, is(1));

      final String sql1 = "explain plan for\n"
          + "insert into t (h, i) values (3, 4)";
      try (ResultSet r = s.executeQuery(sql1)) {
        assertThat(r.next(), is(true));
        final String plan = ""
            + "EnumerableTableModify(table=[[T]], operation=[INSERT], flattened=[false])\n"
            + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)], proj#0..1=[{exprs}], J=[$t3])\n"
            + "    EnumerableValues(tuples=[[{ 3, 4 }]])\n";
        assertThat(r.getString(1), isLinux(plan));
        assertThat(r.next(), is(false));
      }

      try (ResultSet r = s.executeQuery("select * from t")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt("H"), is(3));
        assertThat(r.wasNull(), is(false));
        assertThat(r.getInt("I"), is(4));
        assertThat(r.getInt("J"), is(5)); // j = i + 1
        assertThat(r.next(), is(false));
      }

      // No target column list; too few values provided
      try {
        x = s.executeUpdate("insert into t values (2, 3)");
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Number of INSERT target columns (3) does not equal "
                + "number of source items (2)"));
      }

      // No target column list; too many values provided
      try {
        x = s.executeUpdate("insert into t values (3, 4, 5, 6)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Number of INSERT target columns (3) does not equal "
                + "number of source items (4)"));
      }

      // No target column list;
      // source count = target count;
      // but one of the target columns is virtual.
      try {
        x = s.executeUpdate("insert into t values (3, 4, 5)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Cannot INSERT into generated column 'J'"));
      }

      // Explicit target column list, omits virtual column
      x = s.executeUpdate("insert into t (h, i) values (1, 2)");
      assertThat(x, is(1));

      // Explicit target column list, includes virtual column but assigns
      // DEFAULT.
      x = s.executeUpdate("insert into t (h, i, j) values (1, 2, DEFAULT)");
      assertThat(x, is(1));

      // As previous, re-order columns.
      x = s.executeUpdate("insert into t (h, j, i) values (1, DEFAULT, 3)");
      assertThat(x, is(1));

      // Target column list exists,
      // target column count equals the number of non-virtual columns;
      // but one of the target columns is virtual.
      try {
        x = s.executeUpdate("insert into t (h, j) values (1, 3)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Cannot INSERT into generated column 'J'"));
      }

      // Target column list exists and contains all columns,
      // expression for virtual column is not DEFAULT.
      try {
        x = s.executeUpdate("insert into t (h, i, j) values (2, 3, 3 + 1)");
        fail("expected error, got " + x);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("Cannot INSERT into generated column 'J'"));
      }
      x = s.executeUpdate("insert into t (h, i) values (0, 1)");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t (h, i, j) values (0, 1, DEFAULT)");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t (j, i, h) values (DEFAULT, NULL, 7)");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t (h, i) values (6, 5), (7, 4)");
      assertThat(x, is(2));
      try (ResultSet r = s.executeQuery("select sum(i), count(*) from t")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(22));
        assertThat(r.getInt(2), is(10));
        assertThat(r.next(), is(false));
      }
    }
  }

  @Ignore("not working yet")
  @Test public void testStoredGeneratedColumn2() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      final String sql = "create table t (\n"
          + " h int not null,\n"
          + " i int,\n"
          + " j int as (i + 1) stored)";
      boolean b = s.execute(sql);
      assertThat(b, is(false));

      // Planner uses constraint to optimize away condition.
      final String sql2 = "explain plan for\n"
          + "select * from t where j = i + 1";
      final String plan = "EnumerableTableScan(table=[[T]])\n";
      try (ResultSet r = s.executeQuery(sql2)) {
        assertThat(r.next(), is(true));
        assertThat(r.getString(1), is(plan));
        assertThat(r.next(), is(false));
      }
    }
  }

  @Test public void testVirtualColumn() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      final String sql0 = "create table t (\n"
          + " h int not null,\n"
          + " i int,\n"
          + " j int as (i + 1) virtual)";
      boolean b = s.execute(sql0);
      assertThat(b, is(false));

      int x = s.executeUpdate("insert into t (h, i) values (1, 2)");
      assertThat(x, is(1));

      // In plan, "j" is replaced by "i + 1".
      final String sql = "select * from t";
      try (ResultSet r = s.executeQuery(sql)) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(1));
        assertThat(r.getInt(2), is(2));
        assertThat(r.getInt(3), is(3));
        assertThat(r.next(), is(false));
      }

      final String plan = ""
          + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)], proj#0..1=[{exprs}], J=[$t3])\n"
          + "  EnumerableTableScan(table=[[T]])\n";
      try (ResultSet r = s.executeQuery("explain plan for " + sql)) {
        assertThat(r.next(), is(true));
        assertThat(r.getString(1), isLinux(plan));
      }
    }
  }
}

// End ServerTest.java
