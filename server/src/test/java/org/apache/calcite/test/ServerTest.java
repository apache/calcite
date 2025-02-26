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
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.server.DdlExecutorImpl;
import org.apache.calcite.server.ServerDdlExecutor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlCreateForeignSchema;
import org.apache.calcite.sql.ddl.SqlCreateFunction;
import org.apache.calcite.sql.ddl.SqlCreateMaterializedView;
import org.apache.calcite.sql.ddl.SqlCreateSchema;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateTableLike;
import org.apache.calcite.sql.ddl.SqlCreateType;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.ddl.SqlDropFunction;
import org.apache.calcite.sql.ddl.SqlDropMaterializedView;
import org.apache.calcite.sql.ddl.SqlDropSchema;
import org.apache.calcite.sql.ddl.SqlTruncateTable;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.test.Matchers.isLinux;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit tests for server and DDL.
 */
class ServerTest {

  static final String URL = "jdbc:calcite:";

  static Connection connect() throws SQLException {
    return DriverManager.getConnection(URL,
        CalciteAssert.propBuilder()
            .set(CalciteConnectionProperty.PARSER_FACTORY,
                ServerDdlExecutor.class.getName() + "#PARSER_FACTORY")
            .set(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED,
                "true")
            .set(CalciteConnectionProperty.FUN, "standard,oracle")
            .build());
  }

  /** Contains calls to all overloaded {@code execute} methods in
   * {@link DdlExecutorImpl} to silence warnings that these methods are not
   * called. (They are, not from this test, but via reflection.) */
  @Test void testAll() {
    //noinspection ConstantConditions
    if (true) {
      return;
    }
    final ServerDdlExecutor executor = ServerDdlExecutor.INSTANCE;
    final Object o = "x";
    final CalcitePrepare.Context context = (CalcitePrepare.Context) o;
    executor.execute((SqlNode) o, context);
    executor.execute((SqlCreateFunction) o, context);
    executor.execute((SqlCreateTable) o, context);
    executor.execute((SqlCreateTableLike) o, context);
    executor.execute((SqlCreateSchema) o, context);
    executor.execute((SqlCreateMaterializedView) o, context);
    executor.execute((SqlCreateView) o, context);
    executor.execute((SqlCreateType) o, context);
    executor.execute((SqlCreateSchema) o, context);
    executor.execute((SqlCreateForeignSchema) o, context);
    executor.execute((SqlDropMaterializedView) o, context);
    executor.execute((SqlDropFunction) o, context);
    executor.execute((SqlDropSchema) o, context);
    executor.execute((SqlTruncateTable) o, context);
  }

  @Test void testStatement() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement();
         ResultSet r = s.executeQuery("values 1, 2")) {
      assertThat(r.next(), is(true));
      assertThat(r.getString(1), notNullValue());
      assertThat(r.next(), is(true));
      assertThat(r.next(), is(false));
    }
  }

  @Test void testCreateSchema() throws Exception {
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

      assertDoesNotThrow(() -> {
        s.execute("create schema if not exists s");
        s.executeUpdate("insert into s.t values 2");
      }, "IF NOT EXISTS should not overwrite the existing schema");

      assertDoesNotThrow(() -> {
        s.execute("create or replace schema s");
        s.execute("create table s.t (i int not null)");
      }, "REPLACE must overwrite the existing schema");
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5905">[CALCITE-5905]
   * Documentation for CREATE TYPE is incorrect</a>. */
  @Test void testCreateTypeDocumentationExample() throws SQLException {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      boolean b = s.execute("CREATE TYPE address_typ AS (\n"
          + "   street          VARCHAR(30),\n"
          + "   city            VARCHAR(20),\n"
          + "   state           CHAR(2),\n"
          + "   postal_code     VARCHAR(6))");
      assertThat(b, is(false));
      b = s.execute("CREATE TYPE employee_typ AS (\n"
          + "  employee_id       DECIMAL(6),\n"
          + "  first_name        VARCHAR(20),\n"
          + "  last_name         VARCHAR(25),\n"
          + "  email             VARCHAR(25),\n"
          + "  phone_number      VARCHAR(20),\n"
          + "  hire_date         DATE,\n"
          + "  job_id            VARCHAR(10),\n"
          + "  salary            DECIMAL(8,2),\n"
          + "  commission_pct    DECIMAL(2,2),\n"
          + "  manager_id        DECIMAL(6),\n"
          + "  department_id     DECIMAL(4),\n"
          + "  address           address_typ)\n");
      assertThat(b, is(false));
      try (ResultSet r =
               s.executeQuery("SELECT employee_typ(315, 'Francis', 'Logan', 'FLOGAN',\n"
                   + "    '555.777.2222', DATE '2004-05-01', 'SA_MAN', 11000, .15, 101, 110,\n"
                   + "     address_typ('376 Mission', 'San Francisco', 'CA', '94222'))")) {
        assertThat(r.next(), is(true));
        Struct obj = r.getObject(1, Struct.class);
        Object[] data = obj.getAttributes();
        assertThat(data[0], is(315));
        assertThat(data[1], is("Francis"));
        assertThat(data[2], is("Logan"));
        assertThat(data[3], is("FLOGAN"));
        assertThat(data[4], is("555.777.2222"));
        assertThat(data[5], is(java.sql.Date.valueOf("2004-05-01")));
        assertThat(data[6], is("SA_MAN"));
        assertThat(data[7], is(11000));
        assertThat(data[8], is(new BigDecimal(".15")));
        assertThat(data[9], is(101));
        assertThat(data[10], is(110));
        Struct address = (Struct) data[11];
        assertArrayEquals(address.getAttributes(),
            new Object[] { "376 Mission", "San Francisco", "CA", "94222" });
      }
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6361">[CALCITE-6361]
   * Uncollect.deriveUncollectRowType throws AssertionFailures
   * if the input data is not a collection</a>. */
  @Test void testUnnest() throws SQLException {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      boolean b = s.execute("CREATE TYPE simple AS (s INT, t BOOLEAN)");
      assertThat(b, is(false));
      b = s.execute("CREATE TYPE vec AS (fields SIMPLE ARRAY)");
      assertThat(b, is(false));
      b = s.execute(" CREATE TABLE T(col vec)");
      assertThat(b, is(false));
      SQLException e =
          assertThrows(
              SQLException.class,
              () -> s.executeQuery("SELECT A.* FROM (T CROSS JOIN UNNEST(T.col) A)"));
      assertThat(e.getMessage(), containsString("UNNEST argument must be a collection"));
    }
  }

  @Test void testCreateType() throws Exception {
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

  @Test void testDropType() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      boolean b = s.execute("create type mytype1 as BIGINT");
      assertThat(b, is(false));
      b = s.execute("drop type mytype1");
      assertThat(b, is(false));
    }
  }

  @Test void testCreateTable() throws Exception {
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

      assertDoesNotThrow(() -> {
        s.execute("create or replace table t2 (i int not null)");
        s.executeUpdate("insert into t2 values (1)");
      }, "REPLACE must recreate the table, leaving only one column");
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6022">[CALCITE-6022]
   * Support "CREATE TABLE ... LIKE" DDL in server module</a>. */
  @Test void testCreateTableLike() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      s.execute("create table t (i int not null)");
      s.execute("create table t2 like t");
      int x = s.executeUpdate("insert into t2 values 1");
      assertThat(x, is(1));
      x = s.executeUpdate("insert into t2 values 3");
      assertThat(x, is(1));
      try (ResultSet r = s.executeQuery("select sum(i) from t2")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt(1), is(4));
        assertThat(r.next(), is(false));
      }
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6022">[CALCITE-6022]
   * Support "CREATE TABLE ... LIKE" DDL in server module</a>. */
  @Test void testCreateTableLikeWithStoredGeneratedColumn() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      s.execute("create table t (\n"
          + " h int not null,\n"
          + " i int,\n"
          + " j int as (i + 1) stored,\n"
          + " k int default -1)\n");
      s.execute("create table t2 like t including defaults including generated");

      int x = s.executeUpdate("insert into t2 (h, i) values (3, 4)");
      assertThat(x, is(1));

      final String sql1 = "explain plan for\n"
          + "insert into t2 (h, i) values (3, 4)";
      try (ResultSet r = s.executeQuery(sql1)) {
        assertThat(r.next(), is(true));
        final String plan = ""
            + "EnumerableTableModify(table=[[T2]], operation=[INSERT], flattened=[false])\n"
            + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[1], expr#3=[+($t1, $t2)], expr#4=[-1], proj#0..1=[{exprs}], J=[$t3], K=[$t4])\n"
            + "    EnumerableValues(tuples=[[{ 3, 4 }]])\n";
        assertThat(r.getString(1), isLinux(plan));
        assertThat(r.next(), is(false));
      }

      try (ResultSet r = s.executeQuery("select * from t2")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt("H"), is(3));
        assertThat(r.wasNull(), is(false));
        assertThat(r.getInt("I"), is(4));
        assertThat(r.getInt("J"), is(5)); // j = i + 1
        assertThat(r.getInt("K"), is(-1)); // k = -1 (default)
        assertThat(r.next(), is(false));
      }

      SQLException e =
          assertThrows(
              SQLException.class, () -> s.executeUpdate("insert into t2 values (3, 4, 5, 6)"));
      assertThat(e.getMessage(), containsString("Cannot INSERT into generated column 'J'"));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6022">[CALCITE-6022]
   * Support "CREATE TABLE ... LIKE" DDL in server module</a>. */
  @Test void testCreateTableLikeWithVirtualGeneratedColumn() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      s.execute("create table t (\n"
          + " h int not null,\n"
          + " i int,\n"
          + " j int as (i + 1) virtual)\n");
      s.execute("create table t2 like t including defaults including generated");

      int x = s.executeUpdate("insert into t2 (h, i) values (3, 4)");
      assertThat(x, is(1));

      final String sql1 = "explain plan for\n"
          + "insert into t (h, i) values (3, 4)";
      try (ResultSet r = s.executeQuery(sql1)) {
        final String sql2 = "explain plan for\n"
            + "insert into t2 (h, i) values (3, 4)";
        assertThat(r.next(), is(true));
        final String plan = r.getString(1);
        assertThat(r.next(), is(false));

        ResultSet r2 = s.executeQuery(sql2);
        assertThat(r2.next(), is(true));
        assertThat(r2.getString(1).replace("T2", "T"), is(plan));
        assertThat(r2.next(), is(false));
      }

      try (ResultSet r = s.executeQuery("select * from t2")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt("H"), is(3));
        assertThat(r.wasNull(), is(false));
        assertThat(r.getInt("I"), is(4));
        assertThat(r.getInt("J"), is(5)); // j = i + 1
        assertThat(r.next(), is(false));
      }

      SQLException e =
          assertThrows(
              SQLException.class, () -> s.executeUpdate("insert into t2 values (3, 4, 5)"));
      assertThat(e.getMessage(), containsString("Cannot INSERT into generated column 'J'"));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6022">[CALCITE-6022]
   * Support "CREATE TABLE ... LIKE" DDL in server module</a>. */
  @Test void testCreateTableLikeWithoutLikeOptions() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      s.execute("create table t (\n"
          + " h int not null,\n"
          + " i int,\n"
          + " j int as (i + 1) stored,\n"
          + " k int default -1)");
      // In table t2, only copy the column and type information from t,
      // excluding generated expression and default expression
      s.execute("create table t2 like t");

      int x = s.executeUpdate("insert into t2 (h, i) values (3, 4)");
      assertThat(x, is(1));

      final String sql1 = "explain plan for\n"
          + "insert into t2 (h, i) values (3, 4)";
      try (ResultSet r = s.executeQuery(sql1)) {
        assertThat(r.next(), is(true));
        final String plan = ""
            + "EnumerableTableModify(table=[[T2]], operation=[INSERT], flattened=[false])\n"
            + "  EnumerableCalc(expr#0..1=[{inputs}], expr#2=[null:INTEGER], proj#0..2=[{exprs}], K=[$t2])\n"
            + "    EnumerableValues(tuples=[[{ 3, 4 }]])\n";
        assertThat(r.getString(1), isLinux(plan));
        assertThat(r.next(), is(false));
      }

      try (ResultSet r = s.executeQuery("select * from t2")) {
        assertThat(r.next(), is(true));
        assertThat(r.getInt("H"), is(3));
        assertThat(r.wasNull(), is(false));
        assertThat(r.getInt("I"), is(4));
        assertThat(r.getInt("J"), is(0)); // excluding generated column
        assertThat(r.wasNull(), is(true));
        assertThat(r.getInt("K"), is(0)); // excluding default column
        assertThat(r.wasNull(), is(true));
        assertThat(r.next(), is(false));
      }

      x = s.executeUpdate("insert into t2 values (3, 4, 5, 6)");
      assertThat(x, is(1));
    }
  }

  @Test void testTruncateTable() throws Exception {
    try (Connection c = connect();
        Statement s = c.createStatement()) {
      final boolean b = s.execute("create table t (i int not null)");
      assertThat(b, is(false));

      final String errMsg =
          assertThrows(SQLException.class,
              () -> s.execute("truncate table t restart identity")).getMessage();
      assertThat(errMsg, containsString("RESTART IDENTIFY is not supported"));
    }
  }

  @Test void testCreateFunction() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      boolean b = s.execute("create schema s");
      assertThat(b, is(false));
      try {
        boolean f = s.execute("create function if not exists s.t\n"
                + "as 'org.apache.calcite.udf.TableFun.demoUdf'\n"
                + "using jar 'file:/path/udf/udf-0.0.1-SNAPSHOT.jar'");
        fail("expected error, got " + f);
      } catch (SQLException e) {
        assertThat(e.getMessage(),
            containsString("CREATE FUNCTION is not supported"));
      }
    }
  }

  @Test void testDropFunction() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      boolean b = s.execute("create schema s");
      assertThat(b, is(false));

      boolean f = s.execute("drop function if exists t");
      assertThat(f, is(false));

      try {
        boolean f2 = s.execute("drop function t");
        assertThat(f2, is(false));
      } catch (SQLException e) {
        assertThat(e.getMessage(),
                containsString("Error while executing SQL \"drop function t\":"
                        + " At line 1, column 15: Function 'T' not found"));
      }

      CalciteConnection calciteConnection = (CalciteConnection) c;
      calciteConnection.getRootSchema().add("T", new Function() {
        @Override public List<FunctionParameter> getParameters() {
          return new ArrayList<>();
        }
      });

      boolean f3 = s.execute("drop function t");
      assertThat(f3, is(false));

      // case sensitive function name
      calciteConnection.getRootSchema().add("t", new Function() {
        @Override public List<FunctionParameter> getParameters() {
          return new ArrayList<>();
        }
      });

      try {
        boolean f4 = s.execute("drop function t");
        assertThat(f4, is(false));
      } catch (SQLException e) {
        assertThat(e.getMessage(),
                containsString("Error while executing SQL \"drop function t\":"
                        + " At line 1, column 15: Function 'T' not found"));
      }
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3046">[CALCITE-3046]
   * CompileException when inserting casted value of composited user defined type
   * into table</a>. */
  @Test void testInsertCastedValueOfCompositeUdt() throws Exception {
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

  @Test void testInsertCreateNewCompositeUdt() throws Exception {
    try (Connection c = connect();
        Statement s = c.createStatement()) {
      boolean b = s.execute("create type mytype as (i int, j int)");
      assertFalse(b);
      b = s.execute("create table w (i int not null, j mytype)");
      assertFalse(b);
      int x = s.executeUpdate("insert into w "
          + "values (1, mytype(1, 1))");
      assertThat(x, is(1));

      try (ResultSet r = s.executeQuery("select * from w")) {
        assertTrue(r.next());
        assertThat(r.getInt("i"), is(1));
        assertArrayEquals(r.getObject("j", Struct.class).getAttributes(), new Object[] {1, 1});
        assertFalse(r.next());
      }
    }
  }

  @Test void testStoredGeneratedColumn() throws Exception {
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

  @Disabled("not working yet")
  @Test void testStoredGeneratedColumn2() throws Exception {
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

  @Test void testVirtualColumn() throws Exception {
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

  @Test void testVirtualColumnWithFunctions() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      // Test builtin and library functions.
      final String create = "create table t1 (\n"
          + " h varchar(3) not null,\n"
          + " i varchar(3),\n"
          + " j int not null as (char_length(h)) virtual,\n"
          + " k varchar(3) null as (rtrim(i)) virtual)";
      boolean b = s.execute(create);
      assertThat(b, is(false));

      int x = s.executeUpdate("insert into t1 (h, i) values ('abc', 'de ')");
      assertThat(x, is(1));

      // In plan, "j" is replaced by "char_length(h)".
      final String select = "select * from t1";
      try (ResultSet r = s.executeQuery(select)) {
        assertThat(r.next(), is(true));
        assertThat(r.getString(1), is("abc"));
        assertThat(r.getString(2), is("de "));
        assertThat(r.getInt(3), is(3));
        assertThat(r.getString(4), is("de"));
        assertThat(r.next(), is(false));
      }

      final String plan = ""
          + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[CHAR_LENGTH($t0)], "
          + "expr#3=[FLAG(TRAILING)], expr#4=[' '], "
          + "expr#5=[TRIM($t3, $t4, $t1)], proj#0..2=[{exprs}], K=[$t5])\n"
          + "  EnumerableTableScan(table=[[T1]])\n";
      try (ResultSet r = s.executeQuery("explain plan for " + select)) {
        assertThat(r.next(), is(true));
        assertThat(r.getString(1), isLinux(plan));
      }
    }
  }

  @Test void testDropWithFullyQualifiedNameWhenSchemaDoesntExist() throws Exception {
    try (Connection c = connect();
         Statement s = c.createStatement()) {
      checkDropWithFullyQualifiedNameWhenSchemaDoesntExist(s, "schema", "Schema");
      checkDropWithFullyQualifiedNameWhenSchemaDoesntExist(s, "table", "Table");
      checkDropWithFullyQualifiedNameWhenSchemaDoesntExist(s, "materialized view", "Table");
      checkDropWithFullyQualifiedNameWhenSchemaDoesntExist(s, "view", "View");
      checkDropWithFullyQualifiedNameWhenSchemaDoesntExist(s, "type", "Type");
      checkDropWithFullyQualifiedNameWhenSchemaDoesntExist(s, "function", "Function");
    }
  }

  private void checkDropWithFullyQualifiedNameWhenSchemaDoesntExist(
      Statement statement, String objectType, String objectTypeInErrorMessage) throws Exception {
    SQLException e = assertThrows(SQLException.class, () ->
        statement.execute("drop " + objectType + " s.o"),
        "expected error because the object doesn't exist");
    assertThat(e.getMessage(), containsString(objectTypeInErrorMessage + " 'O' not found"));

    statement.execute("drop " + objectType + " if exists s.o");
  }
}
