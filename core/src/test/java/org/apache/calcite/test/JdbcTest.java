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

import org.apache.calcite.adapter.clone.CloneSchema;
import org.apache.calcite.adapter.generate.RangeTable;
import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Handler;
import org.apache.calcite.avatica.HandlerImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalciteMetaImpl;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.rules.IntersectToDistinctRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFactory;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractTableQueryable;
import org.apache.calcite.schema.impl.TableMacroImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.JsonBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.TryThreadLocal;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import org.hamcrest.Matcher;
import org.hsqldb.jdbcDriver;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import javax.sql.DataSource;

import static org.apache.calcite.test.Matchers.isLinux;
import static org.apache.calcite.util.Static.RESOURCE;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for using Calcite via JDBC.
 */
public class JdbcTest {

  public static final String FOODMART_SCHEMA = "     {\n"
      + "       type: 'jdbc',\n"
      + "       name: 'foodmart',\n"
      + "       jdbcDriver: " + q(CalciteAssert.DB.foodmart.driver) + ",\n"
      + "       jdbcUser: " + q(CalciteAssert.DB.foodmart.username) + ",\n"
      + "       jdbcPassword: " + q(CalciteAssert.DB.foodmart.password) + ",\n"
      + "       jdbcUrl: " + q(CalciteAssert.DB.foodmart.url) + ",\n"
      + "       jdbcCatalog: " + q(CalciteAssert.DB.foodmart.catalog) + ",\n"
      + "       jdbcSchema: " + q(CalciteAssert.DB.foodmart.schema) + "\n"
      + "     }\n";

  public static final String FOODMART_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'foodmart',\n"
      + "   schemas: [\n"
      + FOODMART_SCHEMA
      + "   ]\n"
      + "}";

  public static final ConnectionSpec SCOTT =
      Util.first(CalciteAssert.DB.scott,
          CalciteAssert.DatabaseInstance.HSQLDB.scott);

  public static final String SCOTT_SCHEMA = "     {\n"
      + "       type: 'jdbc',\n"
      + "       name: 'SCOTT',\n"
      + "       jdbcDriver: " + q(SCOTT.driver) + ",\n"
      + "       jdbcUser: " + q(SCOTT.username) + ",\n"
      + "       jdbcPassword: " + q(SCOTT.password) + ",\n"
      + "       jdbcUrl: " + q(SCOTT.url) + ",\n"
      + "       jdbcCatalog: " + q(SCOTT.catalog) + ",\n"
      + "       jdbcSchema: " + q(SCOTT.schema) + "\n"
      + "     }\n";

  public static final String SCOTT_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'SCOTT',\n"
      + "   schemas: [\n"
      + SCOTT_SCHEMA
      + "   ]\n"
      + "}";

  public static final String HR_SCHEMA = "     {\n"
      + "       type: 'custom',\n"
      + "       name: 'hr',\n"
      + "       factory: '"
      + ReflectiveSchema.Factory.class.getName()
      + "',\n"
      + "       operand: {\n"
      + "         class: '" + HrSchema.class.getName() + "'\n"
      + "       }\n"
      + "     }\n";

  public static final String HR_MODEL = "{\n"
      + "  version: '1.0',\n"
      + "  defaultSchema: 'hr',\n"
      + "   schemas: [\n"
      + HR_SCHEMA
      + "   ]\n"
      + "}";

  public static final String START_OF_GROUP_DATA = "(values"
      + "(1,0,1),\n"
      + "(2,0,1),\n"
      + "(3,1,2),\n"
      + "(4,0,3),\n"
      + "(5,0,3),\n"
      + "(6,0,3),\n"
      + "(7,1,4),\n"
      + "(8,1,4))\n"
      + " as t(rn,val,expected)";

  private static String q(String s) {
    return s == null ? "null" : "'" + s + "'";
  }

  public static List<Pair<String, String>> getFoodmartQueries() {
    return FOODMART_QUERIES;
  }

  /** Tests a modifiable view. */
  @Test public void testModelWithModifiableView() throws Exception {
    final List<Employee> employees = new ArrayList<>();
    employees.add(new Employee(135, 10, "Simon", 56.7f, null));
    try (TryThreadLocal.Memo ignore =
             EmpDeptTableFactory.THREAD_COLLECTION.push(employees)) {
      final CalciteAssert.AssertThat with = modelWithView(
          "select \"name\", \"empid\" as e, \"salary\" "
              + "from \"MUTABLE_EMPLOYEES\" where \"deptno\" = 10",
          null);
      with.query("select \"name\" from \"adhoc\".V order by \"name\"")
          .returns("name=Simon\n");
      with.doWithConnection(connection -> {
        try {
          final Statement statement = connection.createStatement();
          ResultSet resultSet =
              statement.executeQuery("explain plan for\n"
                  + "insert into \"adhoc\".V\n"
                  + "values ('Fred', 56, 123.4)");
          assertThat(resultSet.next(), is(true));
          assertThat(resultSet.getString(1),
              isLinux(
                  "EnumerableTableModify(table=[[adhoc, MUTABLE_EMPLOYEES]], operation=[INSERT], flattened=[false])\n"
                  + "  EnumerableCalc(expr#0..2=[{inputs}], expr#3=[CAST($t1):JavaType(int) NOT NULL], expr#4=[10], expr#5=[CAST($t0):JavaType(class java.lang.String)], expr#6=[CAST($t2):JavaType(float) NOT NULL], expr#7=[null], empid=[$t3], deptno=[$t4], name=[$t5], salary=[$t6], commission=[$t7])\n"
                  + "    EnumerableValues(tuples=[[{ 'Fred', 56, 123.4 }]])\n"));

          // With named columns
          resultSet =
              statement.executeQuery("explain plan for\n"
                  + "insert into \"adhoc\".V (\"name\", e, \"salary\")\n"
                  + "values ('Fred', 56, 123.4)");
          assertThat(resultSet.next(), is(true));

          // With named columns, in different order
          resultSet =
              statement.executeQuery("explain plan for\n"
                  + "insert into \"adhoc\".V (e, \"salary\", \"name\")\n"
                  + "values (56, 123.4, 'Fred')");
          assertThat(resultSet.next(), is(true));

          // Mis-named column
          try {
            final PreparedStatement s =
                connection.prepareStatement("explain plan for\n"
                    + "insert into \"adhoc\".V (empno, \"salary\", \"name\")\n"
                    + "values (56, 123.4, 'Fred')");
            fail("expected error, got " + s);
          } catch (SQLException e) {
            assertThat(e.getMessage(),
                startsWith("Error while preparing statement"));
          }

          // Fail to provide mandatory column
          try {
            final PreparedStatement s =
                connection.prepareStatement("explain plan for\n"
                    + "insert into \"adhoc\".V (e, name)\n"
                    + "values (56, 'Fred')");
            fail("expected error, got " + s);
          } catch (SQLException e) {
            assertThat(e.getMessage(),
                startsWith("Error while preparing statement"));
          }

          statement.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
    }
  }

  /** Tests a few cases where modifiable views are invalid. */
  @Test public void testModelWithInvalidModifiableView() throws Exception {
    final List<Employee> employees = new ArrayList<>();
    employees.add(new Employee(135, 10, "Simon", 56.7f, null));
    try (TryThreadLocal.Memo ignore =
             EmpDeptTableFactory.THREAD_COLLECTION.push(employees)) {
      Util.discard(RESOURCE.noValueSuppliedForViewColumn(null, null));
      modelWithView("select \"name\", \"empid\" as e, \"salary\" "
              + "from \"MUTABLE_EMPLOYEES\" where \"commission\" = 10",
          true)
          .query("select \"name\" from \"adhoc\".V order by \"name\"")
          .throws_(
              "View is not modifiable. No value is supplied for NOT NULL "
                  + "column 'deptno' of base table 'MUTABLE_EMPLOYEES'");

      // no error if we do not claim that the view is modifiable
      modelWithView(
          "select \"name\", \"empid\" as e, \"salary\" "
              + "from \"MUTABLE_EMPLOYEES\" where \"commission\" = 10", null)
          .query("select \"name\" from \"adhoc\".V order by \"name\"")
          .runs();

      modelWithView("select \"name\", \"empid\" as e, \"salary\" "
              + "from \"MUTABLE_EMPLOYEES\" where \"deptno\" IN (10, 20)",
          true)
          .query("select \"name\" from \"adhoc\".V order by \"name\"")
          .throws_(
              "Modifiable view must be predicated only on equality expressions");

      // Deduce "deptno = 10" from the constraint, and add a further
      // condition "deptno < 20 OR commission > 1000".
      modelWithView("select \"name\", \"empid\" as e, \"salary\" "
              + "from \"MUTABLE_EMPLOYEES\"\n"
              + "where \"deptno\" = 10 AND (\"deptno\" < 20 OR \"commission\" > 1000)",
          true)
          .query("insert into \"adhoc\".v values ('n',1,2)")
          .throws_(
              "Modifiable view must be predicated only on equality expressions");
      modelWithView("select \"name\", \"empid\" as e, \"salary\" "
              + "from \"MUTABLE_EMPLOYEES\"\n"
              + "where \"deptno\" = 10 AND (\"deptno\" > 20 AND \"commission\" > 1000)",
          true)
          .query("insert into \"adhoc\".v values ('n',1,2)")
          .throws_(
              "Modifiable view must be predicated only on equality expressions");

      modelWithView(
          "select \"name\", \"empid\" as e, \"salary\" "
              + "from \"MUTABLE_EMPLOYEES\"\n"
              + "where \"commission\" = 100 AND \"deptno\" = 20",
          true)
          .query("select \"name\" from \"adhoc\".V order by \"name\"")
          .runs();

      modelWithView(
          "select \"name\", \"empid\" as e, \"salary\", \"empid\" + 3 as e3, 1 as uno\n"
              + "from \"MUTABLE_EMPLOYEES\"\n"
              + "where \"commission\" = 100 AND \"deptno\" = 20",
          true)
          .query("select \"name\" from \"adhoc\".V order by \"name\"")
          .runs();

      Util.discard(RESOURCE.moreThanOneMappedColumn(null, null));
      modelWithView(
          "select \"name\", \"empid\" as e, \"salary\", \"name\" as n2 "
              + "from \"MUTABLE_EMPLOYEES\" where \"deptno\" IN (10, 20)",
          true)
          .query("select \"name\" from \"adhoc\".V order by \"name\"")
          .throws_(
              "View is not modifiable. More than one expression maps to "
              + "column 'name' of base table 'MUTABLE_EMPLOYEES'");

      // no error if we do not claim that the view is modifiable
      modelWithView(
          "select \"name\", \"empid\" as e, \"salary\", \"name\" as n2 "
              + "from \"MUTABLE_EMPLOYEES\" where \"deptno\" IN (10, 20)",
          null)
          .query("select \"name\" from \"adhoc\".V order by \"name\"")
          .runs();
    }
  }

  /**
   * Tests a relation that is accessed via method syntax.
   *
   * <p>The function ({@link Smalls#view(String)} has a return type
   * {@link Table} and the actual returned value implements
   * {@link org.apache.calcite.schema.TranslatableTable}.
   */
  @Test public void testTableMacro()
      throws SQLException, ClassNotFoundException {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final TableMacro tableMacro = TableMacroImpl.create(Smalls.VIEW_METHOD);
    schema.add("View", tableMacro);
    ResultSet resultSet = connection.createStatement().executeQuery("select *\n"
        + "from table(\"s\".\"View\"('(10), (20)')) as t(n)\n"
        + "where n < 15");
    // The call to "View('(10), (2)')" expands to 'values (1), (3), (10), (20)'.
    assertThat(CalciteAssert.toString(resultSet),
        equalTo("N=1\n"
            + "N=3\n"
            + "N=10\n"));
    connection.close();
  }

  /** Table macro that takes a MAP as a parameter.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-588">[CALCITE-588]
   * Allow TableMacro to consume Maps and Collections</a>. */
  @Test public void testTableMacroMap()
      throws SQLException, ClassNotFoundException {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final TableMacro tableMacro = TableMacroImpl.create(Smalls.STR_METHOD);
    schema.add("Str", tableMacro);
    ResultSet resultSet = connection.createStatement().executeQuery("select *\n"
        + "from table(\"s\".\"Str\"(MAP['a', 1, 'baz', 2],\n"
        + "                         ARRAY[3, 4, CAST(null AS INTEGER)])) as t(n)");
    // The call to "View('(10), (2)')" expands to 'values (1), (3), (10), (20)'.
    assertThat(CalciteAssert.toString(resultSet),
        equalTo("N={'a'=1, 'baz'=2}\n"
            + "N=[3, 4, null]    \n"));
    connection.close();
  }

  /** Tests a table macro with named and optional parameters. */
  @Test public void testTableMacroWithNamedParameters() throws Exception {
    // View(String r optional, String s, int t optional)
    final CalciteAssert.AssertThat with =
        assertWithMacro(Smalls.TableMacroFunctionWithNamedParameters.class);
    with.query("select * from table(\"adhoc\".\"View\"('(5)'))")
        .throws_("No match found for function signature View(<CHARACTER>)");
    final String expected1 = "c=1\n"
        + "c=3\n"
        + "c=5\n"
        + "c=6\n";
    with.query("select * from table(\"adhoc\".\"View\"('5', '6'))")
        .returns(expected1);
    final String expected2 = "c=1\n"
        + "c=3\n"
        + "c=5\n"
        + "c=6\n";
    with.query("select * from table(\"adhoc\".\"View\"(r=>'5', s=>'6'))")
        .returns(expected2);
    with.query("select * from table(\"adhoc\".\"View\"(t=>'5', t=>'6'))")
        .throws_("Duplicate argument name 'T'");
    with.query("select * from table(\"adhoc\".\"View\"(t=>'5', s=>'6'))")
        .throws_(
            "No match found for function signature View(T => <CHARACTER>, S => <CHARACTER>)");
    final String expected3 = "c=1\n"
        + "c=3\n"
        + "c=6\n"
        + "c=5\n";
    with.query("select * from table(\"adhoc\".\"View\"(t=>5, s=>'6'))")
        .returns(expected3);
  }

  /** Tests a JDBC connection that provides a model that contains a table
   *  macro. */
  @Test public void testTableMacroInModel() throws Exception {
    checkTableMacroInModel(Smalls.TableMacroFunction.class);
  }

  /** Tests a JDBC connection that provides a model that contains a table
   *  macro defined as a static method. */
  @Test public void testStaticTableMacroInModel() throws Exception {
    checkTableMacroInModel(Smalls.StaticTableMacroFunction.class);
  }

  /** Tests a JDBC connection that provides a model that contains a table
   *  function. */
  @Test public void testTableFunctionInModel() throws Exception {
    checkTableFunctionInModel(Smalls.MyTableFunction.class);
  }

  /** Tests a JDBC connection that provides a model that contains a table
   *  function defined as a static method. */
  @Test public void testStaticTableFunctionInModel() throws Exception {
    checkTableFunctionInModel(Smalls.TestStaticTableFunction.class);
  }

  private CalciteAssert.AssertThat assertWithMacro(Class clazz) {
    return CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'View',\n"
        + "           className: '" + clazz.getName() + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}");
  }

  private void checkTableMacroInModel(Class clazz) {
    assertWithMacro(clazz)
        .query("select * from table(\"adhoc\".\"View\"('(30)'))")
        .returns(""
            + "c=1\n"
            + "c=3\n"
            + "c=30\n");
  }

  private void checkTableFunctionInModel(Class clazz) {
    checkTableMacroInModel(clazz);

    assertWithMacro(clazz)
        .query("select \"a\".\"c\" a, \"b\".\"c\" b\n"
            + "  from table(\"adhoc\".\"View\"('(30)')) \"a\",\n"
            + " lateral(select *\n"
            + "   from table(\"adhoc\".\"View\"('('||\n"
            + "          cast(\"a\".\"c\" as varchar(10))||')'))) \"b\"")
        .returnsUnordered(
            "A=1; B=1",
            "A=1; B=3",
            "A=1; B=1",
            "A=3; B=1",
            "A=3; B=3",
            "A=3; B=3",
            "A=30; B=1",
            "A=30; B=3",
            "A=30; B=30");
  }

  /** Tests {@link org.apache.calcite.avatica.Handler#onConnectionClose}
   * and  {@link org.apache.calcite.avatica.Handler#onStatementClose}. */
  @Test public void testOnConnectionClose() throws Exception {
    final int[] closeCount = {0};
    final int[] statementCloseCount = {0};
    final HandlerImpl h = new HandlerImpl() {
      @Override public void onConnectionClose(AvaticaConnection connection) {
        ++closeCount[0];
        throw new RuntimeException();
      }

      @Override public void onStatementClose(AvaticaStatement statement) {
        ++statementCloseCount[0];
        throw new RuntimeException();
      }
    };
    try (TryThreadLocal.Memo ignore =
             HandlerDriver.HANDLERS.push(h)) {
      final HandlerDriver driver = new HandlerDriver();
      CalciteConnection connection = (CalciteConnection)
          driver.connect("jdbc:calcite:", new Properties());
      SchemaPlus rootSchema = connection.getRootSchema();
      rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
      connection.setSchema("hr");
      final Statement statement = connection.createStatement();
      final ResultSet resultSet =
          statement.executeQuery("select * from \"emps\"");
      assertEquals(0, closeCount[0]);
      assertEquals(0, statementCloseCount[0]);
      resultSet.close();
      try {
        resultSet.next();
        fail("resultSet.next() should throw SQLException when closed");
      } catch (SQLException e) {
        assertThat(e.getMessage(), containsString("ResultSet closed"));
      }
      assertEquals(0, closeCount[0]);
      assertEquals(0, statementCloseCount[0]);

      // Close statement. It throws SQLException, but statement is still closed.
      try {
        statement.close();
        fail("expecting error");
      } catch (SQLException e) {
        // ok
      }
      assertEquals(0, closeCount[0]);
      assertEquals(1, statementCloseCount[0]);

      // Close connection. It throws SQLException, but connection is still closed.
      try {
        connection.close();
        fail("expecting error");
      } catch (SQLException e) {
        // ok
      }
      assertEquals(1, closeCount[0]);
      assertEquals(1, statementCloseCount[0]);

      // Close a closed connection. Handler is not called again.
      connection.close();
      assertEquals(1, closeCount[0]);
      assertEquals(1, statementCloseCount[0]);

    }
  }

  /** Tests {@link java.sql.Statement}.{@code closeOnCompletion()}. */
  @Test public void testStatementCloseOnCompletion() throws Exception {
    String javaVersion = System.getProperty("java.version");
    if (javaVersion.compareTo("1.7") < 0) {
      // Statement.closeOnCompletion was introduced in JDK 1.7.
      return;
    }
    final Driver driver = new Driver();
    CalciteConnection connection = (CalciteConnection)
        driver.connect("jdbc:calcite:", new Properties());
    SchemaPlus rootSchema = connection.getRootSchema();
    rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
    connection.setSchema("hr");
    final Statement statement = connection.createStatement();
    assertFalse((Boolean) CalciteAssert.call(statement, "isCloseOnCompletion"));
    CalciteAssert.call(statement, "closeOnCompletion");
    assertTrue((Boolean) CalciteAssert.call(statement, "isCloseOnCompletion"));
    final ResultSet resultSet =
        statement.executeQuery("select * from \"emps\"");

    assertFalse(resultSet.isClosed());
    assertFalse(statement.isClosed());
    assertFalse(connection.isClosed());

    // when result set is closed, statement is closed automatically
    resultSet.close();
    assertTrue(resultSet.isClosed());
    assertTrue(statement.isClosed());
    assertFalse(connection.isClosed());

    connection.close();
    assertTrue(resultSet.isClosed());
    assertTrue(statement.isClosed());
    assertTrue(connection.isClosed());
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2071">[CALCITE-2071]
   * Query with IN and OR in WHERE clause returns wrong result</a>.
   * More cases in sub-query.iq. */
  @Test public void testWhereInOr() {
    final String sql = "select \"empid\"\n"
        + "from \"hr\".\"emps\" t\n"
        + "where (\"empid\" in (select \"empid\" from \"hr\".\"emps\")\n"
        + "    or \"empid\" in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,\n"
        + "        12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25))\n"
        + "and \"empid\" in (100, 200, 150)";
    CalciteAssert.hr()
        .query(sql)
        .returnsUnordered("empid=100",
            "empid=200",
            "empid=150");
  }

  /** Tests that a driver can be extended with its own parser and can execute
   * its own flavor of DDL. */
  @Test public void testMockDdl() throws Exception {
    final MockDdlDriver driver = new MockDdlDriver();
    try (Connection connection =
             driver.connect("jdbc:calcite:", new Properties());
        Statement statement = connection.createStatement()) {
      assertThat(driver.counter, is(0));
      statement.executeUpdate("COMMIT");
      assertThat(driver.counter, is(1));
    }
  }

  /**
   * The example in the README.
   */
  @Test public void testReadme() throws ClassNotFoundException, SQLException {
    Properties info = new Properties();
    info.setProperty("lex", "JAVA");
    Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    final SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("select d.deptno, min(e.empid)\n"
            + "from hr.emps as e\n"
            + "join hr.depts as d\n"
            + "  on e.deptno = d.deptno\n"
            + "group by d.deptno\n"
            + "having count(*) > 1");
    final String s = CalciteAssert.toString(resultSet);
    assertThat(s, notNullValue());
    resultSet.close();
    statement.close();
    connection.close();
  }

  /** Test for {@link Driver#getPropertyInfo(String, Properties)}. */
  @Test public void testConnectionProperties() throws ClassNotFoundException,
      SQLException {
    java.sql.Driver driver = DriverManager.getDriver("jdbc:calcite:");
    final DriverPropertyInfo[] propertyInfo =
        driver.getPropertyInfo("jdbc:calcite:", new Properties());
    final Set<String> names = new HashSet<>();
    for (DriverPropertyInfo info : propertyInfo) {
      names.add(info.name);
    }
    assertTrue(names.contains("SCHEMA"));
    assertTrue(names.contains("TIME_ZONE"));
    assertTrue(names.contains("MATERIALIZATIONS_ENABLED"));
  }

  /**
   * Make sure that the properties look sane.
   */
  @Test public void testVersion() throws ClassNotFoundException, SQLException {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    final DatabaseMetaData metaData = calciteConnection.getMetaData();
    assertEquals("Calcite JDBC Driver", metaData.getDriverName());

    final String driverVersion = metaData.getDriverVersion();
    final int driverMajor = metaData.getDriverMajorVersion();
    final int driverMinor = metaData.getDriverMinorVersion();
    assertEquals(1, driverMajor);
    assertTrue(driverMinor >= 0 && driverMinor < 20);

    assertEquals("Calcite", metaData.getDatabaseProductName());
    final String databaseVersion =
        metaData.getDatabaseProductVersion();
    final int databaseMajor = metaData.getDatabaseMajorVersion();
    assertEquals(driverMajor, databaseMajor);
    final int databaseMinor = metaData.getDatabaseMinorVersion();
    assertEquals(driverMinor, databaseMinor);

    // Check how version is composed of major and minor version. Note that
    // version is stored in pom.xml; major and minor version are
    // stored in org-apache-calcite-jdbc.properties, but derived from
    // version.major and version.minor in pom.xml.
    //
    // We are more permissive for snapshots.
    // For instance, we allow 1.4.0-SNAPSHOT to match {major=1, minor=3}.
    // Previously, this test would break the first build after a release.
    assertTrue(driverVersion.startsWith(driverMajor + "."));
    assertTrue(driverVersion.split("\\.").length >= 2);
    assertTrue(driverVersion.equals(mm(driverMajor, driverMinor))
        || driverVersion.startsWith(mm(driverMajor, driverMinor) + ".")
        || driverVersion.startsWith(mm(driverMajor, driverMinor) + "-")
        || driverVersion.endsWith("-SNAPSHOT")
            && driverVersion.startsWith(mm(driverMajor, driverMinor + 1)));

    assertTrue(databaseVersion.startsWith("1."));
    assertTrue(databaseVersion.split("\\.").length >= 2);
    assertTrue(databaseVersion.equals(mm(databaseMajor, databaseMinor))
        || databaseVersion.startsWith(mm(databaseMajor, databaseMinor) + ".")
        || databaseVersion.startsWith(mm(databaseMajor, databaseMinor) + "-")
        || databaseVersion.endsWith("-SNAPSHOT")
            && databaseVersion.startsWith(mm(driverMajor, driverMinor + 1)));

    connection.close();
  }

  private String mm(int majorVersion, int minorVersion) {
    return majorVersion + "." + minorVersion;
  }

  /** Tests driver's implementation of {@link DatabaseMetaData#getColumns}. */
  @Test public void testMetaDataColumns()
      throws ClassNotFoundException, SQLException {
    Connection connection = CalciteAssert
        .that(CalciteAssert.Config.REGULAR).connect();
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getColumns(null, null, null, null);
    assertTrue(resultSet.next()); // there's something
    String name = resultSet.getString(4);
    int type = resultSet.getInt(5);
    String typeName = resultSet.getString(6);
    int columnSize = resultSet.getInt(7);
    int decimalDigits = resultSet.getInt(9);
    int numPrecRadix = resultSet.getInt(10);
    int charOctetLength = resultSet.getInt(16);
    String isNullable = resultSet.getString(18);
    resultSet.close();
    connection.close();
  }

  /** Tests driver's implementation of {@link DatabaseMetaData#getPrimaryKeys}.
   * It is empty but it should still have column definitions. */
  @Test public void testMetaDataPrimaryKeys()
      throws ClassNotFoundException, SQLException {
    Connection connection = CalciteAssert
        .that(CalciteAssert.Config.REGULAR).connect();
    DatabaseMetaData metaData = connection.getMetaData();
    ResultSet resultSet = metaData.getPrimaryKeys(null, null, null);
    assertFalse(resultSet.next()); // catalog never contains primary keys
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    assertEquals(6, resultSetMetaData.getColumnCount());
    assertEquals("TABLE_CAT", resultSetMetaData.getColumnName(1));
    assertEquals(java.sql.Types.VARCHAR, resultSetMetaData.getColumnType(1));
    assertEquals("PK_NAME", resultSetMetaData.getColumnName(6));
    resultSet.close();
    connection.close();
  }

  /** Unit test for
   * {@link org.apache.calcite.jdbc.CalciteMetaImpl#likeToRegex(org.apache.calcite.avatica.Meta.Pat)}. */
  @Test public void testLikeToRegex() {
    checkLikeToRegex(true, "%", "abc");
    checkLikeToRegex(true, "abc", "abc");
    checkLikeToRegex(false, "abc", "abcd"); // trailing char fails match
    checkLikeToRegex(false, "abc", "0abc"); // leading char fails match
    checkLikeToRegex(false, "abc", "aBc"); // case-sensitive match
    checkLikeToRegex(true, "a[b]c", "a[b]c"); // nothing special about brackets
    checkLikeToRegex(true, "a$c", "a$c"); // nothing special about dollar
    checkLikeToRegex(false, "a$", "a"); // nothing special about dollar
    checkLikeToRegex(true, "a%c", "ac");
    checkLikeToRegex(true, "a%c", "abbbc");
    checkLikeToRegex(false, "a%c", "acccd");

    // escape using back-slash
    checkLikeToRegex(true, "a\\%c", "a%c");
    checkLikeToRegex(false, "a\\%c", "abc");
    checkLikeToRegex(false, "a\\%c", "a\\%c");

    // multiple wild-cards
    checkLikeToRegex(true, "a%c%d", "abcdaaad");
    checkLikeToRegex(false, "a%c%d", "abcdc");
  }

  private void checkLikeToRegex(boolean b, String pattern, String abc) {
    final Pattern regex = CalciteMetaImpl.likeToRegex(Meta.Pat.of(pattern));
    assertTrue(b == regex.matcher(abc).matches());
  }

  /** Tests driver's implementation of {@link DatabaseMetaData#getColumns},
   * and also
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1222">[CALCITE-1222]
   * DatabaseMetaData.getColumnLabel returns null when query has ORDER
   * BY</a>, */
  @Test public void testResultSetMetaData()
      throws ClassNotFoundException, SQLException {
    try (Connection connection =
             CalciteAssert.that(CalciteAssert.Config.REGULAR).connect()) {
      final String sql0 = "select \"empid\", \"deptno\" as x, 1 as y\n"
          + "from \"hr\".\"emps\"";
      checkResultSetMetaData(connection, sql0);
      final String sql1 = "select \"empid\", \"deptno\" as x, 1 as y\n"
          + "from \"hr\".\"emps\"\n"
          + "order by 1";
      checkResultSetMetaData(connection, sql1);
    }
  }

  private void checkResultSetMetaData(Connection connection, String sql)
      throws SQLException {
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      ResultSetMetaData metaData = resultSet.getMetaData();
      assertEquals(3, metaData.getColumnCount());
      assertEquals("empid", metaData.getColumnLabel(1));
      assertEquals("empid", metaData.getColumnName(1));
      assertEquals("emps", metaData.getTableName(1));
      assertEquals("X", metaData.getColumnLabel(2));
      assertEquals("deptno", metaData.getColumnName(2));
      assertEquals("emps", metaData.getTableName(2));
      assertEquals("Y", metaData.getColumnLabel(3));
      assertEquals("Y", metaData.getColumnName(3));
      assertEquals(null, metaData.getTableName(3));
    }
  }

  /** Tests some queries that have expedited processing because connection pools
   * like to use them to check whether the connection is alive.
   */
  @Test public void testSimple() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("SELECT 1")
        .returns("EXPR$0=1\n");
  }

  /** Tests accessing columns by name. */
  @Test public void testGetByName() throws Exception {
    // JDBC 3.0 specification: "Column names supplied to getter methods are case
    // insensitive. If a select list contains the same column more than once,
    // the first instance of the column will be returned."
    CalciteAssert.that()
        .doWithConnection(c -> {
          try {
            Statement s = c.createStatement();
            ResultSet rs =
                s.executeQuery(""
                    + "SELECT 1 as \"a\", 2 as \"b\", 3 as \"a\", 4 as \"B\"\n"
                    + "FROM (VALUES (0))");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("a"));
            assertEquals(1, rs.getInt("A"));
            assertEquals(2, rs.getInt("b"));
            assertEquals(2, rs.getInt("B"));
            assertEquals(1, rs.getInt(1));
            assertEquals(2, rs.getInt(2));
            assertEquals(3, rs.getInt(3));
            assertEquals(4, rs.getInt(4));
            try {
              int x = rs.getInt("z");
              fail("expected error, got " + x);
            } catch (SQLException e) {
              // ok
            }
            assertEquals(1, rs.findColumn("a"));
            assertEquals(1, rs.findColumn("A"));
            assertEquals(2, rs.findColumn("b"));
            assertEquals(2, rs.findColumn("B"));
            try {
              int x = rs.findColumn("z");
              fail("expected error, got " + x);
            } catch (SQLException e) {
              assertThat(e.getMessage(), equalTo("column 'z' not found"));
            }
            try {
              int x = rs.getInt(0);
              fail("expected error, got " + x);
            } catch (SQLException e) {
              assertThat(e.getMessage(),
                  equalTo("invalid column ordinal: 0"));
            }
            try {
              int x = rs.getInt(5);
              fail("expected error, got " + x);
            } catch (SQLException e) {
              assertThat(e.getMessage(),
                  equalTo("invalid column ordinal: 5"));
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test public void testCloneSchema()
      throws ClassNotFoundException, SQLException {
    final Connection connection =
        CalciteAssert.that(CalciteAssert.Config.JDBC_FOODMART).connect();
    final CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    final SchemaPlus rootSchema = calciteConnection.getRootSchema();
    final SchemaPlus foodmart = rootSchema.getSubSchema("foodmart");
    rootSchema.add("foodmart2", new CloneSchema(foodmart));
    Statement statement = connection.createStatement();
    ResultSet resultSet =
        statement.executeQuery(
            "select count(*) from \"foodmart2\".\"time_by_day\"");
    assertTrue(resultSet.next());
    assertEquals(730, resultSet.getInt(1));
    resultSet.close();
    connection.close();
  }

  @Test public void testCloneGroupBy() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"the_year\", count(*) as c, min(\"the_month\") as m\n"
            + "from \"foodmart2\".\"time_by_day\"\n"
            + "group by \"the_year\"\n"
            + "order by 1, 2")
        .returns(""
            + "the_year=1997; C=365; M=April\n"
            + "the_year=1998; C=365; M=April\n");
  }

  @Ignore("The test returns expected results. Not sure why it is disabled")
  @Test public void testCloneGroupBy2() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(
            "select \"time_by_day\".\"the_year\" as \"c0\", \"time_by_day\".\"quarter\" as \"c1\", \"product_class\".\"product_family\" as \"c2\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"product_class\" as \"product_class\", \"product\" as \"product\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" group by \"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\", \"product_class\".\"product_family\"")
        .returnsUnordered(
            "c0=1997; c1=Q2; c2=Drink; m0=5895.0000",
            "c0=1997; c1=Q1; c2=Food; m0=47809.0000",
            "c0=1997; c1=Q3; c2=Drink; m0=6065.0000",
            "c0=1997; c1=Q4; c2=Drink; m0=6661.0000",
            "c0=1997; c1=Q4; c2=Food; m0=51866.0000",
            "c0=1997; c1=Q1; c2=Drink; m0=5976.0000",
            "c0=1997; c1=Q3; c2=Non-Consumable; m0=12343.0000",
            "c0=1997; c1=Q4; c2=Non-Consumable; m0=13497.0000",
            "c0=1997; c1=Q2; c2=Non-Consumable; m0=11890.0000",
            "c0=1997; c1=Q2; c2=Food; m0=44825.0000",
            "c0=1997; c1=Q3; c2=Food; m0=47440.0000",
            "c0=1997; c1=Q1; c2=Non-Consumable; m0=12506.0000");
  }

  /** Tests plan for a query with 4 tables, 3 joins. */
  @Ignore("The actual and expected plan differ")
  @Test public void testCloneGroupBy2Plan() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(
            "explain plan for select \"time_by_day\".\"the_year\" as \"c0\", \"time_by_day\".\"quarter\" as \"c1\", \"product_class\".\"product_family\" as \"c2\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"product_class\" as \"product_class\", \"product\" as \"product\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" group by \"time_by_day\".\"the_year\", \"time_by_day\".\"quarter\", \"product_class\".\"product_family\"")
        .returns("PLAN=EnumerableAggregate(group=[{0, 1, 2}], m0=[SUM($3)])\n"
            + "  EnumerableCalc(expr#0..37=[{inputs}], c0=[$t9], c1=[$t13], c2=[$t4], unit_sales=[$t22])\n"
            + "    EnumerableJoin(condition=[=($23, $0)], joinType=[inner])\n"
            + "      EnumerableTableScan(table=[[foodmart2, product_class]])\n"
            + "      EnumerableJoin(condition=[=($10, $19)], joinType=[inner])\n"
            + "        EnumerableJoin(condition=[=($11, $0)], joinType=[inner])\n"
            + "          EnumerableCalc(expr#0..9=[{inputs}], expr#10=[CAST($t4):INTEGER], expr#11=[1997], expr#12=[=($t10, $t11)], proj#0..9=[{exprs}], $condition=[$t12])\n"
            + "            EnumerableTableScan(table=[[foodmart2, time_by_day]])\n"
            + "          EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])\n"
            + "        EnumerableTableScan(table=[[foodmart2, product]])\n"
            + "\n");
  }

  @Test public void testOrderByCase() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(
            "select \"time_by_day\".\"the_year\" as \"c0\" from \"time_by_day\" as \"time_by_day\" group by \"time_by_day\".\"the_year\" order by CASE WHEN \"time_by_day\".\"the_year\" IS NULL THEN 1 ELSE 0 END, \"time_by_day\".\"the_year\" ASC")
        .returns("c0=1997\n"
            + "c0=1998\n");
  }

  /** Just short of bushy. */
  @Test public void testAlmostBushy() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select *\n"
            + "from \"sales_fact_1997\" as s\n"
            + "join \"customer\" as c\n"
            + "  on s.\"customer_id\" = c.\"customer_id\"\n"
            + "join \"product\" as p\n"
            + "  on s.\"product_id\" = p.\"product_id\"\n"
            + "where c.\"city\" = 'San Francisco'\n"
            + "and p.\"brand_name\" = 'Washington'")
        .explainMatches("including all attributes ",
            CalciteAssert.checkMaskedResultContains(""
                + "EnumerableJoin(condition=[=($0, $38)], joinType=[inner]): rowcount = 7.050660528307499E8, cumulative cost = {1.0640240216183146E9 rows, 777302.0 cpu, 0.0 io}\n"
                + "  EnumerableJoin(condition=[=($2, $8)], joinType=[inner]): rowcount = 2.0087351932499997E7, cumulative cost = {2.117504719375143E7 rows, 724261.0 cpu, 0.0 io}\n"
                + "    EnumerableTableScan(table=[[foodmart2, sales_fact_1997]]): rowcount = 86837.0, cumulative cost = {86837.0 rows, 86838.0 cpu, 0.0 io}\n"
                + "    EnumerableCalc(expr#0..28=[{inputs}], expr#29=['San Francisco'], expr#30=[=($t9, $t29)], proj#0..28=[{exprs}], $condition=[$t30]): rowcount = 1542.1499999999999, cumulative cost = {11823.15 rows, 637423.0 cpu, 0.0 io}\n"
                + "      EnumerableTableScan(table=[[foodmart2, customer]]): rowcount = 10281.0, cumulative cost = {10281.0 rows, 10282.0 cpu, 0.0 io}\n"
                + "  EnumerableCalc(expr#0..14=[{inputs}], expr#15=['Washington'], expr#16=[=($t2, $t15)], proj#0..14=[{exprs}], $condition=[$t16]): rowcount = 234.0, cumulative cost = {1794.0 rows, 53041.0 cpu, 0.0 io}\n"
                + "    EnumerableTableScan(table=[[foodmart2, product]]): rowcount = 1560.0, cumulative cost = {1560.0 rows, 1561.0 cpu, 0.0 io}\n"));
  }

  /** Tests a query whose best plan is a bushy join.
   * First join sales_fact_1997 to customer;
   * in parallel join product to product_class;
   * then join the results. */
  @Ignore("extremely slow - a bit better if you disable ProjectMergeRule")
  @Test public void testBushy() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select *\n"
            + "from \"sales_fact_1997\" as s\n"
            + "  join \"customer\" as c using (\"customer_id\")\n"
            + "  join \"product\" as p using (\"product_id\")\n"
            + "  join \"product_class\" as pc using (\"product_class_id\")\n"
            + "where c.\"city\" = 'San Francisco'\n"
            + "and pc.\"product_department\" = 'Snacks'\n")
          .explainMatches("including all attributes ",
              CalciteAssert.checkMaskedResultContains(""
                  + "EnumerableCalcRel(expr#0..56=[{inputs}], expr#57=['San Francisco'], expr#58=[=($t9, $t57)], expr#59=['Snacks'], expr#60=[=($t32, $t59)], expr#61=[AND($t58, $t60)], product_id=[$t49], time_id=[$t50], customer_id=[$t51], promotion_id=[$t52], store_id=[$t53], store_sales=[$t54], store_cost=[$t55], unit_sales=[$t56], customer_id0=[$t0], account_num=[$t1], lname=[$t2], fname=[$t3], mi=[$t4], address1=[$t5], address2=[$t6], address3=[$t7], address4=[$t8], city=[$t9], state_province=[$t10], postal_code=[$t11], country=[$t12], customer_region_id=[$t13], phone1=[$t14], phone2=[$t15], birthdate=[$t16], marital_status=[$t17], yearly_income=[$t18], gender=[$t19], total_children=[$t20], num_children_at_home=[$t21], education=[$t22], date_accnt_opened=[$t23], member_card=[$t24], occupation=[$t25], houseowner=[$t26], num_cars_owned=[$t27], fullname=[$t28], product_class_id=[$t34], product_id0=[$t35], brand_name=[$t36], product_name=[$t37], SKU=[$t38], SRP=[$t39], gross_weight=[$t40], net_weight=[$t41], recyclable_package=[$t42], low_fat=[$t43], units_per_case=[$t44], cases_per_pallet=[$t45], shelf_width=[$t46], shelf_height=[$t47], shelf_depth=[$t48], product_class_id0=[$t29], product_subcategory=[$t30], product_category=[$t31], product_department=[$t32], product_family=[$t33], $condition=[$t61]): rowcount = 1953.8325, cumulative cost = {728728.1144018068 rows, 1.0519232E7 cpu, 0.0 io}\n"
                  + "  EnumerableJoinRel(condition=[=($51, $0)], joinType=[inner]): rowcount = 86837.0, cumulative cost = {726774.2819018068 rows, 98792.0 cpu, 0.0 io}\n"
                  + "    EnumerableTableScan(table=[[foodmart2, customer]]): rowcount = 10281.0, cumulative cost = {10281.0 rows, 10282.0 cpu, 0.0 io}\n"
                  + "    EnumerableJoinRel(condition=[=($5, $0)], joinType=[inner]): rowcount = 86837.0, cumulative cost = {447842.86095661717 rows, 88510.0 cpu, 0.0 io}\n"
                  + "      EnumerableTableScan(table=[[foodmart2, product_class]]): rowcount = 110.0, cumulative cost = {110.0 rows, 111.0 cpu, 0.0 io}\n"
                  + "      EnumerableJoinRel(condition=[=($15, $1)], joinType=[inner]): rowcount = 86837.0, cumulative cost = {273541.80811638 rows, 88399.0 cpu, 0.0 io}\n"
                  + "        EnumerableTableScan(table=[[foodmart2, product]]): rowcount = 1560.0, cumulative cost = {1560.0 rows, 1561.0 cpu, 0.0 io}\n"
                  + "        EnumerableTableScan(table=[[foodmart2, sales_fact_1997]]): rowcount = 86837.0, cumulative cost = {86837.0 rows, 86838.0 cpu, 0.0 io}\n"));
  }

  private static final String[] QUERIES = {
      "select count(*) from (select 1 as \"c0\" from \"salary\" as \"salary\") as \"init\"",
      "EXPR$0=21252\n",
      "select count(*) from (select 1 as \"c0\" from \"salary\" as \"salary2\") as \"init\"",
      "EXPR$0=21252\n",
      "select count(*) from (select 1 as \"c0\" from \"department\" as \"department\") as \"init\"",
      "EXPR$0=12\n",
      "select count(*) from (select 1 as \"c0\" from \"employee\" as \"employee\") as \"init\"",
      "EXPR$0=1155\n",
      "select count(*) from (select 1 as \"c0\" from \"employee_closure\" as \"employee_closure\") as \"init\"",
      "EXPR$0=7179\n",
      "select count(*) from (select 1 as \"c0\" from \"position\" as \"position\") as \"init\"",
      "EXPR$0=18\n",
      "select count(*) from (select 1 as \"c0\" from \"promotion\" as \"promotion\") as \"init\"",
      "EXPR$0=1864\n",
      "select count(*) from (select 1 as \"c0\" from \"store\" as \"store\") as \"init\"",
      "EXPR$0=25\n",
      "select count(*) from (select 1 as \"c0\" from \"product\" as \"product\") as \"init\"",
      "EXPR$0=1560\n",
      "select count(*) from (select 1 as \"c0\" from \"product_class\" as \"product_class\") as \"init\"",
      "EXPR$0=110\n",
      "select count(*) from (select 1 as \"c0\" from \"time_by_day\" as \"time_by_day\") as \"init\"",
      "EXPR$0=730\n",
      "select count(*) from (select 1 as \"c0\" from \"customer\" as \"customer\") as \"init\"",
      "EXPR$0=10281\n",
      "select count(*) from (select 1 as \"c0\" from \"sales_fact_1997\" as \"sales_fact_1997\") as \"init\"",
      "EXPR$0=86837\n",
      "select count(*) from (select 1 as \"c0\" from \"inventory_fact_1997\" as \"inventory_fact_1997\") as \"init\"",
      "EXPR$0=4070\n",
      "select count(*) from (select 1 as \"c0\" from \"warehouse\" as \"warehouse\") as \"init\"",
      "EXPR$0=24\n",
      "select count(*) from (select 1 as \"c0\" from \"agg_c_special_sales_fact_1997\" as \"agg_c_special_sales_fact_1997\") as \"init\"",
      "EXPR$0=86805\n",
      "select count(*) from (select 1 as \"c0\" from \"agg_pl_01_sales_fact_1997\" as \"agg_pl_01_sales_fact_1997\") as \"init\"",
      "EXPR$0=86829\n",
      "select count(*) from (select 1 as \"c0\" from \"agg_l_05_sales_fact_1997\" as \"agg_l_05_sales_fact_1997\") as \"init\"",
      "EXPR$0=86154\n",
      "select count(*) from (select 1 as \"c0\" from \"agg_g_ms_pcat_sales_fact_1997\" as \"agg_g_ms_pcat_sales_fact_1997\") as \"init\"",
      "EXPR$0=2637\n",
      "select count(*) from (select 1 as \"c0\" from \"agg_c_14_sales_fact_1997\" as \"agg_c_14_sales_fact_1997\") as \"init\"",
      "EXPR$0=86805\n",
      "select \"time_by_day\".\"the_year\" as \"c0\" from \"time_by_day\" as \"time_by_day\" group by \"time_by_day\".\"the_year\" order by \"time_by_day\".\"the_year\" ASC",
      "c0=1997\n"
        + "c0=1998\n",
      "select \"store\".\"store_country\" as \"c0\" from \"store\" as \"store\" where UPPER(\"store\".\"store_country\") = UPPER('USA') group by \"store\".\"store_country\" order by \"store\".\"store_country\" ASC",
      "c0=USA\n",
      "select \"store\".\"store_state\" as \"c0\" from \"store\" as \"store\" where (\"store\".\"store_country\" = 'USA') and UPPER(\"store\".\"store_state\") = UPPER('CA') group by \"store\".\"store_state\" order by \"store\".\"store_state\" ASC",
      "c0=CA\n",
      "select \"store\".\"store_city\" as \"c0\", \"store\".\"store_state\" as \"c1\" from \"store\" as \"store\" where (\"store\".\"store_state\" = 'CA' and \"store\".\"store_country\" = 'USA') and UPPER(\"store\".\"store_city\") = UPPER('Los Angeles') group by \"store\".\"store_city\", \"store\".\"store_state\" order by \"store\".\"store_city\" ASC",
      "c0=Los Angeles; c1=CA\n",
      "select \"customer\".\"country\" as \"c0\" from \"customer\" as \"customer\" where UPPER(\"customer\".\"country\") = UPPER('USA') group by \"customer\".\"country\" order by \"customer\".\"country\" ASC",
      "c0=USA\n",
      "select \"customer\".\"state_province\" as \"c0\", \"customer\".\"country\" as \"c1\" from \"customer\" as \"customer\" where (\"customer\".\"country\" = 'USA') and UPPER(\"customer\".\"state_province\") = UPPER('CA') group by \"customer\".\"state_province\", \"customer\".\"country\" order by \"customer\".\"state_province\" ASC",
      "c0=CA; c1=USA\n",
      "select \"customer\".\"city\" as \"c0\", \"customer\".\"country\" as \"c1\", \"customer\".\"state_province\" as \"c2\" from \"customer\" as \"customer\" where (\"customer\".\"country\" = 'USA' and \"customer\".\"state_province\" = 'CA' and \"customer\".\"country\" = 'USA' and \"customer\".\"state_province\" = 'CA' and \"customer\".\"country\" = 'USA') and UPPER(\"customer\".\"city\") = UPPER('Los Angeles') group by \"customer\".\"city\", \"customer\".\"country\", \"customer\".\"state_province\" order by \"customer\".\"city\" ASC",
      "c0=Los Angeles; c1=USA; c2=CA\n",
      "select \"store\".\"store_country\" as \"c0\" from \"store\" as \"store\" where UPPER(\"store\".\"store_country\") = UPPER('Gender') group by \"store\".\"store_country\" order by \"store\".\"store_country\" ASC",
      "",
      "select \"store\".\"store_type\" as \"c0\" from \"store\" as \"store\" where UPPER(\"store\".\"store_type\") = UPPER('Gender') group by \"store\".\"store_type\" order by \"store\".\"store_type\" ASC",
      "",
      "select \"product_class\".\"product_family\" as \"c0\" from \"product\" as \"product\", \"product_class\" as \"product_class\" where \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and UPPER(\"product_class\".\"product_family\") = UPPER('Gender') group by \"product_class\".\"product_family\" order by \"product_class\".\"product_family\" ASC",
      "",
      "select \"promotion\".\"media_type\" as \"c0\" from \"promotion\" as \"promotion\" where UPPER(\"promotion\".\"media_type\") = UPPER('Gender') group by \"promotion\".\"media_type\" order by \"promotion\".\"media_type\" ASC",
      "",
      "select \"promotion\".\"promotion_name\" as \"c0\" from \"promotion\" as \"promotion\" where UPPER(\"promotion\".\"promotion_name\") = UPPER('Gender') group by \"promotion\".\"promotion_name\" order by \"promotion\".\"promotion_name\" ASC",
      "",
      "select \"promotion\".\"media_type\" as \"c0\" from \"promotion\" as \"promotion\" where UPPER(\"promotion\".\"media_type\") = UPPER('No Media') group by \"promotion\".\"media_type\" order by \"promotion\".\"media_type\" ASC",
      "c0=No Media\n",
      "select \"promotion\".\"media_type\" as \"c0\" from \"promotion\" as \"promotion\" group by \"promotion\".\"media_type\" order by \"promotion\".\"media_type\" ASC",
      "c0=Bulk Mail\n"
        + "c0=Cash Register Handout\n"
        + "c0=Daily Paper\n"
        + "c0=Daily Paper, Radio\n"
        + "c0=Daily Paper, Radio, TV\n"
        + "c0=In-Store Coupon\n"
        + "c0=No Media\n"
        + "c0=Product Attachment\n"
        + "c0=Radio\n"
        + "c0=Street Handout\n"
        + "c0=Sunday Paper\n"
        + "c0=Sunday Paper, Radio\n"
        + "c0=Sunday Paper, Radio, TV\n"
        + "c0=TV\n",
      "select count(distinct \"the_year\") from \"time_by_day\"",
      "EXPR$0=2\n",
      "select \"time_by_day\".\"the_year\" as \"c0\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 group by \"time_by_day\".\"the_year\"",
      "c0=1997; m0=266773.0000\n",
      "select \"time_by_day\".\"the_year\" as \"c0\", \"promotion\".\"media_type\" as \"c1\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"promotion\" as \"promotion\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\" group by \"time_by_day\".\"the_year\", \"promotion\".\"media_type\"",
      "c0=1997; c1=Bulk Mail; m0=4320.0000\n"
        + "c0=1997; c1=Radio; m0=2454.0000\n"
        + "c0=1997; c1=Street Handout; m0=5753.0000\n"
        + "c0=1997; c1=TV; m0=3607.0000\n"
        + "c0=1997; c1=No Media; m0=195448.0000\n"
        + "c0=1997; c1=In-Store Coupon; m0=3798.0000\n"
        + "c0=1997; c1=Sunday Paper, Radio, TV; m0=2726.0000\n"
        + "c0=1997; c1=Product Attachment; m0=7544.0000\n"
        + "c0=1997; c1=Daily Paper; m0=7738.0000\n"
        + "c0=1997; c1=Cash Register Handout; m0=6697.0000\n"
        + "c0=1997; c1=Daily Paper, Radio; m0=6891.0000\n"
        + "c0=1997; c1=Daily Paper, Radio, TV; m0=9513.0000\n"
        + "c0=1997; c1=Sunday Paper, Radio; m0=5945.0000\n"
        + "c0=1997; c1=Sunday Paper; m0=4339.0000\n",
      "select \"store\".\"store_country\" as \"c0\", sum(\"inventory_fact_1997\".\"supply_time\") as \"m0\" from \"store\" as \"store\", \"inventory_fact_1997\" as \"inventory_fact_1997\" where \"inventory_fact_1997\".\"store_id\" = \"store\".\"store_id\" group by \"store\".\"store_country\"",
      "c0=USA; m0=10425\n",
      "select \"sn\".\"desc\" as \"c0\" from (SELECT * FROM (VALUES (1, 'SameName')) AS \"t\" (\"id\", \"desc\")) as \"sn\" group by \"sn\".\"desc\" order by \"sn\".\"desc\" ASC NULLS LAST",
      "c0=SameName\n",
      "select \"the_year\", count(*) as c, min(\"the_month\") as m\n"
        + "from \"foodmart2\".\"time_by_day\"\n"
        + "group by \"the_year\"\n"
        + "order by 1, 2",
      "the_year=1997; C=365; M=April\n"
        + "the_year=1998; C=365; M=April\n",
      "select\n"
        + " \"store\".\"store_state\" as \"c0\",\n"
        + " \"time_by_day\".\"the_year\" as \"c1\",\n"
        + " sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\",\n"
        + " sum(\"sales_fact_1997\".\"store_sales\") as \"m1\"\n"
        + "from \"store\" as \"store\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"time_by_day\" as \"time_by_day\"\n"
        + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
        + "and \"store\".\"store_state\" in ('DF', 'WA')\n"
        + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "group by \"store\".\"store_state\", \"time_by_day\".\"the_year\"",
      "c0=WA; c1=1997; m0=124366.0000; m1=263793.2200\n",
      "select count(distinct \"product_id\") from \"product\"",
      "EXPR$0=1560\n",
      "select \"store\".\"store_name\" as \"c0\",\n"
        + " \"time_by_day\".\"the_year\" as \"c1\",\n"
        + " sum(\"sales_fact_1997\".\"store_sales\") as \"m0\"\n"
        + "from \"store\" as \"store\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"time_by_day\" as \"time_by_day\"\n"
        + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
        + "and \"store\".\"store_name\" in ('Store 1', 'Store 10', 'Store 11', 'Store 15', 'Store 16', 'Store 24', 'Store 3', 'Store 7')\n"
        + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "group by \"store\".\"store_name\",\n"
        + " \"time_by_day\".\"the_year\"\n",
      "c0=Store 7; c1=1997; m0=54545.2800\n"
        + "c0=Store 24; c1=1997; m0=54431.1400\n"
        + "c0=Store 16; c1=1997; m0=49634.4600\n"
        + "c0=Store 3; c1=1997; m0=52896.3000\n"
        + "c0=Store 15; c1=1997; m0=52644.0700\n"
        + "c0=Store 11; c1=1997; m0=55058.7900\n",
      "select \"customer\".\"yearly_income\" as \"c0\","
        + " \"customer\".\"education\" as \"c1\"\n"
        + "from \"customer\" as \"customer\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\"\n"
        + "where \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
        + " and ((not (\"customer\".\"yearly_income\" in ('$10K - $30K', '$50K - $70K'))\n"
        + " or (\"customer\".\"yearly_income\" is null)))\n"
        + "group by \"customer\".\"yearly_income\",\n"
        + " \"customer\".\"education\"\n"
        + "order by \"customer\".\"yearly_income\" ASC NULLS LAST,\n"
        + " \"customer\".\"education\" ASC NULLS LAST",
      "c0=$110K - $130K; c1=Bachelors Degree\n"
        + "c0=$110K - $130K; c1=Graduate Degree\n"
        + "c0=$110K - $130K; c1=High School Degree\n"
        + "c0=$110K - $130K; c1=Partial College\n"
        + "c0=$110K - $130K; c1=Partial High School\n"
        + "c0=$130K - $150K; c1=Bachelors Degree\n"
        + "c0=$130K - $150K; c1=Graduate Degree\n"
        + "c0=$130K - $150K; c1=High School Degree\n"
        + "c0=$130K - $150K; c1=Partial College\n"
        + "c0=$130K - $150K; c1=Partial High School\n"
        + "c0=$150K +; c1=Bachelors Degree\n"
        + "c0=$150K +; c1=Graduate Degree\n"
        + "c0=$150K +; c1=High School Degree\n"
        + "c0=$150K +; c1=Partial College\n"
        + "c0=$150K +; c1=Partial High School\n"
        + "c0=$30K - $50K; c1=Bachelors Degree\n"
        + "c0=$30K - $50K; c1=Graduate Degree\n"
        + "c0=$30K - $50K; c1=High School Degree\n"
        + "c0=$30K - $50K; c1=Partial College\n"
        + "c0=$30K - $50K; c1=Partial High School\n"
        + "c0=$70K - $90K; c1=Bachelors Degree\n"
        + "c0=$70K - $90K; c1=Graduate Degree\n"
        + "c0=$70K - $90K; c1=High School Degree\n"
        + "c0=$70K - $90K; c1=Partial College\n"
        + "c0=$70K - $90K; c1=Partial High School\n"
        + "c0=$90K - $110K; c1=Bachelors Degree\n"
        + "c0=$90K - $110K; c1=Graduate Degree\n"
        + "c0=$90K - $110K; c1=High School Degree\n"
        + "c0=$90K - $110K; c1=Partial College\n"
        + "c0=$90K - $110K; c1=Partial High School\n",
      "ignore:select \"time_by_day\".\"the_year\" as \"c0\", \"product_class\".\"product_family\" as \"c1\", \"customer\".\"state_province\" as \"c2\", \"customer\".\"city\" as \"c3\", sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\" from \"time_by_day\" as \"time_by_day\", \"sales_fact_1997\" as \"sales_fact_1997\", \"product_class\" as \"product_class\", \"product\" as \"product\", \"customer\" as \"customer\" where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\" and \"time_by_day\".\"the_year\" = 1997 and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\" and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\" and \"product_class\".\"product_family\" = 'Drink' and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\" and \"customer\".\"state_province\" = 'WA' and \"customer\".\"city\" in ('Anacortes', 'Ballard', 'Bellingham', 'Bremerton', 'Burien', 'Edmonds', 'Everett', 'Issaquah', 'Kirkland', 'Lynnwood', 'Marysville', 'Olympia', 'Port Orchard', 'Puyallup', 'Redmond', 'Renton', 'Seattle', 'Sedro Woolley', 'Spokane', 'Tacoma', 'Walla Walla', 'Yakima') group by \"time_by_day\".\"the_year\", \"product_class\".\"product_family\", \"customer\".\"state_province\", \"customer\".\"city\"",
      "c0=1997; c1=Drink; c2=WA; c3=Sedro Woolley; m0=58.0000\n",
      "select \"store\".\"store_country\" as \"c0\",\n"
        + " \"time_by_day\".\"the_year\" as \"c1\",\n"
        + " sum(\"sales_fact_1997\".\"store_cost\") as \"m0\",\n"
        + " count(\"sales_fact_1997\".\"product_id\") as \"m1\",\n"
        + " count(distinct \"sales_fact_1997\".\"customer_id\") as \"m2\",\n"
        + " sum((case when \"sales_fact_1997\".\"promotion_id\" = 0 then 0\n"
        + "     else \"sales_fact_1997\".\"store_sales\" end)) as \"m3\"\n"
        + "from \"store\" as \"store\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"time_by_day\" as \"time_by_day\"\n"
        + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
        + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "group by \"store\".\"store_country\", \"time_by_day\".\"the_year\"",
      "c0=USA; c1=1997; m0=225627.2336; m1=86837; m2=5581; m3=151211.2100\n",
      // query 6077
      // disabled (runs out of memory)
      "ignore:select \"time_by_day\".\"the_year\" as \"c0\",\n"
        + " count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
        + "from \"time_by_day\" as \"time_by_day\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"product_class\" as \"product_class\",\n"
        + " \"product\" as \"product\"\n"
        + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
        + "and (((\"product\".\"brand_name\" = 'Cormorant'\n"
        + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
        + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
        + "   and \"product_class\".\"product_department\" = 'Household'\n"
        + "   and \"product_class\".\"product_family\" = 'Non-Consumable')\n"
        + " or (\"product\".\"brand_name\" = 'Denny'\n"
        + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
        + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
        + "   and \"product_class\".\"product_department\" = 'Household'\n"
        + "   and \"product_class\".\"product_family\" = 'Non-Consumable')\n"
        + " or (\"product\".\"brand_name\" = 'High Quality'\n"
        + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
        + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
        + "   and \"product_class\".\"product_department\" = 'Household'\n"
        + "   and \"product_class\".\"product_family\" = 'Non-Consumable')\n"
        + " or (\"product\".\"brand_name\" = 'Red Wing'\n"
        + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers'\n"
        + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
        + "   and \"product_class\".\"product_department\" = 'Household'\n"
        + "   and \"product_class\".\"product_family\" = 'Non-Consumable'))\n"
        + " or (\"product_class\".\"product_subcategory\" = 'Pots and Pans'\n"
        + "   and \"product_class\".\"product_category\" = 'Kitchen Products'\n"
        + "   and \"product_class\".\"product_department\" = 'Household'\n"
        + "   and \"product_class\".\"product_family\" = 'Non-Consumable'))\n"
        + "group by \"time_by_day\".\"the_year\"\n",
      "xxtodo",
      // query 6077, simplified
      // disabled (slow)
      "ignore:select count(\"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
        + "from \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"product_class\" as \"product_class\",\n"
        + " \"product\" as \"product\"\n"
        + "where \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
        + "and ((\"product\".\"brand_name\" = 'Cormorant'\n"
        + "   and \"product_class\".\"product_subcategory\" = 'Pot Scrubbers')\n"
        + " or (\"product_class\".\"product_subcategory\" = 'Pots and Pans'))\n",
      "xxxx",
      // query 6077, simplified further
      "select count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
        + "from \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"product_class\" as \"product_class\",\n"
        + " \"product\" as \"product\"\n"
        + "where \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
        + "and \"product\".\"brand_name\" = 'Cormorant'\n",
      "m0=1298",
      // query 193
      "select \"store\".\"store_country\" as \"c0\",\n"
        + " \"time_by_day\".\"the_year\" as \"c1\",\n"
        + " \"time_by_day\".\"quarter\" as \"c2\",\n"
        + " \"product_class\".\"product_family\" as \"c3\",\n"
        + " count(\"sales_fact_1997\".\"product_id\") as \"m0\",\n"
        + " count(distinct \"sales_fact_1997\".\"customer_id\") as \"m1\"\n"
        + "from \"store\" as \"store\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
        + " \"time_by_day\" as \"time_by_day\",\n"
        + " \"product_class\" as \"product_class\",\n"
        + " \"product\" as \"product\"\n"
        + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
        + "and \"store\".\"store_country\" = 'USA'\n"
        + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "and \"time_by_day\".\"quarter\" = 'Q3'\n"
        + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
        + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
        + "and \"product_class\".\"product_family\" = 'Food'\n"
        + "group by \"store\".\"store_country\",\n"
        + " \"time_by_day\".\"the_year\",\n"
        + " \"time_by_day\".\"quarter\",\n"
        + " \"product_class\".\"product_family\"",
      "c0=USA; c1=1997; c2=Q3; c3=Food; m0=15449; m1=2939",
  };

  public static final List<Pair<String, String>> FOODMART_QUERIES =
      querify(QUERIES);

  /** Janino bug
   * <a href="https://jira.codehaus.org/browse/JANINO-169">[JANINO-169]</a>
   * running queries against the JDBC adapter. The bug is not present with
   * janino-3.0.9 so the workaround in EnumerableRelImplementor was removed.
   */
  @Test public void testJanino169() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query(
            "select \"time_id\" from \"foodmart\".\"time_by_day\" as \"t\"\n")
        .returnsCount(730);
  }

  /** Tests 3-way AND.
   *
   * <p>With
   * <a href="https://issues.apache.org/jira/browse/CALCITE-127">[CALCITE-127]
   * EnumerableCalcRel can't support 3+ AND conditions</a>, the last condition
   * is ignored and rows with deptno=10 are wrongly returned.</p>
   */
  @Test public void testAnd3() {
    CalciteAssert.hr()
        .query("select \"deptno\" from \"hr\".\"emps\"\n"
            + "where \"emps\".\"empid\" < 240\n"
            + "and \"salary\" > 7500.0"
            + "and \"emps\".\"deptno\" > 10\n")
        .returnsUnordered("deptno=20");
  }

  /** Tests a date literal against a JDBC data source. */
  @Test public void testJdbcDate() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select count(*) as c from (\n"
            + "  select 1 from \"foodmart\".\"employee\" as e1\n"
            + "  where \"position_title\" = 'VP Country Manager'\n"
            + "  and \"birth_date\" < DATE '1950-01-01'\n"
            + "  and \"gender\" = 'F')")
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.ORACLE)
        .returns2("C=1\n");
  }

  /** Tests a timestamp literal against JDBC data source. */
  @Test public void testJdbcTimestamp() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select count(*) as c from (\n"
            + "  select 1 from \"foodmart\".\"employee\" as e1\n"
            + "  where \"hire_date\" < TIMESTAMP '1996-06-05 00:00:00'\n"
            + "  and \"gender\" = 'F')")
        .returns("C=287\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-281">[CALCITE-281]
   * SQL type of EXTRACT is BIGINT but it is implemented as int</a>. */
  @Test public void testExtract() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("values extract(year from date '2008-2-23')")
        .returns(resultSet -> {
          // The following behavior is not quite correct. See
          //   [CALCITE-508] Reading from ResultSet before calling next()
          //   should throw SQLException not NoSuchElementException
          // for details.
          try {
            final BigDecimal bigDecimal = resultSet.getBigDecimal(1);
            fail("expected error, got " + bigDecimal);
          } catch (SQLException e) {
            assertThat(e.getMessage(),
                is("java.util.NoSuchElementException: Expecting cursor "
                    + "position to be Position.OK, actual "
                    + "is Position.BEFORE_START"));
          }
          try {
            assertTrue(resultSet.next());
            final BigDecimal bigDecimal = resultSet.getBigDecimal(1);
            assertThat(bigDecimal, equalTo(BigDecimal.valueOf(2008)));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test public void testExtractMonthFromTimestamp() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select extract(month from \"birth_date\") as c \n"
            + "from \"foodmart\".\"employee\" where \"employee_id\"=1")
        .returns("C=8\n");
  }

  @Test public void testExtractYearFromTimestamp() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select extract(year from \"birth_date\") as c \n"
            + "from \"foodmart\".\"employee\" where \"employee_id\"=1")
        .returns("C=1961\n");
  }

  @Test public void testExtractFromInterval() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select extract(month from interval '2-3' year to month) as c \n"
            + "from \"foodmart\".\"employee\" where \"employee_id\"=1")
        // disable for MySQL, H2; cannot handle EXTRACT yet
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.MYSQL
            && CalciteAssert.DB != CalciteAssert.DatabaseInstance.H2)
        .returns("C=3\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1188">[CALCITE-1188]
   * NullPointerException when EXTRACT is applied to NULL date field</a>.
   * The problem occurs when EXTRACT appears in both SELECT and WHERE ... IN
   * clauses, the latter with at least two values. */
  @Test public void testExtractOnNullDateField() {
    final String sql = "select\n"
        + "  extract(year from \"end_date\"), \"hire_date\", \"birth_date\"\n"
        + "from \"foodmart\".\"employee\"\n"
        + "where extract(year from \"end_date\") in (1994, 1995, 1996)\n"
        + "group by\n"
        + "  extract(year from \"end_date\"), \"hire_date\", \"birth_date\"\n";
    final String sql2 = sql + "\n"
        + "limit 10000";
    final String sql3 = "select *\n"
        + "from \"foodmart\".\"employee\"\n"
        + "where extract(year from \"end_date\") in (1994, 1995, 1996)";
    final CalciteAssert.AssertThat with = CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE);
    with.query(sql).returns("");
    with.query(sql2).returns("");
    with.query(sql3).returns("");
  }

  @Test public void testFloorDate() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select floor(timestamp '2011-9-14 19:27:23' to month) as c \n"
            + "from \"foodmart\".\"employee\" limit 1")
        // disable for MySQL; birth_date suffers timezone shift
        // disable for H2; Calcite generates incorrect FLOOR syntax
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.MYSQL
            && CalciteAssert.DB != CalciteAssert.DatabaseInstance.H2)
        .returns("C=2011-09-01 00:00:00\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-387">[CALCITE-387]
   * CompileException when cast TRUE to nullable boolean</a>. */
  @Test public void testTrue() {
    final CalciteAssert.AssertThat that = CalciteAssert.that();
    that.query("select case when deptno = 10 then null else true end as x\n"
        + "from (values (10), (20)) as t(deptno)")
        .returnsUnordered("X=null", "X=true");
    that.query("select case when deptno = 10 then null else 100 end as x\n"
        + "from (values (10), (20)) as t(deptno)")
        .returnsUnordered("X=null", "X=100");
    that.query("select case when deptno = 10 then null else 'xy' end as x\n"
        + "from (values (10), (20)) as t(deptno)")
        .returnsUnordered("X=null", "X=xy");
  }

  /** Unit test for self-join. Left and right children of the join are the same
   * relational expression. */
  @Test public void testSelfJoin() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select count(*) as c from (\n"
            + "  select 1 from \"foodmart\".\"employee\" as e1\n"
            + "  join \"foodmart\".\"employee\" as e2 using (\"position_title\"))")
        .returns("C=247149\n");
  }

  /** Self-join on different columns, select a different column, and sort and
   * limit on yet another column. */
  @Test public void testSelfJoinDifferentColumns() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select e1.\"full_name\"\n"
            + "  from \"foodmart\".\"employee\" as e1\n"
            + "  join \"foodmart\".\"employee\" as e2 on e1.\"first_name\" = e2.\"last_name\"\n"
            + "order by e1.\"last_name\" limit 3")
        // disable for H2; gives "Unexpected code path" internal error
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.H2)
        .returns("full_name=James Aguilar\n"
            + "full_name=Carol Amyotte\n"
            + "full_name=Terry Anderson\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2029">[CALCITE-2029]
   * Query with "is distinct from" condition in where or join clause fails
   * with AssertionError: Cast for just nullability not allowed</a>. */
  @Test public void testIsNotDistinctInFilter() {
    CalciteAssert.that()
      .with(CalciteAssert.Config.JDBC_FOODMART)
      .query("select *\n"
          + "  from \"foodmart\".\"employee\" as e1\n"
          + "  where e1.\"last_name\" is distinct from e1.\"last_name\"")
      .runs();
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2029">[CALCITE-2029]
   * Query with "is distinct from" condition in where or join clause fails
   * with AssertionError: Cast for just nullability not allowed</a>. */
  @Test public void testMixedEqualAndIsNotDistinctJoin() {
    CalciteAssert.that()
      .with(CalciteAssert.Config.JDBC_FOODMART)
      .query("select *\n"
          + "  from \"foodmart\".\"employee\" as e1\n"
          + "  join \"foodmart\".\"employee\" as e2 on\n"
          + "  e1.\"first_name\" = e1.\"first_name\"\n"
          + "  and e1.\"last_name\" is distinct from e2.\"last_name\"")
      .runs();
  }

  /** A join that has both equi and non-equi conditions.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-371">[CALCITE-371]
   * Cannot implement JOIN whose ON clause contains mixed equi and theta</a>. */
  @Test public void testEquiThetaJoin() {
    CalciteAssert.hr()
        .query("select e.\"empid\", d.\"name\", e.\"name\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "join \"hr\".\"depts\" as d\n"
            + "on e.\"deptno\" = d.\"deptno\"\n"
            + "and e.\"name\" <> d.\"name\"\n")
        .returns("empid=100; name=Sales; name=Bill\n"
            + "empid=150; name=Sales; name=Sebastian\n"
            + "empid=110; name=Sales; name=Theodore\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-451">[CALCITE-451]
   * Implement theta join, inner and outer, in enumerable convention</a>. */
  @Test public void testThetaJoin() {
    CalciteAssert.hr()
        .query(
            "select e.\"empid\", d.\"name\", e.\"name\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "left join \"hr\".\"depts\" as d\n"
            + "on e.\"deptno\" < d.\"deptno\"\n")
        .returnsUnordered("empid=100; name=Marketing; name=Bill",
            "empid=100; name=HR; name=Bill",
            "empid=200; name=Marketing; name=Eric",
            "empid=200; name=HR; name=Eric",
            "empid=150; name=Marketing; name=Sebastian",
            "empid=150; name=HR; name=Sebastian",
            "empid=110; name=Marketing; name=Theodore",
            "empid=110; name=HR; name=Theodore");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-35">[CALCITE-35]
   * Support parenthesized sub-clause in JOIN</a>. */
  @Ignore
  @Test public void testJoinJoin() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select\n"
            + "   \"product_class\".\"product_family\" as \"c0\",\n"
            + "   \"product_class\".\"product_department\" as \"c1\",\n"
            + "   \"customer\".\"country\" as \"c2\",\n"
            + "   \"customer\".\"state_province\" as \"c3\",\n"
            + "   \"customer\".\"city\" as \"c4\"\n"
            + "from\n"
            + "   \"sales_fact_1997\" as \"sales_fact_1997\"\n"
            + "join (\"product\" as \"product\"\n"
            + "     join \"product_class\" as \"product_class\"\n"
            + "     on \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\")\n"
            + "on  \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "join \"customer\" as \"customer\"\n"
            + "on  \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
            + "join \"promotion\" as \"promotion\"\n"
            + "on \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
            + "where (\"promotion\".\"media_type\" = 'Radio'\n"
            + " or \"promotion\".\"media_type\" = 'TV'\n"
            + " or \"promotion\".\"media_type\" = 'Sunday Paper'\n"
            + " or \"promotion\".\"media_type\" = 'Street Handout')\n"
            + " and (\"product_class\".\"product_family\" = 'Drink')\n"
            + " and (\"customer\".\"country\" = 'USA' and \"customer\".\"state_province\""
            + " = 'WA' and \"customer\".\"city\" = 'Bellingham')\n"
            + "group by \"product_class\".\"product_family\",\n"
            + "   \"product_class\".\"product_department\",\n"
            + "   \"customer\".\"country\",\n"
            + "   \"customer\".\"state_province\",\n"
            + "   \"customer\".\"city\"\n"
            + "order by ISNULL(\"product_class\".\"product_family\") ASC,   \"product_class\".\"product_family\" ASC,\n"
            + "   ISNULL(\"product_class\".\"product_department\") ASC,   \"product_class\".\"product_department\" ASC,\n"
            + "   ISNULL(\"customer\".\"country\") ASC,   \"customer\".\"country\" ASC,\n"
            + "   ISNULL(\"customer\".\"state_province\") ASC,   \"customer\".\"state_province\" ASC,\n"
            + "   ISNULL(\"customer\".\"city\") ASC,   \"customer\".\"city\" ASC")
        .returns("+-------+---------------------+-----+------+------------+\n"
            + "| c0    | c1                  | c2  | c3   | c4         |\n"
            + "+-------+---------------------+-----+------+------------+\n"
            + "| Drink | Alcoholic Beverages | USA | WA   | Bellingham |\n"
            + "| Drink | Dairy               | USA | WA   | Bellingham |\n"
            + "+-------+---------------------+-----+------+------------+");
  }

  /** Four-way join. Used to take 80 seconds. */
  @Ignore
  @Test public void testJoinFiveWay() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"store\".\"store_country\" as \"c0\",\n"
            + " \"time_by_day\".\"the_year\" as \"c1\",\n"
            + " \"product_class\".\"product_family\" as \"c2\",\n"
            + " count(\"sales_fact_1997\".\"product_id\") as \"m0\"\n"
            + "from \"store\" as \"store\",\n"
            + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
            + " \"time_by_day\" as \"time_by_day\",\n"
            + " \"product_class\" as \"product_class\",\n"
            + " \"product\" as \"product\"\n"
            + "where \"sales_fact_1997\".\"store_id\" = \"store\".\"store_id\"\n"
            + "and \"store\".\"store_country\" = 'USA'\n"
            + "and \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
            + "and \"time_by_day\".\"the_year\" = 1997\n"
            + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "group by \"store\".\"store_country\",\n"
            + " \"time_by_day\".\"the_year\",\n"
            + " \"product_class\".\"product_family\"")
        .explainContains(""
            + "EnumerableAggregateRel(group=[{0, 1, 2}], m0=[COUNT($3)])\n"
            + "  EnumerableCalcRel(expr#0..61=[{inputs}], c0=[$t19], c1=[$t4], c2=[$t46], product_id=[$t34])\n"
            + "    EnumerableJoinRel(condition=[=($35, $0)], joinType=[inner])\n"
            + "      EnumerableCalcRel(expr#0..9=[{inputs}], expr#10=[CAST($t4):INTEGER], expr#11=[1997], expr#12=[=($t10, $t11)], proj#0..9=[{exprs}], $condition=[$t12])\n"
            + "        EnumerableTableScan(table=[[foodmart2, time_by_day]])\n"
            + "      EnumerableCalcRel(expr#0..51=[{inputs}], proj#0..23=[{exprs}], product_id=[$t44], time_id=[$t45], customer_id=[$t46], promotion_id=[$t47], store_id0=[$t48], store_sales=[$t49], store_cost=[$t50], unit_sales=[$t51], product_class_id=[$t24], product_subcategory=[$t25], product_category=[$t26], product_department=[$t27], product_family=[$t28], product_class_id0=[$t29], product_id0=[$t30], brand_name=[$t31], product_name=[$t32], SKU=[$t33], SRP=[$t34], gross_weight=[$t35], net_weight=[$t36], recyclable_package=[$t37], low_fat=[$t38], units_per_case=[$t39], cases_per_pallet=[$t40], shelf_width=[$t41], shelf_height=[$t42], shelf_depth=[$t43])\n"
            + "        EnumerableJoinRel(condition=[=($48, $0)], joinType=[inner])\n"
            + "          EnumerableCalcRel(expr#0..23=[{inputs}], expr#24=['USA'], expr#25=[=($t9, $t24)], proj#0..23=[{exprs}], $condition=[$t25])\n"
            + "            EnumerableTableScan(table=[[foodmart2, store]])\n"
            + "          EnumerableCalcRel(expr#0..27=[{inputs}], proj#0..4=[{exprs}], product_class_id0=[$t13], product_id=[$t14], brand_name=[$t15], product_name=[$t16], SKU=[$t17], SRP=[$t18], gross_weight=[$t19], net_weight=[$t20], recyclable_package=[$t21], low_fat=[$t22], units_per_case=[$t23], cases_per_pallet=[$t24], shelf_width=[$t25], shelf_height=[$t26], shelf_depth=[$t27], product_id0=[$t5], time_id=[$t6], customer_id=[$t7], promotion_id=[$t8], store_id=[$t9], store_sales=[$t10], store_cost=[$t11], unit_sales=[$t12])\n"
            + "            EnumerableJoinRel(condition=[=($13, $0)], joinType=[inner])\n"
            + "              EnumerableTableScan(table=[[foodmart2, product_class]])\n"
            + "              EnumerableJoinRel(condition=[=($0, $9)], joinType=[inner])\n"
            + "                EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])\n"
            + "                EnumerableTableScan(table=[[foodmart2, product]])\n"
            + "\n"
            + "]>")
        .returns("+-------+---------------------+-----+------+------------+\n"
            + "| c0    | c1                  | c2  | c3   | c4         |\n"
            + "+-------+---------------------+-----+------+------------+\n"
            + "| Drink | Alcoholic Beverages | USA | WA   | Bellingham |\n"
            + "| Drink | Dairy               | USA | WA   | Bellingham |\n"
            + "+-------+---------------------+-----+------+------------+");
  }

  /** Tests a simple (primary key to primary key) N-way join, with arbitrary
   * N. */
  @Test public void testJoinManyWay() {
    // Timings without LoptOptimizeJoinRule
    //    N  Time
    //   == =====
    //    6     2
    //   10    10
    //   11    19
    //   12    36
    //   13   116 - OOM did not complete
    checkJoinNWay(1);
    checkJoinNWay(3);
    checkJoinNWay(6);
  }

  private static void checkJoinNWay(int n) {
    assert n > 0;
    final StringBuilder buf = new StringBuilder();
    buf.append("select count(*)");
    for (int i = 0; i < n; i++) {
      buf.append(i == 0 ? "\nfrom " : ",\n")
          .append("\"hr\".\"depts\" as d").append(i);
    }
    for (int i = 1; i < n; i++) {
      buf.append(i == 1 ? "\nwhere" : "\nand").append(" d")
          .append(i).append(".\"deptno\" = d")
          .append(i - 1).append(".\"deptno\"");
    }
    CalciteAssert.hr()
        .query(buf.toString())
        .returns("EXPR$0=3\n");
  }

  /** Returns a list of (query, expected) pairs. The expected result is
   * sometimes null. */
  private static List<Pair<String, String>> querify(String[] queries1) {
    final List<Pair<String, String>> list = new ArrayList<>();
    for (int i = 0; i < queries1.length; i++) {
      String query = queries1[i];
      String expected = null;
      if (i + 1 < queries1.length
          && queries1[i + 1] != null
          && !queries1[i + 1].startsWith("select")) {
        expected = queries1[++i];
      }
      list.add(Pair.of(query, expected));
    }
    return list;
  }

  /** A selection of queries generated by Mondrian. */
  @Ignore
  @Test public void testCloneQueries() {
    CalciteAssert.AssertThat with =
        CalciteAssert.that()
            .with(CalciteAssert.Config.FOODMART_CLONE);
    for (Ord<Pair<String, String>> query : Ord.zip(FOODMART_QUERIES)) {
      try {
        // uncomment to run specific queries:
//      if (query.i != FOODMART_QUERIES.size() - 1) continue;
        final String sql = query.e.left;
        if (sql.startsWith("ignore:")) {
          continue;
        }
        final String expected = query.e.right;
        final CalciteAssert.AssertQuery query1 = with.query(sql);
        if (expected != null) {
          if (sql.contains("order by")) {
            query1.returns(expected);
          } else {
            query1.returnsUnordered(expected.split("\n"));
          }
        } else {
          query1.runs();
        }
      } catch (Throwable e) {
        throw new RuntimeException("while running query #" + query.i, e);
      }
    }
  }

  /** Tests accessing a column in a JDBC source whose type is ARRAY. */
  @Test public void testArray() throws Exception {
    final String url = MultiJdbcSchemaJoinTest.TempDb.INSTANCE.getUrl();
    Connection baseConnection = DriverManager.getConnection(url);
    Statement baseStmt = baseConnection.createStatement();
    baseStmt.execute("CREATE TABLE ARR_TABLE (\n"
        + "ID INTEGER,\n"
        + "VALS INTEGER ARRAY)");
    baseStmt.execute("INSERT INTO ARR_TABLE VALUES (1, ARRAY[1,2,3])");
    baseStmt.execute("CREATE TABLE ARR_TABLE2 (\n"
        + "ID INTEGER,\n"
        + "VALS INTEGER ARRAY,\n"
        + "VALVALS VARCHAR(10) ARRAY)");
    baseStmt.execute(
        "INSERT INTO ARR_TABLE2 VALUES (1, ARRAY[1,2,3], ARRAY['x','y'])");
    baseStmt.close();
    baseConnection.commit();

    Properties info = new Properties();
    info.put("model",
        "inline:"
            + "{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'BASEJDBC',\n"
            + "  schemas: [\n"
            + "     {\n"
            + "       type: 'jdbc',\n"
            + "       name: 'BASEJDBC',\n"
            + "       jdbcDriver: '" + jdbcDriver.class.getName() + "',\n"
            + "       jdbcUrl: '" + url + "',\n"
            + "       jdbcCatalog: null,\n"
            + "       jdbcSchema: null\n"
            + "     }\n"
            + "  ]\n"
            + "}");

    Connection calciteConnection =
        DriverManager.getConnection("jdbc:calcite:", info);
    Statement calciteStatement = calciteConnection.createStatement();
    final String sql = "SELECT ID, VALS FROM ARR_TABLE";
    ResultSet rs = calciteStatement.executeQuery(sql);
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    Array array = rs.getArray(2);
    assertNotNull(array);
    assertArrayEquals(new int[]{1, 2, 3}, (int[]) array.getArray());
    assertFalse(rs.next());
    rs.close();

    rs = calciteStatement.executeQuery(
        "SELECT ID, CARDINALITY(VALS), VALS[2] FROM ARR_TABLE");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertEquals(3, rs.getInt(2));
    assertEquals(2, rs.getInt(3));
    assertFalse(rs.next());
    rs.close();

    rs = calciteStatement.executeQuery(
        "SELECT * FROM ARR_TABLE2");
    final ResultSetMetaData metaData = rs.getMetaData();
    assertThat(metaData.getColumnTypeName(1), equalTo("INTEGER"));
    assertThat(metaData.getColumnTypeName(2), equalTo("INTEGER ARRAY"));
    assertThat(metaData.getColumnTypeName(3), equalTo("VARCHAR(10) ARRAY"));
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(1));
    assertThat(rs.getArray(2), notNullValue());
    assertThat(rs.getArray(3), notNullValue());
    assertFalse(rs.next());

    calciteConnection.close();
  }

  /** Tests the {@code CARDINALITY} function applied to an array column. */
  @Test public void testArray2() {
    CalciteAssert.hr()
        .query("select \"deptno\", cardinality(\"employees\") as c\n"
            + "from \"hr\".\"depts\"")
        .returnsUnordered("deptno=10; C=2",
            "deptno=30; C=0",
            "deptno=40; C=1");
  }

  /** Tests JDBC support for nested arrays. */
  @Test public void testNestedArray() throws Exception {
    CalciteAssert.hr()
        .doWithConnection(connection -> {
          try {
            final Statement statement = connection.createStatement();
            ResultSet resultSet =
                statement.executeQuery("select \"empid\",\n"
                    + "  array[\n"
                    + "    array['x', 'y', 'z'],\n"
                    + "    array[\"name\"]] as a\n"
                    + "from \"hr\".\"emps\"");
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getInt(1), equalTo(100));
            assertThat(resultSet.getString(2),
                equalTo("[[x, y, z], [Bill]]"));
            final Array array = resultSet.getArray(2);
            assertThat(array.getBaseType(),
                equalTo(Types.ARRAY));
            final Object[] arrayValues =
                (Object[]) array.getArray();
            assertThat(arrayValues.length, equalTo(2));
            final Array subArray = (Array) arrayValues[0];
            assertThat(subArray.getBaseType(),
                equalTo(Types.VARCHAR));
            final Object[] subArrayValues =
                (Object[]) subArray.getArray();
            assertThat(subArrayValues.length, equalTo(3));
            assertThat(subArrayValues[2], equalTo((Object) "z"));

            final ResultSet subResultSet = subArray.getResultSet();
            assertThat(subResultSet.next(), is(true));
            assertThat(subResultSet.getString(1), equalTo("x"));
            try {
              final String string = subResultSet.getString(2);
              fail("expected error, got " + string);
            } catch (SQLException e) {
              assertThat(e.getMessage(),
                  equalTo("invalid column ordinal: 2"));
            }
            assertThat(subResultSet.next(), is(true));
            assertThat(subResultSet.next(), is(true));
            assertThat(subResultSet.isAfterLast(), is(false));
            assertThat(subResultSet.getString(1), equalTo("z"));
            assertThat(subResultSet.next(), is(false));
            assertThat(subResultSet.isAfterLast(), is(true));
            statement.close();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test public void testArrayConstructor() {
    CalciteAssert.that()
        .query("select array[1,2] as a from (values (1))")
        .returnsUnordered("A=[1, 2]");
  }

  @Test public void testMultisetConstructor() {
    CalciteAssert.that()
        .query("select multiset[1,2] as a from (values (1))")
        .returnsUnordered("A=[1, 2]");
  }

  @Test public void testMultisetQuery() {
    CalciteAssert.hr()
        .query("select multiset(\n"
            + "  select \"deptno\", \"empid\" from \"hr\".\"emps\") as a\n"
            + "from (values (1))")
        .returnsUnordered("A=[{10, 100}, {20, 200}, {10, 150}, {10, 110}]");
  }

  @Test public void testMultisetQueryWithSingleColumn() {
    CalciteAssert.hr()
        .query("select multiset(\n"
            + "  select \"deptno\" from \"hr\".\"emps\") as a\n"
            + "from (values (1))")
        .returnsUnordered("A=[{10}, {20}, {10}, {10}]");
  }

  @Test public void testUnnestArray() {
    CalciteAssert.that()
        .query("select*from unnest(array[1,2])")
        .returnsUnordered("EXPR$0=1",
            "EXPR$0=2");
  }

  @Test public void testUnnestArrayWithOrdinality() {
    CalciteAssert.that()
        .query("select*from unnest(array[10,20]) with ordinality as t(i, o)")
        .returnsUnordered("I=10; O=1",
            "I=20; O=2");
  }

  @Test public void testUnnestMultiset() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select*from unnest(multiset[1,2]) as t(c)")
        .returnsUnordered("C=1", "C=2");
  }

  @Test public void testUnnestMultiset2() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select*from unnest(\n"
            + " select \"employees\" from \"hr\".\"depts\"\n"
            + " where \"deptno\" = 10)")
        .returnsUnordered(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000",
            "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2381">[CALCITE-2391]
   * Aggregate query with UNNEST or LATERAL fails with
   * ClassCastException</a>. */
  @Test public void testAggUnnestColumn() {
    final String sql = "select count(d.\"name\") as c\n"
        + "from \"hr\".\"depts\" as d,\n"
        + " UNNEST(d.\"employees\") as e";
    CalciteAssert.hr().query(sql).returnsUnordered("C=3");
  }

  @Test public void testArrayElement() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select element(\"employees\") from \"hr\".\"depts\"\n"
            + "where cardinality(\"employees\") < 2")
        .returnsUnordered("EXPR$0={200, 20, Eric, 8000.0, 500}",
            "EXPR$0=null");
  }

  @Test public void testLateral() {
    CalciteAssert.hr()
        .query("select * from \"hr\".\"emps\",\n"
            + " LATERAL (select * from \"hr\".\"depts\" where \"emps\".\"deptno\" = \"depts\".\"deptno\")")
        .returnsUnordered(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000; deptno0=10; name0=Sales; employees=[{100, 10, Bill, 10000.0, 1000}, {150, 10, Sebastian, 7000.0, null}]; location={-122, 38}",
            "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250; deptno0=10; name0=Sales; employees=[{100, 10, Bill, 10000.0, 1000}, {150, 10, Sebastian, 7000.0, null}]; location={-122, 38}",
            "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null; deptno0=10; name0=Sales; employees=[{100, 10, Bill, 10000.0, 1000}, {150, 10, Sebastian, 7000.0, null}]; location={-122, 38}");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-531">[CALCITE-531]
   * Window function does not work in LATERAL</a>. */
  @Test public void testLateralWithOver() {
    final String sql = "select \"emps\".\"name\", d.\"deptno\", d.m\n"
        + "from \"hr\".\"emps\",\n"
        + "  LATERAL (\n"
        + "    select \"depts\".\"deptno\",\n"
        + "      max(\"deptno\" + \"emps\".\"empid\") over (\n"
        + "        partition by \"emps\".\"deptno\") as m\n"
        + "     from \"hr\".\"depts\"\n"
        + "     where \"emps\".\"deptno\" = \"depts\".\"deptno\") as d";
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query(sql)
        .returnsUnordered("name=Bill; deptno=10; M=190",
            "name=Bill; deptno=30; M=190",
            "name=Bill; deptno=40; M=190",
            "name=Eric; deptno=10; M=240",
            "name=Eric; deptno=30; M=240",
            "name=Eric; deptno=40; M=240",
            "name=Sebastian; deptno=10; M=190",
            "name=Sebastian; deptno=30; M=190",
            "name=Sebastian; deptno=40; M=190",
            "name=Theodore; deptno=10; M=190",
            "name=Theodore; deptno=30; M=190",
            "name=Theodore; deptno=40; M=190");
  }

  /** Per SQL std, UNNEST is implicitly LATERAL. */
  @Test public void testUnnestArrayColumn() {
    CalciteAssert.hr()
        .query("select d.\"name\", e.*\n"
            + "from \"hr\".\"depts\" as d,\n"
            + " UNNEST(d.\"employees\") as e")
        .returnsUnordered(
            "name=HR; empid=200; deptno=20; name0=Eric; salary=8000.0; commission=500",
            "name=Sales; empid=100; deptno=10; name0=Bill; salary=10000.0; commission=1000",
            "name=Sales; empid=150; deptno=10; name0=Sebastian; salary=7000.0; commission=null");
  }

  @Test public void testUnnestArrayScalarArray() {
    CalciteAssert.hr()
        .query("select d.\"name\", e.*\n"
            + "from \"hr\".\"depts\" as d,\n"
            + " UNNEST(d.\"employees\", array[1, 2]) as e")
        .returnsUnordered(
            "name=HR; empid=200; deptno=20; name0=Eric; salary=8000.0; commission=500; EXPR$1=1",
            "name=HR; empid=200; deptno=20; name0=Eric; salary=8000.0; commission=500; EXPR$1=2",
            "name=Sales; empid=100; deptno=10; name0=Bill; salary=10000.0; commission=1000; EXPR$1=1",
            "name=Sales; empid=100; deptno=10; name0=Bill; salary=10000.0; commission=1000; EXPR$1=2",
            "name=Sales; empid=150; deptno=10; name0=Sebastian; salary=7000.0; commission=null; EXPR$1=1",
            "name=Sales; empid=150; deptno=10; name0=Sebastian; salary=7000.0; commission=null; EXPR$1=2");
  }

  @Test public void testUnnestArrayScalarArrayAliased() {
    CalciteAssert.hr()
        .query("select d.\"name\", e.*\n"
            + "from \"hr\".\"depts\" as d,\n"
            + " UNNEST(d.\"employees\", array[1, 2]) as e (ei, d, n, s, c, i)\n"
            + "where ei + i > 151")
        .returnsUnordered(
            "name=HR; EI=200; D=20; N=Eric; S=8000.0; C=500; I=1",
            "name=HR; EI=200; D=20; N=Eric; S=8000.0; C=500; I=2",
            "name=Sales; EI=150; D=10; N=Sebastian; S=7000.0; C=null; I=2");
  }

  @Test public void testUnnestArrayScalarArrayWithOrdinal() {
    CalciteAssert.hr()
        .query("select d.\"name\", e.*\n"
            + "from \"hr\".\"depts\" as d,\n"
            + " UNNEST(d.\"employees\", array[1, 2]) with ordinality as e (ei, d, n, s, c, i, o)\n"
            + "where ei + i > 151")
        .returnsUnordered(
            "name=HR; EI=200; D=20; N=Eric; S=8000.0; C=500; I=1; O=2",
            "name=HR; EI=200; D=20; N=Eric; S=8000.0; C=500; I=2; O=4",
            "name=Sales; EI=150; D=10; N=Sebastian; S=7000.0; C=null; I=2; O=5");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1250">[CALCITE-1250]
   * UNNEST applied to MAP data type</a>. */
  @Test public void testUnnestItemsInMap() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    final String sql = "select * from unnest(MAP['a', 1, 'b', 2]) as um(k, v)";
    ResultSet resultSet = connection.createStatement().executeQuery(sql);
    final String expected = "K=a; V=1\n"
        + "K=b; V=2\n";
    assertThat(CalciteAssert.toString(resultSet), is(expected));
    connection.close();
  }

  @Test public void testUnnestItemsInMapWithOrdinality() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    final String sql = "select *\n"
        + "from unnest(MAP['a', 1, 'b', 2]) with ordinality as um(k, v, i)";
    ResultSet resultSet = connection.createStatement().executeQuery(sql);
    final String expected = "K=a; V=1; I=1\n"
        + "K=b; V=2; I=2\n";
    assertThat(CalciteAssert.toString(resultSet), is(expected));
    connection.close();
  }

  @Test public void testUnnestItemsInMapWithNoAliasAndAdditionalArgument()
      throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    final String sql =
        "select * from unnest(MAP['a', 1, 'b', 2], array[5, 6, 7])";
    ResultSet resultSet = connection.createStatement().executeQuery(sql);

    List<String> map = FlatLists.of("KEY=a; VALUE=1", "KEY=b; VALUE=2");
    List<String> array = FlatLists.of(" EXPR$1=5", " EXPR$1=6", " EXPR$1=7");

    final StringBuilder b = new StringBuilder();
    for (List<String> row : Linq4j.product(FlatLists.of(map, array))) {
      b.append(row.get(0)).append(";").append(row.get(1)).append("\n");
    }
    final String expected = b.toString();

    assertThat(CalciteAssert.toString(resultSet), is(expected));
    connection.close();
  }

  private CalciteAssert.AssertQuery withFoodMartQuery(int id)
      throws IOException {
    final FoodMartQuerySet set = FoodMartQuerySet.instance();
    return CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(set.queries.get(id).sql);
  }

  /** Makes sure that a projection introduced by a call to
   * {@link org.apache.calcite.rel.rules.JoinCommuteRule} does not
   * manifest as an
   * {@link org.apache.calcite.adapter.enumerable.EnumerableCalc} in the
   * plan.
   *
   * <p>Test case for (not yet fixed)
   * <a href="https://issues.apache.org/jira/browse/CALCITE-92">[CALCITE-92]
   * Project should be optimized away, not converted to EnumerableCalcRel</a>.
   */
  @Ignore
  @Test public void testNoCalcBetweenJoins() throws IOException {
    final FoodMartQuerySet set = FoodMartQuerySet.instance();
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(set.queries.get(16).sql)
        .explainContains(""
            + "EnumerableSortRel(sort0=[$0], sort1=[$1], sort2=[$2], sort3=[$4], sort4=[$10], sort5=[$11], sort6=[$12], sort7=[$13], sort8=[$22], sort9=[$23], sort10=[$24], sort11=[$25], sort12=[$26], sort13=[$27], dir0=[Ascending-nulls-last], dir1=[Ascending-nulls-last], dir2=[Ascending-nulls-last], dir3=[Ascending-nulls-last], dir4=[Ascending-nulls-last], dir5=[Ascending-nulls-last], dir6=[Ascending-nulls-last], dir7=[Ascending-nulls-last], dir8=[Ascending-nulls-last], dir9=[Ascending-nulls-last], dir10=[Ascending-nulls-last], dir11=[Ascending-nulls-last], dir12=[Ascending-nulls-last], dir13=[Ascending-nulls-last])\n"
            + "  EnumerableCalcRel(expr#0..26=[{inputs}], proj#0..4=[{exprs}], c5=[$t4], c6=[$t5], c7=[$t6], c8=[$t7], c9=[$t8], c10=[$t9], c11=[$t10], c12=[$t11], c13=[$t12], c14=[$t13], c15=[$t14], c16=[$t15], c17=[$t16], c18=[$t17], c19=[$t18], c20=[$t19], c21=[$t20], c22=[$t21], c23=[$t22], c24=[$t23], c25=[$t24], c26=[$t25], c27=[$t26])\n"
            + "    EnumerableAggregateRel(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26}])\n"
            + "      EnumerableCalcRel(expr#0..80=[{inputs}], c0=[$t12], c1=[$t10], c2=[$t9], c3=[$t0], fullname=[$t28], c6=[$t19], c7=[$t17], c8=[$t22], c9=[$t18], c10=[$t46], c11=[$t44], c12=[$t43], c13=[$t40], c14=[$t38], c15=[$t47], c16=[$t52], c17=[$t53], c18=[$t54], c19=[$t55], c20=[$t56], c21=[$t42], c22=[$t80], c23=[$t79], c24=[$t78], c25=[$t77], c26=[$t63], c27=[$t64])\n"
            + "        EnumerableJoinRel(condition=[=($61, $76)], joinType=[inner])\n"
            + "          EnumerableJoinRel(condition=[=($29, $62)], joinType=[inner])\n"
            + "            EnumerableJoinRel(condition=[=($33, $37)], joinType=[inner])\n"
            + "              EnumerableCalcRel(expr#0..36=[{inputs}], customer_id=[$t8], account_num=[$t9], lname=[$t10], fname=[$t11], mi=[$t12], address1=[$t13], address2=[$t14], address3=[$t15], address4=[$t16], city=[$t17], state_province=[$t18], postal_code=[$t19], country=[$t20], customer_region_id=[$t21], phone1=[$t22], phone2=[$t23], birthdate=[$t24], marital_status=[$t25], yearly_income=[$t26], gender=[$t27], total_children=[$t28], num_children_at_home=[$t29], education=[$t30], date_accnt_opened=[$t31], member_card=[$t32], occupation=[$t33], houseowner=[$t34], num_cars_owned=[$t35], fullname=[$t36], product_id=[$t0], time_id=[$t1], customer_id0=[$t2], promotion_id=[$t3], store_id=[$t4], store_sales=[$t5], store_cost=[$t6], unit_sales=[$t7])\n"
            + "                EnumerableJoinRel(condition=[=($2, $8)], joinType=[inner])\n"
            + "                  EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])\n"
            + "                  EnumerableTableScan(table=[[foodmart2, customer]])\n"
            + "              EnumerableTableScan(table=[[foodmart2, store]])\n"
            + "            EnumerableTableScan(table=[[foodmart2, product]])\n"
            + "          EnumerableTableScan(table=[[foodmart2, product_class]])\n");
  }

  /** Checks that a 3-way join is re-ordered so that join conditions can be
   * applied. The plan must not contain cartesian joins.
   * {@link org.apache.calcite.rel.rules.JoinPushThroughJoinRule} makes this
   * possible. */
  @Ignore
  @Test public void testExplainJoin() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(FOODMART_QUERIES.get(48).left)
        .explainContains(""
            + "EnumerableAggregateRel(group=[{}], m0=[COUNT($0)])\n"
            + "  EnumerableAggregateRel(group=[{0}])\n"
            + "    EnumerableCalcRel(expr#0..27=[{inputs}], customer_id=[$t7])\n"
            + "      EnumerableJoinRel(condition=[=($13, $0)], joinType=[inner])\n"
            + "        EnumerableTableScan(table=[[foodmart2, product_class]])\n"
            + "        EnumerableJoinRel(condition=[=($0, $9)], joinType=[inner])\n"
            + "          EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])\n"
            + "          EnumerableCalcRel(expr#0..14=[{inputs}], expr#15=['Cormorant'], expr#16=[=($t2, $t15)], proj#0..14=[{exprs}], $condition=[$t16])\n"
            + "            EnumerableTableScan(table=[[foodmart2, product]]");
  }

  /** Checks that a 3-way join is re-ordered so that join conditions can be
   * applied. The plan is left-deep (agg_c_14_sales_fact_1997 the most
   * rows, then time_by_day, then store). This makes for efficient
   * hash-joins. */
  @Ignore
  @Test public void testExplainJoin2() throws IOException {
    withFoodMartQuery(2482)
        .explainContains(""
            + "EnumerableSortRel(sort0=[$0], sort1=[$1], dir0=[Ascending-nulls-last], dir1=[Ascending-nulls-last])\n"
            + "  EnumerableAggregateRel(group=[{0, 1}])\n"
            + "    EnumerableCalcRel(expr#0..5=[{inputs}], c0=[$t4], c1=[$t1])\n"
            + "      EnumerableJoinRel(condition=[=($3, $5)], joinType=[inner])\n"
            + "        EnumerableCalcRel(expr#0..3=[{inputs}], store_id=[$t2], store_country=[$t3], store_id0=[$t0], month_of_year=[$t1])\n"
            + "          EnumerableJoinRel(condition=[=($0, $2)], joinType=[inner])\n"
            + "            EnumerableCalcRel(expr#0..10=[{inputs}], store_id=[$t2], month_of_year=[$t4])\n"
            + "              EnumerableTableScan(table=[[foodmart2, agg_c_14_sales_fact_1997]])\n"
            + "            EnumerableCalcRel(expr#0..23=[{inputs}], store_id=[$t0], store_country=[$t9])\n"
            + "              EnumerableTableScan(table=[[foodmart2, store]])\n"
            + "        EnumerableCalcRel(expr#0..9=[{inputs}], the_year=[$t4], month_of_year=[$t7])\n"
            + "          EnumerableTableScan(table=[[foodmart2, time_by_day]])\n")
        .runs();
  }

  /** One of the most expensive foodmart queries. */
  @Ignore // OOME on Travis; works on most other machines
  @Test public void testExplainJoin3() throws IOException {
    withFoodMartQuery(8)
        .explainContains(""
            + "EnumerableSortRel(sort0=[$0], sort1=[$1], sort2=[$2], sort3=[$4], dir0=[Ascending-nulls-last], dir1=[Ascending-nulls-last], dir2=[Ascending-nulls-last], dir3=[Ascending-nulls-last])\n"
            + "  EnumerableCalcRel(expr#0..8=[{inputs}], expr#9=['%Jeanne%'], expr#10=[LIKE($t4, $t9)], proj#0..4=[{exprs}], c5=[$t4], c6=[$t5], c7=[$t6], c8=[$t7], c9=[$t8], $condition=[$t10])\n"
            + "    EnumerableAggregateRel(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8}])\n"
            + "      EnumerableCalcRel(expr#0..46=[{inputs}], c0=[$t12], c1=[$t10], c2=[$t9], c3=[$t0], fullname=[$t28], c6=[$t19], c7=[$t17], c8=[$t22], c9=[$t18])\n"
            + "        EnumerableJoinRel(condition=[=($30, $37)], joinType=[inner])\n"
            + "          EnumerableCalcRel(expr#0..36=[{inputs}], customer_id=[$t8], account_num=[$t9], lname=[$t10], fname=[$t11], mi=[$t12], address1=[$t13], address2=[$t14], address3=[$t15], address4=[$t16], city=[$t17], state_province=[$t18], postal_code=[$t19], country=[$t20], customer_region_id=[$t21], phone1=[$t22], phone2=[$t23], birthdate=[$t24], marital_status=[$t25], yearly_income=[$t26], gender=[$t27], total_children=[$t28], num_children_at_home=[$t29], education=[$t30], date_accnt_opened=[$t31], member_card=[$t32], occupation=[$t33], houseowner=[$t34], num_cars_owned=[$t35], fullname=[$t36], product_id=[$t0], time_id=[$t1], customer_id0=[$t2], promotion_id=[$t3], store_id=[$t4], store_sales=[$t5], store_cost=[$t6], unit_sales=[$t7])\n"
            + "            EnumerableJoinRel(condition=[=($2, $8)], joinType=[inner])\n"
            + "              EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])\n"
            + "              EnumerableTableScan(table=[[foodmart2, customer]])\n"
            + "          EnumerableCalcRel(expr#0..9=[{inputs}], expr#10=[CAST($t4):INTEGER], expr#11=[1997], expr#12=[=($t10, $t11)], proj#0..9=[{exprs}], $condition=[$t12])\n"
            + "            EnumerableTableScan(table=[[foodmart2, time_by_day]])")
        .runs();
  }

  /** Tests that a relatively complex query on the foodmart schema creates
   * an in-memory aggregate table and then uses it. */
  @Ignore // DO NOT CHECK IN
  @Test public void testFoodmartLattice() throws IOException {
    // 8: select ... from customer, sales, time ... group by ...
    final FoodMartQuerySet set = FoodMartQuerySet.instance();
    final FoodMartQuerySet.FoodmartQuery query = set.queries.get(8);
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART_WITH_LATTICE)
        .withDefaultSchema("foodmart")
        .pooled()
        .query(query.sql)
        .enableMaterializations(true)
        .explainContains(""
            + "EnumerableCalc(expr#0..8=[{inputs}], c0=[$t3], c1=[$t2], c2=[$t1], c3=[$t0], c4=[$t8], c5=[$t8], c6=[$t6], c7=[$t4], c8=[$t7], c9=[$t5])\n"
            + "  EnumerableSort(sort0=[$3], sort1=[$2], sort2=[$1], sort3=[$8], dir0=[ASC-nulls-last], dir1=[ASC-nulls-last], dir2=[ASC-nulls-last], dir3=[ASC-nulls-last])\n"
            + "    EnumerableAggregate(group=[{0, 1, 2, 3, 4, 5, 6, 7, 8}])\n"
            + "      EnumerableCalc(expr#0..9=[{inputs}], expr#10=[CAST($t0):INTEGER], expr#11=[1997], expr#12=[=($t10, $t11)], expr#13=['%Jeanne%'], expr#14=[LIKE($t9, $t13)], expr#15=[AND($t12, $t14)], $f0=[$t1], $f1=[$t2], $f2=[$t3], $f3=[$t4], $f4=[$t5], $f5=[$t6], $f6=[$t7], $f7=[$t8], $f8=[$t9], $f9=[$t0], $condition=[$t15])\n"
            + "        EnumerableTableScan(table=[[foodmart, m{12, 18, 27, 28, 30, 35, 36, 37, 40, 46}]])")
        .runs();
  }

  /** Test case for (not yet fixed)
   * <a href="https://issues.apache.org/jira/browse/CALCITE-99">[CALCITE-99]
   * Recognize semi-join that has high selectivity and push it down</a>. */
  @Ignore
  @Test public void testExplainJoin4() throws IOException {
    withFoodMartQuery(5217)
        .explainContains(""
            + "EnumerableAggregateRel(group=[{0, 1, 2, 3}], m0=[COUNT($4)])\n"
            + "  EnumerableCalcRel(expr#0..69=[{inputs}], c0=[$t4], c1=[$t27], c2=[$t61], c3=[$t66], $f11=[$t11])\n"
            + "    EnumerableJoinRel(condition=[=($68, $69)], joinType=[inner])\n"
            + "      EnumerableCalcRel(expr#0..67=[{inputs}], proj#0..67=[{exprs}], $f68=[$t66])\n"
            + "        EnumerableJoinRel(condition=[=($11, $65)], joinType=[inner])\n"
            + "          EnumerableJoinRel(condition=[=($46, $59)], joinType=[inner])\n"
            + "            EnumerableCalcRel(expr#0..58=[{inputs}], $f0=[$t49], $f1=[$t50], $f2=[$t51], $f3=[$t52], $f4=[$t53], $f5=[$t54], $f6=[$t55], $f7=[$t56], $f8=[$t57], $f9=[$t58], $f10=[$t41], $f11=[$t42], $f12=[$t43], $f13=[$t44], $f14=[$t45], $f15=[$t46], $f16=[$t47], $f17=[$t48], $f18=[$t0], $f19=[$t1], $f20=[$t2], $f21=[$t3], $f22=[$t4], $f23=[$t5], $f24=[$t6], $f25=[$t7], $f26=[$t8], $f27=[$t9], $f28=[$t10], $f29=[$t11], $f30=[$t12], $f31=[$t13], $f32=[$t14], $f33=[$t15], $f34=[$t16], $f35=[$t17], $f36=[$t18], $f37=[$t19], $f38=[$t20], $f39=[$t21], $f40=[$t22], $f41=[$t23], $f42=[$t24], $f43=[$t25], $f44=[$t26], $f45=[$t27], $f46=[$t28], $f47=[$t29], $f48=[$t30], $f49=[$t31], $f50=[$t32], $f51=[$t33], $f52=[$t34], $f53=[$t35], $f54=[$t36], $f55=[$t37], $f56=[$t38], $f57=[$t39], $f58=[$t40])\n"
            + "              EnumerableJoinRel(condition=[=($41, $50)], joinType=[inner])\n"
            + "                EnumerableCalcRel(expr#0..48=[{inputs}], $f0=[$t25], $f1=[$t26], $f2=[$t27], $f3=[$t28], $f4=[$t29], $f5=[$t30], $f6=[$t31], $f7=[$t32], $f8=[$t33], $f9=[$t34], $f10=[$t35], $f11=[$t36], $f12=[$t37], $f13=[$t38], $f14=[$t39], $f15=[$t40], $f16=[$t41], $f17=[$t42], $f18=[$t43], $f19=[$t44], $f20=[$t45], $f21=[$t46], $f22=[$t47], $f23=[$t48], $f24=[$t8], $f25=[$t9], $f26=[$t10], $f27=[$t11], $f28=[$t12], $f29=[$t13], $f30=[$t14], $f31=[$t15], $f32=[$t16], $f33=[$t17], $f34=[$t18], $f35=[$t19], $f36=[$t20], $f37=[$t21], $f38=[$t22], $f39=[$t23], $f40=[$t24], $f41=[$t0], $f42=[$t1], $f43=[$t2], $f44=[$t3], $f45=[$t4], $f46=[$t5], $f47=[$t6], $f48=[$t7])\n"
            + "                  EnumerableJoinRel(condition=[=($14, $25)], joinType=[inner])\n"
            + "                    EnumerableJoinRel(condition=[=($1, $8)], joinType=[inner])\n"
            + "                      EnumerableTableScan(table=[[foodmart2, salary]])\n"
            + "                      EnumerableTableScan(table=[[foodmart2, employee]])\n"
            + "                    EnumerableTableScan(table=[[foodmart2, store]])\n"
            + "                EnumerableTableScan(table=[[foodmart2, time_by_day]])\n"
            + "            EnumerableTableScan(table=[[foodmart2, position]])\n"
            + "          EnumerableTableScan(table=[[foodmart2, employee_closure]])\n"
            + "      EnumerableAggregateRel(group=[{0}])\n"
            + "        EnumerableValuesRel(tuples=[[{ 1 }, { 2 }, { 20 }, { 21 }, { 22 }, { 23 }, { 24 }, { 25 }, { 26 }, { 27 }, { 28 }, { 29 }, { 30 }, { 31 }, { 53 }, { 54 }, { 55 }, { 56 }, { 57 }, { 58 }, { 59 }, { 60 }, { 61 }, { 62 }, { 63 }, { 64 }, { 65 }, { 66 }, { 67 }, { 68 }, { 69 }, { 70 }, { 71 }, { 72 }, { 73 }, { 74 }, { 75 }, { 76 }, { 77 }, { 78 }, { 79 }, { 80 }, { 81 }, { 82 }, { 83 }, { 84 }, { 85 }, { 86 }, { 87 }, { 88 }, { 89 }, { 90 }, { 91 }, { 92 }, { 93 }, { 94 }, { 95 }, { 96 }, { 97 }, { 98 }, { 99 }, { 100 }, { 101 }, { 102 }, { 103 }, { 104 }, { 105 }, { 106 }, { 107 }, { 108 }, { 109 }, { 110 }, { 111 }, { 112 }, { 113 }, { 114 }, { 115 }, { 116 }, { 117 }, { 118 }, { 119 }, { 120 }, { 121 }, { 122 }, { 123 }, { 124 }, { 125 }, { 126 }, { 127 }, { 128 }, { 129 }, { 130 }, { 131 }, { 132 }, { 133 }, { 134 }, { 135 }, { 136 }, { 137 }, { 138 }, { 139 }, { 140 }, { 141 }, { 142 }, { 143 }, { 144 }, { 145 }, { 146 }, { 147 }, { 148 }, { 149 }, { 150 }, { 151 }, { 152 }, { 153 }, { 154 }, { 155 }, { 156 }, { 157 }, { 158 }, { 159 }, { 160 }, { 161 }, { 162 }, { 163 }, { 164 }, { 165 }, { 166 }, { 167 }, { 168 }, { 169 }, { 170 }, { 171 }, { 172 }, { 173 }, { 174 }, { 175 }, { 176 }, { 177 }, { 178 }, { 179 }, { 180 }, { 181 }, { 182 }, { 183 }, { 184 }, { 185 }, { 186 }, { 187 }, { 188 }, { 189 }, { 190 }, { 191 }, { 192 }, { 193 }, { 194 }, { 195 }, { 196 }, { 197 }, { 198 }, { 199 }, { 200 }, { 201 }, { 202 }, { 203 }, { 204 }, { 205 }, { 206 }, { 207 }, { 208 }, { 209 }, { 210 }, { 211 }, { 212 }, { 213 }, { 214 }, { 215 }, { 216 }, { 217 }, { 218 }, { 219 }, { 220 }, { 221 }, { 222 }, { 223 }, { 224 }, { 225 }, { 226 }, { 227 }, { 228 }, { 229 }, { 230 }, { 231 }, { 232 }, { 233 }, { 234 }, { 235 }, { 236 }, { 237 }, { 238 }, { 239 }, { 240 }, { 241 }, { 242 }, { 243 }, { 244 }, { 245 }, { 246 }, { 247 }, { 248 }, { 249 }, { 250 }, { 251 }, { 252 }, { 253 }, { 254 }, { 255 }, { 256 }, { 257 }, { 258 }, { 259 }, { 260 }, { 261 }, { 262 }, { 263 }, { 264 }, { 265 }, { 266 }, { 267 }, { 268 }, { 269 }, { 270 }, { 271 }, { 272 }, { 273 }, { 274 }, { 275 }, { 276 }, { 277 }, { 278 }, { 279 }, { 280 }, { 281 }, { 282 }, { 283 }, { 284 }, { 285 }, { 286 }, { 287 }, { 288 }, { 289 }, { 290 }, { 291 }, { 292 }, { 293 }, { 294 }, { 295 }, { 296 }, { 297 }, { 298 }, { 299 }, { 300 }, { 301 }, { 302 }, { 303 }, { 304 }, { 305 }, { 306 }, { 307 }, { 308 }, { 309 }, { 310 }, { 311 }, { 312 }, { 313 }, { 314 }, { 315 }, { 316 }, { 317 }, { 318 }, { 319 }, { 320 }, { 321 }, { 322 }, { 323 }, { 324 }, { 325 }, { 326 }, { 327 }, { 328 }, { 329 }, { 330 }, { 331 }, { 332 }, { 333 }, { 334 }, { 335 }, { 336 }, { 337 }, { 338 }, { 339 }, { 340 }, { 341 }, { 342 }, { 343 }, { 344 }, { 345 }, { 346 }, { 347 }, { 348 }, { 349 }, { 350 }, { 351 }, { 352 }, { 353 }, { 354 }, { 355 }, { 356 }, { 357 }, { 358 }, { 359 }, { 360 }, { 361 }, { 362 }, { 363 }, { 364 }, { 365 }, { 366 }, { 367 }, { 368 }, { 369 }, { 370 }, { 371 }, { 372 }, { 373 }, { 374 }, { 375 }, { 376 }, { 377 }, { 378 }, { 379 }, { 380 }, { 381 }, { 382 }, { 383 }, { 384 }, { 385 }, { 386 }, { 387 }, { 388 }, { 389 }, { 390 }, { 391 }, { 392 }, { 393 }, { 394 }, { 395 }, { 396 }, { 397 }, { 398 }, { 399 }, { 400 }, { 401 }, { 402 }, { 403 }, { 404 }, { 405 }, { 406 }, { 407 }, { 408 }, { 409 }, { 410 }, { 411 }, { 412 }, { 413 }, { 414 }, { 415 }, { 416 }, { 417 }, { 418 }, { 419 }, { 420 }, { 421 }, { 422 }, { 423 }, { 424 }, { 425 }, { 430 }, { 431 }, { 432 }, { 433 }, { 434 }, { 435 }, { 436 }, { 437 }, { 442 }, { 443 }, { 444 }, { 445 }, { 446 }, { 447 }, { 448 }, { 449 }, { 450 }, { 451 }, { 457 }, { 458 }, { 459 }, { 460 }, { 461 }, { 462 }, { 463 }, { 469 }, { 470 }, { 471 }, { 472 }, { 473 }]])\n")
        .runs();
  }

  /** Condition involving OR makes this more complex than
   * {@link #testExplainJoin()}. */
  @Ignore
  @Test public void testExplainJoinOrderingWithOr() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(FOODMART_QUERIES.get(47).left)
        .explainContains("xxx");
  }

  /** There was a bug representing a nullable timestamp using a {@link Long}
   * internally. */
  @Test public void testNullableTimestamp() {
    checkNullableTimestamp(CalciteAssert.Config.FOODMART_CLONE);
  }

  /** Similar to {@link #testNullableTimestamp} but directly off JDBC. */
  @Test public void testNullableTimestamp2() {
    checkNullableTimestamp(CalciteAssert.Config.JDBC_FOODMART);
  }

  private void checkNullableTimestamp(CalciteAssert.Config config) {
    CalciteAssert.that()
        .with(config)
        .query(
            "select \"hire_date\", \"end_date\", \"birth_date\" from \"foodmart\".\"employee\" where \"employee_id\" = 1")
        // disable for MySQL; birth_date suffers timezone shift
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.MYSQL)
        .returns2(
            "hire_date=1994-12-01; end_date=null; birth_date=1961-08-26\n");
  }

  @Test public void testReuseExpressionWhenNullChecking() {
    CalciteAssert.hr()
        .query(
            "select upper((case when \"empid\">\"deptno\"*10 then 'y' else null end)) T from \"hr\".\"emps\"")
        .planContains("static final String "
            + "$L4J$C$org_apache_calcite_runtime_SqlFunctions_upper_y_ = "
            + "org.apache.calcite.runtime.SqlFunctions.upper(\"y\");")
        .planContains("return current.empid <= current.deptno * 10 "
            + "? (String) null "
            + ": $L4J$C$org_apache_calcite_runtime_SqlFunctions_upper_y_;")
        .returns("T=null\n"
            + "T=null\n"
            + "T=Y\n"
            + "T=Y\n");
  }

  @Test public void testReuseExpressionWhenNullChecking2() {
    CalciteAssert.hr()
        .query(
            "select upper((case when \"empid\">\"deptno\"*10 then \"name\" end)) T from \"hr\".\"emps\"")
        .planContains(
            "final String inp2_ = current.name;")
        .planContains("return current.empid <= current.deptno * 10 "
            + "|| inp2_ == null "
            + "? (String) null "
            + ": org.apache.calcite.runtime.SqlFunctions.upper(inp2_);")
        .returns("T=null\n"
            + "T=null\n"
            + "T=SEBASTIAN\n"
            + "T=THEODORE\n");
  }

  @Test public void testReuseExpressionWhenNullChecking3() {
    CalciteAssert.hr()
        .query(
            "select substring(\"name\", \"deptno\"+case when user <> 'sa' then 1 end) from \"hr\".\"emps\"")
        .planContains(
            "final String inp2_ = current.name;")
        .planContains("static final boolean "
            + "$L4J$C$org_apache_calcite_runtime_SqlFunctions_ne_sa_sa_ = "
            + "org.apache.calcite.runtime.SqlFunctions.ne(\"sa\", \"sa\");")
        .planContains("static final boolean "
            + "$L4J$C$_org_apache_calcite_runtime_SqlFunctions_ne_sa_sa_ = "
            + "!$L4J$C$org_apache_calcite_runtime_SqlFunctions_ne_sa_sa_;")
        .planContains("return inp2_ == null "
            + "|| $L4J$C$_org_apache_calcite_runtime_SqlFunctions_ne_sa_sa_ ? (String) null"
            + " : org.apache.calcite.runtime.SqlFunctions.substring(inp2_, "
            + "current.deptno + 1);");
  }

  @Test public void testReuseExpressionWhenNullChecking4() {
    CalciteAssert.hr()
        .query("select substring(trim(\n"
            + "substring(\"name\",\n"
            + "  \"deptno\"*0+case when user = 'sa' then 1 end)\n"
            + "), case when \"empid\">\"deptno\" then 4\n" /* diff from 5 */
            + "   else\n"
            + "     case when \"deptno\"*8>8 then 5 end\n"
            + "   end-2) T\n"
            + "from\n"
            + "\"hr\".\"emps\"")
        .planContains(
            "final String inp2_ = current.name;")
        .planContains(
            "final int inp1_ = current.deptno;")
        .planContains("static final boolean "
            + "$L4J$C$org_apache_calcite_runtime_SqlFunctions_eq_sa_sa_ = "
            + "org.apache.calcite.runtime.SqlFunctions.eq(\"sa\", \"sa\");")
        .planContains("static final boolean "
            + "$L4J$C$_org_apache_calcite_runtime_SqlFunctions_eq_sa_sa_ = "
            + "!$L4J$C$org_apache_calcite_runtime_SqlFunctions_eq_sa_sa_;")
        .planContains("return inp2_ == null "
            + "|| $L4J$C$_org_apache_calcite_runtime_SqlFunctions_eq_sa_sa_ "
            + "|| !v5 && inp1_ * 8 <= 8 "
            + "? (String) null "
            + ": org.apache.calcite.runtime.SqlFunctions.substring("
            + "org.apache.calcite.runtime.SqlFunctions.trim(true, true, \" \", "
            + "org.apache.calcite.runtime.SqlFunctions.substring(inp2_, "
            + "inp1_ * 0 + 1), true), (v5 ? 4 : 5) - 2);")
        .returns("T=ill\n"
            + "T=ric\n"
            + "T=ebastian\n"
            + "T=heodore\n");
  }

  @Test public void testReuseExpressionWhenNullChecking5() {
    CalciteAssert.hr()
        .query("select substring(trim(\n"
            + "substring(\"name\",\n"
            + "  \"deptno\"*0+case when user = 'sa' then 1 end)\n"
            + "), case when \"empid\">\"deptno\" then 5\n" /* diff from 4 */
            + "   else\n"
            + "     case when \"deptno\"*8>8 then 5 end\n"
            + "   end-2) T\n"
            + "from\n"
            + "\"hr\".\"emps\"")
        .planContains(
            "final String inp2_ = current.name;")
        .planContains(
            "final int inp1_ = current.deptno;")
        .planContains(
            "static final int $L4J$C$5_2 = 5 - 2;")
        .planContains("static final boolean "
            + "$L4J$C$org_apache_calcite_runtime_SqlFunctions_eq_sa_sa_ = "
            + "org.apache.calcite.runtime.SqlFunctions.eq(\"sa\", \"sa\");")
        .planContains("static final boolean "
            + "$L4J$C$_org_apache_calcite_runtime_SqlFunctions_eq_sa_sa_ = "
            + "!$L4J$C$org_apache_calcite_runtime_SqlFunctions_eq_sa_sa_;")
        .planContains("return inp2_ == null "
            + "|| $L4J$C$_org_apache_calcite_runtime_SqlFunctions_eq_sa_sa_ "
            + "|| current.empid <= inp1_ && inp1_ * 8 <= 8 "
            + "? (String) null "
            + ": org.apache.calcite.runtime.SqlFunctions.substring("
            + "org.apache.calcite.runtime.SqlFunctions.trim(true, true, \" \", "
            + "org.apache.calcite.runtime.SqlFunctions.substring(inp2_, "
            + "inp1_ * 0 + 1), true), $L4J$C$5_2);")
        .returns("T=ll\n"
            + "T=ic\n"
            + "T=bastian\n"
            + "T=eodore\n");
  }

  @Test public void testValues() {
    CalciteAssert.that()
        .query("values (1), (2)")
        .returns("EXPR$0=1\n"
            + "EXPR$0=2\n");
  }

  @Test public void testValuesAlias() {
    CalciteAssert.that()
        .query(
            "select \"desc\" from (VALUES ROW(1, 'SameName')) AS \"t\" (\"id\", \"desc\")")
        .returns("desc=SameName\n");
  }

  @Test public void testValuesMinus() {
    CalciteAssert.that()
        .query("values (-2-1)")
        .returns("EXPR$0=-3\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1120">[CALCITE-1120]
   * Support SELECT without FROM</a>. */
  @Test public void testSelectWithoutFrom() {
    CalciteAssert.that()
        .query("select 2+2")
        .returns("EXPR$0=4\n");
  }

  /** Tests a table constructor that has multiple rows and multiple columns.
   *
   * <p>Note that the character literals become CHAR(3) and that the first is
   * correctly rendered with trailing spaces: 'a  '. If we were inserting
   * into a VARCHAR column the behavior would be different; the literals
   * would be converted into VARCHAR(3) values and the implied cast from
   * CHAR(1) to CHAR(3) that appends trailing spaces does not occur. See
   * "contextually typed value specification" in the SQL spec.</p>
   */
  @Test public void testValuesComposite() {
    CalciteAssert.that()
        .query("values (1, 'a'), (2, 'abc')")
        .returns("EXPR$0=1; EXPR$1=a  \n"
            + "EXPR$0=2; EXPR$1=abc\n");
  }

  /**
   * Tests that even though trivial "rename columns" projection is removed,
   * the query still returns proper column names.
   */
  @Test public void testValuesCompositeRenamed() {
    CalciteAssert.that()
        .query("select EXPR$0 q, EXPR$1 w from (values (1, 'a'), (2, 'abc'))")
        .explainContains(
            "PLAN=EnumerableValues(tuples=[[{ 1, 'a  ' }, { 2, 'abc' }]])\n")
        .returns("Q=1; W=a  \n"
            + "Q=2; W=abc\n");
  }

  /**
   * Tests that even though trivial "rename columns" projection is removed,
   * the query still returns proper column names.
   */
  @Test public void testValuesCompositeRenamedSameNames() {
    CalciteAssert.that()
        .query("select EXPR$0 q, EXPR$1 q from (values (1, 'a'), (2, 'abc'))")
        .explainContains(
            "PLAN=EnumerableValues(tuples=[[{ 1, 'a  ' }, { 2, 'abc' }]])\n")
        .returnsUnordered(
            "Q=1; Q=a  ",
            "Q=2; Q=abc");
  }

  /**
   * Tests that even though trivial "rename columns" projection is removed,
   * the query still returns proper column names.
   */
  @Test public void testUnionWithSameColumnNames() {
    CalciteAssert.hr()
        .query(
            "select \"deptno\", \"deptno\" from \"hr\".\"depts\" union select \"deptno\", \"empid\" from \"hr\".\"emps\"")
        .explainContains(""
            + "PLAN=EnumerableAggregate(group=[{0, 1}])\n"
            + "  EnumerableUnion(all=[true])\n"
            + "    EnumerableCalc(expr#0=[{inputs}], deptno=[$t0], deptno0=[$t0])\n"
            + "      EnumerableAggregate(group=[{0}])\n"
            + "        EnumerableTableScan(table=[[hr, depts]])\n"
            + "    EnumerableCalc(expr#0..1=[{inputs}], deptno=[$t1], empid=[$t0])\n"
            + "      EnumerableAggregate(group=[{0, 1}])\n"
            + "        EnumerableTableScan(table=[[hr, emps]])\n")
        .returnsUnordered(
            "deptno=10; deptno=110",
            "deptno=10; deptno=10",
            "deptno=20; deptno=200",
            "deptno=10; deptno=100",
            "deptno=10; deptno=150",
            "deptno=30; deptno=30",
            "deptno=40; deptno=40");
  }

  /** Tests inner join to an inline table ({@code VALUES} clause). */
  @Test public void testInnerJoinValues() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.LINGUAL)
        .query("select empno, desc from sales.emps,\n"
            + "  (SELECT * FROM (VALUES (10, 'SameName')) AS t (id, desc)) as sn\n"
            + "where emps.deptno = sn.id and sn.desc = 'SameName' group by empno, desc")
        .explainContains("EnumerableCalc(expr#0..1=[{inputs}], EMPNO=[$t1], DESC=[$t0])\n"
            + "  EnumerableAggregate(group=[{1, 2}])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], expr#4=[CAST($t3):INTEGER NOT NULL], expr#5=[=($t4, $t0)], expr#6=['SameName'], expr#7=[=($t1, $t6)], expr#8=[AND($t5, $t7)], proj#0..3=[{exprs}], $condition=[$t8])\n"
            + "      EnumerableJoin(condition=[true], joinType=[inner])\n"
            + "        EnumerableValues(tuples=[[{ 10, 'SameName' }]])\n"
            + "        EnumerableTableScan(table=[[SALES, EMPS]])\n")
        .returns("EMPNO=1; DESC=SameName\n");
  }

  /** Tests a merge-join. */
  @Test public void testMergeJoin() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select \"emps\".\"empid\",\n"
            + " \"depts\".\"deptno\", \"depts\".\"name\"\n"
            + "from \"hr\".\"emps\"\n"
            + " join \"hr\".\"depts\" using (\"deptno\")")
        .explainContains(""
            + "EnumerableCalc(expr#0..3=[{inputs}], empid=[$t2], deptno=[$t0], name=[$t1])\n"
            + "  EnumerableJoin(condition=[=($0, $3)], joinType=[inner])\n"
            + "    EnumerableCalc(expr#0..3=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[hr, depts]])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], proj#0..1=[{exprs}])\n"
            + "      EnumerableTableScan(table=[[hr, emps]])")
        .returns("empid=100; deptno=10; name=Sales\n"
            + "empid=150; deptno=10; name=Sales\n"
            + "empid=110; deptno=10; name=Sales\n");
  }

  /** Tests a cartesian product aka cross join. */
  @Test public void testCartesianJoin() {
    CalciteAssert.hr()
        .query(
            "select * from \"hr\".\"emps\", \"hr\".\"depts\" where \"emps\".\"empid\" < 140 and \"depts\".\"deptno\" > 20")
        .returnsUnordered(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000; deptno0=30; name0=Marketing; employees=[]; location={0, 52}",
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000; deptno0=40; name0=HR; employees=[{200, 20, Eric, 8000.0, 500}]; location=null",
            "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250; deptno0=30; name0=Marketing; employees=[]; location={0, 52}",
            "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250; deptno0=40; name0=HR; employees=[{200, 20, Eric, 8000.0, 500}]; location=null");
  }

  @Test public void testDistinctCountSimple() {
    final String s =
        "select count(distinct \"sales_fact_1997\".\"unit_sales\") as \"m0\"\n"
            + "from \"sales_fact_1997\" as \"sales_fact_1997\"";
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(s)
        .explainContains("EnumerableAggregate(group=[{}], m0=[COUNT($0)])\n"
            + "  EnumerableAggregate(group=[{7}])\n"
            + "    EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])")
        .returns("m0=6\n");
  }

  @Test public void testDistinctCount2() {
    final String s = "select cast(\"unit_sales\" as integer) as \"u\",\n"
        + " count(distinct \"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
        + "from \"sales_fact_1997\" as \"sales_fact_1997\"\n"
        + "group by \"unit_sales\"";
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(s)
        .explainContains(""
            + "EnumerableCalc(expr#0..1=[{inputs}], expr#2=[CAST($t0):INTEGER NOT NULL], u=[$t2], m0=[$t1])\n"
            + "  EnumerableAggregate(group=[{1}], m0=[COUNT($0)])\n"
            + "    EnumerableAggregate(group=[{2, 7}])\n"
            + "      EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])")
        .returnsUnordered(
            "u=1; m0=523",
            "u=5; m0=1059",
            "u=4; m0=4459",
            "u=6; m0=19",
            "u=3; m0=4895",
            "u=2; m0=4735");
  }

  @Test public void testDistinctCount() {
    final String s = "select \"time_by_day\".\"the_year\" as \"c0\",\n"
        + " count(distinct \"sales_fact_1997\".\"unit_sales\") as \"m0\"\n"
        + "from \"time_by_day\" as \"time_by_day\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\"\n"
        + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "group by \"time_by_day\".\"the_year\"";
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(s)
        .enable(CalciteAssert.DB != CalciteAssert.DatabaseInstance.ORACLE)
        .explainContains(""
            + "EnumerableAggregate(group=[{0}], m0=[COUNT($1)])\n"
            + "  EnumerableAggregate(group=[{1, 3}])\n"
            + "    EnumerableJoin(condition=[=($0, $2)], joinType=[inner])\n"
            + "      EnumerableCalc(expr#0..9=[{inputs}], expr#10=[CAST($t4):INTEGER], expr#11=[1997], expr#12=[=($t10, $t11)], time_id=[$t0], the_year=[$t4], $condition=[$t12])\n"
            + "        EnumerableTableScan(table=[[foodmart2, time_by_day]])\n"
            + "      EnumerableCalc(expr#0..7=[{inputs}], time_id=[$t1], unit_sales=[$t7])\n"
            + "        EnumerableTableScan(table=[[foodmart2, sales_fact_1997]])")
        .returns("c0=1997; m0=6\n");
  }

  @Test public void testDistinctCountComposite() {
    final String s = "select \"time_by_day\".\"the_year\" as \"c0\",\n"
        + " count(distinct \"sales_fact_1997\".\"product_id\",\n"
        + "       \"sales_fact_1997\".\"customer_id\") as \"m0\"\n"
        + "from \"time_by_day\" as \"time_by_day\",\n"
        + " \"sales_fact_1997\" as \"sales_fact_1997\"\n"
        + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
        + "and \"time_by_day\".\"the_year\" = 1997\n"
        + "group by \"time_by_day\".\"the_year\"";
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(s)
        .returns("c0=1997; m0=85452\n");
  }

  @Test public void testAggregateFilter() {
    final String s = "select \"the_month\",\n"
        + " count(*) as \"c\",\n"
        + " count(*) filter (where \"day_of_month\" > 20) as \"c2\"\n"
        + "from \"time_by_day\" as \"time_by_day\"\n"
        + "where \"time_by_day\".\"the_year\" = 1997\n"
        + "group by \"time_by_day\".\"the_month\"\n"
        + "order by \"time_by_day\".\"the_month\"";
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query(s)
        .returns("the_month=April; c=30; c2=10\n"
                + "the_month=August; c=31; c2=11\n"
                + "the_month=December; c=31; c2=11\n"
                + "the_month=February; c=28; c2=8\n"
                + "the_month=January; c=31; c2=11\n"
                + "the_month=July; c=31; c2=11\n"
                + "the_month=June; c=30; c2=10\n"
                + "the_month=March; c=31; c2=11\n"
                + "the_month=May; c=31; c2=11\n"
                + "the_month=November; c=30; c2=10\n"
                + "the_month=October; c=31; c2=11\n"
                + "the_month=September; c=30; c2=10\n");
  }

  /** Tests a simple IN query implemented as a semi-join. */
  @Test public void testSimpleIn() {
    CalciteAssert.hr()
        .query("select * from \"hr\".\"depts\" where \"deptno\" in (\n"
            + "  select \"deptno\" from \"hr\".\"emps\"\n"
            + "  where \"empid\" < 150)")
        .convertContains(""
            + "LogicalProject(deptno=[$0], name=[$1], employees=[$2], location=[$3])\n"
            + "  LogicalFilter(condition=[IN($0, {\n"
            + "LogicalProject(deptno=[$1])\n"
            + "  LogicalFilter(condition=[<($0, 150)])\n"
            + "    EnumerableTableScan(table=[[hr, emps]])\n"
            + "})])\n"
            + "    EnumerableTableScan(table=[[hr, depts]])")
        .explainContains(""
            + "EnumerableSemiJoin(condition=[=($0, $5)], joinType=[inner])\n"
            + "  EnumerableTableScan(table=[[hr, depts]])\n"
            + "  EnumerableCalc(expr#0..4=[{inputs}], expr#5=[150], expr#6=[<($t0, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "    EnumerableTableScan(table=[[hr, emps]])")
        .returnsUnordered(
            "deptno=10; name=Sales; employees=[{100, 10, Bill, 10000.0, 1000}, {150, 10, Sebastian, 7000.0, null}]; location={-122, 38}");
  }

  /** A difficult query: an IN list so large that the planner promotes it
   * to a semi-join against a VALUES relation. */
  @Ignore
  @Test public void testIn() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"time_by_day\".\"the_year\" as \"c0\",\n"
            + " \"product_class\".\"product_family\" as \"c1\",\n"
            + " \"customer\".\"country\" as \"c2\",\n"
            + " \"customer\".\"state_province\" as \"c3\",\n"
            + " \"customer\".\"city\" as \"c4\",\n"
            + " sum(\"sales_fact_1997\".\"unit_sales\") as \"m0\"\n"
            + "from \"time_by_day\" as \"time_by_day\",\n"
            + " \"sales_fact_1997\" as \"sales_fact_1997\",\n"
            + " \"product_class\" as \"product_class\",\n"
            + " \"product\" as \"product\", \"customer\" as \"customer\"\n"
            + "where \"sales_fact_1997\".\"time_id\" = \"time_by_day\".\"time_id\"\n"
            + "and \"time_by_day\".\"the_year\" = 1997\n"
            + "and \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "and \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\"\n"
            + "and \"product_class\".\"product_family\" = 'Drink'\n"
            + "and \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
            + "and \"customer\".\"country\" = 'USA'\n"
            + "and \"customer\".\"state_province\" = 'WA'\n"
            + "and \"customer\".\"city\" in ('Anacortes', 'Ballard', 'Bellingham', 'Bremerton', 'Burien', 'Edmonds', 'Everett', 'Issaquah', 'Kirkland', 'Lynnwood', 'Marysville', 'Olympia', 'Port Orchard', 'Puyallup', 'Redmond', 'Renton', 'Seattle', 'Sedro Woolley', 'Spokane', 'Tacoma', 'Walla Walla', 'Yakima')\n"
            + "group by \"time_by_day\".\"the_year\",\n"
            + " \"product_class\".\"product_family\",\n"
            + " \"customer\".\"country\",\n"
            + " \"customer\".\"state_province\",\n"
            + " \"customer\".\"city\"")
        .returns(
            "c0=1997; c1=Drink; c2=USA; c3=WA; c4=Sedro Woolley; m0=58.0000\n");
  }

  /** Query that uses parenthesized JOIN. */
  @Test public void testSql92JoinParenthesized() {
    if (!Bug.TODO_FIXED) {
      return;
    }
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select\n"
            + "   \"product_class\".\"product_family\" as \"c0\",\n"
            + "   \"product_class\".\"product_department\" as \"c1\",\n"
            + "   \"customer\".\"country\" as \"c2\",\n"
            + "   \"customer\".\"state_province\" as \"c3\",\n"
            + "   \"customer\".\"city\" as \"c4\"\n"
            + "from\n"
            + "   \"sales_fact_1997\" as \"sales_fact_1997\"\n"
            + "join (\"product\" as \"product\"\n"
            + "     join \"product_class\" as \"product_class\"\n"
            + "     on \"product\".\"product_class_id\" = \"product_class\".\"product_class_id\")\n"
            + "on  \"sales_fact_1997\".\"product_id\" = \"product\".\"product_id\"\n"
            + "join \"customer\" as \"customer\"\n"
            + "on  \"sales_fact_1997\".\"customer_id\" = \"customer\".\"customer_id\"\n"
            + "join \"promotion\" as \"promotion\"\n"
            + "on \"sales_fact_1997\".\"promotion_id\" = \"promotion\".\"promotion_id\"\n"
            + "where (\"promotion\".\"media_type\" = 'Radio'\n"
            + " or \"promotion\".\"media_type\" = 'TV'\n"
            + " or \"promotion\".\"media_type\" = 'Sunday Paper'\n"
            + " or \"promotion\".\"media_type\" = 'Street Handout')\n"
            + " and (\"product_class\".\"product_family\" = 'Drink')\n"
            + " and (\"customer\".\"country\" = 'USA' and \"customer\".\"state_province\""
            + " = 'WA' and \"customer\".\"city\" = 'Bellingham')\n"
            + "group by \"product_class\".\"product_family\",\n"
            + "   \"product_class\".\"product_department\",\n"
            + "   \"customer\".\"country\",\n"
            + "   \"customer\".\"state_province\",\n"
            + "   \"customer\".\"city\"\n"
            + "order by \"product_class\".\"product_family\" ASC,\n"
            + "   \"product_class\".\"product_department\" ASC,\n"
            + "   \"customer\".\"country\" ASC,\n"
            + "   \"customer\".\"state_province\" ASC,\n"
            + "   \"customer\".\"city\" ASC")
        .returns("+-------+---------------------+-----+------+------------+\n"
            + "| c0    | c1                  | c2  | c3   | c4         |\n"
            + "+-------+---------------------+-----+------+------------+\n"
            + "| Drink | Alcoholic Beverages | USA | WA   | Bellingham |\n"
            + "| Drink | Dairy               | USA | WA   | Bellingham |\n"
            + "+-------+---------------------+-----+------+------------+\n");
  }

  /** Tests ORDER BY with no options. Nulls come last.
   *
   * @see org.apache.calcite.avatica.AvaticaDatabaseMetaData#nullsAreSortedAtEnd()
   */
  @Test public void testOrderBy() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 3 order by 2")
        .returns("store_id=1; grocery_sqft=17475\n"
            + "store_id=2; grocery_sqft=22271\n"
            + "store_id=0; grocery_sqft=null\n");
  }

  /** Tests ORDER BY ... DESC. Nulls come first (they come last for ASC). */
  @Test public void testOrderByDesc() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 3 order by 2 desc")
        .returns("store_id=0; grocery_sqft=null\n"
            + "store_id=2; grocery_sqft=22271\n"
            + "store_id=1; grocery_sqft=17475\n");
  }

  /** Tests sorting by an expression not in the select clause. */
  @Test public void testOrderByExpr() {
    CalciteAssert.hr()
        .query("select \"name\", \"empid\" from \"hr\".\"emps\"\n"
            + "order by - \"empid\"")
        .returns("name=Eric; empid=200\n"
            + "name=Sebastian; empid=150\n"
            + "name=Theodore; empid=110\n"
            + "name=Bill; empid=100\n");
  }

  /** Tests sorting by an expression not in the '*' select clause. Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-176">[CALCITE-176]
   * ORDER BY expression doesn't work with SELECT *</a>. */
  @Test public void testOrderStarByExpr() {
    CalciteAssert.hr()
        .query("select * from \"hr\".\"emps\"\n"
            + "order by - \"empid\"")
        .explainContains("EnumerableSort(sort0=[$5], dir0=[ASC])\n"
            + "  EnumerableCalc(expr#0..4=[{inputs}], expr#5=[-($t0)], proj#0..5=[{exprs}])\n"
            + "    EnumerableTableScan(table=[[hr, emps]])")
        .returns(""
            + "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n"
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");
  }

  @Test public void testOrderUnionStarByExpr() {
    CalciteAssert.hr()
        .query("select * from \"hr\".\"emps\" where \"empid\" < 150\n"
            + "union all\n"
            + "select * from \"hr\".\"emps\" where \"empid\" > 150\n"
            + "order by - \"empid\"")
        .returns(""
            + "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n"
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");
  }

  /** Tests sorting by a CAST expression not in the select clause. */
  @Test public void testOrderByCast() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"customer_id\", \"postal_code\" from \"customer\"\n"
            + "where \"customer_id\" < 5\n"
            + "order by cast(substring(\"postal_code\" from 3) as integer) desc")
        // ordered by last 3 digits (980, 674, 172, 057)
        .returns("customer_id=3; postal_code=73980\n"
            + "customer_id=4; postal_code=74674\n"
            + "customer_id=2; postal_code=17172\n"
            + "customer_id=1; postal_code=15057\n");
  }

  /** Tests ORDER BY with all combinations of ASC, DESC, NULLS FIRST,
   * NULLS LAST. */
  @Test public void testOrderByNulls() {
    checkOrderByNulls(CalciteAssert.Config.FOODMART_CLONE);
    checkOrderByNulls(CalciteAssert.Config.JDBC_FOODMART);
  }

  private void checkOrderByNulls(CalciteAssert.Config clone) {
    checkOrderByDescNullsFirst(clone);
    checkOrderByNullsFirst(clone);
    checkOrderByDescNullsLast(clone);
    checkOrderByNullsLast(clone);
  }

  /** Tests ORDER BY ... DESC NULLS FIRST. */
  private void checkOrderByDescNullsFirst(CalciteAssert.Config config) {
    CalciteAssert.that()
        .with(config)
        .query("select \"store_id\", \"grocery_sqft\"\n"
            + "from \"foodmart\".\"store\"\n"
            + "where \"store_id\" < 3 order by 2 desc nulls first")
        .returns("store_id=0; grocery_sqft=null\n"
            + "store_id=2; grocery_sqft=22271\n"
            + "store_id=1; grocery_sqft=17475\n");
  }

  /** Tests ORDER BY ... NULLS FIRST. */
  private void checkOrderByNullsFirst(CalciteAssert.Config config) {
    CalciteAssert.that()
        .with(config)
        .query("select \"store_id\", \"grocery_sqft\"\n"
            + "from \"foodmart\".\"store\"\n"
            + "where \"store_id\" < 3 order by 2 nulls first")
        .returns("store_id=0; grocery_sqft=null\n"
            + "store_id=1; grocery_sqft=17475\n"
            + "store_id=2; grocery_sqft=22271\n");
  }

  /** Tests ORDER BY ... DESC NULLS LAST. */
  private void checkOrderByDescNullsLast(CalciteAssert.Config config) {
    CalciteAssert.that()
        .with(config)
        .query("select \"store_id\", \"grocery_sqft\"\n"
            + "from \"foodmart\".\"store\"\n"
            + "where \"store_id\" < 3 order by 2 desc nulls last")
        .returns("store_id=2; grocery_sqft=22271\n"
            + "store_id=1; grocery_sqft=17475\n"
            + "store_id=0; grocery_sqft=null\n");
  }

  /** Tests ORDER BY ... NULLS LAST. */
  private void checkOrderByNullsLast(CalciteAssert.Config config) {
    CalciteAssert.that()
        .with(config)
        .query("select \"store_id\", \"grocery_sqft\"\n"
            + "from \"foodmart\".\"store\"\n"
            + "where \"store_id\" < 3 order by 2 nulls last")
        .returns("store_id=1; grocery_sqft=17475\n"
            + "store_id=2; grocery_sqft=22271\n"
            + "store_id=0; grocery_sqft=null\n");
  }

  /** Tests ORDER BY ...  with various values of
   * {@link CalciteConnectionConfig#defaultNullCollation()}. */
  @Test public void testOrderByVarious() {
    final boolean[] booleans = {false, true};
    for (NullCollation nullCollation : NullCollation.values()) {
      for (boolean asc : booleans) {
        checkOrderBy(asc, nullCollation);
      }
    }
  }

  public void checkOrderBy(final boolean desc,
      final NullCollation nullCollation) {
    final Consumer<ResultSet> checker = resultSet -> {
      final String msg = (desc ? "DESC" : "ASC") + ":" + nullCollation;
      final List<Number> numbers = new ArrayList<>();
      try {
        while (resultSet.next()) {
          numbers.add((Number) resultSet.getObject(2));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      assertThat(msg, numbers.size(), is(3));
      assertThat(msg, numbers.get(nullCollation.last(desc) ? 2 : 0),
          nullValue());
    };
    final CalciteAssert.AssertThat with = CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .with(CalciteConnectionProperty.DEFAULT_NULL_COLLATION, nullCollation);
    final String sql = "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
        + "where \"store_id\" < 3 order by 2 "
        + (desc ? " DESC" : "");
    final String sql1 = "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
        + "where \"store_id\" < 3 order by \"florist\", 2 "
        + (desc ? " DESC" : "");
    final String sql2 = "select \"store_id\", \"grocery_sqft\" from \"store\"\n"
        + "where \"store_id\" < 3 order by 2 "
        + (desc ? " DESC" : "")
        + ", 1";
    with.query(sql).returns(checker);
    with.query(sql1).returns(checker);
    with.query(sql2).returns(checker);
  }

  /** Tests ORDER BY ... FETCH. */
  @Test public void testOrderByFetch() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 10\n"
            + "order by 1 fetch first 5 rows only")
        .explainContains("PLAN="
            + "EnumerableCalc(expr#0..23=[{inputs}], store_id=[$t0], grocery_sqft=[$t16])\n"
            + "  EnumerableLimit(fetch=[5])\n"
            + "    EnumerableCalc(expr#0..23=[{inputs}], expr#24=[10], expr#25=[<($t0, $t24)], proj#0..23=[{exprs}], $condition=[$t25])\n"
            + "      EnumerableTableScan(table=[[foodmart2, store]])\n")
        .returns("store_id=0; grocery_sqft=null\n"
            + "store_id=1; grocery_sqft=17475\n"
            + "store_id=2; grocery_sqft=22271\n"
            + "store_id=3; grocery_sqft=24390\n"
            + "store_id=4; grocery_sqft=16844\n");
  }

  /** Tests ORDER BY ... OFFSET ... FETCH. */
  @Test public void testOrderByOffsetFetch() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"store_id\", \"grocery_sqft\" from \"store\"\n"
            + "where \"store_id\" < 10\n"
            + "order by 1 offset 2 rows fetch next 5 rows only")
        .returns("store_id=2; grocery_sqft=22271\n"
            + "store_id=3; grocery_sqft=24390\n"
            + "store_id=4; grocery_sqft=16844\n"
            + "store_id=5; grocery_sqft=15012\n"
            + "store_id=6; grocery_sqft=15337\n");
  }

  /** Tests FETCH with no ORDER BY. */
  @Test public void testFetch() {
    CalciteAssert.hr()
        .query("select \"empid\" from \"hr\".\"emps\"\n"
            + "fetch first 2 rows only")
        .returns("empid=100\n"
            + "empid=200\n");
  }

  @Test public void testFetchStar() {
    CalciteAssert.hr()
        .query("select * from \"hr\".\"emps\"\n"
            + "fetch first 2 rows only")
        .returns(""
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n");
  }

  /** "SELECT ... LIMIT 0" is executed differently. A planner rule converts the
   * whole query to an empty rel. */
  @Test public void testLimitZero() {
    CalciteAssert.hr()
        .query("select * from \"hr\".\"emps\"\n"
            + "limit 0")
        .returns("")
        .planContains(
            "return org.apache.calcite.linq4j.Linq4j.asEnumerable(new Object[] {})");
  }

  /** Alternative formulation for {@link #testFetchStar()}. */
  @Test public void testLimitStar() {
    CalciteAssert.hr()
        .query("select * from \"hr\".\"emps\"\n"
            + "limit 2")
        .returns(""
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n");
  }

  /** Limit implemented using {@link Queryable#take}. Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-96">[CALCITE-96]
   * LIMIT against a table in a clone schema causes
   * UnsupportedOperationException</a>. */
  @Test public void testLimitOnQueryableTable() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select * from \"days\"\n"
            + "limit 2")
        .returns("day=1; week_day=Sunday\n"
            + "day=2; week_day=Monday\n");
  }

  /** Limit implemented using {@link Queryable#take}. Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-70">[CALCITE-70]
   * Joins seem to be very expensive in memory</a>. */
  @Test public void testSelfJoinCount() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query(
            "select count(*) as c from \"foodmart\".\"store\" as p1 join \"foodmart\".\"store\" as p2 using (\"store_id\")")
        .returns("C=25\n")
        .explainContains("JdbcToEnumerableConverter\n"
            + "  JdbcAggregate(group=[{}], C=[COUNT()])\n"
            + "    JdbcJoin(condition=[=($0, $1)], joinType=[inner])\n"
            + "      JdbcProject(store_id=[$0])\n"
            + "        JdbcTableScan(table=[[foodmart, store]])\n"
            + "      JdbcProject(store_id=[$0])\n"
            + "        JdbcTableScan(table=[[foodmart, store]])\n");
  }

  /** Tests composite GROUP BY where one of the columns has NULL values. */
  @Test public void testGroupByNull() {
    CalciteAssert.hr()
        .query("select \"deptno\", \"commission\", sum(\"salary\") s\n"
            + "from \"hr\".\"emps\"\n"
            + "group by \"deptno\", \"commission\"")
        .returnsUnordered(
            "deptno=10; commission=null; S=7000.0",
            "deptno=20; commission=500; S=8000.0",
            "deptno=10; commission=1000; S=10000.0",
            "deptno=10; commission=250; S=11500.0");
  }

  @Test public void testGroupingSets() {
    CalciteAssert.hr()
        .query("select \"deptno\", count(*) as c, sum(\"salary\") as s\n"
            + "from \"hr\".\"emps\"\n"
            + "group by grouping sets((\"deptno\"), ())")
        .returnsUnordered(
            "deptno=null; C=4; S=36500.0",
            "deptno=10; C=3; S=28500.0",
            "deptno=20; C=1; S=8000.0");
  }

  @Test public void testRollup() {
    CalciteAssert.hr()
        .query("select \"deptno\", count(*) as c, sum(\"salary\") as s\n"
            + "from \"hr\".\"emps\"\n"
            + "group by rollup(\"deptno\")")
        .returnsUnordered(
            "deptno=null; C=4; S=36500.0",
            "deptno=10; C=3; S=28500.0",
            "deptno=20; C=1; S=8000.0");
  }

  @Test public void testSelectDistinct() {
    CalciteAssert.hr()
        .query("select distinct \"deptno\"\n"
            + "from \"hr\".\"emps\"\n")
        .returnsUnordered(
            "deptno=10",
            "deptno=20");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-397">[CALCITE-397]
   * "SELECT DISTINCT *" on reflective schema gives ClassCastException at
   * runtime</a>. */
  @Test public void testSelectDistinctStar() {
    CalciteAssert.hr()
        .query("select distinct *\n"
            + "from \"hr\".\"emps\"\n")
        .returnsCount(4)
        .planContains(".distinct(");
  }

  /** Select distinct on composite key, one column of which is boolean to
   * boot. */
  @Test public void testSelectDistinctComposite() {
    CalciteAssert.hr()
        .query("select distinct \"empid\" > 140 as c, \"deptno\"\n"
            + "from \"hr\".\"emps\"\n")
        .returnsUnordered(
            "C=false; deptno=10",
            "C=true; deptno=10",
            "C=true; deptno=20")
        .planContains(".distinct(");
  }

  /** Same result (and plan) as {@link #testSelectDistinct}. */
  @Test public void testGroupByNoAggregates() {
    CalciteAssert.hr()
        .query("select \"deptno\"\n"
            + "from \"hr\".\"emps\"\n"
            + "group by \"deptno\"")
        .returnsUnordered(
            "deptno=10",
            "deptno=20");
  }

  /** Same result (and plan) as {@link #testSelectDistinct}. */
  @Test public void testGroupByNoAggregatesAllColumns() {
    CalciteAssert.hr()
        .query("select \"deptno\"\n"
            + "from \"hr\".\"emps\"\n"
            + "group by \"deptno\", \"empid\", \"name\", \"salary\", \"commission\"")
        .returnsCount(4)
        .planContains(".distinct(");
  }

  /** Same result (and plan) as {@link #testSelectDistinct}. */
  @Test public void testGroupByMax1IsNull() {
    CalciteAssert.hr()
        .query("select * from (\n"
            + "select max(1) max_id\n"
            + "from \"hr\".\"emps\" where 1=2\n"
            + ") where max_id is null")
        .returnsUnordered(
            "MAX_ID=null");
  }

  /** Same result (and plan) as {@link #testSelectDistinct}. */
  @Test public void testGroupBy1Max1() {
    CalciteAssert.hr()
        .query("select * from (\n"
            + "select max(u) max_id\n"
            + "from (select \"empid\"+\"deptno\" u, 1 cnst\n"
            + "from \"hr\".\"emps\" a) where 1=2\n"
            + "group by cnst\n"
            + ") where max_id is null")
        .returnsCount(0);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-403">[CALCITE-403]
   * Enumerable gives NullPointerException with NOT on nullable
   * expression</a>. */
  @Test public void testHavingNot() throws IOException {
    withFoodMartQuery(6597).runs();
  }

  /** Minimal case of {@link #testHavingNot()}. */
  @Test public void testHavingNot2() throws IOException {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select 1\n"
            + "from \"store\"\n"
            + "group by \"store\".\"store_street_address\"\n"
            + "having NOT (sum(\"store\".\"grocery_sqft\") < 20000)")
        .returnsCount(10);
  }

  /** ORDER BY on a sort-key does not require a sort. */
  @Test public void testOrderOnSortedTable() throws IOException {
    // The ArrayTable "store" is sorted by "store_id".
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"day\"\n"
            + "from \"days\"\n"
            + "order by \"day\"")
        .returns("day=1\n"
            + "day=2\n"
            + "day=3\n"
            + "day=4\n"
            + "day=5\n"
            + "day=6\n"
            + "day=7\n");
  }

  /** ORDER BY on a sort-key does not require a sort. */
  @Test public void testOrderSorted() throws IOException {
    // The ArrayTable "store" is sorted by "store_id".
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"store_id\"\n"
            + "from \"store\"\n"
            + "order by \"store_id\" limit 3")
        .returns("store_id=0\n"
            + "store_id=1\n"
            + "store_id=2\n");
  }

  @Test public void testWhereNot() throws IOException {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select 1\n"
            + "from \"store\"\n"
            + "where NOT (\"store\".\"grocery_sqft\" < 22000)\n"
            + "group by \"store\".\"store_street_address\"\n")
        .returnsCount(8);
  }

  /** Query that reads no columns from either underlying table. */
  @Test public void testCountStar() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      CalciteAssert.hr()
          .query("select count(*) c from \"hr\".\"emps\", \"hr\".\"depts\"")
          .convertContains("LogicalAggregate(group=[{}], C=[COUNT()])\n"
              + "  LogicalProject(DUMMY=[0])\n"
              + "    LogicalJoin(condition=[true], joinType=[inner])\n"
              + "      LogicalProject(DUMMY=[0])\n"
              + "        EnumerableTableScan(table=[[hr, emps]])\n"
              + "      LogicalProject(DUMMY=[0])\n"
              + "        EnumerableTableScan(table=[[hr, depts]])");
    }
  }

  /** Same result (and plan) as {@link #testSelectDistinct}. */
  @Test public void testCountUnionAll() {
    CalciteAssert.hr()
        .query("select count(*) c from (\n"
            + "select * from \"hr\".\"emps\" where 1=2\n"
            + "union all\n"
            + "select * from \"hr\".\"emps\" where 3=4\n"
            + ")")
        .returnsUnordered(
            "C=0");
  }

  @Test public void testUnionAll() {
    CalciteAssert.hr()
        .query("select \"empid\", \"name\" from \"hr\".\"emps\" where \"deptno\"=10\n"
            + "union all\n"
            + "select \"empid\", \"name\" from \"hr\".\"emps\" where \"empid\">=150")
        .explainContains(""
            + "PLAN=EnumerableUnion(all=[true])")
        .returnsUnordered("empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian",
            "empid=150; name=Sebastian",
            "empid=200; name=Eric");
  }

  @Test public void testUnion() {
    final String sql = ""
        + "select \"empid\", \"name\" from \"hr\".\"emps\" where \"deptno\"=10\n"
        + "union\n"
        + "select \"empid\", \"name\" from \"hr\".\"emps\" where \"empid\">=150";
    CalciteAssert.hr()
        .query(sql)
        .explainContains(""
            + "PLAN=EnumerableAggregate(group=[{0, 1}])\n"
            + "  EnumerableUnion(all=[true])\n"
            + "    EnumerableAggregate(group=[{0, 2}])\n"
            + "      EnumerableCalc(expr#0..4=[{inputs}], expr#5=[CAST($t1):INTEGER NOT NULL], expr#6=[10], expr#7=[=($t5, $t6)], proj#0..4=[{exprs}], $condition=[$t7])\n"
            + "        EnumerableTableScan(table=[[hr, emps]])\n"
            + "    EnumerableAggregate(group=[{0, 2}])\n"
            + "      EnumerableCalc(expr#0..4=[{inputs}], expr#5=[150], expr#6=[>=($t0, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "        EnumerableTableScan(table=[[hr, emps]])")
        .returnsUnordered("empid=100; name=Bill",
            "empid=110; name=Theodore",
            "empid=150; name=Sebastian",
            "empid=200; name=Eric");
  }

  @Test public void testIntersect() {
    final String sql = ""
        + "select \"empid\", \"name\" from \"hr\".\"emps\" where \"deptno\"=10\n"
        + "intersect\n"
        + "select \"empid\", \"name\" from \"hr\".\"emps\" where \"empid\">=150";
    CalciteAssert.hr()
        .query(sql)
        .withHook(Hook.PLANNER, (Consumer<RelOptPlanner>) planner ->
            planner.removeRule(IntersectToDistinctRule.INSTANCE))
        .explainContains(""
            + "PLAN=EnumerableIntersect(all=[false])")
        .returnsUnordered("empid=150; name=Sebastian");
  }

  @Test public void testExcept() {
    final String sql = ""
        + "select \"empid\", \"name\" from \"hr\".\"emps\" where \"deptno\"=10\n"
        + "except\n"
        + "select \"empid\", \"name\" from \"hr\".\"emps\" where \"empid\">=150";
    CalciteAssert.hr()
        .query(sql)
        .explainContains(""
            + "PLAN=EnumerableMinus(all=[false])")
        .returnsUnordered("empid=100; name=Bill",
            "empid=110; name=Theodore");
  }

  /** Tests that SUM and AVG over empty set return null. COUNT returns 0. */
  @Test public void testAggregateEmpty() {
    CalciteAssert.hr()
        .query("select\n"
            + " count(*) as cs,\n"
            + " count(\"deptno\") as c,\n"
            + " sum(\"deptno\") as s,\n"
            + " avg(\"deptno\") as a\n"
            + "from \"hr\".\"emps\"\n"
            + "where \"deptno\" < 0")
        .explainContains(""
            + "PLAN=EnumerableCalc(expr#0..1=[{inputs}], expr#2=[0], expr#3=[=($t0, $t2)], expr#4=[null], expr#5=[CASE($t3, $t4, $t1)], expr#6=[/($t5, $t0)], expr#7=[CAST($t6):JavaType(class java.lang.Integer)], CS=[$t0], C=[$t0], S=[$t5], A=[$t7])\n"
            + "  EnumerableAggregate(group=[{}], CS=[COUNT()], S=[$SUM0($1)])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[0], expr#6=[<($t1, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[hr, emps]])\n")
        .returns("CS=0; C=0; S=null; A=null\n");
  }

  /** Tests that count(deptno) is reduced to count(). */
  @Test public void testReduceCountNotNullable() {
    CalciteAssert.hr()
        .query("select\n"
            + " count(\"deptno\") as cs,\n"
            + " count(*) as cs2\n"
            + "from \"hr\".\"emps\"\n"
            + "where \"deptno\" < 0")
        .explainContains(""
            + "PLAN=EnumerableCalc(expr#0=[{inputs}], CS=[$t0], CS2=[$t0])\n"
            + "  EnumerableAggregate(group=[{}], CS=[COUNT()])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[0], expr#6=[<($t1, $t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
            + "      EnumerableTableScan(table=[[hr, emps]])\n")
        .returns("CS=0; CS2=0\n");
  }

  /** Tests that {@code count(deptno, commission, commission + 1)} is reduced to
   * {@code count(commission, commission + 1)}, because deptno is NOT NULL. */
  @Test public void testReduceCompositeCountNotNullable() {
    CalciteAssert.hr()
        .query("select\n"
            + " count(\"deptno\", \"commission\", \"commission\" + 1) as cs\n"
            + "from \"hr\".\"emps\"")
        .explainContains(""
            + "EnumerableAggregate(group=[{}], CS=[COUNT($0, $1)])\n"
            + "  EnumerableCalc(expr#0..4=[{inputs}], expr#5=[1], expr#6=[+($t4, $t5)], commission=[$t4], $f2=[$t6])\n"
            + "    EnumerableTableScan(table=[[hr, emps]])")
        .returns("CS=3\n");
  }

  /** Tests sorting by a column that is already sorted. */
  @Test public void testOrderByOnSortedTable() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select * from \"time_by_day\"\n"
            + "order by \"time_id\"")
        .explainContains(
            "PLAN=EnumerableTableScan(table=[[foodmart2, time_by_day]])\n");
  }

  /** Tests sorting by a column that is already sorted. */
  @Test public void testOrderByOnSortedTable2() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .query("select \"time_id\", \"the_date\" from \"time_by_day\"\n"
            + "where \"time_id\" < 370\n"
            + "order by \"time_id\"")
        .returns("time_id=367; the_date=1997-01-01 00:00:00\n"
            + "time_id=368; the_date=1997-01-02 00:00:00\n"
            + "time_id=369; the_date=1997-01-03 00:00:00\n")
        .explainContains(""
            + "PLAN=EnumerableCalc(expr#0..9=[{inputs}], expr#10=[370], expr#11=[<($t0, $t10)], proj#0..1=[{exprs}], $condition=[$t11])\n"
            + "  EnumerableTableScan(table=[[foodmart2, time_by_day]])\n\n");
  }

  @Test public void testWithInsideWhereExists() {
    CalciteAssert.hr()
        .query("select \"deptno\" from \"hr\".\"emps\"\n"
            + "where exists (\n"
            + "  with dept2 as (select * from \"hr\".\"depts\" where \"depts\".\"deptno\" >= \"emps\".\"deptno\")\n"
            + "  select 1 from dept2 where \"deptno\" <= \"emps\".\"deptno\")")
        .returnsUnordered("deptno=10",
            "deptno=10",
            "deptno=10");
  }

  @Test public void testWithOrderBy() {
    CalciteAssert.hr()
        .query("with emp2 as (select * from \"hr\".\"emps\")\n"
            + "select * from emp2\n"
            + "order by \"deptno\" desc, \"empid\" desc")
        .returns(""
            + "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n"
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");
  }

  /** Tests windowed aggregation. */
  @Test public void testWinAgg() {
    CalciteAssert.hr()
        .query("select"
            + " \"deptno\",\n"
            + " \"empid\",\n"
            + "sum(\"salary\" + \"empid\") over w as s,\n"
            + " 5 as five,\n"
            + " min(\"salary\") over w as m,\n"
            + " count(*) over w as c\n"
            + "from \"hr\".\"emps\"\n"
            + "window w as (partition by \"deptno\" order by \"empid\" rows 1 preceding)")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, S REAL, FIVE INTEGER NOT NULL, M REAL, C BIGINT NOT NULL]")
        .explainContains(""
            + "EnumerableCalc(expr#0..7=[{inputs}], expr#8=[0], expr#9=[>($t4, $t8)], expr#10=[null], expr#11=[CASE($t9, $t5, $t10)], expr#12=[5], deptno=[$t1], empid=[$t0], S=[$t11], FIVE=[$t12], M=[$t6], C=[$t7])\n"
            + "  EnumerableWindow(window#0=[window(partition {1} order by [0] rows between $4 PRECEDING and CURRENT ROW aggs [COUNT($3), $SUM0($3), MIN($2), COUNT()])])\n"
            + "    EnumerableCalc(expr#0..4=[{inputs}], expr#5=[+($t3, $t0)], proj#0..1=[{exprs}], salary=[$t3], $3=[$t5])\n"
            + "      EnumerableTableScan(table=[[hr, emps]])\n")
        .returnsUnordered(
            "deptno=10; empid=100; S=10100.0; FIVE=5; M=10000.0; C=1",
            "deptno=10; empid=110; S=21710.0; FIVE=5; M=10000.0; C=2",
            "deptno=10; empid=150; S=18760.0; FIVE=5; M=7000.0; C=2",
            "deptno=20; empid=200; S=8200.0; FIVE=5; M=8000.0; C=1")
        .planContains(CalcitePrepareImpl.DEBUG
            ? "_list.add(new Object[] {\n"
            + "        row[0],\n" // box-unbox is optimized
            + "        row[1],\n"
            + "        row[2],\n"
            + "        row[3],\n"
            + "        COUNTa0w0,\n"
            + "        $SUM0a1w0,\n"
            + "        MINa2w0,\n"
            + "        COUNTa3w0});"
            : "_list.add(new Object[] {\n"
                + "        row[0],\n" // box-unbox is optimized
                + "        row[1],\n"
                + "        row[2],\n"
                + "        row[3],\n"
                + "        a0w0,\n"
                + "        a1w0,\n"
                + "        a2w0,\n"
                + "        a3w0});")
        .planContains("return new Object[] {\n"
            + "                  current[1],\n"
            + "                  current[0],\n"
            // Float.valueOf(SqlFunctions.toFloat(current[5])) comes from SUM0
            + "                  org.apache.calcite.runtime.SqlFunctions.toLong(current[4]) > 0L ? Float.valueOf(org.apache.calcite.runtime.SqlFunctions.toFloat(current[5])) : (Float) null,\n"
            + "                  5,\n"
            + "                  current[6],\n"
            + "                  current[7]};\n");
  }

  /** Tests windowed aggregation with multiple windows.
   * One window straddles the current row.
   * Some windows have no PARTITION BY clause. */
  @Test public void testWinAgg2() {
    CalciteAssert.hr()
        .query("select"
            + " \"deptno\",\n"
            + " \"empid\",\n"
            + "sum(\"salary\" + \"empid\") over w as s,\n"
            + " 5 as five,\n"
            + " min(\"salary\") over w as m,\n"
            + " count(*) over w as c,\n"
            + " count(*) over w2 as c2,\n"
            + " count(*) over w11 as c11,\n"
            + " count(*) over w11dept as c11dept\n"
            + "from \"hr\".\"emps\"\n"
            + "window w as (order by \"empid\" rows 1 preceding),\n"
            + " w2 as (order by \"empid\" rows 2 preceding),\n"
            + " w11 as (order by \"empid\" rows between 1 preceding and 1 following),\n"
            + " w11dept as (partition by \"deptno\" order by \"empid\" rows between 1 preceding and 1 following)")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, S REAL, FIVE INTEGER NOT NULL, M REAL, C BIGINT NOT NULL, C2 BIGINT NOT NULL, C11 BIGINT NOT NULL, C11DEPT BIGINT NOT NULL]")
        // Check that optimizes for window whose PARTITION KEY is empty
        .planContains("tempList.size()")
        .returnsUnordered(
            "deptno=20; empid=200; S=15350.0; FIVE=5; M=7000.0; C=2; C2=3; C11=2; C11DEPT=1",
            "deptno=10; empid=100; S=10100.0; FIVE=5; M=10000.0; C=1; C2=1; C11=2; C11DEPT=2",
            "deptno=10; empid=110; S=21710.0; FIVE=5; M=10000.0; C=2; C2=2; C11=3; C11DEPT=3",
            "deptno=10; empid=150; S=18760.0; FIVE=5; M=7000.0; C=2; C2=3; C11=3; C11DEPT=2");
  }

  /**
   * Tests that window aggregates work when computed over non-nullable
   * {@link org.apache.calcite.adapter.enumerable.JavaRowFormat#SCALAR} inputs.
   * Window aggregates use temporary buffers, thus need to check if
   * primitives are properly boxed and un-boxed.
   */
  @Test public void testWinAggScalarNonNullPhysType() {
    String planLine =
        "a0s0w0 = org.apache.calcite.runtime.SqlFunctions.lesser(a0s0w0, org.apache.calcite.runtime.SqlFunctions.toFloat(_rows[j]));";
    if (CalcitePrepareImpl.DEBUG) {
      planLine = planLine.replaceAll("a0s0w0", "MINa0s0w0");
    }
    CalciteAssert.hr()
        .query("select min(\"salary\"+1) over w as m\n"
            + "from \"hr\".\"emps\"\n"
            + "window w as (order by \"salary\"+1 rows 1 preceding)\n")
        .typeIs(
            "[M REAL]")
        .planContains(planLine)
        .returnsUnordered(
            "M=7001.0",
            "M=7001.0",
            "M=8001.0",
            "M=10001.0");
  }

  /**
   * Tests that {@link org.apache.calcite.rel.logical.LogicalCalc} is
   * implemented properly when input is
   * {@link org.apache.calcite.rel.logical.LogicalWindow} and literal.
   */
  @Test public void testWinAggScalarNonNullPhysTypePlusOne() {
    String planLine =
        "a0s0w0 = org.apache.calcite.runtime.SqlFunctions.lesser(a0s0w0, org.apache.calcite.runtime.SqlFunctions.toFloat(_rows[j]));";
    if (CalcitePrepareImpl.DEBUG) {
      planLine = planLine.replaceAll("a0s0w0", "MINa0s0w0");
    }
    CalciteAssert.hr()
        .query("select 1+min(\"salary\"+1) over w as m\n"
            + "from \"hr\".\"emps\"\n"
            + "window w as (order by \"salary\"+1 rows 1 preceding)\n")
        .typeIs(
            "[M REAL]")
        .planContains(planLine)
        .returnsUnordered(
            "M=7002.0",
            "M=7002.0",
            "M=8002.0",
            "M=10002.0");
  }

  /** Tests for RANK and ORDER BY ... DESCENDING, NULLS FIRST, NULLS LAST. */
  @Test public void testWinAggRank() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " \"empid\",\n"
            + " \"commission\",\n"
            + " rank() over (partition by \"deptno\" order by \"commission\" desc nulls first) as rcnf,\n"
            + " rank() over (partition by \"deptno\" order by \"commission\" desc nulls last) as rcnl,\n"
            + " rank() over (partition by \"deptno\" order by \"empid\") as r,\n"
            + " rank() over (partition by \"deptno\" order by \"empid\" desc) as rd\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, commission INTEGER, RCNF BIGINT NOT NULL, RCNL BIGINT NOT NULL, R BIGINT NOT NULL, RD BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; empid=100; commission=1000; RCNF=2; RCNL=1; R=1; RD=3",
            "deptno=10; empid=110; commission=250; RCNF=3; RCNL=2; R=2; RD=2",
            "deptno=10; empid=150; commission=null; RCNF=1; RCNL=3; R=3; RD=1",
            "deptno=20; empid=200; commission=500; RCNF=1; RCNL=1; R=1; RD=1");
  }

  /** Tests for RANK with same values */
  @Test public void testWinAggRankValues() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " rank() over (order by \"deptno\") as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, R BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; R=1",
            "deptno=10; R=1",
            "deptno=10; R=1",
            "deptno=20; R=4"); // 4 for rank and 2 for dense_rank
  }

  /** Tests for RANK with same values */
  @Test public void testWinAggRankValuesDesc() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " rank() over (order by \"deptno\" desc) as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, R BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; R=2",
            "deptno=10; R=2",
            "deptno=10; R=2",
            "deptno=20; R=1");
  }

  /** Tests for DENSE_RANK with same values */
  @Test public void testWinAggDenseRankValues() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " dense_rank() over (order by \"deptno\") as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, R BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; R=1",
            "deptno=10; R=1",
            "deptno=10; R=1",
            "deptno=20; R=2");
  }

  /** Tests for DENSE_RANK with same values */
  @Test public void testWinAggDenseRankValuesDesc() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " dense_rank() over (order by \"deptno\" desc) as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, R BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; R=2",
            "deptno=10; R=2",
            "deptno=10; R=2",
            "deptno=20; R=1");
  }

  /** Tests for DATE +- INTERVAL window frame */
  @Test public void testWinIntervalFrame() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " \"empid\",\n"
            + " \"hire_date\",\n"
            + " count(*) over (partition by \"deptno\" order by \"hire_date\""
            + " range between interval '1' year preceding and interval '1' year following) as r\n"
            + "from (select \"empid\", \"deptno\",\n"
            + "  DATE '2014-06-12' + \"empid\"*interval '0' day \"hire_date\"\n"
            + "  from \"hr\".\"emps\")")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, hire_date DATE NOT NULL, R BIGINT NOT NULL]")
        .returnsUnordered("deptno=10; empid=100; hire_date=2014-06-12; R=3",
            "deptno=10; empid=110; hire_date=2014-06-12; R=3",
            "deptno=10; empid=150; hire_date=2014-06-12; R=3",
            "deptno=20; empid=200; hire_date=2014-06-12; R=1");
  }

  private void startOfGroupStep1(String startOfGroup) {
    CalciteAssert.that()
        .query("select t.*\n"
            + "  from (\n"
            + "       select  t.*,\n"
            + "               case when " + startOfGroup
            + " then 0 else 1 end start_of_group\n"
            + "         from "
            + START_OF_GROUP_DATA
            + ") t\n")
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, START_OF_GROUP INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; VAL=0; EXPECTED=1; START_OF_GROUP=1",
            "RN=2; VAL=0; EXPECTED=1; START_OF_GROUP=0",
            "RN=3; VAL=1; EXPECTED=2; START_OF_GROUP=1",
            "RN=4; VAL=0; EXPECTED=3; START_OF_GROUP=1",
            "RN=5; VAL=0; EXPECTED=3; START_OF_GROUP=0",
            "RN=6; VAL=0; EXPECTED=3; START_OF_GROUP=0",
            "RN=7; VAL=1; EXPECTED=4; START_OF_GROUP=1",
            "RN=8; VAL=1; EXPECTED=4; START_OF_GROUP=0");
  }

  private void startOfGroupStep2(String startOfGroup) {
    CalciteAssert.that()
        .query("select t.*\n"
            // current row is assumed, group_id should be NOT NULL
            + "       ,sum(start_of_group) over (order by rn rows unbounded preceding) group_id\n"
            + "  from (\n"
            + "       select  t.*,\n"
            + "               case when " + startOfGroup
            + " then 0 else 1 end start_of_group\n"
            + "         from "
            + START_OF_GROUP_DATA
            + ") t\n")
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, START_OF_GROUP INTEGER NOT NULL, GROUP_ID INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; VAL=0; EXPECTED=1; START_OF_GROUP=1; GROUP_ID=1",
            "RN=2; VAL=0; EXPECTED=1; START_OF_GROUP=0; GROUP_ID=1",
            "RN=3; VAL=1; EXPECTED=2; START_OF_GROUP=1; GROUP_ID=2",
            "RN=4; VAL=0; EXPECTED=3; START_OF_GROUP=1; GROUP_ID=3",
            "RN=5; VAL=0; EXPECTED=3; START_OF_GROUP=0; GROUP_ID=3",
            "RN=6; VAL=0; EXPECTED=3; START_OF_GROUP=0; GROUP_ID=3",
            "RN=7; VAL=1; EXPECTED=4; START_OF_GROUP=1; GROUP_ID=4",
            "RN=8; VAL=1; EXPECTED=4; START_OF_GROUP=0; GROUP_ID=4");
  }

  private void startOfGroupStep3(String startOfGroup) {
    CalciteAssert.that()
        .query("select group_id, min(rn) min_rn, max(rn) max_rn,\n"
            + "  count(rn) cnt_rn, avg(val) avg_val"
            + " from (\n"
            + "select t.*\n"
            // current row is assumed, group_id should be NOT NULL
            + "       ,sum(start_of_group) over (order by rn rows unbounded preceding) group_id\n"
            + "  from (\n"
            + "       select  t.*,\n"
            + "               case when " + startOfGroup
            + " then 0 else 1 end start_of_group\n"
            + "         from "
            + START_OF_GROUP_DATA
            + ") t\n"
            + ") group by group_id\n")
        .typeIs(
            "[GROUP_ID INTEGER NOT NULL, MIN_RN INTEGER NOT NULL, MAX_RN INTEGER NOT NULL, CNT_RN BIGINT NOT NULL, AVG_VAL INTEGER NOT NULL]")
        .returnsUnordered(
            "GROUP_ID=1; MIN_RN=1; MAX_RN=2; CNT_RN=2; AVG_VAL=0",
            "GROUP_ID=2; MIN_RN=3; MAX_RN=3; CNT_RN=1; AVG_VAL=1",
            "GROUP_ID=3; MIN_RN=4; MAX_RN=6; CNT_RN=3; AVG_VAL=0",
            "GROUP_ID=4; MIN_RN=7; MAX_RN=8; CNT_RN=2; AVG_VAL=1");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step1, implemented as last_value.
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test public void testStartOfGroupLastValueStep1() {
    startOfGroupStep1(
        "val = last_value(val) over (order by rn rows between 1 preceding and 1 preceding)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step2, that gets the final group numbers
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test public void testStartOfGroupLastValueStep2() {
    startOfGroupStep2(
        "val = last_value(val) over (order by rn rows between 1 preceding and 1 preceding)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step3, that aggregates the computed groups
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test public void testStartOfGroupLastValueStep3() {
    startOfGroupStep3(
        "val = last_value(val) over (order by rn rows between 1 preceding and 1 preceding)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step1, implemented as last_value.
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test public void testStartOfGroupLagStep1() {
    startOfGroupStep1("val = lag(val) over (order by rn)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step2, that gets the final group numbers
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test public void testStartOfGroupLagValueStep2() {
    startOfGroupStep2("val = lag(val) over (order by rn)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step3, that aggregates the computed groups
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test public void testStartOfGroupLagStep3() {
    startOfGroupStep3("val = lag(val) over (order by rn)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step1, implemented as last_value.
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test public void testStartOfGroupLeadStep1() {
    startOfGroupStep1("val = lead(val, -1) over (order by rn)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step2, that gets the final group numbers
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test public void testStartOfGroupLeadValueStep2() {
    startOfGroupStep2("val = lead(val, -1) over (order by rn)");
  }

  /**
   * Tests start_of_group approach for grouping of adjacent intervals.
   * This is a step3, that aggregates the computed groups
   * http://timurakhmadeev.wordpress.com/2013/07/21/start_of_group/
   */
  @Test public void testStartOfGroupLeadStep3() {
    startOfGroupStep3("val = lead(val, -1) over (order by rn)");
  }

  /**
   * Tests default value of LAG function.
   */
  @Test public void testLagDefaultValue() {
    CalciteAssert.that()
        .query("select t.*, lag(rn+expected,1,42) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, L INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; VAL=0; EXPECTED=1; L=42",
            "RN=2; VAL=0; EXPECTED=1; L=2",
            "RN=3; VAL=1; EXPECTED=2; L=3",
            "RN=4; VAL=0; EXPECTED=3; L=5",
            "RN=5; VAL=0; EXPECTED=3; L=7",
            "RN=6; VAL=0; EXPECTED=3; L=8",
            "RN=7; VAL=1; EXPECTED=4; L=9",
            "RN=8; VAL=1; EXPECTED=4; L=11");
  }

  /**
   * Tests default value of LEAD function.
   */
  @Test public void testLeadDefaultValue() {
    CalciteAssert.that()
        .query("select t.*, lead(rn+expected,1,42) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, L INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; VAL=0; EXPECTED=1; L=3",
            "RN=2; VAL=0; EXPECTED=1; L=5",
            "RN=3; VAL=1; EXPECTED=2; L=7",
            "RN=4; VAL=0; EXPECTED=3; L=8",
            "RN=5; VAL=0; EXPECTED=3; L=9",
            "RN=6; VAL=0; EXPECTED=3; L=11",
            "RN=7; VAL=1; EXPECTED=4; L=12",
            "RN=8; VAL=1; EXPECTED=4; L=42");
  }

  /**
   * Tests expression in offset value of LAG function.
   */
  @Test public void testLagExpressionOffset() {
    CalciteAssert.that()
        .query("select t.*, lag(rn, expected, 42) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, L INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; VAL=0; EXPECTED=1; L=42",
            "RN=2; VAL=0; EXPECTED=1; L=1",
            "RN=3; VAL=1; EXPECTED=2; L=1",
            "RN=4; VAL=0; EXPECTED=3; L=1",
            "RN=5; VAL=0; EXPECTED=3; L=2",
            "RN=6; VAL=0; EXPECTED=3; L=3",
            "RN=7; VAL=1; EXPECTED=4; L=3",
            "RN=8; VAL=1; EXPECTED=4; L=4");
  }

  /**
   * Tests DATE as offset argument of LAG function.
   */
  @Test public void testLagInvalidOffsetArgument() {
    CalciteAssert.that()
        .query("select t.*,\n"
            + "  lag(rn, DATE '2014-06-20', 42) over (order by rn) l\n"
            + "from " + START_OF_GROUP_DATA)
        .throws_(
            "Cannot apply 'LAG' to arguments of type 'LAG(<INTEGER>, <DATE>, <INTEGER>)'");
  }

  /**
   * Tests NTILE(2).
   */
  @Test public void testNtile1() {
    CalciteAssert.that()
        .query("select rn, ntile(1) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, L BIGINT NOT NULL]")
        .returnsUnordered(
            "RN=1; L=1",
            "RN=2; L=1",
            "RN=3; L=1",
            "RN=4; L=1",
            "RN=5; L=1",
            "RN=6; L=1",
            "RN=7; L=1",
            "RN=8; L=1");
  }

  /**
   * Tests NTILE(2).
   */
  @Test public void testNtile2() {
    CalciteAssert.that()
        .query("select rn, ntile(2) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, L BIGINT NOT NULL]")
        .returnsUnordered(
            "RN=1; L=1",
            "RN=2; L=1",
            "RN=3; L=1",
            "RN=4; L=1",
            "RN=5; L=2",
            "RN=6; L=2",
            "RN=7; L=2",
            "RN=8; L=2");
  }

  /**
   * Tests expression in offset value of LAG function.
   */
  @Ignore("Have no idea how to validate that expression is constant")
  @Test public void testNtileConstantArgs() {
    CalciteAssert.that()
        .query("select rn, ntile(1+1) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .typeIs(
            "[RN INTEGER NOT NULL, VAL INTEGER NOT NULL, EXPECTED INTEGER NOT NULL, L INTEGER NOT NULL]")
        .returnsUnordered(
            "RN=1; L=1",
            "RN=2; L=1",
            "RN=3; L=1",
            "RN=4; L=1",
            "RN=5; L=2",
            "RN=6; L=2",
            "RN=7; L=2",
            "RN=8; L=2");
  }

  /**
   * Tests expression in offset value of LAG function.
   */
  @Test public void testNtileNegativeArg() {
    CalciteAssert.that()
        .query("select rn, ntile(-1) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .throws_(
            "Argument to function 'NTILE' must be a positive integer literal");
  }

  /**
   * Tests expression in offset value of LAG function.
   */
  @Test public void testNtileDecimalArg() {
    CalciteAssert.that()
        .query("select rn, ntile(3.141592653) over (order by rn) l\n"
            + " from " + START_OF_GROUP_DATA)
        .throws_(
            "Cannot apply 'NTILE' to arguments of type 'NTILE(<DECIMAL(10, 9)>)'");
  }

  /** Tests for FIRST_VALUE */
  @Test public void testWinAggFirstValue() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " \"empid\",\n"
            + " \"commission\",\n"
            + " first_value(\"commission\") over (partition by \"deptno\" order by \"empid\") as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, commission INTEGER, R INTEGER]")
        .returnsUnordered(
            "deptno=10; empid=100; commission=1000; R=1000",
            "deptno=10; empid=110; commission=250; R=1000",
            "deptno=10; empid=150; commission=null; R=1000",
            "deptno=20; empid=200; commission=500; R=500");
  }

  /** Tests for FIRST_VALUE desc */
  @Test public void testWinAggFirstValueDesc() {
    CalciteAssert.hr()
        .query("select  \"deptno\",\n"
            + " \"empid\",\n"
            + " \"commission\",\n"
            + " first_value(\"commission\") over (partition by \"deptno\" order by \"empid\" desc) as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, commission INTEGER, R INTEGER]")
        .returnsUnordered(
            "deptno=10; empid=100; commission=1000; R=null",
            "deptno=10; empid=110; commission=250; R=null",
            "deptno=10; empid=150; commission=null; R=null",
            "deptno=20; empid=200; commission=500; R=500");
  }

  /** Tests for FIRST_VALUE empty window */
  @Test public void testWinAggFirstValueEmptyWindow() {
    CalciteAssert.hr()
        .query("select \"deptno\",\n"
            + " \"empid\",\n"
            + " \"commission\",\n"
            + " first_value(\"commission\") over (partition by \"deptno\" order by \"empid\" desc range between 1000 preceding and 999 preceding) as r\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, commission INTEGER, R INTEGER]")
        .returnsUnordered(
            "deptno=10; empid=100; commission=1000; R=null",
            "deptno=10; empid=110; commission=250; R=null",
            "deptno=10; empid=150; commission=null; R=null",
            "deptno=20; empid=200; commission=500; R=null");
  }

  /** Tests for ROW_NUMBER */
  @Test public void testWinRowNumber() {
    CalciteAssert.hr()
        .query("select \"deptno\",\n"
            + " \"empid\",\n"
            + " \"commission\",\n"
            + " row_number() over (partition by \"deptno\") as r,\n"
            + " row_number() over (partition by \"deptno\" order by \"commission\" desc nulls first) as rcnf,\n"
            + " row_number() over (partition by \"deptno\" order by \"commission\" desc nulls last) as rcnl,\n"
            + " row_number() over (partition by \"deptno\" order by \"empid\") as r,\n"
            + " row_number() over (partition by \"deptno\" order by \"empid\" desc) as rd\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[deptno INTEGER NOT NULL, empid INTEGER NOT NULL, commission INTEGER, R BIGINT NOT NULL, RCNF BIGINT NOT NULL, RCNL BIGINT NOT NULL, R BIGINT NOT NULL, RD BIGINT NOT NULL]")
        .returnsUnordered(
            "deptno=10; empid=100; commission=1000; R=1; RCNF=2; RCNL=1; R=1; RD=3",
            "deptno=10; empid=110; commission=250; R=3; RCNF=3; RCNL=2; R=2; RD=2",
            "deptno=10; empid=150; commission=null; R=2; RCNF=1; RCNL=3; R=3; RD=1",
            "deptno=20; empid=200; commission=500; R=1; RCNF=1; RCNL=1; R=1; RD=1");
  }

  /** Tests UNBOUNDED PRECEDING clause. */
  @Test public void testOverUnboundedPreceding() {
    CalciteAssert.hr()
        .query("select \"empid\",\n"
            + "  \"commission\",\n"
            + "  count(\"empid\") over (partition by 42\n"
            + "    order by \"commission\" nulls first\n"
            + "    rows between UNBOUNDED PRECEDING and current row) as m\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[empid INTEGER NOT NULL, commission INTEGER, M BIGINT NOT NULL]")
        .returnsUnordered(
            "empid=100; commission=1000; M=4",
            "empid=200; commission=500; M=3",
            "empid=150; commission=null; M=1",
            "empid=110; commission=250; M=2");
  }

  /** Tests UNBOUNDED PRECEDING clause. */
  @Test public void testSumOverUnboundedPreceding() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select \"empid\",\n"
            + "  \"commission\",\n"
            + "  sum(\"empid\") over (partition by 42\n"
            + "    order by \"commission\" nulls first\n"
            + "    rows between UNBOUNDED PRECEDING and current row) as m\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[empid INTEGER NOT NULL, commission INTEGER, M INTEGER NOT NULL]")
        .returnsUnordered(
            "empid=100; commission=1000; M=560",
            "empid=110; commission=250; M=260",
            "empid=150; commission=null; M=150",
            "empid=200; commission=500; M=460");
  }

  /** Tests that sum over possibly empty window is nullable. */
  @Test public void testSumOverPossiblyEmptyWindow() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select \"empid\",\n"
            + "  \"commission\",\n"
            + "  sum(\"empid\") over (partition by 42\n"
            + "    order by \"commission\" nulls first\n"
            + "    rows between UNBOUNDED PRECEDING and 1 preceding) as m\n"
            + "from \"hr\".\"emps\"")
        .typeIs(
            "[empid INTEGER NOT NULL, commission INTEGER, M INTEGER]")
        .returnsUnordered(
            "empid=100; commission=1000; M=460",
            "empid=110; commission=250; M=150",
            "empid=150; commission=null; M=null",
            "empid=200; commission=500; M=260");
  }

  /** Tests windowed aggregation with no ORDER BY clause.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-285">[CALCITE-285]
   * Window functions throw exception without ORDER BY</a>.
   *
   * <p>Note:</p>
   *
   * <ul>
   * <li>With no ORDER BY, the window is over all rows in the partition.
   * <li>With an ORDER BY, the implicit frame is 'RANGE BETWEEN
   *     UNBOUNDED PRECEDING AND CURRENT ROW'.
   * <li>With no ORDER BY or PARTITION BY, the window contains all rows in the
   *     table.
   * </ul>
   */
  @Test public void testOverNoOrder() {
    // If no range is specified, default is "RANGE BETWEEN UNBOUNDED PRECEDING
    // AND CURRENT ROW".
    // The aggregate function is within the current partition;
    // if there is no partition, that means the whole table.
    // Rows are deemed "equal to" the current row per the ORDER BY clause.
    // If there is no ORDER BY clause, CURRENT ROW has the same effect as
    // UNBOUNDED FOLLOWING; that is, no filtering effect at all.
    final String sql = "select *,\n"
        + " count(*) over (partition by deptno) as m1,\n"
        + " count(*) over (partition by deptno order by ename) as m2,\n"
        + " count(*) over () as m3\n"
        + "from emp";
    withEmpDept(sql).returnsUnordered(
        "ENAME=Adam ; DEPTNO=50; GENDER=M; M1=2; M2=1; M3=9",
        "ENAME=Alice; DEPTNO=30; GENDER=F; M1=2; M2=1; M3=9",
        "ENAME=Bob  ; DEPTNO=10; GENDER=M; M1=2; M2=1; M3=9",
        "ENAME=Eric ; DEPTNO=20; GENDER=M; M1=1; M2=1; M3=9",
        "ENAME=Eve  ; DEPTNO=50; GENDER=F; M1=2; M2=2; M3=9",
        "ENAME=Grace; DEPTNO=60; GENDER=F; M1=1; M2=1; M3=9",
        "ENAME=Jane ; DEPTNO=10; GENDER=F; M1=2; M2=2; M3=9",
        "ENAME=Susan; DEPTNO=30; GENDER=F; M1=2; M2=2; M3=9",
        "ENAME=Wilma; DEPTNO=null; GENDER=F; M1=1; M2=1; M3=9");
  }

  /** Tests that field-trimming creates a project near the table scan. */
  @Test public void testTrimFields() throws Exception {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      CalciteAssert.hr()
          .query("select \"name\", count(\"commission\") + 1\n"
              + "from \"hr\".\"emps\"\n"
              + "group by \"deptno\", \"name\"")
          .convertContains("LogicalProject(name=[$1], EXPR$1=[+($2, 1)])\n"
              + "  LogicalAggregate(group=[{0, 1}], agg#0=[COUNT($2)])\n"
              + "    LogicalProject(deptno=[$1], name=[$2], commission=[$4])\n"
              + "      EnumerableTableScan(table=[[hr, emps]])\n");
    }
  }

  /** Tests that field-trimming creates a project near the table scan, in a
   * query with windowed-aggregation. */
  @Test public void testTrimFieldsOver() throws Exception {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_TRIM.push(true)) {
      // The correct plan has a project on a filter on a project on a scan.
      CalciteAssert.hr()
          .query("select \"name\",\n"
              + "  count(\"commission\") over (partition by \"deptno\") + 1\n"
              + "from \"hr\".\"emps\"\n"
              + "where \"empid\" > 10")
          .convertContains(""
              + "LogicalProject(name=[$2], EXPR$1=[+(COUNT($3) OVER (PARTITION BY $1 RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING), 1)])\n"
              + "  LogicalFilter(condition=[>($0, 10)])\n"
              + "    LogicalProject(empid=[$0], deptno=[$1], name=[$2], commission=[$4])\n"
              + "      EnumerableTableScan(table=[[hr, emps]])\n");
    }
  }

  /** Tests window aggregate whose argument is a constant. */
  @Test public void testWinAggConstant() {
    CalciteAssert.hr()
        .query("select max(1) over (partition by \"deptno\"\n"
            + "  order by \"empid\") as m\n"
            + "from \"hr\".\"emps\"")
        .returnsUnordered(
            "M=1",
            "M=1",
            "M=1",
            "M=1");
  }

  /** Tests multiple window aggregates over constants.
   * This tests that EnumerableWindowRel is able to reference the right slot
   * when accessing constant for aggregation argument. */
  @Test public void testWinAggConstantMultipleConstants() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select \"deptno\", sum(1) over (partition by \"deptno\"\n"
            + "  order by \"empid\" rows between unbounded preceding and current row) as a,\n"
            + " sum(-1) over (partition by \"deptno\"\n"
            + "  order by \"empid\" rows between unbounded preceding and current row) as b\n"
            + "from \"hr\".\"emps\"")
        .returnsUnordered(
            "deptno=10; A=1; B=-1",
            "deptno=10; A=2; B=-2",
            "deptno=10; A=3; B=-3",
            "deptno=20; A=1; B=-1");
  }

  /** Tests window aggregate PARTITION BY constant. */
  @Test public void testWinAggPartitionByConstant() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query(""
            // *0 is used to make results predictable.
            // If using just max(empid) calcite cannot compute the result
            // properly since it does not support range windows yet :(
            + "select max(\"empid\"*0) over (partition by 42\n"
            + "  order by \"empid\") as m\n"
            + "from \"hr\".\"emps\"")
        .returnsUnordered(
            "M=0",
            "M=0",
            "M=0",
            "M=0");
  }

  /** Tests window aggregate ORDER BY constant. Unlike in SELECT ... ORDER BY,
   * the constant does not mean a column. It means a constant, therefore the
   * order of the rows is not changed. */
  @Test public void testWinAggOrderByConstant() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query(""
            // *0 is used to make results predictable.
            // If using just max(empid) calcite cannot compute the result
            // properly since it does not support range windows yet :(
            + "select max(\"empid\"*0) over (partition by \"deptno\"\n"
            + "  order by 42) as m\n"
            + "from \"hr\".\"emps\"")
        .returnsUnordered(
            "M=0",
            "M=0",
            "M=0",
            "M=0");
  }

  /** Tests WHERE comparing a nullable integer with an integer literal. */
  @Test public void testWhereNullable() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select * from \"hr\".\"emps\"\n"
            + "where \"commission\" > 800")
        .returns(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");
  }

  /** Tests CALCITE-980: Not (C='a' or C='b') causes NPE */
  @Test public void testWhereOrAndNullable() {
    /* Generates the following code:
       public boolean moveNext() {
         while (inputEnumerator.moveNext()) {
           final Object[] current = (Object[]) inputEnumerator.current();
           final String inp0_ = current[0] == null ? (String) null : current[0].toString();
           final String inp1_ = current[1] == null ? (String) null : current[1].toString();
           if (inp0_ != null && org.apache.calcite.runtime.SqlFunctions.eq(inp0_, "a")
               && (inp1_ != null && org.apache.calcite.runtime.SqlFunctions.eq(inp1_, "b"))
               || inp0_ != null && org.apache.calcite.runtime.SqlFunctions.eq(inp0_, "b")
               && (inp1_ != null && org.apache.calcite.runtime.SqlFunctions.eq(inp1_, "c"))) {
             return true;
           }
         }
         return false;
       }
     */
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("with tst(c) as (values('a'),('b'),('c'),(cast(null as varchar)))"
            + " select u.c u, v.c v from tst u, tst v where ((u.c = 'a' and v.c = 'b') or (u.c = 'b' and v.c = 'c'))")
        .returnsUnordered(
            "U=a; V=b",
            "U=b; V=c");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-980">[CALCITE-980]
   * different flavors of boolean logic</a>.
   *
   * @see QuidemTest sql/conditions.iq */
  @Ignore("Fails with org.codehaus.commons.compiler.CompileException: Line 16, Column 112:"
      + " Cannot compare types \"int\" and \"java.lang.String\"\n")
  @Test public void testComparingIntAndString() throws Exception {
    // if (((...test.ReflectiveSchemaTest.IntAndString) inputEnumerator.current()).id == "T")

    CalciteAssert.that()
        .withSchema("s",
            new ReflectiveSchema(
                new ReflectiveSchemaTest.CatchallSchema()))
        .query("select a.\"value\", b.\"value\"\n"
            + "  from \"bools\" a\n"
            + "     , \"bools\" b\n"
            + " where b.\"value\" = 'T'\n"
            + " order by 1, 2")
        .returnsUnordered(
            "should fail with 'not a number' sql error while converting text to number");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1015">[CALCITE-1015]
   * OFFSET 0 causes AssertionError</a>. */
  @Test public void testTrivialSort() {
    final String sql = "select a.\"value\", b.\"value\"\n"
        + "  from \"bools\" a\n"
        + "     , \"bools\" b\n"
        + " offset 0";
    CalciteAssert.that()
        .withSchema("s",
            new ReflectiveSchema(
                new ReflectiveSchemaTest.CatchallSchema()))
        .query(sql)
        .returnsUnordered("value=T; value=T",
            "value=T; value=F",
            "value=T; value=null",
            "value=F; value=T",
            "value=F; value=F",
            "value=F; value=null");
  }

  /** Tests the LIKE operator. */
  @Test public void testLike() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select * from \"hr\".\"emps\"\n"
            + "where \"name\" like '%i__'")
        .returns(""
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n");
  }

  /** Tests array index. */
  @Test public void testArrayIndexing() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query(
            "select \"deptno\", \"employees\"[1] as e from \"hr\".\"depts\"\n").returnsUnordered(
        "deptno=10; E={100, 10, Bill, 10000.0, 1000}",
        "deptno=30; E=null",
        "deptno=40; E={200, 20, Eric, 8000.0, 500}");
  }

  @Test public void testVarcharEquals() {
    CalciteAssert.model(FOODMART_MODEL)
        .query("select \"lname\" from \"customer\" where \"lname\" = 'Nowmer'")
        .returns("lname=Nowmer\n");

    // lname is declared as VARCHAR(30), comparing it with a string longer
    // than 30 characters would introduce a cast to the least restrictive
    // type, thus lname would be cast to a varchar(40) in this case.
    // These sorts of casts are removed though when constructing the jdbc
    // sql, since e.g. HSQLDB does not support them.
    CalciteAssert.model(FOODMART_MODEL)
        .query("select count(*) as c from \"customer\" "
            + "where \"lname\" = 'this string is longer than 30 characters'")
        .returns("C=0\n");

    CalciteAssert.model(FOODMART_MODEL)
        .query("select count(*) as c from \"customer\" "
            + "where cast(\"customer_id\" as char(20)) = 'this string is longer than 30 characters'")
        .returns("C=0\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1153">[CALCITE-1153]
   * Invalid CAST when push JOIN down to Oracle</a>. */
  @Test public void testJoinMismatchedVarchar() {
    final String sql = "select count(*) as c\n"
        + "from \"customer\" as c\n"
        + "join \"product\" as p on c.\"lname\" = p.\"brand_name\"";
    CalciteAssert.model(FOODMART_MODEL)
        .query(sql)
        .returns("C=607\n");
  }

  @Test public void testIntersectMismatchedVarchar() {
    final String sql = "select count(*) as c from (\n"
        + "  select \"lname\" from \"customer\" as c\n"
        + "  intersect\n"
        + "  select \"brand_name\" from \"product\" as p)";
    CalciteAssert.model(FOODMART_MODEL)
        .query(sql)
        .returns("C=12\n");
  }

  /** Tests the NOT IN operator. Problems arose in code-generation because
   * the column allows nulls. */
  @Test public void testNotIn() {
    predicate("\"name\" not in ('a', 'b') or \"name\" is null")
        .returns(""
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n");

    // And some similar combinations...
    predicate("\"name\" in ('a', 'b') or \"name\" is null");
    predicate("\"name\" in ('a', 'b', null) or \"name\" is null");
    predicate("\"name\" in ('a', 'b') or \"name\" is not null");
    predicate("\"name\" in ('a', 'b', null) or \"name\" is not null");
    predicate("\"name\" not in ('a', 'b', null) or \"name\" is not null");
    predicate("\"name\" not in ('a', 'b', null) and \"name\" is not null");
  }

  @Test public void testNotInEmptyQuery() {
    // RHS is empty, therefore returns all rows from emp, including the one
    // with deptno = NULL.
    final String sql = "select deptno from emp where deptno not in (\n"
        + "select deptno from dept where deptno = -1)";
    withEmpDept(sql)
//        .explainContains("EnumerableCalc(expr#0..2=[{inputs}], "
//            + "expr#3=[IS NOT NULL($t2)], expr#4=[true], "
//            + "expr#5=[IS NULL($t0)], expr#6=[null], expr#7=[false], "
//            + "expr#8=[CASE($t3, $t4, $t5, $t6, $t7)], expr#9=[NOT($t8)], "
//            + "EXPR$1=[$t0], $condition=[$t9])")
        .returnsUnordered("DEPTNO=null",
            "DEPTNO=10",
            "DEPTNO=10",
            "DEPTNO=20",
            "DEPTNO=30",
            "DEPTNO=30",
            "DEPTNO=50",
            "DEPTNO=50",
            "DEPTNO=60");
  }

  @Test public void testNotInQuery() {
    // None of the rows from RHS is NULL.
    final String sql = "select deptno from emp where deptno not in (\n"
        + "select deptno from dept)";
    withEmpDept(sql)
        .returnsUnordered("DEPTNO=50",
            "DEPTNO=50",
            "DEPTNO=60");
  }

  @Test public void testNotInQueryWithNull() {
    // There is a NULL on the RHS, and '10 not in (20, null)' yields unknown
    // (similarly for every other value of deptno), so no rows are returned.
    final String sql = "select deptno from emp where deptno not in (\n"
        + "select deptno from emp)";
    withEmpDept(sql)
        .returnsCount(0);
  }

  @Test public void testTrim() {
    CalciteAssert.model(FOODMART_MODEL)
        .query("select trim(\"lname\") as \"lname\" "
            + "from \"customer\" where \"lname\" = 'Nowmer'")
        .returns("lname=Nowmer\n");

    CalciteAssert.model(FOODMART_MODEL)
        .query("select trim(leading 'N' from \"lname\") as \"lname\" "
            + "from \"customer\" where \"lname\" = 'Nowmer'")
        .returns("lname=owmer\n");
  }

  private CalciteAssert.AssertQuery predicate(String foo) {
    return CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query("select * from \"hr\".\"emps\"\n"
            + "where " + foo)
        .runs();
  }

  @Test public void testExistsCorrelated() {
    final String sql = "select*from \"hr\".\"emps\" where exists (\n"
        + " select 1 from \"hr\".\"depts\"\n"
        + " where \"emps\".\"deptno\"=\"depts\".\"deptno\")";
    final String plan = ""
        + "LogicalProject(empid=[$0], deptno=[$1], name=[$2], salary=[$3], commission=[$4])\n"
        + "  LogicalFilter(condition=[EXISTS({\n"
        + "LogicalFilter(condition=[=($cor0.deptno, $0)])\n"
        + "  EnumerableTableScan(table=[[hr, depts]])\n"
        + "})], variablesSet=[[$cor0]])\n"
        + "    EnumerableTableScan(table=[[hr, emps]])\n";
    CalciteAssert.hr().query(sql).convertContains(plan)
        .returnsUnordered(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000",
            "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null",
            "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250");
  }

  @Test public void testNotExistsCorrelated() {
    final String plan = "PLAN="
        + "EnumerableCalc(expr#0..5=[{inputs}], expr#6=[IS NULL($t5)], proj#0..4=[{exprs}], $condition=[$t6])\n"
        + "  EnumerableCorrelate(correlation=[$cor0], joinType=[left], requiredColumns=[{1}])\n"
        + "    EnumerableTableScan(table=[[hr, emps]])\n"
        + "    EnumerableAggregate(group=[{0}])\n"
        + "      EnumerableCalc(expr#0..3=[{inputs}], expr#4=[true], expr#5=[$cor0], expr#6=[$t5.deptno], expr#7=[=($t6, $t0)], i=[$t4], $condition=[$t7])\n"
        + "        EnumerableTableScan(table=[[hr, depts]])\n";
    final String sql = "select * from \"hr\".\"emps\" where not exists (\n"
        + " select 1 from \"hr\".\"depts\"\n"
        + " where \"emps\".\"deptno\"=\"depts\".\"deptno\")";
    CalciteAssert.hr()
        .with(CalciteConnectionProperty.FORCE_DECORRELATE, false)
        .query(sql)
        .explainContains(plan)
        .returnsUnordered(
            "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500");
  }

  /** Manual expansion of EXISTS in {@link #testNotExistsCorrelated()}. */
  @Test public void testNotExistsCorrelated2() {
    final String sql = "select * from \"hr\".\"emps\" as e left join lateral (\n"
        + " select distinct true as i\n"
        + " from \"hr\".\"depts\"\n"
        + " where e.\"deptno\"=\"depts\".\"deptno\") on true";
    final String explain = ""
        + "EnumerableCalc(expr#0..6=[{inputs}], proj#0..4=[{exprs}], I=[$t6])\n"
        + "  EnumerableJoin(condition=[=($1, $5)], joinType=[left])\n"
        + "    EnumerableTableScan(table=[[hr, emps]])\n"
        + "    EnumerableCalc(expr#0=[{inputs}], expr#1=[true], proj#0..1=[{exprs}])\n"
        + "      EnumerableAggregate(group=[{0}])\n"
        + "        EnumerableTableScan(table=[[hr, depts]])";
    CalciteAssert.hr()
        .query(sql)
        .explainContains(explain)
        .returnsUnordered(
            "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000; I=true",
            "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250; I=true",
            "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null; I=true",
            "empid=200; deptno=20; name=Eric; salary=8000.0; commission=500; I=null");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-313">[CALCITE-313]
   * Query decorrelation fails</a>. */
  @Test public void testJoinInCorrelatedSubQuery() {
    CalciteAssert.hr()
        .query("select *\n"
            + "from \"hr\".\"depts\" as d\n"
            + "where \"deptno\" in (\n"
            + "  select d2.\"deptno\"\n"
            + "  from \"hr\".\"depts\" as d2\n"
            + "  join \"hr\".\"emps\" as e2 using (\"deptno\")\n"
            + "where d.\"deptno\" = d2.\"deptno\")")
        .convertMatches(relNode -> {
          String s = RelOptUtil.toString(relNode);
          assertThat(s, not(containsString("Correlate")));
          return null;
        });
  }

  /** Tests a correlated scalar sub-query in the SELECT clause.
   *
   * <p>Note that there should be an extra row "empid=200; deptno=20;
   * DNAME=null" but left join doesn't work.</p> */
  @Test public void testScalarSubQuery() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push(true)) {
      CalciteAssert.hr()
          .query("select \"empid\", \"deptno\",\n"
              + " (select \"name\" from \"hr\".\"depts\"\n"
              + "  where \"deptno\" = e.\"deptno\") as dname\n"
              + "from \"hr\".\"emps\" as e")
          .returnsUnordered("empid=100; deptno=10; DNAME=Sales",
              "empid=110; deptno=10; DNAME=Sales",
              "empid=150; deptno=10; DNAME=Sales",
              "empid=200; deptno=20; DNAME=null");
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-559">[CALCITE-559]
   * Correlated scalar sub-query in WHERE gives error</a>. */
  @Test public void testJoinCorrelatedScalarSubQuery() throws SQLException {
    final String sql = "select e.employee_id, d.department_id "
        + " from employee e, department d "
        + " where e.department_id = d.department_id "
        + " and e.salary > (select avg(e2.salary) "
        + "                 from employee e2 "
        + "                 where e2.store_id = e.store_id)";
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .with(Lex.JAVA)
        .query(sql)
        .returnsCount(599);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-685">[CALCITE-685]
   * Correlated scalar sub-query in SELECT clause throws</a>. */
  @Ignore("[CALCITE-685]")
  @Test public void testCorrelatedScalarSubQuery() throws SQLException {
    final String sql = "select e.department_id, sum(e.employee_id),\n"
        + "       ( select sum(e2.employee_id)\n"
        + "         from  employee e2\n"
        + "         where e.department_id = e2.department_id\n"
        + "       )\n"
        + "from employee e\n"
        + "group by e.department_id\n";
    final String explain = "EnumerableJoin(condition=[true], joinType=[left])\n"
        + "  EnumerableAggregate(group=[{7}], EXPR$1=[$SUM0($0)])\n"
        + "    EnumerableTableScan(table=[[foodmart2, employee]])\n"
        + "  EnumerableAggregate(group=[{}], EXPR$0=[SUM($0)])\n"
        + "    EnumerableCalc(expr#0..16=[{inputs}], expr#17=[$cor0], expr#18=[$t17.department_id], expr#19=[=($t18, $t7)], employee_id=[$t0], department_id=[$t7], $condition=[$t19])\n"
        + "      EnumerableTableScan(table=[[foodmart2, employee]])\n";
    CalciteAssert.that()
        .with(CalciteAssert.Config.FOODMART_CLONE)
        .with(Lex.JAVA)
        .query(sql)
        .explainContains(explain)
        .returnsCount(0);
  }

  @Test public void testLeftJoin() {
    CalciteAssert.hr()
        .query("select e.\"deptno\", d.\"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "  left join \"hr\".\"depts\" as d using (\"deptno\")")
        .returnsUnordered(
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=20; deptno=null");
  }

  @Test public void testFullJoin() {
    CalciteAssert.hr()
        .query("select e.\"deptno\", d.\"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "  full join \"hr\".\"depts\" as d using (\"deptno\")")
        .returnsUnordered(
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=20; deptno=null",
            "deptno=null; deptno=30",
            "deptno=null; deptno=40");
  }

  @Test public void testRightJoin() {
    CalciteAssert.hr()
        .query("select e.\"deptno\", d.\"deptno\"\n"
            + "from \"hr\".\"emps\" as e\n"
            + "  right join \"hr\".\"depts\" as d using (\"deptno\")")
        .returnsUnordered(
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=10; deptno=10",
            "deptno=null; deptno=30",
            "deptno=null; deptno=40");
  }

  /** Various queries against EMP and DEPT, in particular involving composite
   * join conditions in various flavors of outer join. Results are verified
   * against MySQL (except full join, which MySQL does not support). */
  @Test public void testVariousOuter() {
    final String sql =
        "select * from emp join dept on emp.deptno = dept.deptno";
    withEmpDept(sql).returnsUnordered(
        "ENAME=Alice; DEPTNO=30; GENDER=F; DEPTNO0=30; DNAME=Engineering",
        "ENAME=Bob  ; DEPTNO=10; GENDER=M; DEPTNO0=10; DNAME=Sales      ",
        "ENAME=Eric ; DEPTNO=20; GENDER=M; DEPTNO0=20; DNAME=Marketing  ",
        "ENAME=Jane ; DEPTNO=10; GENDER=F; DEPTNO0=10; DNAME=Sales      ",
        "ENAME=Susan; DEPTNO=30; GENDER=F; DEPTNO0=30; DNAME=Engineering");
  }

  private CalciteAssert.AssertQuery withEmpDept(String sql) {
    // Append a 'WITH' clause that supplies EMP and DEPT tables like this:
    //
    // drop table emp;
    // drop table dept;
    // create table emp(ename varchar(10), deptno int, gender varchar(1));
    // insert into emp values ('Jane', 10, 'F');
    // insert into emp values ('Bob', 10, 'M');
    // insert into emp values ('Eric', 20, 'M');
    // insert into emp values ('Susan', 30, 'F');
    // insert into emp values ('Alice', 30, 'F');
    // insert into emp values ('Adam', 50, 'M');
    // insert into emp values ('Eve', 50, 'F');
    // insert into emp values ('Grace', 60, 'F');
    // insert into emp values ('Wilma', null, 'F');
    // create table dept (deptno int, dname varchar(12));
    // insert into dept values (10, 'Sales');
    // insert into dept values (20, 'Marketing');
    // insert into dept values (30, 'Engineering');
    // insert into dept values (40, 'Empty');
    return CalciteAssert.that()
        .query("with\n"
            + "  emp(ename, deptno, gender) as (values\n"
            + "    ('Jane', 10, 'F'),\n"
            + "    ('Bob', 10, 'M'),\n"
            + "    ('Eric', 20, 'M'),\n"
            + "    ('Susan', 30, 'F'),\n"
            + "    ('Alice', 30, 'F'),\n"
            + "    ('Adam', 50, 'M'),\n"
            + "    ('Eve', 50, 'F'),\n"
            + "    ('Grace', 60, 'F'),\n"
            + "    ('Wilma', cast(null as integer), 'F')),\n"
            + "  dept(deptno, dname) as (values\n"
            + "    (10, 'Sales'),\n"
            + "    (20, 'Marketing'),\n"
            + "    (30, 'Engineering'),\n"
            + "    (40, 'Empty'))\n"
            + sql);
  }

  @Test public void testScalarSubQueryUncorrelated() {
    CalciteAssert.hr()
        .query("select \"empid\", \"deptno\",\n"
            + " (select \"name\" from \"hr\".\"depts\"\n"
            + "  where \"deptno\" = 30) as dname\n"
            + "from \"hr\".\"emps\" as e")
        .returnsUnordered("empid=100; deptno=10; DNAME=Marketing",
            "empid=110; deptno=10; DNAME=Marketing",
            "empid=150; deptno=10; DNAME=Marketing",
            "empid=200; deptno=20; DNAME=Marketing");
  }

  @Test public void testScalarSubQueryInCase() {
    try (TryThreadLocal.Memo ignored = Prepare.THREAD_EXPAND.push(true)) {
      CalciteAssert.hr()
          .query("select e.\"name\",\n"
              + " (CASE e.\"deptno\"\n"
              + "  WHEN (Select \"deptno\" from \"hr\".\"depts\" d\n"
              + "        where d.\"deptno\" = e.\"deptno\")\n"
              + "  THEN (Select d.\"name\" from \"hr\".\"depts\" d\n"
              + "        where d.\"deptno\" = e.\"deptno\")\n"
              + "  ELSE 'DepartmentNotFound'  END) AS DEPTNAME\n"
              + "from \"hr\".\"emps\" e")
          .returnsUnordered("name=Bill; DEPTNAME=Sales",
              "name=Eric; DEPTNAME=DepartmentNotFound",
              "name=Sebastian; DEPTNAME=Sales",
              "name=Theodore; DEPTNAME=Sales");
    }
  }

  @Test public void testScalarSubQueryInCase2() {
    CalciteAssert.hr()
        .query("select e.\"name\",\n"
            + " (CASE WHEN e.\"deptno\" = (\n"
            + "    Select \"deptno\" from \"hr\".\"depts\" d\n"
            + "    where d.\"name\" = 'Sales')\n"
            + "  THEN 'Sales'\n"
            + "  ELSE 'Not Matched'  END) AS DEPTNAME\n"
            + "from \"hr\".\"emps\" e")
        .returnsUnordered("name=Bill; DEPTNAME=Sales      ",
            "name=Eric; DEPTNAME=Not Matched",
            "name=Sebastian; DEPTNAME=Sales      ",
            "name=Theodore; DEPTNAME=Sales      ");
  }

  /** Tests the TABLES table in the information schema. */
  @Test public void testMetaTables() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR_PLUS_METADATA)
        .query("select * from \"metadata\".TABLES")
        .returns(
            CalciteAssert.checkResultContains(
                "tableSchem=metadata; tableName=COLUMNS; tableType=SYSTEM TABLE; "));

    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR_PLUS_METADATA)
        .query("select count(distinct \"tableSchem\") as c\n"
            + "from \"metadata\".TABLES")
        .returns("C=3\n");
  }

  /** Tests that {@link java.sql.Statement#setMaxRows(int)} is honored. */
  @Test public void testSetMaxRows() throws Exception {
    CalciteAssert.hr()
        .doWithConnection(connection -> {
          try {
            final Statement statement = connection.createStatement();
            try {
              statement.setMaxRows(-1);
              fail("expected error");
            } catch (SQLException e) {
              assertEquals(e.getMessage(), "illegal maxRows value: -1");
            }
            statement.setMaxRows(2);
            assertEquals(2, statement.getMaxRows());
            final ResultSet resultSet = statement.executeQuery(
                "select * from \"hr\".\"emps\"");
            assertTrue(resultSet.next());
            assertTrue(resultSet.next());
            assertFalse(resultSet.next());
            resultSet.close();
            statement.close();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Tests a {@link PreparedStatement} with parameters. */
  @Test public void testPreparedStatement() throws Exception {
    CalciteAssert.hr()
        .doWithConnection(connection -> {
          try {
            final PreparedStatement preparedStatement =
                connection.prepareStatement("select \"deptno\", \"name\" "
                    + "from \"hr\".\"emps\"\n"
                    + "where \"deptno\" < ? and \"name\" like ?");

            // execute with vars unbound - gives error
            ResultSet resultSet;
            try {
              resultSet = preparedStatement.executeQuery();
              fail("expected error, got " + resultSet);
            } catch (SQLException e) {
              assertThat(e.getMessage(),
                  containsString(
                      "exception while executing query: unbound parameter"));
            }

            // execute with both vars null - no results
            preparedStatement.setNull(1, Types.INTEGER);
            preparedStatement.setNull(2, Types.VARCHAR);
            resultSet = preparedStatement.executeQuery();
            assertFalse(resultSet.next());

            // execute with ?0=15, ?1='%' - 3 rows
            preparedStatement.setInt(1, 15);
            preparedStatement.setString(2, "%");
            resultSet = preparedStatement.executeQuery();
            assertEquals("deptno=10; name=Bill\n"
                + "deptno=10; name=Sebastian\n"
                + "deptno=10; name=Theodore\n",
                CalciteAssert.toString(resultSet));

            // execute with ?0=15 (from last bind), ?1='%r%' - 1 row
            preparedStatement.setString(2, "%r%");
            resultSet = preparedStatement.executeQuery();
            assertEquals(
                "deptno=10; name=Theodore\n",
                CalciteAssert.toString(resultSet));

            // Now BETWEEN, with 3 arguments, 2 of which are parameters
            final String sql2 = "select \"deptno\", \"name\" "
                + "from \"hr\".\"emps\"\n"
                + "where \"deptno\" between symmetric ? and ?\n"
                + "order by 2";
            final PreparedStatement preparedStatement2 =
                connection.prepareStatement(sql2);
            preparedStatement2.setInt(1, 15);
            preparedStatement2.setInt(2, 5);
            resultSet = preparedStatement2.executeQuery();
            assertThat(CalciteAssert.toString(resultSet),
                is("deptno=10; name=Bill\n"
                    + "deptno=10; name=Sebastian\n"
                    + "deptno=10; name=Theodore\n"));

            resultSet.close();
            preparedStatement2.close();
            preparedStatement.close();
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2061">[CALCITE-2061]
   * Dynamic parameters in offset/fetch</a>. */
  @Test public void testPreparedOffsetFetch() throws Exception {
    checkPreparedOffsetFetch(0, 0, Matchers.returnsUnordered());
    checkPreparedOffsetFetch(100, 4, Matchers.returnsUnordered());
    checkPreparedOffsetFetch(3, 4,
        Matchers.returnsUnordered("name=Eric"));
  }

  private void checkPreparedOffsetFetch(final int offset, final int fetch,
      final Matcher<? super ResultSet> matcher) throws Exception {
    CalciteAssert.hr()
        .doWithConnection(connection -> {
          final String sql = "select \"name\"\n"
              + "from \"hr\".\"emps\"\n"
              + "order by \"empid\" offset ? fetch next ? rows only";
          try (PreparedStatement p =
                   connection.prepareStatement(sql)) {
            final ParameterMetaData pmd = p.getParameterMetaData();
            assertThat(pmd.getParameterCount(), is(2));
            assertThat(pmd.getParameterType(1), is(Types.INTEGER));
            assertThat(pmd.getParameterType(2), is(Types.INTEGER));
            p.setInt(1, offset);
            p.setInt(2, fetch);
            try (ResultSet r = p.executeQuery()) {
              assertThat(r, matcher);
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Tests a JDBC connection that provides a model (a single schema based on
   * a JDBC database). */
  @Test public void testModel() {
    CalciteAssert.model(FOODMART_MODEL)
        .query("select count(*) as c from \"foodmart\".\"time_by_day\"")
        .returns("C=730\n");
  }

  /** Tests a JSON model with a comment. Not standard JSON, but harmless to
   * allow Jackson's comments extension.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-160">[CALCITE-160]
   * Allow comments in schema definitions</a>. */
  @Test public void testModelWithComment() {
    final String model =
        FOODMART_MODEL.replace("schemas:", "/* comment */ schemas:");
    assertThat(model, not(equalTo(FOODMART_MODEL)));
    CalciteAssert.model(model)
        .query("select count(*) as c from \"foodmart\".\"time_by_day\"")
        .returns("C=730\n");
  }

  /** Defines a materialized view and tests that the query is rewritten to use
   * it, and that the query produces the same result with and without it. There
   * are more comprehensive tests in {@link MaterializationTest}. */
  @Ignore("until JdbcSchema can define materialized views")
  @Test public void testModelWithMaterializedView() {
    CalciteAssert.model(FOODMART_MODEL)
        .enable(false)
        .query(
            "select count(*) as c from \"foodmart\".\"sales_fact_1997\" join \"foodmart\".\"time_by_day\" using (\"time_id\")")
        .returns("C=86837\n");
    CalciteAssert.that().withMaterializations(
        FOODMART_MODEL,
        "agg_c_10_sales_fact_1997",
            "select t.`month_of_year`, t.`quarter`, t.`the_year`, sum(s.`store_sales`) as `store_sales`, sum(s.`store_cost`), sum(s.`unit_sales`), count(distinct s.`customer_id`), count(*) as `fact_count` from `time_by_day` as t join `sales_fact_1997` as s using (`time_id`) group by t.`month_of_year`, t.`quarter`, t.`the_year`")
        .query(
            "select t.\"month_of_year\", t.\"quarter\", t.\"the_year\", sum(s.\"store_sales\") as \"store_sales\", sum(s.\"store_cost\"), sum(s.\"unit_sales\"), count(distinct s.\"customer_id\"), count(*) as \"fact_count\" from \"time_by_day\" as t join \"sales_fact_1997\" as s using (\"time_id\") group by t.\"month_of_year\", t.\"quarter\", t.\"the_year\"")
        .explainContains(
            "JdbcTableScan(table=[[foodmart, agg_c_10_sales_fact_1997]])")
        .enableMaterializations(false)
        .explainContains("JdbcTableScan(table=[[foodmart, sales_fact_1997]])")
        .sameResultWithMaterializationsDisabled();
  }

  /** Tests a JDBC connection that provides a model that contains custom
   * tables. */
  @Test public void testModelCustomTable() {
    CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '"
        + EmpDeptTableFactory.class.getName() + "',\n"
        + "           operand: {'foo': 1, 'bar': [345, 357] }\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}")
        .query("select * from \"adhoc\".EMPLOYEES where \"deptno\" = 10")
        .returns(""
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n");
  }

  /** Tests a JDBC connection that provides a model that contains custom
   * tables. */
  @Test public void testModelCustomTable2() {
    testRangeTable("object");
  }

  /** Tests a JDBC connection that provides a model that contains custom
   * tables. */
  @Test public void testModelCustomTableArrayRowSingleColumn() {
    testRangeTable("array");
  }

  /** Tests a JDBC connection that provides a model that contains custom
   * tables. */
  @Test public void testModelCustomTableIntegerRowSingleColumn() {
    testRangeTable("integer");
  }

  private void testRangeTable(String elementType) {
    CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'MATH',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'INTEGERS',\n"
        + "           type: 'custom',\n"
        + "           factory: '"
        + RangeTable.Factory.class.getName() + "',\n"
        + "           operand: {'column': 'N', 'start': 3, 'end': 7, "
        + " 'elementType': '" + elementType + "'}\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}")
        .query("select * from math.integers")
        .returns("N=3\n"
            + "N=4\n"
            + "N=5\n"
            + "N=6\n");
  }

  /** Tests a JDBC connection that provides a model that contains a custom
   * schema. */
  @Test public void testModelCustomSchema() throws Exception {
    final CalciteAssert.AssertThat that =
        CalciteAssert.model("{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'adhoc',\n"
            + "  schemas: [\n"
            + "    {\n"
            + "      name: 'empty'\n"
            + "    },\n"
            + "    {\n"
            + "      name: 'adhoc',\n"
            + "      type: 'custom',\n"
            + "      factory: '"
            + MySchemaFactory.class.getName()
            + "',\n"
            + "      operand: {'tableName': 'ELVIS'}\n"
            + "    }\n"
            + "  ]\n"
            + "}");
    // check that the specified 'defaultSchema' was used
    that.doWithConnection(connection -> {
      try {
        assertEquals("adhoc", connection.getSchema());
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
    that.query("select * from \"adhoc\".ELVIS where \"deptno\" = 10")
        .returns(""
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n");
    that.query("select * from \"adhoc\".EMPLOYEES")
        .throws_("Object 'EMPLOYEES' not found within 'adhoc'");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1360">[CALCITE-1360]
   * Custom schema in file in current directory</a>. */
  @Test public void testCustomSchemaInFileInPwd() throws SQLException {
    checkCustomSchemaInFileInPwd("custom-schema-model.json");
    switch (File.pathSeparatorChar) {
    case '/':
      // Skip this test on Windows; the mapping from file names to URLs is too
      // weird.
      checkCustomSchemaInFileInPwd("." + File.pathSeparatorChar
          + "custom-schema-model2.json");
    }
  }

  private void checkCustomSchemaInFileInPwd(String fileName)
      throws SQLException {
    final File file = new File(fileName);
    try (PrintWriter pw = Util.printWriter(file)) {
      file.deleteOnExit();
      pw.println("{\n"
          + "  version: '1.0',\n"
          + "  defaultSchema: 'adhoc',\n"
          + "  schemas: [\n"
          + "    {\n"
          + "      name: 'empty'\n"
          + "    },\n"
          + "    {\n"
          + "      name: 'adhoc',\n"
          + "      type: 'custom',\n"
          + "      factory: '"
          + MySchemaFactory.class.getName()
          + "',\n"
          + "      operand: {'tableName': 'ELVIS'}\n"
          + "    }\n"
          + "  ]\n"
          + "}");
      pw.flush();
      final String url = "jdbc:calcite:model=" + file;
      try (Connection c = DriverManager.getConnection(url);
           Statement s = c.createStatement();
           ResultSet r = s.executeQuery("values 1")) {
        assertThat(r.next(), is(true));
      }
      //noinspection ResultOfMethodCallIgnored
      file.delete();
    } catch (IOException e) {
      // current directory is not writable; an environment issue, not
      // necessarily a bug
    }
  }

  /** Connects to a custom schema without writing a model.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1259">[CALCITE-1259]
   * Allow connecting to a single schema without writing a model</a>. */
  @Test public void testCustomSchemaDirectConnection() throws Exception {
    final String url = "jdbc:calcite:"
        + "schemaFactory=" + MySchemaFactory.class.getName()
        + "; schema.tableName=ELVIS";
    checkCustomSchema(url, "adhoc"); // implicit schema is called 'adhoc'
    checkCustomSchema(url + "; schema=xyz", "xyz"); // explicit schema
  }

  private void checkCustomSchema(String url, String schemaName) throws SQLException {
    try (Connection connection = DriverManager.getConnection(url)) {
      assertThat(connection.getSchema(), is(schemaName));
      final String sql = "select * from \"" + schemaName + "\".ELVIS where \"deptno\" = 10";
      final String sql2 = "select * from ELVIS where \"deptno\" = 10";
      String expected = ""
          + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
          + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
          + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n";
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery(sql)) {
          assertThat(CalciteAssert.toString(resultSet), is(expected));
        }
        try (ResultSet resultSet = statement.executeQuery(sql2)) {
          assertThat(CalciteAssert.toString(resultSet), is(expected));
        }
      }
    }
  }

  /** Connects to a JDBC schema without writing a model. */
  @Test public void testJdbcSchemaDirectConnection() throws Exception {
    checkJdbcSchemaDirectConnection(
        "schemaFactory=org.apache.calcite.adapter.jdbc.JdbcSchema$Factory");
    checkJdbcSchemaDirectConnection("schemaType=JDBC");
  }

  private void checkJdbcSchemaDirectConnection(String s) throws SQLException {
    final StringBuilder b = new StringBuilder("jdbc:calcite:");
    b.append(s);
    pv(b, "schema.jdbcUser", SCOTT.username);
    pv(b, "schema.jdbcPassword", SCOTT.password);
    pv(b, "schema.jdbcUrl", SCOTT.url);
    pv(b, "schema.jdbcCatalog", SCOTT.catalog);
    pv(b, "schema.jdbcDriver", SCOTT.driver);
    pv(b, "schema.jdbcSchema", SCOTT.schema);
    final String url =  b.toString();
    Connection connection = DriverManager.getConnection(url);
    assertThat(connection.getSchema(), is("adhoc"));
    String expected = "C=14\n";
    final String sql = "select count(*) as c from emp";
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(CalciteAssert.toString(resultSet), is(expected));
    }
  }

  private void pv(StringBuilder b, String p, String v) {
    if (v != null) {
      b.append("; ").append(p).append("=").append(v);
    }
  }

  /** Connects to a map schema without writing a model. */
  @Test public void testMapSchemaDirectConnection() throws Exception {
    checkMapSchemaDirectConnection("schemaType=MAP");
    checkMapSchemaDirectConnection(
        "schemaFactory=org.apache.calcite.schema.impl.AbstractSchema$Factory");
  }

  private void checkMapSchemaDirectConnection(String s) throws SQLException {
    final String url = "jdbc:calcite:" + s;
    Connection connection = DriverManager.getConnection(url);
    assertThat(connection.getSchema(), is("adhoc"));
    String expected = "EXPR$0=1\n";
    final String sql = "values 1";
    try (Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      assertThat(CalciteAssert.toString(resultSet), is(expected));
    }
  }

  /** Tests that an immutable schema in a model cannot contain a view. */
  @Test public void testModelImmutableSchemaCannotContainView()
      throws Exception {
    CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [\n"
        + "    {\n"
        + "      name: 'empty'\n"
        + "    },\n"
        + "    {\n"
        + "      name: 'adhoc',\n"
        + "      type: 'custom',\n"
        + "      tables: [\n"
        + "        {\n"
        + "          name: 'v',\n"
        + "          type: 'view',\n"
        + "          sql: 'values (1)'\n"
        + "        }\n"
        + "      ],\n"
        + "      factory: '"
        + MySchemaFactory.class.getName()
        + "',\n"
        + "      operand: {\n"
        + "           'tableName': 'ELVIS',\n"
        + "           'mutable': false\n"
        + "      }\n"
        + "    }\n"
        + "  ]\n"
        + "}")
        .connectThrows(
            "Cannot define view; parent schema 'adhoc' is not mutable");
  }

  private CalciteAssert.AssertThat modelWithView(String view,
      Boolean modifiable) {
    final Class<EmpDeptTableFactory> clazz = EmpDeptTableFactory.class;
    return CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '" + clazz.getName() + "',\n"
        + "           operand: {'foo': true, 'bar': 345}\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MUTABLE_EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '" + clazz.getName() + "',\n"
        + "           operand: {'foo': false}\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'V',\n"
        + "           type: 'view',\n"
        + (modifiable == null ? "" : " modifiable: " + modifiable + ",\n")
        + "           sql: " + new JsonBuilder().toJsonString(view) + "\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}");
  }

  /** Tests a JDBC connection that provides a model that contains a view. */
  @Test public void testModelView() throws Exception {
    final CalciteAssert.AssertThat with =
        modelWithView("select * from \"EMPLOYEES\" where \"deptno\" = 10",
            null);

    with.query("select * from \"adhoc\".V order by \"name\" desc")
        .returns(""
            + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n"
            + "empid=150; deptno=10; name=Sebastian; salary=7000.0; commission=null\n"
            + "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n");

    // Make sure that views appear in metadata.
    with.doWithConnection(connection -> {
      try {
        final DatabaseMetaData metaData = connection.getMetaData();

        // all table types
        try (ResultSet r =
             metaData.getTables(null, "adhoc", null, null)) {
          assertEquals(
              "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=EMPLOYEES; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=MUTABLE_EMPLOYEES; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; TABLE_TYPE=VIEW; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n",
              CalciteAssert.toString(r));
        }

        // including system tables; note that table type is "SYSTEM TABLE"
        // not "SYSTEM_TABLE"
        try (ResultSet r = metaData.getTables(null, null, null, null)) {
          assertEquals(
              "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=EMPLOYEES; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=MUTABLE_EMPLOYEES; TABLE_TYPE=TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; TABLE_TYPE=VIEW; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=metadata; TABLE_NAME=COLUMNS; TABLE_TYPE=SYSTEM TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=metadata; TABLE_NAME=TABLES; TABLE_TYPE=SYSTEM TABLE; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n",
              CalciteAssert.toString(r));
        }

        // views only
        try (ResultSet r = metaData.getTables(null, "adhoc", null,
            new String[]{Schema.TableType.VIEW.jdbcName})) {
          assertEquals(
              "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; TABLE_TYPE=VIEW; REMARKS=null; TYPE_CAT=null; TYPE_SCHEM=null; TYPE_NAME=null; SELF_REFERENCING_COL_NAME=null; REF_GENERATION=null\n",
              CalciteAssert.toString(r));
        }

        // columns
        try (ResultSet r =
                 metaData.getColumns(null, "adhoc", "V", null)) {
          assertEquals(
              "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=empid; DATA_TYPE=4; TYPE_NAME=JavaType(int) NOT NULL; COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=1; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=deptno; DATA_TYPE=4; TYPE_NAME=JavaType(int) NOT NULL; COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=2; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=name; DATA_TYPE=12; TYPE_NAME=JavaType(class java.lang.String); COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=1; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=3; IS_NULLABLE=YES; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=salary; DATA_TYPE=7; TYPE_NAME=JavaType(float) NOT NULL; COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=0; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=4; IS_NULLABLE=NO; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=\n"
                  + "TABLE_CAT=null; TABLE_SCHEM=adhoc; TABLE_NAME=V; COLUMN_NAME=commission; DATA_TYPE=4; TYPE_NAME=JavaType(class java.lang.Integer); COLUMN_SIZE=-1; BUFFER_LENGTH=null; DECIMAL_DIGITS=null; NUM_PREC_RADIX=10; NULLABLE=1; REMARKS=null; COLUMN_DEF=null; SQL_DATA_TYPE=null; SQL_DATETIME_SUB=null; CHAR_OCTET_LENGTH=-1; ORDINAL_POSITION=5; IS_NULLABLE=YES; SCOPE_CATALOG=null; SCOPE_SCHEMA=null; SCOPE_TABLE=null; SOURCE_DATA_TYPE=null; IS_AUTOINCREMENT=; IS_GENERATEDCOLUMN=\n",
              CalciteAssert.toString(r));
        }

        // catalog
        try (ResultSet r = metaData.getCatalogs()) {
          assertEquals(
              "TABLE_CAT=null\n",
              CalciteAssert.toString(r));
        }

        // schemas
        try (ResultSet r = metaData.getSchemas()) {
          assertEquals(
              "TABLE_SCHEM=adhoc; TABLE_CATALOG=null\n"
                  + "TABLE_SCHEM=metadata; TABLE_CATALOG=null\n",
              CalciteAssert.toString(r));
        }

        // schemas (qualified)
        try (ResultSet r = metaData.getSchemas(null, "adhoc")) {
          assertEquals(
              "TABLE_SCHEM=adhoc; TABLE_CATALOG=null\n",
              CalciteAssert.toString(r));
        }

        // table types
        try (ResultSet r = metaData.getTableTypes()) {
          assertEquals("TABLE_TYPE=TABLE\n"
                  + "TABLE_TYPE=VIEW\n",
              CalciteAssert.toString(r));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /** Tests a view with ORDER BY and LIMIT clauses. */
  @Test public void testOrderByView() throws Exception {
    final CalciteAssert.AssertThat with =
        modelWithView("select * from \"EMPLOYEES\" where \"deptno\" = 10 "
            + "order by \"empid\" limit 2", null);
    with
        .query("select \"name\" from \"adhoc\".V order by \"name\"")
        .returns("name=Bill\n"
            + "name=Theodore\n");

    // Now a sub-query with ORDER BY and LIMIT clauses. (Same net effect, but
    // ORDER BY and LIMIT in sub-query were not standard SQL until SQL:2008.)
    with
        .query("select \"name\" from (\n"
            + "select * from \"adhoc\".\"EMPLOYEES\" where \"deptno\" = 10\n"
            + "order by \"empid\" limit 2)\n"
            + "order by \"name\"")
        .returns("name=Bill\n"
            + "name=Theodore\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1900">[CALCITE-1900]
   * Improve error message for cyclic views</a>.
   * Previously got a {@link StackOverflowError}. */
  @Test public void testSelfReferentialView() throws Exception {
    final CalciteAssert.AssertThat with =
        modelWithView("select * from \"V\"", null);
    with.query("select \"name\" from \"adhoc\".V")
        .throws_("Cannot resolve 'adhoc.V'; it references view 'adhoc.V', "
            + "whose definition is cyclic");
  }

  @Test public void testSelfReferentialView2() throws Exception {
    final String model = "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'adhoc',\n"
        + "  schemas: [ {\n"
        + "    name: 'adhoc',\n"
        + "    tables: [ {\n"
        + "      name: 'A',\n"
        + "      type: 'view',\n"
        + "      sql: "
        + new JsonBuilder().toJsonString("select * from B") + "\n"
        + "    }, {\n"
        + "      name: 'B',\n"
        + "      type: 'view',\n"
        + "      sql: "
        + new JsonBuilder().toJsonString("select * from C") + "\n"
        + "    }, {\n"
        + "      name: 'C',\n"
        + "      type: 'view',\n"
        + "      sql: "
        + new JsonBuilder().toJsonString("select * from D, B") + "\n"
        + "    }, {\n"
        + "      name: 'D',\n"
        + "      type: 'view',\n"
        + "      sql: "
        + new JsonBuilder().toJsonString(
            "select * from (values (1, 'a')) as t(x, y)") + "\n"
        + "    } ]\n"
        + "  } ]\n"
        + "}";
    final CalciteAssert.AssertThat with =
        CalciteAssert.model(model);
    //
    //       +-----+
    //       V     |
    // A --> B --> C --> D
    //
    // A is not in a cycle, but depends on cyclic views
    // B is cyclic
    // C is cyclic
    // D is not cyclic
    with.query("select x from \"adhoc\".a")
        .throws_("Cannot resolve 'adhoc.A'; it references view 'adhoc.B', "
            + "whose definition is cyclic");
    with.query("select x from \"adhoc\".b")
        .throws_("Cannot resolve 'adhoc.B'; it references view 'adhoc.B', "
            + "whose definition is cyclic");
    // as previous, but implicit schema
    with.query("select x from b")
        .throws_("Cannot resolve 'B'; it references view 'adhoc.B', "
            + "whose definition is cyclic");
    with.query("select x from \"adhoc\".c")
        .throws_("Cannot resolve 'adhoc.C'; it references view 'adhoc.C', "
            + "whose definition is cyclic");
    with.query("select x from \"adhoc\".d")
        .returns("X=1\n");
    with.query("select x from \"adhoc\".d except select x from \"adhoc\".a")
        .throws_("Cannot resolve 'adhoc.A'; it references view 'adhoc.B', "
            + "whose definition is cyclic");
  }

  /** Tests saving query results into temporary tables, per
   * {@link org.apache.calcite.avatica.Handler.ResultSink}. */
  @Test public void testAutomaticTemporaryTable() throws Exception {
    final List<Object> objects = new ArrayList<>();
    CalciteAssert.that()
        .with(
            new CalciteAssert.ConnectionFactory() {
              public CalciteConnection createConnection() throws SQLException {
                CalciteConnection connection = (CalciteConnection)
                    new AutoTempDriver(objects)
                        .connect("jdbc:calcite:", new Properties());
                final SchemaPlus rootSchema = connection.getRootSchema();
                rootSchema.add("hr",
                    new ReflectiveSchema(new HrSchema()));
                connection.setSchema("hr");
                return connection;
              }
            })
        .doWithConnection(connection -> {
          try {
            final String sql = "select * from \"hr\".\"emps\" "
                + "where \"deptno\" = 10";
            connection.createStatement()
                .executeQuery(sql);
            assertThat(objects.size(), is(1));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test public void testExplain() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with(CalciteAssert.Config.FOODMART_CLONE);
    with.query("explain plan for values (1, 'ab')")
        .returns("PLAN=EnumerableValues(tuples=[[{ 1, 'ab' }]])\n\n");
    final String expectedXml = "PLAN=<RelNode type=\"EnumerableValues\">\n"
        + "\t<Property name=\"tuples\">\n"
        + "\t\t[{ 1, &#39;ab&#39; }]\t</Property>\n"
        + "\t<Inputs/>\n"
        + "</RelNode>\n"
        + "\n";
    with.query("explain plan as xml for values (1, 'ab')")
        .returns(expectedXml);
    final String expectedJson = "PLAN={\n"
        + "  \"rels\": [\n"
        + "    {\n"
        + "      \"id\": \"0\",\n"
        + "      \"relOp\": \"org.apache.calcite.adapter.enumerable.EnumerableValues\",\n"
        + "      \"type\": [\n"
        + "        {\n"
        + "          \"type\": \"INTEGER\",\n"
        + "          \"nullable\": false,\n"
        + "          \"name\": \"EXPR$0\"\n"
        + "        },\n"
        + "        {\n"
        + "          \"type\": \"CHAR\",\n"
        + "          \"nullable\": false,\n"
        + "          \"precision\": 2,\n"
        + "          \"name\": \"EXPR$1\"\n"
        + "        }\n"
        + "      ],\n"
        + "      \"tuples\": [\n"
        + "        [\n"
        + "          1,\n"
        + "          \"ab\"\n"
        + "        ]\n"
        + "      ],\n"
        + "      \"inputs\": []\n"
        + "    }\n"
        + "  ]\n"
        + "}\n";
    with.query("explain plan as json for values (1, 'ab')")
        .returns(expectedJson);
    with.query("explain plan with implementation for values (1, 'ab')")
        .returns("PLAN=EnumerableValues(tuples=[[{ 1, 'ab' }]])\n\n");
    with.query("explain plan without implementation for values (1, 'ab')")
        .returns("PLAN=LogicalValues(tuples=[[{ 1, 'ab' }]])\n\n");
    with.query("explain plan with type for values (1, 'ab')")
        .returns("PLAN=EXPR$0 INTEGER NOT NULL,\n"
            + "EXPR$1 CHAR(2) CHARACTER SET \"ISO-8859-1\" COLLATE \"ISO-8859-1$en_US$primary\" NOT NULL\n");
  }

  /** Test case for bug where if two tables have different element classes
   * but those classes have identical fields, Calcite would generate code to use
   * the wrong element class; a {@link ClassCastException} would ensue. */
  @Test public void testDifferentTypesSameFields() throws Exception {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    final SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("TEST", new ReflectiveSchema(new MySchema()));
    Statement statement = calciteConnection.createStatement();
    ResultSet resultSet =
        statement.executeQuery("SELECT \"myvalue\" from TEST.\"mytable2\"");
    assertEquals("myvalue=2\n", CalciteAssert.toString(resultSet));
    resultSet.close();
    statement.close();
    connection.close();
  }

  /** Tests that CURRENT_TIMESTAMP gives different values each time a statement
   * is executed. */
  @Test public void testCurrentTimestamp() throws Exception {
    CalciteAssert.that()
        .with(CalciteConnectionProperty.TIME_ZONE, "GMT+1:00")
        .doWithConnection(connection -> {
          try {
            final PreparedStatement statement =
                connection.prepareStatement("VALUES CURRENT_TIMESTAMP");
            ResultSet resultSet;

            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());
            String s0 = resultSet.getString(1);
            assertFalse(resultSet.next());

            try {
              Thread.sleep(1000);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }

            resultSet = statement.executeQuery();
            assertTrue(resultSet.next());
            String s1 = resultSet.getString(1);
            assertFalse(resultSet.next());

            assertTrue("\n"
                    + "s0=" + s0 + "\n"
                    + "s1=" + s1 + "\n",
                s0.compareTo(s1) < 0);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Test for timestamps and time zones, based on pgsql TimezoneTest. */
  @Test public void testGetTimestamp() throws Exception {
    CalciteAssert.that()
        .with(CalciteConnectionProperty.TIME_ZONE, "GMT+1:00")
        .doWithConnection(connection -> {
          try {
            checkGetTimestamp(connection);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private void checkGetTimestamp(Connection con) throws SQLException {
    Statement statement = con.createStatement();

    // Not supported yet. We set timezone using connect-string parameters.
    //statement.executeUpdate("alter session set timezone = 'gmt-3'");

    ResultSet rs = statement.executeQuery("SELECT * FROM (VALUES(\n"
        + " TIMESTAMP '1970-01-01 00:00:00',\n"
        + " /* TIMESTAMP '2005-01-01 15:00:00 +0300', */\n"
        + " TIMESTAMP '2005-01-01 15:00:00',\n"
        + " TIME '15:00:00',\n"
        + " /* TIME '15:00:00 +0300', */\n"
        + " DATE '2005-01-01'\n"
        + ")) AS t(ts0, /* tstz, */ ts, t, /* tz, */ d)");
    assertTrue(rs.next());

    TimeZone tzUtc   = TimeZone.getTimeZone("UTC");    // +0000 always
    TimeZone tzGmt03 = TimeZone.getTimeZone("GMT+03"); // +0300 always
    TimeZone tzGmt05 = TimeZone.getTimeZone("GMT-05"); // -0500 always
    TimeZone tzGmt13 = TimeZone.getTimeZone("GMT+13"); // +1000 always

    Calendar cUtc   = Calendar.getInstance(tzUtc, Locale.ROOT);
    Calendar cGmt03 = Calendar.getInstance(tzGmt03, Locale.ROOT);
    Calendar cGmt05 = Calendar.getInstance(tzGmt05, Locale.ROOT);
    Calendar cGmt13 = Calendar.getInstance(tzGmt13, Locale.ROOT);

    Timestamp ts;
    String s;
    int c = 1;

    // timestamp: 1970-01-01 00:00:00
    ts = rs.getTimestamp(c);                     // Convert timestamp to +0100
    assertEquals(-3600000L,      ts.getTime());  // 1970-01-01 00:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);               // Convert timestamp to UTC
    assertEquals(0L,             ts.getTime());  // 1970-01-01 00:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);             // Convert timestamp to +0300
    assertEquals(-10800000L,     ts.getTime());  // 1970-01-01 00:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);             // Convert timestamp to -0500
    assertEquals(18000000L,      ts.getTime());  // 1970-01-01 00:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);             // Convert timestamp to +1300
    assertEquals(-46800000,      ts.getTime());  // 1970-01-01 00:00:00 +1300
    s = rs.getString(c);
    assertEquals("1970-01-01 00:00:00", s);
    ++c;

    if (false) {
      // timestamptz: 2005-01-01 15:00:00+03
      ts = rs.getTimestamp(c);                      // Represents an instant in
                                                    // time, TZ is irrelevant.
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cUtc);                // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cGmt03);              // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cGmt05);              // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ts = rs.getTimestamp(c, cGmt13);              // TZ irrelevant, as above
      assertEquals(1104580800000L, ts.getTime());   // 2005-01-01 12:00:00 UTC
      ++c;
    }

    // timestamp: 2005-01-01 15:00:00
    ts = rs.getTimestamp(c);                     // Convert timestamp to +0100
    assertEquals(1104588000000L, ts.getTime());  // 2005-01-01 15:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);               // Convert timestamp to UTC
    assertEquals(1104591600000L, ts.getTime());  // 2005-01-01 15:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);             // Convert timestamp to +0300
    assertEquals(1104580800000L, ts.getTime());  // 2005-01-01 15:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);             // Convert timestamp to -0500
    assertEquals(1104609600000L, ts.getTime());  // 2005-01-01 15:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);             // Convert timestamp to +1300
    assertEquals(1104544800000L, ts.getTime());  // 2005-01-01 15:00:00 +1300
    s = rs.getString(c);
    assertEquals("2005-01-01 15:00:00", s);
    ++c;

    // time: 15:00:00
    ts = rs.getTimestamp(c);
    assertEquals(50400000L, ts.getTime());        // 1970-01-01 15:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);
    assertEquals(54000000L, ts.getTime());        // 1970-01-01 15:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);
    assertEquals(43200000L, ts.getTime());        // 1970-01-01 15:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);
    assertEquals(72000000L, ts.getTime());        // 1970-01-01 15:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);
    assertEquals(7200000L, ts.getTime());         // 1970-01-01 15:00:00 +1300
    s = rs.getString(c);
    assertEquals("15:00:00", s);
    ++c;

    if (false) {
      // timetz: 15:00:00+03
      ts = rs.getTimestamp(c);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
                                                // 1970-01-01 13:00:00 +0100
      ts = rs.getTimestamp(c, cUtc);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
                                                // 1970-01-01 12:00:00 +0000
      ts = rs.getTimestamp(c, cGmt03);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
                                                // 1970-01-01 15:00:00 +0300
      ts = rs.getTimestamp(c, cGmt05);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
                                                // 1970-01-01 07:00:00 -0500
      ts = rs.getTimestamp(c, cGmt13);
      assertEquals(43200000L, ts.getTime());    // 1970-01-01 15:00:00 +0300 ->
                                                // 1970-01-02 01:00:00 +1300
      ++c;
    }

    // date: 2005-01-01
    ts = rs.getTimestamp(c);
    assertEquals(1104534000000L, ts.getTime()); // 2005-01-01 00:00:00 +0100
    ts = rs.getTimestamp(c, cUtc);
    assertEquals(1104537600000L, ts.getTime()); // 2005-01-01 00:00:00 +0000
    ts = rs.getTimestamp(c, cGmt03);
    assertEquals(1104526800000L, ts.getTime()); // 2005-01-01 00:00:00 +0300
    ts = rs.getTimestamp(c, cGmt05);
    assertEquals(1104555600000L, ts.getTime()); // 2005-01-01 00:00:00 -0500
    ts = rs.getTimestamp(c, cGmt13);
    assertEquals(1104490800000L, ts.getTime()); // 2005-01-01 00:00:00 +1300
    s = rs.getString(c);
    assertEquals("2005-01-01", s);              // 2005-01-01 00:00:00 +0100
    ++c;

    assertTrue(!rs.next());
  }

  /** Tests accessing a column in a JDBC source whose type is DATE. */
  @Test
  public void testGetDate() throws Exception {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .doWithConnection(connection -> {
          try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "select min(\"date\") mindate from \"foodmart\".\"currency\"");
            assertTrue(rs.next());
            assertEquals(
                Date.valueOf("1997-01-01"),
                rs.getDate(1));
            assertFalse(rs.next());
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Tests accessing a date as a string in a JDBC source whose type is DATE. */
  @Test public void testGetDateAsString() throws Exception {
    CalciteAssert.that()
      .with(CalciteAssert.Config.JDBC_FOODMART)
      .query("select min(\"date\") mindate from \"foodmart\".\"currency\"")
      .returns2("MINDATE=1997-01-01\n");
  }

  @Test public void testGetTimestampObject() throws Exception {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .doWithConnection(connection -> {
          try {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(
                "select \"hire_date\" from \"foodmart\".\"employee\" where \"employee_id\" = 1");
            assertTrue(rs.next());
            assertEquals(
                Timestamp.valueOf("1994-12-01 00:00:00"),
                rs.getTimestamp(1));
            assertFalse(rs.next());
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  @Test public void testRowComparison() {
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_SCOTT)
        .query("SELECT empno FROM JDBC_SCOTT.emp WHERE (ename, job) < ('Blake', 'Manager')")
        .returnsUnordered("EMPNO=7876", "EMPNO=7499", "EMPNO=7698");
  }

  @Test public void testUnicode() throws Exception {
    CalciteAssert.AssertThat with =
        CalciteAssert.that().with(CalciteAssert.Config.FOODMART_CLONE);

    // Note that \u82f1 in a Java string is a Java unicode escape;
    // But \\82f1 in a SQL string is a SQL unicode escape.
    // various ways to create a unicode string literal
    with.query("values _UTF16'\u82f1\u56fd'")
        .returns("EXPR$0=\u82f1\u56fd\n");
    with.query("values U&'\\82F1\\56FD'")
        .returns("EXPR$0=\u82f1\u56fd\n");
    with.query("values u&'\\82f1\\56fd'")
        .returns("EXPR$0=\u82f1\u56fd\n");
    with.query("values '\u82f1\u56fd'")
        .throws_(
            "Failed to encode '\u82f1\u56fd' in character set 'ISO-8859-1'");

    // comparing a unicode string literal with a regular string literal
    with.query(
        "select * from \"employee\" where \"full_name\" = '\u82f1\u56fd'")
        .throws_(
            "Failed to encode '\u82f1\u56fd' in character set 'ISO-8859-1'");
    with.query(
        "select * from \"employee\" where \"full_name\" = _UTF16'\u82f1\u56fd'")
        .throws_(
            "Cannot apply = to the two different charsets ISO-8859-1 and UTF-16LE");

    // The CONVERT function (what SQL:2011 calls "character transliteration") is
    // not implemented yet. See
    // https://issues.apache.org/jira/browse/CALCITE-111.
    with.query("select * from \"employee\"\n"
        + "where convert(\"full_name\" using UTF16) = _UTF16'\u82f1\u56fd'")
        .throws_("Column 'UTF16' not found in any table");
  }

  /** Tests metadata for the MySQL lexical scheme. */
  @Test public void testLexMySQL() throws Exception {
    CalciteAssert.that()
        .with(Lex.MYSQL)
        .doWithConnection(connection -> {
          try {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getIdentifierQuoteString(), equalTo("`"));
            assertThat(metaData.supportsMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                equalTo(false));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Tests metadata for the MySQL ANSI lexical scheme. */
  @Test public void testLexMySQLANSI() throws Exception {
    CalciteAssert.that()
        .with(Lex.MYSQL_ANSI)
        .doWithConnection(connection -> {
          try {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getIdentifierQuoteString(), equalTo("\""));
            assertThat(metaData.supportsMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                equalTo(false));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Tests metadata for different the "SQL_SERVER" lexical scheme. */
  @Test public void testLexSqlServer() throws Exception {
    CalciteAssert.that()
        .with(Lex.SQL_SERVER)
        .doWithConnection(connection -> {
          try {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getIdentifierQuoteString(), equalTo("["));
            assertThat(metaData.supportsMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                equalTo(false));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Tests metadata for the ORACLE (and default) lexical scheme. */
  @Test public void testLexOracle() throws Exception {
    CalciteAssert.that()
        .with(Lex.ORACLE)
        .doWithConnection(connection -> {
          try {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getIdentifierQuoteString(),
                equalTo("\""));
            assertThat(metaData.supportsMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesLowerCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                equalTo(true));
            // Oracle JDBC 12.1.0.1.0 returns true here, however it is
            // not clear if the bug is in JDBC specification or Oracle
            // driver
            assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesUpperCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                equalTo(false));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Tests metadata for the JAVA lexical scheme. */
  @Test public void testLexJava() throws Exception {
    CalciteAssert.that()
        .with(Lex.JAVA)
        .doWithConnection(connection -> {
          try {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getIdentifierQuoteString(),
                equalTo("`"));
            assertThat(metaData.supportsMixedCaseIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesUpperCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                equalTo(false));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Tests metadata for the ORACLE lexical scheme overridden like JAVA. */
  @Test public void testLexOracleAsJava() throws Exception {
    CalciteAssert.that()
        .with(Lex.ORACLE)
        .with(CalciteConnectionProperty.QUOTING, Quoting.BACK_TICK)
        .with(CalciteConnectionProperty.UNQUOTED_CASING, Casing.UNCHANGED)
        .with(CalciteConnectionProperty.QUOTED_CASING, Casing.UNCHANGED)
        .with(CalciteConnectionProperty.CASE_SENSITIVE, true)
        .doWithConnection(connection -> {
          try {
            DatabaseMetaData metaData = connection.getMetaData();
            assertThat(metaData.getIdentifierQuoteString(),
                equalTo("`"));
            assertThat(metaData.supportsMixedCaseIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesMixedCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesUpperCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseIdentifiers(),
                equalTo(false));
            assertThat(metaData.supportsMixedCaseQuotedIdentifiers(),
                equalTo(true));
            assertThat(metaData.storesMixedCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesUpperCaseQuotedIdentifiers(),
                equalTo(false));
            assertThat(metaData.storesLowerCaseQuotedIdentifiers(),
                equalTo(false));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Tests case-insensitive resolution of schema and table names. */
  @Test public void testLexCaseInsensitive() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with(Lex.MYSQL);
    with.query("select COUNT(*) as c from metaData.tAbles")
        .returns("c=2\n");
    with.query("select COUNT(*) as c from `metaData`.`tAbles`")
        .returns("c=2\n");

    // case-sensitive gives error
    final CalciteAssert.AssertThat with2 =
        CalciteAssert.that().with(Lex.JAVA);
    with2.query("select COUNT(*) as c from `metaData`.`tAbles`")
        .throws_("Object 'metaData' not found; did you mean 'metadata'?");
    with2.query("select COUNT(*) as c from `metaData`.`TABLES`")
        .throws_("Object 'metaData' not found; did you mean 'metadata'?");
    with2.query("select COUNT(*) as c from `metaData`.`tables`")
        .throws_("Object 'metaData' not found; did you mean 'metadata'?");
    with2.query("select COUNT(*) as c from `metaData`.`nonExistent`")
        .throws_("Object 'metaData' not found; did you mean 'metadata'?");
    with2.query("select COUNT(*) as c from `metadata`.`tAbles`")
        .throws_("Object 'tAbles' not found within 'metadata'; did you mean 'TABLES'?");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1563">[CALCITE-1563]
   * In case-insensitive connection, non-existent tables use alphabetically
   * preceding table</a>. */
  @Test public void testLexCaseInsensitiveFindsNonexistentTable() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with(Lex.MYSQL);
    // With [CALCITE-1563], the following query succeeded; it queried
    // metadata.tables.
    with.query("select COUNT(*) as c from `metaData`.`zoo`")
        .throws_("Object 'zoo' not found within 'metadata'");
    with.query("select COUNT(*) as c from `metaData`.`tAbLes`")
        .returns("c=2\n");
  }

  /** Tests case-insensitive resolution of sub-query columns.
   *
   * <p>Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-550">[CALCITE-550]
   * Case-insensitive matching of sub-query columns fails</a>. */
  @Test public void testLexCaseInsensitiveSubQueryField() {
    CalciteAssert.that()
        .with(Lex.MYSQL)
        .query("select DID\n"
            + "from (select deptid as did\n"
            + "         FROM\n"
            + "            ( values (1), (2) ) as T1(deptid)\n"
            + "         ) ")
        .returnsUnordered("DID=1", "DID=2");
  }

  @Test public void testLexCaseInsensitiveTableAlias() {
    CalciteAssert.that()
        .with(Lex.MYSQL)
        .query("select e.empno\n"
            + "from (values (1, 2)) as E (empno, deptno),\n"
            + "  (values (3, 4)) as d (deptno, name)")
        .returnsUnordered("empno=1");
  }

  @Test public void testFunOracle() {
    CalciteAssert.that(CalciteAssert.Config.REGULAR)
        .with(CalciteConnectionProperty.FUN, "oracle")
        .query("select nvl(\"commission\", -99) as c from \"hr\".\"emps\"")
        .returnsUnordered("C=-99",
            "C=1000",
            "C=250",
            "C=500");

    // NVL is not present in the default operator table
    CalciteAssert.that(CalciteAssert.Config.REGULAR)
        .query("select nvl(\"commission\", -99) as c from \"hr\".\"emps\"")
        .throws_("No match found for function signature NVL(<NUMERIC>, <NUMERIC>)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2072">[CALCITE-2072]
   * Enable spatial operator table by adding 'fun=spatial'to JDBC URL</a>. */
  @Test public void testFunSpatial() {
    final String sql = "select distinct\n"
        + "  ST_PointFromText('POINT(-71.0642.28)') as c\n"
        + "from \"hr\".\"emps\"";
    CalciteAssert.that(CalciteAssert.Config.REGULAR)
        .with(CalciteConnectionProperty.FUN, "spatial")
        .query(sql)
        .returnsUnordered("C={\"x\":-71.0642,\"y\":0.28}");

    // NVL is present in the Oracle operator table, but not spatial or core
    CalciteAssert.that(CalciteAssert.Config.REGULAR)
        .query("select nvl(\"commission\", -99) as c from \"hr\".\"emps\"")
        .throws_("No match found for function signature NVL(<NUMERIC>, <NUMERIC>)");
  }

  /** Unit test for LATERAL CROSS JOIN to table function. */
  @Test public void testLateralJoin() {
    final String sql = "SELECT *\n"
        + "FROM AUX.SIMPLETABLE ST\n"
        + "CROSS JOIN LATERAL TABLE(AUX.TBLFUN(ST.INTCOL))";
    CalciteAssert.that(CalciteAssert.Config.AUX)
        .query(sql)
        .returnsUnordered(
            "STRCOL=ABC; INTCOL=1; n=0; s=",
            "STRCOL=DEF; INTCOL=2; n=0; s=",
            "STRCOL=DEF; INTCOL=2; n=1; s=a",
            "STRCOL=GHI; INTCOL=3; n=0; s=",
            "STRCOL=GHI; INTCOL=3; n=1; s=a",
            "STRCOL=GHI; INTCOL=3; n=2; s=ab");
  }

  /** Unit test for view expansion with lateral join. */
  @Test public void testExpandViewWithLateralJoin() {
    final String sql = "SELECT * FROM AUX.VIEWLATERAL";
    CalciteAssert.that(CalciteAssert.Config.AUX)
        .query(sql)
        .returnsUnordered(
            "STRCOL=ABC; INTCOL=1; n=0; s=",
            "STRCOL=DEF; INTCOL=2; n=0; s=",
            "STRCOL=DEF; INTCOL=2; n=1; s=a",
            "STRCOL=GHI; INTCOL=3; n=0; s=",
            "STRCOL=GHI; INTCOL=3; n=1; s=a",
            "STRCOL=GHI; INTCOL=3; n=2; s=ab");
  }

  /** Tests that {@link Hook#PARSE_TREE} works. */
  @Test public void testHook() {
    final int[] callCount = {0};
    try (Hook.Closeable ignored = Hook.PARSE_TREE.<Object[]>addThread(
        args -> {
          assertThat(args.length, equalTo(2));
          assertThat(args[0], instanceOf(String.class));
          assertThat(args[0],
              equalTo("select \"deptno\", \"commission\", sum(\"salary\") s\n"
                  + "from \"hr\".\"emps\"\n"
                  + "group by \"deptno\", \"commission\""));
          assertThat(args[1], instanceOf(SqlSelect.class));
          ++callCount[0];
        })) {
      // Simple query does not run the hook.
      testSimple();
      assertThat(callCount[0], equalTo(0));

      // Non-trivial query runs hook once.
      testGroupByNull();
      assertThat(callCount[0], equalTo(1));
    }
  }

  /** Tests {@link SqlDialect}. */
  @Test public void testDialect() {
    final String[] sqls = {null};
    CalciteAssert.that()
        .with(CalciteAssert.Config.JDBC_FOODMART)
        .query("select count(*) as c from \"foodmart\".\"employee\" as e1\n"
            + "  where \"first_name\" = 'abcde'\n"
            + "  and \"gender\" = 'F'")
        .withHook(Hook.QUERY_PLAN, (Consumer<String>) sql -> sqls[0] = sql)
        .returns("C=0\n");
    switch (CalciteAssert.DB) {
    case HSQLDB:
      assertThat(sqls[0],
          isLinux("SELECT COUNT(*) AS \"C\"\n"
              + "FROM \"foodmart\".\"employee\"\n"
              + "WHERE \"first_name\" = 'abcde' AND \"gender\" = 'F'"));
      break;
    }
  }

  @Test public void testExplicitImplicitSchemaSameName() throws Exception {
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();

    // create schema "/a"
    final Map<String, Schema> aSubSchemaMap = new HashMap<>();
    final SchemaPlus aSchema = rootSchema.add("a",
        new AbstractSchema() {
          @Override protected Map<String, Schema> getSubSchemaMap() {
            return aSubSchemaMap;
          }
        });

    // add explicit schema "/a/b".
    aSchema.add("b", new AbstractSchema());

    // add implicit schema "/a/b"
    aSubSchemaMap.put("b", new AbstractSchema());

    aSchema.setCacheEnabled(true);

    // explicit should win implicit.
    assertThat(aSchema.getSubSchemaNames().size(), is(1));
  }

  @Test public void testSimpleCalciteSchema() throws Exception {
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();

    // create schema "/a"
    final Map<String, Schema> aSubSchemaMap = new HashMap<>();
    final SchemaPlus aSchema = rootSchema.add("a",
        new AbstractSchema() {
          @Override protected Map<String, Schema> getSubSchemaMap() {
            return aSubSchemaMap;
          }
        });

    // add explicit schema "/a/b".
    aSchema.add("b", new AbstractSchema());

    // add implicit schema "/a/c"
    aSubSchemaMap.put("c", new AbstractSchema());

    assertThat(aSchema.getSubSchema("c"), notNullValue());
    assertThat(aSchema.getSubSchema("b"), notNullValue());

    // add implicit schema "/a/b"
    aSubSchemaMap.put("b", new AbstractSchema());
    // explicit should win implicit.
    assertThat(aSchema.getSubSchemaNames().size(), is(2));
  }

  @Test public void testSimpleCalciteSchemaWithView() throws Exception {
    final SchemaPlus rootSchema = CalciteSchema.createRootSchema(false, false).plus();

    final Multimap<String, org.apache.calcite.schema.Function> functionMap =
        LinkedListMultimap.create();
    // create schema "/a"
    final SchemaPlus aSchema = rootSchema.add("a",
        new AbstractSchema() {
          @Override protected Multimap<String, org.apache.calcite.schema.Function>
          getFunctionMultimap() {
            return functionMap;
          }
        });
    // add view definition
    final String viewName = "V";
    final org.apache.calcite.schema.Function view =
        ViewTable.viewMacro(rootSchema.getSubSchema("a"),
            "values('1', '2')", null, null, false);
    functionMap.put(viewName, view);

    final CalciteSchema calciteSchema = CalciteSchema.from(aSchema);
    assertThat(
        calciteSchema.getTableBasedOnNullaryFunction(viewName, true), notNullValue());
    assertThat(
        calciteSchema.getTableBasedOnNullaryFunction(viewName, false), notNullValue());
    assertThat(
        calciteSchema.getTableBasedOnNullaryFunction("V1", true), nullValue());
    assertThat(
        calciteSchema.getTableBasedOnNullaryFunction("V1", false), nullValue());

    assertThat(calciteSchema.getFunctions(viewName, true), hasItem(view));
    assertThat(calciteSchema.getFunctions(viewName, false), hasItem(view));
    assertThat(calciteSchema.getFunctions("V1", true), not(hasItem(view)));
    assertThat(calciteSchema.getFunctions("V1", false), not(hasItem(view)));
  }

  @Test public void testSchemaCaching() throws Exception {
    final Connection connection =
        CalciteAssert.that(CalciteAssert.Config.JDBC_FOODMART).connect();
    final CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    final SchemaPlus rootSchema = calciteConnection.getRootSchema();

    // create schema "/a"
    final Map<String, Schema> aSubSchemaMap = new HashMap<>();
    final SchemaPlus aSchema = rootSchema.add("a", new AbstractSchema() {
      @Override protected Map<String, Schema> getSubSchemaMap() {
        return aSubSchemaMap;
      }
    });
    aSchema.setCacheEnabled(true);
    assertThat(aSchema.getSubSchemaNames().size(), is(0));

    // first call, to populate the cache
    assertThat(aSchema.getSubSchemaNames().size(), is(0));

    // create schema "/a/b1". Appears only when we disable caching.
    aSubSchemaMap.put("b1", new AbstractSchema());
    assertThat(aSchema.getSubSchemaNames().size(), is(0));
    assertThat(aSchema.getSubSchema("b1"), nullValue());
    aSchema.setCacheEnabled(false);
    assertThat(aSchema.getSubSchemaNames().size(), is(1));
    assertThat(aSchema.getSubSchema("b1"), notNullValue());

    // create schema "/a/b2". Appears immediately, because caching is disabled.
    aSubSchemaMap.put("b2", new AbstractSchema());
    assertThat(aSchema.getSubSchemaNames().size(), is(2));

    // an explicit sub-schema appears immediately, even if caching is enabled
    aSchema.setCacheEnabled(true);
    assertThat(aSchema.getSubSchemaNames().size(), is(2));
    aSchema.add("b3", new AbstractSchema()); // explicit
    aSubSchemaMap.put("b4", new AbstractSchema()); // implicit
    assertThat(aSchema.getSubSchemaNames().size(), is(3));
    aSchema.setCacheEnabled(false);
    assertThat(aSchema.getSubSchemaNames().size(), is(4));
    for (String name : aSchema.getSubSchemaNames()) {
      assertThat(aSchema.getSubSchema(name), notNullValue());
    }

    // create schema "/a2"
    final Map<String, Schema> a2SubSchemaMap = new HashMap<>();
    final SchemaPlus a2Schema = rootSchema.add("a", new AbstractSchema() {
      @Override protected Map<String, Schema> getSubSchemaMap() {
        return a2SubSchemaMap;
      }
    });
    a2Schema.setCacheEnabled(true);
    assertThat(a2Schema.getSubSchemaNames().size(), is(0));

    // create schema "/a2/b3". Change not visible since caching is enabled.
    a2SubSchemaMap.put("b3", new AbstractSchema());
    assertThat(a2Schema.getSubSchemaNames().size(), is(0));
    Thread.sleep(1);
    assertThat(a2Schema.getSubSchemaNames().size(), is(0));

    // Change visible after we turn off caching.
    a2Schema.setCacheEnabled(false);
    assertThat(a2Schema.getSubSchemaNames().size(), is(1));
    a2SubSchemaMap.put("b4", new AbstractSchema());
    assertThat(a2Schema.getSubSchemaNames().size(), is(2));
    for (String name : aSchema.getSubSchemaNames()) {
      assertThat(aSchema.getSubSchema(name), notNullValue());
    }

    // add tables and retrieve with various case sensitivities
    final TableInRootSchemaTest.SimpleTable table =
        new TableInRootSchemaTest.SimpleTable();
    a2Schema.add("table1", table);
    a2Schema.add("TABLE1", table);
    a2Schema.add("tabLe1", table);
    a2Schema.add("tabLe2", table);
    assertThat(a2Schema.getTableNames().size(), equalTo(4));
    final CalciteSchema a2CalciteSchema = CalciteSchema.from(a2Schema);
    assertThat(a2CalciteSchema.getTable("table1", true), notNullValue());
    assertThat(a2CalciteSchema.getTable("table1", false), notNullValue());
    assertThat(a2CalciteSchema.getTable("taBle1", true), nullValue());
    assertThat(a2CalciteSchema.getTable("taBle1", false), notNullValue());
    final TableMacro function =
        ViewTable.viewMacro(a2Schema, "values 1", null, null, null);
    Util.discard(function);

    connection.close();
  }

  @Test public void testCaseSensitiveSubQueryOracle() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that()
            .with(Lex.ORACLE);

    with.query("select DID from (select DEPTID as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select x.DID from (select DEPTID as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X")
        .returnsUnordered("DID=1", "DID=2");
  }

  @Test public void testUnquotedCaseSensitiveSubQueryMySql() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that()
            .with(Lex.MYSQL);

    with.query("select DID from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select x.DID from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select X.DID from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select X.DID2 from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X (DID2)")
        .returnsUnordered("DID2=1", "DID2=2");

    with.query("select X.DID2 from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X (DID2)")
        .returnsUnordered("DID2=1", "DID2=2");
  }

  @Test public void testQuotedCaseSensitiveSubQueryMySql() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that()
            .with(Lex.MYSQL);

    with.query("select `DID` from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select `x`.`DID` from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select `X`.`DID` from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X ")
        .returnsUnordered("DID=1", "DID=2");

    with.query("select `X`.`DID2` from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X (DID2)")
        .returnsUnordered("DID2=1", "DID2=2");

    with.query("select `X`.`DID2` from (select deptid as did FROM\n"
        + "     ( values (1), (2) ) as T1(deptid) ) X (DID2)")
        .returnsUnordered("DID2=1", "DID2=2");
  }

  @Test public void testUnquotedCaseSensitiveSubQuerySqlServer() {
    CalciteAssert.that()
        .with(Lex.SQL_SERVER)
        .query("select DID from (select deptid as did FROM\n"
            + "     ( values (1), (2) ) as T1(deptid) ) ")
        .returnsUnordered("DID=1", "DID=2");
  }

  @Test public void testQuotedCaseSensitiveSubQuerySqlServer() {
    CalciteAssert.that()
        .with(Lex.SQL_SERVER)
        .query("select [DID] from (select deptid as did FROM\n"
            + "     ( values (1), (2) ) as T1([deptid]) ) ")
        .returnsUnordered("DID=1", "DID=2");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-596">[CALCITE-596]
   * JDBC adapter incorrectly reads null values as 0</a>.
   */
  @Test public void testPrimitiveColumnsWithNullValues() throws Exception {
    String hsqldbMemUrl = "jdbc:hsqldb:mem:.";
    Connection baseConnection = DriverManager.getConnection(hsqldbMemUrl);
    Statement baseStmt = baseConnection.createStatement();
    baseStmt.execute("CREATE TABLE T1 (\n"
        + "ID INTEGER,\n"
        + "VALS DOUBLE)");
    baseStmt.execute("INSERT INTO T1 VALUES (1, 1.0)");
    baseStmt.execute("INSERT INTO T1 VALUES (2, null)");
    baseStmt.execute("INSERT INTO T1 VALUES (null, 2.0)");

    baseStmt.close();
    baseConnection.commit();

    Properties info = new Properties();
    info.put("model",
        "inline:"
        + "{\n"
        + "  version: '1.0',\n"
        + "  defaultSchema: 'BASEJDBC',\n"
        + "  schemas: [\n"
        + "     {\n"
        + "       type: 'jdbc',\n"
        + "       name: 'BASEJDBC',\n"
        + "       jdbcDriver: '" + jdbcDriver.class.getName() + "',\n"
        + "       jdbcUrl: '" + hsqldbMemUrl + "',\n"
        + "       jdbcCatalog: null,\n"
        + "       jdbcSchema: null\n"
        + "     }\n"
        + "  ]\n"
        + "}");

    Connection calciteConnection =
        DriverManager.getConnection("jdbc:calcite:", info);

    ResultSet rs = calciteConnection.prepareStatement("select * from t1")
        .executeQuery();

    assertThat(rs.next(), is(true));
    assertThat((Integer) rs.getObject("ID"), equalTo(1));
    assertThat((Double) rs.getObject("VALS"), equalTo(1.0));

    assertThat(rs.next(), is(true));
    assertThat((Integer) rs.getObject("ID"), equalTo(2));
    assertThat(rs.getObject("VALS"), nullValue());

    assertThat(rs.next(), is(true));
    assertThat(rs.getObject("ID"), nullValue());
    assertThat((Double) rs.getObject("VALS"), equalTo(2.0));

    rs.close();
    calciteConnection.close();

  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2054">[CALCITE-2054]
   * Error while validating UPDATE with dynamic parameter in SET clause</a>.
   */
  @Test public void testUpdateBind() throws Exception {
    String hsqldbMemUrl = "jdbc:hsqldb:mem:.";
    try (Connection baseConnection = DriverManager.getConnection(hsqldbMemUrl);
         Statement baseStmt = baseConnection.createStatement()) {
      baseStmt.execute("CREATE TABLE T2 (\n"
          + "ID INTEGER,\n"
          + "VALS DOUBLE)");
      baseStmt.execute("INSERT INTO T2 VALUES (1, 1.0)");
      baseStmt.execute("INSERT INTO T2 VALUES (2, null)");
      baseStmt.execute("INSERT INTO T2 VALUES (null, 2.0)");

      baseStmt.close();
      baseConnection.commit();

      Properties info = new Properties();
      final String model = "inline:"
          + "{\n"
          + "  version: '1.0',\n"
          + "  defaultSchema: 'BASEJDBC',\n"
          + "  schemas: [\n"
          + "     {\n"
          + "       type: 'jdbc',\n"
          + "       name: 'BASEJDBC',\n"
          + "       jdbcDriver: '" + jdbcDriver.class.getName() + "',\n"
          + "       jdbcUrl: '" + hsqldbMemUrl + "',\n"
          + "       jdbcCatalog: null,\n"
          + "       jdbcSchema: null\n"
          + "     }\n"
          + "  ]\n"
          + "}";
      info.put("model", model);

      Connection calciteConnection =
          DriverManager.getConnection("jdbc:calcite:", info);

      ResultSet rs = calciteConnection.prepareStatement("select * from t2")
          .executeQuery();

      assertThat(rs.next(), is(true));
      assertThat((Integer) rs.getObject("ID"), is(1));
      assertThat((Double) rs.getObject("VALS"), is(1.0));

      assertThat(rs.next(), is(true));
      assertThat((Integer) rs.getObject("ID"), is(2));
      assertThat(rs.getObject("VALS"), nullValue());

      assertThat(rs.next(), is(true));
      assertThat(rs.getObject("ID"), nullValue());
      assertThat((Double) rs.getObject("VALS"), equalTo(2.0));

      rs.close();

      final String sql = "update t2 set vals=? where id=?";
      try (PreparedStatement ps =
               calciteConnection.prepareStatement(sql)) {
        ParameterMetaData pmd = ps.getParameterMetaData();
        assertThat(pmd.getParameterCount(), is(2));
        assertThat(pmd.getParameterType(1), is(Types.DOUBLE));
        assertThat(pmd.getParameterType(2), is(Types.INTEGER));
        ps.close();
      }
      calciteConnection.close();
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-730">[CALCITE-730]
   * ClassCastException in table from CloneSchema</a>. */
  @Test public void testNullableNumericColumnInCloneSchema() {
    CalciteAssert.model("{\n"
            + "  version: '1.0',\n"
            + "  defaultSchema: 'SCOTT_CLONE',\n"
            + "  schemas: [ {\n"
            + "    name: 'SCOTT_CLONE',\n"
            + "    type: 'custom',\n"
            + "    factory: 'org.apache.calcite.adapter.clone.CloneSchema$Factory',\n"
            + "    operand: {\n"
            + "      jdbcDriver: '" + JdbcTest.SCOTT.driver + "',\n"
            + "      jdbcUser: '" + JdbcTest.SCOTT.username + "',\n"
            + "      jdbcPassword: '" + JdbcTest.SCOTT.password + "',\n"
            + "      jdbcUrl: '" + JdbcTest.SCOTT.url + "',\n"
            + "      jdbcSchema: 'SCOTT'\n"
            + "   } } ]\n"
            + "}")
        .query("select * from emp")
        .returns(input -> {
          final StringBuilder buf = new StringBuilder();
          try {
            final int columnCount = input.getMetaData().getColumnCount();
            while (input.next()) {
              for (int i = 0; i < columnCount; i++) {
                buf.append(input.getObject(i + 1));
              }
            }
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1097">[CALCITE-1097]
   * Exception when executing query with too many aggregation columns</a>. */
  @Test public void testAggMultipleMeasures() throws SQLException {
    final Driver driver = new Driver();
    CalciteConnection connection = (CalciteConnection)
        driver.connect("jdbc:calcite:", new Properties());
    SchemaPlus rootSchema = connection.getRootSchema();
    rootSchema.add("sale", new ReflectiveSchema(new Smalls.WideSaleSchema()));
    connection.setSchema("sale");
    final Statement statement = connection.createStatement();

    // 200 columns: sum(sale0) + ... sum(sale199)
    ResultSet resultSet =
        statement.executeQuery("select s.\"prodId\"" + sums(200, true) + "\n"
            + "from \"sale\".\"prod\" as s group by s.\"prodId\"\n");
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getInt(1), is(100));
    assertThat(resultSet.getInt(2), is(10));
    assertThat(resultSet.getInt(200), is(10));
    assertThat(resultSet.next(), is(false));

    // 800 columns:
    //   sum(sale0 + 0) + ... + sum(sale0 + 100) + ... sum(sale99 + 799)
    final int n = 800;
    resultSet =
        statement.executeQuery("select s.\"prodId\"" + sums(n, false) + "\n"
            + "from \"sale\".\"prod\" as s group by s.\"prodId\"\n");
    assertThat(resultSet.next(), is(true));
    assertThat(resultSet.getInt(1), is(100));
    assertThat(resultSet.getInt(2), is(10));
    assertThat(resultSet.getInt(n), is(n + 8));
    assertThat(resultSet.next(), is(false));

    connection.close();
  }


  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2224">[CALCITE-2224]
   * WITHIN GROUP clause for aggregate functions</a>. */
  @Test public void testWithinGroupClause1() {
    final String sql = "select X,\n"
        + " collect(Y) within group (order by Y desc) as \"SET\"\n"
        + "from (values (1, 'a'), (1, 'b'),\n"
        + "             (3, 'c'), (3, 'd')) AS t(X, Y)\n"
        + "group by X\n"
        + "limit 10";
    CalciteAssert.that().query(sql)
        .returnsUnordered("X=1; SET=[b, a]",
            "X=3; SET=[d, c]");
  }

  @Test public void testWithinGroupClause2() {
    final String sql = "select X,\n"
        + " collect(Y) within group (order by Y desc) as SET_1,\n"
        + " collect(Y) within group (order by Y asc) as SET_2\n"
        + "from (values (1, 'a'), (1, 'b'), (3, 'c'), (3, 'd')) AS t(X, Y)\n"
        + "group by X\n"
        + "limit 10";
    CalciteAssert
        .that()
        .query(sql)
        .returnsUnordered("X=1; SET_1=[b, a]; SET_2=[a, b]",
            "X=3; SET_1=[d, c]; SET_2=[c, d]");
  }

  @Test public void testWithinGroupClause3() {
    final String sql = "select"
        + " collect(Y) within group (order by Y desc) as SET_1,\n"
        + " collect(Y) within group (order by Y asc) as SET_2\n"
        + "from (values (1, 'a'), (1, 'b'), (3, 'c'), (3, 'd')) AS t(X, Y)\n"
        + "limit 10";
    CalciteAssert.that().query(sql)
        .returns("SET_1=[d, c, b, a]; SET_2=[a, b, c, d]\n");
  }

  @Test public void testWithinGroupClause4() {
    final String sql = "select"
        + " collect(Y) within group (order by Y desc) as SET_1,\n"
        + " collect(Y) within group (order by Y asc) as SET_2\n"
        + "from (values (1, 'a'), (1, 'b'), (3, 'c'), (3, 'd')) AS t(X, Y)\n"
        + "group by X\n"
        + "limit 10";
    CalciteAssert.that().query(sql)
        .returnsUnordered("SET_1=[b, a]; SET_2=[a, b]",
            "SET_1=[d, c]; SET_2=[c, d]");
  }

  @Test public void testWithinGroupClause5() {
    CalciteAssert
        .that()
        .query("select collect(array[X, Y])\n"
            + " within group (order by Y desc) as \"SET\"\n"
            + "from (values ('b', 'a'), ('a', 'b'), ('a', 'c'),\n"
            + "             ('a', 'd')) AS t(X, Y)\n"
            + "limit 10")
        .returns("SET=[[a, d], [a, c], [a, b], [b, a]]\n");
  }

  @Test public void testWithinGroupClause6() {
    final String sql = "select collect(\"commission\")"
        + " within group (order by \"commission\")\n"
        + "from \"hr\".\"emps\"";
    CalciteAssert.that()
        .with(CalciteAssert.Config.REGULAR)
        .query(sql)
        .explainContains("EnumerableAggregate(group=[{}], "
            + "EXPR$0=[COLLECT($4) WITHIN GROUP ([4])])")
        .returns("EXPR$0=[250, 500, 1000]\n");
  }

  /**
   * Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2609">[CALCITE-2609]
   * Dynamic parameters ("?") pushed to underlying JDBC schema, causing
   * error</a>.
   */
  @Test public void testQueryWithParameter() throws Exception {
    String hsqldbMemUrl = "jdbc:hsqldb:mem:.";
    try (Connection baseConnection = DriverManager.getConnection(hsqldbMemUrl);
         Statement baseStmt = baseConnection.createStatement()) {
      baseStmt.execute("CREATE TABLE T3 (\n"
          + "ID INTEGER,\n"
          + "VALS DOUBLE)");
      baseStmt.execute("INSERT INTO T3 VALUES (1, 1.0)");
      baseStmt.execute("INSERT INTO T3 VALUES (2, null)");
      baseStmt.execute("INSERT INTO T3 VALUES (null, 2.0)");
      baseStmt.close();
      baseConnection.commit();

      Properties info = new Properties();
      final String model = "inline:"
          + "{\n"
          + "  version: '1.0',\n"
          + "  defaultSchema: 'BASEJDBC',\n"
          + "  schemas: [\n"
          + "     {\n"
          + "       type: 'jdbc',\n"
          + "       name: 'BASEJDBC',\n"
          + "       jdbcDriver: '" + jdbcDriver.class.getName() + "',\n"
          + "       jdbcUrl: '" + hsqldbMemUrl + "',\n"
          + "       jdbcCatalog: null,\n"
          + "       jdbcSchema: null\n"
          + "     }\n"
          + "  ]\n"
          + "}";
      info.put("model", model);

      Connection calciteConnection =
          DriverManager.getConnection("jdbc:calcite:", info);

      final String sql = "select * from t3 where vals = ?";
      try (PreparedStatement ps =
               calciteConnection.prepareStatement(sql)) {
        ParameterMetaData pmd = ps.getParameterMetaData();
        assertThat(pmd.getParameterCount(), is(1));
        assertThat(pmd.getParameterType(1), is(Types.DOUBLE));
        ps.setDouble(1, 1.0);
        ps.executeQuery();
      }
      calciteConnection.close();
    }
  }

  private static String sums(int n, boolean c) {
    final StringBuilder b = new StringBuilder();
    for (int i = 0; i < n; i++) {
      if (c) {
        b.append(", sum(s.\"sale").append(i).append("\")");
      } else {
        b.append(", sum(s.\"sale").append(i % 100).append("\"").append(" + ")
            .append(i).append(")");
      }
    }
    return b.toString();
  }

  // Disable checkstyle, so it doesn't complain about fields like "customer_id".
  //CHECKSTYLE: OFF

  public static class HrSchema {
    @Override public String toString() {
      return "HrSchema";
    }

    public final Employee[] emps = {
      new Employee(100, 10, "Bill", 10000, 1000),
      new Employee(200, 20, "Eric", 8000, 500),
      new Employee(150, 10, "Sebastian", 7000, null),
      new Employee(110, 10, "Theodore", 11500, 250),
    };
    public final Department[] depts = {
      new Department(10, "Sales", Arrays.asList(emps[0], emps[2]),
          new Location(-122, 38)),
      new Department(30, "Marketing", ImmutableList.of(), new Location(0, 52)),
      new Department(40, "HR", Collections.singletonList(emps[1]), null),
    };
    public final Dependent[] dependents = {
      new Dependent(10, "Michael"),
      new Dependent(10, "Jane"),
    };
    public final Dependent[] locations = {
      new Dependent(10, "San Francisco"),
      new Dependent(20, "San Diego"),
    };

    public QueryableTable foo(int count) {
      return Smalls.generateStrings(count);
    }

    public TranslatableTable view(String s) {
      return Smalls.view(s);
    }
  }

  public static class Employee {
    public final int empid;
    public final int deptno;
    public final String name;
    public final float salary;
    public final Integer commission;

    public Employee(int empid, int deptno, String name, float salary,
        Integer commission) {
      this.empid = empid;
      this.deptno = deptno;
      this.name = name;
      this.salary = salary;
      this.commission = commission;
    }

    @Override public String toString() {
      return "Employee [empid: " + empid + ", deptno: " + deptno
          + ", name: " + name + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Employee
          && empid == ((Employee) obj).empid;
    }
  }

  public static class Department {
    public final int deptno;
    public final String name;

    @org.apache.calcite.adapter.java.Array(component = Employee.class)
    public final List<Employee> employees;
    public final Location location;

    public Department(int deptno, String name, List<Employee> employees,
        Location location) {
      this.deptno = deptno;
      this.name = name;
      this.employees = employees;
      this.location = location;
    }

    @Override public String toString() {
      return "Department [deptno: " + deptno + ", name: " + name
          + ", employees: " + employees + ", location: " + location + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Department
          && deptno == ((Department) obj).deptno;
    }
  }

  public static class Location {
    public final int x;
    public final int y;

    public Location(int x, int y) {
      this.x = x;
      this.y = y;
    }

    @Override public String toString() {
      return "Location [x: " + x + ", y: " + y + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Location
          && x == ((Location) obj).x
          && y == ((Location) obj).y;
    }
  }

  public static class Dependent {
    public final int empid;
    public final String name;

    public Dependent(int empid, String name) {
      this.empid = empid;
      this.name = name;
    }

    @Override public String toString() {
      return "Dependent [empid: " + empid + ", name: " + name + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Dependent
          && empid == ((Dependent) obj).empid
          && Objects.equals(name, ((Dependent) obj).name);
    }
  }

  public static class Event {
    public final int eventid;
    public final Timestamp ts;

    public Event(int eventid, Timestamp ts) {
      this.eventid = eventid;
      this.ts = ts;
    }

    @Override public String toString() {
      return "Event [eventid: " + eventid + ", ts: " + ts + "]";
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Event
          && eventid == ((Event) obj).eventid;
    }
  }

  public static class FoodmartSchema {
    public final SalesFact[] sales_fact_1997 = {
      new SalesFact(100, 10),
      new SalesFact(150, 20),
    };
  }

  public static class LingualSchema {
    public final LingualEmp[] EMPS = {
      new LingualEmp(1, 10),
      new LingualEmp(2, 30)
    };
  }

  public static class LingualEmp {
    public final int EMPNO;
    public final int DEPTNO;

    public LingualEmp(int EMPNO, int DEPTNO) {
      this.EMPNO = EMPNO;
      this.DEPTNO = DEPTNO;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof LingualEmp
          && EMPNO == ((LingualEmp) obj).EMPNO;
    }
  }

  public static class FoodmartJdbcSchema extends JdbcSchema {
    public FoodmartJdbcSchema(DataSource dataSource, SqlDialect dialect,
        JdbcConvention convention, String catalog, String schema) {
      super(dataSource, dialect, convention, catalog, schema);
    }

    public final Table customer = getTable("customer");
  }

  public static class Customer {
    public final int customer_id;

    public Customer(int customer_id) {
      this.customer_id = customer_id;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof Customer
          && customer_id == ((Customer) obj).customer_id;
    }
  }

  public static class SalesFact {
    public final int cust_id;
    public final int prod_id;

    public SalesFact(int cust_id, int prod_id) {
      this.cust_id = cust_id;
      this.prod_id = prod_id;
    }

    @Override public boolean equals(Object obj) {
      return obj == this
          || obj instanceof SalesFact
          && cust_id == ((SalesFact) obj).cust_id
          && prod_id == ((SalesFact) obj).prod_id;
    }
  }

  //CHECKSTYLE: ON

  /** Abstract base class for implementations of {@link ModifiableTable}. */
  public abstract static class AbstractModifiableTable
      extends AbstractTable implements ModifiableTable {
    protected AbstractModifiableTable(String tableName) {
      super();
    }

    public TableModify toModificationRel(
        RelOptCluster cluster,
        RelOptTable table,
        Prepare.CatalogReader catalogReader,
        RelNode child,
        TableModify.Operation operation,
        List<String> updateColumnList,
        List<RexNode> sourceExpressionList,
        boolean flattened) {
      return LogicalTableModify.create(table, catalogReader, child, operation,
          updateColumnList, sourceExpressionList, flattened);
    }
  }

  /** Abstract base class for implementations of {@link ModifiableView}. */
  public abstract static class AbstractModifiableView
      extends AbstractTable implements ModifiableView {
    protected AbstractModifiableView() {
      super();
    }
  }

  /** Factory for EMP and DEPT tables. */
  public static class EmpDeptTableFactory implements TableFactory<Table> {
    public static final TryThreadLocal<List<Employee>> THREAD_COLLECTION =
        TryThreadLocal.of(null);

    public Table create(
        SchemaPlus schema,
        String name,
        Map<String, Object> operand,
        RelDataType rowType) {
      final Class clazz;
      final Object[] array;
      switch (name) {
      case "EMPLOYEES":
        clazz = Employee.class;
        array = new HrSchema().emps;
        break;
      case "MUTABLE_EMPLOYEES":
        List<Employee> employees = THREAD_COLLECTION.get();
        if (employees == null) {
          employees = Collections.emptyList();
        }
        return JdbcFrontLinqBackTest.mutable(name, employees);
      case "DEPARTMENTS":
        clazz = Department.class;
        array = new HrSchema().depts;
        break;
      default:
        throw new AssertionError(name);
      }
      return new AbstractQueryableTable(clazz) {
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
          return ((JavaTypeFactory) typeFactory).createType(clazz);
        }

        public <T> Queryable<T> asQueryable(QueryProvider queryProvider,
            SchemaPlus schema, String tableName) {
          return new AbstractTableQueryable<T>(queryProvider, schema, this,
              tableName) {
            public Enumerator<T> enumerator() {
              @SuppressWarnings("unchecked") final List<T> list =
                  (List) Arrays.asList(array);
              return Linq4j.enumerator(list);
            }
          };
        }
      };
    }
  }

  /** Schema factory that creates {@link MySchema} objects. */
  public static class MySchemaFactory implements SchemaFactory {
    public Schema create(
        SchemaPlus parentSchema,
        String name,
        final Map<String, Object> operand) {
      final boolean mutable =
          SqlFunctions.isNotFalse((Boolean) operand.get("mutable"));
      return new ReflectiveSchema(new HrSchema()) {
        @Override protected Map<String, Table> getTableMap() {
          // Mine the EMPS table and add it under another name e.g. ELVIS
          final Map<String, Table> tableMap = super.getTableMap();
          final Table table = tableMap.get("emps");
          final String tableName = (String) operand.get("tableName");
          return FlatLists.append(tableMap, tableName, table);
        }

        @Override public boolean isMutable() {
          return mutable;
        }
      };
    }
  }

  /** Mock driver that has a handler that stores the results of each query in
   * a temporary table. */
  public static class AutoTempDriver
      extends org.apache.calcite.jdbc.Driver {
    private final List<Object> results;

    AutoTempDriver(List<Object> results) {
      super();
      this.results = results;
    }

    @Override protected Handler createHandler() {
      return new HandlerImpl() {
        @Override public void onStatementExecute(
            AvaticaStatement statement,
            ResultSink resultSink) {
          super.onStatementExecute(statement, resultSink);
          results.add(resultSink);
        }
      };
    }
  }

  /** Mock driver that a given {@link Handler}. */
  public static class HandlerDriver extends org.apache.calcite.jdbc.Driver {
    private static final TryThreadLocal<Handler> HANDLERS =
        TryThreadLocal.of(null);

    public HandlerDriver() {
    }

    @Override protected Handler createHandler() {
      return HANDLERS.get();
    }
  }

  /** Mock driver that can execute a trivial DDL statement. */
  public static class MockDdlDriver extends org.apache.calcite.jdbc.Driver {
    public int counter;

    public MockDdlDriver() {
    }

    @Override protected Function0<CalcitePrepare> createPrepareFactory() {
      return new Function0<CalcitePrepare>() {
        @Override public CalcitePrepare apply() {
          return new CalcitePrepareImpl() {
            @Override protected SqlParser.ConfigBuilder createParserConfig() {
              return super.createParserConfig().setParserFactory(stream ->
                  new SqlParserImpl(stream) {
                    @Override public SqlNode parseSqlStmtEof() {
                      return new SqlCall(SqlParserPos.ZERO) {
                        @Override public SqlOperator getOperator() {
                          return new SqlSpecialOperator("COMMIT",
                              SqlKind.COMMIT);
                        }

                        @Override public List<SqlNode> getOperandList() {
                          return ImmutableList.of();
                        }
                      };
                    }
                  });
            }

            @Override public void executeDdl(Context context, SqlNode node) {
              ++counter;
            }
          };
        }
      };
    }
  }

  /** Dummy table. */
  public static class MyTable {
    public String mykey = "foo";
    public Integer myvalue = 1;
  }

  /** Another dummy table. */
  public static class MyTable2 {
    public String mykey = "foo";
    public Integer myvalue = 2;
  }

  /** Schema containing dummy tables. */
  public static class MySchema {
    public MyTable[] mytable = { new MyTable() };
    public MyTable2[] mytable2 = { new MyTable2() };
  }

}

// End JdbcTest.java
