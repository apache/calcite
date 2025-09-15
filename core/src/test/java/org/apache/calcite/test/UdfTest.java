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

import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.SemiStrict;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.test.schemata.hr.HrSchema;
import org.apache.calcite.util.Smalls;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests for user-defined functions;
 * includes user-defined aggregate functions
 * but user-defined table functions are in {@link TableFunctionTest}.
 *
 * @see Smalls
 */
class UdfTest {
  private CalciteAssert.AssertThat withUdf() {
    final String model = "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '"
        + JdbcTest.EmpDeptTableFactory.class.getName()
        + "',\n"
        + "           operand: {'foo': true, 'bar': 345}\n"
        + "         }\n"
        + "       ],\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'MY_PLUS',\n"
        + "           className: '"
        + Smalls.MyPlusFunction.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_DET_PLUS',\n"
        + "           className: '"
        + Smalls.MyDeterministicPlusFunction.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_LEFT',\n"
        + "           className: '"
        + Smalls.MyLeftFunction.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'ABCDE',\n"
        + "           className: '"
        + Smalls.MyAbcdeFunction.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_STR',\n"
        + "           className: '"
        + Smalls.MyToStringFunction.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_DOUBLE',\n"
        + "           className: '"
        + Smalls.MyDoubleFunction.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_EXCEPTION',\n"
        + "           className: '"
        + Smalls.MyExceptionFunction.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_NILADIC_PARENTHESES',\n"
        + "           className: '"
        + Smalls.MyNiladicParenthesesFunction.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'COUNT_ARGS',\n"
        + "           className: '"
        + Smalls.CountArgs0Function.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'COUNT_ARGS',\n"
        + "           className: '"
        + Smalls.CountArgs1Function.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'COUNT_ARGS',\n"
        + "           className: '"
        + Smalls.CountArgs1NullableFunction.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'COUNT_ARGS',\n"
        + "           className: '"
        + Smalls.CountArgs2Function.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_ABS',\n"
        + "           className: '"
        + java.lang.Math.class.getName()
        + "',\n"
        + "           methodName: 'abs'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'NULL4',\n"
        + "           className: '"
        + Smalls.Null4Function.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'NULL8',\n"
        + "           className: '"
        + Smalls.Null8Function.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           className: '"
        + Smalls.MultipleFunction.class.getName()
        + "',\n"
        + "           methodName: '*'\n"
        + "         },\n"
        + "         {\n"
        + "           className: '"
        + Smalls.AllTypesFunction.class.getName()
        + "',\n"
        + "           methodName: '*'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'UNBASE64',\n"
        + "           className: '"
        + Smalls.MyUnbase64Function.class.getName()
        + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'CHARACTERARRAY',\n"
        + "           className: '"
        + Smalls.CharacterArrayFunction.class.getName()
        + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}";
    return CalciteAssert.model(model);
  }

  /** Tests a user-defined function that is defined in terms of a class with
   * non-static methods. */
  @Disabled("[CALCITE-1561] Intermittent test failures")
  @Test void testUserDefinedFunction() {
    final String sql = "select \"adhoc\".my_plus(\"deptno\", 100) as p\n"
        + "from \"adhoc\".EMPLOYEES";
    final AtomicInteger c = Smalls.MyPlusFunction.INSTANCE_COUNT.get();
    final int before = c.get();
    withUdf().query(sql).returnsUnordered("P=110",
        "P=120",
        "P=110",
        "P=110");
    final int after = c.get();
    assertThat(after, is(before + 4));
  }

  /** As {@link #testUserDefinedFunction()}, but checks that the class is
   * instantiated exactly once, per
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1548">[CALCITE-1548]
   * Instantiate function objects once per query</a>. */
  @Test void testUserDefinedFunctionInstanceCount() {
    final String sql = "select \"adhoc\".my_det_plus(\"deptno\", 100) as p\n"
        + "from \"adhoc\".EMPLOYEES";
    final AtomicInteger c = Smalls.MyDeterministicPlusFunction.INSTANCE_COUNT.get();
    final int before = c.get();
    withUdf().query(sql).returnsUnordered("P=110",
        "P=120",
        "P=110",
        "P=110");
    final int after = c.get();
    assertThat(after, is(before + 1));
  }

  @Test void testUserDefinedFunctionB() {
    final String sql = "select \"adhoc\".my_double(\"deptno\") as p\n"
        + "from \"adhoc\".EMPLOYEES";
    final String expected = "P=20\n"
        + "P=40\n"
        + "P=20\n"
        + "P=20\n";
    withUdf().query(sql).returns(expected);
  }

  @Test void testUserDefinedFunctionWithNull() {
    final String sql = "select \"adhoc\".my_det_plus(\"deptno\", 1 + null) as p\n"
        + "from \"adhoc\".EMPLOYEES where 1 > 0 or nullif(null, 1) is null";
    final AtomicInteger c = Smalls.MyDeterministicPlusFunction.INSTANCE_COUNT.get();
    final int before = c.get();
    withUdf()
        .query(sql)
        .returnsUnordered("P=null",
            "P=null",
            "P=null",
            "P=null");
    final int after = c.get();
    assertThat(after, is(before + 1));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3195">[CALCITE-3195]
   * Handle a UDF that throws checked exceptions in the Enumerable code generator</a>. */
  @Test void testUserDefinedFunctionWithException() {
    final String sql1 = "select \"adhoc\".my_exception(\"deptno\") as p\n"
        + "from \"adhoc\".EMPLOYEES";
    final String expected1 = "P=20\n"
        + "P=30\n"
        + "P=20\n"
        + "P=20\n";
    withUdf().query(sql1).returns(expected1);

    final String sql2 = "select cast(\"adhoc\".my_exception(\"deptno\") as double) as p\n"
        + "from \"adhoc\".EMPLOYEES";
    final String expected2 = "P=20.0\n"
        + "P=30.0\n"
        + "P=20.0\n"
        + "P=20.0\n";
    withUdf().query(sql2).returns(expected2);

    final String sql3 = "select \"adhoc\".my_exception(\"deptno\" * 2 + 11) as p\n"
        + "from \"adhoc\".EMPLOYEES";
    final String expected3 = "P=41\n"
        + "P=61\n"
        + "P=41\n"
        + "P=41\n";
    withUdf().query(sql3).returns(expected3);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-937">[CALCITE-937]
   * User-defined function within view</a>. */
  @Test void testUserDefinedFunctionInView() throws Exception {
    Class.forName("org.apache.calcite.jdbc.Driver");
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));

    SchemaPlus post = rootSchema.add("POST", new AbstractSchema());
    post.add("MY_INCREMENT",
        ScalarFunctionImpl.create(Smalls.MyIncrement.class, "eval"));

    final String viewSql = "select \"empid\" as EMPLOYEE_ID,\n"
        + "  \"name\" || ' ' || \"name\" as EMPLOYEE_NAME,\n"
        + "  \"salary\" as EMPLOYEE_SALARY,\n"
        + "  POST.MY_INCREMENT(\"empid\", 10) as INCREMENTED_SALARY\n"
        + "from \"hr\".\"emps\"";
    post.add("V_EMP",
        ViewTable.viewMacro(post, viewSql, ImmutableList.of(),
            ImmutableList.of("POST", "V_EMP"), null));

    final String result = ""
        + "EMPLOYEE_ID=100; EMPLOYEE_NAME=Bill Bill;"
        + " EMPLOYEE_SALARY=10000.0; INCREMENTED_SALARY=110.0\n"
        + "EMPLOYEE_ID=200; EMPLOYEE_NAME=Eric Eric;"
        + " EMPLOYEE_SALARY=8000.0; INCREMENTED_SALARY=220.0\n"
        + "EMPLOYEE_ID=150; EMPLOYEE_NAME=Sebastian Sebastian;"
        + " EMPLOYEE_SALARY=7000.0; INCREMENTED_SALARY=165.0\n"
        + "EMPLOYEE_ID=110; EMPLOYEE_NAME=Theodore Theodore;"
        + " EMPLOYEE_SALARY=11500.0; INCREMENTED_SALARY=121.0\n";

    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(viewSql);
    assertThat(CalciteAssert.toString(resultSet), is(result));
    resultSet.close();

    ResultSet viewResultSet =
        statement.executeQuery("select * from \"POST\".\"V_EMP\"");
    assertThat(CalciteAssert.toString(viewResultSet), is(result));
    statement.close();
    connection.close();
  }

  /**
   * Tests that IS NULL/IS NOT NULL is properly implemented for non-strict
   * functions.
   */
  @Test void testNotNullImplementor() {
    final CalciteAssert.AssertThat with = withUdf();
    with.query(
        "select upper(\"adhoc\".my_str(\"name\")) as p from \"adhoc\".EMPLOYEES")
        .returns("P=<BILL>\n"
            + "P=<ERIC>\n"
            + "P=<SEBASTIAN>\n"
            + "P=<THEODORE>\n");
    with.query("select \"name\" as p from \"adhoc\".EMPLOYEES\n"
        + "where \"adhoc\".my_str(\"name\") is not null")
        .returns("P=Bill\n"
            + "P=Eric\n"
            + "P=Sebastian\n"
            + "P=Theodore\n");
    with.query("select \"name\" as p from \"adhoc\".EMPLOYEES\n"
        + "where \"adhoc\".my_str(upper(\"name\")) is not null")
        .returns("P=Bill\n"
            + "P=Eric\n"
            + "P=Sebastian\n"
            + "P=Theodore\n");
    with.query("select \"name\" as p from \"adhoc\".EMPLOYEES\n"
        + "where upper(\"adhoc\".my_str(\"name\")) is not null")
        .returns("P=Bill\n"
            + "P=Eric\n"
            + "P=Sebastian\n"
            + "P=Theodore\n");
    with.query("select \"name\" as p from \"adhoc\".EMPLOYEES\n"
        + "where \"adhoc\".my_str(\"name\") is null")
        .returns("");
    with.query("select \"name\" as p from \"adhoc\".EMPLOYEES\n"
        + "where \"adhoc\".my_str(upper(\"adhoc\".my_str(\"name\")"
        + ")) ='8'")
        .returns("");
  }

  /** Tests that we generate the appropriate checks for a "semi-strict"
   * function.
   *
   * <p>The difference between "strict" and "semi-strict" functions is that a
   * "semi-strict" function might return null even if none of its arguments
   * are null. (Both always return null if one of their arguments is null.)
   * Thus, a nasty function is more unpredictable.
   *
   * @see SemiStrict */
  @Test void testSemiStrict() {
    final CalciteAssert.AssertThat with = withUdf();
    final String sql = "select\n"
        + "  \"adhoc\".null4(upper(\"name\")) as p\n"
        + " from \"adhoc\".EMPLOYEES";
    with.query(sql)
        .returnsUnordered("P=null",
            "P=null",
            "P=SEBASTIAN",
            "P=THEODORE");
    // my_str is non-strict; it must be called when args are null
    final String sql2 = "select\n"
        + "  \"adhoc\".my_str(upper(\"adhoc\".null4(\"name\"))) as p\n"
        + " from \"adhoc\".EMPLOYEES";
    with.query(sql2)
        .returnsUnordered("P=<null>",
            "P=<null>",
            "P=<SEBASTIAN>",
            "P=<THEODORE>");
    // null8 throws NPE if its argument is null,
    // so we had better know that null4 might return null
    final String sql3 = "select\n"
        + "  \"adhoc\".null8(\"adhoc\".null4(\"name\")) as p\n"
        + " from \"adhoc\".EMPLOYEES";
    with.query(sql3)
        .returnsUnordered("P=null",
            "P=null",
            "P=Sebastian",
            "P=null");
  }

  /** Tests derived return type of user-defined function. */
  @Test void testUdfDerivedReturnType() {
    final CalciteAssert.AssertThat with = withUdf();
    with.query(
        "select max(\"adhoc\".my_double(\"deptno\")) as p from \"adhoc\".EMPLOYEES")
        .returns("P=40\n");
    with.query("select max(\"adhoc\".my_str(\"name\")) as p\n"
        + "from \"adhoc\".EMPLOYEES\n"
        + "where \"adhoc\".my_str(\"name\") is null")
        .returns("P=null\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6645">[CALCITE-6645]
   * User-defined function with niladic parentheses</a>. */
  @Test void testUserDefinedFunctionWithNiladicParentheses() {
    final CalciteAssert.AssertThat with = withUdf();
    with.with(SqlConformanceEnum.MYSQL_5)
        .query("select \"adhoc\".my_niladic_parentheses() as p\n"
            + "from \"adhoc\".EMPLOYEES limit 1")
        .returns("P=foo\n");
    with.with(SqlConformanceEnum.ORACLE_10)
        .query("select \"adhoc\".my_niladic_parentheses as p\n"
            + "from \"adhoc\".EMPLOYEES limit 1")
        .returns("P=foo\n");
    with.with(SqlConformanceEnum.DEFAULT)
        .query("select \"adhoc\".my_niladic_parentheses as p\n"
            + "from \"adhoc\".EMPLOYEES limit 1")
        .returns("P=foo\n");
    with.with(SqlConformanceEnum.DEFAULT)
        .query("select \"adhoc\".my_niladic_parentheses() as p\n"
            + "from \"adhoc\".EMPLOYEES limit 1")
        .returns("P=foo\n");
    // wrong niladic function with mysql_5 conformance
    with.with(SqlConformanceEnum.MYSQL_5)
        .query("select \"adhoc\".my_niladic_parentheses as p\n"
            + "from \"adhoc\".EMPLOYEES limit 1")
        .throws_("Table 'adhoc' not found");
    // wrong niladic function with oracle_10 conformance
    with.with(SqlConformanceEnum.ORACLE_10)
        .query("select \"adhoc\".my_niladic_parentheses() as p\n"
            + "from \"adhoc\".EMPLOYEES limit 1")
        .throws_("No match found for function signature MY_NILADIC_PARENTHESES()");
  }

  /** Tests a user-defined function that has multiple overloads. */
  @Test void testUdfOverloaded() {
    final CalciteAssert.AssertThat with = withUdf();
    // MYSQL_5 conformance support niladic function with parentheses
    with.with(SqlConformanceEnum.MYSQL_5)
        .query("values (\"adhoc\".count_args(),\n"
        + " \"adhoc\".count_args(0),\n"
        + " \"adhoc\".count_args(0, 0))")
        .returns("EXPR$0=0; EXPR$1=1; EXPR$2=2\n");
    // MYSQL_5 conformance support niladic function with parentheses
    with.with(SqlConformanceEnum.MYSQL_5)
        .query("select max(\"adhoc\".count_args()) as p0,\n"
        + " min(\"adhoc\".count_args(0)) as p1,\n"
        + " max(\"adhoc\".count_args(0, 0)) as p2\n"
        + "from \"adhoc\".EMPLOYEES limit 1")
        .returns("P0=0; P1=1; P2=2\n");
  }

  @Test void testUdfOverloadedNullable() {
    final CalciteAssert.AssertThat with = withUdf();
    // MYSQL_5 conformance support niladic function with parentheses
    with.with(SqlConformanceEnum.MYSQL_5)
        .query("values (\"adhoc\".count_args(),\n"
        + " \"adhoc\".count_args(cast(null as smallint)),\n"
        + " \"adhoc\".count_args(0, 0))")
        .returns("EXPR$0=0; EXPR$1=-1; EXPR$2=2\n");
  }

  /** Tests passing parameters to user-defined function by name. */
  @Test void testUdfArgumentName() {
    final CalciteAssert.AssertThat with = withUdf();
    // arguments in physical order
    with.query("values (\"adhoc\".my_left(\"s\" => 'hello', \"n\" => 3))")
        .returns("EXPR$0=hel\n");
    // arguments in reverse order
    with.query("values (\"adhoc\".my_left(\"n\" => 3, \"s\" => 'hello'))")
        .returns("EXPR$0=hel\n");
    with.query("values (\"adhoc\".my_left(\"n\" => 1 + 2, \"s\" => 'hello'))")
        .returns("EXPR$0=hel\n");
    // duplicate argument names
    with.query("values (\"adhoc\".my_left(\"n\" => 3, \"n\" => 2, \"s\" => 'hello'))")
        .throws_("Duplicate argument name 'n'");
    // invalid argument names
    with.query("values (\"adhoc\".my_left(\"n\" => 3, \"m\" => 2, \"s\" => 'h'))")
        .throws_("No match found for function signature "
            + "MY_LEFT(n => <NUMERIC>, m => <NUMERIC>, s => <CHARACTER>)");
    // missing arguments
    with.query("values (\"adhoc\".my_left(\"n\" => 3))")
        .throws_("No match found for function signature MY_LEFT(n => <NUMERIC>)");
    with.query("values (\"adhoc\".my_left(\"s\" => 'hello'))")
        .throws_("No match found for function signature MY_LEFT(s => <CHARACTER>)");
    // arguments of wrong type, will do implicitly type coercion.
    with.query("values (\"adhoc\".my_left(\"n\" => 'hello', \"s\" => 'x'))")
        .throws_("java.lang.NumberFormatException: For input string: \"hello\"");
    with.query("values (\"adhoc\".my_left(\"n\" => '1', \"s\" => 'x'))")
        .returns("EXPR$0=x\n");
    with.query("values (\"adhoc\".my_left(\"n\" => 1, \"s\" => 0))")
        .returns("EXPR$0=0\n");
  }

  /** Tests calling a user-defined function some of whose parameters are
   * optional. */
  @Test void testUdfArgumentOptional() {
    final CalciteAssert.AssertThat with = withUdf();
    with.query("values (\"adhoc\".abcde(a=>1,b=>2,c=>3,d=>4,e=>5))")
        .returns("EXPR$0={a: 1, b: 2, c: 3, d: 4, e: 5}\n");
    with.query("values (\"adhoc\".abcde(1,2,3,4,CAST(NULL AS INTEGER)))")
        .returns("EXPR$0={a: 1, b: 2, c: 3, d: 4, e: null}\n");
    with.query("values (\"adhoc\".abcde(a=>1,b=>2,c=>3,d=>4))")
        .returns("EXPR$0={a: 1, b: 2, c: 3, d: 4, e: null}\n");
    with.query("values (\"adhoc\".abcde(a=>1,b=>2,c=>3))")
        .returns("EXPR$0={a: 1, b: 2, c: 3, d: null, e: null}\n");
    with.query("values (\"adhoc\".abcde(a=>1,e=>5,c=>3))")
        .returns("EXPR$0={a: 1, b: null, c: 3, d: null, e: 5}\n");
    with.query("values (\"adhoc\".abcde(1,2,3))")
        .returns("EXPR$0={a: 1, b: 2, c: 3, d: null, e: null}\n");
    with.query("values (\"adhoc\".abcde(1,2,3,4))")
        .returns("EXPR$0={a: 1, b: 2, c: 3, d: 4, e: null}\n");
    with.query("values (\"adhoc\".abcde(1,2,3,4,5))")
        .returns("EXPR$0={a: 1, b: 2, c: 3, d: 4, e: 5}\n");
    with.query("values (\"adhoc\".abcde(1,2))")
        .throws_("No match found for function signature ABCDE(<NUMERIC>, <NUMERIC>)");
    with.query("values (\"adhoc\".abcde(1,DEFAULT,3))")
        .returns("EXPR$0={a: 1, b: null, c: 3, d: null, e: null}\n");
    // implicit type coercion.
    with.query("values (\"adhoc\".abcde(1,DEFAULT,'abcde'))")
        .throws_("java.lang.NumberFormatException: For input string: \"abcde\"");
    with.query("values (\"adhoc\".abcde(1,DEFAULT,'123'))")
        .returns("EXPR$0={a: 1, b: null, c: 123, d: null, e: null}\n");
    with.query("values (\"adhoc\".abcde(true))")
        .throws_("No match found for function signature ABCDE(<BOOLEAN>)");
    with.query("values (\"adhoc\".abcde(true,DEFAULT))")
        .throws_("No match found for function signature ABCDE(<BOOLEAN>, <ANY>)");
    with.query("values (\"adhoc\".abcde(1,DEFAULT,3,DEFAULT))")
        .returns("EXPR$0={a: 1, b: null, c: 3, d: null, e: null}\n");
    with.query("values (\"adhoc\".abcde(1,2,DEFAULT))")
        .throws_("DEFAULT is only allowed for optional parameters");
    with.query("values (\"adhoc\".abcde(a=>1,b=>2,c=>DEFAULT))")
        .throws_("DEFAULT is only allowed for optional parameters");
    with.query("values (\"adhoc\".abcde(a=>1,b=>DEFAULT,c=>3))")
        .returns("EXPR$0={a: 1, b: null, c: 3, d: null, e: null}\n");
  }

  /** Test for
   * {@link org.apache.calcite.runtime.CalciteResource#requireDefaultConstructor(String)}. */
  @Test void testUserDefinedFunction2() {
    String message = "Declaring class "
        + "'org.apache.calcite.util.Smalls$AwkwardFunction' of non-static "
        + "user-defined function must have a public constructor with zero "
        + "parameters";
    withBadUdf(Smalls.AwkwardFunction.class).connectThrows(message);
  }

  /** Tests user-defined function, with multiple methods per class. */
  @Test void testUserDefinedFunctionWithMethodName() {
    // java.lang.Math has abs(int) and abs(double).
    final CalciteAssert.AssertThat with = withUdf();
    with.query("values abs(-4)").returnsValue("4");
    with.query("values abs(-4.5)").returnsValue("4.5");

    // 3 overloads of "fun1", another method "fun2", but method "nonStatic"
    // cannot be used as a function
    with.query("values \"adhoc\".\"fun1\"(2)").returnsValue("4");
    with.query("values \"adhoc\".\"fun1\"(2, 3)").returnsValue("5");
    with.query("values \"adhoc\".\"fun1\"('Foo Bar')").returnsValue("foo bar");
    with.query("values \"adhoc\".\"fun2\"(10)").returnsValue("30");
    with.query("values \"adhoc\".\"nonStatic\"(2)")
        .throws_("No match found for function signature nonStatic(<NUMERIC>)");
  }

  /** Tests user-defined aggregate function. */
  @Test void testUserDefinedAggregateFunction() {
    final String empDept = JdbcTest.EmpDeptTableFactory.class.getName();
    final String sum = Smalls.MyStaticSumFunction.class.getName();
    final String sum2 = Smalls.MySumFunction.class.getName();
    final CalciteAssert.AssertThat with = CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '" + empDept + "',\n"
        + "           operand: {'foo': true, 'bar': 345}\n"
        + "         }\n"
        + "       ],\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'MY_SUM',\n"
        + "           className: '" + sum + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_SUM2',\n"
        + "           className: '" + sum2 + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}")
        .withDefaultSchema("adhoc");
    with.withDefaultSchema(null)
        .query(
            "select \"adhoc\".my_sum(\"deptno\") as p from \"adhoc\".EMPLOYEES\n")
        .returns("P=50\n");
    with.query("select my_sum(\"empid\"), \"deptno\" as p from EMPLOYEES\n")
        .throws_(
            "Expression 'deptno' is not being grouped");
    with.query("select my_sum(\"deptno\") as p from EMPLOYEES\n")
        .returns("P=50\n");
    // implicit type coercion.
    with.query("select my_sum(\"name\") as p from EMPLOYEES\n")
        .throws_("java.lang.NumberFormatException: For input string: \"Bill\"");
    with.query("select my_sum(\"deptno\", 1) as p from EMPLOYEES\n")
        .throws_(
            "No match found for function signature MY_SUM(<NUMERIC>, <NUMERIC>)");
    with.query("select my_sum() as p from EMPLOYEES\n")
        .throws_(
            "No match found for function signature MY_SUM()");
    with.query("select \"deptno\", my_sum(\"deptno\") as p from EMPLOYEES\n"
        + "group by \"deptno\"")
        .returnsUnordered(
            "deptno=20; P=20",
            "deptno=10; P=30");
    with.query("select \"deptno\", my_sum2(\"deptno\") as p from EMPLOYEES\n"
        + "group by \"deptno\"")
        .returnsUnordered("deptno=20; P=20", "deptno=10; P=30");
  }

  /** Tests user-defined aggregate function. */
  @Test void testUserDefinedAggregateFunctionWithMultipleParameters() {
    final String empDept = JdbcTest.EmpDeptTableFactory.class.getName();
    final String sum21 = Smalls.MyTwoParamsSumFunctionFilter1.class.getName();
    final String sum22 = Smalls.MyTwoParamsSumFunctionFilter2.class.getName();
    final String sum31 = Smalls.MyThreeParamsSumFunctionWithFilter1.class.getName();
    final String sum32 = Smalls.MyThreeParamsSumFunctionWithFilter2.class.getName();
    final CalciteAssert.AssertThat with = CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '" + empDept + "',\n"
        + "           operand: {'foo': true, 'bar': 345}\n"
        + "         }\n"
        + "       ],\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'MY_SUM2',\n"
        + "           className: '" + sum21 + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_SUM2',\n"
        + "           className: '" + sum22 + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_SUM3',\n"
        + "           className: '" + sum31 + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_SUM3',\n"
        + "           className: '" + sum32 + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}")
        .withDefaultSchema("adhoc");
    with.withDefaultSchema(null)
        .query("select \"adhoc\".my_sum3(\"deptno\",\"name\",'Eric') as p\n"
            + "from \"adhoc\".EMPLOYEES\n")
        .returns("P=20\n");
    with.query("select \"adhoc\".my_sum3(\"empid\",\"deptno\",\"commission\") as p "
        + "from \"adhoc\".EMPLOYEES\n")
        .returns("P=330\n");
    with.query("select \"adhoc\".my_sum3(\"empid\",\"deptno\",\"commission\"),\n"
        + "  \"name\"\n"
        + "from \"adhoc\".EMPLOYEES\n")
        .throws_("Expression 'name' is not being grouped");
    with.query("select \"name\",\n"
        + "  \"adhoc\".my_sum3(\"empid\",\"deptno\",\"commission\") as p\n"
        + "from \"adhoc\".EMPLOYEES\n"
        + "group by \"name\"")
        .returnsUnordered("name=Theodore; P=0",
            "name=Eric; P=220",
            "name=Bill; P=110",
            "name=Sebastian; P=0");
    // implicit type coercion.
    with.query("select \"adhoc\".my_sum3(\"empid\",\"deptno\",\"salary\") as p\n"
        + "from \"adhoc\".EMPLOYEES\n");
    with.query("select \"adhoc\".my_sum3(\"empid\",\"deptno\",\"name\") as p\n"
        + "from \"adhoc\".EMPLOYEES\n");
    with.query("select \"adhoc\".my_sum2(\"commission\",250) as p\n"
        + "from \"adhoc\".EMPLOYEES\n")
        .returns("P=1500\n");
    // implicit type coercion.
    with.query("select \"adhoc\".my_sum2(\"name\",250) as p\n"
        + "from \"adhoc\".EMPLOYEES\n")
        .throws_("java.lang.NumberFormatException: For input string: \"Bill\"");
    // implicit type coercion.
    with.query("select \"adhoc\".my_sum2(\"empid\",0.0) as p\n"
        + "from \"adhoc\".EMPLOYEES\n")
        .returns("P=560\n");
  }

  /** Test for
   * {@link org.apache.calcite.runtime.CalciteResource#firstParameterOfAdd(String)}. */
  @Test void testUserDefinedAggregateFunction3() {
    String message = "Caused by: java.lang.RuntimeException: In user-defined "
        + "aggregate class 'org.apache.calcite.util.Smalls$SumFunctionBadIAdd'"
        + ", first parameter to 'add' method must be the accumulator (the "
        + "return type of the 'init' method)";
    withBadUdf(Smalls.SumFunctionBadIAdd.class).connectThrows(message);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1434">[CALCITE-1434]
   * AggregateFunctionImpl doesn't work if the class implements a generic
   * interface</a>. */
  @Test void testUserDefinedAggregateFunctionImplementsInterface() {
    final String empDept = JdbcTest.EmpDeptTableFactory.class.getName();
    final String mySum3 = Smalls.MySum3.class.getName();
    final String model = "{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '" + empDept + "',\n"
        + "           operand: {'foo': true, 'bar': 345}\n"
        + "         }\n"
        + "       ],\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'MY_SUM3',\n"
        + "           className: '" + mySum3 + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}";
    final CalciteAssert.AssertThat with = CalciteAssert.model(model)
        .withDefaultSchema("adhoc");

    with.query("select my_sum3(\"deptno\") as p from EMPLOYEES\n")
        .returns("P=50\n");
    with.withDefaultSchema(null)
        .query("select \"adhoc\".my_sum3(\"deptno\") as p\n"
            + "from \"adhoc\".EMPLOYEES\n")
        .returns("P=50\n");
    with.query("select my_sum3(\"empid\"), \"deptno\" as p from EMPLOYEES\n")
        .throws_("Expression 'deptno' is not being grouped");
    with.query("select my_sum3(\"deptno\") as p from EMPLOYEES\n")
        .returns("P=50\n");
    // implicit type coercion.
    with.query("select my_sum3(\"name\") as p from EMPLOYEES\n")
        .throws_("java.lang.NumberFormatException: For input string: \"Bill\"");
    with.query("select my_sum3(\"deptno\", 1) as p from EMPLOYEES\n")
        .throws_("No match found for function signature "
            + "MY_SUM3(<NUMERIC>, <NUMERIC>)");
    with.query("select my_sum3() as p from EMPLOYEES\n")
        .throws_("No match found for function signature MY_SUM3()");
    with.query("select \"deptno\", my_sum3(\"deptno\") as p from EMPLOYEES\n"
        + "group by \"deptno\"")
        .returnsUnordered("deptno=20; P=20",
            "deptno=10; P=30");
  }

  private static CalciteAssert.AssertThat withBadUdf(Class<?> clazz) {
    final String empDept = JdbcTest.EmpDeptTableFactory.class.getName();
    final String className = clazz.getName();
    return CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       tables: [\n"
        + "         {\n"
        + "           name: 'EMPLOYEES',\n"
        + "           type: 'custom',\n"
        + "           factory: '" + empDept + "',\n"
        + "           operand: {'foo': true, 'bar': 345}\n"
        + "         }\n"
        + "       ],\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'AWKWARD',\n"
        + "           className: '" + className + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}")
        .withDefaultSchema("adhoc");
  }

  /** Tests user-defined aggregate function with FILTER.
   *
   * <p>Also tests that we do not try to push ADAF to JDBC source. */
  @Test void testUserDefinedAggregateFunctionWithFilter() {
    final String sum = Smalls.MyStaticSumFunction.class.getName();
    final String sum2 = Smalls.MySumFunction.class.getName();
    final CalciteAssert.AssertThat with = CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + JdbcTest.SCOTT_SCHEMA
        + ",\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'MY_SUM',\n"
        + "           className: '" + sum + "'\n"
        + "         },\n"
        + "         {\n"
        + "           name: 'MY_SUM2',\n"
        + "           className: '" + sum2 + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}")
        .withDefaultSchema("adhoc");
    with.query("select deptno, \"adhoc\".my_sum(deptno) as p\n"
        + "from scott.emp\n"
        + "group by deptno\n")
        .returns(
            "DEPTNO=20; P=100\n"
                + "DEPTNO=10; P=30\n"
                + "DEPTNO=30; P=180\n");

    with.query("select deptno,\n"
        + "  \"adhoc\".my_sum(deptno) filter (where job = 'CLERK') as c,\n"
        + "  \"adhoc\".my_sum(deptno) filter (where job = 'XXX') as x\n"
        + "from scott.emp\n"
        + "group by deptno\n")
        .returns(
            "DEPTNO=20; C=40; X=0\n"
                + "DEPTNO=10; C=10; X=0\n"
                + "DEPTNO=30; C=30; X=0\n");
  }

  /** Tests resolution of functions using schema paths. */
  @Test void testPath() {
    final String name = Smalls.MyPlusFunction.class.getName();
    final CalciteAssert.AssertThat with = CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 'adhoc',\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'MY_PLUS',\n"
        + "           className: '" + name + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     },\n"
        + "     {\n"
        + "       name: 'adhoc2',\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'MY_PLUS2',\n"
        + "           className: '" + name + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     },\n"
        + "     {\n"
        + "       name: 'adhoc3',\n"
        + "       path: ['adhoc2','adhoc3'],\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'MY_PLUS3',\n"
        + "           className: '" + name + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}");

    final String err = "No match found for function signature";
    final String res = "EXPR$0=2\n";

    // adhoc can see own function MY_PLUS but not adhoc2.MY_PLUS2 unless
    // qualified
    final CalciteAssert.AssertThat adhoc = with.withDefaultSchema("adhoc");
    adhoc.query("values MY_PLUS(1, 1)").returns(res);
    adhoc.query("values MY_PLUS2(1, 1)").throws_(err);
    adhoc.query("values \"adhoc2\".MY_PLUS(1, 1)").throws_(err);
    adhoc.query("values \"adhoc2\".MY_PLUS2(1, 1)").returns(res);

    // adhoc2 can see own function MY_PLUS2 but not adhoc2.MY_PLUS unless
    // qualified
    final CalciteAssert.AssertThat adhoc2 = with.withDefaultSchema("adhoc2");
    adhoc2.query("values MY_PLUS2(1, 1)").returns(res);
    adhoc2.query("values MY_PLUS(1, 1)").throws_(err);
    adhoc2.query("values \"adhoc\".MY_PLUS(1, 1)").returns(res);

    // adhoc3 can see own adhoc2.MY_PLUS2 because in path, with or without
    // qualification, but can only see adhoc.MY_PLUS with qualification
    final CalciteAssert.AssertThat adhoc3 = with.withDefaultSchema("adhoc3");
    adhoc3.query("values MY_PLUS2(1, 1)").returns(res);
    adhoc3.query("values MY_PLUS(1, 1)").throws_(err);
    adhoc3.query("values \"adhoc\".MY_PLUS(1, 1)").returns(res);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-986">[CALCITE-986]
   * User-defined function with Date or Timestamp parameters</a>. */
  @Test void testDate() {
    final CalciteAssert.AssertThat with = withUdf();
    with.query("values \"adhoc\".\"dateFun\"(DATE '1970-01-01')")
        .returnsValue("0");
    with.query("values \"adhoc\".\"dateFun\"(DATE '1970-01-02')")
        .returnsValue("86400000");
    with.query("values \"adhoc\".\"dateFun\"(cast(null as date))")
        .returnsValue("-1");
    with.query("values \"adhoc\".\"timeFun\"(TIME '00:00:00')")
        .returnsValue("0");
    with.query("values \"adhoc\".\"timeFun\"(TIME '00:01:30')")
        .returnsValue("90000");
    with.query("values \"adhoc\".\"timeFun\"(cast(null as time))")
        .returnsValue("-1");
    with.query("values \"adhoc\".\"timestampFun\"(TIMESTAMP '1970-01-01 00:00:00')")
        .returnsValue("0");
    with.query("values \"adhoc\".\"timestampFun\"(TIMESTAMP '1970-01-02 00:01:30')")
        .returnsValue("86490000");
    with.query("values \"adhoc\".\"timestampFun\"(cast(null as timestamp))")
        .returnsValue("-1");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1041">[CALCITE-1041]
   * User-defined function returns DATE or TIMESTAMP value</a>. */
  @Test void testReturnDate() {
    final CalciteAssert.AssertThat with = withUdf();
    with.query("values \"adhoc\".\"toDateFun\"(0)")
        .returnsValue("1970-01-01");
    with.query("values \"adhoc\".\"toDateFun\"(1)")
        .returnsValue("1970-01-02");
    with.query("values \"adhoc\".\"toDateFun\"(cast(null as bigint))")
        .returnsValue(null);
    with.query("values \"adhoc\".\"toTimeFun\"(0)")
        .returnsValue("00:00:00");
    with.query("values \"adhoc\".\"toTimeFun\"(90000)")
        .returnsValue("00:01:30");
    with.query("values \"adhoc\".\"toTimeFun\"(cast(null as bigint))")
        .returnsValue(null);
    with.query("values \"adhoc\".\"toTimestampFun\"(0)")
        .returnsValue("1970-01-01 00:00:00");
    with.query("values \"adhoc\".\"toTimestampFun\"(86490000)")
        .returnsValue("1970-01-02 00:01:30");
    with.query("values \"adhoc\".\"toTimestampFun\"(cast(null as bigint))")
        .returnsValue(null);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1881">[CALCITE-1881]
   * Can't distinguish overloaded user-defined functions that have DATE and
   * TIMESTAMP arguments</a>. */
  @Test void testDateAndTimestamp() {
    final CalciteAssert.AssertThat with = withUdf();
    with.query("values \"adhoc\".\"toLong\"(DATE '1970-01-15')")
        .returns("EXPR$0=1209600000\n");
    with.query("values \"adhoc\".\"toLong\"(DATE '2002-08-11')")
        .returns("EXPR$0=1029024000000\n");
    with.query("values \"adhoc\".\"toLong\"(DATE '2003-04-11')")
        .returns("EXPR$0=1050019200000\n");
    with.query("values \"adhoc\".\"toLong\"(TIMESTAMP '2003-04-11 00:00:00')")
        .returns("EXPR$0=1050019200000\n");
    with.query("values \"adhoc\".\"toLong\"(TIMESTAMP '2003-04-11 00:00:06')")
        .returns("EXPR$0=1050019206000\n");
    with.query("values \"adhoc\".\"toLong\"(TIMESTAMP '2003-04-18 01:20:00')")
        .returns("EXPR$0=1050628800000\n");
    with.query("values \"adhoc\".\"toLong\"(TIME '00:20:00')")
        .returns("EXPR$0=1200000\n");
    with.query("values \"adhoc\".\"toLong\"(TIME '00:20:10')")
        .returns("EXPR$0=1210000\n");
    with.query("values \"adhoc\".\"toLong\"(TIME '01:20:00')")
        .returns("EXPR$0=4800000\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2053">[CALCITE-2053]
   * Overloaded user-defined functions that have Double and BigDecimal arguments
   * will goes wrong </a>. */
  @Test void testBigDecimalAndLong() {
    final CalciteAssert.AssertThat with = withUdf();
    with.query("values \"adhoc\".\"toDouble\"(cast(1.0 as double))")
            .returns("EXPR$0=1.0\n");
    with.query("values \"adhoc\".\"toDouble\"(cast(1.0 as decimal))")
            .returns("EXPR$0=1.0\n");
    with.query("values \"adhoc\".\"toDouble\"(cast(1 as double))")
            .returns("EXPR$0=1.0\n");
    with.query("values \"adhoc\".\"toDouble\"(cast(1 as decimal))")
            .returns("EXPR$0=1.0\n");
    with.query("values \"adhoc\".\"toDouble\"(cast(1 as float))")
            .returns("EXPR$0=1.0\n");
    with.query("values \"adhoc\".\"toDouble\"(cast(1.0 as float))")
            .returns("EXPR$0=1.0\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1041">[CALCITE-1041]
   * User-defined function returns DATE or TIMESTAMP value</a>. */
  @Test void testReturnDate2() {
    final CalciteAssert.AssertThat with = withUdf();
    with.query("select * from (values 0) as t(c)\n"
        + "where \"adhoc\".\"toTimestampFun\"(c) in (\n"
        + "  cast('1970-01-01 00:00:00' as timestamp),\n"
        + "  cast('1997-02-01 00:00:00' as timestamp))")
        .returnsValue("0");
    with.query("select * from (values 0) as t(c)\n"
        + "where \"adhoc\".\"toTimestampFun\"(c) in (\n"
        + "  timestamp '1970-01-01 00:00:00',\n"
        + "  timestamp '1997-02-01 00:00:00')")
        .returnsValue("0");
    with.query("select * from (values 0) as t(c)\n"
        + "where \"adhoc\".\"toTimestampFun\"(c) in (\n"
        + "  '1970-01-01 00:00:00',\n"
        + "  '1997-02-01 00:00:00')")
        .returnsValue("0");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1834">[CALCITE-1834]
   * User-defined function for Arrays</a>. */
  @Test void testArrayUserDefinedFunction() throws Exception {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));

      SchemaPlus post = rootSchema.add("POST", new AbstractSchema());
      post.add("ARRAY_APPEND", new ArrayAppendDoubleFunction());
      post.add("ARRAY_APPEND", new ArrayAppendIntegerFunction());
      final String sql = "select \"empid\" as EMPLOYEE_ID,\n"
          + "  \"name\" || ' ' || \"name\" as EMPLOYEE_NAME,\n"
          + "  \"salary\" as EMPLOYEE_SALARY,\n"
          + "  POST.ARRAY_APPEND(ARRAY[1,2,3], \"deptno\") as DEPARTMENTS\n"
          + "from \"hr\".\"emps\"";

      final String result = ""
          + "EMPLOYEE_ID=100; EMPLOYEE_NAME=Bill Bill;"
          + " EMPLOYEE_SALARY=10000.0; DEPARTMENTS=[1, 2, 3, 10]\n"
          + "EMPLOYEE_ID=200; EMPLOYEE_NAME=Eric Eric;"
          + " EMPLOYEE_SALARY=8000.0; DEPARTMENTS=[1, 2, 3, 20]\n"
          + "EMPLOYEE_ID=150; EMPLOYEE_NAME=Sebastian Sebastian;"
          + " EMPLOYEE_SALARY=7000.0; DEPARTMENTS=[1, 2, 3, 10]\n"
          + "EMPLOYEE_ID=110; EMPLOYEE_NAME=Theodore Theodore;"
          + " EMPLOYEE_SALARY=11500.0; DEPARTMENTS=[1, 2, 3, 10]\n";

      try (Statement statement = connection.createStatement();
           ResultSet resultSet = statement.executeQuery(sql)) {
        assertThat(CalciteAssert.toString(resultSet), is(result));
      }
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7073">[CALCITE-7073]
   * If the UDF's Java return type is ByteString, Calcite should deduce that the
   * SQL type is VARBINARY</a>.
   *
   * <p>Tests that the {@code UNBASE64} user-defined function correctly decodes
   * a Base64 string and its return type ({@code VARBINARY}, mapped from
   * {@link org.apache.calcite.avatica.util.ByteString}) is fully compatible for
   * direct comparison with SQL {@code VARBINARY} literals ({@code X'...'})
   * within queries. */
  @Test void testUnbase64DirectComparison() {
    final String testHex = "74657374"; // "test" in bytes
    final String testBase64 = "dGVzdA=="; // Base64 for "test"

    final String sql = "values \"adhoc\".unbase64('" + testBase64 + "')";
    withUdf().query(sql).typeIs("[EXPR$0 VARBINARY]");

    final String sql2 = "select \"adhoc\".unbase64(cast('" + testBase64
        + "' as varchar)) = x'" + testHex + "' as C\n";
    withUdf().query(sql2).returns("C=true\n");
  }

  /**
   * Test for <a href="https://issues.apache.org/jira/browse/CALCITE-7186">[CALCITE-7186]</a>
   * Add mapping from Character[] to VARCHAR in Java UDF.
   */
  @Test void testCharacterArrayComparison() {
    final String testString = "abc";
    String sql = "values \"adhoc\".characterarray('" + testString + "')";
    withUdf().query(sql).typeIs("[EXPR$0 VARCHAR]");
  }

  /**
   * Base class for functions that append arrays.
   */
  private abstract static class ArrayAppendScalarFunction
      implements ScalarFunction, ImplementableFunction {
    public List<FunctionParameter> getParameters() {
      final List<FunctionParameter> parameters = new ArrayList<>();
      for (final Ord<RelProtoDataType> type : Ord.zip(getParams())) {
        parameters.add(
            new FunctionParameter() {
              public int getOrdinal() {
                return type.i;
              }

              public String getName() {
                return "arg" + type.i;
              }

              public RelDataType getType(RelDataTypeFactory typeFactory) {
                return type.e.apply(typeFactory);
              }

              public boolean isOptional() {
                return false;
              }
            });
      }
      return parameters;
    }

    protected abstract List<RelProtoDataType> getParams();

    @Override public CallImplementor getImplementor() {
      return (translator, call, nullAs) -> {
        Method lookupMethod =
            Types.lookupMethod(Smalls.AllTypesFunction.class,
                "arrayAppendFun", List.class, Integer.class);
        return Expressions.call(lookupMethod,
            translator.translateList(call.getOperands(), nullAs));
      };
    }
  }

  /** Function with signature "f(ARRAY OF INTEGER, INTEGER) returns ARRAY OF
   * INTEGER". */
  private static class ArrayAppendIntegerFunction
      extends ArrayAppendScalarFunction {
    @Override public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
      return typeFactory.createArrayType(
          typeFactory.createSqlType(SqlTypeName.INTEGER), -1);
    }

    @Override public List<RelProtoDataType> getParams() {
      return ImmutableList.of(
          typeFactory -> typeFactory.createArrayType(
              typeFactory.createSqlType(SqlTypeName.INTEGER), -1),
          typeFactory -> typeFactory.createSqlType(SqlTypeName.INTEGER));
    }
  }

  /** Function with signature "f(ARRAY OF DOUBLE, INTEGER) returns ARRAY OF
   * DOUBLE". */
  private static class ArrayAppendDoubleFunction
      extends ArrayAppendScalarFunction {
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
      return typeFactory.createArrayType(
          typeFactory.createSqlType(SqlTypeName.DOUBLE), -1);
    }

    public List<RelProtoDataType> getParams() {
      return ImmutableList.of(
          typeFactory -> typeFactory.createArrayType(
              typeFactory.createSqlType(SqlTypeName.DOUBLE), -1),
          typeFactory -> typeFactory.createSqlType(SqlTypeName.INTEGER));
    }
  }

}
