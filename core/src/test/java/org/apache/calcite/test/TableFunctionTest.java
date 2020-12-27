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
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableFunction;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.TableFunctionImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.TestUtil;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for user-defined table functions.
 *
 * @see UdfTest
 * @see Smalls
 */
class TableFunctionTest {
  private CalciteAssert.AssertThat with() {
    final String c = Smalls.class.getName();
    final String m = Smalls.MULTIPLICATION_TABLE_METHOD.getName();
    final String m2 = Smalls.FIBONACCI_TABLE_METHOD.getName();
    final String m3 = Smalls.FIBONACCI_LIMIT_TABLE_METHOD.getName();
    return CalciteAssert.model("{\n"
        + "  version: '1.0',\n"
        + "   schemas: [\n"
        + "     {\n"
        + "       name: 's',\n"
        + "       functions: [\n"
        + "         {\n"
        + "           name: 'multiplication',\n"
        + "           className: '" + c + "',\n"
        + "           methodName: '" + m + "'\n"
        + "         }, {\n"
        + "           name: 'fibonacci',\n"
        + "           className: '" + c + "',\n"
        + "           methodName: '" + m2 + "'\n"
        + "         }, {\n"
        + "           name: 'fibonacci2',\n"
        + "           className: '" + c + "',\n"
        + "           methodName: '" + m3 + "'\n"
        + "         }\n"
        + "       ]\n"
        + "     }\n"
        + "   ]\n"
        + "}")
        .withDefaultSchema("s");
  }

  /**
   * Tests a table function with literal arguments.
   */
  @Test void testTableFunction() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
      final TableFunction table =
          TableFunctionImpl.create(Smalls.GENERATE_STRINGS_METHOD);
      schema.add("GenerateStrings", table);
      final String sql = "select *\n"
          + "from table(\"s\".\"GenerateStrings\"(5)) as t(n, c)\n"
          + "where char_length(c) > 3";
      ResultSet resultSet = connection.createStatement().executeQuery(sql);
      assertThat(CalciteAssert.toString(resultSet),
          equalTo("N=4; C=abcd\n"));
    }
  }

  /**
   * Tests correlated subquery with 2 identical params is being processed correctly.
   */
  @Test void testInterpretFunctionWithInitializer() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
      final TableFunction table =
          TableFunctionImpl.create(Smalls.DUMMY_TABLE_METHOD_WITH_TWO_PARAMS);
      final String callMethodName = Smalls.DUMMY_TABLE_METHOD_WITH_TWO_PARAMS.getName();
      schema.add(callMethodName, table);
      final String sql = "select x, (select * from table (\"s\".\"" + callMethodName + "\"(x, x))) "
          + "from (values (2), (4)) as t (x)";
      ResultSet resultSet = connection.createStatement().executeQuery(sql);
      assertThat(CalciteAssert.toString(resultSet),
          equalTo("X=2; EXPR$1=null\nX=4; EXPR$1=null\n"));
    }
  }

  @Test void testTableFunctionWithArrayParameter() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
      final TableFunction table =
          TableFunctionImpl.create(Smalls.GENERATE_STRINGS_OF_INPUT_SIZE_METHOD);
      schema.add("GenerateStringsOfInputSize", table);
      final String sql = "select *\n"
          + "from table(\"s\".\"GenerateStringsOfInputSize\"(ARRAY[5,4,3,1,2])) as t(n, c)\n"
          + "where char_length(c) > 3";
      ResultSet resultSet = connection.createStatement().executeQuery(sql);
      assertThat(CalciteAssert.toString(resultSet),
          equalTo("N=4; C=abcd\n"));
    }
  }

  @Test void testTableFunctionWithMapParameter() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
      final TableFunction table =
          TableFunctionImpl.create(Smalls.GENERATE_STRINGS_OF_INPUT_MAP_SIZE_METHOD);
      schema.add("GenerateStringsOfInputMapSize", table);
      final String sql = "select *\n"
          + "from table(\"s\".\"GenerateStringsOfInputMapSize\"(Map[5,4,3,1])) as t(n, c)\n"
          + "where char_length(c) > 0";
      ResultSet resultSet = connection.createStatement().executeQuery(sql);
      assertThat(CalciteAssert.toString(resultSet),
          equalTo("N=1; C=a\n"));
    }
  }

  /**
   * Tests a table function that implements {@link ScannableTable} and returns
   * a single column.
   */
  @Test void testScannableTableFunction() throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final TableFunction table = TableFunctionImpl.create(Smalls.MAZE_METHOD);
    schema.add("Maze", table);
    final String sql = "select *\n"
        + "from table(\"s\".\"Maze\"(5, 3, 1))";
    ResultSet resultSet = connection.createStatement().executeQuery(sql);
    final String result = "S=abcde\n"
        + "S=xyz\n"
        + "S=generate(w=5, h=3, s=1)\n";
    assertThat(CalciteAssert.toString(resultSet), is(result));
  }

  /** As {@link #testScannableTableFunction()} but with named parameters. */
  @Test void testScannableTableFunctionWithNamedParameters()
      throws SQLException {
    Connection connection = DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final TableFunction table = TableFunctionImpl.create(Smalls.MAZE2_METHOD);
    schema.add("Maze", table);
    final String sql = "select *\n"
        + "from table(\"s\".\"Maze\"(5, 3, 1))";
    final Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery(sql);
    final String result = "S=abcde\n"
        + "S=xyz\n";
    assertThat(CalciteAssert.toString(resultSet),
        is(result + "S=generate2(w=5, h=3, s=1)\n"));

    final String sql2 = "select *\n"
        + "from table(\"s\".\"Maze\"(WIDTH => 5, HEIGHT => 3, SEED => 1))";
    resultSet = statement.executeQuery(sql2);
    assertThat(CalciteAssert.toString(resultSet),
        is(result + "S=generate2(w=5, h=3, s=1)\n"));

    final String sql3 = "select *\n"
        + "from table(\"s\".\"Maze\"(HEIGHT => 3, WIDTH => 5))";
    resultSet = statement.executeQuery(sql3);
    assertThat(CalciteAssert.toString(resultSet),
        is(result + "S=generate2(w=5, h=3, s=null)\n"));
    connection.close();
  }

  /** As {@link #testScannableTableFunction()} but with named parameters. */
  @Test void testMultipleScannableTableFunctionWithNamedParameters()
      throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:");
         Statement statement = connection.createStatement()) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
      final TableFunction table1 = TableFunctionImpl.create(Smalls.MAZE_METHOD);
      schema.add("Maze", table1);
      final TableFunction table2 = TableFunctionImpl.create(Smalls.MAZE2_METHOD);
      schema.add("Maze", table2);
      final TableFunction table3 = TableFunctionImpl.create(Smalls.MAZE3_METHOD);
      schema.add("Maze", table3);
      final String sql = "select *\n"
          + "from table(\"s\".\"Maze\"(5, 3, 1))";
      ResultSet resultSet = statement.executeQuery(sql);
      final String result = "S=abcde\n"
          + "S=xyz\n";
      assertThat(CalciteAssert.toString(resultSet),
          is(result + "S=generate(w=5, h=3, s=1)\n"));

      final String sql2 = "select *\n"
          + "from table(\"s\".\"Maze\"(WIDTH => 5, HEIGHT => 3, SEED => 1))";
      resultSet = statement.executeQuery(sql2);
      assertThat(CalciteAssert.toString(resultSet),
          is(result + "S=generate2(w=5, h=3, s=1)\n"));

      final String sql3 = "select *\n"
          + "from table(\"s\".\"Maze\"(HEIGHT => 3, WIDTH => 5))";
      resultSet = statement.executeQuery(sql3);
      assertThat(CalciteAssert.toString(resultSet),
          is(result + "S=generate2(w=5, h=3, s=null)\n"));

      final String sql4 = "select *\n"
          + "from table(\"s\".\"Maze\"(FOO => 'a'))";
      resultSet = statement.executeQuery(sql4);
      assertThat(CalciteAssert.toString(resultSet),
          is(result + "S=generate3(foo=a)\n"));
    }
  }

  /**
   * Tests a table function that returns different row type based on
   * actual call arguments.
   */
  @Test void testTableFunctionDynamicStructure() throws SQLException {
    Connection connection = getConnectionWithMultiplyFunction();
    final PreparedStatement ps = connection.prepareStatement("select *\n"
        + "from table(\"s\".\"multiplication\"(4, 3, ?))\n");
    ps.setInt(1, 100);
    ResultSet resultSet = ps.executeQuery();
    assertThat(CalciteAssert.toString(resultSet),
        equalTo("row_name=row 0; c1=101; c2=102; c3=103; c4=104\n"
            + "row_name=row 1; c1=102; c2=104; c3=106; c4=108\n"
            + "row_name=row 2; c1=103; c2=106; c3=109; c4=112\n"));
  }

  /**
   * Tests that non-nullable arguments of a table function must be provided
   * as literals.
   */
  @Disabled("SQLException does not include message from nested exception")
  @Test void testTableFunctionNonNullableMustBeLiterals()
      throws SQLException {
    Connection connection = getConnectionWithMultiplyFunction();
    try {
      final PreparedStatement ps = connection.prepareStatement("select *\n"
          + "from table(\"s\".\"multiplication\"(?, 3, 100))\n");
      ps.setInt(1, 100);
      ResultSet resultSet = ps.executeQuery();
      fail("Should fail, got " + resultSet);
    } catch (SQLException e) {
      assertThat(e.getMessage(),
          containsString("Wrong arguments for table function 'public static "
              + "org.apache.calcite.schema.QueryableTable "
              + "org.apache.calcite.test.JdbcTest"
              + ".multiplicationTable(int,int,java.lang.Integer)'"
              + " call. Expected '[int, int, class"
              + "java.lang.Integer]', actual '[null, 3, 100]'"));
    }
  }

  private Connection getConnectionWithMultiplyFunction() throws SQLException {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    final TableFunction table =
        TableFunctionImpl.create(Smalls.MULTIPLICATION_TABLE_METHOD);
    schema.add("multiplication", table);
    return connection;
  }

  /**
   * Tests a table function that takes cursor input.
   */
  @Disabled("CannotPlanException: Node [rel#18:Subset#4.ENUMERABLE.[]] "
      + "could not be implemented")
  @Test void testTableFunctionCursorInputs() throws SQLException {
    try (Connection connection =
             DriverManager.getConnection("jdbc:calcite:")) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
      final TableFunction table =
          TableFunctionImpl.create(Smalls.GENERATE_STRINGS_METHOD);
      schema.add("GenerateStrings", table);
      final TableFunction add =
          TableFunctionImpl.create(Smalls.PROCESS_CURSOR_METHOD);
      schema.add("process", add);
      final PreparedStatement ps = connection.prepareStatement("select *\n"
          + "from table(\"s\".\"process\"(2,\n"
          + "cursor(select * from table(\"s\".\"GenerateStrings\"(?)))\n"
          + ")) as t(u)\n"
          + "where u > 3");
      ps.setInt(1, 5);
      ResultSet resultSet = ps.executeQuery();
      // GenerateStrings returns 0..4, then 2 is added (process function),
      // thus 2..6, finally where u > 3 leaves just 4..6
      assertThat(CalciteAssert.toString(resultSet),
          equalTo("u=4\n"
              + "u=5\n"
              + "u=6\n"));
    }
  }

  /**
   * Tests a table function that takes multiple cursor inputs.
   */
  @Disabled("CannotPlanException: Node [rel#24:Subset#6.ENUMERABLE.[]] "
      + "could not be implemented")
  @Test void testTableFunctionCursorsInputs() throws SQLException {
    try (Connection connection = getConnectionWithMultiplyFunction()) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus schema = rootSchema.getSubSchema("s");
      final TableFunction table =
          TableFunctionImpl.create(Smalls.GENERATE_STRINGS_METHOD);
      schema.add("GenerateStrings", table);
      final TableFunction add =
          TableFunctionImpl.create(Smalls.PROCESS_CURSORS_METHOD);
      schema.add("process", add);
      final PreparedStatement ps = connection.prepareStatement("select *\n"
          + "from table(\"s\".\"process\"(2,\n"
          + "cursor(select * from table(\"s\".\"multiplication\"(5,5,0))),\n"
          + "cursor(select * from table(\"s\".\"GenerateStrings\"(?)))\n"
          + ")) as t(u)\n"
          + "where u > 3");
      ps.setInt(1, 5);
      ResultSet resultSet = ps.executeQuery();
      // GenerateStrings produce 0..4
      // multiplication produce 1..5
      // process sums and adds 2
      // sum is 2 + 1..9 == 3..9
      assertThat(CalciteAssert.toString(resultSet),
          equalTo("u=4\n"
              + "u=5\n"
              + "u=6\n"
              + "u=7\n"
              + "u=8\n"
              + "u=9\n"));
    }
  }

  /** Tests a query with a table function in the FROM clause.
   *
   * @see Smalls#multiplicationTable */
  @Test void testUserDefinedTableFunction() {
    final String q = "select *\n"
        + "from table(\"s\".\"multiplication\"(2, 3, 100))\n";
    with().query(q)
        .returnsUnordered(
            "row_name=row 0; c1=101; c2=102",
            "row_name=row 1; c1=102; c2=104",
            "row_name=row 2; c1=103; c2=106");
  }

  /** Tests a query with a table function in the FROM clause,
   * attempting to reference a column from the table function in the WHERE
   * clause but getting the case wrong.
   *
   * @see Smalls#multiplicationTable */
  @Test void testUserDefinedTableFunction2() {
    final String q = "select c1\n"
        + "from table(\"s\".\"multiplication\"(2, 3, 100))\n"
        + "where c1 + 2 < c2";
    with().query(q)
        .throws_("Column 'C1' not found in any table; did you mean 'c1'?");
  }

  /** Tests a query with a table function in the FROM clause,
   * referencing columns in the WHERE clause.
   *
   * @see Smalls#multiplicationTable */
  @Test void testUserDefinedTableFunction3() {
    final String q = "select \"c1\"\n"
        + "from table(\"s\".\"multiplication\"(2, 3, 100))\n"
        + "where \"c1\" + 2 < \"c2\"";
    with().query(q).returnsUnordered("c1=103");
  }

  /** As {@link #testUserDefinedTableFunction3()}, but provides a character
   * literal argument for an integer parameter. */
  @Test void testUserDefinedTableFunction4() {
    final String q = "select \"c1\"\n"
        + "from table(\"s\".\"multiplication\"('2', 3, 100))\n"
        + "where \"c1\" + 2 < \"c2\"";
    with().query(q).returnsUnordered("c1=103");
  }

  @Test void testUserDefinedTableFunction5() {
    final String q = "select *\n"
        + "from table(\"s\".\"multiplication\"(3, 100))\n"
        + "where c1 + 2 < c2";
    final String e = "No match found for function signature "
        + "multiplication(<NUMERIC>, <NUMERIC>)";
    with().query(q).throws_(e);
  }

  @Test void testUserDefinedTableFunction6() {
    final String q = "select *\n"
        + "from table(\"s\".\"fibonacci\"())";
    with().query(q)
        .returns(r -> {
          try {
            final List<Long> numbers = new ArrayList<>();
            while (r.next() && numbers.size() < 13) {
              numbers.add(r.getLong(1));
            }
            assertThat(numbers.toString(),
                is("[1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233]"));
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
        });
  }

  @Test void testUserDefinedTableFunction7() {
    final String q = "select *\n"
        + "from table(\"s\".\"fibonacci2\"(20))\n"
        + "where n > 7";
    with().query(q).returnsUnordered("N=13", "N=8");
  }

  @Test void testUserDefinedTableFunction8() {
    final String q = "select count(*) as c\n"
        + "from table(\"s\".\"fibonacci2\"(20))";
    with().query(q).returnsUnordered("C=7");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3364">[CALCITE-3364]
   * Can't group table function result due to a type cast error if table function
   * returns a row with a single value</a>. */
  @Test void testUserDefinedTableFunction9() {
    final String q = "select \"N\" + 1 as c\n"
        + "from table(\"s\".\"fibonacci2\"(3))\n"
        + "group by \"N\"";
    with().query(q).returnsUnordered("C=2\nC=3\nC=4");
  }

  @Test void testCrossApply() {
    final String q1 = "select *\n"
        + "from (values 2, 5) as t (c)\n"
        + "cross apply table(\"s\".\"fibonacci2\"(c))";
    final String q2 = "select *\n"
        + "from (values 2, 5) as t (c)\n"
        + "cross apply table(\"s\".\"fibonacci2\"(t.c))";
    for (String q : new String[] {q1, q2}) {
      with()
          .with(CalciteConnectionProperty.CONFORMANCE,
              SqlConformanceEnum.LENIENT)
          .query(q)
          .returnsUnordered("C=2; N=1",
              "C=2; N=1",
              "C=2; N=2",
              "C=5; N=1",
              "C=5; N=1",
              "C=5; N=2",
              "C=5; N=3",
              "C=5; N=5");
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2004">[CALCITE-2004]
   * Wrong plan generated for left outer apply with table function</a>. */
  @Test void testLeftOuterApply() {
    final String sql = "select *\n"
        + "from (values 4) as t (c)\n"
        + "left join lateral table(\"s\".\"fibonacci2\"(c)) as R(n) on c=n";
    with()
        .with(CalciteConnectionProperty.CONFORMANCE,
            SqlConformanceEnum.LENIENT)
        .query(sql)
        .returnsUnordered("C=4; N=null");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2382">[CALCITE-2382]
   * Sub-query lateral joined to table function</a>. */
  @Test void testInlineViewLateralTableFunction() throws SQLException {
    try (Connection connection = DriverManager.getConnection("jdbc:calcite:")) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
      final TableFunction table =
          TableFunctionImpl.create(Smalls.GENERATE_STRINGS_METHOD);
      schema.add("GenerateStrings", table);
      Table tbl = new ScannableTableTest.SimpleTable();
      schema.add("t", tbl);

      final String sql = "select *\n"
          + "from (select 5 as f0 from \"s\".\"t\") \"a\",\n"
          + "  lateral table(\"s\".\"GenerateStrings\"(f0)) as t(n, c)\n"
          + "where char_length(c) > 3";
      ResultSet resultSet = connection.createStatement().executeQuery(sql);
      final String expected = "F0=5; N=4; C=abcd\n"
          + "F0=5; N=4; C=abcd\n"
          + "F0=5; N=4; C=abcd\n"
          + "F0=5; N=4; C=abcd\n";
      assertThat(CalciteAssert.toString(resultSet), equalTo(expected));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4448">[CALCITE-4448]
   * Use TableMacro user-defined table functions with QueryableTable</a>. */
  @Test void testQueryableTableWithTableMacro() throws SQLException {
    try (Connection connection =
        DriverManager.getConnection("jdbc:calcite:")) {
      CalciteConnection calciteConnection =
          connection.unwrap(CalciteConnection.class);
      SchemaPlus rootSchema = calciteConnection.getRootSchema();
      SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
      schema.add("simple", new Smalls.SimpleTableMacro());

      String sql = "select * from table(\"s\".\"simple\"())";
      ResultSet resultSet = connection.createStatement().executeQuery(sql);
      String expected = "A=foo; B=5\n"
          + "A=bar; B=4\n"
          + "A=foo; B=3\n";
      assertThat(CalciteAssert.toString(resultSet),
          equalTo(expected));
    }
  }
}
