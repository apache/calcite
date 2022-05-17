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

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.TableMacroImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.test.schemata.catchall.CatchallSchema;
import org.apache.calcite.test.schemata.catchall.CatchallSchema.EveryType;
import org.apache.calcite.test.schemata.hr.Employee;
import org.apache.calcite.test.schemata.hr.HrSchema;
import org.apache.calcite.util.Smalls;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Unit tests for {@link ReflectiveSchema}.
 */
public class ReflectiveSchemaTest {
  public static final Method LINQ4J_AS_ENUMERABLE_METHOD =
      Types.lookupMethod(
          Linq4j.class, "asEnumerable", Object[].class);

  private static final ReflectiveSchema CATCHALL =
      new ReflectiveSchema(new CatchallSchema());

  /**
   * Test that uses a JDBC connection as a linq4j
   * {@link org.apache.calcite.linq4j.QueryProvider}.
   *
   * @throws Exception on error
   */
  @Test void testQueryProvider() throws Exception {
    Connection connection = CalciteAssert
        .that(CalciteAssert.Config.REGULAR).connect();
    QueryProvider queryProvider = connection.unwrap(QueryProvider.class);
    ParameterExpression e = Expressions.parameter(Employee.class, "e");

    // "Enumerable<T> asEnumerable(final T[] ts)"
    List<Object[]> list =
        queryProvider.createQuery(
            Expressions.call(
                Expressions.call(
                    Types.of(Enumerable.class, Employee.class),
                    null,
                    LINQ4J_AS_ENUMERABLE_METHOD,
                    Expressions.constant(
                        new HrSchema().emps)),
                "asQueryable"),
            Employee.class)
            .where(
                Expressions.lambda(
                    Expressions.lessThan(
                        Expressions.field(
                            e, "empid"),
                        Expressions.constant(160)),
                    e))
            .where(
                Expressions.lambda(
                    Expressions.greaterThan(
                        Expressions.field(
                            e, "empid"),
                        Expressions.constant(140)),
                    e))
            .select(
                Expressions.<Function1<Employee, Object[]>>lambda(
                    Expressions.new_(
                        Object[].class,
                        Expressions.field(
                            e, "empid"),
                        Expressions.call(
                            Expressions.field(
                                e, "name"),
                            "toUpperCase")),
                    e))
            .toList();
    assertEquals(1, list.size());
    assertEquals(2, list.get(0).length);
    assertEquals(150, list.get(0)[0]);
    assertEquals("SEBASTIAN", list.get(0)[1]);
  }

  @Test void testQueryProviderSingleColumn() throws Exception {
    Connection connection = CalciteAssert
        .that(CalciteAssert.Config.REGULAR).connect();
    QueryProvider queryProvider = connection.unwrap(QueryProvider.class);
    ParameterExpression e = Expressions.parameter(Employee.class, "e");

    // "Enumerable<T> asEnumerable(final T[] ts)"
    List<Integer> list =
        queryProvider.createQuery(
            Expressions.call(
                Expressions.call(
                    Types.of(Enumerable.class, Employee.class),
                    null,
                    LINQ4J_AS_ENUMERABLE_METHOD,
                    Expressions.constant(new HrSchema().emps)),
                "asQueryable"),
            Employee.class)
            .select(
                Expressions.<Function1<Employee, Integer>>lambda(
                    Expressions.field(e, "empid"),
                    e))
            .toList();
    assertEquals(Arrays.asList(100, 200, 150, 110), list);
  }

  /**
   * Tests a relation that is accessed via method syntax.
   * The function returns a {@link org.apache.calcite.linq4j.Queryable}.
   */
  @Disabled
  @Test void testOperator() throws SQLException, ClassNotFoundException {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    schema.add("GenerateStrings",
        TableMacroImpl.create(Smalls.GENERATE_STRINGS_METHOD));
    schema.add("StringUnion",
        TableMacroImpl.create(Smalls.STRING_UNION_METHOD));
    rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
    ResultSet resultSet = connection.createStatement().executeQuery(
        "select *\n"
        + "from table(s.StringUnion(\n"
        + "  GenerateStrings(5),\n"
        + "  cursor (select name from emps)))\n"
        + "where char_length(s) > 3");
    assertTrue(resultSet.next());
  }

  /**
   * Tests a view.
   */
  @Test void testView() throws SQLException, ClassNotFoundException {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    schema.add("emps_view",
        ViewTable.viewMacro(schema,
            "select * from \"hr\".\"emps\" where \"deptno\" = 10",
            null, Arrays.asList("s", "emps_view"), null));
    rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
    ResultSet resultSet = connection.createStatement().executeQuery(
        "select *\n"
        + "from \"s\".\"emps_view\"\n"
        + "where \"empid\" < 120");
    assertEquals(
        "empid=100; deptno=10; name=Bill; salary=10000.0; commission=1000\n"
        + "empid=110; deptno=10; name=Theodore; salary=11500.0; commission=250\n",
        CalciteAssert.toString(resultSet));
  }

  /**
   * Tests a view with a path.
   */
  @Test void testViewPath() throws SQLException, ClassNotFoundException {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    // create a view s.emps based on hr.emps. uses explicit schema path "hr".
    schema.add("emps",
        ViewTable.viewMacro(schema,
            "select * from \"emps\" where \"deptno\" = 10",
            ImmutableList.of("hr"), ImmutableList.of("s", "emps"), null));
    schema.add("hr_emps",
        ViewTable.viewMacro(schema,
            "select * from \"emps\"",
            ImmutableList.of("hr"), ImmutableList.of("s", "hr_emps"), null));
    schema.add("s_emps",
        ViewTable.viewMacro(schema,
            "select * from \"emps\"",
            ImmutableList.of("s"), ImmutableList.of("s", "s_emps"), null));
    schema.add("null_emps",
        ViewTable.viewMacro(schema, "select * from \"emps\"", null,
            ImmutableList.of("s", "null_emps"), null));
    rootSchema.add("hr", new ReflectiveSchema(new HrSchema()));
    final Statement statement = connection.createStatement();
    ResultSet resultSet;
    resultSet = statement.executeQuery(
        "select * from \"s\".\"hr_emps\"");
    assertEquals(4, count(resultSet)); // "hr_emps" -> "hr"."emps", 4 rows
    resultSet = statement.executeQuery(
        "select * from \"s\".\"s_emps\""); // "s_emps" -> "s"."emps", 3 rows
    assertEquals(3, count(resultSet));
    resultSet = statement.executeQuery(
        "select * from \"s\".\"null_emps\""); // "null_emps" -> "s"."emps", 3
    assertEquals(3, count(resultSet));
    statement.close();
  }

  private int count(ResultSet resultSet) throws SQLException {
    int i = 0;
    while (resultSet.next()) {
      ++i;
    }
    resultSet.close();
    return i;
  }

  /** Tests column based on java.sql.Date field. */
  @Test void testDateColumn() throws Exception {
    CalciteAssert.that()
        .withSchema("s", new ReflectiveSchema(new DateColumnSchema()))
        .query("select * from \"s\".\"emps\"")
        .returns(""
            + "hireDate=1970-01-01; empid=10; deptno=20; name=fred; salary=0.0; commission=null\n"
            + "hireDate=1970-04-11; empid=10; deptno=20; name=bill; salary=0.0; commission=null\n");
  }

  /** Tests querying an object that has no public fields. */
  @Test void testNoPublicFields() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    with.query("select 1 from \"s\".\"allPrivates\"")
        .returns("EXPR$0=1\n");
    with.query("select \"x\" from \"s\".\"allPrivates\"")
        .throws_("Column 'x' not found in any table");
  }

  /** Tests columns based on types such as java.sql.Date and java.util.Date.
   *
   * @see CatchallSchema#everyTypes */
  @Test void testColumnTypes() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    with.query("select \"primitiveBoolean\" from \"s\".\"everyTypes\"")
        .returns("primitiveBoolean=false\n"
            + "primitiveBoolean=true\n");
    with.query("select * from \"s\".\"everyTypes\"")
        .returns(""
            + "primitiveBoolean=false; primitiveByte=0; primitiveChar=\u0000; primitiveShort=0; primitiveInt=0; primitiveLong=0; primitiveFloat=0.0; primitiveDouble=0.0; wrapperBoolean=false; wrapperByte=0; wrapperCharacter=\u0000; wrapperShort=0; wrapperInteger=0; wrapperLong=0; wrapperFloat=0.0; wrapperDouble=0.0; sqlDate=1970-01-01; sqlTime=00:00:00; sqlTimestamp=1970-01-01 00:00:00; utilDate=1970-01-01 00:00:00; string=1; bigDecimal=0\n"
            + "primitiveBoolean=true; primitiveByte=127; primitiveChar=\uffff; primitiveShort=32767; primitiveInt=2147483647; primitiveLong=9223372036854775807; primitiveFloat=3.4028235E38; primitiveDouble=1.7976931348623157E308; wrapperBoolean=null; wrapperByte=null; wrapperCharacter=null; wrapperShort=null; wrapperInteger=null; wrapperLong=null; wrapperFloat=null; wrapperDouble=null; sqlDate=null; sqlTime=null; sqlTimestamp=null; utilDate=null; string=null; bigDecimal=null\n");
  }

  /** Tests NOT for nullable columns.
   *
   * @see CatchallSchema#everyTypes */
  @Test void testWhereNOT() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    with.query(
        "select \"wrapperByte\" from \"s\".\"everyTypes\" where NOT (\"wrapperByte\" is null)")
        .returnsUnordered("wrapperByte=0");
  }

  /** Tests NOT for nullable columns.
   *
   * @see CatchallSchema#everyTypes */
  @Test void testSelectNOT() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    with.query(
        "select NOT \"wrapperBoolean\" \"value\" from \"s\".\"everyTypes\"")
        .returnsUnordered(
            "value=null",
            "value=true");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2404">[CALCITE-2404]
   * Accessing structured-types is not implemented by the runtime</a>. */
  @Test void testSelectWithFieldAccessOnFirstLevelRecordType() {
    CalciteAssert.that()
        .with(CalciteAssert.SchemaSpec.BOOKSTORE)
        .query("select au.\"birthPlace\".\"city\" as city from \"bookstore\".\"authors\" au\n")
        .returnsUnordered("CITY=Heraklion", "CITY=Besançon", "CITY=Ionia");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5157">[CALCITE-5157]
   * ClassCastException in checkRollUp with DOT operator</a>. */
  @Test void testSelectWithFieldAccessOnFirstLevelRecordTypeWithParentheses() {
    CalciteAssert.that()
        .with(CalciteAssert.SchemaSpec.BOOKSTORE)
        .query("select (\"birthPlace\").\"city\" as city from \"bookstore\".\"authors\"\n")
        .returnsUnordered("CITY=Heraklion", "CITY=Besançon", "CITY=Ionia");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2404">[CALCITE-2404]
   * Accessing structured-types is not implemented by the runtime</a>. */
  @Test void testSelectWithFieldAccessOnSecondLevelRecordType() {
    CalciteAssert.that()
        .with(CalciteAssert.SchemaSpec.BOOKSTORE)
        .query("select au.\"birthPlace\".\"coords\".\"latitude\" as lat\n"
            + "from \"bookstore\".\"authors\" au\n")
        .returnsUnordered("LAT=47.24", "LAT=35.3387", "LAT=null");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2404">[CALCITE-2404]
   * Accessing structured-types is not implemented by the runtime</a>. */
  @Test void testWhereWithFieldAccessOnFirstLevelRecordType() {
    CalciteAssert.that()
        .with(CalciteAssert.SchemaSpec.BOOKSTORE)
        .query("select au.\"aid\" as aid from \"bookstore\".\"authors\" au\n"
            + "where au.\"birthPlace\".\"city\"='Heraklion'")
        .returnsUnordered("AID=2");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2404">[CALCITE-2404]
   * Accessing structured-types is not implemented by the runtime</a>. */
  @Test void testWhereWithFieldAccessOnSecondLevelRecordType() {
    CalciteAssert.that()
        .with(CalciteAssert.SchemaSpec.BOOKSTORE)
        .query("select au.\"aid\" as aid from \"bookstore\".\"authors\" au\n"
            + "where au.\"birthPlace\".\"coords\".\"latitude\"=35.3387")
        .returnsUnordered("AID=2");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2404">[CALCITE-2404]
   * Accessing structured-types is not implemented by the runtime</a>. */
  @Test void testSelectWithFieldAccessOnFirstLevelRecordTypeArray() {
    CalciteAssert.that()
        .with(CalciteAssert.SchemaSpec.BOOKSTORE)
        .query("select au.\"books\"[1].\"title\" as title from \"bookstore\".\"authors\" au\n")
        .returnsUnordered("TITLE=Les Misérables", "TITLE=Zorba the Greek", "TITLE=null");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2404">[CALCITE-2404]
   * Accessing structured-types is not implemented by the runtime</a>. */
  @Test void testSelectWithFieldAccessOnSecondLevelRecordTypeArray() {
    CalciteAssert.that()
        .with(CalciteAssert.SchemaSpec.BOOKSTORE)
        .query("select au.\"books\"[1].\"pages\"[1].\"pageNo\" as pno\n"
            + "from \"bookstore\".\"authors\" au\n")
        .returnsUnordered("PNO=1", "PNO=1", "PNO=null");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2404">[CALCITE-2404]
   * Accessing structured-types is not implemented by the runtime</a>. */
  @Test void testWhereWithFieldAccessOnFirstLevelRecordTypeArray() {
    CalciteAssert.that()
        .with(CalciteAssert.SchemaSpec.BOOKSTORE)
        .query("select au.\"aid\" as aid from \"bookstore\".\"authors\" au\n"
            + "where au.\"books\"[1].\"title\"='Les Misérables'")
        .returnsUnordered("AID=1");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2404">[CALCITE-2404]
   * Accessing structured-types is not implemented by the runtime</a>. */
  @Test void testWhereWithFieldAccessOnSecondLevelRecordTypeArray() {
    CalciteAssert.that()
        .with(CalciteAssert.SchemaSpec.BOOKSTORE)
        .query("select au.\"aid\" as aid from \"bookstore\".\"authors\" au\n"
            + "where au.\"books\"[1].\"pages\"[2].\"contentType\"='Acknowledgements'")
        .returnsUnordered("AID=2");
  }

  /** Tests columns based on types such as java.sql.Date and java.util.Date.
   *
   * @see CatchallSchema#everyTypes */
  @Test void testAggregateFunctions() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    checkAgg(with, "min");
    checkAgg(with, "max");
    checkAgg(with, "avg");
    checkAgg(with, "count");
  }

  private void checkAgg(CalciteAssert.AssertThat with, String fn) {
    for (final Field field
        : fn.equals("avg") ? EveryType.numericFields() : EveryType.fields()) {
      with.query(
          "select " + fn + "(\"" + field.getName() + "\") as c\n"
              + "from \"s\".\"everyTypes\"")
          .returns(
              input -> {
                int n = 0;
                try {
                  while (input.next()) {
                    final Object o = get(input);
                    Util.discard(o);
                    ++n;
                  }
                } catch (SQLException e) {
                  throw TestUtil.rethrow(e);
                }
                assertThat(n, equalTo(1));
              });
    }
  }

  private Object get(ResultSet input) throws SQLException {
    final int type = input.getMetaData().getColumnType(1);
    switch (type) {
    case java.sql.Types.BOOLEAN:
      return input.getBoolean(1);
    case java.sql.Types.TINYINT:
      return input.getByte(1);
    case java.sql.Types.SMALLINT:
      return input.getShort(1);
    case java.sql.Types.INTEGER:
      return input.getInt(1);
    case java.sql.Types.BIGINT:
      return input.getLong(1);
    case java.sql.Types.REAL:
      return input.getFloat(1);
    case java.sql.Types.DOUBLE:
      return input.getDouble(1);
    case java.sql.Types.CHAR:
    case java.sql.Types.VARCHAR:
      return input.getString(1);
    case java.sql.Types.DATE:
      return input.getDate(1);
    case java.sql.Types.TIME:
      return input.getTime(1);
    case java.sql.Types.TIMESTAMP:
      return input.getTimestamp(1);
    case java.sql.Types.DECIMAL:
      return input.getBigDecimal(1);
    default:
      throw new AssertionError(type);
    }
  }

  @Test void testClassNames() throws Exception {
    CalciteAssert.that()
        .withSchema("s", CATCHALL).query("select * from \"s\".\"everyTypes\"")
        .returns(
            resultSet -> {
              try {
                final ResultSetMetaData metaData = resultSet.getMetaData();
                check(metaData, "primitiveBoolean", Boolean.class);
                check(metaData, "primitiveByte", Byte.class);
                check(metaData, "primitiveChar", String.class);
                check(metaData, "primitiveShort", Short.class);
                check(metaData, "primitiveInt", Integer.class);
                check(metaData, "primitiveLong", Long.class);
                check(metaData, "primitiveFloat", Float.class);
                check(metaData, "primitiveDouble", Double.class);
                check(metaData, "wrapperBoolean", Boolean.class);
                check(metaData, "wrapperByte", Byte.class);
                check(metaData, "wrapperCharacter", String.class);
                check(metaData, "wrapperShort", Short.class);
                check(metaData, "wrapperInteger", Integer.class);
                check(metaData, "wrapperLong", Long.class);
                check(metaData, "wrapperFloat", Float.class);
                check(metaData, "wrapperDouble", Double.class);
                check(metaData, "sqlDate", java.sql.Date.class);
                check(metaData, "sqlTime", Time.class);
                check(metaData, "sqlTimestamp", Timestamp.class);
                check(metaData, "utilDate", Timestamp.class);
                check(metaData, "string", String.class);
              } catch (SQLException e) {
                throw TestUtil.rethrow(e);
              }
            });
  }

  private void check(ResultSetMetaData metaData, String columnName,
      Class expectedType) throws SQLException {
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      if (metaData.getColumnName(i).equals(columnName)) {
        assertThat(metaData.getColumnClassName(i),
            equalTo(expectedType.getName()));
        return;
      }
    }
    fail("column not found: " + columnName);
  }

  @Test void testJavaBoolean() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"primitiveBoolean\"")
        .returns("C=1\n");
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"wrapperBoolean\"")
        .returns("C=0\n");
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"wrapperBoolean\" is true")
        .returns("C=0\n");
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"wrapperBoolean\" is not true")
        .returns("C=2\n");
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"wrapperBoolean\" is false")
        .returns("C=1\n");
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"wrapperBoolean\" is not false")
        .returns("C=1\n");
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"wrapperBoolean\" is null")
        .returns("C=1\n");
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"wrapperBoolean\" is not null")
        .returns("C=1\n");
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"primitiveInt\" > 0")
        .returns("C=1\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-119">[CALCITE-119]
   * Comparing a Java type long with a SQL type INTEGER gives wrong
   * answer</a>. */
  @Test void testCompareJavaAndSqlTypes() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    // With CALCITE-119, returned 0 rows. The problem was that when comparing
    // a Java type (long) and a SQL type (INTEGER), the SQL type was deemed
    // "less restrictive". So, the long value got truncated to an int value.
    with.query("select \"primitiveLong\" as c from \"s\".\"everyTypes\"\n"
        + "where \"primitiveLong\" > 0")
        .returns("C=9223372036854775807\n");

    // count(nullif(b, false)) counts how many times b is true
    with.query("select count(\"primitiveBoolean\") as p,\n"
        + "  count(\"wrapperBoolean\") as w,\n"
        + "  count(nullif(\"primitiveShort\" >= 0, false)) as sp,\n"
        + "  count(nullif(\"wrapperShort\" >= 0, false)) as sw,\n"
        + "  count(nullif(\"primitiveInt\" >= 0, false)) as ip,\n"
        + "  count(nullif(\"wrapperInteger\" >= 0, false)) as iw,\n"
        + "  count(nullif(\"primitiveLong\" >= 0, false)) as lp,\n"
        + "  count(nullif(\"wrapperLong\" >= 0, false)) as lw\n"
        + "from \"s\".\"everyTypes\"")
        .returns("P=2; W=1; SP=2; SW=1; IP=2; IW=1; LP=2; LW=1\n");
  }

  @Test void testDivideWraperPrimitive() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    with.query("select \"wrapperLong\" / \"primitiveLong\" as c\n"
        + " from \"s\".\"everyTypes\" where \"primitiveLong\" <> 0")
        .planContains(
            "final Long input_value = current.wrapperLong;")
        .planContains(
            "return input_value == null ? (Long) null : Long.valueOf(input_value.longValue() / current.primitiveLong);")
        .returns("C=null\n");
  }

  @Test void testDivideDoubleBigDecimal() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    with.query("select \"wrapperDouble\" / \"bigDecimal\" as c\n"
        + " from \"s\".\"everyTypes\"")
        .runs();
  }

  @Test void testDivideWraperWrapper() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    with.query("select \"wrapperLong\" / \"wrapperLong\" as c\n"
        + " from \"s\".\"everyTypes\" where \"primitiveLong\" <> 0")
        .planContains(
            "final Long input_value = ((org.apache.calcite.test.schemata.catchall.CatchallSchema.EveryType) inputEnumerator.current()).wrapperLong;")
        .planContains(
            "return input_value == null ? (Long) null : Long.valueOf(input_value.longValue() / input_value.longValue());")
        .returns("C=null\n");
  }

  @Test void testDivideWraperWrapperMultipleTimes() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    with.query("select \"wrapperLong\" / \"wrapperLong\"\n"
        + "+ \"wrapperLong\" / \"wrapperLong\" as c\n"
        + " from \"s\".\"everyTypes\" where \"primitiveLong\" <> 0")
        .planContains(
            "final Long input_value = ((org.apache.calcite.test.schemata.catchall.CatchallSchema.EveryType) inputEnumerator.current()).wrapperLong;")
        .planContains(
            "final Long binary_call_value = input_value == null ? (Long) null : Long.valueOf(input_value.longValue() / input_value.longValue());")
        .planContains(
            "return binary_call_value == null ? (Long) null : Long.valueOf(binary_call_value.longValue() + binary_call_value.longValue());")
        .returns("C=null\n");
  }

  @Test void testOp() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that()
            .withSchema("s", CATCHALL);
    checkOp(with, "+");
    checkOp(with, "-");
    checkOp(with, "*");
    checkOp(with, "/");
  }

  private void checkOp(CalciteAssert.AssertThat with, String fn) {
    for (Field field : EveryType.numericFields()) {
      for (Field field2 : EveryType.numericFields()) {
        final String name = "\"" + field.getName() + "\"";
        final String name2 = "\"" + field2.getName() + "\"";
        with.query("select " + name + "\n"
            + " " + fn + " " + name2 + " as c\n"
            + "from \"s\".\"everyTypes\"\n"
            + "where " + name + " <> 0")
            .returns(resultSet -> { });
      }
    }
  }

  @Test void testCastFromString() {
    CalciteAssert.that().withSchema("s", CATCHALL)
        .query("select cast(\"string\" as int) as c from \"s\".\"everyTypes\"")
        .returns("C=1\n"
            + "C=null\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-580">[CALCITE-580]
   * Average aggregation on an Integer column throws ClassCastException</a>. */
  @Test void testAvgInt() throws Exception {
    CalciteAssert.that().withSchema("s", CATCHALL).with(Lex.JAVA)
        .query("select primitiveLong, avg(primitiveInt)\n"
            + "from s.everyTypes\n"
            + "group by primitiveLong order by primitiveLong")
        .returns(input -> {
          StringBuilder buf = new StringBuilder();
          try {
            while (input.next()) {
              buf.append(input.getInt(2)).append("\n");
            }
          } catch (SQLException e) {
            throw TestUtil.rethrow(e);
          }
          assertThat(buf.toString(), equalTo("0\n2147483647\n"));
        });
  }

  /** Tests that if a field of a relation has an unrecognized type (in this
   * case a {@link BitSet}) then it is treated as an object.
   *
   * @see CatchallSchema#badTypes */
  @Test void testTableFieldHasBadType() throws Exception {
    CalciteAssert.that()
        .withSchema("s", CATCHALL)
        .query("select * from \"s\".\"badTypes\"")
        .returns("integer=0; bitSet={}\n");
  }

  /** Tests that a schema with a field whose type cannot be recognized
   * throws an informative exception.
   *
   * @see CatchallSchema#enumerable
   * @see CatchallSchema#list */
  @Test void testSchemaFieldHasBadType() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    // BitSet is not a valid relation type. It's as if "bitSet" field does
    // not exist.
    with.query("select * from \"s\".\"bitSet\"")
        .throws_("Object 'bitSet' not found within 's'");
    // Enumerable field returns 3 records with 0 fields
    with.query("select * from \"s\".\"enumerable\"")
        .returns("\n"
            + "\n"
            + "\n"
            + "\n");
    // List is implicitly converted to Enumerable
    with.query("select * from \"s\".\"list\"")
        .returns("\n"
            + "\n"
            + "\n"
            + "\n");
  }

  /** Test case for a bug where a Java string 'Abc' compared to a char 'Ab'
   * would be truncated to the char precision and falsely match. */
  @Test void testPrefix() throws Exception {
    CalciteAssert.that()
        .withSchema("s", CATCHALL)
        .query(
            "select * from \"s\".\"prefixEmps\" where \"name\" in ('Ab', 'Abd')")
        .returns("empid=2; deptno=10; name=Ab; salary=0.0; commission=null\n"
            + "empid=4; deptno=10; name=Abd; salary=0.0; commission=null\n");
  }

  /** If a method returns a
   * {@link ViewTable}.{@code ViewTableMacro}, then it
   * should be expanded. */
  @Disabled
  @Test void testTableMacroIsView() throws Exception {
    CalciteAssert.that()
        .withSchema("s", new ReflectiveSchema(new HrSchema()))
        .query("select * from table(\"s\".\"view\"('abc'))")
        .returns(
            "empid=2; deptno=10; name=Ab; salary=0.0; commission=null\n"
                + "empid=4; deptno=10; name=Abd; salary=0.0; commission=null\n");
  }

  /** Finds a table-macro using reflection. */
  @Disabled
  @Test void testTableMacro() throws Exception {
    CalciteAssert.that()
        .withSchema("s", new ReflectiveSchema(new HrSchema()))
        .query("select * from table(\"s\".\"foo\"(3))")
        .returns(
            "empid=2; deptno=10; name=Ab; salary=0.0; commission=null\n"
                + "empid=4; deptno=10; name=Abd; salary=0.0; commission=null\n");
  }

  /** Table with single field as Integer[]. */
  @Disabled(
      "java.lang.AssertionError RelDataTypeImpl.getFieldList(RelDataTypeImpl.java:99)")
  @Test void testArrayOfBoxedPrimitives() {
    CalciteAssert.that()
        .withSchema("s", CATCHALL)
        .query("select * from \"s\".\"primesBoxed\"")
        .returnsUnordered("value=1", "value=3", "value=7");
  }

  /** Table with single field as int[]. */
  @Disabled(
      "java.lang.AssertionError RelDataTypeImpl.getFieldList(RelDataTypeImpl.java:99)")
  @Test void testArrayOfPrimitives() {
    CalciteAssert.that()
        .withSchema("s", CATCHALL)
        .query("select * from \"s\".\"primes\"")
        .returnsUnordered("value=1", "value=3", "value=7");
  }

  @Test void testCustomBoxedScalar() {
    CalciteAssert.that()
        .withSchema("s", CATCHALL)
        .query("select \"value\" from \"s\".\"primesCustomBoxed\"")
        .returnsUnordered("value=1", "value=3", "value=5");
  }

  @Test void testCustomBoxedSalarCalc() {
    CalciteAssert.that()
        .withSchema("s", CATCHALL)
        .query("select \"value\"*2 \"value\" from \"s\".\"primesCustomBoxed\"")
        .returnsUnordered("value=2", "value=6", "value=10");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1569">[CALCITE-1569]
   * Date condition can generates Integer == Integer, which is always
   * false</a>. */
  @Test void testDateCanCompare() {
    final String sql = "select a.v\n"
        + "from (select \"sqlDate\" v\n"
        + "  from \"s\".\"everyTypes\" "
        + "  group by \"sqlDate\") a,"
        + "    (select \"sqlDate\" v\n"
        + "  from \"s\".\"everyTypes\"\n"
        + "  group by \"sqlDate\") b\n"
        + "where a.v >= b.v\n"
        + "group by a.v";
    CalciteAssert.that()
        .withSchema("s", CATCHALL)
        .query(sql)
        .returnsUnordered("V=1970-01-01");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3512">[CALCITE-3512]
   * Query fails when comparing Time/TimeStamp types</a>. */
  @Test void testTimeCanCompare() {
    final String sql = "select a.v\n"
        + "from (select \"sqlTime\" v\n"
        + "  from \"s\".\"everyTypes\" "
        + "  group by \"sqlTime\") a,"
        + "    (select \"sqlTime\" v\n"
        + "  from \"s\".\"everyTypes\"\n"
        + "  group by \"sqlTime\") b\n"
        + "where a.v >= b.v\n"
        + "group by a.v";
    CalciteAssert.that()
        .withSchema("s", CATCHALL)
        .query(sql)
        .returnsUnordered("V=00:00:00");
  }

  @Test void testTimestampCanCompare() {
    final String sql = "select a.v\n"
        + "from (select \"sqlTimestamp\" v\n"
        + "  from \"s\".\"everyTypes\" "
        + "  group by \"sqlTimestamp\") a,"
        + "    (select \"sqlTimestamp\" v\n"
        + "  from \"s\".\"everyTypes\"\n"
        + "  group by \"sqlTimestamp\") b\n"
        + "where a.v >= b.v\n"
        + "group by a.v";
    CalciteAssert.that()
        .withSchema("s", CATCHALL)
        .query(sql)
        .returnsUnordered("V=1970-01-01 00:00:00");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1919">[CALCITE-1919]
   * NPE when target in ReflectiveSchema belongs to the unnamed package</a>. */
  @Test void testReflectiveSchemaInUnnamedPackage() throws Exception {
    final Driver driver = new Driver();
    try (CalciteConnection connection = (CalciteConnection)
        driver.connect("jdbc:calcite:", new Properties())) {
      SchemaPlus rootSchema = connection.getRootSchema();
      final Class<?> c = Class.forName("RootHr");
      final Object o = c.getDeclaredConstructor().newInstance();
      rootSchema.add("hr", new ReflectiveSchema(o));
      connection.setSchema("hr");
      final Statement statement = connection.createStatement();
      final String sql = "select * from \"emps\"";
      final ResultSet resultSet = statement.executeQuery(sql);
      final String expected = "empid=100; name=Bill\n"
          + "empid=200; name=Eric\n"
          + "empid=150; name=Sebastian\n";
      assertThat(CalciteAssert.toString(resultSet), is(expected));
    }
  }

  /** Extension to {@link Employee} with a {@code hireDate} column. */
  public static class EmployeeWithHireDate extends Employee {
    public final java.sql.Date hireDate;

    public EmployeeWithHireDate(
        int empid, int deptno, String name, float salary, Integer commission,
        java.sql.Date hireDate) {
      super(empid, deptno, name, salary, commission);
      this.hireDate = hireDate;
    }
  }

  /** Schema that contains a table with a date column. */
  public static class DateColumnSchema {
    public final EmployeeWithHireDate[] emps = {
        new EmployeeWithHireDate(
            10, 20, "fred", 0f, null, new java.sql.Date(0)), // 1970-1-1
        new EmployeeWithHireDate(
            10, 20, "bill", 0f, null,
            new java.sql.Date(100 * DateTimeUtils.MILLIS_PER_DAY)) // 1970-04-11
    };
  }

  /** Tests
   * <a href="https://issues.apache.org/jira/browse/CALCITE-2611">[CALCITE-2611]
   * UNKNOWN on one side of an OR may lead to uncompilable code</a>. */
  @Test void testUnknownInOr() {
    CalciteAssert.that()
        .withSchema("s", CATCHALL)
        .query("select (\"value\" = 3 and unknown) or ( \"value\"  = 3 ) "
            + "from \"s\".\"primesCustomBoxed\"")
        .returnsUnordered("EXPR$0=false\nEXPR$0=false\nEXPR$0=true");
  }

  @Test void testDecimalNegate() {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().withSchema("s", CATCHALL);
    with.query("select - \"bigDecimal\" from \"s\".\"everyTypes\"")
        .planContains("negate()")
        .returnsUnordered(
            "EXPR$0=0",
            "EXPR$0=null");
  }
}
