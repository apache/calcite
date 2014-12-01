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
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Predicate1;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.TableMacroImpl;
import org.apache.calcite.schema.impl.ViewTable;
import org.apache.calcite.util.DateTimeUtil;

import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.apache.calcite.test.JdbcTest.Employee;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ReflectiveSchema}.
 */
public class ReflectiveSchemaTest {
  public static final Method LINQ4J_AS_ENUMERABLE_METHOD =
      Types.lookupMethod(
          Linq4j.class, "asEnumerable", Object[].class);

  /**
   * Test that uses a JDBC connection as a linq4j
   * {@link org.apache.calcite.linq4j.QueryProvider}.
   *
   * @throws Exception on error
   */
  @Test public void testQueryProvider() throws Exception {
    Connection connection = CalciteAssert.getConnection("hr", "foodmart");
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
                        new JdbcTest.HrSchema().emps)),
                "asQueryable"),
            Employee.class)
            .where(
                Expressions.<Predicate1<Employee>>lambda(
                    Expressions.lessThan(
                        Expressions.field(
                            e, "empid"),
                        Expressions.constant(160)),
                    e))
            .where(
                Expressions.<Predicate1<Employee>>lambda(
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

  @Test public void testQueryProviderSingleColumn() throws Exception {
    Connection connection = CalciteAssert.getConnection("hr", "foodmart");
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
                    Arrays.<Expression>asList(
                        Expressions.constant(
                            new JdbcTest.HrSchema().emps))), "asQueryable"),
            Employee.class)
            .select(Expressions.<Function1<Employee, Integer>>lambda(
                Expressions.field(e, "empid"),
                e))
            .toList();
    assertEquals(Arrays.asList(100, 200, 150, 110), list);
  }

  /**
   * Tests a relation that is accessed via method syntax.
   * The function returns a {@link org.apache.calcite.linq4j.Queryable}.
   */
  @Ignore
  @Test public void testOperator() throws SQLException, ClassNotFoundException {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    schema.add("GenerateStrings",
        TableMacroImpl.create(JdbcTest.GENERATE_STRINGS_METHOD));
    schema.add("StringUnion",
        TableMacroImpl.create(JdbcTest.STRING_UNION_METHOD));
    rootSchema.add("hr", new ReflectiveSchema(new JdbcTest.HrSchema()));
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
  @Test public void testView() throws SQLException, ClassNotFoundException {
    Connection connection =
        DriverManager.getConnection("jdbc:calcite:");
    CalciteConnection calciteConnection =
        connection.unwrap(CalciteConnection.class);
    SchemaPlus rootSchema = calciteConnection.getRootSchema();
    SchemaPlus schema = rootSchema.add("s", new AbstractSchema());
    schema.add("emps_view",
        ViewTable.viewMacro(schema,
            "select * from \"hr\".\"emps\" where \"deptno\" = 10",
            null));
    rootSchema.add("hr", new ReflectiveSchema(new JdbcTest.HrSchema()));
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
  @Test public void testViewPath() throws SQLException, ClassNotFoundException {
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
            Collections.singletonList("hr")));
    schema.add("hr_emps",
        ViewTable.viewMacro(schema,
            "select * from \"emps\"",
            Collections.singletonList("hr")));
    schema.add("s_emps",
        ViewTable.viewMacro(schema,
            "select * from \"emps\"",
            Collections.singletonList("s")));
    schema.add("null_emps",
        ViewTable.viewMacro(schema, "select * from \"emps\"", null));
    rootSchema.add("hr", new ReflectiveSchema(new JdbcTest.HrSchema()));
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
  @Test public void testDateColumn() throws Exception {
    CalciteAssert.that()
        .with("s", new DateColumnSchema())
        .query("select * from \"s\".\"emps\"")
        .returns(""
            + "hireDate=1970-01-01; empid=10; deptno=20; name=fred; salary=0.0; commission=null\n"
            + "hireDate=1970-04-11; empid=10; deptno=20; name=bill; salary=0.0; commission=null\n");
  }

  /** Tests querying an object that has no public fields. */
  @Test public void testNoPublicFields() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with("s", new CatchallSchema());
    with.query("select 1 from \"s\".\"allPrivates\"")
        .returns("EXPR$0=1\n");
    with.query("select \"x\" from \"s\".\"allPrivates\"")
        .throws_("Column 'x' not found in any table");
  }

  /** Tests columns based on types such as java.sql.Date and java.util.Date.
   *
   * @see CatchallSchema#everyTypes */
  @Test public void testColumnTypes() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with("s", new CatchallSchema());
    with.query("select \"primitiveBoolean\" from \"s\".\"everyTypes\"")
        .returns("primitiveBoolean=false\n"
            + "primitiveBoolean=true\n");
    with.query("select * from \"s\".\"everyTypes\"")
        .returns(""
            + "primitiveBoolean=false; primitiveByte=0; primitiveChar=\u0000; primitiveShort=0; primitiveInt=0; primitiveLong=0; primitiveFloat=0.0; primitiveDouble=0.0; wrapperBoolean=false; wrapperByte=0; wrapperCharacter=\u0000; wrapperShort=0; wrapperInteger=0; wrapperLong=0; wrapperFloat=0.0; wrapperDouble=0.0; sqlDate=1970-01-01; sqlTime=00:00:00; sqlTimestamp=1970-01-01 00:00:00; utilDate=1970-01-01 00:00:00; string=1\n"
            + "primitiveBoolean=true; primitiveByte=127; primitiveChar=\uffff; primitiveShort=32767; primitiveInt=2147483647; primitiveLong=9223372036854775807; primitiveFloat=3.4028235E38; primitiveDouble=1.7976931348623157E308; wrapperBoolean=null; wrapperByte=null; wrapperCharacter=null; wrapperShort=null; wrapperInteger=null; wrapperLong=null; wrapperFloat=null; wrapperDouble=null; sqlDate=null; sqlTime=null; sqlTimestamp=null; utilDate=null; string=null\n");
  }

  /**
   * Tests NOT for nullable columns
   * @see CatchallSchema#everyTypes */
  @Test public void testWhereNOT() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with("s", new CatchallSchema());
    with.query(
        "select \"wrapperByte\" from \"s\".\"everyTypes\" where NOT (\"wrapperByte\" is null)")
        .returnsUnordered("wrapperByte=0");
  }

  /**
   * Tests NOT for nullable columns
   * @see CatchallSchema#everyTypes */
  @Test public void testSelectNOT() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with("s", new CatchallSchema());
    with.query(
        "select NOT \"wrapperBoolean\" \"value\" from \"s\".\"everyTypes\"")
        .returnsUnordered(
            "value=null",
            "value=true");
  }

  /** Tests columns based on types such as java.sql.Date and java.util.Date.
   *
   * @see CatchallSchema#everyTypes */
  @Test public void testAggregateFunctions() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that()
            .with("s", new CatchallSchema());
    checkAgg(with, "min");
    checkAgg(with, "max");
    checkAgg(with, "avg");
    checkAgg(with, "count");
  }

  private void checkAgg(CalciteAssert.AssertThat with, String fn) {
    for (Field field
        : fn.equals("avg") ? EveryType.numericFields() : EveryType.fields()) {
      with.query("select " + fn + "(\"" + field.getName() + "\") as c\n"
          + "from \"s\".\"everyTypes\"")
          .returns(CalciteAssert.<ResultSet, Void>constantNull());
    }
  }

  @Test public void testJavaBoolean() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with("s", new CatchallSchema());
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"primitiveBoolean\"")
        .returns("C=1\n");
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"wrapperBoolean\"")
        .returns("C=0\n");
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"wrapperBoolean\" is not true")
        .returns("C=2\n");
    with.query("select count(*) as c from \"s\".\"everyTypes\"\n"
        + "where \"primitiveInt\" > 0")
        .returns("C=1\n");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-119">CALCITE-119</a>,
   * "Comparing a Java type long with a SQL type INTEGER gives wrong answer". */
  @Test public void testCompareJavaAndSqlTypes() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with("s", new CatchallSchema());
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

  @Test public void testDivideWraperPrimitive() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with("s", new CatchallSchema());
    with.query("select \"wrapperLong\" / \"primitiveLong\" as c\n"
        + " from \"s\".\"everyTypes\" where \"primitiveLong\" <> 0")
        .planContains(
            "final Long inp13_ = current.wrapperLong;")
        .planContains(
            "return inp13_ == null ? (Long) null : Long.valueOf(inp13_.longValue() / current.primitiveLong);")
        .returns("C=null\n");
  }

  @Test public void testDivideWraperWrapper() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with("s", new CatchallSchema());
    with.query("select \"wrapperLong\" / \"wrapperLong\" as c\n"
        + " from \"s\".\"everyTypes\" where \"primitiveLong\" <> 0")
        .planContains(
            "final Long inp13_ = ((org.apache.calcite.test.ReflectiveSchemaTest.EveryType) inputEnumerator.current()).wrapperLong;")
        .planContains(
            "return inp13_ == null ? (Long) null : Long.valueOf(inp13_.longValue() / inp13_.longValue());")
        .returns("C=null\n");
  }

  @Test public void testDivideWraperWrapperMultipleTimes() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that().with("s", new CatchallSchema());
    with.query("select \"wrapperLong\" / \"wrapperLong\"\n"
        + "+ \"wrapperLong\" / \"wrapperLong\" as c\n"
        + " from \"s\".\"everyTypes\" where \"primitiveLong\" <> 0")
        .planContains(
            "final Long inp13_ = ((org.apache.calcite.test.ReflectiveSchemaTest.EveryType) inputEnumerator.current()).wrapperLong;")
        .planContains(
            "return inp13_ == null ? (Long) null : Long.valueOf(inp13_.longValue() / inp13_.longValue() + inp13_.longValue() / inp13_.longValue());")
        .returns("C=null\n");
  }

  @Test public void testOp() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that()
            .with("s", new CatchallSchema());
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
            .returns(CalciteAssert.<ResultSet, Void>constantNull());
      }
    }
  }

  @Test public void testCastFromString() {
    CalciteAssert.that()
        .with("s", new CatchallSchema())
        .query("select cast(\"string\" as int) as c from \"s\".\"everyTypes\"")
        .returns("C=1\n"
            + "C=null\n");
  }

  private static boolean isNumeric(Class type) {
    switch (Primitive.flavor(type)) {
    case BOX:
      return Primitive.ofBox(type).isNumeric();
    case PRIMITIVE:
      return Primitive.of(type).isNumeric();
    default:
      return Number.class.isAssignableFrom(type); // e.g. BigDecimal
    }
  }

  /** Tests that if a field of a relation has an unrecognized type (in this
   * case a {@link BitSet}) then it is treated as an object.
   *
   * @see CatchallSchema#badTypes */
  @Test public void testTableFieldHasBadType() throws Exception {
    CalciteAssert.that()
        .with("s", new CatchallSchema())
        .query("select * from \"s\".\"badTypes\"")
        .returns("integer=0; bitSet={}\n");
  }

  /** Tests that a schema with a field whose type cannot be recognized
   * throws an informative exception.
   *
   * @see CatchallSchema#enumerable
   * @see CatchallSchema#list */
  @Test public void testSchemaFieldHasBadType() throws Exception {
    final CalciteAssert.AssertThat with =
        CalciteAssert.that()
            .with("s", new CatchallSchema());
    // BitSet is not a valid relation type. It's as if "bitSet" field does
    // not exist.
    with.query("select * from \"s\".\"bitSet\"")
        .throws_("Table 's.bitSet' not found");
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
  @Test public void testPrefix() throws Exception {
    CalciteAssert.that()
        .with("s", new CatchallSchema())
        .query(
            "select * from \"s\".\"prefixEmps\" where \"name\" in ('Ab', 'Abd')")
        .returns("empid=2; deptno=10; name=Ab; salary=0.0; commission=null\n"
            + "empid=4; deptno=10; name=Abd; salary=0.0; commission=null\n");
  }

  /** If a method returns a
   * {@link ViewTable}.{@code ViewTableMacro}, then it
   * should be expanded. */
  @Ignore
  @Test public void testTableMacroIsView() throws Exception {
    CalciteAssert.that()
        .with("s", new JdbcTest.HrSchema())
        .query("select * from table(\"s\".\"view\"('abc'))")
        .returns("empid=2; deptno=10; name=Ab; salary=0.0; commission=null\n"
            + "empid=4; deptno=10; name=Abd; salary=0.0; commission=null\n");
  }

  /** Finds a table-macro using reflection. */
  @Ignore
  @Test public void testTableMacro() throws Exception {
    CalciteAssert.that()
        .with("s", new JdbcTest.HrSchema())
        .query("select * from table(\"s\".\"foo\"(3))")
        .returns("empid=2; deptno=10; name=Ab; salary=0.0; commission=null\n"
            + "empid=4; deptno=10; name=Abd; salary=0.0; commission=null\n");
  }

  /** Table with single field as Integer[] */
  @Ignore(
      "java.lang.AssertionError RelDataTypeImpl.getFieldList(RelDataTypeImpl.java:99)")
  @Test public void testArrayOfBoxedPrimitives() {
    CalciteAssert.that()
        .with("s", new CatchallSchema())
        .query("select * from \"s\".\"primesBoxed\"")
        .returnsUnordered("value=1", "value=3", "value=7");
  }

  /** Table with single field as int[] */
  @Ignore(
      "java.lang.AssertionError RelDataTypeImpl.getFieldList(RelDataTypeImpl.java:99)")
  @Test public void testArrayOfPrimitives() {
    CalciteAssert.that()
        .with("s", new CatchallSchema())
        .query("select * from \"s\".\"primes\"")
        .returnsUnordered("value=1", "value=3", "value=7");
  }

  @Test public void testCustomBoxedScalar() {
    CalciteAssert.that()
        .with("s", new ReflectiveSchemaTest.CatchallSchema())
        .query("select \"value\" from \"s\".\"primesCustomBoxed\"")
        .returnsUnordered("value=1", "value=3", "value=5");
  }

  @Test public void testCustomBoxedSalarCalc() {
    CalciteAssert.that()
        .with("s", new ReflectiveSchemaTest.CatchallSchema())
        .query("select \"value\"*2 \"value\" from \"s\".\"primesCustomBoxed\"")
        .returnsUnordered("value=2", "value=6", "value=10");
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

  /** Record that has a field of every interesting type. */
  public static class EveryType {
    public final boolean primitiveBoolean;
    public final byte primitiveByte;
    public final char primitiveChar;
    public final short primitiveShort;
    public final int primitiveInt;
    public final long primitiveLong;
    public final float primitiveFloat;
    public final double primitiveDouble;
    public final Boolean wrapperBoolean;
    public final Byte wrapperByte;
    public final Character wrapperCharacter;
    public final Short wrapperShort;
    public final Integer wrapperInteger;
    public final Long wrapperLong;
    public final Float wrapperFloat;
    public final Double wrapperDouble;
    public final java.sql.Date sqlDate;
    public final Time sqlTime;
    public final Timestamp sqlTimestamp;
    public final Date utilDate;
    public final String string;

    public EveryType(
        boolean primitiveBoolean,
        byte primitiveByte,
        char primitiveChar,
        short primitiveShort,
        int primitiveInt,
        long primitiveLong,
        float primitiveFloat,
        double primitiveDouble,
        Boolean wrapperBoolean,
        Byte wrapperByte,
        Character wrapperCharacter,
        Short wrapperShort,
        Integer wrapperInteger,
        Long wrapperLong,
        Float wrapperFloat,
        Double wrapperDouble,
        java.sql.Date sqlDate,
        Time sqlTime,
        Timestamp sqlTimestamp,
        Date utilDate,
        String string) {
      this.primitiveBoolean = primitiveBoolean;
      this.primitiveByte = primitiveByte;
      this.primitiveChar = primitiveChar;
      this.primitiveShort = primitiveShort;
      this.primitiveInt = primitiveInt;
      this.primitiveLong = primitiveLong;
      this.primitiveFloat = primitiveFloat;
      this.primitiveDouble = primitiveDouble;
      this.wrapperBoolean = wrapperBoolean;
      this.wrapperByte = wrapperByte;
      this.wrapperCharacter = wrapperCharacter;
      this.wrapperShort = wrapperShort;
      this.wrapperInteger = wrapperInteger;
      this.wrapperLong = wrapperLong;
      this.wrapperFloat = wrapperFloat;
      this.wrapperDouble = wrapperDouble;
      this.sqlDate = sqlDate;
      this.sqlTime = sqlTime;
      this.sqlTimestamp = sqlTimestamp;
      this.utilDate = utilDate;
      this.string = string;
    }

    static Enumerable<Field> fields() {
      return Linq4j.asEnumerable(EveryType.class.getFields());
    }

    static Enumerable<Field> numericFields() {
      return fields()
          .where(
              new Predicate1<Field>() {
                public boolean apply(Field v1) {
                  return isNumeric(v1.getType());
                }
              });
    }
  }

  /** All field are private, therefore the resulting record has no fields. */
  public static class AllPrivate {
    private final int x = 0;
  }

  /** Table that has a field that cannot be recognized as a SQL type. */
  public static class BadType {
    public final int integer = 0;
    public final BitSet bitSet = new BitSet(0);
  }

  /** Object whose fields are relations. Called "catch-all" because it's OK
   * if tests add new fields. */
  public static class CatchallSchema {
    public final Enumerable<Employee> enumerable =
        Linq4j.asEnumerable(
            Arrays.asList(new JdbcTest.HrSchema().emps));

    public final List<Employee> list =
        Arrays.asList(new JdbcTest.HrSchema().emps);

    public final BitSet bitSet = new BitSet(1);

    public final EveryType[] everyTypes = {
      new EveryType(
          false, (byte) 0, (char) 0, (short) 0, 0, 0L, 0F, 0D,
          false, (byte) 0, (char) 0, (short) 0, 0, 0L, 0F, 0D,
          new java.sql.Date(0), new Time(0), new Timestamp(0),
          new Date(0), "1"),
      new EveryType(
          true, Byte.MAX_VALUE, Character.MAX_VALUE, Short.MAX_VALUE,
          Integer.MAX_VALUE, Long.MAX_VALUE, Float.MAX_VALUE,
          Double.MAX_VALUE,
          null, null, null, null, null, null, null, null,
          null, null, null, null, null),
    };

    public final AllPrivate[] allPrivates = { new AllPrivate() };

    public final BadType[] badTypes = { new BadType() };

    public final Employee[] prefixEmps = {
      new Employee(1, 10, "A", 0f, null),
      new Employee(2, 10, "Ab", 0f, null),
      new Employee(3, 10, "Abc", 0f, null),
      new Employee(4, 10, "Abd", 0f, null),
    };

    public final Integer[] primesBoxed = new Integer[]{1, 3, 5};

    public final int[] primes = new int[]{1, 3, 5};

    public final IntHolder[] primesCustomBoxed =
        new IntHolder[]{new IntHolder(1), new IntHolder(3), new IntHolder(5)};
  }

  /**
   * Custom java class that holds just a single field.
   */
  public static class IntHolder {
    public final int value;

    public IntHolder(int value) {
      this.value = value;
    }
  }

  /** Schema that contains a table with a date column. */
  public static class DateColumnSchema {
    public final EmployeeWithHireDate[] emps = {
      new EmployeeWithHireDate(
          10, 20, "fred", 0f, null, new java.sql.Date(0)), // 1970-1-1
      new EmployeeWithHireDate(
            10, 20, "bill", 0f, null,
            new java.sql.Date(100 * DateTimeUtil.MILLIS_PER_DAY)) // 1970-04-11
    };
  }
}

// End ReflectiveSchemaTest.java
