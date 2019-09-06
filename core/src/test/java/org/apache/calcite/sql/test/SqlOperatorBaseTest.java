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
package org.apache.calcite.sql.test;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.SqlLimitsTest;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.google.common.base.Throwables;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Contains unit tests for all operators. Each of the methods is named after an
 * operator.
 *
 * <p>The class is abstract. It contains a test for every operator, but does not
 * provide a mechanism to execute the tests: parse, validate, and execute
 * expressions on the operators. This is left to a {@link SqlTester} object
 * which the derived class must provide.</p>
 *
 * <p>Different implementations of {@link SqlTester} are possible, such as:</p>
 *
 * <ul>
 * <li>Execute against a real farrago database
 * <li>Execute in pure java (parsing and validation can be done, but expression
 * evaluation is not possible)
 * <li>Generate a SQL script.
 * <li>Analyze which operators are adequately tested.
 * </ul>
 *
 * <p>A typical method will be named after the operator it is testing (say
 * <code>testSubstringFunc</code>). It first calls
 * {@link SqlTester#setFor(org.apache.calcite.sql.SqlOperator, org.apache.calcite.sql.test.SqlTester.VmName...)}
 * to declare which operator it is testing.
 *
 * <blockquote>
 * <pre><code>
 * public void testSubstringFunc() {
 *     tester.setFor(SqlStdOperatorTable.substringFunc);
 *     tester.checkScalar("sin(0)", "0");
 *     tester.checkScalar("sin(1.5707)", "1");
 * }</code></pre>
 * </blockquote>
 *
 * <p>The rest of the method contains calls to the various {@code checkXxx}
 * methods in the {@link SqlTester} interface. For an operator
 * to be adequately tested, there need to be tests for:
 *
 * <ul>
 * <li>Parsing all of its the syntactic variants.
 * <li>Deriving the type of in all combinations of arguments.
 *
 * <ul>
 * <li>Pay particular attention to nullability. For example, the result of the
 * "+" operator is NOT NULL if and only if both of its arguments are NOT
 * NULL.</li>
 * <li>Also pay attention to precision/scale/length. For example, the maximum
 * length of the "||" operator is the sum of the maximum lengths of its
 * arguments.</li>
 * </ul>
 * </li>
 * <li>Executing the function. Pay particular attention to corner cases such as
 * null arguments or null results.</li>
 * </ul>
 */
public abstract class SqlOperatorBaseTest {
  //~ Static fields/initializers ---------------------------------------------

  // TODO: Change message when Fnl3Fixed to something like
  // "Invalid character for cast: PC=0 Code=22018"
  public static final String INVALID_CHAR_MESSAGE =
      Bug.FNL3_FIXED ? null : "(?s).*";

  // TODO: Change message when Fnl3Fixed to something like
  // "Overflow during calculation or cast: PC=0 Code=22003"
  public static final String OUT_OF_RANGE_MESSAGE =
      Bug.FNL3_FIXED ? null : "(?s).*";

  // TODO: Change message when Fnl3Fixed to something like
  // "Division by zero: PC=0 Code=22012"
  public static final String DIVISION_BY_ZERO_MESSAGE =
      Bug.FNL3_FIXED ? null : "(?s).*";

  // TODO: Change message when Fnl3Fixed to something like
  // "String right truncation: PC=0 Code=22001"
  public static final String STRING_TRUNC_MESSAGE =
      Bug.FNL3_FIXED ? null : "(?s).*";

  // TODO: Change message when Fnl3Fixed to something like
  // "Invalid datetime format: PC=0 Code=22007"
  public static final String BAD_DATETIME_MESSAGE =
      Bug.FNL3_FIXED ? null : "(?s).*";

  // Error messages when an invalid time unit is given as
  // input to extract for a particular input type.
  public static final String INVALID_EXTRACT_UNIT_CONVERTLET_ERROR =
      "Extract.*from.*type data is not supported";

  public static final String INVALID_EXTRACT_UNIT_VALIDATION_ERROR =
      "Cannot apply 'EXTRACT' to arguments of type .*'\n.*";

  public static final String LITERAL_OUT_OF_RANGE_MESSAGE =
      "(?s).*Numeric literal.*out of range.*";

  public static final boolean TODO = false;

  /**
   * Regular expression for a SQL TIME(0) value.
   */
  public static final Pattern TIME_PATTERN =
      Pattern.compile(
          "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");

  /**
   * Regular expression for a SQL TIMESTAMP(0) value.
   */
  public static final Pattern TIMESTAMP_PATTERN =
      Pattern.compile(
          "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] "
              + "[0-9][0-9]:[0-9][0-9]:[0-9][0-9]");

  /**
   * Regular expression for a SQL DATE value.
   */
  public static final Pattern DATE_PATTERN =
      Pattern.compile(
          "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

  public static final String[] NUMERIC_TYPE_NAMES = {
      "TINYINT", "SMALLINT", "INTEGER", "BIGINT",
      "DECIMAL(5, 2)", "REAL", "FLOAT", "DOUBLE"
  };

  // REVIEW jvs 27-Apr-2006:  for Float and Double, MIN_VALUE
  // is the smallest positive value, not the smallest negative value
  public static final String[] MIN_NUMERIC_STRINGS = {
      Long.toString(Byte.MIN_VALUE),
      Long.toString(Short.MIN_VALUE),
      Long.toString(Integer.MIN_VALUE),
      Long.toString(Long.MIN_VALUE),
      "-999.99",

      // NOTE jvs 26-Apr-2006:  Win32 takes smaller values from win32_values.h
      "1E-37", /*Float.toString(Float.MIN_VALUE)*/
      "2E-307", /*Double.toString(Double.MIN_VALUE)*/
      "2E-307" /*Double.toString(Double.MIN_VALUE)*/,
  };

  public static final String[] MIN_OVERFLOW_NUMERIC_STRINGS = {
      Long.toString(Byte.MIN_VALUE - 1),
      Long.toString(Short.MIN_VALUE - 1),
      Long.toString((long) Integer.MIN_VALUE - 1),
      new BigDecimal(Long.MIN_VALUE).subtract(BigDecimal.ONE).toString(),
      "-1000.00",
      "1e-46",
      "1e-324",
      "1e-324"
  };

  public static final String[] MAX_NUMERIC_STRINGS = {
      Long.toString(Byte.MAX_VALUE),
      Long.toString(Short.MAX_VALUE),
      Long.toString(Integer.MAX_VALUE),
      Long.toString(Long.MAX_VALUE), "999.99",

      // NOTE jvs 26-Apr-2006:  use something slightly less than MAX_VALUE
      // because roundtripping string to approx to string doesn't preserve
      // MAX_VALUE on win32
      "3.4028234E38", /*Float.toString(Float.MAX_VALUE)*/
      "1.79769313486231E308", /*Double.toString(Double.MAX_VALUE)*/
      "1.79769313486231E308" /*Double.toString(Double.MAX_VALUE)*/
  };

  public static final String[] MAX_OVERFLOW_NUMERIC_STRINGS = {
      Long.toString(Byte.MAX_VALUE + 1),
      Long.toString(Short.MAX_VALUE + 1),
      Long.toString((long) Integer.MAX_VALUE + 1),
      (new BigDecimal(Long.MAX_VALUE)).add(BigDecimal.ONE).toString(),
      "1000.00",
      "1e39",
      "-1e309",
      "1e309"
  };
  private static final boolean[] FALSE_TRUE = {false, true};
  private static final SqlTester.VmName VM_FENNEL = SqlTester.VmName.FENNEL;
  private static final SqlTester.VmName VM_JAVA = SqlTester.VmName.JAVA;
  private static final SqlTester.VmName VM_EXPAND = SqlTester.VmName.EXPAND;
  protected static final TimeZone UTC_TZ = TimeZone.getTimeZone("GMT");
  // time zone for the LOCAL_{DATE,TIME,TIMESTAMP} functions
  protected static final TimeZone LOCAL_TZ = TimeZone.getDefault();
  // time zone for the CURRENT{DATE,TIME,TIMESTAMP} functions
  protected static final TimeZone CURRENT_TZ = LOCAL_TZ;

  private static final Pattern INVALID_ARG_FOR_POWER = Pattern.compile(
      "(?s).*Invalid argument\\(s\\) for 'POWER' function.*");

  private static final Pattern CODE_2201F = Pattern.compile(
      "(?s).*could not calculate results for the following row.*PC=5 Code=2201F.*");

  /**
   * Whether DECIMAL type is implemented.
   */
  public static final boolean DECIMAL = false;

  private final boolean enable;

  protected final SqlTester tester;

  // same with tester but without implicit type coercion.
  protected final SqlTester strictTester;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlOperatorBaseTest.
   *
   * @param enable Whether to run "failing" tests.
   * @param tester Means to validate, execute various statements.
   */
  protected SqlOperatorBaseTest(boolean enable, SqlTester tester) {
    this.enable = enable;
    this.tester = tester;
    assert tester != null;
    this.strictTester = tester.enableTypeCoercion(false);
  }

  //~ Methods ----------------------------------------------------------------

  @Before
  public void setUp() throws Exception {
    tester.setFor(null);
  }

  protected SqlTester oracleTester() {
    return tester.withOperatorTable(
            SqlLibraryOperatorTableFactory.INSTANCE
                .getOperatorTable(SqlLibrary.STANDARD, SqlLibrary.ORACLE))
        .withConnectionFactory(
            CalciteAssert.EMPTY_CONNECTION_FACTORY
                .with(new CalciteAssert
                    .AddSchemaSpecPostProcessor(CalciteAssert.SchemaSpec.HR))
                .with(CalciteConnectionProperty.FUN, "oracle"));
  }

  protected SqlTester oracleTester(SqlConformance conformance) {
    if (conformance == null) {
      conformance = SqlConformanceEnum.DEFAULT;
    }
    return tester
        .withConformance(conformance)
        .withOperatorTable(
            SqlLibraryOperatorTableFactory.INSTANCE
                .getOperatorTable(SqlLibrary.STANDARD, SqlLibrary.ORACLE))
        .withConnectionFactory(
            CalciteAssert.EMPTY_CONNECTION_FACTORY
                .with(new CalciteAssert
                    .AddSchemaSpecPostProcessor(CalciteAssert.SchemaSpec.HR))
                .with("fun", "oracle")
                .with("conformance", conformance));
  }

  /** Creates a tester with special sql library. */
  protected SqlTester tester(SqlLibrary library) {
    return tester.withOperatorTable(
        SqlLibraryOperatorTableFactory.INSTANCE
            .getOperatorTable(SqlLibrary.STANDARD, library))
        .withConnectionFactory(
            CalciteAssert.EMPTY_CONNECTION_FACTORY
                .with(new CalciteAssert
                    .AddSchemaSpecPostProcessor(CalciteAssert.SchemaSpec.HR))
                .with("fun", library.name()));
  }

  //--- Tests -----------------------------------------------------------

  /**
   * For development. Put any old code in here.
   */
  @Test public void testDummy() {
  }

  @Test public void testSqlOperatorOverloading() {
    final SqlStdOperatorTable operatorTable = SqlStdOperatorTable.instance();
    for (SqlOperator sqlOperator : operatorTable.getOperatorList()) {
      String operatorName = sqlOperator.getName();
      List<SqlOperator> routines = new ArrayList<>();
      final SqlIdentifier id =
          new SqlIdentifier(operatorName, SqlParserPos.ZERO);
      operatorTable.lookupOperatorOverloads(id, null, sqlOperator.getSyntax(),
          routines, SqlNameMatchers.withCaseSensitive(true));

      routines.removeIf(operator ->
          !sqlOperator.getClass().isInstance(operator));
      assertThat(routines.size(), equalTo(1));
      assertThat(sqlOperator, equalTo(routines.get(0)));
    }
  }

  @Test public void testBetween() {
    tester.setFor(
        SqlStdOperatorTable.BETWEEN,
        SqlTester.VmName.EXPAND);
    tester.checkBoolean("2 between 1 and 3", Boolean.TRUE);
    tester.checkBoolean("2 between 3 and 2", Boolean.FALSE);
    tester.checkBoolean("2 between symmetric 3 and 2", Boolean.TRUE);
    tester.checkBoolean("3 between 1 and 3", Boolean.TRUE);
    tester.checkBoolean("4 between 1 and 3", Boolean.FALSE);
    tester.checkBoolean("1 between 4 and -3", Boolean.FALSE);
    tester.checkBoolean("1 between -1 and -3", Boolean.FALSE);
    tester.checkBoolean("1 between -1 and 3", Boolean.TRUE);
    tester.checkBoolean("1 between 1 and 1", Boolean.TRUE);
    tester.checkBoolean("1.5 between 1 and 3", Boolean.TRUE);
    tester.checkBoolean("1.2 between 1.1 and 1.3", Boolean.TRUE);
    tester.checkBoolean("1.5 between 2 and 3", Boolean.FALSE);
    tester.checkBoolean("1.5 between 1.6 and 1.7", Boolean.FALSE);
    tester.checkBoolean("1.2e1 between 1.1 and 1.3", Boolean.FALSE);
    tester.checkBoolean("1.2e0 between 1.1 and 1.3", Boolean.TRUE);
    tester.checkBoolean("1.5e0 between 2 and 3", Boolean.FALSE);
    tester.checkBoolean("1.5e0 between 2e0 and 3e0", Boolean.FALSE);
    tester.checkBoolean(
        "1.5e1 between 1.6e1 and 1.7e1",
        Boolean.FALSE);
    tester.checkBoolean("x'' between x'' and x''", Boolean.TRUE);
    tester.checkNull("cast(null as integer) between -1 and 2");
    tester.checkNull("1 between -1 and cast(null as integer)");
    tester.checkNull(
        "1 between cast(null as integer) and cast(null as integer)");
    tester.checkNull("1 between cast(null as integer) and 1");
    tester.checkBoolean("x'0A00015A' between x'0A000130' and x'0A0001B0'", Boolean.TRUE);
    tester.checkBoolean("x'0A00015A' between x'0A0001A0' and x'0A0001B0'", Boolean.FALSE);
  }

  @Test public void testNotBetween() {
    tester.setFor(SqlStdOperatorTable.NOT_BETWEEN, VM_EXPAND);
    tester.checkBoolean("2 not between 1 and 3", Boolean.FALSE);
    tester.checkBoolean("3 not between 1 and 3", Boolean.FALSE);
    tester.checkBoolean("4 not between 1 and 3", Boolean.TRUE);
    tester.checkBoolean(
        "1.2e0 not between 1.1 and 1.3",
        Boolean.FALSE);
    tester.checkBoolean("1.2e1 not between 1.1 and 1.3", Boolean.TRUE);
    tester.checkBoolean("1.5e0 not between 2 and 3", Boolean.TRUE);
    tester.checkBoolean("1.5e0 not between 2e0 and 3e0", Boolean.TRUE);
    tester.checkBoolean("x'0A00015A' not between x'0A000130' and x'0A0001B0'", Boolean.FALSE);
    tester.checkBoolean("x'0A00015A' not between x'0A0001A0' and x'0A0001B0'", Boolean.TRUE);
  }

  private String getCastString(
      String value,
      String targetType,
      boolean errorLoc) {
    if (errorLoc) {
      value = "^" + value + "^";
    }
    return "cast(" + value + " as " + targetType + ")";
  }

  private void checkCastToApproxOkay(
      String value,
      String targetType,
      double expected,
      double delta) {
    tester.checkScalarApprox(
        getCastString(value, targetType, false),
        targetType + " NOT NULL",
        expected,
        delta);
  }

  private void checkCastToStringOkay(
      String value,
      String targetType,
      String expected) {
    tester.checkString(
        getCastString(value, targetType, false),
        expected,
        targetType + " NOT NULL");
  }

  private void checkCastToScalarOkay(
      String value,
      String targetType,
      String expected) {
    tester.checkScalarExact(
        getCastString(value, targetType, false),
        targetType + " NOT NULL",
        expected);
  }

  private void checkCastToScalarOkay(String value, String targetType) {
    checkCastToScalarOkay(value, targetType, value);
  }

  private void checkCastFails(
      String value,
      String targetType,
      String expectedError,
      boolean runtime) {
    tester.checkFails(
        getCastString(value, targetType, !runtime),
        expectedError,
        runtime);
  }

  private void checkCastToString(String value, String type, String expected) {
    String spaces = "     ";
    if (expected == null) {
      expected = value.trim();
    }
    int len = expected.length();
    if (type != null) {
      value = getCastString(value, type, false);
    }

    // currently no exception thrown for truncation
    if (Bug.DT239_FIXED) {
      checkCastFails(
          value,
          "VARCHAR(" + (len - 1) + ")", STRING_TRUNC_MESSAGE,
          true);
    }

    checkCastToStringOkay(value, "VARCHAR(" + len + ")", expected);
    checkCastToStringOkay(value, "VARCHAR(" + (len + 5) + ")", expected);

    // currently no exception thrown for truncation
    if (Bug.DT239_FIXED) {
      checkCastFails(
          value,
          "CHAR(" + (len - 1) + ")", STRING_TRUNC_MESSAGE,
          true);
    }

    checkCastToStringOkay(
        value,
        "CHAR(" + len + ")",
        expected);
    checkCastToStringOkay(
        value,
        "CHAR(" + (len + 5) + ")",
        expected + spaces);
  }

  @Test public void testCastToString() {
    tester.setFor(SqlStdOperatorTable.CAST);
    checkCastToString("cast(cast('abc' as char(4)) as varchar(6))", null,
        "abc ");

    // integer
    checkCastToString("123", "CHAR(3)", "123");
    checkCastToString("0", "CHAR", "0");
    checkCastToString("-123", "CHAR(4)", "-123");

    // decimal
    checkCastToString("123.4", "CHAR(5)", "123.4");
    checkCastToString("-0.0", "CHAR(2)", ".0");
    checkCastToString("-123.4", "CHAR(6)", "-123.4");

    tester.checkString(
        "cast(1.29 as varchar(10))",
        "1.29",
        "VARCHAR(10) NOT NULL");
    tester.checkString(
        "cast(.48 as varchar(10))",
        ".48",
        "VARCHAR(10) NOT NULL");
    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails(
          "cast(2.523 as char(2))", STRING_TRUNC_MESSAGE,
          true);
    }

    tester.checkString(
        "cast(-0.29 as varchar(10))",
        "-.29",
        "VARCHAR(10) NOT NULL");
    tester.checkString(
        "cast(-1.29 as varchar(10))",
        "-1.29",
        "VARCHAR(10) NOT NULL");

    // approximate
    checkCastToString("1.23E45", "CHAR(7)", "1.23E45");
    checkCastToString("CAST(0 AS DOUBLE)", "CHAR(3)", "0E0");
    checkCastToString("-1.20e-07", "CHAR(7)", "-1.2E-7");
    checkCastToString("cast(0e0 as varchar(5))", "CHAR(3)", "0E0");
    if (TODO) {
      checkCastToString(
          "cast(-45e-2 as varchar(17))", "CHAR(7)",
          "-4.5E-1");
    }
    if (TODO) {
      checkCastToString(
          "cast(4683442.3432498375e0 as varchar(20))",
          "CHAR(19)",
          "4.683442343249838E6");
    }
    if (TODO) {
      checkCastToString("cast(-0.1 as real)", "CHAR(5)", "-1E-1");
    }
    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails(
          "cast(1.3243232e0 as varchar(4))", STRING_TRUNC_MESSAGE,
          true);
      tester.checkFails(
          "cast(1.9e5 as char(4))", STRING_TRUNC_MESSAGE,
          true);
    }

    // string
    checkCastToString("'abc'", "CHAR(1)", "a");
    checkCastToString("'abc'", "CHAR(3)", "abc");
    checkCastToString("cast('abc' as varchar(6))", "CHAR(3)", "abc");
    checkCastToString("cast(' abc  ' as varchar(10))", null, " abc  ");
    checkCastToString("cast(cast('abc' as char(4)) as varchar(6))", null,
        "abc ");
    tester.checkString("cast(cast('a' as char(2)) as varchar(3)) || 'x' ",
        "a x", "VARCHAR(4) NOT NULL");
    tester.checkString("cast(cast('a' as char(3)) as varchar(5)) || 'x' ",
        "a  x", "VARCHAR(6) NOT NULL");
    tester.checkString("cast('a' as char(3)) || 'x'", "a  x",
        "CHAR(4) NOT NULL");

    tester.checkScalar("char_length(cast(' x ' as char(4)))", 4,
        "INTEGER NOT NULL");
    tester.checkScalar("char_length(cast(' x ' as varchar(3)))", 3,
        "INTEGER NOT NULL");
    tester.checkScalar("char_length(cast(' x ' as varchar(4)))", 3,
        "INTEGER NOT NULL");
    tester.checkScalar("char_length(cast(cast(' x ' as char(4)) as varchar(5)))",
        4, "INTEGER NOT NULL");
    tester.checkScalar("char_length(cast(' x ' as varchar(3)))", 3,
        "INTEGER NOT NULL");

    // date & time
    checkCastToString("date '2008-01-01'", "CHAR(10)", "2008-01-01");
    checkCastToString("time '1:2:3'", "CHAR(8)", "01:02:03");
    checkCastToString(
        "timestamp '2008-1-1 1:2:3'",
        "CHAR(19)",
        "2008-01-01 01:02:03");
    checkCastToString(
        "timestamp '2008-1-1 1:2:3'",
        "VARCHAR(30)",
        "2008-01-01 01:02:03");

    checkCastToString(
        "interval '3-2' year to month",
        "CHAR(5)",
        "+3-02");
    checkCastToString(
        "interval '32' month",
        "CHAR(3)",
        "+32");
    checkCastToString(
        "interval '1 2:3:4' day to second",
        "CHAR(11)",
        "+1 02:03:04");
    checkCastToString(
        "interval '1234.56' second(4,2)",
        "CHAR(8)",
        "+1234.56");
    checkCastToString(
        "interval '60' day",
        "CHAR(8)",
        "+60     ");

    // boolean
    checkCastToString("True", "CHAR(4)", "TRUE");
    checkCastToString("True", "CHAR(6)", "TRUE  ");
    checkCastToString("True", "VARCHAR(6)", "TRUE");
    checkCastToString("False", "CHAR(5)", "FALSE");

    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails(
          "cast(true as char(3))", INVALID_CHAR_MESSAGE,
          true);
      tester.checkFails(
          "cast(false as char(4))", INVALID_CHAR_MESSAGE,
          true);
      tester.checkFails(
          "cast(true as varchar(3))", INVALID_CHAR_MESSAGE,
          true);
      tester.checkFails(
          "cast(false as varchar(4))", INVALID_CHAR_MESSAGE,
          true);
    }
  }

  @Test public void testCastExactNumericLimits() {
    tester.setFor(SqlStdOperatorTable.CAST);

    // Test casting for min,max, out of range for exact numeric types
    for (int i = 0; i < NUMERIC_TYPE_NAMES.length; i++) {
      String type = NUMERIC_TYPE_NAMES[i];

      if (type.equalsIgnoreCase("DOUBLE")
          || type.equalsIgnoreCase("FLOAT")
          || type.equalsIgnoreCase("REAL")) {
        // Skip approx types
        continue;
      }

      // Convert from literal to type
      checkCastToScalarOkay(MAX_NUMERIC_STRINGS[i], type);
      checkCastToScalarOkay(MIN_NUMERIC_STRINGS[i], type);

      // Overflow test
      if (type.equalsIgnoreCase("BIGINT")) {
        // Literal of range
        checkCastFails(
            MAX_OVERFLOW_NUMERIC_STRINGS[i],
            type, LITERAL_OUT_OF_RANGE_MESSAGE,
            false);
        checkCastFails(
            MIN_OVERFLOW_NUMERIC_STRINGS[i],
            type, LITERAL_OUT_OF_RANGE_MESSAGE,
            false);
      } else {
        if (Bug.CALCITE_2539_FIXED) {
          checkCastFails(
              MAX_OVERFLOW_NUMERIC_STRINGS[i],
              type, OUT_OF_RANGE_MESSAGE,
              true);
          checkCastFails(
              MIN_OVERFLOW_NUMERIC_STRINGS[i],
              type, OUT_OF_RANGE_MESSAGE,
              true);
        }
      }

      // Convert from string to type
      checkCastToScalarOkay(
          "'" + MAX_NUMERIC_STRINGS[i] + "'",
          type,
          MAX_NUMERIC_STRINGS[i]);
      checkCastToScalarOkay(
          "'" + MIN_NUMERIC_STRINGS[i] + "'",
          type,
          MIN_NUMERIC_STRINGS[i]);

      if (Bug.CALCITE_2539_FIXED) {
        checkCastFails(
            "'" + MAX_OVERFLOW_NUMERIC_STRINGS[i] + "'",
            type, OUT_OF_RANGE_MESSAGE,
            true);
        checkCastFails(
            "'" + MIN_OVERFLOW_NUMERIC_STRINGS[i] + "'",
            type,
            OUT_OF_RANGE_MESSAGE,
            true);
      }

      // Convert from type to string
      checkCastToString(MAX_NUMERIC_STRINGS[i], null, null);
      checkCastToString(MAX_NUMERIC_STRINGS[i], type, null);

      checkCastToString(MIN_NUMERIC_STRINGS[i], null, null);
      checkCastToString(MIN_NUMERIC_STRINGS[i], type, null);

      if (Bug.CALCITE_2539_FIXED) {
        checkCastFails("'notnumeric'", type, INVALID_CHAR_MESSAGE, true);
      }
    }
  }

  @Test public void testCastToExactNumeric() {
    tester.setFor(SqlStdOperatorTable.CAST);

    checkCastToScalarOkay("1", "BIGINT");
    checkCastToScalarOkay("1", "INTEGER");
    checkCastToScalarOkay("1", "SMALLINT");
    checkCastToScalarOkay("1", "TINYINT");
    checkCastToScalarOkay("1", "DECIMAL(4, 0)");
    checkCastToScalarOkay("-1", "BIGINT");
    checkCastToScalarOkay("-1", "INTEGER");
    checkCastToScalarOkay("-1", "SMALLINT");
    checkCastToScalarOkay("-1", "TINYINT");
    checkCastToScalarOkay("-1", "DECIMAL(4, 0)");

    checkCastToScalarOkay("1.234E3", "INTEGER", "1234");
    checkCastToScalarOkay("-9.99E2", "INTEGER", "-999");
    checkCastToScalarOkay("'1'", "INTEGER", "1");
    checkCastToScalarOkay("' 01 '", "INTEGER", "1");
    checkCastToScalarOkay("'-1'", "INTEGER", "-1");
    checkCastToScalarOkay("' -00 '", "INTEGER", "0");

    // string to integer
    tester.checkScalarExact("cast('6543' as integer)", "6543");
    tester.checkScalarExact("cast(' -123 ' as int)", "-123");
    tester.checkScalarExact(
        "cast('654342432412312' as bigint)",
        "BIGINT NOT NULL",
        "654342432412312");
  }

  @Test public void testCastStringToDecimal() {
    tester.setFor(SqlStdOperatorTable.CAST);
    if (!DECIMAL) {
      return;
    }
    // string to decimal
    tester.checkScalarExact(
        "cast('1.29' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "1.3");
    tester.checkScalarExact(
        "cast(' 1.25 ' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "1.3");
    tester.checkScalarExact(
        "cast('1.21' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "1.2");
    tester.checkScalarExact(
        "cast(' -1.29 ' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "-1.3");
    tester.checkScalarExact(
        "cast('-1.25' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "-1.3");
    tester.checkScalarExact(
        "cast(' -1.21 ' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "-1.2");
    tester.checkFails(
        "cast(' -1.21e' as decimal(2,1))", INVALID_CHAR_MESSAGE,
        true);
  }

  @Test public void testCastIntervalToNumeric() {
    tester.setFor(SqlStdOperatorTable.CAST);

    // interval to decimal
    if (DECIMAL) {
      tester.checkScalarExact(
          "cast(INTERVAL '1.29' second(1,2) as decimal(2,1))",
          "DECIMAL(2, 1) NOT NULL",
          "1.3");
      tester.checkScalarExact(
          "cast(INTERVAL '1.25' second as decimal(2,1))",
          "DECIMAL(2, 1) NOT NULL",
          "1.3");
      tester.checkScalarExact(
          "cast(INTERVAL '-1.29' second as decimal(2,1))",
          "DECIMAL(2, 1) NOT NULL",
          "-1.3");
      tester.checkScalarExact(
          "cast(INTERVAL '-1.25' second as decimal(2,1))",
          "DECIMAL(2, 1) NOT NULL",
          "-1.3");
      tester.checkScalarExact(
          "cast(INTERVAL '-1.21' second as decimal(2,1))",
          "DECIMAL(2, 1) NOT NULL",
          "-1.2");
      tester.checkScalarExact(
          "cast(INTERVAL '5' minute as decimal(2,1))",
          "DECIMAL(2, 1) NOT NULL",
          "5.0");
      tester.checkScalarExact(
          "cast(INTERVAL '5' hour as decimal(2,1))",
          "DECIMAL(2, 1) NOT NULL",
          "5.0");
      tester.checkScalarExact(
          "cast(INTERVAL '5' day as decimal(2,1))",
          "DECIMAL(2, 1) NOT NULL",
          "5.0");
      tester.checkScalarExact(
          "cast(INTERVAL '5' month as decimal(2,1))",
          "DECIMAL(2, 1) NOT NULL",
          "5.0");
      tester.checkScalarExact(
          "cast(INTERVAL '5' year as decimal(2,1))",
          "DECIMAL(2, 1) NOT NULL",
          "5.0");
      tester.checkScalarExact(
          "cast(INTERVAL '-5' day as decimal(2,1))",
          "DECIMAL(2, 1) NOT NULL",
          "-5.0");
    }

    // Interval to bigint
    tester.checkScalarExact(
        "cast(INTERVAL '1.25' second as bigint)",
        "BIGINT NOT NULL",
        "1");
    tester.checkScalarExact(
        "cast(INTERVAL '-1.29' second(1,2) as bigint)",
        "BIGINT NOT NULL",
        "-1");
    tester.checkScalarExact(
        "cast(INTERVAL '5' day as bigint)",
        "BIGINT NOT NULL",
        "5");

    // Interval to integer
    tester.checkScalarExact(
        "cast(INTERVAL '1.25' second as integer)",
        "INTEGER NOT NULL",
        "1");
    tester.checkScalarExact(
        "cast(INTERVAL '-1.29' second(1,2) as integer)",
        "INTEGER NOT NULL",
        "-1");
    tester.checkScalarExact(
        "cast(INTERVAL '5' day as integer)",
        "INTEGER NOT NULL",
        "5");

    tester.checkScalarExact(
        "cast(INTERVAL '1' year as integer)",
        "INTEGER NOT NULL",
        "1");
    tester.checkScalarExact(
        "cast((INTERVAL '1' year - INTERVAL '2' year) as integer)",
        "INTEGER NOT NULL",
        "-1");
    tester.checkScalarExact(
        "cast(INTERVAL '1' month as integer)",
        "INTEGER NOT NULL",
        "1");
    tester.checkScalarExact(
        "cast((INTERVAL '1' month - INTERVAL '2' month) as integer)",
        "INTEGER NOT NULL",
        "-1");
    tester.checkScalarExact(
        "cast(INTERVAL '1' day as integer)",
        "INTEGER NOT NULL",
        "1");
    tester.checkScalarExact(
        "cast((INTERVAL '1' day - INTERVAL '2' day) as integer)",
        "INTEGER NOT NULL",
        "-1");
    tester.checkScalarExact(
        "cast(INTERVAL '1' hour as integer)",
        "INTEGER NOT NULL",
        "1");
    tester.checkScalarExact(
        "cast((INTERVAL '1' hour - INTERVAL '2' hour) as integer)",
        "INTEGER NOT NULL",
        "-1");
    tester.checkScalarExact(
        "cast(INTERVAL '1' hour as integer)",
        "INTEGER NOT NULL",
        "1");
    tester.checkScalarExact(
        "cast((INTERVAL '1' minute - INTERVAL '2' minute) as integer)",
        "INTEGER NOT NULL",
        "-1");
    tester.checkScalarExact(
        "cast(INTERVAL '1' minute as integer)",
        "INTEGER NOT NULL",
        "1");
    tester.checkScalarExact(
        "cast((INTERVAL '1' second - INTERVAL '2' second) as integer)",
        "INTEGER NOT NULL",
        "-1");
  }

  @Test public void testCastToInterval() {
    tester.setFor(SqlStdOperatorTable.CAST);
    tester.checkScalar(
        "cast(5 as interval second)",
        "+5.000000",
        "INTERVAL SECOND NOT NULL");
    tester.checkScalar(
        "cast(5 as interval minute)",
        "+5",
        "INTERVAL MINUTE NOT NULL");
    tester.checkScalar(
        "cast(5 as interval hour)",
        "+5",
        "INTERVAL HOUR NOT NULL");
    tester.checkScalar(
        "cast(5 as interval day)",
        "+5",
        "INTERVAL DAY NOT NULL");
    tester.checkScalar(
        "cast(5 as interval month)",
        "+5",
        "INTERVAL MONTH NOT NULL");
    tester.checkScalar(
        "cast(5 as interval year)",
        "+5",
        "INTERVAL YEAR NOT NULL");
    if (DECIMAL) {
      // Due to DECIMAL rounding bugs, currently returns "+5"
      tester.checkScalar(
          "cast(5.7 as interval day)",
          "+6",
          "INTERVAL DAY NOT NULL");
      tester.checkScalar(
          "cast(-5.7 as interval day)",
          "-6",
          "INTERVAL DAY NOT NULL");
    } else {
      // An easier case
      tester.checkScalar(
          "cast(6.2 as interval day)",
          "+6",
          "INTERVAL DAY NOT NULL");
    }
    tester.checkScalar(
        "cast(3456 as interval month(4))",
        "+3456",
        "INTERVAL MONTH(4) NOT NULL");
    tester.checkScalar(
        "cast(-5723 as interval minute(4))",
        "-5723",
        "INTERVAL MINUTE(4) NOT NULL");
  }

  @Test public void testCastIntervalToInterval() {
    tester.checkScalar(
        "cast(interval '2 5' day to hour as interval hour to minute)",
        "+53:00",
        "INTERVAL HOUR TO MINUTE NOT NULL");
    tester.checkScalar(
        "cast(interval '2 5' day to hour as interval day to minute)",
        "+2 05:00",
        "INTERVAL DAY TO MINUTE NOT NULL");
    tester.checkScalar(
        "cast(interval '2 5' day to hour as interval hour to second)",
        "+53:00:00.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    tester.checkScalar(
        "cast(interval '2 5' day to hour as interval hour)",
        "+53",
        "INTERVAL HOUR NOT NULL");
    tester.checkScalar(
        "cast(interval '-29:15' hour to minute as interval day to hour)",
        "-1 05",
        "INTERVAL DAY TO HOUR NOT NULL");
  }

  @Test public void testCastWithRoundingToScalar() {
    tester.setFor(SqlStdOperatorTable.CAST);

    checkCastToScalarOkay("1.25", "INTEGER", "1");
    checkCastToScalarOkay("1.25E0", "INTEGER", "1");
    if (!enable) {
      return;
    }
    checkCastToScalarOkay("1.5", "INTEGER", "2");
    checkCastToScalarOkay("5E-1", "INTEGER", "1");
    checkCastToScalarOkay("1.75", "INTEGER", "2");
    checkCastToScalarOkay("1.75E0", "INTEGER", "2");

    checkCastToScalarOkay("-1.25", "INTEGER", "-1");
    checkCastToScalarOkay("-1.25E0", "INTEGER", "-1");
    checkCastToScalarOkay("-1.5", "INTEGER", "-2");
    checkCastToScalarOkay("-5E-1", "INTEGER", "-1");
    checkCastToScalarOkay("-1.75", "INTEGER", "-2");
    checkCastToScalarOkay("-1.75E0", "INTEGER", "-2");

    checkCastToScalarOkay("1.23454", "DECIMAL(8, 4)", "1.2345");
    checkCastToScalarOkay("1.23454E0", "DECIMAL(8, 4)", "1.2345");
    checkCastToScalarOkay("1.23455", "DECIMAL(8, 4)", "1.2346");
    checkCastToScalarOkay("5E-5", "DECIMAL(8, 4)", "0.0001");
    checkCastToScalarOkay("1.99995", "DECIMAL(8, 4)", "2.0000");
    checkCastToScalarOkay("1.99995E0", "DECIMAL(8, 4)", "2.0000");

    checkCastToScalarOkay("-1.23454", "DECIMAL(8, 4)", "-1.2345");
    checkCastToScalarOkay("-1.23454E0", "DECIMAL(8, 4)", "-1.2345");
    checkCastToScalarOkay("-1.23455", "DECIMAL(8, 4)", "-1.2346");
    checkCastToScalarOkay("-5E-5", "DECIMAL(8, 4)", "-0.0001");
    checkCastToScalarOkay("-1.99995", "DECIMAL(8, 4)", "-2.0000");
    checkCastToScalarOkay("-1.99995E0", "DECIMAL(8, 4)", "-2.0000");

    // 9.99 round to 10.0, should give out of range error
    tester.checkFails(
        "cast(9.99 as decimal(2,1))", OUT_OF_RANGE_MESSAGE,
        true);
  }

  @Test public void testCastDecimalToDoubleToInteger() {
    tester.setFor(SqlStdOperatorTable.CAST);

    tester.checkScalarExact(
        "cast( cast(1.25 as double) as integer)",
        "1");
    tester.checkScalarExact(
        "cast( cast(-1.25 as double) as integer)",
        "-1");
    if (!enable) {
      return;
    }
    tester.checkScalarExact(
        "cast( cast(1.75 as double) as integer)",
        "2");
    tester.checkScalarExact(
        "cast( cast(-1.75 as double) as integer)",
        "-2");
    tester.checkScalarExact(
        "cast( cast(1.5 as double) as integer)",
        "2");
    tester.checkScalarExact(
        "cast( cast(-1.5 as double) as integer)",
        "-2");
  }

  @Test public void testCastApproxNumericLimits() {
    tester.setFor(SqlStdOperatorTable.CAST);

    // Test casting for min,max, out of range for approx numeric types
    for (int i = 0; i < NUMERIC_TYPE_NAMES.length; i++) {
      String type = NUMERIC_TYPE_NAMES[i];
      boolean isFloat;

      if (type.equalsIgnoreCase("DOUBLE")
          || type.equalsIgnoreCase("FLOAT")) {
        isFloat = false;
      } else if (type.equalsIgnoreCase("REAL")) {
        isFloat = true;
      } else {
        // Skip non-approx types
        continue;
      }

      if (!enable) {
        return;
      }

      // Convert from literal to type
      checkCastToApproxOkay(
          MAX_NUMERIC_STRINGS[i],
          type,
          Double.parseDouble(MAX_NUMERIC_STRINGS[i]),
          isFloat ? 1E32 : 0);
      checkCastToApproxOkay(
          MIN_NUMERIC_STRINGS[i],
          type,
          Double.parseDouble(MIN_NUMERIC_STRINGS[i]),
          0);

      if (isFloat) {
        checkCastFails(
            MAX_OVERFLOW_NUMERIC_STRINGS[i],
            type, OUT_OF_RANGE_MESSAGE,
            true);
      } else {
        // Double: Literal out of range
        checkCastFails(
            MAX_OVERFLOW_NUMERIC_STRINGS[i],
            type, LITERAL_OUT_OF_RANGE_MESSAGE,
            false);
      }

      // Underflow: goes to 0
      checkCastToApproxOkay(MIN_OVERFLOW_NUMERIC_STRINGS[i], type, 0, 0);

      // Convert from string to type
      checkCastToApproxOkay(
          "'" + MAX_NUMERIC_STRINGS[i] + "'",
          type,
          Double.parseDouble(MAX_NUMERIC_STRINGS[i]),
          isFloat ? 1E32 : 0);
      checkCastToApproxOkay(
          "'" + MIN_NUMERIC_STRINGS[i] + "'",
          type,
          Double.parseDouble(MIN_NUMERIC_STRINGS[i]),
          0);

      checkCastFails(
          "'" + MAX_OVERFLOW_NUMERIC_STRINGS[i] + "'",
          type,
          OUT_OF_RANGE_MESSAGE,
          true);

      // Underflow: goes to 0
      checkCastToApproxOkay(
          "'" + MIN_OVERFLOW_NUMERIC_STRINGS[i] + "'",
          type,
          0,
          0);

      // Convert from type to string

      // Treated as DOUBLE
      checkCastToString(
          MAX_NUMERIC_STRINGS[i], null,
          isFloat ? null : "1.79769313486231E308");

      // TODO: The following tests are slightly different depending on
      // whether the java or fennel calc are used.
      // Try to make them the same
      if (false /* fennel calc*/) { // Treated as FLOAT or DOUBLE
        checkCastToString(
            MAX_NUMERIC_STRINGS[i],
            type,
            // Treated as DOUBLE
            isFloat ? "3.402824E38" : "1.797693134862316E308");
        checkCastToString(
            MIN_NUMERIC_STRINGS[i],
            null,
            // Treated as FLOAT or DOUBLE
            isFloat ? null : "4.940656458412465E-324");
        checkCastToString(
            MIN_NUMERIC_STRINGS[i],
            type,
            isFloat ? "1.401299E-45" : "4.940656458412465E-324");
      } else if (false /* JavaCalc */) {
        // Treated as FLOAT or DOUBLE
        checkCastToString(
            MAX_NUMERIC_STRINGS[i],
            type,
            // Treated as DOUBLE
            isFloat ? "3.402823E38" : "1.797693134862316E308");
        checkCastToString(
            MIN_NUMERIC_STRINGS[i],
            null,
            isFloat ? null : null); // Treated as FLOAT or DOUBLE
        checkCastToString(
            MIN_NUMERIC_STRINGS[i],
            type,
            isFloat ? "1.401298E-45" : null);
      }

      checkCastFails("'notnumeric'", type, INVALID_CHAR_MESSAGE, true);
    }
  }

  @Test public void testCastToApproxNumeric() {
    tester.setFor(SqlStdOperatorTable.CAST);

    checkCastToApproxOkay("1", "DOUBLE", 1, 0);
    checkCastToApproxOkay("1.0", "DOUBLE", 1, 0);
    checkCastToApproxOkay("-2.3", "FLOAT", -2.3, 0.000001);
    checkCastToApproxOkay("'1'", "DOUBLE", 1, 0);
    checkCastToApproxOkay("'  -1e-37  '", "DOUBLE", -1e-37, 0);
    checkCastToApproxOkay("1e0", "DOUBLE", 1, 0);
    checkCastToApproxOkay("0e0", "REAL", 0, 0);
  }

  @Test public void testCastNull() {
    tester.setFor(SqlStdOperatorTable.CAST);

    // null
    tester.checkNull("cast(null as integer)");
    if (DECIMAL) {
      tester.checkNull("cast(null as decimal(4,3))");
    }
    tester.checkNull("cast(null as double)");
    tester.checkNull("cast(null as varchar(10))");
    tester.checkNull("cast(null as char(10))");
    tester.checkNull("cast(null as date)");
    tester.checkNull("cast(null as time)");
    tester.checkNull("cast(null as timestamp)");
    tester.checkNull("cast(null as interval year to month)");
    tester.checkNull("cast(null as interval day to second(3))");
    tester.checkNull("cast(null as boolean)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1439">[CALCITE-1439]
   * Handling errors during constant reduction</a>. */
  @Test public void testCastInvalid() {
    // Before CALCITE-1439 was fixed, constant reduction would kick in and
    // generate Java constants that throw when the class is loaded, thus
    // ExceptionInInitializerError.
    tester.checkScalarExact("cast('15' as integer)", "INTEGER NOT NULL", "15");
    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails("cast('15.4' as integer)", "xxx", true);
      tester.checkFails("cast('15.6' as integer)", "xxx", true);
      tester.checkFails("cast('ue' as boolean)", "xxx", true);
      tester.checkFails("cast('' as boolean)", "xxx", true);
      tester.checkFails("cast('' as integer)", "xxx", true);
      tester.checkFails("cast('' as real)", "xxx", true);
      tester.checkFails("cast('' as double)", "xxx", true);
      tester.checkFails("cast('' as smallint)", "xxx", true);
    }
  }

  @Test public void testCastDateTime() {
    // Test cast for date/time/timestamp
    tester.setFor(SqlStdOperatorTable.CAST);

    tester.checkScalar(
        "cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIMESTAMP)",
        "1945-02-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");

    tester.checkScalar(
        "cast(TIME '12:42:25.34' as TIME)",
        "12:42:25",
        "TIME(0) NOT NULL");

    // test rounding
    if (enable) {
      tester.checkScalar(
          "cast(TIME '12:42:25.9' as TIME)",
          "12:42:26",
          "TIME(0) NOT NULL");
    }

    if (Bug.FRG282_FIXED) {
      // test precision
      tester.checkScalar(
          "cast(TIME '12:42:25.34' as TIME(2))",
          "12:42:25.34",
          "TIME(2) NOT NULL");
    }

    tester.checkScalar(
        "cast(DATE '1945-02-24' as DATE)",
        "1945-02-24",
        "DATE NOT NULL");

    // timestamp <-> time
    tester.checkScalar(
        "cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIME)",
        "12:42:25",
        "TIME(0) NOT NULL");

    // time <-> string
    checkCastToString("TIME '12:42:25'", null, "12:42:25");
    if (TODO) {
      checkCastToString("TIME '12:42:25.34'", null, "12:42:25.34");
    }

    // Generate the current date as a string, e.g. "2007-04-18". The value
    // is guaranteed to be good for at least 2 minutes, which should give
    // us time to run the rest of the tests.
    final String today =
        new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT).format(
            getCalendarNotTooNear(Calendar.DAY_OF_MONTH).getTime());

    tester.checkScalar(
        "cast(DATE '1945-02-24' as TIMESTAMP)",
        "1945-02-24 00:00:00",
        "TIMESTAMP(0) NOT NULL");

    // Note: Casting to time(0) should lose date info and fractional
    // seconds, then casting back to timestamp should initialize to
    // current_date.
    tester.checkScalar(
        "cast(cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIME) as TIMESTAMP)",
        today + " 12:42:25",
        "TIMESTAMP(0) NOT NULL");

    tester.checkScalar(
        "cast(TIME '12:42:25.34' as TIMESTAMP)",
        today + " 12:42:25",
        "TIMESTAMP(0) NOT NULL");

    // timestamp <-> date
    tester.checkScalar(
        "cast(TIMESTAMP '1945-02-24 12:42:25.34' as DATE)",
        "1945-02-24",
        "DATE NOT NULL");

    // Note: casting to Date discards Time fields
    tester.checkScalar(
        "cast(cast(TIMESTAMP '1945-02-24 12:42:25.34' as DATE) as TIMESTAMP)",
        "1945-02-24 00:00:00",
        "TIMESTAMP(0) NOT NULL");
  }

  @Test public void testCastStringToDateTime() {
    tester.checkScalar(
        "cast('12:42:25' as TIME)",
        "12:42:25",
        "TIME(0) NOT NULL");
    tester.checkScalar(
        "cast('1:42:25' as TIME)",
        "01:42:25",
        "TIME(0) NOT NULL");
    tester.checkScalar(
        "cast('1:2:25' as TIME)",
        "01:02:25",
        "TIME(0) NOT NULL");
    tester.checkScalar(
        "cast('  12:42:25  ' as TIME)",
        "12:42:25",
        "TIME(0) NOT NULL");
    tester.checkScalar(
        "cast('12:42:25.34' as TIME)",
        "12:42:25",
        "TIME(0) NOT NULL");

    if (Bug.FRG282_FIXED) {
      tester.checkScalar(
          "cast('12:42:25.34' as TIME(2))",
          "12:42:25.34",
          "TIME(2) NOT NULL");
    }

    tester.checkFails(
        "cast('nottime' as TIME)", BAD_DATETIME_MESSAGE,
        true);
    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails(
          "cast('1241241' as TIME)", BAD_DATETIME_MESSAGE,
          true);
      tester.checkFails(
          "cast('12:54:78' as TIME)", BAD_DATETIME_MESSAGE,
          true);
      tester.checkFails(
          "cast('12:34:5' as TIME)", BAD_DATETIME_MESSAGE,
          true);
      tester.checkFails(
          "cast('12:3:45' as TIME)", BAD_DATETIME_MESSAGE,
          true);
      tester.checkFails(
          "cast('1:23:45' as TIME)", BAD_DATETIME_MESSAGE,
          true);
    }

    // timestamp <-> string
    checkCastToString(
        "TIMESTAMP '1945-02-24 12:42:25'",
        null,
        "1945-02-24 12:42:25");

    if (TODO) {
      // TODO: casting allows one to discard precision without error
      checkCastToString(
          "TIMESTAMP '1945-02-24 12:42:25.34'",
          null,
          "1945-02-24 12:42:25.34");
    }

    tester.checkScalar(
        "cast('1945-02-24 12:42:25' as TIMESTAMP)",
        "1945-02-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "cast('1945-2-2 12:2:5' as TIMESTAMP)",
        "1945-02-02 12:02:05",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "cast('  1945-02-24 12:42:25  ' as TIMESTAMP)",
        "1945-02-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "cast('1945-02-24 12:42:25.34' as TIMESTAMP)",
        "1945-02-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "cast('1945-12-31' as TIMESTAMP)",
        "1945-12-31 00:00:00",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "cast('2004-02-29' as TIMESTAMP)",
        "2004-02-29 00:00:00",
        "TIMESTAMP(0) NOT NULL");

    if (Bug.FRG282_FIXED) {
      tester.checkScalar(
          "cast('1945-02-24 12:42:25.34' as TIMESTAMP(2))",
          "1945-02-24 12:42:25.34",
          "TIMESTAMP(2) NOT NULL");
    }
    tester.checkFails(
        "cast('nottime' as TIMESTAMP)", BAD_DATETIME_MESSAGE,
        true);

    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails(
          "cast('1241241' as TIMESTAMP)", BAD_DATETIME_MESSAGE,
          true);
      tester.checkFails(
          "cast('1945-20-24 12:42:25.34' as TIMESTAMP)", BAD_DATETIME_MESSAGE,
          true);
      tester.checkFails(
          "cast('1945-01-24 25:42:25.34' as TIMESTAMP)", BAD_DATETIME_MESSAGE,
          true);
      tester.checkFails(
          "cast('1945-1-24 12:23:34.454' as TIMESTAMP)", BAD_DATETIME_MESSAGE,
          true);
    }

    // date <-> string
    checkCastToString("DATE '1945-02-24'", null, "1945-02-24");
    checkCastToString("DATE '1945-2-24'", null, "1945-02-24");

    tester.checkScalar(
        "cast('1945-02-24' as DATE)",
        "1945-02-24",
        "DATE NOT NULL");
    tester.checkScalar(
        "cast(' 1945-2-4 ' as DATE)",
        "1945-02-04",
        "DATE NOT NULL");
    tester.checkScalar(
        "cast('  1945-02-24  ' as DATE)",
        "1945-02-24",
        "DATE NOT NULL");
    tester.checkFails(
        "cast('notdate' as DATE)", BAD_DATETIME_MESSAGE,
        true);

    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails(
          "cast('52534253' as DATE)", BAD_DATETIME_MESSAGE,
          true);
      tester.checkFails(
          "cast('1945-30-24' as DATE)", BAD_DATETIME_MESSAGE,
          true);
    }

    // cast null
    tester.checkNull("cast(null as date)");
    tester.checkNull("cast(null as timestamp)");
    tester.checkNull("cast(null as time)");
    tester.checkNull("cast(cast(null as varchar(10)) as time)");
    tester.checkNull("cast(cast(null as varchar(10)) as date)");
    tester.checkNull("cast(cast(null as varchar(10)) as timestamp)");
    tester.checkNull("cast(cast(null as date) as timestamp)");
    tester.checkNull("cast(cast(null as time) as timestamp)");
    tester.checkNull("cast(cast(null as timestamp) as date)");
    tester.checkNull("cast(cast(null as timestamp) as time)");
  }

  /**
   * Returns a Calendar that is the current time, pausing if we are within 2
   * minutes of midnight or the top of the hour.
   *
   * @param timeUnit Time unit
   * @return calendar
   */
  protected static Calendar getCalendarNotTooNear(int timeUnit) {
    final Calendar cal = Util.calendar();
    while (true) {
      cal.setTimeInMillis(System.currentTimeMillis());
      try {
        switch (timeUnit) {
        case Calendar.DAY_OF_MONTH:
          // Within two minutes of the end of the day. Wait in 10s
          // increments until calendar moves into the next next day.
          if ((cal.get(Calendar.HOUR_OF_DAY) == 23)
              && (cal.get(Calendar.MINUTE) >= 58)) {
            Thread.sleep(10 * 1000);
            continue;
          }
          return cal;

        case Calendar.HOUR_OF_DAY:
          // Within two minutes of the top of the hour. Wait in 10s
          // increments until calendar moves into the next next day.
          if (cal.get(Calendar.MINUTE) >= 58) {
            Thread.sleep(10 * 1000);
            continue;
          }
          return cal;

        default:
          throw new AssertionError("unexpected time unit: " + timeUnit);
        }
      } catch (InterruptedException e) {
        throw TestUtil.rethrow(e);
      }
    }
  }

  @Test public void testCastToBoolean() {
    tester.setFor(SqlStdOperatorTable.CAST);

    // string to boolean
    tester.checkBoolean("cast('true' as boolean)", Boolean.TRUE);
    tester.checkBoolean("cast('false' as boolean)", Boolean.FALSE);
    tester.checkBoolean("cast('  trUe' as boolean)", Boolean.TRUE);
    tester.checkBoolean("cast('  tr' || 'Ue' as boolean)", Boolean.TRUE);
    tester.checkBoolean("cast('  fALse' as boolean)", Boolean.FALSE);
    tester.checkFails(
        "cast('unknown' as boolean)", INVALID_CHAR_MESSAGE,
        true);

    tester.checkBoolean(
        "cast(cast('true' as varchar(10))  as boolean)",
        Boolean.TRUE);
    tester.checkBoolean(
        "cast(cast('false' as varchar(10)) as boolean)",
        Boolean.FALSE);
    tester.checkFails(
        "cast(cast('blah' as varchar(10)) as boolean)", INVALID_CHAR_MESSAGE,
        true);
  }

  @Test public void testCase() {
    tester.setFor(SqlStdOperatorTable.CASE);
    tester.checkScalarExact("case when 'a'='a' then 1 end", "1");

    tester.checkString(
        "case 2 when 1 then 'a' when 2 then 'bcd' end",
        "bcd",
        "CHAR(3)");
    tester.checkString(
        "case 1 when 1 then 'a' when 2 then 'bcd' end",
        "a  ",
        "CHAR(3)");
    tester.checkString(
        "case 1 when 1 then cast('a' as varchar(1)) "
            + "when 2 then cast('bcd' as varchar(3)) end",
        "a",
        "VARCHAR(3)");
    if (DECIMAL) {
      tester.checkScalarExact(
          "case 2 when 1 then 11.2 when 2 then 4.543 else null end",
          "DECIMAL(5, 3)",
          "4.543");
      tester.checkScalarExact(
          "case 1 when 1 then 11.2 when 2 then 4.543 else null end",
          "DECIMAL(5, 3)",
          "11.200");
    }
    tester.checkScalarExact("case 'a' when 'a' then 1 end", "1");
    tester.checkScalarApprox(
        "case 1 when 1 then 11.2e0 when 2 then cast(4 as bigint) else 3 end",
        "DOUBLE NOT NULL",
        11.2,
        0);
    tester.checkScalarApprox(
        "case 1 when 1 then 11.2e0 when 2 then 4 else null end",
        "DOUBLE",
        11.2,
        0);
    tester.checkScalarApprox(
        "case 2 when 1 then 11.2e0 when 2 then 4 else null end",
        "DOUBLE",
        4,
        0);
    tester.checkScalarApprox(
        "case 1 when 1 then 11.2e0 when 2 then 4.543 else null end",
        "DOUBLE",
        11.2,
        0);
    tester.checkScalarApprox(
        "case 2 when 1 then 11.2e0 when 2 then 4.543 else null end",
        "DOUBLE",
        4.543,
        0);
    tester.checkNull("case 'a' when 'b' then 1 end");

    // Per spec, 'case x when y then ...'
    // translates to 'case when x = y then ...'
    // so nulls do not match.
    // (Unlike Oracle's 'decode(null, null, ...)', by the way.)
    tester.checkString(
        "case cast(null as int) when cast(null as int) then 'nulls match' else 'nulls do not match' end",
        "nulls do not match",
        "CHAR(18) NOT NULL");

    tester.checkScalarExact(
        "case when 'a'=cast(null as varchar(1)) then 1 else 2 end",
        "2");

    // equivalent to "nullif('a',cast(null as varchar(1)))"
    tester.checkString(
        "case when 'a' = cast(null as varchar(1)) then null else 'a' end",
        "a",
        "CHAR(1)");

    if (TODO) {
      tester.checkScalar(
          "case 1 when 1 then row(1,2) when 2 then row(2,3) end",
          "ROW(INTEGER NOT NULL, INTEGER NOT NULL)",
          "row(1,2)");
      tester.checkScalar(
          "case 1 when 1 then row('a','b') when 2 then row('ab','cd') end",
          "ROW(CHAR(2) NOT NULL, CHAR(2) NOT NULL)",
          "row('a ','b ')");
    }

    // multiple values in some cases (introduced in SQL:2011)
    tester.checkString(
        "case 1 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "1 or 2           ",
        "CHAR(17) NOT NULL");
    tester.checkString(
        "case 2 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "1 or 2           ",
        "CHAR(17) NOT NULL");
    tester.checkString(
        "case 3 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "3                ",
        "CHAR(17) NOT NULL");
    tester.checkString(
        "case 4 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "none of the above",
        "CHAR(17) NOT NULL");

    // tests with SqlConformance
    final SqlTester tester2 =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    tester2.checkString(
        "case 2 when 1 then 'a' when 2 then 'bcd' end",
        "bcd",
        "VARCHAR(3)");
    tester2.checkString(
        "case 1 when 1 then 'a' when 2 then 'bcd' end",
        "a",
        "VARCHAR(3)");
    tester2.checkString(
        "case 1 when 1 then cast('a' as varchar(1)) "
            + "when 2 then cast('bcd' as varchar(3)) end",
        "a",
        "VARCHAR(3)");

    tester2.checkString(
        "case cast(null as int) when cast(null as int)"
            + " then 'nulls match'"
            + " else 'nulls do not match' end",
        "nulls do not match",
        "VARCHAR(18) NOT NULL");
    tester2.checkScalarExact(
        "case when 'a'=cast(null as varchar(1)) then 1 else 2 end",
        "2");

    // equivalent to "nullif('a',cast(null as varchar(1)))"
    tester2.checkString(
        "case when 'a' = cast(null as varchar(1)) then null else 'a' end",
        "a",
        "CHAR(1)");

    // multiple values in some cases (introduced in SQL:2011)
    tester2.checkString(
        "case 1 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "1 or 2",
        "VARCHAR(17) NOT NULL");
    tester2.checkString(
        "case 2 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "1 or 2",
        "VARCHAR(17) NOT NULL");
    tester2.checkString(
        "case 3 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "3",
        "VARCHAR(17) NOT NULL");
    tester2.checkString(
        "case 4 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "none of the above",
        "VARCHAR(17) NOT NULL");

    // TODO: Check case with multisets
  }

  @Test public void testCaseNull() {
    tester.setFor(SqlStdOperatorTable.CASE);
    tester.checkScalarExact("case when 1 = 1 then 10 else null end", "10");
    tester.checkNull("case when 1 = 2 then 10 else null end");
  }

  @Test public void testCaseType() {
    tester.setFor(SqlStdOperatorTable.CASE);
    tester.checkType(
        "case 1 when 1 then current_timestamp else null end",
        "TIMESTAMP(0)");
    tester.checkType(
        "case 1 when 1 then current_timestamp else current_timestamp end",
        "TIMESTAMP(0) NOT NULL");
    tester.checkType(
        "case when true then current_timestamp else null end",
        "TIMESTAMP(0)");
    tester.checkType(
        "case when true then current_timestamp end",
        "TIMESTAMP(0)");
    tester.checkType(
        "case 'x' when 'a' then 3 when 'b' then null else 4.5 end",
        "DECIMAL(11, 1)");
  }

  /**
   * Tests support for JDBC functions.
   *
   * <p>See FRG-97 "Support for JDBC escape syntax is incomplete".
   */
  @Test public void testJdbcFn() {
    tester.setFor(new SqlJdbcFunctionCall("dummy"));

    // There follows one test for each function in appendix C of the JDBC
    // 3.0 specification. The test is 'if-false'd out if the function is
    // not implemented or is broken.

    // Numeric Functions
    tester.checkScalar("{fn ABS(-3)}", 3, "INTEGER NOT NULL");
    tester.checkScalarApprox("{fn ACOS(0.2)}", "DOUBLE NOT NULL", 1.36943, 0.001);
    tester.checkScalarApprox("{fn ASIN(0.2)}", "DOUBLE NOT NULL", 0.20135, 0.001);
    tester.checkScalarApprox("{fn ATAN(0.2)}", "DOUBLE NOT NULL", 0.19739, 0.001);
    tester.checkScalarApprox("{fn ATAN2(-2, 2)}", "DOUBLE NOT NULL", -0.78539, 0.001);
    tester.checkScalar("{fn CEILING(-2.6)}", -2, "DECIMAL(2, 0) NOT NULL");
    tester.checkScalarApprox("{fn COS(0.2)}", "DOUBLE NOT NULL", 0.98007, 0.001);
    tester.checkScalarApprox("{fn COT(0.2)}", "DOUBLE NOT NULL", 4.93315, 0.001);
    tester.checkScalarApprox("{fn DEGREES(-1)}", "DOUBLE NOT NULL", -57.29578, 0.001);

    tester.checkScalarApprox(
        "{fn EXP(2)}",
        "DOUBLE NOT NULL",
        7.389,
        0.001);
    tester.checkScalar("{fn FLOOR(2.6)}", 2, "DECIMAL(2, 0) NOT NULL");
    tester.checkScalarApprox(
        "{fn LOG(10)}",
        "DOUBLE NOT NULL",
        2.30258,
        0.001);
    tester.checkScalarApprox(
        "{fn LOG10(100)}",
        "DOUBLE NOT NULL",
        2,
        0);
    tester.checkScalar("{fn MOD(19, 4)}", 3, "INTEGER NOT NULL");
    tester.checkScalarApprox("{fn PI()}", "DOUBLE NOT NULL", 3.14159, 0.0001);
    tester.checkScalarApprox("{fn POWER(2, 3)}", "DOUBLE NOT NULL", 8.0, 0.001);
    tester.checkScalarApprox("{fn RADIANS(90)}", "DOUBLE NOT NULL", 1.57080, 0.001);
    tester.checkScalarApprox("{fn RAND(42)}", "DOUBLE NOT NULL", 0.63708, 0.001);
    tester.checkScalar("{fn ROUND(1251, -2)}", 1300, "INTEGER NOT NULL");
    tester.checkFails("^{fn ROUND(1251)}^", "Cannot apply '\\{fn ROUND\\}' to "
        + "arguments of type '\\{fn ROUND\\}\\(<INTEGER>\\)'.*", false);
    tester.checkScalar("{fn SIGN(-1)}", -1, "INTEGER NOT NULL");
    tester.checkScalarApprox("{fn SIN(0.2)}", "DOUBLE NOT NULL", 0.19867, 0.001);
    tester.checkScalarApprox("{fn SQRT(4.2)}", "DOUBLE NOT NULL", 2.04939, 0.001);
    tester.checkScalarApprox("{fn TAN(0.2)}", "DOUBLE NOT NULL", 0.20271, 0.001);
    tester.checkScalar("{fn TRUNCATE(12.34, 1)}", 12.3, "DECIMAL(4, 2) NOT NULL");
    tester.checkScalar("{fn TRUNCATE(-12.34, -1)}", -10, "DECIMAL(4, 2) NOT NULL");

    // String Functions
    tester.checkScalar("{fn ASCII('a')}", 97, "INTEGER NOT NULL");
    tester.checkScalar("{fn ASCII('ABC')}", "65", "INTEGER NOT NULL");
    tester.checkNull("{fn ASCII(cast(null as varchar(1)))}");

    if (false) {
      tester.checkScalar("{fn CHAR(code)}", null, "");
    }
    tester.checkScalar(
        "{fn CONCAT('foo', 'bar')}",
        "foobar",
        "CHAR(6) NOT NULL");

    tester.checkScalar("{fn DIFFERENCE('Miller', 'miller')}", "4", "INTEGER NOT NULL");
    tester.checkNull("{fn DIFFERENCE('muller', cast(null as varchar(1)))}");

    tester.checkString("{fn REVERSE('abc')}", "cba", "VARCHAR(3) NOT NULL");
    tester.checkNull("{fn REVERSE(cast(null as varchar(1)))}");

    tester.checkString("{fn LEFT('abcd', 3)}", "abc", "VARCHAR(4) NOT NULL");
    tester.checkString("{fn LEFT('abcd', 4)}", "abcd", "VARCHAR(4) NOT NULL");
    tester.checkString("{fn LEFT('abcd', 5)}", "abcd", "VARCHAR(4) NOT NULL");
    tester.checkNull("{fn LEFT(cast(null as varchar(1)), 3)}");
    tester.checkString("{fn RIGHT('abcd', 3)}", "bcd", "VARCHAR(4) NOT NULL");
    tester.checkString("{fn RIGHT('abcd', 4)}", "abcd", "VARCHAR(4) NOT NULL");
    tester.checkString("{fn RIGHT('abcd', 5)}", "abcd", "VARCHAR(4) NOT NULL");
    tester.checkNull("{fn RIGHT(cast(null as varchar(1)), 3)}");

    // REVIEW: is this result correct? I think it should be "abcCdef"
    tester.checkScalar(
        "{fn INSERT('abc', 1, 2, 'ABCdef')}",
        "ABCdefc",
        "VARCHAR(9) NOT NULL");
    tester.checkScalar(
        "{fn LCASE('foo' || 'bar')}",
        "foobar",
        "CHAR(6) NOT NULL");
    if (false) {
      tester.checkScalar("{fn LENGTH(string)}", null, "");
    }
    tester.checkScalar(
        "{fn LOCATE('ha', 'alphabet')}",
        4,
        "INTEGER NOT NULL");

    tester.checkScalar(
        "{fn LOCATE('ha', 'alphabet', 6)}",
        0,
        "INTEGER NOT NULL");

    tester.checkScalar(
        "{fn LTRIM(' xxx  ')}",
        "xxx  ",
        "VARCHAR(6) NOT NULL");

    tester.checkScalar("{fn REPEAT('a', -100)}", "", "VARCHAR(1) NOT NULL");
    tester.checkNull("{fn REPEAT('abc', cast(null as integer))}");
    tester.checkNull("{fn REPEAT(cast(null as varchar(1)), cast(null as integer))}");

    tester.checkString("{fn REPLACE('JACK and JUE','J','BL')}",
        "BLACK and BLUE", "VARCHAR(12) NOT NULL");

    // REPLACE returns NULL in Oracle but not in Postgres or in Calcite.
    // When [CALCITE-815] is implemented and SqlConformance#emptyStringIsNull is
    // enabled, it will return empty string as NULL.
    tester.checkString("{fn REPLACE('ciao', 'ciao', '')}", "",
        "VARCHAR(4) NOT NULL");

    tester.checkString("{fn REPLACE('hello world', 'o', '')}", "hell wrld",
        "VARCHAR(11) NOT NULL");

    tester.checkNull("{fn REPLACE(cast(null as varchar(5)), 'ciao', '')}");
    tester.checkNull("{fn REPLACE('ciao', cast(null as varchar(3)), 'zz')}");
    tester.checkNull("{fn REPLACE('ciao', 'bella', cast(null as varchar(3)))}");


    tester.checkScalar(
        "{fn RTRIM(' xxx  ')}",
        " xxx",
        "VARCHAR(6) NOT NULL");

    tester.checkScalar("{fn SOUNDEX('Miller')}", "M460", "VARCHAR(4) NOT NULL");
    tester.checkNull("{fn SOUNDEX(cast(null as varchar(1)))}");

    tester.checkScalar("{fn SPACE(-100)}", "", "VARCHAR(2000) NOT NULL");
    tester.checkNull("{fn SPACE(cast(null as integer))}");

    tester.checkScalar(
        "{fn SUBSTRING('abcdef', 2, 3)}",
        "bcd",
        "VARCHAR(6) NOT NULL");
    tester.checkScalar("{fn UCASE('xxx')}", "XXX", "CHAR(3) NOT NULL");

    // Time and Date Functions
    tester.checkType("{fn CURDATE()}", "DATE NOT NULL");
    tester.checkType("{fn CURTIME()}", "TIME(0) NOT NULL");
    tester.checkScalar("{fn DAYNAME(DATE '2014-12-10')}",
        // Day names in root locale changed from long to short in JDK 9
        TestUtil.getJavaMajorVersion() <= 8 ? "Wednesday" : "Wed",
        "VARCHAR(2000) NOT NULL");
    tester.checkScalar("{fn DAYOFMONTH(DATE '2014-12-10')}", 10,
        "BIGINT NOT NULL");
    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails("{fn DAYOFWEEK(DATE '2014-12-10')}",
          "cannot translate call EXTRACT.*",
          true);
      tester.checkFails("{fn DAYOFYEAR(DATE '2014-12-10')}",
          "cannot translate call EXTRACT.*",
          true);
    }
    tester.checkScalar("{fn HOUR(TIMESTAMP '2014-12-10 12:34:56')}", 12,
        "BIGINT NOT NULL");
    tester.checkScalar("{fn MINUTE(TIMESTAMP '2014-12-10 12:34:56')}", 34,
        "BIGINT NOT NULL");
    tester.checkScalar("{fn MONTH(DATE '2014-12-10')}", 12, "BIGINT NOT NULL");
    tester.checkScalar("{fn MONTHNAME(DATE '2014-12-10')}",
        // Month names in root locale changed from long to short in JDK 9
        TestUtil.getJavaMajorVersion() <= 8 ? "December" : "Dec",
        "VARCHAR(2000) NOT NULL");
    tester.checkType("{fn NOW()}", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("{fn QUARTER(DATE '2014-12-10')}", "4",
        "BIGINT NOT NULL");
    tester.checkScalar("{fn SECOND(TIMESTAMP '2014-12-10 12:34:56')}", 56,
        "BIGINT NOT NULL");
    tester.checkScalar("{fn TIMESTAMPADD(HOUR, 5,"
            + " TIMESTAMP '2014-03-29 12:34:56')}",
        "2014-03-29 17:34:56", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("{fn TIMESTAMPDIFF(HOUR,"
        + " TIMESTAMP '2014-03-29 12:34:56',"
        + " TIMESTAMP '2014-03-29 12:34:56')}", "0", "INTEGER NOT NULL");

    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails("{fn WEEK(DATE '2014-12-10')}",
          "cannot translate call EXTRACT.*",
          true);
    }
    tester.checkScalar("{fn YEAR(DATE '2014-12-10')}", 2014, "BIGINT NOT NULL");

    // System Functions
    tester.checkType("{fn DATABASE()}", "VARCHAR(2000) NOT NULL");
    tester.checkString("{fn IFNULL('a', 'b')}", "a", "CHAR(1) NOT NULL");
    tester.checkString("{fn USER()}", "sa", "VARCHAR(2000) NOT NULL");


    // Conversion Functions
    // Legacy JDBC style
    tester.checkScalar("{fn CONVERT('123', INTEGER)}", 123, "INTEGER NOT NULL");
    // ODBC/JDBC style
    tester.checkScalar("{fn CONVERT('123', SQL_INTEGER)}", 123, "INTEGER NOT NULL");
    tester.checkScalar("{fn CONVERT(INTERVAL '1' DAY, SQL_INTERVAL_DAY_TO_SECOND)}",
        "+1 00:00:00.000000", "INTERVAL DAY TO SECOND NOT NULL");

  }

  @Test public void testChr() {
    tester.setFor(SqlLibraryOperators.CHR, VM_FENNEL, VM_JAVA);
    final SqlTester tester1 = oracleTester();
    tester1.checkScalar("chr(97)",
            "a", "CHAR(1) NOT NULL");
    tester1.checkScalar("chr(48)",
            "0", "CHAR(1) NOT NULL");
    tester1.checkScalar("chr(0)",
            String.valueOf('\u0000'), "CHAR(1) NOT NULL");
    tester.checkFails("^chr(97.1)^",
            "No match found for function signature CHR\\(<NUMERIC>\\)", false);
  }

  @Test public void testSelect() {
    tester.check(
        "select * from (values(1))",
        SqlTests.INTEGER_TYPE_CHECKER,
        "1",
        0);

    // Check return type on scalar sub-query in select list.  Note return
    // type is always nullable even if sub-query select value is NOT NULL.
    // Bug FRG-189 causes this test to fail only in SqlOperatorTest; not
    // in subtypes.
    if (Bug.FRG189_FIXED
        || (getClass() != SqlOperatorTest.class) && Bug.TODO_FIXED) {
      tester.checkType(
          "SELECT *,(SELECT * FROM (VALUES(1))) FROM (VALUES(2))",
          "RecordType(INTEGER NOT NULL EXPR$0, INTEGER EXPR$1) NOT NULL");
      tester.checkType(
          "SELECT *,(SELECT * FROM (VALUES(CAST(10 as BIGINT)))) "
              + "FROM (VALUES(CAST(10 as bigint)))",
          "RecordType(BIGINT NOT NULL EXPR$0, BIGINT EXPR$1) NOT NULL");
      tester.checkType(
          " SELECT *,(SELECT * FROM (VALUES(10.5))) FROM (VALUES(10.5))",
          "RecordType(DECIMAL(3, 1) NOT NULL EXPR$0, DECIMAL(3, 1) EXPR$1) NOT NULL");
      tester.checkType(
          "SELECT *,(SELECT * FROM (VALUES('this is a char'))) "
              + "FROM (VALUES('this is a char too'))",
          "RecordType(CHAR(18) NOT NULL EXPR$0, CHAR(14) EXPR$1) NOT NULL");
      tester.checkType(
          "SELECT *,(SELECT * FROM (VALUES(true))) FROM (values(false))",
          "RecordType(BOOLEAN NOT NULL EXPR$0, BOOLEAN EXPR$1) NOT NULL");
      tester.checkType(
          " SELECT *,(SELECT * FROM (VALUES(cast('abcd' as varchar(10))))) "
              + "FROM (VALUES(CAST('abcd' as varchar(10))))",
          "RecordType(VARCHAR(10) NOT NULL EXPR$0, VARCHAR(10) EXPR$1) NOT NULL");
      tester.checkType(
          "SELECT *,"
              + "  (SELECT * FROM (VALUES(TIMESTAMP '2006-01-01 12:00:05'))) "
              + "FROM (VALUES(TIMESTAMP '2006-01-01 12:00:05'))",
          "RecordType(TIMESTAMP(0) NOT NULL EXPR$0, TIMESTAMP(0) EXPR$1) NOT NULL");
    }
  }

  @Test public void testLiteralChain() {
    tester.setFor(SqlStdOperatorTable.LITERAL_CHAIN, VM_EXPAND);
    tester.checkString(
        "'buttered'\n' toast'",
        "buttered toast",
        "CHAR(14) NOT NULL");
    tester.checkString(
        "'corned'\n' beef'\n' on'\n' rye'",
        "corned beef on rye",
        "CHAR(18) NOT NULL");
    tester.checkString(
        "_latin1'Spaghetti'\n' all''Amatriciana'",
        "Spaghetti all'Amatriciana",
        "CHAR(25) NOT NULL");
    tester.checkBoolean("x'1234'\n'abcd' = x'1234abcd'", Boolean.TRUE);
    tester.checkBoolean("x'1234'\n'' = x'1234'", Boolean.TRUE);
    tester.checkBoolean("x''\n'ab' = x'ab'", Boolean.TRUE);
  }

  @Test public void testComplexLiteral() {
    tester.check("select 2 * 2 * x from (select 2 as x)",
        new SqlTests.StringTypeChecker("INTEGER NOT NULL"),
        "8",
        0);
    tester.check("select 1 * 2 * 3 * x from (select 2 as x)",
        new SqlTests.StringTypeChecker("INTEGER NOT NULL"),
        "12",
        0);
    tester.check("select 1 + 2 + 3 + 4 + x from (select 2 as x)",
        new SqlTests.StringTypeChecker("INTEGER NOT NULL"),
        "12",
        0);
  }

  @Test public void testRow() {
    tester.setFor(SqlStdOperatorTable.ROW, VM_FENNEL);
  }

  @Test public void testAndOperator() {
    tester.setFor(SqlStdOperatorTable.AND);
    tester.checkBoolean("true and false", Boolean.FALSE);
    tester.checkBoolean("true and true", Boolean.TRUE);
    tester.checkBoolean(
        "cast(null as boolean) and false",
        Boolean.FALSE);
    tester.checkBoolean(
        "false and cast(null as boolean)",
        Boolean.FALSE);
    tester.checkNull("cast(null as boolean) and true");
    tester.checkBoolean("true and (not false)", Boolean.TRUE);
  }

  @Test public void testAndOperator2() {
    tester.checkBoolean(
        "case when false then unknown else true end and true",
        Boolean.TRUE);
    tester.checkBoolean(
        "case when false then cast(null as boolean) else true end and true",
        Boolean.TRUE);
    tester.checkBoolean(
        "case when false then null else true end and true",
        Boolean.TRUE);
  }

  @Test public void testAndOperatorLazy() {
    tester.setFor(SqlStdOperatorTable.AND);

    // lazy eval returns FALSE;
    // eager eval executes RHS of AND and throws;
    // both are valid
    tester.check(
        "values 1 > 2 and sqrt(-4) = -2",
        SqlTests.BOOLEAN_TYPE_CHECKER, SqlTests.ANY_PARAMETER_CHECKER,
        new ValueOrExceptionResultChecker(
            Boolean.FALSE, INVALID_ARG_FOR_POWER, CODE_2201F));
  }

  @Test public void testConcatOperator() {
    tester.setFor(SqlStdOperatorTable.CONCAT);
    tester.checkString(" 'a'||'b' ", "ab", "CHAR(2) NOT NULL");
    tester.checkNull(" 'a' || cast(null as char(2)) ");
    tester.checkNull(" cast(null as char(2)) || 'b' ");
    tester.checkNull(
        " cast(null as char(1)) || cast(null as char(2)) ");

    tester.checkString(
        " x'fe'||x'df' ",
        "fedf",
        "BINARY(2) NOT NULL");
    tester.checkString(
        " cast('fe' as char(2)) || cast('df' as varchar)",
        "fedf",
        "VARCHAR NOT NULL");
    // Precision is larger than VARCHAR allows, so result is unbounded
    tester.checkString(
        " cast('fe' as char(2)) || cast('df' as varchar(65535))",
        "fedf",
        "VARCHAR NOT NULL");
    tester.checkString(
        " cast('fe' as char(2)) || cast('df' as varchar(33333))",
        "fedf",
        "VARCHAR(33335) NOT NULL");
    tester.checkNull("x'ff' || cast(null as varbinary)");
    tester.checkNull(" cast(null as ANY) || cast(null as ANY) ");
  }

  @Test public void testModOperator() {
    // "%" is allowed under MYSQL_5 SQL conformance level
    final SqlTester tester1 = tester.withConformance(SqlConformanceEnum.MYSQL_5);
    tester1.setFor(SqlStdOperatorTable.PERCENT_REMAINDER);
    tester1.checkScalarExact("4%2", "0");
    tester1.checkScalarExact("8%5", "3");
    tester1.checkScalarExact("-12%7", "-5");
    tester1.checkScalarExact("-12%-7", "-5");
    tester1.checkScalarExact("12%-7", "5");
    tester1.checkScalarExact(
        "cast(12 as tinyint) % cast(-7 as tinyint)",
        "TINYINT NOT NULL",
        "5");
    if (!DECIMAL) {
      return;
    }
    tester1.checkScalarExact(
        "cast(9 as decimal(2, 0)) % 7",
        "INTEGER NOT NULL",
        "2");
    tester1.checkScalarExact(
        "7 % cast(9 as decimal(2, 0))",
        "DECIMAL(2, 0) NOT NULL",
        "7");
    tester1.checkScalarExact(
        "cast(-9 as decimal(2, 0)) % cast(7 as decimal(1, 0))",
        "DECIMAL(1, 0) NOT NULL",
        "-2");
  }

  @Test public void testModPrecedence() {
    // "%" is allowed under MYSQL_5 SQL conformance level
    final SqlTester tester1 = tester.withConformance(SqlConformanceEnum.MYSQL_5);
    tester1.setFor(SqlStdOperatorTable.PERCENT_REMAINDER);
    tester1.checkScalarExact("1 + 5 % 3 % 4 * 14 % 17", "12");
    tester1.checkScalarExact("(1 + 5 % 3) % 4 + 14 % 17", "17");
  }

  @Test public void testModOperatorNull() {
    // "%" is allowed under MYSQL_5 SQL conformance level
    final SqlTester tester1 = tester.withConformance(SqlConformanceEnum.MYSQL_5);
    tester1.checkNull("cast(null as integer) % 2");
    tester1.checkNull("4 % cast(null as tinyint)");
    if (!DECIMAL) {
      return;
    }
    tester1.checkNull("4 % cast(null as decimal(12,0))");
  }

  @Test public void testModOperatorDivByZero() {
    // "%" is allowed under MYSQL_5 SQL conformance level
    final SqlTester tester1 = tester.withConformance(SqlConformanceEnum.MYSQL_5);
    // The extra CASE expression is to fool Janino.  It does constant
    // reduction and will throw the divide by zero exception while
    // compiling the expression.  The test frame work would then issue
    // unexpected exception occurred during "validation".  You cannot
    // submit as non-runtime because the janino exception does not have
    // error position information and the framework is unhappy with that.
    tester1.checkFails(
        "3 % case 'a' when 'a' then 0 end", DIVISION_BY_ZERO_MESSAGE, true);
  }

  @Test public void testDivideOperator() {
    tester.setFor(SqlStdOperatorTable.DIVIDE);
    tester.checkScalarExact(
        "10 / 5",
        "INTEGER NOT NULL",
        "2");
    tester.checkScalarExact(
        "-10 / 5",
        "INTEGER NOT NULL",
        "-2");
    tester.checkScalarExact(
        "-10 / 5.0",
        "DECIMAL(17, 6) NOT NULL",
        "-2");
    tester.checkScalarApprox(
        " cast(10.0 as double) / 5",
        "DOUBLE NOT NULL",
        2.0,
        0);
    tester.checkScalarApprox(
        " cast(10.0 as real) / 4",
        "REAL NOT NULL",
        2.5,
        0);
    tester.checkScalarApprox(
        " 6.0 / cast(10.0 as real) ",
        "DOUBLE NOT NULL",
        0.6,
        0);
    tester.checkScalarExact(
        "10.0 / 5.0",
        "DECIMAL(9, 6) NOT NULL",
        "2");
    if (DECIMAL) {
      tester.checkScalarExact(
          "1.0 / 3.0",
          "DECIMAL(8, 6) NOT NULL",
          "0.333333");
      tester.checkScalarExact(
          "100.1 / 0.0001",
          "DECIMAL(14, 7) NOT NULL",
          "1001000.0000000");
      tester.checkScalarExact(
          "100.1 / 0.00000001",
          "DECIMAL(19, 8) NOT NULL",
          "10010000000.00000000");
    }
    tester.checkNull("1e1 / cast(null as float)");

    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails(
          "100.1 / 0.00000000000000001", OUT_OF_RANGE_MESSAGE,
          true);
    }
  }

  @Test public void testDivideOperatorIntervals() {
    tester.checkScalar(
        "interval '-2:2' hour to minute / 3",
        "-0:41",
        "INTERVAL HOUR TO MINUTE NOT NULL");
    tester.checkScalar(
        "interval '2:5:12' hour to second / 2 / -3",
        "-0:20:52.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    tester.checkNull(
        "interval '2' day / cast(null as bigint)");
    tester.checkNull(
        "cast(null as interval month) / 2");
    tester.checkScalar(
        "interval '3-3' year to month / 15e-1",
        "+2-02",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "interval '3-4' year to month / 4.5",
        "+0-09",
        "INTERVAL YEAR TO MONTH NOT NULL");
  }

  @Test public void testEqualsOperator() {
    tester.setFor(SqlStdOperatorTable.EQUALS);
    tester.checkBoolean("1=1", Boolean.TRUE);
    tester.checkBoolean("1=1.0", Boolean.TRUE);
    tester.checkBoolean("1.34=1.34", Boolean.TRUE);
    tester.checkBoolean("1=1.34", Boolean.FALSE);
    tester.checkBoolean("1e2=100e0", Boolean.TRUE);
    tester.checkBoolean("1e2=101", Boolean.FALSE);
    tester.checkBoolean(
        "cast(1e2 as real)=cast(101 as bigint)",
        Boolean.FALSE);
    tester.checkBoolean("'a'='b'", Boolean.FALSE);
    tester.checkBoolean("true = true", Boolean.TRUE);
    tester.checkBoolean("true = false", Boolean.FALSE);
    tester.checkBoolean("false = true", Boolean.FALSE);
    tester.checkBoolean("false = false", Boolean.TRUE);
    tester.checkBoolean(
        "cast('a' as varchar(30))=cast('a' as varchar(30))",
        Boolean.TRUE);
    tester.checkBoolean(
        "cast('a ' as varchar(30))=cast('a' as varchar(30))",
        Boolean.FALSE);
    tester.checkBoolean(
        "cast(' a' as varchar(30))=cast(' a' as varchar(30))",
        Boolean.TRUE);
    tester.checkBoolean(
        "cast('a ' as varchar(15))=cast('a ' as varchar(30))",
        Boolean.TRUE);
    tester.checkBoolean(
        "cast(' ' as varchar(3))=cast(' ' as varchar(2))",
        Boolean.TRUE);
    tester.checkBoolean(
        "cast('abcd' as varchar(2))='ab'",
        Boolean.TRUE);
    tester.checkBoolean(
        "cast('a' as varchar(30))=cast('b' as varchar(30))",
        Boolean.FALSE);
    tester.checkBoolean(
        "cast('a' as varchar(30))=cast('a' as varchar(15))",
        Boolean.TRUE);
    tester.checkNull("cast(null as boolean)=cast(null as boolean)");
    tester.checkNull("cast(null as integer)=1");
    tester.checkNull("cast(null as varchar(10))='a'");
  }

  @Test public void testEqualsOperatorInterval() {
    tester.checkBoolean(
        "interval '2' day = interval '1' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' day = interval '2' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2:2:2' hour to second = interval '2' hour",
        Boolean.FALSE);
    tester.checkNull(
        "cast(null as interval hour) = interval '2' minute");
  }

  @Test public void testGreaterThanOperator() {
    tester.setFor(SqlStdOperatorTable.GREATER_THAN);
    tester.checkBoolean("1>2", Boolean.FALSE);
    tester.checkBoolean(
        "cast(-1 as TINYINT)>cast(1 as TINYINT)",
        Boolean.FALSE);
    tester.checkBoolean(
        "cast(1 as SMALLINT)>cast(1 as SMALLINT)",
        Boolean.FALSE);
    tester.checkBoolean("2>1", Boolean.TRUE);
    tester.checkBoolean("1.1>1.2", Boolean.FALSE);
    tester.checkBoolean("-1.1>-1.2", Boolean.TRUE);
    tester.checkBoolean("1.1>1.1", Boolean.FALSE);
    tester.checkBoolean("1.2>1", Boolean.TRUE);
    tester.checkBoolean("1.1e1>1.2e1", Boolean.FALSE);
    tester.checkBoolean(
        "cast(-1.1 as real) > cast(-1.2 as real)",
        Boolean.TRUE);
    tester.checkBoolean("1.1e2>1.1e2", Boolean.FALSE);
    tester.checkBoolean("1.2e0>1", Boolean.TRUE);
    tester.checkBoolean("cast(1.2e0 as real)>1", Boolean.TRUE);
    tester.checkBoolean("true>false", Boolean.TRUE);
    tester.checkBoolean("true>true", Boolean.FALSE);
    tester.checkBoolean("false>false", Boolean.FALSE);
    tester.checkBoolean("false>true", Boolean.FALSE);
    tester.checkNull("3.0>cast(null as double)");

    tester.checkBoolean(
        "DATE '2013-02-23' > DATE '1945-02-24'", Boolean.TRUE);
    tester.checkBoolean(
        "DATE '2013-02-23' > CAST(NULL AS DATE)", null);

    tester.checkBoolean("x'0A000130'>x'0A0001B0'", Boolean.FALSE);
  }

  @Test public void testGreaterThanOperatorIntervals() {
    tester.checkBoolean(
        "interval '2' day > interval '1' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' day > interval '5' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2 2:2:2' day to second > interval '2' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' day > interval '2' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' day > interval '-2' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' day > interval '2' hour",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' minute > interval '2' hour",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' second > interval '2' minute",
        Boolean.FALSE);
    tester.checkNull(
        "cast(null as interval hour) > interval '2' minute");
    tester.checkNull(
        "interval '2:2' hour to minute > cast(null as interval second)");
  }

  @Test public void testIsDistinctFromOperator() {
    tester.setFor(
        SqlStdOperatorTable.IS_DISTINCT_FROM,
        VM_EXPAND);
    tester.checkBoolean("1 is distinct from 1", Boolean.FALSE);
    tester.checkBoolean("1 is distinct from 1.0", Boolean.FALSE);
    tester.checkBoolean("1 is distinct from 2", Boolean.TRUE);
    tester.checkBoolean(
        "cast(null as integer) is distinct from 2",
        Boolean.TRUE);
    tester.checkBoolean(
        "cast(null as integer) is distinct from cast(null as integer)",
        Boolean.FALSE);
    tester.checkBoolean("1.23 is distinct from 1.23", Boolean.FALSE);
    tester.checkBoolean("1.23 is distinct from 5.23", Boolean.TRUE);
    tester.checkBoolean(
        "-23e0 is distinct from -2.3e1",
        Boolean.FALSE);

    // IS DISTINCT FROM not implemented for ROW yet
    if (false) {
      tester.checkBoolean(
          "row(1,1) is distinct from row(1,1)",
          true);
      tester.checkBoolean(
          "row(1,1) is distinct from row(1,2)",
          false);
    }

    // Intervals
    tester.checkBoolean(
        "interval '2' day is distinct from interval '1' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '10' hour is distinct from interval '10' hour",
        Boolean.FALSE);
  }

  @Test public void testIsNotDistinctFromOperator() {
    tester.setFor(
        SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        VM_EXPAND);
    tester.checkBoolean("1 is not distinct from 1", Boolean.TRUE);
    tester.checkBoolean("1 is not distinct from 1.0", Boolean.TRUE);
    tester.checkBoolean("1 is not distinct from 2", Boolean.FALSE);
    tester.checkBoolean(
        "cast(null as integer) is not distinct from 2",
        Boolean.FALSE);
    tester.checkBoolean(
        "cast(null as integer) is not distinct from cast(null as integer)",
        Boolean.TRUE);
    tester.checkBoolean(
        "1.23 is not distinct from 1.23",
        Boolean.TRUE);
    tester.checkBoolean(
        "1.23 is not distinct from 5.23",
        Boolean.FALSE);
    tester.checkBoolean(
        "-23e0 is not distinct from -2.3e1",
        Boolean.TRUE);

    // IS NOT DISTINCT FROM not implemented for ROW yet
    if (false) {
      tester.checkBoolean(
          "row(1,1) is not distinct from row(1,1)",
          false);
      tester.checkBoolean(
          "row(1,1) is not distinct from row(1,2)",
          true);
    }

    // Intervals
    tester.checkBoolean(
        "interval '2' day is not distinct from interval '1' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '10' hour is not distinct from interval '10' hour",
        Boolean.TRUE);
  }

  @Test public void testGreaterThanOrEqualOperator() {
    tester.setFor(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
    tester.checkBoolean("1>=2", Boolean.FALSE);
    tester.checkBoolean("-1>=1", Boolean.FALSE);
    tester.checkBoolean("1>=1", Boolean.TRUE);
    tester.checkBoolean("2>=1", Boolean.TRUE);
    tester.checkBoolean("1.1>=1.2", Boolean.FALSE);
    tester.checkBoolean("-1.1>=-1.2", Boolean.TRUE);
    tester.checkBoolean("1.1>=1.1", Boolean.TRUE);
    tester.checkBoolean("1.2>=1", Boolean.TRUE);
    tester.checkBoolean("1.2e4>=1e5", Boolean.FALSE);
    tester.checkBoolean("1.2e4>=cast(1e5 as real)", Boolean.FALSE);
    tester.checkBoolean("1.2>=cast(1e5 as double)", Boolean.FALSE);
    tester.checkBoolean("120000>=cast(1e5 as real)", Boolean.TRUE);
    tester.checkBoolean("true>=false", Boolean.TRUE);
    tester.checkBoolean("true>=true", Boolean.TRUE);
    tester.checkBoolean("false>=false", Boolean.TRUE);
    tester.checkBoolean("false>=true", Boolean.FALSE);
    tester.checkNull("cast(null as real)>=999");
    tester.checkBoolean("x'0A000130'>=x'0A0001B0'", Boolean.FALSE);
    tester.checkBoolean("x'0A0001B0'>=x'0A0001B0'", Boolean.TRUE);
  }

  @Test public void testGreaterThanOrEqualOperatorIntervals() {
    tester.checkBoolean(
        "interval '2' day >= interval '1' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' day >= interval '5' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2 2:2:2' day to second >= interval '2' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' day >= interval '2' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' day >= interval '-2' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' day >= interval '2' hour",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' minute >= interval '2' hour",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' second >= interval '2' minute",
        Boolean.FALSE);
    tester.checkNull(
        "cast(null as interval hour) >= interval '2' minute");
    tester.checkNull(
        "interval '2:2' hour to minute >= cast(null as interval second)");
  }

  @Test public void testInOperator() {
    tester.setFor(SqlStdOperatorTable.IN, VM_EXPAND);
    tester.checkBoolean("1 in (0, 1, 2)", true);
    tester.checkBoolean("3 in (0, 1, 2)", false);
    tester.checkBoolean("cast(null as integer) in (0, 1, 2)", null);
    tester.checkBoolean(
        "cast(null as integer) in (0, cast(null as integer), 2)",
        null);
    if (Bug.FRG327_FIXED) {
      tester.checkBoolean(
          "cast(null as integer) in (0, null, 2)",
          null);
      tester.checkBoolean("1 in (0, null, 2)", null);
    }

    if (!enable) {
      return;
    }
    // AND has lower precedence than IN
    tester.checkBoolean("false and true in (false, false)", false);

    if (!Bug.TODO_FIXED) {
      return;
    }
    tester.checkFails(
        "'foo' in (^)^",
        "(?s).*Encountered \"\\)\" at .*",
        false);
  }

  @Test public void testNotInOperator() {
    tester.setFor(SqlStdOperatorTable.NOT_IN, VM_EXPAND);
    tester.checkBoolean("1 not in (0, 1, 2)", false);
    tester.checkBoolean("3 not in (0, 1, 2)", true);
    if (!enable) {
      return;
    }
    tester.checkBoolean(
        "cast(null as integer) not in (0, 1, 2)",
        null);
    tester.checkBoolean(
        "cast(null as integer) not in (0, cast(null as integer), 2)",
        null);
    if (Bug.FRG327_FIXED) {
      tester.checkBoolean(
          "cast(null as integer) not in (0, null, 2)",
          null);
      tester.checkBoolean("1 not in (0, null, 2)", null);
    }

    // AND has lower precedence than NOT IN
    tester.checkBoolean("true and false not in (true, true)", true);

    if (!Bug.TODO_FIXED) {
      return;
    }
    tester.checkFails(
        "'foo' not in (^)^",
        "(?s).*Encountered \"\\)\" at .*",
        false);
  }

  @Test public void testOverlapsOperator() {
    tester.setFor(SqlStdOperatorTable.OVERLAPS, VM_EXPAND);
    tester.checkBoolean(
        "(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', interval '1' year)",
        Boolean.TRUE);
    tester.checkBoolean(
        "(date '1-2-3', date '1-2-3') overlaps (date '4-5-6', interval '1' year)",
        Boolean.FALSE);
    tester.checkBoolean(
        "(date '1-2-3', date '4-5-6') overlaps (date '2-2-3', date '3-4-5')",
        Boolean.TRUE);
    tester.checkNull(
        "(cast(null as date), date '1-2-3') overlaps (date '1-2-3', interval '1' year)");
    tester.checkNull(
        "(date '1-2-3', date '1-2-3') overlaps (date '1-2-3', cast(null as date))");

    tester.checkBoolean(
        "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time '1:2:3')",
        Boolean.TRUE);
    tester.checkBoolean(
        "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', time '1:2:2')",
        Boolean.TRUE);
    tester.checkBoolean(
        "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', interval '2' hour)",
        Boolean.FALSE);
    tester.checkNull(
        "(time '1:2:3', cast(null as time)) overlaps (time '23:59:59', time '1:2:3')");
    tester.checkNull(
        "(time '1:2:3', interval '1' second) overlaps (time '23:59:59', cast(null as interval hour))");

    tester.checkBoolean(
        "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)",
        Boolean.TRUE);
    tester.checkBoolean(
        "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (timestamp '2-2-3 4:5:6', interval '1 2:3:4.5' day to second)",
        Boolean.FALSE);
    tester.checkNull(
        "(timestamp '1-2-3 4:5:6', cast(null as interval day) ) overlaps (timestamp '1-2-3 4:5:6', interval '1 2:3:4.5' day to second)");
    tester.checkNull(
        "(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) overlaps (cast(null as timestamp), interval '1 2:3:4.5' day to second)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-715">[CALCITE-715]
   * Add PERIOD type constructor and period operators (CONTAINS, PRECEDES,
   * etc.)</a>.
   *
   * <p>Tests OVERLAP and similar period operators CONTAINS, EQUALS, PRECEDES,
   * SUCCEEDS, IMMEDIATELY PRECEDES, IMMEDIATELY SUCCEEDS for DATE, TIME and
   * TIMESTAMP values. */
  @Test public void testPeriodOperators() {
    String[] times = {
        "TIME '01:00:00'",
        "TIME '02:00:00'",
        "TIME '03:00:00'",
        "TIME '04:00:00'",
    };
    String[] dates = {
        "DATE '1970-01-01'",
        "DATE '1970-02-01'",
        "DATE '1970-03-01'",
        "DATE '1970-04-01'",
    };
    String[] timestamps = {
        "TIMESTAMP '1970-01-01 00:00:00'",
        "TIMESTAMP '1970-02-01 00:00:00'",
        "TIMESTAMP '1970-03-01 00:00:00'",
        "TIMESTAMP '1970-04-01 00:00:00'",
    };
    checkOverlaps(new OverlapChecker(times));
    checkOverlaps(new OverlapChecker(dates));
    checkOverlaps(new OverlapChecker(timestamps));
  }

  private void checkOverlaps(OverlapChecker c) {
    c.isTrue("($0,$0) OVERLAPS ($0,$0)");
    c.isFalse("($0,$1) OVERLAPS ($2,$3)");
    c.isTrue("($0,$1) OVERLAPS ($1,$2)");
    c.isTrue("($0,$2) OVERLAPS ($1,$3)");
    c.isTrue("($0,$2) OVERLAPS ($3,$1)");
    c.isTrue("($2,$0) OVERLAPS ($3,$1)");
    c.isFalse("($3,$2) OVERLAPS ($1,$0)");
    c.isTrue("($2,$3) OVERLAPS ($0,$2)");
    c.isTrue("($2,$3) OVERLAPS ($2,$0)");
    c.isTrue("($3,$2) OVERLAPS ($2,$0)");
    c.isTrue("($0,$2) OVERLAPS ($2,$0)");
    c.isTrue("($0,$3) OVERLAPS ($1,$3)");
    c.isTrue("($0,$3) OVERLAPS ($3,$3)");

    c.isTrue("($0,$0) CONTAINS ($0,$0)");
    c.isFalse("($0,$1) CONTAINS ($2,$3)");
    c.isFalse("($0,$1) CONTAINS ($1,$2)");
    c.isFalse("($0,$2) CONTAINS ($1,$3)");
    c.isFalse("($0,$2) CONTAINS ($3,$1)");
    c.isFalse("($2,$0) CONTAINS ($3,$1)");
    c.isFalse("($3,$2) CONTAINS ($1,$0)");
    c.isFalse("($2,$3) CONTAINS ($0,$2)");
    c.isFalse("($2,$3) CONTAINS ($2,$0)");
    c.isFalse("($3,$2) CONTAINS ($2,$0)");
    c.isTrue("($0,$2) CONTAINS ($2,$0)");
    c.isTrue("($0,$3) CONTAINS ($1,$3)");
    c.isTrue("($0,$3) CONTAINS ($3,$3)");
    c.isTrue("($3,$0) CONTAINS ($3,$3)");
    c.isTrue("($3,$0) CONTAINS ($0,$0)");

    c.isTrue("($0,$0) CONTAINS $0");
    c.isTrue("($3,$0) CONTAINS $0");
    c.isTrue("($3,$0) CONTAINS $1");
    c.isTrue("($3,$0) CONTAINS $2");
    c.isTrue("($3,$0) CONTAINS $3");
    c.isTrue("($0,$3) CONTAINS $0");
    c.isTrue("($0,$3) CONTAINS $1");
    c.isTrue("($0,$3) CONTAINS $2");
    c.isTrue("($0,$3) CONTAINS $3");
    c.isFalse("($1,$3) CONTAINS $0");
    c.isFalse("($1,$2) CONTAINS $3");

    c.isTrue("($0,$0) EQUALS ($0,$0)");
    c.isFalse("($0,$1) EQUALS ($2,$3)");
    c.isFalse("($0,$1) EQUALS ($1,$2)");
    c.isFalse("($0,$2) EQUALS ($1,$3)");
    c.isFalse("($0,$2) EQUALS ($3,$1)");
    c.isFalse("($2,$0) EQUALS ($3,$1)");
    c.isFalse("($3,$2) EQUALS ($1,$0)");
    c.isFalse("($2,$3) EQUALS ($0,$2)");
    c.isFalse("($2,$3) EQUALS ($2,$0)");
    c.isFalse("($3,$2) EQUALS ($2,$0)");
    c.isTrue("($0,$2) EQUALS ($2,$0)");
    c.isFalse("($0,$3) EQUALS ($1,$3)");
    c.isFalse("($0,$3) EQUALS ($3,$3)");
    c.isFalse("($3,$0) EQUALS ($3,$3)");
    c.isFalse("($3,$0) EQUALS ($0,$0)");

    c.isTrue("($0,$0) PRECEDES ($0,$0)");
    c.isTrue("($0,$1) PRECEDES ($2,$3)");
    c.isTrue("($0,$1) PRECEDES ($1,$2)");
    c.isFalse("($0,$2) PRECEDES ($1,$3)");
    c.isFalse("($0,$2) PRECEDES ($3,$1)");
    c.isFalse("($2,$0) PRECEDES ($3,$1)");
    c.isFalse("($3,$2) PRECEDES ($1,$0)");
    c.isFalse("($2,$3) PRECEDES ($0,$2)");
    c.isFalse("($2,$3) PRECEDES ($2,$0)");
    c.isFalse("($3,$2) PRECEDES ($2,$0)");
    c.isFalse("($0,$2) PRECEDES ($2,$0)");
    c.isFalse("($0,$3) PRECEDES ($1,$3)");
    c.isTrue("($0,$3) PRECEDES ($3,$3)");
    c.isTrue("($3,$0) PRECEDES ($3,$3)");
    c.isFalse("($3,$0) PRECEDES ($0,$0)");

    c.isTrue("($0,$0) SUCCEEDS ($0,$0)");
    c.isFalse("($0,$1) SUCCEEDS ($2,$3)");
    c.isFalse("($0,$1) SUCCEEDS ($1,$2)");
    c.isFalse("($0,$2) SUCCEEDS ($1,$3)");
    c.isFalse("($0,$2) SUCCEEDS ($3,$1)");
    c.isFalse("($2,$0) SUCCEEDS ($3,$1)");
    c.isTrue("($3,$2) SUCCEEDS ($1,$0)");
    c.isTrue("($2,$3) SUCCEEDS ($0,$2)");
    c.isTrue("($2,$3) SUCCEEDS ($2,$0)");
    c.isTrue("($3,$2) SUCCEEDS ($2,$0)");
    c.isFalse("($0,$2) SUCCEEDS ($2,$0)");
    c.isFalse("($0,$3) SUCCEEDS ($1,$3)");
    c.isFalse("($0,$3) SUCCEEDS ($3,$3)");
    c.isFalse("($3,$0) SUCCEEDS ($3,$3)");
    c.isTrue("($3,$0) SUCCEEDS ($0,$0)");

    c.isTrue("($0,$0) IMMEDIATELY PRECEDES ($0,$0)");
    c.isFalse("($0,$1) IMMEDIATELY PRECEDES ($2,$3)");
    c.isTrue("($0,$1) IMMEDIATELY PRECEDES ($1,$2)");
    c.isFalse("($0,$2) IMMEDIATELY PRECEDES ($1,$3)");
    c.isFalse("($0,$2) IMMEDIATELY PRECEDES ($3,$1)");
    c.isFalse("($2,$0) IMMEDIATELY PRECEDES ($3,$1)");
    c.isFalse("($3,$2) IMMEDIATELY PRECEDES ($1,$0)");
    c.isFalse("($2,$3) IMMEDIATELY PRECEDES ($0,$2)");
    c.isFalse("($2,$3) IMMEDIATELY PRECEDES ($2,$0)");
    c.isFalse("($3,$2) IMMEDIATELY PRECEDES ($2,$0)");
    c.isFalse("($0,$2) IMMEDIATELY PRECEDES ($2,$0)");
    c.isFalse("($0,$3) IMMEDIATELY PRECEDES ($1,$3)");
    c.isTrue("($0,$3) IMMEDIATELY PRECEDES ($3,$3)");
    c.isTrue("($3,$0) IMMEDIATELY PRECEDES ($3,$3)");
    c.isFalse("($3,$0) IMMEDIATELY PRECEDES ($0,$0)");

    c.isTrue("($0,$0) IMMEDIATELY SUCCEEDS ($0,$0)");
    c.isFalse("($0,$1) IMMEDIATELY SUCCEEDS ($2,$3)");
    c.isFalse("($0,$1) IMMEDIATELY SUCCEEDS ($1,$2)");
    c.isFalse("($0,$2) IMMEDIATELY SUCCEEDS ($1,$3)");
    c.isFalse("($0,$2) IMMEDIATELY SUCCEEDS ($3,$1)");
    c.isFalse("($2,$0) IMMEDIATELY SUCCEEDS ($3,$1)");
    c.isFalse("($3,$2) IMMEDIATELY SUCCEEDS ($1,$0)");
    c.isTrue("($2,$3) IMMEDIATELY SUCCEEDS ($0,$2)");
    c.isTrue("($2,$3) IMMEDIATELY SUCCEEDS ($2,$0)");
    c.isTrue("($3,$2) IMMEDIATELY SUCCEEDS ($2,$0)");
    c.isFalse("($0,$2) IMMEDIATELY SUCCEEDS ($2,$0)");
    c.isFalse("($0,$3) IMMEDIATELY SUCCEEDS ($1,$3)");
    c.isFalse("($0,$3) IMMEDIATELY SUCCEEDS ($3,$3)");
    c.isFalse("($3,$0) IMMEDIATELY SUCCEEDS ($3,$3)");
    c.isTrue("($3,$0) IMMEDIATELY SUCCEEDS ($0,$0)");
  }

  @Test public void testLessThanOperator() {
    tester.setFor(SqlStdOperatorTable.LESS_THAN);
    tester.checkBoolean("1<2", Boolean.TRUE);
    tester.checkBoolean("-1<1", Boolean.TRUE);
    tester.checkBoolean("1<1", Boolean.FALSE);
    tester.checkBoolean("2<1", Boolean.FALSE);
    tester.checkBoolean("1.1<1.2", Boolean.TRUE);
    tester.checkBoolean("-1.1<-1.2", Boolean.FALSE);
    tester.checkBoolean("1.1<1.1", Boolean.FALSE);
    tester.checkBoolean("cast(1.1 as real)<1", Boolean.FALSE);
    tester.checkBoolean("cast(1.1 as real)<1.1", Boolean.FALSE);
    tester.checkBoolean(
        "cast(1.1 as real)<cast(1.2 as real)",
        Boolean.TRUE);
    tester.checkBoolean("-1.1e-1<-1.2e-1", Boolean.FALSE);
    tester.checkBoolean(
        "cast(1.1 as real)<cast(1.1 as double)",
        Boolean.FALSE);
    tester.checkBoolean("true<false", Boolean.FALSE);
    tester.checkBoolean("true<true", Boolean.FALSE);
    tester.checkBoolean("false<false", Boolean.FALSE);
    tester.checkBoolean("false<true", Boolean.TRUE);
    tester.checkNull("123<cast(null as bigint)");
    tester.checkNull("cast(null as tinyint)<123");
    tester.checkNull("cast(null as integer)<1.32");
    tester.checkBoolean("x'0A000130'<x'0A0001B0'", Boolean.TRUE);
  }

  @Test public void testLessThanOperatorInterval() {
    if (!DECIMAL) {
      return;
    }
    tester.checkBoolean(
        "interval '2' day < interval '1' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' day < interval '5' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2 2:2:2' day to second < interval '2' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' day < interval '2' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' day < interval '-2' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' day < interval '2' hour",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' minute < interval '2' hour",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' second < interval '2' minute",
        Boolean.TRUE);
    tester.checkNull(
        "cast(null as interval hour) < interval '2' minute");
    tester.checkNull(
        "interval '2:2' hour to minute < cast(null as interval second)");
  }

  @Test public void testLessThanOrEqualOperator() {
    tester.setFor(SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
    tester.checkBoolean("1<=2", Boolean.TRUE);
    tester.checkBoolean("1<=1", Boolean.TRUE);
    tester.checkBoolean("-1<=1", Boolean.TRUE);
    tester.checkBoolean("2<=1", Boolean.FALSE);
    tester.checkBoolean("1.1<=1.2", Boolean.TRUE);
    tester.checkBoolean("-1.1<=-1.2", Boolean.FALSE);
    tester.checkBoolean("1.1<=1.1", Boolean.TRUE);
    tester.checkBoolean("1.2<=1", Boolean.FALSE);
    tester.checkBoolean("1<=cast(1e2 as real)", Boolean.TRUE);
    tester.checkBoolean("1000<=cast(1e2 as real)", Boolean.FALSE);
    tester.checkBoolean("1.2e1<=1e2", Boolean.TRUE);
    tester.checkBoolean("1.2e1<=cast(1e2 as real)", Boolean.TRUE);
    tester.checkBoolean("true<=false", Boolean.FALSE);
    tester.checkBoolean("true<=true", Boolean.TRUE);
    tester.checkBoolean("false<=false", Boolean.TRUE);
    tester.checkBoolean("false<=true", Boolean.TRUE);
    tester.checkNull("cast(null as real)<=cast(1 as real)");
    tester.checkNull("cast(null as integer)<=3");
    tester.checkNull("3<=cast(null as smallint)");
    tester.checkNull("cast(null as integer)<=1.32");
    tester.checkBoolean("x'0A000130'<=x'0A0001B0'", Boolean.TRUE);
    tester.checkBoolean("x'0A0001B0'<=x'0A0001B0'", Boolean.TRUE);
  }

  @Test public void testLessThanOrEqualOperatorInterval() {
    tester.checkBoolean(
        "interval '2' day <= interval '1' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' day <= interval '5' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2 2:2:2' day to second <= interval '2' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' day <= interval '2' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' day <= interval '-2' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' day <= interval '2' hour",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2' minute <= interval '2' hour",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' second <= interval '2' minute",
        Boolean.TRUE);
    tester.checkNull(
        "cast(null as interval hour) <= interval '2' minute");
    tester.checkNull(
        "interval '2:2' hour to minute <= cast(null as interval second)");
  }

  @Test public void testMinusOperator() {
    tester.setFor(SqlStdOperatorTable.MINUS);
    tester.checkScalarExact("-2-1", "-3");
    tester.checkScalarExact("-2-1-5", "-8");
    tester.checkScalarExact("2-1", "1");
    tester.checkScalarApprox(
        "cast(2.0 as double) -1",
        "DOUBLE NOT NULL",
        1,
        0);
    tester.checkScalarApprox(
        "cast(1 as smallint)-cast(2.0 as real)",
        "REAL NOT NULL",
        -1,
        0);
    tester.checkScalarApprox(
        "2.4-cast(2.0 as real)",
        "DOUBLE NOT NULL",
        0.4,
        0.00000001);
    tester.checkScalarExact("1-2", "-1");
    tester.checkScalarExact(
        "10.0 - 5.0",
        "DECIMAL(4, 1) NOT NULL",
        "5.0");
    tester.checkScalarExact(
        "19.68 - 4.2",
        "DECIMAL(5, 2) NOT NULL",
        "15.48");
    tester.checkNull("1e1-cast(null as double)");
    tester.checkNull("cast(null as tinyint) - cast(null as smallint)");

    // TODO: Fix bug
    if (Bug.FNL25_FIXED) {
      // Should throw out of range error
      tester.checkFails(
          "cast(100 as tinyint) - cast(-100 as tinyint)", OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(-20000 as smallint) - cast(20000 as smallint)",
          OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(1.5e9 as integer) - cast(-1.5e9 as integer)",
          OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(-5e18 as bigint) - cast(5e18 as bigint)", OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(5e18 as decimal(19,0)) - cast(-5e18 as decimal(19,0))",
          OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(-5e8 as decimal(19,10)) - cast(5e8 as decimal(19,10))",
          OUT_OF_RANGE_MESSAGE,
          true);
    }
  }

  @Test public void testMinusIntervalOperator() {
    tester.setFor(SqlStdOperatorTable.MINUS);
    tester.checkScalar(
        "interval '2' day - interval '1' day",
        "+1",
        "INTERVAL DAY NOT NULL");
    tester.checkScalar(
        "interval '2' day - interval '1' minute",
        "+1 23:59",
        "INTERVAL DAY TO MINUTE NOT NULL");
    tester.checkScalar(
        "interval '2' year - interval '1' month",
        "+1-11",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "interval '2' year - interval '1' month - interval '3' year",
        "-1-01",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkNull(
        "cast(null as interval day) + interval '2' hour");

    // Datetime minus interval
    tester.checkScalar(
        "time '12:03:01' - interval '1:1' hour to minute",
        "11:02:01",
        "TIME(0) NOT NULL");
    // Per [CALCITE-1632] Return types of datetime + interval
    // make sure that TIME values say in range
    tester.checkScalar(
        "time '12:03:01' - interval '1' day",
        "12:03:01",
        "TIME(0) NOT NULL");
    tester.checkScalar(
        "time '12:03:01' - interval '25' hour",
        "11:03:01",
        "TIME(0) NOT NULL");
    tester.checkScalar(
        "time '12:03:03' - interval '25:0:1' hour to second",
        "11:03:02",
        "TIME(0) NOT NULL");
    tester.checkScalar(
        "date '2005-03-02' - interval '5' day",
        "2005-02-25",
        "DATE NOT NULL");
    tester.checkScalar(
        "date '2005-03-02' - interval '5' day",
        "2005-02-25",
        "DATE NOT NULL");
    tester.checkScalar(
        "date '2005-03-02' - interval '5' hour",
        "2005-03-02",
        "DATE NOT NULL");
    tester.checkScalar(
        "date '2005-03-02' - interval '25' hour",
        "2005-03-01",
        "DATE NOT NULL");
    tester.checkScalar(
        "date '2005-03-02' - interval '25:45' hour to minute",
        "2005-03-01",
        "DATE NOT NULL");
    tester.checkScalar(
        "date '2005-03-02' - interval '25:45:54' hour to second",
        "2005-03-01",
        "DATE NOT NULL");
    tester.checkScalar(
        "timestamp '2003-08-02 12:54:01' - interval '-4 2:4' day to minute",
        "2003-08-06 14:58:01",
        "TIMESTAMP(0) NOT NULL");

    // Datetime minus year-month interval
    tester.checkScalar(
        "timestamp '2003-08-02 12:54:01' - interval '12' year",
        "1991-08-02 12:54:01",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "date '2003-08-02' - interval '12' year",
        "1991-08-02",
        "DATE NOT NULL");
    tester.checkScalar(
        "date '2003-08-02' - interval '12-3' year to month",
        "1991-05-02",
        "DATE NOT NULL");
  }

  @Test public void testMinusDateOperator() {
    tester.setFor(SqlStdOperatorTable.MINUS_DATE);
    tester.checkScalar(
        "(time '12:03:34' - time '11:57:23') minute to second",
        "+6:11.000000",
        "INTERVAL MINUTE TO SECOND NOT NULL");
    tester.checkScalar(
        "(time '12:03:23' - time '11:57:23') minute",
        "+6",
        "INTERVAL MINUTE NOT NULL");
    tester.checkScalar(
        "(time '12:03:34' - time '11:57:23') minute",
        "+6",
        "INTERVAL MINUTE NOT NULL");
    tester.checkScalar(
        "(timestamp '2004-05-01 12:03:34' - timestamp '2004-04-29 11:57:23') day to second",
        "+2 00:06:11.000000",
        "INTERVAL DAY TO SECOND NOT NULL");
    tester.checkScalar(
        "(timestamp '2004-05-01 12:03:34' - timestamp '2004-04-29 11:57:23') day to hour",
        "+2 00",
        "INTERVAL DAY TO HOUR NOT NULL");
    tester.checkScalar(
        "(date '2004-12-02' - date '2003-12-01') day",
        "+367",
        "INTERVAL DAY NOT NULL");
    tester.checkNull(
        "(cast(null as date) - date '2003-12-01') day");

    // combine '<datetime> + <interval>' with '<datetime> - <datetime>'
    tester.checkScalar(
        "timestamp '1969-04-29 0:0:0' +"
            + " (timestamp '2008-07-15 15:28:00' - "
            + "  timestamp '1969-04-29 0:0:0') day to second / 2",
        "1988-12-06 07:44:00",
        "TIMESTAMP(0) NOT NULL");

    tester.checkScalar(
        "date '1969-04-29' +"
            + " (date '2008-07-15' - "
            + "  date '1969-04-29') day / 2",
        "1988-12-06",
        "DATE NOT NULL");

    tester.checkScalar(
        "time '01:23:44' +"
            + " (time '15:28:00' - "
            + "  time '01:23:44') hour to second / 2",
        "08:25:52",
        "TIME(0) NOT NULL");

    if (Bug.DT1684_FIXED) {
      tester.checkBoolean(
          "(date '1969-04-29' +"
              + " (CURRENT_DATE - "
              + "  date '1969-04-29') day / 2) is not null",
          Boolean.TRUE);
    }
    // TODO: Add tests for year month intervals (currently not supported)
  }

  @Test public void testMultiplyOperator() {
    tester.setFor(SqlStdOperatorTable.MULTIPLY);
    tester.checkScalarExact("2*3", "6");
    tester.checkScalarExact("2*-3", "-6");
    tester.checkScalarExact("+2*3", "6");
    tester.checkScalarExact("2*0", "0");
    tester.checkScalarApprox(
        "cast(2.0 as float)*3",
        "FLOAT NOT NULL",
        6,
        0);
    tester.checkScalarApprox(
        "3*cast(2.0 as real)",
        "REAL NOT NULL",
        6,
        0);
    tester.checkScalarApprox(
        "cast(2.0 as real)*3.2",
        "DOUBLE NOT NULL",
        6.4,
        0);
    tester.checkScalarExact(
        "10.0 * 5.0",
        "DECIMAL(5, 2) NOT NULL",
        "50.00");
    tester.checkScalarExact(
        "19.68 * 4.2",
        "DECIMAL(6, 3) NOT NULL",
        "82.656");
    tester.checkNull("cast(1 as real)*cast(null as real)");
    tester.checkNull("2e-3*cast(null as integer)");
    tester.checkNull("cast(null as tinyint) * cast(4 as smallint)");

    if (Bug.FNL25_FIXED) {
      // Should throw out of range error
      tester.checkFails(
          "cast(100 as tinyint) * cast(-2 as tinyint)", OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(200 as smallint) * cast(200 as smallint)", OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(1.5e9 as integer) * cast(-2 as integer)", OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(5e9 as bigint) * cast(2e9 as bigint)", OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(2e9 as decimal(19,0)) * cast(-5e9 as decimal(19,0))",
          OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(5e4 as decimal(19,10)) * cast(2e4 as decimal(19,10))",
          OUT_OF_RANGE_MESSAGE,
          true);
    }
  }

  @Test public void testMultiplyIntervals() {
    tester.checkScalar(
        "interval '2:2' hour to minute * 3",
        "+6:06",
        "INTERVAL HOUR TO MINUTE NOT NULL");
    tester.checkScalar(
        "3 * 2 * interval '2:5:12' hour to second",
        "+12:31:12.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    tester.checkNull(
        "interval '2' day * cast(null as bigint)");
    tester.checkNull(
        "cast(null as interval month) * 2");
    if (TODO) {
      tester.checkScalar(
          "interval '3-2' year to month * 15e-1",
          "+04-09",
          "INTERVAL YEAR TO MONTH NOT NULL");
      tester.checkScalar(
          "interval '3-4' year to month * 4.5",
          "+15-00",
          "INTERVAL YEAR TO MONTH NOT NULL");
    }
  }

  @Test public void testDatePlusInterval() {
    tester.checkScalar(
        "date '2014-02-11' + interval '2' day",
        "2014-02-13",
        "DATE NOT NULL");
    // 60 days is more than 2^32 milliseconds
    tester.checkScalar(
        "date '2014-02-11' + interval '60' day",
        "2014-04-12",
        "DATE NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1864">[CALCITE-1864]
   * Allow NULL literal as argument</a>. */
  @Test public void testNullOperand() {
    checkNullOperand(tester, "=");
    checkNullOperand(tester, ">");
    checkNullOperand(tester, "<");
    checkNullOperand(tester, "<=");
    checkNullOperand(tester, ">=");
    checkNullOperand(tester, "<>");

    // "!=" is allowed under ORACLE_10 SQL conformance level
    final SqlTester tester1 =
        tester.withConformance(SqlConformanceEnum.ORACLE_10);
    checkNullOperand(tester1, "<>");
  }

  private void checkNullOperand(SqlTester tester, String op) {
    tester.checkBoolean("1 " + op + " null", null);
    tester.checkBoolean("null " + op + " -3", null);
    tester.checkBoolean("null " + op + " null", null);
  }

  @Test public void testNotEqualsOperator() {
    tester.setFor(SqlStdOperatorTable.NOT_EQUALS);
    tester.checkBoolean("1<>1", Boolean.FALSE);
    tester.checkBoolean("'a'<>'A'", Boolean.TRUE);
    tester.checkBoolean("1e0<>1e1", Boolean.TRUE);
    tester.checkNull("'a'<>cast(null as varchar(1))");

    // "!=" is not an acceptable alternative to "<>" under default SQL conformance level
    tester.checkFails(
        "1 != 1",
        "Bang equal '!=' is not allowed under the current SQL conformance level",
        false);
    // "!=" is allowed under ORACLE_10 SQL conformance level
    final SqlTester tester1 =
        tester.withConformance(SqlConformanceEnum.ORACLE_10);

    tester1.checkBoolean("1 <> 1", Boolean.FALSE);
    tester1.checkBoolean("1 != 1", Boolean.FALSE);
    tester1.checkBoolean("1 != null", null);
  }

  @Test public void testNotEqualsOperatorIntervals() {
    tester.checkBoolean(
        "interval '2' day <> interval '1' day",
        Boolean.TRUE);
    tester.checkBoolean(
        "interval '2' day <> interval '2' day",
        Boolean.FALSE);
    tester.checkBoolean(
        "interval '2:2:2' hour to second <> interval '2' hour",
        Boolean.TRUE);
    tester.checkNull(
        "cast(null as interval hour) <> interval '2' minute");
  }

  @Test public void testOrOperator() {
    tester.setFor(SqlStdOperatorTable.OR);
    tester.checkBoolean("true or false", Boolean.TRUE);
    tester.checkBoolean("false or false", Boolean.FALSE);
    tester.checkBoolean("true or cast(null as boolean)", Boolean.TRUE);
    tester.checkNull("false or cast(null as boolean)");
  }

  @Test public void testOrOperatorLazy() {
    tester.setFor(SqlStdOperatorTable.OR);

    // need to evaluate 2nd argument if first evaluates to null, therefore
    // get error
    tester.check(
        "values 1 < cast(null as integer) or sqrt(-4) = -2",
        SqlTests.BOOLEAN_TYPE_CHECKER, SqlTests.ANY_PARAMETER_CHECKER,
        new ValueOrExceptionResultChecker(
            null, INVALID_ARG_FOR_POWER, CODE_2201F));

    // Do not need to evaluate 2nd argument if first evaluates to true.
    // In eager evaluation, get error;
    // lazy evaluation returns true;
    // both are valid.
    tester.check(
        "values 1 < 2 or sqrt(-4) = -2",
        SqlTests.BOOLEAN_TYPE_CHECKER, SqlTests.ANY_PARAMETER_CHECKER,
        new ValueOrExceptionResultChecker(
            Boolean.TRUE, INVALID_ARG_FOR_POWER, CODE_2201F));

    // NULL OR FALSE --> NULL
    // In eager evaluation, get error;
    // lazy evaluation returns NULL;
    // both are valid.
    tester.check(
        "values 1 < cast(null as integer) or sqrt(4) = -2",
        SqlTests.BOOLEAN_TYPE_CHECKER, SqlTests.ANY_PARAMETER_CHECKER,
        new ValueOrExceptionResultChecker(
            null, INVALID_ARG_FOR_POWER, CODE_2201F));

    // NULL OR TRUE --> TRUE
    tester.checkBoolean(
        "1 < cast(null as integer) or sqrt(4) = 2", Boolean.TRUE);
  }

  @Test public void testPlusOperator() {
    tester.setFor(SqlStdOperatorTable.PLUS);
    tester.checkScalarExact("1+2", "3");
    tester.checkScalarExact("-1+2", "1");
    tester.checkScalarExact("1+2+3", "6");
    tester.checkScalarApprox(
        "1+cast(2.0 as double)",
        "DOUBLE NOT NULL",
        3,
        0);
    tester.checkScalarApprox(
        "1+cast(2.0 as double)+cast(6.0 as float)",
        "DOUBLE NOT NULL",
        9,
        0);
    tester.checkScalarExact(
        "10.0 + 5.0",
        "DECIMAL(4, 1) NOT NULL",
        "15.0");
    tester.checkScalarExact(
        "19.68 + 4.2",
        "DECIMAL(5, 2) NOT NULL",
        "23.88");
    tester.checkScalarExact(
        "19.68 + 4.2 + 6",
        "DECIMAL(13, 2) NOT NULL",
        "29.88");
    tester.checkScalarApprox(
        "19.68 + cast(4.2 as float)",
        "DOUBLE NOT NULL",
        23.88,
        0.02);
    tester.checkNull("cast(null as tinyint)+1");
    tester.checkNull("1e-2+cast(null as double)");

    if (Bug.FNL25_FIXED) {
      // Should throw out of range error
      tester.checkFails(
          "cast(100 as tinyint) + cast(100 as tinyint)", OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(-20000 as smallint) + cast(-20000 as smallint)",
          OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(1.5e9 as integer) + cast(1.5e9 as integer)",
          OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(5e18 as bigint) + cast(5e18 as bigint)", OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(-5e18 as decimal(19,0)) + cast(-5e18 as decimal(19,0))",
          OUT_OF_RANGE_MESSAGE,
          true);
      tester.checkFails(
          "cast(5e8 as decimal(19,10)) + cast(5e8 as decimal(19,10))",
          OUT_OF_RANGE_MESSAGE,
          true);
    }
  }

  @Test public void testPlusOperatorAny() {
    tester.setFor(SqlStdOperatorTable.PLUS);
    tester.checkScalar("1+CAST(2 AS ANY)", "3", "ANY NOT NULL");
  }

  @Test public void testPlusIntervalOperator() {
    tester.setFor(SqlStdOperatorTable.PLUS);
    tester.checkScalar(
        "interval '2' day + interval '1' day",
        "+3",
        "INTERVAL DAY NOT NULL");
    tester.checkScalar(
        "interval '2' day + interval '1' minute",
        "+2 00:01",
        "INTERVAL DAY TO MINUTE NOT NULL");
    tester.checkScalar(
        "interval '2' day + interval '5' minute + interval '-3' second",
        "+2 00:04:57.000000",
        "INTERVAL DAY TO SECOND NOT NULL");
    tester.checkScalar(
        "interval '2' year + interval '1' month",
        "+2-01",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkNull(
        "interval '2' year + cast(null as interval month)");

    // Datetime plus interval
    tester.checkScalar(
        "time '12:03:01' + interval '1:1' hour to minute",
        "13:04:01",
        "TIME(0) NOT NULL");
    // Per [CALCITE-1632] Return types of datetime + interval
    // make sure that TIME values say in range
    tester.checkScalar(
        "time '12:03:01' + interval '1' day",
        "12:03:01",
        "TIME(0) NOT NULL");
    tester.checkScalar(
        "time '12:03:01' + interval '25' hour",
        "13:03:01",
        "TIME(0) NOT NULL");
    tester.checkScalar(
        "time '12:03:01' + interval '25:0:1' hour to second",
        "13:03:02",
        "TIME(0) NOT NULL");
    tester.checkScalar(
        "interval '5' day + date '2005-03-02'",
        "2005-03-07",
        "DATE NOT NULL");
    tester.checkScalar(
        "date '2005-03-02' + interval '5' day",
        "2005-03-07",
        "DATE NOT NULL");
    tester.checkScalar(
        "date '2005-03-02' + interval '5' hour",
        "2005-03-02",
        "DATE NOT NULL");
    tester.checkScalar(
        "date '2005-03-02' + interval '25' hour",
        "2005-03-03",
        "DATE NOT NULL");
    tester.checkScalar(
        "date '2005-03-02' + interval '25:45' hour to minute",
        "2005-03-03",
        "DATE NOT NULL");
    tester.checkScalar(
        "date '2005-03-02' + interval '25:45:54' hour to second",
        "2005-03-03",
        "DATE NOT NULL");
    tester.checkScalar(
        "timestamp '2003-08-02 12:54:01' + interval '-4 2:4' day to minute",
        "2003-07-29 10:50:01",
        "TIMESTAMP(0) NOT NULL");

    // Datetime plus year-to-month interval
    tester.checkScalar(
        "interval '5-3' year to month + date '2005-03-02'",
        "2010-06-02",
        "DATE NOT NULL");
    tester.checkScalar(
        "timestamp '2003-08-02 12:54:01' + interval '5-3' year to month",
        "2008-11-02 12:54:01",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "interval '5-3' year to month + timestamp '2003-08-02 12:54:01'",
        "2008-11-02 12:54:01",
        "TIMESTAMP(0) NOT NULL");
  }

  @Test public void testDescendingOperator() {
    tester.setFor(SqlStdOperatorTable.DESC, VM_EXPAND);
  }

  @Test public void testIsNotNullOperator() {
    tester.setFor(SqlStdOperatorTable.IS_NOT_NULL);
    tester.checkBoolean("true is not null", Boolean.TRUE);
    tester.checkBoolean(
        "cast(null as boolean) is not null",
        Boolean.FALSE);
  }

  @Test public void testIsNullOperator() {
    tester.setFor(SqlStdOperatorTable.IS_NULL);
    tester.checkBoolean("true is null", Boolean.FALSE);
    tester.checkBoolean(
        "cast(null as boolean) is null",
        Boolean.TRUE);
  }

  @Test public void testIsNotTrueOperator() {
    tester.setFor(SqlStdOperatorTable.IS_NOT_TRUE);
    tester.checkBoolean("true is not true", Boolean.FALSE);
    tester.checkBoolean("false is not true", Boolean.TRUE);
    tester.checkBoolean(
        "cast(null as boolean) is not true",
        Boolean.TRUE);
    tester.checkFails(
        "select ^'a string' is not true^ from (values (1))",
        "(?s)Cannot apply 'IS NOT TRUE' to arguments of type '<CHAR\\(8\\)> IS NOT TRUE'. Supported form\\(s\\): '<BOOLEAN> IS NOT TRUE'.*",
        false);
  }

  @Test public void testIsTrueOperator() {
    tester.setFor(SqlStdOperatorTable.IS_TRUE);
    tester.checkBoolean("true is true", Boolean.TRUE);
    tester.checkBoolean("false is true", Boolean.FALSE);
    tester.checkBoolean(
        "cast(null as boolean) is true",
        Boolean.FALSE);
  }

  @Test public void testIsNotFalseOperator() {
    tester.setFor(SqlStdOperatorTable.IS_NOT_FALSE);
    tester.checkBoolean("false is not false", Boolean.FALSE);
    tester.checkBoolean("true is not false", Boolean.TRUE);
    tester.checkBoolean(
        "cast(null as boolean) is not false",
        Boolean.TRUE);
  }

  @Test public void testIsFalseOperator() {
    tester.setFor(SqlStdOperatorTable.IS_FALSE);
    tester.checkBoolean("false is false", Boolean.TRUE);
    tester.checkBoolean("true is false", Boolean.FALSE);
    tester.checkBoolean(
        "cast(null as boolean) is false",
        Boolean.FALSE);
  }

  @Test public void testIsNotUnknownOperator() {
    tester.setFor(SqlStdOperatorTable.IS_NOT_UNKNOWN, VM_EXPAND);
    tester.checkBoolean("false is not unknown", Boolean.TRUE);
    tester.checkBoolean("true is not unknown", Boolean.TRUE);
    tester.checkBoolean(
        "cast(null as boolean) is not unknown",
        Boolean.FALSE);
    tester.checkBoolean("unknown is not unknown", Boolean.FALSE);
    tester.checkFails(
        "^'abc' IS NOT UNKNOWN^",
        "(?s).*Cannot apply 'IS NOT UNKNOWN'.*",
        false);
  }

  @Test public void testIsUnknownOperator() {
    tester.setFor(SqlStdOperatorTable.IS_UNKNOWN, VM_EXPAND);
    tester.checkBoolean("false is unknown", Boolean.FALSE);
    tester.checkBoolean("true is unknown", Boolean.FALSE);
    tester.checkBoolean(
        "cast(null as boolean) is unknown",
        Boolean.TRUE);
    tester.checkBoolean("unknown is unknown", Boolean.TRUE);
    tester.checkFails(
        "0 = 1 AND ^2 IS UNKNOWN^ AND 3 > 4",
        "(?s).*Cannot apply 'IS UNKNOWN'.*",
        false);
  }

  @Test public void testIsASetOperator() {
    tester.setFor(SqlStdOperatorTable.IS_A_SET, VM_EXPAND);
    tester.checkBoolean("multiset[1] is a set", Boolean.TRUE);
    tester.checkBoolean("multiset[1, 1] is a set", Boolean.FALSE);
    tester.checkBoolean("multiset[cast(null as boolean), cast(null as boolean)] is a set",
        Boolean.FALSE);
    tester.checkBoolean("multiset[cast(null as boolean)] is a set", Boolean.TRUE);
    tester.checkBoolean("multiset['a'] is a set", Boolean.TRUE);
    tester.checkBoolean("multiset['a', 'b'] is a set", Boolean.TRUE);
    tester.checkBoolean("multiset['a', 'b', 'a'] is a set", Boolean.FALSE);
  }

  @Test public void testIsNotASetOperator() {
    tester.setFor(SqlStdOperatorTable.IS_NOT_A_SET, VM_EXPAND);
    tester.checkBoolean("multiset[1] is not a set", Boolean.FALSE);
    tester.checkBoolean("multiset[1, 1] is not a set", Boolean.TRUE);
    tester.checkBoolean("multiset[cast(null as boolean), cast(null as boolean)] is not a set",
        Boolean.TRUE);
    tester.checkBoolean("multiset[cast(null as boolean)] is not a set", Boolean.FALSE);
    tester.checkBoolean("multiset['a'] is not a set", Boolean.FALSE);
    tester.checkBoolean("multiset['a', 'b'] is not a set", Boolean.FALSE);
    tester.checkBoolean("multiset['a', 'b', 'a'] is not a set", Boolean.TRUE);
  }

  @Test public void testIntersectOperator() {
    tester.setFor(SqlStdOperatorTable.MULTISET_INTERSECT, VM_EXPAND);
    tester.checkScalar("multiset[1] multiset intersect multiset[1]",
        "[1]",
        "INTEGER NOT NULL MULTISET NOT NULL");
    tester.checkScalar("multiset[2] multiset intersect all multiset[1]",
        "[]",
        "INTEGER NOT NULL MULTISET NOT NULL");
    tester.checkScalar("multiset[2] multiset intersect distinct multiset[1]",
        "[]",
        "INTEGER NOT NULL MULTISET NOT NULL");
    tester.checkScalar("multiset[1, 1] multiset intersect distinct multiset[1, 1]",
        "[1]",
        "INTEGER NOT NULL MULTISET NOT NULL");
    tester.checkScalar("multiset[1, 1] multiset intersect all multiset[1, 1]",
        "[1, 1]",
        "INTEGER NOT NULL MULTISET NOT NULL");
    tester.checkScalar("multiset[1, 1] multiset intersect distinct multiset[1, 1]",
        "[1]",
        "INTEGER NOT NULL MULTISET NOT NULL");
    tester.checkScalar(
        "multiset[cast(null as integer), cast(null as integer)] "
            + "multiset intersect distinct multiset[cast(null as integer)]",
        "[null]",
        "INTEGER MULTISET NOT NULL");
    tester.checkScalar(
        "multiset[cast(null as integer), cast(null as integer)] "
            + "multiset intersect all multiset[cast(null as integer)]",
        "[null]",
        "INTEGER MULTISET NOT NULL");
    tester.checkScalar(
        "multiset[cast(null as integer), cast(null as integer)] "
            + "multiset intersect distinct multiset[cast(null as integer)]",
        "[null]",
        "INTEGER MULTISET NOT NULL");
  }

  @Test public void testExceptOperator() {
    tester.setFor(SqlStdOperatorTable.MULTISET_EXCEPT, VM_EXPAND);
    tester.checkScalar("multiset[1] multiset except multiset[1]",
        "[]",
        "INTEGER NOT NULL MULTISET NOT NULL");
    tester.checkScalar("multiset[1] multiset except distinct multiset[1]",
        "[]",
        "INTEGER NOT NULL MULTISET NOT NULL");
    tester.checkScalar("multiset[2] multiset except multiset[1]",
        "[2]",
        "INTEGER NOT NULL MULTISET NOT NULL");
    tester.checkScalar("multiset[1,2,3] multiset except multiset[1]",
        "[2, 3]",
        "INTEGER NOT NULL MULTISET NOT NULL");
    tester.checkScalar("cardinality(multiset[1,2,3,2] multiset except distinct multiset[1])",
        "2",
        "INTEGER NOT NULL");
    tester.checkScalar("cardinality(multiset[1,2,3,2] multiset except all multiset[1])",
        "3",
        "INTEGER NOT NULL");
    tester.checkBoolean(
        "(multiset[1,2,3,2] multiset except distinct multiset[1]) submultiset of multiset[2, 3]",
        Boolean.TRUE);
    tester.checkBoolean(
        "(multiset[1,2,3,2] multiset except distinct multiset[1]) submultiset of multiset[2, 3]",
        Boolean.TRUE);
    tester.checkBoolean(
        "(multiset[1,2,3,2] multiset except all multiset[1]) submultiset of multiset[2, 2, 3]",
        Boolean.TRUE);
    tester.checkBoolean("(multiset[1,2,3] multiset except multiset[1]) is empty", Boolean.FALSE);
    tester.checkBoolean("(multiset[1] multiset except multiset[1]) is empty", Boolean.TRUE);
  }

  @Test public void testIsEmptyOperator() {
    tester.setFor(SqlStdOperatorTable.IS_EMPTY, VM_EXPAND);
    tester.checkBoolean("multiset[1] is empty", Boolean.FALSE);
  }

  @Test public void testIsNotEmptyOperator() {
    tester.setFor(SqlStdOperatorTable.IS_NOT_EMPTY, VM_EXPAND);
    tester.checkBoolean("multiset[1] is not empty", Boolean.TRUE);
  }

  @Test public void testExistsOperator() {
    tester.setFor(SqlStdOperatorTable.EXISTS, VM_EXPAND);
  }

  @Test public void testNotOperator() {
    tester.setFor(SqlStdOperatorTable.NOT);
    tester.checkBoolean("not true", Boolean.FALSE);
    tester.checkBoolean("not false", Boolean.TRUE);
    tester.checkBoolean("not unknown", null);
    tester.checkNull("not cast(null as boolean)");
  }

  @Test public void testPrefixMinusOperator() {
    tester.setFor(SqlStdOperatorTable.UNARY_MINUS);
    strictTester.checkFails(
        "'a' + ^- 'b'^ + 'c'",
        "(?s)Cannot apply '-' to arguments of type '-<CHAR\\(1\\)>'.*",
        false);
    tester.checkType("'a' + - 'b' + 'c'", "DECIMAL(19, 19) NOT NULL");
    tester.checkScalarExact("-1", "-1");
    tester.checkScalarExact(
        "-1.23",
        "DECIMAL(3, 2) NOT NULL",
        "-1.23");
    tester.checkScalarApprox("-1.0e0", "DOUBLE NOT NULL", -1, 0);
    tester.checkNull("-cast(null as integer)");
    tester.checkNull("-cast(null as tinyint)");
  }

  @Test public void testPrefixMinusOperatorIntervals() {
    tester.checkScalar(
        "-interval '-6:2:8' hour to second",
        "+6:02:08.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    tester.checkScalar(
        "- -interval '-6:2:8' hour to second",
        "-6:02:08.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    tester.checkScalar(
        "-interval '5' month",
        "-5",
        "INTERVAL MONTH NOT NULL");
    tester.checkNull(
        "-cast(null as interval day to minute)");
  }

  @Test public void testPrefixPlusOperator() {
    tester.setFor(SqlStdOperatorTable.UNARY_PLUS, VM_EXPAND);
    tester.checkScalarExact("+1", "1");
    tester.checkScalarExact("+1.23", "DECIMAL(3, 2) NOT NULL", "1.23");
    tester.checkScalarApprox("+1.0e0", "DOUBLE NOT NULL", 1, 0);
    tester.checkNull("+cast(null as integer)");
    tester.checkNull("+cast(null as tinyint)");
  }

  @Test public void testPrefixPlusOperatorIntervals() {
    tester.checkScalar(
        "+interval '-6:2:8' hour to second",
        "-6:02:08.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    tester.checkScalar(
        "++interval '-6:2:8' hour to second",
        "-6:02:08.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    if (Bug.FRG254_FIXED) {
      tester.checkScalar(
          "+interval '6:2:8.234' hour to second",
          "+06:02:08.234",
          "INTERVAL HOUR TO SECOND NOT NULL");
    }
    tester.checkScalar(
        "+interval '5' month",
        "+5",
        "INTERVAL MONTH NOT NULL");
    tester.checkNull(
        "+cast(null as interval day to minute)");
  }

  @Test public void testExplicitTableOperator() {
    tester.setFor(
        SqlStdOperatorTable.EXPLICIT_TABLE,
        VM_EXPAND);
  }

  @Test public void testValuesOperator() {
    tester.setFor(SqlStdOperatorTable.VALUES, VM_EXPAND);
    tester.check(
        "select 'abc' from (values(true))",
        new SqlTests.StringTypeChecker("CHAR(3) NOT NULL"),
        "abc",
        0);
  }

  @Test public void testNotLikeOperator() {
    tester.setFor(SqlStdOperatorTable.NOT_LIKE, VM_EXPAND);
    tester.checkBoolean("'abc' not like '_b_'", Boolean.FALSE);
    tester.checkBoolean("'ab\ncd' not like 'ab%'", Boolean.FALSE);
    tester.checkBoolean("'123\n\n45\n' not like '%'", Boolean.FALSE);
    tester.checkBoolean("'ab\ncd\nef' not like '%cd%'", Boolean.FALSE);
    tester.checkBoolean("'ab\ncd\nef' not like '%cde%'", Boolean.TRUE);
  }

  @Test public void testLikeEscape() {
    tester.setFor(SqlStdOperatorTable.LIKE);
    tester.checkBoolean("'a_c' like 'a#_c' escape '#'", Boolean.TRUE);
    tester.checkBoolean("'axc' like 'a#_c' escape '#'", Boolean.FALSE);
    tester.checkBoolean("'a_c' like 'a\\_c' escape '\\'", Boolean.TRUE);
    tester.checkBoolean("'axc' like 'a\\_c' escape '\\'", Boolean.FALSE);
    tester.checkBoolean("'a%c' like 'a\\%c' escape '\\'", Boolean.TRUE);
    tester.checkBoolean("'a%cde' like 'a\\%c_e' escape '\\'", Boolean.TRUE);
    tester.checkBoolean("'abbc' like 'a%c' escape '\\'", Boolean.TRUE);
    tester.checkBoolean("'abbc' like 'a\\%c' escape '\\'", Boolean.FALSE);
  }

  @Ignore("[CALCITE-525] Exception-handling in built-in functions")
  @Test public void testLikeEscape2() {
    tester.checkBoolean("'x' not like 'x' escape 'x'", Boolean.TRUE);
    tester.checkBoolean("'xyz' not like 'xyz' escape 'xyz'", Boolean.TRUE);
  }

  @Test public void testLikeOperator() {
    tester.setFor(SqlStdOperatorTable.LIKE);
    tester.checkBoolean("''  like ''", Boolean.TRUE);
    tester.checkBoolean("'a' like 'a'", Boolean.TRUE);
    tester.checkBoolean("'a' like 'b'", Boolean.FALSE);
    tester.checkBoolean("'a' like 'A'", Boolean.FALSE);
    tester.checkBoolean("'a' like 'a_'", Boolean.FALSE);
    tester.checkBoolean("'a' like '_a'", Boolean.FALSE);
    tester.checkBoolean("'a' like '%a'", Boolean.TRUE);
    tester.checkBoolean("'a' like '%a%'", Boolean.TRUE);
    tester.checkBoolean("'a' like 'a%'", Boolean.TRUE);
    tester.checkBoolean("'ab'   like 'a_'", Boolean.TRUE);
    tester.checkBoolean("'abc'  like 'a_'", Boolean.FALSE);
    tester.checkBoolean("'abcd' like 'a%'", Boolean.TRUE);
    tester.checkBoolean("'ab'   like '_b'", Boolean.TRUE);
    tester.checkBoolean("'abcd' like '_d'", Boolean.FALSE);
    tester.checkBoolean("'abcd' like '%d'", Boolean.TRUE);
    tester.checkBoolean("'ab\ncd' like 'ab%'", Boolean.TRUE);
    tester.checkBoolean("'abc\ncd' like 'ab%'", Boolean.TRUE);
    tester.checkBoolean("'123\n\n45\n' like '%'", Boolean.TRUE);
    tester.checkBoolean("'ab\ncd\nef' like '%cd%'", Boolean.TRUE);
    tester.checkBoolean("'ab\ncd\nef' like '%cde%'", Boolean.FALSE);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1898">[CALCITE-1898]
   * LIKE must match '.' (period) literally</a>. */
  @Test public void testLikeDot() {
    tester.checkBoolean("'abc' like 'a.c'", Boolean.FALSE);
    tester.checkBoolean("'abcde' like '%c.e'", Boolean.FALSE);
    tester.checkBoolean("'abc.e' like '%c.e'", Boolean.TRUE);
  }

  @Test public void testNotSimilarToOperator() {
    tester.setFor(SqlStdOperatorTable.NOT_SIMILAR_TO, VM_EXPAND);
    tester.checkBoolean("'ab' not similar to 'a_'", false);
    tester.checkBoolean("'aabc' not similar to 'ab*c+d'", true);
    tester.checkBoolean("'ab' not similar to 'a' || '_'", false);
    tester.checkBoolean("'ab' not similar to 'ba_'", true);
    tester.checkBoolean(
        "cast(null as varchar(2)) not similar to 'a_'",
        null);
    tester.checkBoolean(
        "cast(null as varchar(3)) not similar to cast(null as char(2))",
        null);
  }

  @Test public void testSimilarToOperator() {
    tester.setFor(SqlStdOperatorTable.SIMILAR_TO);

    // like LIKE
    tester.checkBoolean("''  similar to ''", Boolean.TRUE);
    tester.checkBoolean("'a' similar to 'a'", Boolean.TRUE);
    tester.checkBoolean("'a' similar to 'b'", Boolean.FALSE);
    tester.checkBoolean("'a' similar to 'A'", Boolean.FALSE);
    tester.checkBoolean("'a' similar to 'a_'", Boolean.FALSE);
    tester.checkBoolean("'a' similar to '_a'", Boolean.FALSE);
    tester.checkBoolean("'a' similar to '%a'", Boolean.TRUE);
    tester.checkBoolean("'a' similar to '%a%'", Boolean.TRUE);
    tester.checkBoolean("'a' similar to 'a%'", Boolean.TRUE);
    tester.checkBoolean("'ab'   similar to 'a_'", Boolean.TRUE);
    tester.checkBoolean("'abc'  similar to 'a_'", Boolean.FALSE);
    tester.checkBoolean("'abcd' similar to 'a%'", Boolean.TRUE);
    tester.checkBoolean("'ab'   similar to '_b'", Boolean.TRUE);
    tester.checkBoolean("'abcd' similar to '_d'", Boolean.FALSE);
    tester.checkBoolean("'abcd' similar to '%d'", Boolean.TRUE);
    tester.checkBoolean("'ab\ncd' similar to 'ab%'", Boolean.TRUE);
    tester.checkBoolean("'abc\ncd' similar to 'ab%'", Boolean.TRUE);
    tester.checkBoolean("'123\n\n45\n' similar to '%'", Boolean.TRUE);
    tester.checkBoolean("'ab\ncd\nef' similar to '%cd%'", Boolean.TRUE);
    tester.checkBoolean("'ab\ncd\nef' similar to '%cde%'", Boolean.FALSE);

    // simple regular expressions
    // ab*c+d matches acd, abcd, acccd, abcccd but not abd, aabc
    tester.checkBoolean("'acd'    similar to 'ab*c+d'", Boolean.TRUE);
    tester.checkBoolean("'abcd'   similar to 'ab*c+d'", Boolean.TRUE);
    tester.checkBoolean("'acccd'  similar to 'ab*c+d'", Boolean.TRUE);
    tester.checkBoolean("'abcccd' similar to 'ab*c+d'", Boolean.TRUE);
    tester.checkBoolean("'abd'    similar to 'ab*c+d'", Boolean.FALSE);
    tester.checkBoolean("'aabc'   similar to 'ab*c+d'", Boolean.FALSE);

    // compound regular expressions
    // x(ab|c)*y matches xy, xccy, xababcy but not xbcy
    tester.checkBoolean(
        "'xy'      similar to 'x(ab|c)*y'",
        Boolean.TRUE);
    tester.checkBoolean(
        "'xccy'    similar to 'x(ab|c)*y'",
        Boolean.TRUE);
    tester.checkBoolean(
        "'xababcy' similar to 'x(ab|c)*y'",
        Boolean.TRUE);
    tester.checkBoolean(
        "'xbcy'    similar to 'x(ab|c)*y'",
        Boolean.FALSE);

    // x(ab|c)+y matches xccy, xababcy but not xy, xbcy
    tester.checkBoolean(
        "'xy'      similar to 'x(ab|c)+y'",
        Boolean.FALSE);
    tester.checkBoolean(
        "'xccy'    similar to 'x(ab|c)+y'",
        Boolean.TRUE);
    tester.checkBoolean(
        "'xababcy' similar to 'x(ab|c)+y'",
        Boolean.TRUE);
    tester.checkBoolean(
        "'xbcy'    similar to 'x(ab|c)+y'",
        Boolean.FALSE);

    tester.checkBoolean("'ab' similar to 'a%' ", Boolean.TRUE);
    tester.checkBoolean("'a' similar to 'a%' ", Boolean.TRUE);
    tester.checkBoolean("'abcd' similar to 'a_' ", Boolean.FALSE);
    tester.checkBoolean("'abcd' similar to 'a%' ", Boolean.TRUE);
    tester.checkBoolean("'1a' similar to '_a' ", Boolean.TRUE);
    tester.checkBoolean("'123aXYZ' similar to '%a%'", Boolean.TRUE);

    tester.checkBoolean(
        "'123aXYZ' similar to '_%_a%_' ",
        Boolean.TRUE);

    tester.checkBoolean("'xy' similar to '(xy)' ", Boolean.TRUE);

    tester.checkBoolean(
        "'abd' similar to '[ab][bcde]d' ",
        Boolean.TRUE);

    tester.checkBoolean(
        "'bdd' similar to '[ab][bcde]d' ",
        Boolean.TRUE);

    tester.checkBoolean("'abd' similar to '[ab]d' ", Boolean.FALSE);
    tester.checkBoolean("'cd' similar to '[a-e]d' ", Boolean.TRUE);
    tester.checkBoolean("'amy' similar to 'amy|fred' ", Boolean.TRUE);
    tester.checkBoolean("'fred' similar to 'amy|fred' ", Boolean.TRUE);

    tester.checkBoolean(
        "'mike' similar to 'amy|fred' ",
        Boolean.FALSE);

    tester.checkBoolean("'acd' similar to 'ab*c+d' ", Boolean.TRUE);
    tester.checkBoolean("'accccd' similar to 'ab*c+d' ", Boolean.TRUE);
    tester.checkBoolean("'abd' similar to 'ab*c+d' ", Boolean.FALSE);
    tester.checkBoolean("'aabc' similar to 'ab*c+d' ", Boolean.FALSE);
    tester.checkBoolean("'abb' similar to 'a(b{3})' ", Boolean.FALSE);
    tester.checkBoolean("'abbb' similar to 'a(b{3})' ", Boolean.TRUE);

    tester.checkBoolean(
        "'abbbbb' similar to 'a(b{3})' ",
        Boolean.FALSE);

    tester.checkBoolean(
        "'abbbbb' similar to 'ab{3,6}' ",
        Boolean.TRUE);

    tester.checkBoolean(
        "'abbbbbbbb' similar to 'ab{3,6}' ",
        Boolean.FALSE);
    tester.checkBoolean("'' similar to 'ab?' ", Boolean.FALSE);
    tester.checkBoolean("'a' similar to 'ab?' ", Boolean.TRUE);
    tester.checkBoolean("'a' similar to 'a(b?)' ", Boolean.TRUE);
    tester.checkBoolean("'ab' similar to 'ab?' ", Boolean.TRUE);
    tester.checkBoolean("'ab' similar to 'a(b?)' ", Boolean.TRUE);
    tester.checkBoolean("'abb' similar to 'ab?' ", Boolean.FALSE);

    tester.checkBoolean(
        "'ab' similar to 'a\\_' ESCAPE '\\' ",
        Boolean.FALSE);

    tester.checkBoolean(
        "'ab' similar to 'a\\%' ESCAPE '\\' ",
        Boolean.FALSE);

    tester.checkBoolean(
        "'a_' similar to 'a\\_' ESCAPE '\\' ",
        Boolean.TRUE);

    tester.checkBoolean(
        "'a%' similar to 'a\\%' ESCAPE '\\' ",
        Boolean.TRUE);

    tester.checkBoolean(
        "'a(b{3})' similar to 'a(b{3})' ",
        Boolean.FALSE);

    tester.checkBoolean(
        "'a(b{3})' similar to 'a\\(b\\{3\\}\\)' ESCAPE '\\' ",
        Boolean.TRUE);

    tester.checkBoolean("'yd' similar to '[a-ey]d'", Boolean.TRUE);
    tester.checkBoolean("'yd' similar to '[^a-ey]d'", Boolean.FALSE);
    tester.checkBoolean("'yd' similar to '[^a-ex-z]d'", Boolean.FALSE);
    tester.checkBoolean("'yd' similar to '[a-ex-z]d'", Boolean.TRUE);
    tester.checkBoolean("'yd' similar to '[x-za-e]d'", Boolean.TRUE);
    tester.checkBoolean("'yd' similar to '[^a-ey]?d'", Boolean.FALSE);
    tester.checkBoolean("'yyyd' similar to '[a-ey]*d'", Boolean.TRUE);

    // range must be specified in []
    tester.checkBoolean("'yd' similar to 'x-zd'", Boolean.FALSE);
    tester.checkBoolean("'y' similar to 'x-z'", Boolean.FALSE);

    tester.checkBoolean("'cd' similar to '([a-e])d'", Boolean.TRUE);
    tester.checkBoolean("'xy' similar to 'x*?y'", Boolean.TRUE);
    tester.checkBoolean("'y' similar to 'x*?y'", Boolean.TRUE);
    tester.checkBoolean("'y' similar to '(x?)*y'", Boolean.TRUE);
    tester.checkBoolean("'y' similar to 'x+?y'", Boolean.FALSE);

    tester.checkBoolean("'y' similar to 'x?+y'", Boolean.TRUE);
    tester.checkBoolean("'y' similar to 'x*+y'", Boolean.TRUE);

    // dot is a wildcard for SIMILAR TO but not LIKE
    tester.checkBoolean("'abc' similar to 'a.c'", Boolean.TRUE);
    tester.checkBoolean("'a.c' similar to 'a.c'", Boolean.TRUE);
    tester.checkBoolean("'abcd' similar to 'a.*d'", Boolean.TRUE);
    tester.checkBoolean("'abc' like 'a.c'", Boolean.FALSE);
    tester.checkBoolean("'a.c' like 'a.c'", Boolean.TRUE);
    tester.checkBoolean("'abcd' like 'a.*d'", Boolean.FALSE);

    // The following two tests throws exception(They probably should).
    // "Dangling meta character '*' near index 2"

    if (enable) {
      tester.checkBoolean("'y' similar to 'x+*y'", Boolean.TRUE);
      tester.checkBoolean("'y' similar to 'x?*y'", Boolean.TRUE);
    }

    // some negative tests
    tester.checkFails(
        "'yd' similar to '[x-ze-a]d'",
        "Illegal character range near index 6\n"
            + "\\[x-ze-a\\]d\n"
            + "      \\^",
        true);   // illegal range

    // Slightly different error message from JDK 13 onwards
    final String expectedError =
        TestUtil.getJavaMajorVersion() >= 13
            ? "Illegal repetition near index 22\n"
              + "\\[\\:LOWER\\:\\]\\{2\\}\\[\\:DIGIT\\:\\]\\{,5\\}\n"
              + "                      \\^"
            : "Illegal repetition near index 20\n"
                + "\\[\\:LOWER\\:\\]\\{2\\}\\[\\:DIGIT\\:\\]\\{,5\\}\n"
                + "                    \\^";
    tester.checkFails("'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{,5}'",
        expectedError, true);

    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails(
          "'cd' similar to '[(a-e)]d' ",
          "Invalid regular expression: \\[\\(a-e\\)\\]d at 1",
          true);

      tester.checkFails(
          "'yd' similar to '[(a-e)]d' ",
          "Invalid regular expression: \\[\\(a-e\\)\\]d at 1",
          true);
    }

    // all the following tests wrong results due to missing functionality
    // or defect (FRG-375, 377).

    if (Bug.FRG375_FIXED) {
      tester.checkBoolean(
          "'cd' similar to '[a-e^c]d' ", Boolean.FALSE); // FRG-375
    }

    // following tests use regular character set identifiers.
    // Not implemented yet. FRG-377.
    if (Bug.FRG377_FIXED) {
      tester.checkBoolean(
          "'y' similar to '[:ALPHA:]*'",
          Boolean.TRUE);

      tester.checkBoolean(
          "'yd32' similar to '[:LOWER:]{2}[:DIGIT:]*'",
          Boolean.TRUE);

      tester.checkBoolean(
          "'yd32' similar to '[:ALNUM:]*'",
          Boolean.TRUE);

      tester.checkBoolean(
          "'yd32' similar to '[:ALNUM:]*[:DIGIT:]?'",
          Boolean.TRUE);

      tester.checkBoolean(
          "'yd32' similar to '[:ALNUM:]?[:DIGIT:]*'",
          Boolean.FALSE);

      tester.checkBoolean(
          "'yd3223' similar to '([:LOWER:]{2})[:DIGIT:]{2,5}'",
          Boolean.TRUE);

      tester.checkBoolean(
          "'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{2,}'",
          Boolean.TRUE);

      tester.checkBoolean(
          "'yd3223' similar to '[:LOWER:]{2}||[:DIGIT:]{4}'",
          Boolean.TRUE);

      tester.checkBoolean(
          "'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{3}'",
          Boolean.FALSE);

      tester.checkBoolean(
          "'yd  3223' similar to '[:UPPER:]{2}  [:DIGIT:]{3}'",
          Boolean.FALSE);

      tester.checkBoolean(
          "'YD  3223' similar to '[:UPPER:]{2}  [:DIGIT:]{3}'",
          Boolean.FALSE);

      tester.checkBoolean(
          "'YD  3223' similar to "
              + "'[:UPPER:]{2}||[:WHITESPACE:]*[:DIGIT:]{4}'",
          Boolean.TRUE);

      tester.checkBoolean(
          "'YD\t3223' similar to '[:UPPER:]{2}[:SPACE:]*[:DIGIT:]{4}'",
          Boolean.FALSE);

      tester.checkBoolean(
          "'YD\t3223' similar to "
              + "'[:UPPER:]{2}[:WHITESPACE:]*[:DIGIT:]{4}'",
          Boolean.TRUE);

      tester.checkBoolean(
          "'YD\t\t3223' similar to "
              + "'([:UPPER:]{2}[:WHITESPACE:]+)||[:DIGIT:]{4}'",
          Boolean.TRUE);
    }
  }

  @Test public void testEscapeOperator() {
    tester.setFor(SqlStdOperatorTable.ESCAPE, VM_EXPAND);
  }

  @Test public void testConvertFunc() {
    tester.setFor(
        SqlStdOperatorTable.CONVERT,
        VM_FENNEL,
        VM_JAVA);
  }

  @Test public void testTranslateFunc() {
    tester.setFor(
        SqlStdOperatorTable.TRANSLATE,
        VM_FENNEL,
        VM_JAVA);
  }

  @Test public void testTranslate3Func() {
    final SqlTester tester1 = oracleTester();
    tester1.setFor(SqlLibraryOperators.TRANSLATE3);
    tester1.checkString(
        "translate('aabbcc', 'ab', '+-')",
        "++--cc",
        "VARCHAR(6) NOT NULL");
    tester1.checkString(
        "translate('aabbcc', 'ab', 'ba')",
        "bbaacc",
        "VARCHAR(6) NOT NULL");
    tester1.checkString(
        "translate('aabbcc', 'ab', '')",
        "cc",
        "VARCHAR(6) NOT NULL");
    tester1.checkString(
        "translate('aabbcc', '', '+-')",
        "aabbcc",
        "VARCHAR(6) NOT NULL");
    tester1.checkString(
        "translate(cast('aabbcc' as varchar(10)), 'ab', '+-')",
        "++--cc",
        "VARCHAR(10) NOT NULL");
    tester1.checkNull(
        "translate(cast(null as varchar(7)), 'ab', '+-')");
    tester1.checkNull(
        "translate('aabbcc', cast(null as varchar(2)), '+-')");
    tester1.checkNull(
        "translate('aabbcc', 'ab', cast(null as varchar(2)))");
  }

  @Test public void testOverlayFunc() {
    tester.setFor(SqlStdOperatorTable.OVERLAY);
    tester.checkString(
        "overlay('ABCdef' placing 'abc' from 1)",
        "abcdef",
        "VARCHAR(9) NOT NULL");
    tester.checkString(
        "overlay('ABCdef' placing 'abc' from 1 for 2)",
        "abcCdef",
        "VARCHAR(9) NOT NULL");
    if (enable) {
      tester.checkString(
          "overlay(cast('ABCdef' as varchar(10)) placing "
              + "cast('abc' as char(5)) from 1 for 2)",
          "abc  Cdef",
          "VARCHAR(15) NOT NULL");
    }
    if (enable) {
      tester.checkString(
          "overlay(cast('ABCdef' as char(10)) placing "
              + "cast('abc' as char(5)) from 1 for 2)",
          "abc  Cdef    ",
          "VARCHAR(15) NOT NULL");
    }
    tester.checkNull(
        "overlay('ABCdef' placing 'abc' from 1 for cast(null as integer))");
    tester.checkNull(
        "overlay(cast(null as varchar(1)) placing 'abc' from 1)");

    tester.checkString(
        "overlay(x'ABCdef' placing x'abcd' from 1)",
        "abcdef",
        "VARBINARY(5) NOT NULL");
    tester.checkString(
        "overlay(x'ABCDEF1234' placing x'2345' from 1 for 2)",
        "2345ef1234",
        "VARBINARY(7) NOT NULL");
    if (enable) {
      tester.checkString(
          "overlay(cast(x'ABCdef' as varbinary(5)) placing "
              + "cast(x'abcd' as binary(3)) from 1 for 2)",
          "abc  Cdef",
          "VARBINARY(8) NOT NULL");
    }
    if (enable) {
      tester.checkString(
          "overlay(cast(x'ABCdef' as binary(5)) placing "
              + "cast(x'abcd' as binary(3)) from 1 for 2)",
          "abc  Cdef    ",
          "VARBINARY(8) NOT NULL");
    }
    tester.checkNull(
        "overlay(x'ABCdef' placing x'abcd' from 1 for cast(null as integer))");
    tester.checkNull(
        "overlay(cast(null as varbinary(1)) placing x'abcd' from 1)");

    tester.checkNull(
        "overlay(x'abcd' placing x'abcd' from cast(null as integer))");
  }

  @Test public void testPositionFunc() {
    tester.setFor(SqlStdOperatorTable.POSITION);
    tester.checkScalarExact("position('b' in 'abc')", "2");
    tester.checkScalarExact("position('' in 'abc')", "1");
    tester.checkScalarExact("position('b' in 'abcabc' FROM 3)", "5");
    tester.checkScalarExact("position('b' in 'abcabc' FROM 5)", "5");
    tester.checkScalarExact("position('b' in 'abcabc' FROM 6)", "0");
    tester.checkScalarExact("position('b' in 'abcabc' FROM -5)", "0");
    tester.checkScalarExact("position('' in 'abc' FROM 3)", "3");
    tester.checkScalarExact("position('' in 'abc' FROM 10)", "0");

    tester.checkScalarExact("position(x'bb' in x'aabbcc')", "2");
    tester.checkScalarExact("position(x'' in x'aabbcc')", "1");
    tester.checkScalarExact("position(x'bb' in x'aabbccaabbcc' FROM 3)", "5");
    tester.checkScalarExact("position(x'bb' in x'aabbccaabbcc' FROM 5)", "5");
    tester.checkScalarExact("position(x'bb' in x'aabbccaabbcc' FROM 6)", "0");
    tester.checkScalarExact("position(x'bb' in x'aabbccaabbcc' FROM -5)", "0");
    tester.checkScalarExact("position(x'cc' in x'aabbccdd' FROM 2)", "3");
    tester.checkScalarExact("position(x'' in x'aabbcc' FROM 3)", "3");
    tester.checkScalarExact("position(x'' in x'aabbcc' FROM 10)", "0");

    // FRG-211
    tester.checkScalarExact("position('tra' in 'fdgjklewrtra')", "10");

    tester.checkNull("position(cast(null as varchar(1)) in '0010')");
    tester.checkNull("position('a' in cast(null as varchar(1)))");

    tester.checkScalar(
        "position(cast('a' as char) in cast('bca' as varchar))",
        3,
        "INTEGER NOT NULL");
  }

  @Test public void testReplaceFunc() {
    tester.setFor(SqlStdOperatorTable.REPLACE);
    tester.checkString("REPLACE('ciao', 'ciao', '')", "",
        "VARCHAR(4) NOT NULL");
    tester.checkString("REPLACE('hello world', 'o', '')", "hell wrld",
        "VARCHAR(11) NOT NULL");
    tester.checkNull("REPLACE(cast(null as varchar(5)), 'ciao', '')");
    tester.checkNull("REPLACE('ciao', cast(null as varchar(3)), 'zz')");
    tester.checkNull("REPLACE('ciao', 'bella', cast(null as varchar(3)))");
  }

  @Test public void testCharLengthFunc() {
    tester.setFor(SqlStdOperatorTable.CHAR_LENGTH);
    tester.checkScalarExact("char_length('abc')", "3");
    tester.checkNull("char_length(cast(null as varchar(1)))");
  }

  @Test public void testCharacterLengthFunc() {
    tester.setFor(SqlStdOperatorTable.CHARACTER_LENGTH);
    tester.checkScalarExact("CHARACTER_LENGTH('abc')", "3");
    tester.checkNull("CHARACTER_LENGTH(cast(null as varchar(1)))");
  }

  @Test public void testAsciiFunc() {
    tester.setFor(SqlStdOperatorTable.ASCII);
    tester.checkScalarExact("ASCII('')", "0");
    tester.checkScalarExact("ASCII('a')", "97");
    tester.checkScalarExact("ASCII('1')", "49");
    tester.checkScalarExact("ASCII('abc')", "97");
    tester.checkScalarExact("ASCII('ABC')", "65");
    tester.checkScalarExact("ASCII(_UTF8'\u0082')", "130");
    tester.checkScalarExact("ASCII(_UTF8'\u5B57')", "23383");
    tester.checkScalarExact("ASCII(_UTF8'\u03a9')", "937"); // omega
    tester.checkNull("ASCII(cast(null as varchar(1)))");
  }

  @Test public void testToBase64() {
    final SqlTester tester1 = tester(SqlLibrary.MYSQL);
    tester1.setFor(SqlLibraryOperators.TO_BASE64);
    tester1.checkString("to_base64(x'546869732069732061207465737420537472696e672e')",
            "VGhpcyBpcyBhIHRlc3QgU3RyaW5nLg==",
            "VARCHAR NOT NULL");
    tester1.checkString("to_base64(x'546869732069732061207465737420537472696e672e20636865"
                    + "636b20726573756c7465206f7574206f66203736546869732069732061207465737420537472696e"
                    + "672e546869732069732061207465737420537472696e672e54686973206973206120746573742053"
                    + "7472696e672e546869732069732061207465737420537472696e672e546869732069732061207465"
                    + "737420537472696e672e20546869732069732061207465737420537472696e672e20636865636b20"
                    + "726573756c7465206f7574206f66203736546869732069732061207465737420537472696e672e54"
                    + "6869732069732061207465737420537472696e672e54686973206973206120746573742053747269"
                    + "6e672e546869732069732061207465737420537472696e672e546869732069732061207465737420"
                    + "537472696e672e20546869732069732061207465737420537472696e672e20636865636b20726573"
                    + "756c7465206f7574206f66203736546869732069732061207465737420537472696e672e54686973"
                    + "2069732061207465737420537472696e672e546869732069732061207465737420537472696e672e"
                    + "546869732069732061207465737420537472696e672e546869732069732061207465737420537472"
                    + "696e672e')",
            "VGhpcyBpcyBhIHRlc3QgU3RyaW5nLiBjaGVjayByZXN1bHRlIG91dCBvZiA3NlRoaXMgaXMgYSB0\n"
                    + "ZXN0IFN0cmluZy5UaGlzIGlzIGEgdGVzdCBTdHJpbmcuVGhpcyBpcyBhIHRlc3QgU3RyaW5nLlRo\n"
                    + "aXMgaXMgYSB0ZXN0IFN0cmluZy5UaGlzIGlzIGEgdGVzdCBTdHJpbmcuIFRoaXMgaXMgYSB0ZXN0\n"
                    + "IFN0cmluZy4gY2hlY2sgcmVzdWx0ZSBvdXQgb2YgNzZUaGlzIGlzIGEgdGVzdCBTdHJpbmcuVGhp\n"
                    + "cyBpcyBhIHRlc3QgU3RyaW5nLlRoaXMgaXMgYSB0ZXN0IFN0cmluZy5UaGlzIGlzIGEgdGVzdCBT\n"
                    + "dHJpbmcuVGhpcyBpcyBhIHRlc3QgU3RyaW5nLiBUaGlzIGlzIGEgdGVzdCBTdHJpbmcuIGNoZWNr\n"
                    + "IHJlc3VsdGUgb3V0IG9mIDc2VGhpcyBpcyBhIHRlc3QgU3RyaW5nLlRoaXMgaXMgYSB0ZXN0IFN0\n"
                    + "cmluZy5UaGlzIGlzIGEgdGVzdCBTdHJpbmcuVGhpcyBpcyBhIHRlc3QgU3RyaW5nLlRoaXMgaXMg\n"
                    + "YSB0ZXN0IFN0cmluZy4=",
            "VARCHAR NOT NULL");
    tester1.checkString("to_base64('This is a test String.')",
            "VGhpcyBpcyBhIHRlc3QgU3RyaW5nLg==",
            "VARCHAR NOT NULL");
    tester1.checkString("to_base64('This is a test String. check resulte out of 76T"
                    + "his is a test String.This is a test String.This is a test String.This is a "
                    + "test String.This is a test String. This is a test String. check resulte out "
                    + "of 76This is a test String.This is a test String.This is a test String.This "
                    + "is a test String.This is a test String. This is a test String. check resulte "
                    + "out of 76This is a test String.This is a test String.This is a test String."
                    + "This is a test String.This is a test String.')",
            "VGhpcyBpcyBhIHRlc3QgU3RyaW5nLiBjaGVjayByZXN1bHRlIG91dCBvZiA3NlRoaXMgaXMgYSB0\n"
                    + "ZXN0IFN0cmluZy5UaGlzIGlzIGEgdGVzdCBTdHJpbmcuVGhpcyBpcyBhIHRlc3QgU3RyaW5nLlRo\n"
                    + "aXMgaXMgYSB0ZXN0IFN0cmluZy5UaGlzIGlzIGEgdGVzdCBTdHJpbmcuIFRoaXMgaXMgYSB0ZXN0\n"
                    + "IFN0cmluZy4gY2hlY2sgcmVzdWx0ZSBvdXQgb2YgNzZUaGlzIGlzIGEgdGVzdCBTdHJpbmcuVGhp\n"
                    + "cyBpcyBhIHRlc3QgU3RyaW5nLlRoaXMgaXMgYSB0ZXN0IFN0cmluZy5UaGlzIGlzIGEgdGVzdCBT\n"
                    + "dHJpbmcuVGhpcyBpcyBhIHRlc3QgU3RyaW5nLiBUaGlzIGlzIGEgdGVzdCBTdHJpbmcuIGNoZWNr\n"
                    + "IHJlc3VsdGUgb3V0IG9mIDc2VGhpcyBpcyBhIHRlc3QgU3RyaW5nLlRoaXMgaXMgYSB0ZXN0IFN0\n"
                    + "cmluZy5UaGlzIGlzIGEgdGVzdCBTdHJpbmcuVGhpcyBpcyBhIHRlc3QgU3RyaW5nLlRoaXMgaXMg\n"
                    + "YSB0ZXN0IFN0cmluZy4=",
            "VARCHAR NOT NULL");
    tester1.checkString("to_base64('')", "", "VARCHAR NOT NULL");
    tester1.checkString("to_base64('a')", "YQ==", "VARCHAR NOT NULL");
    tester1.checkString("to_base64(x'61')", "YQ==", "VARCHAR NOT NULL");
  }

  @Test public void testFromBase64() {
    final SqlTester tester1 = tester(SqlLibrary.MYSQL);
    tester1.setFor(SqlLibraryOperators.FROM_BASE64);
    tester1.checkString("from_base64('VGhpcyBpcyBhIHRlc3QgU3RyaW5nLg==')",
            "546869732069732061207465737420537472696e672e",
            "VARBINARY NOT NULL");
    tester1.checkString("from_base64('VGhpcyBpcyBhIHRlc\t3QgU3RyaW5nLg==')",
            "546869732069732061207465737420537472696e672e",
            "VARBINARY NOT NULL");
    tester1.checkString("from_base64('VGhpcyBpcyBhIHRlc\t3QgU3\nRyaW5nLg==')",
            "546869732069732061207465737420537472696e672e",
            "VARBINARY NOT NULL");
    tester1.checkString("from_base64('VGhpcyB  pcyBhIHRlc3Qg\tU3Ry\naW5nLg==')",
            "546869732069732061207465737420537472696e672e",
            "VARBINARY NOT NULL");
    tester1.checkNull("from_base64('-1')");
    tester1.checkNull("from_base64('-100')");
  }

  @Test public void testMd5() {
    final SqlTester tester1 = tester(SqlLibrary.MYSQL);
    tester1.setFor(SqlLibraryOperators.MD5);
    tester1.checkString("md5(x'')",
        "d41d8cd98f00b204e9800998ecf8427e",
        "VARCHAR NOT NULL");
    tester1.checkString("md5('')",
        "d41d8cd98f00b204e9800998ecf8427e",
        "VARCHAR NOT NULL");
    tester1.checkString("md5('ABC')",
        "902fbdd2b1df0c4f70b4a5d23525e932",
        "VARCHAR NOT NULL");
    tester1.checkString("md5(x'414243')",
        "902fbdd2b1df0c4f70b4a5d23525e932",
        "VARCHAR NOT NULL");
  }

  @Test public void testSha1() {
    final SqlTester tester1 = tester(SqlLibrary.MYSQL);
    tester1.setFor(SqlLibraryOperators.SHA1);
    tester1.checkString("sha1(x'')",
        "da39a3ee5e6b4b0d3255bfef95601890afd80709",
        "VARCHAR NOT NULL");
    tester1.checkString("sha1('')",
        "da39a3ee5e6b4b0d3255bfef95601890afd80709",
        "VARCHAR NOT NULL");
    tester1.checkString("sha1('ABC')",
        "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8",
        "VARCHAR NOT NULL");
    tester1.checkString("sha1(x'414243')",
        "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8",
        "VARCHAR NOT NULL");
  }

  @Test public void testRepeatFunc() {
    final SqlTester tester1 = tester(SqlLibrary.MYSQL);
    tester1.setFor(SqlLibraryOperators.REPEAT);
    tester1.checkString("REPEAT('a', -100)", "", "VARCHAR(1) NOT NULL");
    tester1.checkString("REPEAT('a', -1)", "", "VARCHAR(1) NOT NULL");
    tester1.checkString("REPEAT('a', 0)", "", "VARCHAR(1) NOT NULL");
    tester1.checkString("REPEAT('a', 2)", "aa", "VARCHAR(1) NOT NULL");
    tester1.checkString("REPEAT('abc', 3)", "abcabcabc", "VARCHAR(3) NOT NULL");
    tester1.checkNull("REPEAT(cast(null as varchar(1)), -1)");
    tester1.checkNull("REPEAT(cast(null as varchar(1)), 2)");
    tester1.checkNull("REPEAT('abc', cast(null as integer))");
    tester1.checkNull("REPEAT(cast(null as varchar(1)), cast(null as integer))");

  }

  @Test public void testSpaceFunc() {
    final SqlTester tester1 = tester(SqlLibrary.MYSQL);
    tester1.setFor(SqlLibraryOperators.SPACE);
    tester1.checkString("SPACE(-100)", "", "VARCHAR(2000) NOT NULL");
    tester1.checkString("SPACE(-1)", "", "VARCHAR(2000) NOT NULL");
    tester1.checkString("SPACE(0)", "", "VARCHAR(2000) NOT NULL");
    tester1.checkString("SPACE(2)", "  ", "VARCHAR(2000) NOT NULL");
    tester1.checkString("SPACE(5)", "     ", "VARCHAR(2000) NOT NULL");
    tester1.checkNull("SPACE(cast(null as integer))");
  }

  @Test public void testSoundexFunc() {
    final SqlTester tester1 = oracleTester();
    tester1.setFor(SqlLibraryOperators.SOUNDEX);
    tester1.checkString("SOUNDEX('TECH ON THE NET')", "T253", "VARCHAR(4) NOT NULL");
    tester1.checkString("SOUNDEX('Miller')", "M460", "VARCHAR(4) NOT NULL");
    tester1.checkString("SOUNDEX('miler')", "M460", "VARCHAR(4) NOT NULL");
    tester1.checkString("SOUNDEX('myller')", "M460", "VARCHAR(4) NOT NULL");
    tester1.checkString("SOUNDEX('muller')", "M460", "VARCHAR(4) NOT NULL");
    tester1.checkString("SOUNDEX('m')", "M000", "VARCHAR(4) NOT NULL");
    tester1.checkString("SOUNDEX('mu')", "M000", "VARCHAR(4) NOT NULL");
    tester1.checkString("SOUNDEX('mile')", "M400", "VARCHAR(4) NOT NULL");
    tester1.checkNull("SOUNDEX(cast(null as varchar(1)))");
    tester1.checkFails("SOUNDEX(_UTF8'\u5B57\u5B57')", "The character is not mapped.*", true);
  }

  @Test public void testDifferenceFunc() {
    final SqlTester tester1 = tester(SqlLibrary.POSTGRESQL);
    tester1.setFor(SqlLibraryOperators.DIFFERENCE);
    tester1.checkScalarExact("DIFFERENCE('Miller', 'miller')", "4");
    tester1.checkScalarExact("DIFFERENCE('Miller', 'myller')", "4");
    tester1.checkScalarExact("DIFFERENCE('muller', 'miller')", "4");
    tester1.checkScalarExact("DIFFERENCE('muller', 'miller')", "4");
    tester1.checkScalarExact("DIFFERENCE('muller', 'milk')", "2");
    tester1.checkScalarExact("DIFFERENCE('muller', 'mile')", "2");
    tester1.checkScalarExact("DIFFERENCE('muller', 'm')", "1");
    tester1.checkScalarExact("DIFFERENCE('muller', 'lee')", "0");
    tester1.checkNull("DIFFERENCE('muller', cast(null as varchar(1)))");
    tester1.checkNull("DIFFERENCE(cast(null as varchar(1)), 'muller')");
  }

  @Test public void testReverseFunc() {
    final SqlTester testerMysql = tester(SqlLibrary.MYSQL);
    testerMysql.setFor(SqlLibraryOperators.REVERSE);
    testerMysql.checkString("reverse('')", "", "VARCHAR(0) NOT NULL");
    testerMysql.checkString("reverse('123')", "321", "VARCHAR(3) NOT NULL");
    testerMysql.checkString("reverse('abc')", "cba", "VARCHAR(3) NOT NULL");
    testerMysql.checkString("reverse('ABC')", "CBA", "VARCHAR(3) NOT NULL");
    testerMysql.checkString("reverse('Hello World')", "dlroW olleH",
        "VARCHAR(11) NOT NULL");
    testerMysql.checkString("reverse(_UTF8'\u4F60\u597D')", "\u597D\u4F60",
        "VARCHAR(2) NOT NULL");
    testerMysql.checkNull("reverse(cast(null as varchar(1)))");
  }

  @Test public void testUpperFunc() {
    tester.setFor(SqlStdOperatorTable.UPPER);
    tester.checkString("upper('a')", "A", "CHAR(1) NOT NULL");
    tester.checkString("upper('A')", "A", "CHAR(1) NOT NULL");
    tester.checkString("upper('1')", "1", "CHAR(1) NOT NULL");
    tester.checkString("upper('aa')", "AA", "CHAR(2) NOT NULL");
    tester.checkNull("upper(cast(null as varchar(1)))");
  }

  @Test public void testLeftFunc() {
    Stream.of(SqlLibrary.MYSQL, SqlLibrary.POSTGRESQL)
        .map(this::tester)
        .forEach(t -> {
          t.setFor(SqlLibraryOperators.LEFT);
          t.checkString("left('abcd', 3)", "abc", "VARCHAR(4) NOT NULL");
          t.checkString("left('abcd', 0)", "", "VARCHAR(4) NOT NULL");
          t.checkString("left('abcd', 5)", "abcd", "VARCHAR(4) NOT NULL");
          t.checkString("left('abcd', -2)", "", "VARCHAR(4) NOT NULL");
          t.checkNull("left(cast(null as varchar(1)), -2)");
          t.checkNull("left('abcd', cast(null as Integer))");

          // test for ByteString
          t.checkString("left(x'ABCdef', 1)", "ab", "VARBINARY(3) NOT NULL");
          t.checkString("left(x'ABCdef', 0)", "", "VARBINARY(3) NOT NULL");
          t.checkString("left(x'ABCdef', 4)", "abcdef",
              "VARBINARY(3) NOT NULL");
          t.checkString("left(x'ABCdef', -2)", "", "VARBINARY(3) NOT NULL");
          t.checkNull("left(cast(null as binary(1)), -2)");
          t.checkNull("left(x'ABCdef', cast(null as Integer))");
        });
  }

  @Test public void testRightFunc() {
    Stream.of(SqlLibrary.MYSQL, SqlLibrary.POSTGRESQL)
        .map(this::tester)
        .forEach(t -> {
          t.setFor(SqlLibraryOperators.RIGHT);
          t.checkString("right('abcd', 3)", "bcd", "VARCHAR(4) NOT NULL");
          t.checkString("right('abcd', 0)", "", "VARCHAR(4) NOT NULL");
          t.checkString("right('abcd', 5)", "abcd", "VARCHAR(4) NOT NULL");
          t.checkString("right('abcd', -2)", "", "VARCHAR(4) NOT NULL");
          t.checkNull("right(cast(null as varchar(1)), -2)");
          t.checkNull("right('abcd', cast(null as Integer))");

          // test for ByteString
          t.checkString("right(x'ABCdef', 1)", "ef", "VARBINARY(3) NOT NULL");
          t.checkString("right(x'ABCdef', 0)", "", "VARBINARY(3) NOT NULL");
          t.checkString("right(x'ABCdef', 4)", "abcdef",
              "VARBINARY(3) NOT NULL");
          t.checkString("right(x'ABCdef', -2)", "", "VARBINARY(3) NOT NULL");
          t.checkNull("right(cast(null as binary(1)), -2)");
          t.checkNull("right(x'ABCdef', cast(null as Integer))");
        });
  }

  @Test public void testRegexpReplaceFunc() {
    Stream.of(SqlLibrary.MYSQL, SqlLibrary.ORACLE)
        .map(this::tester)
        .forEach(t -> {
          t.setFor(SqlLibraryOperators.REGEXP_REPLACE);
          t.checkString("regexp_replace('a b c', 'b', 'X')", "a X c",
              "VARCHAR NOT NULL");
          t.checkString("regexp_replace('abc def ghi', '[a-z]+', 'X')", "X X X",
              "VARCHAR NOT NULL");
          t.checkString("regexp_replace('100-200', '(\\d+)', 'num')", "num-num",
              "VARCHAR NOT NULL");
          t.checkString("regexp_replace('100-200', '(-)', '###')", "100###200",
              "VARCHAR NOT NULL");
          t.checkNull("regexp_replace(cast(null as varchar), '(-)', '###')");
          t.checkNull("regexp_replace('100-200', cast(null as varchar), '###')");
          t.checkNull("regexp_replace('100-200', '(-)', cast(null as varchar))");
          t.checkString("regexp_replace('abc def ghi', '[a-z]+', 'X', 2)", "aX X X",
              "VARCHAR NOT NULL");
          t.checkString("regexp_replace('abc def ghi', '[a-z]+', 'X', 1, 3)", "abc def X",
              "VARCHAR NOT NULL");
          t.checkString("regexp_replace('abc def GHI', '[a-z]+', 'X', 1, 3, 'c')", "abc def GHI",
              "VARCHAR NOT NULL");
          t.checkString("regexp_replace('abc def GHI', '[a-z]+', 'X', 1, 3, 'i')", "abc def X",
              "VARCHAR NOT NULL");
          t.checkString("regexp_replace('abc def GHI', '[a-z]+', 'X', 1, 3, 'i')", "abc def X",
              "VARCHAR NOT NULL");
          t.checkString("regexp_replace('abc\t\ndef\t\nghi', '\t', '+')", "abc+\ndef+\nghi",
              "VARCHAR NOT NULL");
          t.checkString("regexp_replace('abc\t\ndef\t\nghi', '\t\n', '+')", "abc+def+ghi",
              "VARCHAR NOT NULL");
          t.checkString("regexp_replace('abc\t\ndef\t\nghi', '\\w+', '+')", "+\t\n+\t\n+",
              "VARCHAR NOT NULL");
          t.checkQuery("select regexp_replace('a b c', 'b', 'X')");
          t.checkQuery("select regexp_replace('a b c', 'b', 'X', 1)");
          t.checkQuery("select regexp_replace('a b c', 'b', 'X', 1, 3)");
          t.checkQuery("select regexp_replace('a b c', 'b', 'X', 1, 3, 'i')");
        });
  }

  @Test public void testJsonExists() {
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo' false on error)", Boolean.TRUE);
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo' true on error)", Boolean.TRUE);
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo' unknown on error)", Boolean.TRUE);
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo' false on error)", Boolean.TRUE);
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo' true on error)", Boolean.TRUE);
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo' unknown on error)", Boolean.TRUE);
    tester.checkBoolean("json_exists('{}', "
        + "'invalid $.foo' false on error)", Boolean.FALSE);
    tester.checkBoolean("json_exists('{}', "
        + "'invalid $.foo' true on error)", Boolean.TRUE);
    tester.checkBoolean("json_exists('{}', "
        + "'invalid $.foo' unknown on error)", null);

    // not exists
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo1' false on error)", Boolean.FALSE);
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo1' true on error)", Boolean.TRUE);
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo1' unknown on error)", null);
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo1' true on error)", Boolean.FALSE);
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo1' false on error)", Boolean.FALSE);
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo1' error on error)", Boolean.FALSE);
    tester.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo1' unknown on error)", Boolean.FALSE);

    // nulls
    strictTester.checkFails("json_exists(^null^, "
        + "'lax $' unknown on error)", "(?s).*Illegal use of 'NULL'.*", false);
    tester.checkString("json_exists(null, 'lax $' unknown on error)",
        null, "BOOLEAN");
    tester.checkNull("json_exists(cast(null as varchar), "
        + "'lax $.foo1' unknown on error)");

  }

  @Test public void testJsonValue() {
    // type casting test
    tester.checkString("json_value('{\"foo\":100}', 'strict $.foo')",
        "100", "VARCHAR(2000)");
    tester.checkScalar("json_value('{\"foo\":100}', 'strict $.foo' returning integer)",
        100, "INTEGER");
    tester.checkFails("json_value('{\"foo\":\"100\"}', 'strict $.foo' returning boolean)",
        INVALID_CHAR_MESSAGE, true);
    tester.checkScalar("json_value('{\"foo\":100}', 'lax $.foo1' returning integer "
        + "null on empty)", null, "INTEGER");
    tester.checkScalar("json_value('{\"foo\":\"100\"}', 'strict $.foo1' returning boolean "
        + "null on error)", null, "BOOLEAN");

    // lax test
    tester.checkString("json_value('{\"foo\":100}', 'lax $.foo' null on empty)",
        "100", "VARCHAR(2000)");
    tester.checkString("json_value('{\"foo\":100}', 'lax $.foo' error on empty)",
        "100", "VARCHAR(2000)");
    tester.checkString("json_value('{\"foo\":100}', 'lax $.foo' default 'empty' on empty)",
        "100", "VARCHAR(2000)");
    tester.checkString("json_value('{\"foo\":100}', 'lax $.foo1' null on empty)",
        null, "VARCHAR(2000)");
    tester.checkFails("json_value('{\"foo\":100}', 'lax $.foo1' error on empty)",
        "(?s).*Empty result of JSON_VALUE function is not allowed.*", true);
    tester.checkString("json_value('{\"foo\":100}', 'lax $.foo1' default 'empty' on empty)",
        "empty", "VARCHAR(2000)");
    tester.checkString("json_value('{\"foo\":{}}', 'lax $.foo' null on empty)",
        null, "VARCHAR(2000)");
    tester.checkFails("json_value('{\"foo\":{}}', 'lax $.foo' error on empty)",
        "(?s).*Empty result of JSON_VALUE function is not allowed.*", true);
    tester.checkString("json_value('{\"foo\":{}}', 'lax $.foo' default 'empty' on empty)",
        "empty", "VARCHAR(2000)");
    tester.checkString("json_value('{\"foo\":100}', 'lax $.foo' null on error)",
        "100", "VARCHAR(2000)");
    tester.checkString("json_value('{\"foo\":100}', 'lax $.foo' error on error)",
        "100", "VARCHAR(2000)");
    tester.checkString("json_value('{\"foo\":100}', 'lax $.foo' default 'empty' on error)",
        "100", "VARCHAR(2000)");

    // path error test
    tester.checkString("json_value('{\"foo\":100}', 'invalid $.foo' null on error)",
        null, "VARCHAR(2000)");
    tester.checkFails("json_value('{\"foo\":100}', 'invalid $.foo' error on error)",
        "(?s).*Illegal jsonpath spec.*", true);
    tester.checkString("json_value('{\"foo\":100}', "
            + "'invalid $.foo' default 'empty' on error)",
        "empty", "VARCHAR(2000)");

    // strict test
    tester.checkString("json_value('{\"foo\":100}', 'strict $.foo' null on empty)",
        "100", "VARCHAR(2000)");
    tester.checkString("json_value('{\"foo\":100}', 'strict $.foo' error on empty)",
        "100", "VARCHAR(2000)");
    tester.checkString("json_value('{\"foo\":100}', "
            + "'strict $.foo' default 'empty' on empty)",
        "100", "VARCHAR(2000)");
    tester.checkString("json_value('{\"foo\":100}', 'strict $.foo1' null on error)",
        null, "VARCHAR(2000)");
    tester.checkFails("json_value('{\"foo\":100}', 'strict $.foo1' error on error)",
        "(?s).*No results for path: \\$\\['foo1'\\].*", true);
    tester.checkString("json_value('{\"foo\":100}', "
            + "'strict $.foo1' default 'empty' on error)",
        "empty", "VARCHAR(2000)");
    tester.checkString("json_value('{\"foo\":{}}', 'strict $.foo' null on error)",
        null, "VARCHAR(2000)");
    tester.checkFails("json_value('{\"foo\":{}}', 'strict $.foo' error on error)",
        "(?s).*Strict jsonpath mode requires scalar value, "
            + "and the actual value is: '\\{\\}'.*", true);
    tester.checkString("json_value('{\"foo\":{}}', "
            + "'strict $.foo' default 'empty' on error)",
        "empty", "VARCHAR(2000)");

    // nulls
    strictTester.checkFails("json_value(^null^, 'strict $')",
        "(?s).*Illegal use of 'NULL'.*", false);
    tester.checkString("json_value(null, 'strict $')", null, "VARCHAR(2000)");
    tester.checkNull("json_value(cast(null as varchar), 'strict $')");
  }

  @Test public void testJsonQuery() {
    // lax test
    tester.checkString("json_query('{\"foo\":100}', 'lax $' null on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'lax $' error on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'lax $' empty array on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'lax $' empty object on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'lax $.foo' null on empty)",
        null, "VARCHAR(2000)");
    tester.checkFails("json_query('{\"foo\":100}', 'lax $.foo' error on empty)",
        "(?s).*Empty result of JSON_QUERY function is not allowed.*", true);
    tester.checkString("json_query('{\"foo\":100}', 'lax $.foo' empty array on empty)",
        "[]", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'lax $.foo' empty object on empty)",
        "{}", "VARCHAR(2000)");

    // path error test
    tester.checkString("json_query('{\"foo\":100}', 'invalid $.foo' null on error)",
        null, "VARCHAR(2000)");
    tester.checkFails("json_query('{\"foo\":100}', 'invalid $.foo' error on error)",
        "(?s).*Illegal jsonpath spec.*", true);
    tester.checkString("json_query('{\"foo\":100}', "
            + "'invalid $.foo' empty array on error)",
        "[]", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', "
            + "'invalid $.foo' empty object on error)",
        "{}", "VARCHAR(2000)");

    // strict test
    tester.checkString("json_query('{\"foo\":100}', 'strict $' null on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'strict $' error on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'strict $' empty array on error)",
        "{\"foo\":100}", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'strict $' empty object on error)",
        "{\"foo\":100}", "VARCHAR(2000)");

    tester.checkString("json_query('{\"foo\":100}', 'strict $.foo1' null on error)",
        null, "VARCHAR(2000)");
    tester.checkFails("json_query('{\"foo\":100}', 'strict $.foo1' error on error)",
        "(?s).*No results for path: \\$\\['foo1'\\].*", true);
    tester.checkString("json_query('{\"foo\":100}', 'strict $.foo1' empty array on error)",
        "[]", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'strict $.foo1' empty object on error)",
        "{}", "VARCHAR(2000)");

    tester.checkString("json_query('{\"foo\":100}', 'strict $.foo' null on error)",
        null, "VARCHAR(2000)");
    tester.checkFails("json_query('{\"foo\":100}', 'strict $.foo' error on error)",
        "(?s).*Strict jsonpath mode requires array or object value, "
            + "and the actual value is: '100'.*", true);
    tester.checkString("json_query('{\"foo\":100}', 'strict $.foo' empty array on error)",
        "[]", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'strict $.foo' empty object on error)",
        "{}", "VARCHAR(2000)");

    // array wrapper test
    tester.checkString("json_query('{\"foo\":100}', 'strict $.foo' without wrapper)",
        null, "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'strict $.foo' without array wrapper)",
        null, "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'strict $.foo' with wrapper)",
        "[100]", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'strict $.foo' "
            + "with unconditional wrapper)",
        "[100]", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":100}', 'strict $.foo' "
            + "with conditional wrapper)",
        "[100]", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":[100]}', 'strict $.foo' without wrapper)",
        "[100]", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":[100]}', 'strict $.foo' without array wrapper)",
        "[100]", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":[100]}', 'strict $.foo' with wrapper)",
        "[[100]]", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":[100]}', 'strict $.foo' "
            + "with unconditional wrapper)",
        "[[100]]", "VARCHAR(2000)");
    tester.checkString("json_query('{\"foo\":[100]}', 'strict $.foo' "
            + "with conditional wrapper)",
        "[100]", "VARCHAR(2000)");


    // nulls
    strictTester.checkFails("json_query(^null^, 'lax $')",
        "(?s).*Illegal use of 'NULL'.*", false);
    tester.checkString("json_query(null, 'lax $')", null, "VARCHAR(2000)");
    tester.checkNull("json_query(cast(null as varchar), 'lax $')");
  }

  @Test public void testJsonPretty() {
    tester.checkString("json_pretty('{\"foo\":100}')",
        "{\n  \"foo\" : 100\n}", "VARCHAR(2000)");
    tester.checkString("json_pretty('[1,2,3]')",
        "[ 1, 2, 3 ]", "VARCHAR(2000)");
    tester.checkString("json_pretty('null')",
        "null", "VARCHAR(2000)");

    // nulls
    strictTester.checkFails("json_pretty(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    tester.checkString("json_pretty(null)", null, "VARCHAR(2000)");
    tester.checkNull("json_pretty(cast(null as varchar))");
  }

  @Test public void testJsonStorageSize() {
    tester.checkString("json_storage_size('[100, \"sakila\", [1, 3, 5], 425.05]')",
        "29", "INTEGER");
    tester.checkString("json_storage_size('{\"a\": 1000,\"b\": \"aa\", \"c\": \"[1, 3, 5]\"}')",
        "35", "INTEGER");
    tester.checkString("json_storage_size('{\"a\": 1000, \"b\": \"wxyz\", \"c\": \"[1, 3]\"}')",
        "34", "INTEGER");
    tester.checkString("json_storage_size('[100, \"json\", [[10, 20, 30], 3, 5], 425.05]')",
        "36", "INTEGER");
    tester.checkString("json_storage_size('12')",
        "2", "INTEGER");
    tester.checkString("json_storage_size('12' format json)",
        "2", "INTEGER");
    tester.checkString("json_storage_size('null')",
        "4", "INTEGER");

    // nulls
    strictTester.checkFails("json_storage_size(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    tester.checkString("json_storage_size(null)", null, "INTEGER");
    tester.checkNull("json_storage_size(cast(null as varchar))");
  }

  @Test public void testJsonType() {
    tester.setFor(SqlLibraryOperators.JSON_TYPE);
    tester.checkString("json_type('\"1\"')",
        "STRING", "VARCHAR(20)");
    tester.checkString("json_type('1')",
        "INTEGER", "VARCHAR(20)");
    tester.checkString("json_type('11.45')",
        "DOUBLE", "VARCHAR(20)");
    tester.checkString("json_type('true')",
        "BOOLEAN", "VARCHAR(20)");
    tester.checkString("json_type('null')",
        "NULL", "VARCHAR(20)");
    tester.checkNull("json_type(cast(null as varchar(1)))");
    tester.checkString("json_type('{\"a\": [10, true]}')",
        "OBJECT", "VARCHAR(20)");
    tester.checkString("json_type('{}')",
        "OBJECT", "VARCHAR(20)");
    tester.checkString("json_type('[10, true]')",
        "ARRAY", "VARCHAR(20)");
    tester.checkString("json_type('\"2019-01-27 21:24:00\"')",
        "STRING", "VARCHAR(20)");

    // nulls
    strictTester.checkFails("json_type(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    tester.checkString("json_type(null)", null, "VARCHAR(20)");
    tester.checkNull("json_type(cast(null as varchar))");
  }

  @Test public void testJsonDepth() {
    tester.setFor(SqlLibraryOperators.JSON_DEPTH);
    tester.checkString("json_depth('1')",
        "1", "INTEGER");
    tester.checkString("json_depth('11.45')",
        "1", "INTEGER");
    tester.checkString("json_depth('true')",
        "1", "INTEGER");
    tester.checkString("json_depth('\"2019-01-27 21:24:00\"')",
        "1", "INTEGER");
    tester.checkString("json_depth('{}')",
        "1", "INTEGER");
    tester.checkString("json_depth('[]')",
        "1", "INTEGER");
    tester.checkString("json_depth('null')",
        null, "INTEGER");
    tester.checkString("json_depth(cast(null as varchar(1)))",
        null, "INTEGER");
    tester.checkString("json_depth('[10, true]')",
        "2", "INTEGER");
    tester.checkString("json_depth('[[], {}]')",
        "2", "INTEGER");
    tester.checkString("json_depth('{\"a\": [10, true]}')",
        "3", "INTEGER");
    tester.checkString("json_depth('[10, {\"a\": [[1,2]]}]')",
        "5", "INTEGER");

    // nulls
    strictTester.checkFails("json_depth(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    tester.checkString("json_depth(null)", null, "INTEGER");
    tester.checkNull("json_depth(cast(null as varchar))");
  }

  @Test public void testJsonLength() {
    // no path context
    tester.checkString("json_length('{}')",
        "0", "INTEGER");
    tester.checkString("json_length('[]')",
        "0", "INTEGER");
    tester.checkString("json_length('{\"foo\":100}')",
        "1", "INTEGER");
    tester.checkString("json_length('{\"a\": 1, \"b\": {\"c\": 30}}')",
        "2", "INTEGER");
    tester.checkString("json_length('[1, 2, {\"a\": 3}]')",
        "3", "INTEGER");

    // lax test
    tester.checkString("json_length('{}', 'lax $')",
        "0", "INTEGER");
    tester.checkString("json_length('[]', 'lax $')",
        "0", "INTEGER");
    tester.checkString("json_length('{\"foo\":100}', 'lax $')",
        "1", "INTEGER");
    tester.checkString("json_length('{\"a\": 1, \"b\": {\"c\": 30}}', 'lax $')",
        "2", "INTEGER");
    tester.checkString("json_length('[1, 2, {\"a\": 3}]', 'lax $')",
        "3", "INTEGER");
    tester.checkString("json_length('{\"a\": 1, \"b\": {\"c\": 30}}', 'lax $.b')",
        "1", "INTEGER");
    tester.checkString("json_length('{\"foo\":100}', 'lax $.foo1')",
        null, "INTEGER");

    // strict test
    tester.checkString("json_length('{}', 'strict $')",
        "0", "INTEGER");
    tester.checkString("json_length('[]', 'strict $')",
        "0", "INTEGER");
    tester.checkString("json_length('{\"foo\":100}', 'strict $')",
        "1", "INTEGER");
    tester.checkString("json_length('{\"a\": 1, \"b\": {\"c\": 30}}', 'strict $')",
        "2", "INTEGER");
    tester.checkString("json_length('[1, 2, {\"a\": 3}]', 'strict $')",
        "3", "INTEGER");
    tester.checkString("json_length('{\"a\": 1, \"b\": {\"c\": 30}}', 'strict $.b')",
        "1", "INTEGER");

    // catch error test
    tester.checkFails("json_length('{\"foo\":100}', 'invalid $.foo')",
        "(?s).*Illegal jsonpath spec.*", true);
    tester.checkFails("json_length('{\"foo\":100}', 'strict $.foo1')",
        "(?s).*No results for path.*", true);

    // nulls
    strictTester.checkFails("json_length(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    tester.checkString("json_length(null)", null, "INTEGER");
    tester.checkNull("json_length(cast(null as varchar))");
  }

  @Test public void testJsonKeys() {
    // no path context
    tester.checkString("json_keys('{}')",
        "[]", "VARCHAR(2000)");
    tester.checkString("json_keys('[]')",
        "null", "VARCHAR(2000)");
    tester.checkString("json_keys('{\"foo\":100}')",
        "[\"foo\"]", "VARCHAR(2000)");
    tester.checkString("json_keys('{\"a\": 1, \"b\": {\"c\": 30}}')",
        "[\"a\",\"b\"]", "VARCHAR(2000)");
    tester.checkString("json_keys('[1, 2, {\"a\": 3}]')",
        "null", "VARCHAR(2000)");

    // lax test
    tester.checkString("json_keys('{}', 'lax $')",
        "[]", "VARCHAR(2000)");
    tester.checkString("json_keys('[]', 'lax $')",
        "null", "VARCHAR(2000)");
    tester.checkString("json_keys('{\"foo\":100}', 'lax $')",
        "[\"foo\"]", "VARCHAR(2000)");
    tester.checkString("json_keys('{\"a\": 1, \"b\": {\"c\": 30}}', 'lax $')",
        "[\"a\",\"b\"]", "VARCHAR(2000)");
    tester.checkString("json_keys('[1, 2, {\"a\": 3}]', 'lax $')",
        "null", "VARCHAR(2000)");
    tester.checkString("json_keys('{\"a\": 1, \"b\": {\"c\": 30}}', 'lax $.b')",
        "[\"c\"]", "VARCHAR(2000)");
    tester.checkString("json_keys('{\"foo\":100}', 'lax $.foo1')",
        "null", "VARCHAR(2000)");

    // strict test
    tester.checkString("json_keys('{}', 'strict $')",
        "[]", "VARCHAR(2000)");
    tester.checkString("json_keys('[]', 'strict $')",
        "null", "VARCHAR(2000)");
    tester.checkString("json_keys('{\"foo\":100}', 'strict $')",
        "[\"foo\"]", "VARCHAR(2000)");
    tester.checkString("json_keys('{\"a\": 1, \"b\": {\"c\": 30}}', 'strict $')",
        "[\"a\",\"b\"]", "VARCHAR(2000)");
    tester.checkString("json_keys('[1, 2, {\"a\": 3}]', 'strict $')",
        "null", "VARCHAR(2000)");
    tester.checkString("json_keys('{\"a\": 1, \"b\": {\"c\": 30}}', 'strict $.b')",
        "[\"c\"]", "VARCHAR(2000)");

    // catch error test
    tester.checkFails("json_keys('{\"foo\":100}', 'invalid $.foo')",
        "(?s).*Illegal jsonpath spec.*", true);
    tester.checkFails("json_keys('{\"foo\":100}', 'strict $.foo1')",
        "(?s).*No results for path.*", true);

    // nulls
    strictTester.checkFails("json_keys(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    tester.checkString("json_keys(null)", null, "VARCHAR(2000)");
    tester.checkNull("json_keys(cast(null as varchar))");
  }

  @Test public void testJsonRemove() {
    tester.checkString("json_remove('{\"foo\":100}', '$.foo')",
        "{}", "VARCHAR(2000)");
    tester.checkString("json_remove('{\"foo\":100, \"foo1\":100}', '$.foo')",
        "{\"foo1\":100}", "VARCHAR(2000)");
    tester.checkString("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$[1][0]')",
        "[\"a\",[\"c\"],\"d\"]", "VARCHAR(2000)");
    tester.checkString("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$[1]')",
        "[\"a\",\"d\"]", "VARCHAR(2000)");
    tester.checkString("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$[0]', '$[0]')",
        "[\"d\"]", "VARCHAR(2000)");
    tester.checkFails("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$')",
        "(?s).*Invalid input for.*", true);

    // nulls
    strictTester.checkFails("json_remove(^null^, '$')",
        "(?s).*Illegal use of 'NULL'.*", false);
    tester.checkString("json_remove(null, '$')", null, "VARCHAR(2000)");
    tester.checkNull("json_remove(cast(null as varchar), '$')");
  }

  @Test public void testJsonObject() {
    tester.checkString("json_object()", "{}", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_object('foo': 'bar')",
        "{\"foo\":\"bar\"}", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_object('foo': 'bar', 'foo2': 'bar2')",
        "{\"foo\":\"bar\",\"foo2\":\"bar2\"}", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_object('foo': null)",
        "{\"foo\":null}", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_object('foo': null null on null)",
        "{\"foo\":null}", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_object('foo': null absent on null)",
        "{}", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_object('foo': 100)",
        "{\"foo\":100}", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_object('foo': json_object('foo': 'bar'))",
        "{\"foo\":\"{\\\"foo\\\":\\\"bar\\\"}\"}", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_object('foo': json_object('foo': 'bar') format json)",
        "{\"foo\":{\"foo\":\"bar\"}}", "VARCHAR(2000) NOT NULL");
  }

  @Test public void testJsonObjectAgg() {
    checkAggType(tester, "json_objectagg('foo': 'bar')", "VARCHAR(2000) NOT NULL");
    checkAggType(tester, "json_objectagg('foo': null)", "VARCHAR(2000) NOT NULL");
    checkAggType(tester, "json_objectagg(100: 'bar')", "VARCHAR(2000) NOT NULL");
    strictTester.checkFails("^json_objectagg(100: 'bar')^",
        "(?s).*Cannot apply.*", false);
    final String[][] values = {
        {"'foo'", "'bar'"},
        {"'foo2'", "cast(null as varchar(2000))"},
        {"'foo3'", "'bar3'"}
    };
    tester.checkAggWithMultipleArgs("json_objectagg(x: x2)",
        values,
        "{\"foo\":\"bar\",\"foo2\":null,\"foo3\":\"bar3\"}",
        0.0D);
    tester.checkAggWithMultipleArgs("json_objectagg(x: x2 null on null)",
        values,
        "{\"foo\":\"bar\",\"foo2\":null,\"foo3\":\"bar3\"}",
        0.0D);
    tester.checkAggWithMultipleArgs("json_objectagg(x: x2 absent on null)",
        values,
        "{\"foo\":\"bar\",\"foo3\":\"bar3\"}",
        0.0D);
  }

  @Test public void testJsonValueExpressionOperator() {
    tester.checkScalar("'{}' format json", "{}", "ANY NOT NULL");
    tester.checkScalar("'[1, 2, 3]' format json", "[1, 2, 3]", "ANY NOT NULL");
    tester.checkNull("cast(null as varchar) format json");
    tester.checkScalar("'null' format json", "null", "ANY NOT NULL");
    strictTester.checkFails("^null^ format json", "(?s).*Illegal use of .NULL.*", false);
  }

  @Test public void testJsonArray() {
    tester.checkString("json_array()", "[]", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_array('foo')",
        "[\"foo\"]", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_array('foo', 'bar')",
        "[\"foo\",\"bar\"]", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_array(null)",
        "[]", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_array(null null on null)",
        "[null]", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_array(null absent on null)",
        "[]", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_array(100)",
        "[100]", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_array(json_array('foo'))",
        "[\"[\\\"foo\\\"]\"]", "VARCHAR(2000) NOT NULL");
    tester.checkString("json_array(json_array('foo') format json)",
        "[[\"foo\"]]", "VARCHAR(2000) NOT NULL");
  }

  @Test public void testJsonArrayAgg() {
    checkAggType(tester, "json_arrayagg('foo')", "VARCHAR(2000) NOT NULL");
    checkAggType(tester, "json_arrayagg(null)", "VARCHAR(2000) NOT NULL");
    final String[] values = {
        "'foo'",
        "cast(null as varchar(2000))",
        "'foo3'"
    };
    tester.checkAgg("json_arrayagg(x)",
        values,
        "[\"foo\",\"foo3\"]",
        0.0D);
    tester.checkAgg("json_arrayagg(x null on null)",
        values,
        "[\"foo\",null,\"foo3\"]",
        0.0D);
    tester.checkAgg("json_arrayagg(x absent on null)",
        values,
        "[\"foo\",\"foo3\"]",
        0.0D);
  }

  @Test public void testJsonPredicate() {
    tester.checkBoolean("'{}' is json value", true);
    tester.checkBoolean("'{]' is json value", false);
    tester.checkBoolean("'{}' is json object", true);
    tester.checkBoolean("'[]' is json object", false);
    tester.checkBoolean("'{}' is json array", false);
    tester.checkBoolean("'[]' is json array", true);
    tester.checkBoolean("'100' is json scalar", true);
    tester.checkBoolean("'[]' is json scalar", false);
    tester.checkBoolean("'{}' is not json value", false);
    tester.checkBoolean("'{]' is not json value", true);
    tester.checkBoolean("'{}' is not json object", false);
    tester.checkBoolean("'[]' is not json object", true);
    tester.checkBoolean("'{}' is not json array", true);
    tester.checkBoolean("'[]' is not json array", false);
    tester.checkBoolean("'100' is not json scalar", false);
    tester.checkBoolean("'[]' is not json scalar", true);
  }

  @Test public void testLowerFunc() {
    tester.setFor(SqlStdOperatorTable.LOWER);

    // SQL:2003 6.29.8 The type of lower is the type of its argument
    tester.checkString("lower('A')", "a", "CHAR(1) NOT NULL");
    tester.checkString("lower('a')", "a", "CHAR(1) NOT NULL");
    tester.checkString("lower('1')", "1", "CHAR(1) NOT NULL");
    tester.checkString("lower('AA')", "aa", "CHAR(2) NOT NULL");
    tester.checkNull("lower(cast(null as varchar(1)))");
  }

  @Test public void testInitcapFunc() {
    // Note: the initcap function is an Oracle defined function and is not
    // defined in the SQL:2003 standard
    // todo: implement in fennel
    tester.setFor(SqlStdOperatorTable.INITCAP, VM_FENNEL);

    tester.checkString("initcap('aA')", "Aa", "CHAR(2) NOT NULL");
    tester.checkString("initcap('Aa')", "Aa", "CHAR(2) NOT NULL");
    tester.checkString("initcap('1a')", "1a", "CHAR(2) NOT NULL");
    tester.checkString(
        "initcap('ab cd Ef 12')",
        "Ab Cd Ef 12",
        "CHAR(11) NOT NULL");
    tester.checkNull("initcap(cast(null as varchar(1)))");

    // dtbug 232
    strictTester.checkFails(
        "^initcap(cast(null as date))^",
        "Cannot apply 'INITCAP' to arguments of type 'INITCAP\\(<DATE>\\)'\\. Supported form\\(s\\): 'INITCAP\\(<CHARACTER>\\)'",
        false);
    tester.checkType("initcap(cast(null as date))", "VARCHAR");
  }

  @Test public void testPowerFunc() {
    tester.setFor(SqlStdOperatorTable.POWER);
    tester.checkScalarApprox(
        "power(2,-2)",
        "DOUBLE NOT NULL",
        0.25,
        0);
    tester.checkNull("power(cast(null as integer),2)");
    tester.checkNull("power(2,cast(null as double))");

    // 'pow' is an obsolete form of the 'power' function
    tester.checkFails(
        "^pow(2,-2)^",
        "No match found for function signature POW\\(<NUMERIC>, <NUMERIC>\\)",
        false);
  }

  @Test public void testSqrtFunc() {
    tester.setFor(
        SqlStdOperatorTable.SQRT, SqlTester.VmName.EXPAND);
    tester.checkType("sqrt(2)", "DOUBLE NOT NULL");
    tester.checkType("sqrt(cast(2 as float))", "DOUBLE NOT NULL");
    tester.checkType(
        "sqrt(case when false then 2 else null end)", "DOUBLE");
    strictTester.checkFails(
        "^sqrt('abc')^",
        "Cannot apply 'SQRT' to arguments of type 'SQRT\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'SQRT\\(<NUMERIC>\\)'",
        false);
    tester.checkType("sqrt('abc')", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "sqrt(2)",
        "DOUBLE NOT NULL",
        1.4142d,
        0.0001d);
    tester.checkScalarApprox(
        "sqrt(cast(2 as decimal(2, 0)))",
        "DOUBLE NOT NULL",
        1.4142d,
        0.0001d);
    tester.checkNull("sqrt(cast(null as integer))");
    tester.checkNull("sqrt(cast(null as double))");
  }

  @Test public void testExpFunc() {
    tester.setFor(SqlStdOperatorTable.EXP, VM_FENNEL);
    tester.checkScalarApprox(
        "exp(2)", "DOUBLE NOT NULL", 7.389056, 0.000001);
    tester.checkScalarApprox(
        "exp(-2)",
        "DOUBLE NOT NULL",
        0.1353,
        0.0001);
    tester.checkNull("exp(cast(null as integer))");
    tester.checkNull("exp(cast(null as double))");
  }

  @Test public void testModFunc() {
    tester.setFor(SqlStdOperatorTable.MOD);
    tester.checkScalarExact("mod(4,2)", "0");
    tester.checkScalarExact("mod(8,5)", "3");
    tester.checkScalarExact("mod(-12,7)", "-5");
    tester.checkScalarExact("mod(-12,-7)", "-5");
    tester.checkScalarExact("mod(12,-7)", "5");
    tester.checkScalarExact(
        "mod(cast(12 as tinyint), cast(-7 as tinyint))",
        "TINYINT NOT NULL",
        "5");

    if (!DECIMAL) {
      return;
    }
    tester.checkScalarExact(
        "mod(cast(9 as decimal(2, 0)), 7)",
        "INTEGER NOT NULL",
        "2");
    tester.checkScalarExact(
        "mod(7, cast(9 as decimal(2, 0)))",
        "DECIMAL(2, 0) NOT NULL",
        "7");
    tester.checkScalarExact(
        "mod(cast(-9 as decimal(2, 0)), cast(7 as decimal(1, 0)))",
        "DECIMAL(1, 0) NOT NULL",
        "-2");
  }

  @Test public void testModFuncNull() {
    tester.checkNull("mod(cast(null as integer),2)");
    tester.checkNull("mod(4,cast(null as tinyint))");
    if (!DECIMAL) {
      return;
    }
    tester.checkNull("mod(4,cast(null as decimal(12,0)))");
  }

  @Test public void testModFuncDivByZero() {
    // The extra CASE expression is to fool Janino.  It does constant
    // reduction and will throw the divide by zero exception while
    // compiling the expression.  The test frame work would then issue
    // unexpected exception occurred during "validation".  You cannot
    // submit as non-runtime because the janino exception does not have
    // error position information and the framework is unhappy with that.
    tester.checkFails(
        "mod(3,case 'a' when 'a' then 0 end)", DIVISION_BY_ZERO_MESSAGE, true);
  }

  @Test public void testLnFunc() {
    tester.setFor(SqlStdOperatorTable.LN);
    tester.checkScalarApprox(
        "ln(2.71828)",
        "DOUBLE NOT NULL",
        1.0,
        0.000001);
    tester.checkScalarApprox(
        "ln(2.71828)",
        "DOUBLE NOT NULL",
        0.999999327,
        0.0000001);
    tester.checkNull("ln(cast(null as tinyint))");
  }

  @Test public void testLogFunc() {
    tester.setFor(SqlStdOperatorTable.LOG10);
    tester.checkScalarApprox(
        "log10(10)",
        "DOUBLE NOT NULL",
        1.0,
        0.000001);
    tester.checkScalarApprox(
        "log10(100.0)",
        "DOUBLE NOT NULL",
        2.0,
        0.000001);
    tester.checkScalarApprox(
        "log10(cast(10e8 as double))",
        "DOUBLE NOT NULL",
        9.0,
        0.000001);
    tester.checkScalarApprox(
        "log10(cast(10e2 as float))",
        "DOUBLE NOT NULL",
        3.0,
        0.000001);
    tester.checkScalarApprox(
        "log10(cast(10e-3 as real))",
        "DOUBLE NOT NULL",
        -2.0,
        0.000001);
    tester.checkNull("log10(cast(null as real))");
  }

  @Test public void testRandFunc() {
    tester.setFor(SqlStdOperatorTable.RAND);
    tester.checkFails("^rand^", "Column 'RAND' not found in any table", false);
    for (int i = 0; i < 100; i++) {
      // Result must always be between 0 and 1, inclusive.
      tester.checkScalarApprox("rand()", "DOUBLE NOT NULL", 0.5, 0.5);
    }
  }

  @Test public void testRandSeedFunc() {
    tester.setFor(SqlStdOperatorTable.RAND);
    tester.checkScalarApprox("rand(1)", "DOUBLE NOT NULL", 0.6016, 0.0001);
    tester.checkScalarApprox("rand(2)", "DOUBLE NOT NULL", 0.4728, 0.0001);
  }

  @Test public void testRandIntegerFunc() {
    tester.setFor(SqlStdOperatorTable.RAND_INTEGER);
    for (int i = 0; i < 100; i++) {
      // Result must always be between 0 and 10, inclusive.
      tester.checkScalarApprox("rand_integer(11)", "INTEGER NOT NULL", 5.0,
          5.0);
    }
  }

  @Test public void testRandIntegerSeedFunc() {
    tester.setFor(SqlStdOperatorTable.RAND_INTEGER);
    tester.checkScalar("rand_integer(1, 11)", 4, "INTEGER NOT NULL");
    tester.checkScalar("rand_integer(2, 11)", 1, "INTEGER NOT NULL");
  }

  @Test public void testAbsFunc() {
    tester.setFor(SqlStdOperatorTable.ABS);
    tester.checkScalarExact("abs(-1)", "1");
    tester.checkScalarExact(
        "abs(cast(10 as TINYINT))", "TINYINT NOT NULL", "10");
    tester.checkScalarExact(
        "abs(cast(-20 as SMALLINT))", "SMALLINT NOT NULL", "20");
    tester.checkScalarExact(
        "abs(cast(-100 as INT))", "INTEGER NOT NULL", "100");
    tester.checkScalarExact(
        "abs(cast(1000 as BIGINT))", "BIGINT NOT NULL", "1000");
    tester.checkScalarExact(
        "abs(54.4)",
        "DECIMAL(3, 1) NOT NULL",
        "54.4");
    tester.checkScalarExact(
        "abs(-54.4)",
        "DECIMAL(3, 1) NOT NULL",
        "54.4");
    tester.checkScalarApprox(
        "abs(-9.32E-2)",
        "DOUBLE NOT NULL",
        0.0932,
        0);
    tester.checkScalarApprox(
        "abs(cast(-3.5 as double))",
        "DOUBLE NOT NULL",
        3.5,
        0);
    tester.checkScalarApprox(
        "abs(cast(-3.5 as float))",
        "FLOAT NOT NULL",
        3.5,
        0);
    tester.checkScalarApprox(
        "abs(cast(3.5 as real))",
        "REAL NOT NULL",
        3.5,
        0);

    tester.checkNull("abs(cast(null as double))");
  }

  @Test public void testAbsFuncIntervals() {
    tester.checkScalar(
        "abs(interval '-2' day)",
        "+2",
        "INTERVAL DAY NOT NULL");
    tester.checkScalar(
        "abs(interval '-5-03' year to month)",
        "+5-03",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkNull("abs(cast(null as interval hour))");
  }

  @Test public void testAcosFunc() {
    tester.setFor(
        SqlStdOperatorTable.ACOS);
    tester.checkType("acos(0)", "DOUBLE NOT NULL");
    tester.checkType("acos(cast(1 as float))", "DOUBLE NOT NULL");
    tester.checkType(
        "acos(case when false then 0.5 else null end)", "DOUBLE");
    strictTester.checkFails(
        "^acos('abc')^",
        "Cannot apply 'ACOS' to arguments of type 'ACOS\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'ACOS\\(<NUMERIC>\\)'",
        false);
    tester.checkType("acos('abc')", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "acos(0.5)",
        "DOUBLE NOT NULL",
        1.0472d,
        0.0001d);
    tester.checkScalarApprox(
        "acos(cast(0.5 as decimal(1, 1)))",
        "DOUBLE NOT NULL",
        1.0472d,
        0.0001d);
    tester.checkNull("acos(cast(null as integer))");
    tester.checkNull("acos(cast(null as double))");
  }

  @Test public void testAsinFunc() {
    tester.setFor(
        SqlStdOperatorTable.ASIN);
    tester.checkType("asin(0)", "DOUBLE NOT NULL");
    tester.checkType("asin(cast(1 as float))", "DOUBLE NOT NULL");
    tester.checkType(
        "asin(case when false then 0.5 else null end)", "DOUBLE");
    strictTester.checkFails(
        "^asin('abc')^",
        "Cannot apply 'ASIN' to arguments of type 'ASIN\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'ASIN\\(<NUMERIC>\\)'",
        false);
    tester.checkType("asin('abc')", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "asin(0.5)",
        "DOUBLE NOT NULL",
        0.5236d,
        0.0001d);
    tester.checkScalarApprox(
        "asin(cast(0.5 as decimal(1, 1)))",
        "DOUBLE NOT NULL",
        0.5236d,
        0.0001d);
    tester.checkNull("asin(cast(null as integer))");
    tester.checkNull("asin(cast(null as double))");
  }

  @Test public void testAtanFunc() {
    tester.setFor(
        SqlStdOperatorTable.ATAN);
    tester.checkType("atan(2)", "DOUBLE NOT NULL");
    tester.checkType("atan(cast(2 as float))", "DOUBLE NOT NULL");
    tester.checkType(
        "atan(case when false then 2 else null end)", "DOUBLE");
    strictTester.checkFails(
        "^atan('abc')^",
        "Cannot apply 'ATAN' to arguments of type 'ATAN\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'ATAN\\(<NUMERIC>\\)'",
        false);
    tester.checkType("atan('abc')", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "atan(2)",
        "DOUBLE NOT NULL",
        1.1071d,
        0.0001d);
    tester.checkScalarApprox(
        "atan(cast(2 as decimal(1, 0)))",
        "DOUBLE NOT NULL",
        1.1071d,
        0.0001d);
    tester.checkNull("atan(cast(null as integer))");
    tester.checkNull("atan(cast(null as double))");
  }

  @Test public void testAtan2Func() {
    tester.setFor(
        SqlStdOperatorTable.ATAN2);
    tester.checkType("atan2(2, -2)", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "atan2(cast(1 as float), -1)",
        "DOUBLE NOT NULL",
        2.3562d,
        0.0001d);
    tester.checkType(
        "atan2(case when false then 0.5 else null end, -1)", "DOUBLE");
    strictTester.checkFails(
        "^atan2('abc', 'def')^",
        "Cannot apply 'ATAN2' to arguments of type 'ATAN2\\(<CHAR\\(3\\)>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'ATAN2\\(<NUMERIC>, <NUMERIC>\\)'",
        false);
    tester.checkType("atan2('abc', 'def')", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "atan2(0.5, -0.5)",
        "DOUBLE NOT NULL",
        2.3562d,
        0.0001d);
    tester.checkScalarApprox(
        "atan2(cast(0.5 as decimal(1, 1)), cast(-0.5 as decimal(1, 1)))",
        "DOUBLE NOT NULL",
        2.3562d,
        0.0001d);
    tester.checkNull("atan2(cast(null as integer), -1)");
    tester.checkNull("atan2(1, cast(null as double))");
  }

  @Test public void testCosFunc() {
    tester.setFor(
        SqlStdOperatorTable.COS);
    tester.checkType("cos(1)", "DOUBLE NOT NULL");
    tester.checkType("cos(cast(1 as float))", "DOUBLE NOT NULL");
    tester.checkType(
        "cos(case when false then 1 else null end)", "DOUBLE");
    strictTester.checkFails(
        "^cos('abc')^",
        "Cannot apply 'COS' to arguments of type 'COS\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'COS\\(<NUMERIC>\\)'",
        false);
    tester.checkType("cos('abc')", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "cos(1)",
        "DOUBLE NOT NULL",
        0.5403d,
        0.0001d);
    tester.checkScalarApprox(
        "cos(cast(1 as decimal(1, 0)))",
        "DOUBLE NOT NULL",
        0.5403d,
        0.0001d);
    tester.checkNull("cos(cast(null as integer))");
    tester.checkNull("cos(cast(null as double))");
  }

  @Test public void testCotFunc() {
    tester.setFor(
        SqlStdOperatorTable.COT);
    tester.checkType("cot(1)", "DOUBLE NOT NULL");
    tester.checkType("cot(cast(1 as float))", "DOUBLE NOT NULL");
    tester.checkType(
        "cot(case when false then 1 else null end)", "DOUBLE");
    strictTester.checkFails(
        "^cot('abc')^",
        "Cannot apply 'COT' to arguments of type 'COT\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'COT\\(<NUMERIC>\\)'",
        false);
    tester.checkType("cot('abc')", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "cot(1)",
        "DOUBLE NOT NULL",
        0.6421d,
        0.0001d);
    tester.checkScalarApprox(
        "cot(cast(1 as decimal(1, 0)))",
        "DOUBLE NOT NULL",
        0.6421d,
        0.0001d);
    tester.checkNull("cot(cast(null as integer))");
    tester.checkNull("cot(cast(null as double))");
  }

  @Test public void testDegreesFunc() {
    tester.setFor(
        SqlStdOperatorTable.DEGREES);
    tester.checkType("degrees(1)", "DOUBLE NOT NULL");
    tester.checkType("degrees(cast(1 as float))", "DOUBLE NOT NULL");
    tester.checkType(
        "degrees(case when false then 1 else null end)", "DOUBLE");
    strictTester.checkFails(
        "^degrees('abc')^",
        "Cannot apply 'DEGREES' to arguments of type 'DEGREES\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'DEGREES\\(<NUMERIC>\\)'",
        false);
    tester.checkType("degrees('abc')", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "degrees(1)",
        "DOUBLE NOT NULL",
        57.2958d,
        0.0001d);
    tester.checkScalarApprox(
        "degrees(cast(1 as decimal(1, 0)))",
        "DOUBLE NOT NULL",
        57.2958d,
        0.0001d);
    tester.checkNull("degrees(cast(null as integer))");
    tester.checkNull("degrees(cast(null as double))");
  }

  @Test public void testPiFunc() {
    tester.setFor(SqlStdOperatorTable.PI);
    tester.checkScalarApprox("PI", "DOUBLE NOT NULL", 3.1415d, 0.0001d);
    tester.checkFails("^PI()^",
        "No match found for function signature PI\\(\\)", false);

    // assert that PI function is not dynamic [CALCITE-2750]
    assertEquals(
        "PI operator should not be identified as dynamic function",
        SqlStdOperatorTable.PI.isDynamicFunction(),
        false);
  }

  @Test public void testRadiansFunc() {
    tester.setFor(
        SqlStdOperatorTable.RADIANS);
    tester.checkType("radians(42)", "DOUBLE NOT NULL");
    tester.checkType("radians(cast(42 as float))", "DOUBLE NOT NULL");
    tester.checkType(
        "radians(case when false then 42 else null end)", "DOUBLE");
    strictTester.checkFails(
        "^radians('abc')^",
        "Cannot apply 'RADIANS' to arguments of type 'RADIANS\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'RADIANS\\(<NUMERIC>\\)'",
        false);
    tester.checkType("radians('abc')", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "radians(42)",
        "DOUBLE NOT NULL",
        0.7330d,
        0.0001d);
    tester.checkScalarApprox(
        "radians(cast(42 as decimal(2, 0)))",
        "DOUBLE NOT NULL",
        0.7330d,
        0.0001d);
    tester.checkNull("radians(cast(null as integer))");
    tester.checkNull("radians(cast(null as double))");
  }


  @Test public void testRoundFunc() {
    tester.setFor(
        SqlStdOperatorTable.ROUND);
    tester.checkType("round(42, -1)", "INTEGER NOT NULL");
    tester.checkType("round(cast(42 as float), 1)", "FLOAT NOT NULL");
    tester.checkType(
        "round(case when false then 42 else null end, -1)", "INTEGER");
    strictTester.checkFails(
        "^round('abc', 'def')^",
        "Cannot apply 'ROUND' to arguments of type 'ROUND\\(<CHAR\\(3\\)>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'ROUND\\(<NUMERIC>, <INTEGER>\\)'",
        false);
    tester.checkType("round('abc', 'def')", "DECIMAL(19, 19) NOT NULL");
    tester.checkScalar(
        "round(42, -1)",
        40,
        "INTEGER NOT NULL");
    tester.checkScalar(
        "round(cast(42.346 as decimal(2, 3)), 2)",
        BigDecimal.valueOf(4235, 2),
        "DECIMAL(2, 3) NOT NULL");
    tester.checkScalar(
        "round(cast(-42.346 as decimal(2, 3)), 2)",
        BigDecimal.valueOf(-4235, 2),
        "DECIMAL(2, 3) NOT NULL");
    tester.checkNull("round(cast(null as integer), 1)");
    tester.checkNull("round(cast(null as double), 1)");
    tester.checkNull("round(43.21, cast(null as integer))");

    tester.checkNull("round(cast(null as double))");
    tester.checkScalar("round(42)", 42, "INTEGER NOT NULL");
    tester.checkScalar(
        "round(cast(42.346 as decimal(2, 3)))",
        BigDecimal.valueOf(42, 0),
        "DECIMAL(2, 3) NOT NULL");
    tester.checkScalar("round(42.324)",
        BigDecimal.valueOf(42, 0),
        "DECIMAL(5, 3) NOT NULL");
    tester.checkScalar("round(42.724)",
        BigDecimal.valueOf(43, 0),
        "DECIMAL(5, 3) NOT NULL");
  }

  @Test public void testSignFunc() {
    tester.setFor(
        SqlStdOperatorTable.SIGN);
    tester.checkType("sign(1)", "INTEGER NOT NULL");
    tester.checkType("sign(cast(1 as float))", "FLOAT NOT NULL");
    tester.checkType(
        "sign(case when false then 1 else null end)", "INTEGER");
    strictTester.checkFails(
        "^sign('abc')^",
        "Cannot apply 'SIGN' to arguments of type 'SIGN\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'SIGN\\(<NUMERIC>\\)'",
        false);
    tester.checkType("sign('abc')", "DECIMAL(19, 19) NOT NULL");
    tester.checkScalar(
        "sign(1)",
        1,
        "INTEGER NOT NULL");
    tester.checkScalar(
        "sign(cast(-1 as decimal(1, 0)))",
        BigDecimal.valueOf(-1),
        "DECIMAL(1, 0) NOT NULL");
    tester.checkScalar(
        "sign(cast(0 as float))",
        0d,
        "FLOAT NOT NULL");
    tester.checkNull("sign(cast(null as integer))");
    tester.checkNull("sign(cast(null as double))");
  }

  @Test public void testSinFunc() {
    tester.setFor(
        SqlStdOperatorTable.SIN);
    tester.checkType("sin(1)", "DOUBLE NOT NULL");
    tester.checkType("sin(cast(1 as float))", "DOUBLE NOT NULL");
    tester.checkType(
        "sin(case when false then 1 else null end)", "DOUBLE");
    strictTester.checkFails(
        "^sin('abc')^",
        "Cannot apply 'SIN' to arguments of type 'SIN\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'SIN\\(<NUMERIC>\\)'",
        false);
    tester.checkType("sin('abc')", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "sin(1)",
        "DOUBLE NOT NULL",
        0.8415d,
        0.0001d);
    tester.checkScalarApprox(
        "sin(cast(1 as decimal(1, 0)))",
        "DOUBLE NOT NULL",
        0.8415d,
        0.0001d);
    tester.checkNull("sin(cast(null as integer))");
    tester.checkNull("sin(cast(null as double))");
  }

  @Test public void testTanFunc() {
    tester.setFor(
        SqlStdOperatorTable.TAN);
    tester.checkType("tan(1)", "DOUBLE NOT NULL");
    tester.checkType("tan(cast(1 as float))", "DOUBLE NOT NULL");
    tester.checkType(
        "tan(case when false then 1 else null end)", "DOUBLE");
    strictTester.checkFails(
        "^tan('abc')^",
        "Cannot apply 'TAN' to arguments of type 'TAN\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'TAN\\(<NUMERIC>\\)'",
        false);
    tester.checkType("tan('abc')", "DOUBLE NOT NULL");
    tester.checkScalarApprox(
        "tan(1)",
        "DOUBLE NOT NULL",
        1.5574d,
        0.0001d);
    tester.checkScalarApprox(
        "tan(cast(1 as decimal(1, 0)))",
        "DOUBLE NOT NULL",
        1.5574d,
        0.0001d);
    tester.checkNull("tan(cast(null as integer))");
    tester.checkNull("tan(cast(null as double))");
  }

  @Test public void testTruncateFunc() {
    tester.setFor(
        SqlStdOperatorTable.TRUNCATE);
    tester.checkType("truncate(42, -1)", "INTEGER NOT NULL");
    tester.checkType("truncate(cast(42 as float), 1)", "FLOAT NOT NULL");
    tester.checkType(
        "truncate(case when false then 42 else null end, -1)", "INTEGER");
    strictTester.checkFails(
        "^truncate('abc', 'def')^",
        "Cannot apply 'TRUNCATE' to arguments of type 'TRUNCATE\\(<CHAR\\(3\\)>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): 'TRUNCATE\\(<NUMERIC>, <INTEGER>\\)'",
        false);
    tester.checkType("truncate('abc', 'def')", "DECIMAL(19, 19) NOT NULL");
    tester.checkScalar(
        "truncate(42, -1)",
        40,
        "INTEGER NOT NULL");
    tester.checkScalar(
        "truncate(cast(42.345 as decimal(2, 3)), 2)",
        BigDecimal.valueOf(4234, 2),
        "DECIMAL(2, 3) NOT NULL");
    tester.checkScalar(
        "truncate(cast(-42.345 as decimal(2, 3)), 2)",
        BigDecimal.valueOf(-4234, 2),
        "DECIMAL(2, 3) NOT NULL");
    tester.checkNull("truncate(cast(null as integer), 1)");
    tester.checkNull("truncate(cast(null as double), 1)");
    tester.checkNull("truncate(43.21, cast(null as integer))");

    tester.checkScalar("truncate(42)", 42, "INTEGER NOT NULL");
    tester.checkScalar("truncate(42.324)",
        BigDecimal.valueOf(42, 0),
        "DECIMAL(5, 3) NOT NULL");
    tester.checkScalar("truncate(cast(42.324 as float))", 42F, "FLOAT NOT NULL");
    tester.checkScalar(
        "truncate(cast(42.345 as decimal(2, 3)))",
        BigDecimal.valueOf(42, 0),
        "DECIMAL(2, 3) NOT NULL");
    tester.checkNull("truncate(cast(null as integer))");
    tester.checkNull("truncate(cast(null as double))");
  }

  @Test public void testNullifFunc() {
    tester.setFor(SqlStdOperatorTable.NULLIF, VM_EXPAND);
    tester.checkNull("nullif(1,1)");
    tester.checkScalarExact(
        "nullif(1.5, 13.56)",
        "DECIMAL(2, 1)",
        "1.5");
    tester.checkScalarExact(
        "nullif(13.56, 1.5)",
        "DECIMAL(4, 2)",
        "13.56");
    tester.checkScalarExact("nullif(1.5, 3)", "DECIMAL(2, 1)", "1.5");
    tester.checkScalarExact("nullif(3, 1.5)", "INTEGER", "3");
    tester.checkScalarApprox("nullif(1.5e0, 3e0)", "DOUBLE", 1.5, 0);
    tester.checkScalarApprox(
        "nullif(1.5, cast(3e0 as REAL))",
        "DECIMAL(2, 1)",
        1.5,
        0);
    tester.checkScalarExact("nullif(3, 1.5e0)", "INTEGER", "3");
    tester.checkScalarExact(
        "nullif(3, cast(1.5e0 as REAL))",
        "INTEGER",
        "3");
    tester.checkScalarApprox("nullif(1.5e0, 3.4)", "DOUBLE", 1.5, 0);
    tester.checkScalarExact(
        "nullif(3.4, 1.5e0)",
        "DECIMAL(2, 1)",
        "3.4");
    tester.checkString(
        "nullif('a','bc')",
        "a",
        "CHAR(1)");
    tester.checkString(
        "nullif('a',cast(null as varchar(1)))",
        "a",
        "CHAR(1)");
    tester.checkNull("nullif(cast(null as varchar(1)),'a')");
    tester.checkNull("nullif(cast(null as numeric(4,3)), 4.3)");

    // Error message reflects the fact that Nullif is expanded before it is
    // validated (like a C macro). Not perfect, but good enough.
    tester.checkFails(
        "1 + ^nullif(1, date '2005-8-4')^ + 2",
        "(?s)Cannot apply '=' to arguments of type '<INTEGER> = <DATE>'\\..*",
        false);

    tester.checkFails(
        "1 + ^nullif(1, 2, 3)^ + 2",
        "Invalid number of arguments to function 'NULLIF'\\. Was expecting 2 arguments",
        false);
  }

  @Test public void testNullIfOperatorIntervals() {
    tester.checkScalar(
        "nullif(interval '2' month, interval '3' year)",
        "+2",
        "INTERVAL MONTH");
    tester.checkScalar(
        "nullif(interval '2 5' day to hour, interval '5' second)",
        "+2 05",
        "INTERVAL DAY TO HOUR");
    tester.checkNull(
        "nullif(interval '3' day, interval '3' day)");
  }

  @Test public void testCoalesceFunc() {
    tester.setFor(SqlStdOperatorTable.COALESCE, VM_EXPAND);
    tester.checkString("coalesce('a','b')", "a", "CHAR(1) NOT NULL");
    tester.checkScalarExact("coalesce(null,null,3)", "3");
    strictTester.checkFails(
        "1 + ^coalesce('a', 'b', 1, null)^ + 2",
        "Illegal mixing of types in CASE or COALESCE statement",
        false);
    tester.checkType("1 + coalesce('a', 'b', 1, null) + 2",
        "INTEGER");
  }

  @Test public void testUserFunc() {
    tester.setFor(SqlStdOperatorTable.USER, VM_FENNEL);
    tester.checkString("USER", "sa", "VARCHAR(2000) NOT NULL");
  }

  @Test public void testCurrentUserFunc() {
    tester.setFor(SqlStdOperatorTable.CURRENT_USER, VM_FENNEL);
    tester.checkString("CURRENT_USER", "sa", "VARCHAR(2000) NOT NULL");
  }

  @Test public void testSessionUserFunc() {
    tester.setFor(SqlStdOperatorTable.SESSION_USER, VM_FENNEL);
    tester.checkString("SESSION_USER", "sa", "VARCHAR(2000) NOT NULL");
  }

  @Test public void testSystemUserFunc() {
    tester.setFor(SqlStdOperatorTable.SYSTEM_USER, VM_FENNEL);
    String user = System.getProperty("user.name"); // e.g. "jhyde"
    tester.checkString("SYSTEM_USER", user, "VARCHAR(2000) NOT NULL");
  }

  @Test public void testCurrentPathFunc() {
    tester.setFor(SqlStdOperatorTable.CURRENT_PATH, VM_FENNEL);
    tester.checkString("CURRENT_PATH", "", "VARCHAR(2000) NOT NULL");
  }

  @Test public void testCurrentRoleFunc() {
    tester.setFor(SqlStdOperatorTable.CURRENT_ROLE, VM_FENNEL);
    // By default, the CURRENT_ROLE function returns
    // the empty string because a role has to be set explicitly.
    tester.checkString("CURRENT_ROLE", "", "VARCHAR(2000) NOT NULL");
  }

  @Test public void testCurrentCatalogFunc() {
    tester.setFor(SqlStdOperatorTable.CURRENT_CATALOG, VM_FENNEL);
    // By default, the CURRENT_CATALOG function returns
    // the empty string because a catalog has to be set explicitly.
    tester.checkString("CURRENT_CATALOG", "", "VARCHAR(2000) NOT NULL");
  }

  @Test public void testLocalTimeFunc() {
    tester.setFor(SqlStdOperatorTable.LOCALTIME);
    tester.checkScalar("LOCALTIME", TIME_PATTERN, "TIME(0) NOT NULL");
    tester.checkFails(
        "^LOCALTIME()^",
        "No match found for function signature LOCALTIME\\(\\)",
        false);
    tester.checkScalar(
        "LOCALTIME(1)", TIME_PATTERN,
        "TIME(1) NOT NULL");

    final Pair<String, Hook.Closeable> pair = currentTimeString(LOCAL_TZ);
    tester.checkScalar(
        "CAST(LOCALTIME AS VARCHAR(30))",
        Pattern.compile(
            pair.left.substring(11) + "[0-9][0-9]:[0-9][0-9]"),
        "VARCHAR(30) NOT NULL");
    tester.checkScalar(
        "LOCALTIME",
        Pattern.compile(
            pair.left.substring(11) + "[0-9][0-9]:[0-9][0-9]"),
        "TIME(0) NOT NULL");
    pair.right.close();
  }

  @Test public void testLocalTimestampFunc() {
    tester.setFor(SqlStdOperatorTable.LOCALTIMESTAMP);
    tester.checkScalar(
        "LOCALTIMESTAMP", TIMESTAMP_PATTERN,
        "TIMESTAMP(0) NOT NULL");
    tester.checkFails(
        "^LOCALTIMESTAMP()^",
        "No match found for function signature LOCALTIMESTAMP\\(\\)",
        false);
    tester.checkFails(
        "^LOCALTIMESTAMP(4000000000)^", LITERAL_OUT_OF_RANGE_MESSAGE, false);
    tester.checkFails(
        "^LOCALTIMESTAMP(9223372036854775807)^", LITERAL_OUT_OF_RANGE_MESSAGE,
        false);
    tester.checkScalar(
        "LOCALTIMESTAMP(1)", TIMESTAMP_PATTERN,
        "TIMESTAMP(1) NOT NULL");

    // Check that timestamp is being generated in the right timezone by
    // generating a specific timestamp.
    final Pair<String, Hook.Closeable> pair = currentTimeString(
        LOCAL_TZ);
    tester.checkScalar(
        "CAST(LOCALTIMESTAMP AS VARCHAR(30))",
        Pattern.compile(pair.left + "[0-9][0-9]:[0-9][0-9]"),
        "VARCHAR(30) NOT NULL");
    tester.checkScalar(
        "LOCALTIMESTAMP",
        Pattern.compile(pair.left + "[0-9][0-9]:[0-9][0-9]"),
        "TIMESTAMP(0) NOT NULL");
    pair.right.close();
  }

  @Test public void testCurrentTimeFunc() {
    tester.setFor(SqlStdOperatorTable.CURRENT_TIME);
    tester.checkScalar(
        "CURRENT_TIME", TIME_PATTERN,
        "TIME(0) NOT NULL");
    tester.checkFails(
        "^CURRENT_TIME()^",
        "No match found for function signature CURRENT_TIME\\(\\)",
        false);
    tester.checkScalar(
        "CURRENT_TIME(1)", TIME_PATTERN, "TIME(1) NOT NULL");

    final Pair<String, Hook.Closeable> pair = currentTimeString(CURRENT_TZ);
    tester.checkScalar(
        "CAST(CURRENT_TIME AS VARCHAR(30))",
        Pattern.compile(pair.left.substring(11) + "[0-9][0-9]:[0-9][0-9]"),
        "VARCHAR(30) NOT NULL");
    tester.checkScalar(
        "CURRENT_TIME",
        Pattern.compile(pair.left.substring(11) + "[0-9][0-9]:[0-9][0-9]"),
        "TIME(0) NOT NULL");
    pair.right.close();
  }

  @Test public void testCurrentTimestampFunc() {
    tester.setFor(SqlStdOperatorTable.CURRENT_TIMESTAMP);
    tester.checkScalar(
        "CURRENT_TIMESTAMP", TIMESTAMP_PATTERN,
        "TIMESTAMP(0) NOT NULL");
    tester.checkFails(
        "^CURRENT_TIMESTAMP()^",
        "No match found for function signature CURRENT_TIMESTAMP\\(\\)",
        false);
    tester.checkFails(
        "^CURRENT_TIMESTAMP(4000000000)^", LITERAL_OUT_OF_RANGE_MESSAGE, false);
    tester.checkScalar(
        "CURRENT_TIMESTAMP(1)", TIMESTAMP_PATTERN,
        "TIMESTAMP(1) NOT NULL");

    final Pair<String, Hook.Closeable> pair = currentTimeString(
        CURRENT_TZ);
    tester.checkScalar(
        "CAST(CURRENT_TIMESTAMP AS VARCHAR(30))",
        Pattern.compile(pair.left + "[0-9][0-9]:[0-9][0-9]"),
        "VARCHAR(30) NOT NULL");
    tester.checkScalar(
        "CURRENT_TIMESTAMP",
        Pattern.compile(pair.left + "[0-9][0-9]:[0-9][0-9]"),
        "TIMESTAMP(0) NOT NULL");
    pair.right.close();
  }

  /**
   * Returns a time string, in GMT, that will be valid for at least 2 minutes.
   *
   * <p>For example, at "2005-01-01 12:34:56 PST", returns "2005-01-01 20:".
   * At "2005-01-01 12:34:59 PST", waits a minute, then returns "2005-01-01
   * 21:".
   *
   * @param tz Time zone
   * @return Time string
   */
  protected static Pair<String, Hook.Closeable> currentTimeString(TimeZone tz) {
    final Calendar calendar;
    final Hook.Closeable closeable;
    if (CalciteSystemProperty.TEST_SLOW.value()) {
      calendar = getCalendarNotTooNear(Calendar.HOUR_OF_DAY);
      closeable = () -> { };
    } else {
      calendar = Util.calendar();
      calendar.set(Calendar.YEAR, 2014);
      calendar.set(Calendar.MONTH, 8);
      calendar.set(Calendar.DATE, 7);
      calendar.set(Calendar.HOUR_OF_DAY, 17);
      calendar.set(Calendar.MINUTE, 8);
      calendar.set(Calendar.SECOND, 48);
      calendar.set(Calendar.MILLISECOND, 15);
      final long timeInMillis = calendar.getTimeInMillis();
      closeable = Hook.CURRENT_TIME.addThread(
          (Consumer<Holder<Long>>) o -> o.set(timeInMillis));
    }

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:", Locale.ROOT);
    sdf.setTimeZone(tz);
    return Pair.of(sdf.format(calendar.getTime()), closeable);
  }

  @Test public void testCurrentDateFunc() {
    tester.setFor(SqlStdOperatorTable.CURRENT_DATE, VM_FENNEL);

    // A tester with a lenient conformance that allows parentheses.
    final SqlTester tester1 = tester
        .withConformance(SqlConformanceEnum.LENIENT);

    tester.checkScalar("CURRENT_DATE", DATE_PATTERN, "DATE NOT NULL");
    tester.checkScalar(
        "(CURRENT_DATE - CURRENT_DATE) DAY",
        "+0",
        "INTERVAL DAY NOT NULL");
    tester.checkBoolean("CURRENT_DATE IS NULL", false);
    tester.checkBoolean("CURRENT_DATE IS NOT NULL", true);
    tester.checkBoolean("NOT (CURRENT_DATE IS NULL)", true);
    tester.checkFails(
        "^CURRENT_DATE()^",
        "No match found for function signature CURRENT_DATE\\(\\)",
        false);

    tester1.checkBoolean("CURRENT_DATE() IS NULL", false);
    tester1.checkBoolean("CURRENT_DATE IS NOT NULL", true);
    tester1.checkBoolean("NOT (CURRENT_DATE() IS NULL)", true);
    tester1.checkType("CURRENT_DATE", "DATE NOT NULL");
    tester1.checkType("CURRENT_DATE()", "DATE NOT NULL");
    tester1.checkType("CURRENT_TIMESTAMP()", "TIMESTAMP(0) NOT NULL");
    tester1.checkType("CURRENT_TIME()", "TIME(0) NOT NULL");

    // Check the actual value.
    final Pair<String, Hook.Closeable> pair = currentTimeString(LOCAL_TZ);
    final String dateString = pair.left;
    try (Hook.Closeable ignore = pair.right) {
      tester.checkScalar("CAST(CURRENT_DATE AS VARCHAR(30))",
          dateString.substring(0, 10),
          "VARCHAR(30) NOT NULL");
      tester.checkScalar("CURRENT_DATE",
          dateString.substring(0, 10),
          "DATE NOT NULL");

      tester1.checkScalar("CAST(CURRENT_DATE AS VARCHAR(30))",
          dateString.substring(0, 10),
          "VARCHAR(30) NOT NULL");
      tester1.checkScalar("CAST(CURRENT_DATE() AS VARCHAR(30))",
          dateString.substring(0, 10),
          "VARCHAR(30) NOT NULL");
      tester1.checkScalar("CURRENT_DATE",
          dateString.substring(0, 10),
          "DATE NOT NULL");
      tester1.checkScalar("CURRENT_DATE()",
          dateString.substring(0, 10),
          "DATE NOT NULL");
    }
  }

  @Test public void testLastDayFunc() {
    tester.setFor(SqlStdOperatorTable.LAST_DAY);
    tester.checkScalar("last_day(DATE '2019-02-10')",
        "2019-02-28", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '2019-06-10')",
        "2019-06-30", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '2019-07-10')",
        "2019-07-31", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '2019-09-10')",
        "2019-09-30", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '2019-12-10')",
        "2019-12-31", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '9999-12-10')",
        "9999-12-31", "DATE NOT NULL");

    // Edge tests
    tester.checkScalar("last_day(DATE '1900-01-01')",
        "1900-01-31", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '1935-02-01')",
        "1935-02-28", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '1965-09-01')",
        "1965-09-30", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '1970-01-01')",
        "1970-01-31", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '2019-02-28')",
        "2019-02-28", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '2019-12-31')",
        "2019-12-31", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '2019-01-01')",
        "2019-01-31", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '2019-06-30')",
        "2019-06-30", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '2020-02-20')",
        "2020-02-29", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '2020-02-29')",
        "2020-02-29", "DATE NOT NULL");
    tester.checkScalar("last_day(DATE '9999-12-31')",
        "9999-12-31", "DATE NOT NULL");

    tester.checkNull("last_day(cast(null as date))");

    tester.checkScalar("last_day(TIMESTAMP '2019-02-10 02:10:12')",
        "2019-02-28", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '2019-06-10 06:10:16')",
        "2019-06-30", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '2019-07-10 07:10:17')",
        "2019-07-31", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '2019-09-10 09:10:19')",
        "2019-09-30", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '2019-12-10 12:10:22')",
        "2019-12-31", "DATE NOT NULL");

    // Edge tests
    tester.checkScalar("last_day(TIMESTAMP '1900-01-01 01:01:02')",
        "1900-01-31", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '1935-02-01 02:01:03')",
        "1935-02-28", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '1970-01-01 01:01:02')",
        "1970-01-31", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '2019-02-28 02:28:30')",
        "2019-02-28", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '2019-12-31 12:31:43')",
        "2019-12-31", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '2019-01-01 01:01:02')",
        "2019-01-31", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '2019-06-30 06:30:36')",
        "2019-06-30", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '2020-02-20 02:20:33')",
        "2020-02-29", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '2020-02-29 02:29:31')",
        "2020-02-29", "DATE NOT NULL");
    tester.checkScalar("last_day(TIMESTAMP '9999-12-31 12:31:43')",
        "9999-12-31", "DATE NOT NULL");

    tester.checkNull("last_day(cast(null as timestamp))");
  }

  @Test public void testSubstringFunction() {
    tester.setFor(SqlStdOperatorTable.SUBSTRING);
    tester.checkString(
        "substring('abc' from 1 for 2)",
        "ab",
        "VARCHAR(3) NOT NULL");
    tester.checkString(
        "substring('abc' from 2 for 8)",
        "bc",
        "VARCHAR(3) NOT NULL");
    tester.checkString(
        "substring('abc' from 0 for 2)",
        "a",
        "VARCHAR(3) NOT NULL");
    tester.checkString(
        "substring('abc' from 0 for 0)",
        "",
        "VARCHAR(3) NOT NULL");
    tester.checkString(
        "substring('abc' from 8 for 2)",
        "",
        "VARCHAR(3) NOT NULL");
    tester.checkFails(
        "substring('abc' from 1 for -1)",
        "Substring error: negative substring length not allowed",
        true);
    tester.checkString(
        "substring('abc' from 2)", "bc", "VARCHAR(3) NOT NULL");
    tester.checkString(
        "substring('abc' from 0)", "abc", "VARCHAR(3) NOT NULL");
    tester.checkString(
        "substring('abc' from 8)", "", "VARCHAR(3) NOT NULL");
    tester.checkString(
        "substring('abc' from -2)", "bc", "VARCHAR(3) NOT NULL");

    tester.checkString(
        "substring(x'aabbcc' from 1 for 2)",
        "aabb",
        "VARBINARY(3) NOT NULL");
    tester.checkString(
        "substring(x'aabbcc' from 2 for 8)",
        "bbcc",
        "VARBINARY(3) NOT NULL");
    tester.checkString(
        "substring(x'aabbcc' from 0 for 2)",
        "aa",
        "VARBINARY(3) NOT NULL");
    tester.checkString(
        "substring(x'aabbcc' from 0 for 0)",
        "",
        "VARBINARY(3) NOT NULL");
    tester.checkString(
        "substring(x'aabbcc' from 8 for 2)",
        "",
        "VARBINARY(3) NOT NULL");
    tester.checkFails(
        "substring(x'aabbcc' from 1 for -1)",
        "Substring error: negative substring length not allowed",
        true);
    tester.checkString(
        "substring(x'aabbcc' from 2)", "bbcc", "VARBINARY(3) NOT NULL");
    tester.checkString(
        "substring(x'aabbcc' from 0)", "aabbcc", "VARBINARY(3) NOT NULL");
    tester.checkString(
        "substring(x'aabbcc' from 8)", "", "VARBINARY(3) NOT NULL");
    tester.checkString(
        "substring(x'aabbcc' from -2)", "bbcc", "VARBINARY(3) NOT NULL");

    if (Bug.FRG296_FIXED) {
      // substring regexp not supported yet
      tester.checkString(
          "substring('foobar' from '%#\"o_b#\"%' for'#')",
          "oob",
          "xx");
    }
    tester.checkNull("substring(cast(null as varchar(1)),1,2)");
  }

  @Test public void testTrimFunc() {
    tester.setFor(SqlStdOperatorTable.TRIM);

    // SQL:2003 6.29.11 Trimming a CHAR yields a VARCHAR
    tester.checkString(
        "trim('a' from 'aAa')",
        "A",
        "VARCHAR(3) NOT NULL");
    tester.checkString(
        "trim(both 'a' from 'aAa')", "A", "VARCHAR(3) NOT NULL");
    tester.checkString(
        "trim(leading 'a' from 'aAa')",
        "Aa",
        "VARCHAR(3) NOT NULL");
    tester.checkString(
        "trim(trailing 'a' from 'aAa')",
        "aA",
        "VARCHAR(3) NOT NULL");
    tester.checkNull("trim(cast(null as varchar(1)) from 'a')");
    tester.checkNull("trim('a' from cast(null as varchar(1)))");

    // SQL:2003 6.29.9: trim string must have length=1. Failure occurs
    // at runtime.
    //
    // TODO: Change message to "Invalid argument\(s\) for
    // 'TRIM' function".
    // The message should come from a resource file, and should still
    // have the SQL error code 22027.
    tester.checkFails(
        "trim('xy' from 'abcde')",
        "Trim error: trim character must be exactly 1 character",
        true);
    tester.checkFails(
        "trim('' from 'abcde')",
        "Trim error: trim character must be exactly 1 character",
        true);

    final SqlTester tester1 = tester.withConformance(SqlConformanceEnum.MYSQL_5);
    tester1.checkString(
        "trim(leading 'eh' from 'hehe__hehe')", "__hehe", "VARCHAR(10) NOT NULL");
    tester1.checkString(
        "trim(trailing 'eh' from 'hehe__hehe')", "hehe__", "VARCHAR(10) NOT NULL");
    tester1.checkString(
        "trim('eh' from 'hehe__hehe')", "__", "VARCHAR(10) NOT NULL");
  }

  @Test public void testRtrimFunc() {
    tester.setFor(SqlLibraryOperators.RTRIM);
    final SqlTester tester1 = oracleTester();
    tester1.checkString("rtrim(' aAa  ')", " aAa", "VARCHAR(6) NOT NULL");
    tester1.checkNull("rtrim(CAST(NULL AS VARCHAR(6)))");
  }

  @Test public void testLtrimFunc() {
    tester.setFor(SqlLibraryOperators.LTRIM);
    final SqlTester tester1 = oracleTester();
    tester1.checkString("ltrim(' aAa  ')", "aAa  ", "VARCHAR(6) NOT NULL");
    tester1.checkNull("ltrim(CAST(NULL AS VARCHAR(6)))");
  }

  @Test public void testGreatestFunc() {
    tester.setFor(SqlLibraryOperators.GREATEST);
    final SqlTester tester1 = oracleTester();
    tester1.checkString("greatest('on', 'earth')", "on   ", "CHAR(5) NOT NULL");
    tester1.checkString("greatest('show', 'on', 'earth')", "show ",
        "CHAR(5) NOT NULL");
    tester1.checkScalar("greatest(12, CAST(NULL AS INTEGER), 3)", null, "INTEGER");
    tester1.checkScalar("greatest(false, true)", true, "BOOLEAN NOT NULL");

    final SqlTester tester2 = oracleTester(SqlConformanceEnum.ORACLE_12);
    tester2.checkString("greatest('on', 'earth')", "on", "VARCHAR(5) NOT NULL");
    tester2.checkString("greatest('show', 'on', 'earth')", "show",
        "VARCHAR(5) NOT NULL");
  }

  @Test public void testLeastFunc() {
    tester.setFor(SqlLibraryOperators.LEAST);
    final SqlTester tester1 = oracleTester();
    tester1.checkString("least('on', 'earth')", "earth", "CHAR(5) NOT NULL");
    tester1.checkString("least('show', 'on', 'earth')", "earth",
        "CHAR(5) NOT NULL");
    tester1.checkScalar("least(12, CAST(NULL AS INTEGER), 3)", null, "INTEGER");
    tester1.checkScalar("least(false, true)", false, "BOOLEAN NOT NULL");

    final SqlTester tester2 = oracleTester(SqlConformanceEnum.ORACLE_12);
    tester2.checkString("least('on', 'earth')", "earth", "VARCHAR(5) NOT NULL");
    tester2.checkString("least('show', 'on', 'earth')", "earth",
        "VARCHAR(5) NOT NULL");
  }

  @Test public void testNvlFunc() {
    tester.setFor(SqlLibraryOperators.NVL);
    final SqlTester tester1 = oracleTester();
    tester1.checkScalar("nvl(1, 2)", "1", "INTEGER NOT NULL");
    tester1.checkFails("^nvl(1, true)^", "Parameters must be of the same type",
        false);
    tester1.checkScalar("nvl(true, false)", true, "BOOLEAN NOT NULL");
    tester1.checkScalar("nvl(false, true)", false, "BOOLEAN NOT NULL");
    tester1.checkString("nvl('abc', 'de')", "abc", "CHAR(3) NOT NULL");
    tester1.checkString("nvl('abc', 'defg')", "abc ", "CHAR(4) NOT NULL");
    tester1.checkString("nvl('abc', CAST(NULL AS VARCHAR(20)))", "abc",
        "VARCHAR(20) NOT NULL");
    tester1.checkString("nvl(CAST(NULL AS VARCHAR(20)), 'abc')", "abc",
        "VARCHAR(20) NOT NULL");
    tester1.checkNull(
        "nvl(CAST(NULL AS VARCHAR(6)), cast(NULL AS VARCHAR(4)))");

    final SqlTester tester2 = oracleTester(SqlConformanceEnum.ORACLE_12);
    tester2.checkString("nvl('abc', 'de')", "abc", "VARCHAR(3) NOT NULL");
    tester2.checkString("nvl('abc', 'defg')", "abc", "VARCHAR(4) NOT NULL");
    tester2.checkString("nvl('abc', CAST(NULL AS VARCHAR(20)))", "abc",
        "VARCHAR(20) NOT NULL");
    tester2.checkString("nvl(CAST(NULL AS VARCHAR(20)), 'abc')", "abc",
        "VARCHAR(20) NOT NULL");
    tester2.checkNull(
        "nvl(CAST(NULL AS VARCHAR(6)), cast(NULL AS VARCHAR(4)))");
  }

  @Test public void testDecodeFunc() {
    tester.setFor(SqlLibraryOperators.DECODE);
    final SqlTester tester1 = oracleTester();
    tester1.checkScalar("decode(0, 0, 'a', 1, 'b', 2, 'c')", "a", "CHAR(1)");
    tester1.checkScalar("decode(1, 0, 'a', 1, 'b', 2, 'c')", "b", "CHAR(1)");
    // if there are duplicates, take the first match
    tester1.checkScalar("decode(1, 0, 'a', 1, 'b', 1, 'z', 2, 'c')", "b",
        "CHAR(1)");
    // if there's no match, and no "else", return null
    tester1.checkScalar("decode(3, 0, 'a', 1, 'b', 2, 'c')", null, "CHAR(1)");
    // if there's no match, return the "else" value
    tester1.checkScalar("decode(3, 0, 'a', 1, 'b', 2, 'c', 'd')", "d",
        "CHAR(1) NOT NULL");
    tester1.checkScalar("decode(1, 0, 'a', 1, 'b', 2, 'c', 'd')", "b",
        "CHAR(1) NOT NULL");
    // nulls match
    tester1.checkScalar("decode(cast(null as integer), 0, 'a',\n"
        + " cast(null as integer), 'b', 2, 'c', 'd')", "b",
        "CHAR(1) NOT NULL");
  }

  @Test public void testWindow() {
    if (!enable) {
      return;
    }
    tester.check(
        "select sum(1) over (order by x) from (select 1 as x, 2 as y from (values (true)))",
        new SqlTests.StringTypeChecker("INTEGER"),
        "1",
        0);
  }

  @Test public void testElementFunc() {
    tester.setFor(
        SqlStdOperatorTable.ELEMENT,
        VM_FENNEL,
        VM_JAVA);
    tester.checkString(
        "element(multiset['abc'])",
        "abc",
        "CHAR(3) NOT NULL");
    tester.checkNull("element(multiset[cast(null as integer)])");
  }

  @Test public void testCardinalityFunc() {
    tester.setFor(
        SqlStdOperatorTable.CARDINALITY,
        VM_FENNEL,
        VM_JAVA);
    tester.checkScalarExact(
        "cardinality(multiset[cast(null as integer),2])", "2");

    if (!enable) {
      return;
    }

    // applied to array
    tester.checkScalarExact(
        "cardinality(array['foo', 'bar'])", "2");

    // applied to map
    tester.checkScalarExact(
        "cardinality(map['foo', 1, 'bar', 2])", "2");
  }

  @Test public void testMemberOfOperator() {
    tester.setFor(
        SqlStdOperatorTable.MEMBER_OF,
        VM_FENNEL,
        VM_JAVA);
    tester.checkBoolean("1 member of multiset[1]", Boolean.TRUE);
    tester.checkBoolean(
        "'2' member of multiset['1']",
         Boolean.FALSE);
    tester.checkBoolean(
        "cast(null as double) member of multiset[cast(null as double)]",
         Boolean.TRUE);
    tester.checkBoolean(
        "cast(null as double) member of multiset[1.1]",
         Boolean.FALSE);
    tester.checkBoolean(
        "1.1 member of multiset[cast(null as double)]", Boolean.FALSE);
  }

  @Test public void testMultisetUnionOperator() {
    tester.setFor(
        SqlStdOperatorTable.MULTISET_UNION_DISTINCT,
        VM_FENNEL,
        VM_JAVA);
    tester.checkBoolean("multiset[1,2] submultiset of (multiset[2] multiset union multiset[1])",
        Boolean.TRUE);
    tester.checkScalar("cardinality(multiset[1, 2, 3, 4, 2] "
            + "multiset union distinct multiset[1, 4, 5, 7, 8])",
        "7",
        "INTEGER NOT NULL");
    tester.checkScalar("cardinality(multiset[1, 2, 3, 4, 2] "
            + "multiset union distinct multiset[1, 4, 5, 7, 8])",
        "7",
        "INTEGER NOT NULL");
    tester.checkBoolean("(multiset[1, 2, 3, 4, 2] "
            + "multiset union distinct multiset[1, 4, 5, 7, 8]) "
            + "submultiset of multiset[1, 2, 3, 4, 5, 7, 8]",
        Boolean.TRUE);
    tester.checkBoolean("(multiset[1, 2, 3, 4, 2] "
            + "multiset union distinct multiset[1, 4, 5, 7, 8]) "
            + "submultiset of multiset[1, 2, 3, 4, 5, 7, 8]",
        Boolean.TRUE);
    tester.checkScalar("cardinality(multiset['a', 'b', 'c'] "
            + "multiset union distinct multiset['c', 'd', 'e'])",
        "5",
        "INTEGER NOT NULL");
    tester.checkScalar("cardinality(multiset['a', 'b', 'c'] "
            + "multiset union distinct multiset['c', 'd', 'e'])",
        "5",
        "INTEGER NOT NULL");
    tester.checkBoolean("(multiset['a', 'b', 'c'] "
            + "multiset union distinct multiset['c', 'd', 'e'])"
            + " submultiset of multiset['a', 'b', 'c', 'd', 'e']",
        Boolean.TRUE);
    tester.checkBoolean("(multiset['a', 'b', 'c'] "
            + "multiset union distinct multiset['c', 'd', 'e'])"
            + " submultiset of multiset['a', 'b', 'c', 'd', 'e']",
        Boolean.TRUE);
    tester.checkScalar(
        "multiset[cast(null as double)] multiset union multiset[cast(null as double)]",
        "[null, null]",
        "DOUBLE MULTISET NOT NULL");
    tester.checkScalar(
        "multiset[cast(null as boolean)] multiset union multiset[cast(null as boolean)]",
        "[null, null]",
        "BOOLEAN MULTISET NOT NULL");
  }

  @Test public void testMultisetUnionAllOperator() {
    tester.setFor(
        SqlStdOperatorTable.MULTISET_UNION,
        VM_FENNEL,
        VM_JAVA);
    tester.checkScalar("cardinality(multiset[1, 2, 3, 4, 2] "
            + "multiset union all multiset[1, 4, 5, 7, 8])",
        "10",
        "INTEGER NOT NULL");
    tester.checkBoolean("(multiset[1, 2, 3, 4, 2] "
            + "multiset union all multiset[1, 4, 5, 7, 8]) "
            + "submultiset of multiset[1, 2, 3, 4, 5, 7, 8]",
        Boolean.FALSE);
    tester.checkBoolean("(multiset[1, 2, 3, 4, 2] "
            + "multiset union all multiset[1, 4, 5, 7, 8]) "
            + "submultiset of multiset[1, 1, 2, 2, 3, 4, 4, 5, 7, 8]",
        Boolean.TRUE);
    tester.checkScalar("cardinality(multiset['a', 'b', 'c'] "
            + "multiset union all multiset['c', 'd', 'e'])",
        "6",
        "INTEGER NOT NULL");
    tester.checkBoolean("(multiset['a', 'b', 'c'] "
            + "multiset union all multiset['c', 'd', 'e']) "
            + "submultiset of multiset['a', 'b', 'c', 'd', 'e']",
        Boolean.FALSE);
    tester.checkBoolean("(multiset['a', 'b', 'c'] "
            + "multiset union distinct multiset['c', 'd', 'e']) "
            + "submultiset of multiset['a', 'b', 'c', 'd', 'e', 'c']",
        Boolean.TRUE);
    tester.checkScalar(
        "multiset[cast(null as double)] multiset union all multiset[cast(null as double)]",
        "[null, null]",
        "DOUBLE MULTISET NOT NULL");
    tester.checkScalar(
        "multiset[cast(null as boolean)] multiset union all multiset[cast(null as boolean)]",
        "[null, null]",
        "BOOLEAN MULTISET NOT NULL");
  }

  @Test public void testSubMultisetOfOperator() {
    tester.setFor(
        SqlStdOperatorTable.SUBMULTISET_OF,
        VM_FENNEL,
        VM_JAVA);
    tester.checkBoolean("multiset[2] submultiset of multiset[1]", Boolean.FALSE);
    tester.checkBoolean("multiset[1] submultiset of multiset[1]", Boolean.TRUE);
    tester.checkBoolean("multiset[1, 2] submultiset of multiset[1]", Boolean.FALSE);
    tester.checkBoolean("multiset[1] submultiset of multiset[1, 2]", Boolean.TRUE);
    tester.checkBoolean("multiset[1, 2] submultiset of multiset[1, 2]", Boolean.TRUE);
    tester.checkBoolean(
        "multiset['a', 'b'] submultiset of multiset['c', 'd', 's', 'a']", Boolean.FALSE);
    tester.checkBoolean(
        "multiset['a', 'd'] submultiset of multiset['c', 's', 'a', 'w', 'd']", Boolean.TRUE);
    tester.checkBoolean("multiset['q', 'a'] submultiset of multiset['a', 'q']", Boolean.TRUE);
  }

  @Test public void testNotSubMultisetOfOperator() {
    tester.setFor(
        SqlStdOperatorTable.NOT_SUBMULTISET_OF,
        VM_FENNEL,
        VM_JAVA);
    tester.checkBoolean("multiset[2] not submultiset of multiset[1]", Boolean.TRUE);
    tester.checkBoolean("multiset[1] not submultiset of multiset[1]", Boolean.FALSE);
    tester.checkBoolean("multiset[1, 2] not submultiset of multiset[1]", Boolean.TRUE);
    tester.checkBoolean("multiset[1] not submultiset of multiset[1, 2]", Boolean.FALSE);
    tester.checkBoolean("multiset[1, 2] not submultiset of multiset[1, 2]", Boolean.FALSE);
    tester.checkBoolean(
        "multiset['a', 'b'] not submultiset of multiset['c', 'd', 's', 'a']", Boolean.TRUE);
    tester.checkBoolean(
        "multiset['a', 'd'] not submultiset of multiset['c', 's', 'a', 'w', 'd']",
        Boolean.FALSE);
    tester.checkBoolean("multiset['q', 'a'] not submultiset of multiset['a', 'q']", Boolean.FALSE);
  }

  @Test public void testCollectFunc() {
    tester.setFor(SqlStdOperatorTable.COLLECT, VM_FENNEL, VM_JAVA);
    tester.checkFails("collect(^*^)", "Unknown identifier '\\*'", false);
    checkAggType(tester, "collect(1)", "INTEGER NOT NULL MULTISET NOT NULL");
    checkAggType(tester,
        "collect(1.2)", "DECIMAL(2, 1) NOT NULL MULTISET NOT NULL");
    checkAggType(tester,
        "collect(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL MULTISET NOT NULL");
    tester.checkFails("^collect()^",
        "Invalid number of arguments to function 'COLLECT'. Was expecting 1 arguments",
        false);
    tester.checkFails("^collect(1, 2)^",
        "Invalid number of arguments to function 'COLLECT'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    tester.checkAgg("collect(x)", values,
        Collections.singletonList("[0, 2, 2]"), (double) 0);
    tester.checkAgg("collect(x) within group(order by x desc)", values,
        Collections.singletonList("[2, 2, 0]"), (double) 0);
    Object result1 = -3;
    if (!enable) {
      return;
    }
    tester.checkAgg("collect(CASE x WHEN 0 THEN NULL ELSE -1 END)", values,
        result1, (double) 0);
    Object result = -1;
    tester.checkAgg("collect(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values, result, (double) 0);
    tester.checkAgg("collect(DISTINCT x)", values, 2, (double) 0);
  }

  @Test public void testListAggFunc() {
    tester.setFor(SqlStdOperatorTable.LISTAGG, VM_FENNEL, VM_JAVA);
    tester.checkFails("listagg(^*^)", "Unknown identifier '\\*'", false);
    checkAggType(tester, "listagg(12)", "VARCHAR NOT NULL");
    strictTester.checkFails("^listagg(12)^",
        "Cannot apply 'LISTAGG' to arguments of type .*'\n.*'", false);
    checkAggType(tester, "listagg(cast(12 as double))", "VARCHAR NOT NULL");
    strictTester.checkFails("^listagg(cast(12 as double))^",
        "Cannot apply 'LISTAGG' to arguments of type .*'\n.*'", false);
    tester.checkFails("^listagg()^",
        "Invalid number of arguments to function 'LISTAGG'. Was expecting 1 arguments",
        false);
    tester.checkFails("^listagg('1', '2', '3')^",
        "Invalid number of arguments to function 'LISTAGG'. Was expecting 1 arguments",
        false);
    checkAggType(tester, "listagg('test')", "CHAR(4) NOT NULL");
    checkAggType(tester, "listagg('test', ', ')", "CHAR(4) NOT NULL");
    final String[] values1 = {"'hello'", "CAST(null AS CHAR)", "'world'", "'!'"};
    tester.checkAgg("listagg(x)", values1, "hello,world,!", (double) 0);
    final String[] values2 = {"0", "1", "2", "3"};
    tester.checkAgg("listagg(cast(x as CHAR))", values2, "0,1,2,3", (double) 0);
  }

  @Test public void testFusionFunc() {
    tester.setFor(SqlStdOperatorTable.FUSION, VM_FENNEL, VM_JAVA);
  }

  @Test public void testYear() {
    tester.setFor(
        SqlStdOperatorTable.YEAR,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "year(date '2008-1-23')",
        "2008",
        "BIGINT NOT NULL");
    tester.checkNull("year(cast(null as date))");
  }

  @Test public void testQuarter() {
    tester.setFor(
        SqlStdOperatorTable.QUARTER,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "quarter(date '2008-1-23')",
        "1",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-2-23')",
        "1",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-3-23')",
        "1",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-4-23')",
        "2",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-5-23')",
        "2",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-6-23')",
        "2",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-7-23')",
        "3",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-8-23')",
        "3",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-9-23')",
        "3",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-10-23')",
        "4",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-11-23')",
        "4",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "quarter(date '2008-12-23')",
        "4",
        "BIGINT NOT NULL");
    tester.checkNull("quarter(cast(null as date))");
  }

  @Test public void testMonth() {
    tester.setFor(
        SqlStdOperatorTable.MONTH,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "month(date '2008-1-23')",
        "1",
        "BIGINT NOT NULL");
    tester.checkNull("month(cast(null as date))");
  }

  @Test public void testWeek() {
    tester.setFor(
        SqlStdOperatorTable.WEEK,
        VM_FENNEL,
        VM_JAVA);
    if (Bug.CALCITE_2539_FIXED) {
      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "week(date '2008-1-23')",
          "cannot translate call EXTRACT.*",
          true);
      tester.checkFails(
          "week(cast(null as date))",
          "cannot translate call EXTRACT.*",
          true);
    }
  }

  @Test public void testDayOfYear() {
    tester.setFor(
        SqlStdOperatorTable.DAYOFYEAR,
        VM_FENNEL,
        VM_JAVA);
    if (Bug.CALCITE_2539_FIXED) {
      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "dayofyear(date '2008-1-23')",
          "cannot translate call EXTRACT.*",
          true);
      tester.checkFails(
          "dayofyear(cast(null as date))",
          "cannot translate call EXTRACT.*",
          true);
    }
  }

  @Test public void testDayOfMonth() {
    tester.setFor(
        SqlStdOperatorTable.DAYOFMONTH,
        VM_FENNEL,
        VM_JAVA);
    tester.checkScalar(
        "dayofmonth(date '2008-1-23')",
        "23",
        "BIGINT NOT NULL");
    tester.checkNull("dayofmonth(cast(null as date))");
  }

  @Test public void testDayOfWeek() {
    tester.setFor(
        SqlStdOperatorTable.DAYOFWEEK,
        VM_FENNEL,
        VM_JAVA);
    if (Bug.CALCITE_2539_FIXED) {
      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "dayofweek(date '2008-1-23')",
          "cannot translate call EXTRACT.*",
          true);
      tester.checkFails("dayofweek(cast(null as date))",
          "cannot translate call EXTRACT.*",
          true);
    }
  }

  @Test public void testHour() {
    tester.setFor(
        SqlStdOperatorTable.HOUR,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "hour(timestamp '2008-1-23 12:34:56')",
        "12",
        "BIGINT NOT NULL");
    tester.checkNull("hour(cast(null as timestamp))");
  }

  @Test public void testMinute() {
    tester.setFor(
        SqlStdOperatorTable.MINUTE,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "minute(timestamp '2008-1-23 12:34:56')",
        "34",
        "BIGINT NOT NULL");
    tester.checkNull("minute(cast(null as timestamp))");
  }

  @Test public void testSecond() {
    tester.setFor(
        SqlStdOperatorTable.SECOND,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "second(timestamp '2008-1-23 12:34:56')",
        "56",
        "BIGINT NOT NULL");
    tester.checkNull("second(cast(null as timestamp))");
  }

  @Test public void testExtractIntervalYearMonth() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    if (TODO) {
      // Not supported, fails in type validation because the extract
      // unit is not YearMonth interval type.

      tester.checkScalar(
          "extract(epoch from interval '4-2' year to month)",
          // number of seconds elapsed since timestamp
          // '1970-01-01 00:00:00' + input interval
          "131328000",
          "BIGINT NOT NULL");

      tester.checkScalar(
          "extract(second from interval '4-2' year to month)",
          "0",
          "BIGINT NOT NULL");

      tester.checkScalar(
          "extract(millisecond from interval '4-2' year to month)",
          "0",
          "BIGINT NOT NULL");

      tester.checkScalar(
          "extract(microsecond from interval '4-2' year to month)",
          "0",
          "BIGINT NOT NULL");

      tester.checkScalar(
          "extract(nanosecond from interval '4-2' year to month)",
          "0",
          "BIGINT NOT NULL");

      tester.checkScalar(
          "extract(minute from interval '4-2' year to month)",
          "0",
          "BIGINT NOT NULL");

      tester.checkScalar(
          "extract(hour from interval '4-2' year to month)",
          "0",
          "BIGINT NOT NULL");

      tester.checkScalar(
          "extract(day from interval '4-2' year to month)",
          "0",
          "BIGINT NOT NULL");
    }

    // Postgres doesn't support DOW, ISODOW, DOY and WEEK on INTERVAL YEAR MONTH type.
    // SQL standard doesn't have extract units for DOW, ISODOW, DOY and WEEK.
    tester.checkFails("^extract(doy from interval '4-2' year to month)^",
        INVALID_EXTRACT_UNIT_VALIDATION_ERROR, false);
    tester.checkFails("^extract(dow from interval '4-2' year to month)^",
        INVALID_EXTRACT_UNIT_VALIDATION_ERROR, false);
    tester.checkFails("^extract(week from interval '4-2' year to month)^",
        INVALID_EXTRACT_UNIT_VALIDATION_ERROR, false);
    tester.checkFails("^extract(isodow from interval '4-2' year to month)^",
        INVALID_EXTRACT_UNIT_VALIDATION_ERROR, false);

    tester.checkScalar(
        "extract(month from interval '4-2' year to month)",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(quarter from interval '4-2' year to month)",
        "1",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(year from interval '4-2' year to month)",
        "4",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(decade from interval '426-3' year(3) to month)",
        "42",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(century from interval '426-3' year(3) to month)",
        "4",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(millennium from interval '2005-3' year(4) to month)",
        "2",
        "BIGINT NOT NULL");
  }

  @Test public void testExtractIntervalDayTime() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    if (TODO) {
      // Not implemented in operator test
      tester.checkScalar(
          "extract(epoch from interval '2 3:4:5.678' day to second)",
          // number of seconds elapsed since timestamp
          // '1970-01-01 00:00:00' + input interval
          "183845.678",
          "BIGINT NOT NULL");
    }

    tester.checkScalar(
        "extract(millisecond from interval '2 3:4:5.678' day to second)",
        "5678",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(microsecond from interval '2 3:4:5.678' day to second)",
        "5678000",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(nanosecond from interval '2 3:4:5.678' day to second)",
        "5678000000",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(second from interval '2 3:4:5.678' day to second)",
        "5",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from interval '2 3:4:5.678' day to second)",
        "4",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(hour from interval '2 3:4:5.678' day to second)",
        "3",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(day from interval '2 3:4:5.678' day to second)",
        "2",
        "BIGINT NOT NULL");

    // Postgres doesn't support DOW, ISODOW, DOY and WEEK on INTERVAL DAY TIME type.
    // SQL standard doesn't have extract units for DOW, ISODOW, DOY and WEEK.
    if (Bug.CALCITE_2539_FIXED) {
      tester.checkFails("extract(doy from interval '2 3:4:5.678' day to second)",
          INVALID_EXTRACT_UNIT_CONVERTLET_ERROR, true);
      tester.checkFails("extract(dow from interval '2 3:4:5.678' day to second)",
          INVALID_EXTRACT_UNIT_CONVERTLET_ERROR, true);
      tester.checkFails("extract(week from interval '2 3:4:5.678' day to second)",
          INVALID_EXTRACT_UNIT_CONVERTLET_ERROR, true);
      tester.checkFails("extract(isodow from interval '2 3:4:5.678' day to second)",
          INVALID_EXTRACT_UNIT_CONVERTLET_ERROR, true);
    }

    tester.checkFails(
        "^extract(month from interval '2 3:4:5.678' day to second)^",
        "(?s)Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<INTERVAL "
            + "MONTH> FROM <INTERVAL DAY TO SECOND>\\)'\\. Supported "
            + "form\\(s\\):.*",
        false);

    tester.checkFails(
        "^extract(quarter from interval '2 3:4:5.678' day to second)^",
        "(?s)Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<INTERVAL "
            + "QUARTER> FROM <INTERVAL DAY TO SECOND>\\)'\\. Supported "
            + "form\\(s\\):.*",
        false);

    tester.checkFails(
        "^extract(year from interval '2 3:4:5.678' day to second)^",
        "(?s)Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<INTERVAL "
            + "YEAR> FROM <INTERVAL DAY TO SECOND>\\)'\\. Supported "
            + "form\\(s\\):.*",
        false);

    tester.checkFails(
        "^extract(isoyear from interval '2 3:4:5.678' day to second)^",
        "(?s)Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<INTERVAL "
            + "ISOYEAR> FROM <INTERVAL DAY TO SECOND>\\)'\\. Supported "
            + "form\\(s\\):.*",
        false);

    tester.checkFails(
        "^extract(century from interval '2 3:4:5.678' day to second)^",
        "(?s)Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<INTERVAL "
            + "CENTURY> FROM <INTERVAL DAY TO SECOND>\\)'\\. Supported "
            + "form\\(s\\):.*",
        false);
  }

  @Test public void testExtractDate() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "extract(epoch from date '2008-2-23')",
        "1203724800", // number of seconds elapsed since timestamp
        // '1970-01-01 00:00:00' for given date
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(second from date '2008-2-23')",
        "0",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(millisecond from date '2008-2-23')",
        "0",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(microsecond from date '2008-2-23')",
        "0",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(nanosecond from date '2008-2-23')",
        "0",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from date '9999-2-23')",
        "0",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from date '0001-1-1')",
        "0",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from date '2008-2-23')",
        "0",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(hour from date '2008-2-23')",
        "0",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(day from date '2008-2-23')",
        "23",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(month from date '2008-2-23')",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(quarter from date '2008-4-23')",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(year from date '2008-2-23')",
        "2008",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(isoyear from date '2008-2-23')",
        "2008",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(doy from date '2008-2-23')",
        "54",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(dow from date '2008-2-23')",
        "7",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(dow from date '2008-2-24')",
        "1",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(isodow from date '2008-2-23')",
        "6",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(isodow from date '2008-2-24')",
        "7",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(week from date '2008-2-23')",
        "8",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(week from timestamp '2008-2-23 01:23:45')",
        "8",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(week from cast(null as date))",
        null,
        "BIGINT");

    tester.checkScalar(
        "extract(decade from date '2008-2-23')",
        "200",
        "BIGINT NOT NULL");

    tester.checkScalar("extract(century from date '2008-2-23')",
        "21", "BIGINT NOT NULL");
    tester.checkScalar("extract(century from date '2001-01-01')",
        "21", "BIGINT NOT NULL");
    tester.checkScalar("extract(century from date '2000-12-31')",
        "20", "BIGINT NOT NULL");
    tester.checkScalar("extract(century from date '1852-06-07')",
        "19", "BIGINT NOT NULL");
    tester.checkScalar("extract(century from date '0001-02-01')",
        "1", "BIGINT NOT NULL");

    tester.checkScalar("extract(millennium from date '2000-2-23')",
        "2", "BIGINT NOT NULL");
    tester.checkScalar("extract(millennium from date '1969-2-23')",
        "2", "BIGINT NOT NULL");
    tester.checkScalar("extract(millennium from date '2000-12-31')",
        "2", "BIGINT NOT NULL");
    tester.checkScalar("extract(millennium from date '2001-01-01')",
        "3", "BIGINT NOT NULL");
  }

  @Test public void testExtractTimestamp() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "extract(epoch from timestamp '2008-2-23 12:34:56')",
        "1203770096", // number of seconds elapsed since timestamp
        // '1970-01-01 00:00:00' for given date
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(second from timestamp '2008-2-23 12:34:56')",
        "56",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(millisecond from timestamp '2008-2-23 12:34:56')",
        "56000",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(microsecond from timestamp '2008-2-23 12:34:56')",
        "56000000",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(nanosecond from timestamp '2008-2-23 12:34:56')",
        "56000000000",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from timestamp '2008-2-23 12:34:56')",
        "34",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(hour from timestamp '2008-2-23 12:34:56')",
        "12",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(day from timestamp '2008-2-23 12:34:56')",
        "23",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(month from timestamp '2008-2-23 12:34:56')",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(quarter from timestamp '2008-7-23 12:34:56')",
        "3",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(year from timestamp '2008-2-23 12:34:56')",
        "2008",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(isoyear from timestamp '2008-2-23 12:34:56')",
        "2008",
        "BIGINT NOT NULL");

    if (Bug.CALCITE_2539_FIXED) {
      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "extract(doy from timestamp '2008-2-23 12:34:56')",
          "cannot translate call EXTRACT.*",
          true);

      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "extract(dow from timestamp '2008-2-23 12:34:56')",
          "cannot translate call EXTRACT.*",
          true);

      // TODO: Not implemented in operator test execution code
      tester.checkFails(
          "extract(week from timestamp '2008-2-23 12:34:56')",
          "cannot translate call EXTRACT.*",
          true);
    }

    tester.checkScalar(
        "extract(decade from timestamp '2008-2-23 12:34:56')",
        "200",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(century from timestamp '2008-2-23 12:34:56')",
        "21",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(century from timestamp '2001-01-01 12:34:56')",
        "21",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(century from timestamp '2000-12-31 12:34:56')",
        "20",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(millennium from timestamp '2008-2-23 12:34:56')",
        "3",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(millennium from timestamp '2000-2-23 12:34:56')",
        "2",
        "BIGINT NOT NULL");
  }

  @Test public void testExtractFunc() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "extract(day from interval '2 3:4:5.678' day to second)",
        "2",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(day from interval '23456 3:4:5.678' day(5) to second)",
        "23456",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(hour from interval '2 3:4:5.678' day to second)",
        "3",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(minute from interval '2 3:4:5.678' day to second)",
        "4",
        "BIGINT NOT NULL");

    // TODO: Seconds should include precision
    tester.checkScalar(
        "extract(second from interval '2 3:4:5.678' day to second)",
        "5",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(millisecond from interval '2 3:4:5.678' day to second)",
        "5678",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(microsecond from interval '2 3:4:5.678' day to second)",
        "5678000",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(nanosecond from interval '2 3:4:5.678' day to second)",
        "5678000000",
        "BIGINT NOT NULL");

    tester.checkNull(
        "extract(month from cast(null as interval year))");
  }

  @Test public void testExtractFuncFromDateTime() {
    tester.setFor(
        SqlStdOperatorTable.EXTRACT,
        VM_FENNEL,
        VM_JAVA);

    tester.checkScalar(
        "extract(year from date '2008-2-23')",
        "2008",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(isoyear from date '2008-2-23')",
        "2008",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(month from date '2008-2-23')",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(month from timestamp '2008-2-23 12:34:56')",
        "2",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from timestamp '2008-2-23 12:34:56')",
        "34",
        "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(minute from time '12:23:34')",
        "23",
        "BIGINT NOT NULL");

    tester.checkNull(
        "extract(month from cast(null as timestamp))");

    tester.checkNull(
        "extract(month from cast(null as date))");

    tester.checkNull(
        "extract(second from cast(null as time))");

    tester.checkNull(
        "extract(millisecond from cast(null as time))");

    tester.checkNull(
        "extract(microsecond from cast(null as time))");

    tester.checkNull(
        "extract(nanosecond from cast(null as time))");
  }

  @Test public void testExtractWithDatesBeforeUnixEpoch() {

    tester.checkScalar(
            "extract(millisecond from TIMESTAMP '1969-12-31 21:13:17.357')",
            "17357",
            "BIGINT NOT NULL");

    tester.checkScalar(
        "extract(year from TIMESTAMP '1970-01-01 00:00:00')",
        "1970",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(year from TIMESTAMP '1969-12-31 10:13:17')",
        "1969",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(quarter from TIMESTAMP '1969-12-31 08:13:17')",
        "4",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(quarter from TIMESTAMP '1969-5-31 21:13:17')",
        "2",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(month from TIMESTAMP '1969-12-31 00:13:17')",
        "12",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(day from TIMESTAMP '1969-12-31 12:13:17')",
        "31",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(week from TIMESTAMP '1969-2-23 01:23:45')",
        "8",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(doy from TIMESTAMP '1969-12-31 21:13:17.357')",
        "365",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(dow from TIMESTAMP '1969-12-31 01:13:17.357')",
        "4",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(decade from TIMESTAMP '1969-12-31 21:13:17.357')",
        "196",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(century from TIMESTAMP '1969-12-31 21:13:17.357')",
        "20",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(hour from TIMESTAMP '1969-12-31 21:13:17.357')",
        "21",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(minute from TIMESTAMP '1969-12-31 21:13:17.357')",
        "13",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(second from TIMESTAMP '1969-12-31 21:13:17.357')",
        "17",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(millisecond from TIMESTAMP '1969-12-31 21:13:17.357')",
        "17357",
        "BIGINT NOT NULL");
    tester.checkScalar(
        "extract(microsecond from TIMESTAMP '1969-12-31 21:13:17.357')",
        "17357000",
        "BIGINT NOT NULL");
  }

  @Test public void testArrayValueConstructor() {
    tester.setFor(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR);
    tester.checkScalar(
        "Array['foo', 'bar']",
        "[foo, bar]",
        "CHAR(3) NOT NULL ARRAY NOT NULL");

    // empty array is illegal per SQL spec. presumably because one can't
    // infer type
    tester.checkFails(
        "^Array[]^", "Require at least 1 argument", false);
  }

  @Test public void testItemOp() {
    tester.setFor(SqlStdOperatorTable.ITEM);
    tester.checkScalar("ARRAY ['foo', 'bar'][1]", "foo", "CHAR(3)");
    tester.checkScalar("ARRAY ['foo', 'bar'][0]", null, "CHAR(3)");
    tester.checkScalar("ARRAY ['foo', 'bar'][2]", "bar", "CHAR(3)");
    tester.checkScalar("ARRAY ['foo', 'bar'][3]", null, "CHAR(3)");
    tester.checkNull(
        "ARRAY ['foo', 'bar'][1 + CAST(NULL AS INTEGER)]");
    tester.checkFails(
        "^ARRAY ['foo', 'bar']['baz']^",
        "Cannot apply 'ITEM' to arguments of type 'ITEM\\(<CHAR\\(3\\) ARRAY>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): <ARRAY>\\[<INTEGER>\\]\n"
            + "<MAP>\\[<VALUE>\\]",
        false);

    // Array of INTEGER NOT NULL is interesting because we might be tempted
    // to represent the result as Java "int".
    tester.checkScalar("ARRAY [2, 4, 6][2]", "4", "INTEGER");
    tester.checkScalar("ARRAY [2, 4, 6][4]", null, "INTEGER");

    // Map item
    tester.checkScalarExact(
        "map['foo', 3, 'bar', 7]['bar']", "INTEGER", "7");
    tester.checkScalarExact(
        "map['foo', CAST(NULL AS INTEGER), 'bar', 7]['bar']", "INTEGER",
        "7");
    tester.checkScalarExact(
        "map['foo', CAST(NULL AS INTEGER), 'bar', 7]['baz']",
        "INTEGER",
        null);
    tester.checkColumnType(
        "select cast(null as any)['x'] from (values(1))",
        "ANY");
  }

  @Test public void testMapValueConstructor() {
    tester.setFor(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, VM_JAVA);

    tester.checkFails(
        "^Map[]^", "Map requires at least 2 arguments", false);

    tester.checkFails(
        "^Map[1, 'x', 2]^",
        "Map requires an even number of arguments",
        false);

    tester.checkFails(
        "^map[1, 1, 2, 'x']^", "Parameters must be of the same type",
        false);
    tester.checkScalarExact(
        "map['washington', 1, 'obama', 44]",
        "(CHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL",
        "{washington=1, obama=44}");

    final SqlTester tester2 =
        tester.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    tester2.checkScalarExact(
        "map['washington', 1, 'obama', 44]",
        "(VARCHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL",
        "{washington=1, obama=44}");
  }

  @Test public void testCeilFunc() {
    tester.setFor(SqlStdOperatorTable.CEIL, VM_FENNEL);
    tester.checkScalarApprox("ceil(10.1e0)", "DOUBLE NOT NULL", 11, 0);
    tester.checkScalarApprox("ceil(cast(-11.2e0 as real))", "REAL NOT NULL",
        -11, 0);
    tester.checkScalarExact("ceil(100)", "INTEGER NOT NULL", "100");
    tester.checkScalarExact(
        "ceil(1.3)", "DECIMAL(2, 0) NOT NULL", "2");
    tester.checkScalarExact(
        "ceil(-1.7)", "DECIMAL(2, 0) NOT NULL", "-1");
    tester.checkNull("ceiling(cast(null as decimal(2,0)))");
    tester.checkNull("ceiling(cast(null as double))");
  }

  @Test public void testCeilFuncInterval() {
    if (!enable) {
      return;
    }
    tester.checkScalar(
        "ceil(interval '3:4:5' hour to second)",
        "+4:00:00.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    tester.checkScalar(
        "ceil(interval '-6.3' second)",
        "-6.000000",
        "INTERVAL SECOND NOT NULL");
    tester.checkScalar(
        "ceil(interval '5-1' year to month)",
        "+6-00",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "ceil(interval '-5-1' year to month)",
        "-5-00",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkNull(
        "ceil(cast(null as interval year))");
  }

  @Test public void testFloorFunc() {
    tester.setFor(SqlStdOperatorTable.FLOOR, VM_FENNEL);
    tester.checkScalarApprox("floor(2.5e0)", "DOUBLE NOT NULL", 2, 0);
    tester.checkScalarApprox("floor(cast(-1.2e0 as real))", "REAL NOT NULL", -2,
        0);
    tester.checkScalarExact("floor(100)", "INTEGER NOT NULL", "100");
    tester.checkScalarExact(
        "floor(1.7)", "DECIMAL(2, 0) NOT NULL", "1");
    tester.checkScalarExact(
        "floor(-1.7)", "DECIMAL(2, 0) NOT NULL", "-2");
    tester.checkNull("floor(cast(null as decimal(2,0)))");
    tester.checkNull("floor(cast(null as real))");
  }

  @Test public void testFloorFuncDateTime() {
    strictTester.checkFails("^floor('12:34:56')^",
        "Cannot apply 'FLOOR' to arguments of type 'FLOOR\\(<CHAR\\(8\\)>\\)'\\. Supported form\\(s\\): 'FLOOR\\(<NUMERIC>\\)'\n"
            + "'FLOOR\\(<DATETIME_INTERVAL>\\)'\n"
            + "'FLOOR\\(<DATE> TO <TIME_UNIT>\\)'\n"
            + "'FLOOR\\(<TIME> TO <TIME_UNIT>\\)'\n"
            + "'FLOOR\\(<TIMESTAMP> TO <TIME_UNIT>\\)'",
        false);
    tester.checkType("floor('12:34:56')", "DECIMAL(19, 0) NOT NULL");
    tester.checkFails("^floor(time '12:34:56')^",
        "(?s)Cannot apply 'FLOOR' to arguments .*", false);
    tester.checkFails("^floor(123.45 to minute)^",
        "(?s)Cannot apply 'FLOOR' to arguments .*", false);
    tester.checkFails("^floor('abcde' to minute)^",
        "(?s)Cannot apply 'FLOOR' to arguments .*", false);
    tester.checkFails("^floor(timestamp '2015-02-19 12:34:56.78' to microsecond)^",
            "(?s)Encountered \"microsecond\" at .*", false);
    tester.checkFails("^floor(timestamp '2015-02-19 12:34:56.78' to nanosecond)^",
            "(?s)Encountered \"nanosecond\" at .*", false);
    tester.checkScalar(
        "floor(time '12:34:56' to minute)", "12:34:00", "TIME(0) NOT NULL");
    tester.checkScalar("floor(timestamp '2015-02-19 12:34:56.78' to second)",
        "2015-02-19 12:34:56", "TIMESTAMP(2) NOT NULL");
    tester.checkScalar("floor(timestamp '2015-02-19 12:34:56' to minute)",
        "2015-02-19 12:34:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("floor(timestamp '2015-02-19 12:34:56' to year)",
        "2015-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("floor(timestamp '2015-02-19 12:34:56' to month)",
        "2015-02-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkNull("floor(cast(null as timestamp) to month)");
  }

  @Test public void testCeilFuncDateTime() {
    strictTester.checkFails("^ceil('12:34:56')^",
        "Cannot apply 'CEIL' to arguments of type 'CEIL\\(<CHAR\\(8\\)>\\)'\\. Supported form\\(s\\): 'CEIL\\(<NUMERIC>\\)'\n"
            + "'CEIL\\(<DATETIME_INTERVAL>\\)'\n"
            + "'CEIL\\(<DATE> TO <TIME_UNIT>\\)'\n"
            + "'CEIL\\(<TIME> TO <TIME_UNIT>\\)'\n"
            + "'CEIL\\(<TIMESTAMP> TO <TIME_UNIT>\\)'",
        false);
    tester.checkType("ceil('12:34:56')", "DECIMAL(19, 0) NOT NULL");
    tester.checkFails("^ceil(time '12:34:56')^",
        "(?s)Cannot apply 'CEIL' to arguments .*", false);
    tester.checkFails("^ceil(123.45 to minute)^",
        "(?s)Cannot apply 'CEIL' to arguments .*", false);
    tester.checkFails("^ceil('abcde' to minute)^",
        "(?s)Cannot apply 'CEIL' to arguments .*", false);
    tester.checkFails("^ceil(timestamp '2015-02-19 12:34:56.78' to microsecond)^",
            "(?s)Encountered \"microsecond\" at .*", false);
    tester.checkFails("^ceil(timestamp '2015-02-19 12:34:56.78' to nanosecond)^",
            "(?s)Encountered \"nanosecond\" at .*", false);
    tester.checkScalar("ceil(time '12:34:56' to minute)",
        "12:35:00", "TIME(0) NOT NULL");
    tester.checkScalar("ceil(time '12:59:56' to minute)",
        "13:00:00", "TIME(0) NOT NULL");
    tester.checkScalar("ceil(timestamp '2015-02-19 12:34:56.78' to second)",
        "2015-02-19 12:34:57", "TIMESTAMP(2) NOT NULL");
    tester.checkScalar("ceil(timestamp '2015-02-19 12:34:56.00' to second)",
        "2015-02-19 12:34:56", "TIMESTAMP(2) NOT NULL");
    tester.checkScalar("ceil(timestamp '2015-02-19 12:34:56' to minute)",
        "2015-02-19 12:35:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("ceil(timestamp '2015-02-19 12:34:56' to year)",
        "2016-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("ceil(timestamp '2015-02-19 12:34:56' to month)",
        "2015-03-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkNull("ceil(cast(null as timestamp) to month)");

    // ceiling alias
    tester.checkScalar("ceiling(timestamp '2015-02-19 12:34:56' to month)",
        "2015-03-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkNull("ceiling(cast(null as timestamp) to month)");
  }

  @Test public void testFloorFuncInterval() {
    if (!enable) {
      return;
    }
    tester.checkScalar(
        "floor(interval '3:4:5' hour to second)",
        "+3:00:00.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    tester.checkScalar(
        "floor(interval '-6.3' second)",
        "-7.000000",
        "INTERVAL SECOND NOT NULL");
    tester.checkScalar(
        "floor(interval '5-1' year to month)",
        "+5-00",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "floor(interval '-5-1' year to month)",
        "-6-00",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "floor(interval '-6.3' second to second)",
        "-7.000000",
        "INTERVAL SECOND NOT NULL");
    tester.checkScalar(
        "floor(interval '6-3' minute to second to minute)",
        "-7-0",
        "INTERVAL MINUTE TO SECOND NOT NULL");
    tester.checkScalar(
        "floor(interval '6-3' hour to minute to hour)",
        "7-0",
        "INTERVAL HOUR TO MINUTE NOT NULL");
    tester.checkScalar(
        "floor(interval '6 3' day to hour to day)",
        "7 00",
        "INTERVAL DAY TO HOUR NOT NULL");
    tester.checkScalar(
        "floor(interval '102-7' year to month to month)",
        "102-07",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "floor(interval '102-7' year to month to quarter)",
        "102-10",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "floor(interval '102-1' year to month to century)",
        "201",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkScalar(
        "floor(interval '1004-1' year to month to millennium)",
        "2001-00",
        "INTERVAL YEAR TO MONTH NOT NULL");
    tester.checkNull(
        "floor(cast(null as interval year))");
  }

  @Test public void testTimestampAdd() {
    tester.setFor(SqlStdOperatorTable.TIMESTAMP_ADD);
    tester.checkScalar(
        "timestampadd(MICROSECOND, 2000000, timestamp '2016-02-24 12:42:25')",
        "2016-02-24 12:42:27",
        "TIMESTAMP(3) NOT NULL");
    tester.checkScalar(
        "timestampadd(SQL_TSI_SECOND, 2, timestamp '2016-02-24 12:42:25')",
        "2016-02-24 12:42:27",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "timestampadd(NANOSECOND, 3000000000, timestamp '2016-02-24 12:42:25')",
        "2016-02-24 12:42:28",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "timestampadd(SQL_TSI_FRAC_SECOND, 2000000000, timestamp '2016-02-24 12:42:25')",
        "2016-02-24 12:42:27",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "timestampadd(MINUTE, 2, timestamp '2016-02-24 12:42:25')",
        "2016-02-24 12:44:25",
        "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "timestampadd(HOUR, -2000, timestamp '2016-02-24 12:42:25')",
        "2015-12-03 04:42:25",
        "TIMESTAMP(0) NOT NULL");
    tester.checkNull("timestampadd(HOUR, CAST(NULL AS INTEGER),"
        + " timestamp '2016-02-24 12:42:25')");
    tester.checkNull(
        "timestampadd(HOUR, -200, CAST(NULL AS TIMESTAMP))");
    tester.checkScalar(
        "timestampadd(MONTH, 3, timestamp '2016-02-24 12:42:25')",
        "2016-05-24 12:42:25", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar(
        "timestampadd(MONTH, 3, cast(null as timestamp))",
        null, "TIMESTAMP(0)");

    // TIMESTAMPADD with DATE; returns a TIMESTAMP value for sub-day intervals.
    tester.checkScalar("timestampadd(MONTH, 1, date '2016-06-15')",
        "2016-07-15", "DATE NOT NULL");
    tester.checkScalar("timestampadd(DAY, 1, date '2016-06-15')",
        "2016-06-16", "DATE NOT NULL");
    tester.checkScalar("timestampadd(HOUR, -1, date '2016-06-15')",
        "2016-06-14 23:00:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("timestampadd(MINUTE, 1, date '2016-06-15')",
        "2016-06-15 00:01:00", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("timestampadd(SQL_TSI_SECOND, -1, date '2016-06-15')",
        "2016-06-14 23:59:59", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("timestampadd(SECOND, 1, date '2016-06-15')",
        "2016-06-15 00:00:01", "TIMESTAMP(0) NOT NULL");
    tester.checkScalar("timestampadd(SECOND, 1, cast(null as date))",
        null, "TIMESTAMP(0)");
    tester.checkScalar("timestampadd(DAY, 1, cast(null as date))",
        null, "DATE");

    // Round to the last day of previous month
    tester.checkScalar("timestampadd(MONTH, 1, date '2016-05-31')",
        "2016-06-30", "DATE NOT NULL");
    tester.checkScalar("timestampadd(MONTH, 5, date '2016-01-31')",
        "2016-06-30", "DATE NOT NULL");
    tester.checkScalar("timestampadd(MONTH, -1, date '2016-03-31')",
        "2016-02-29", "DATE NOT NULL");

    // TIMESTAMPADD with time; returns a time value.The interval is positive.
    tester.checkScalar("timestampadd(SECOND, 1, time '23:59:59')",
        "00:00:00", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(MINUTE, 1, time '00:00:00')",
        "00:01:00", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(MINUTE, 1, time '23:59:59')",
        "00:00:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(HOUR, 1, time '23:59:59')",
        "00:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(DAY, 15, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(WEEK, 3, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(MONTH, 6, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(QUARTER, 1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(YEAR, 10, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    // TIMESTAMPADD with time; returns a time value .The interval is negative.
    tester.checkScalar("timestampadd(SECOND, -1, time '00:00:00')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(MINUTE, -1, time '00:00:00')",
        "23:59:00", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(HOUR, -1, time '00:00:00')",
        "23:00:00", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(DAY, -1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(WEEK, -1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(MONTH, -1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(QUARTER, -1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
    tester.checkScalar("timestampadd(YEAR, -1, time '23:59:59')",
        "23:59:59", "TIME(0) NOT NULL");
  }

  @Test public void testTimestampAddFractionalSeconds() {
    tester.setFor(SqlStdOperatorTable.TIMESTAMP_ADD);
    tester.checkType(
        "timestampadd(SQL_TSI_FRAC_SECOND, 2, timestamp '2016-02-24 12:42:25.000000')",
        // "2016-02-24 12:42:25.000002",
        "TIMESTAMP(3) NOT NULL");

    // The following test would correctly return "TIMESTAMP(6) NOT NULL" if max
    // precision were 6 or higher
    assumeTrue(tester.getValidator().getTypeFactory().getTypeSystem()
        .getMaxPrecision(SqlTypeName.TIMESTAMP) == 3);
    tester.checkType(
        "timestampadd(MICROSECOND, 2, timestamp '2016-02-24 12:42:25.000000')",
        // "2016-02-24 12:42:25.000002",
        "TIMESTAMP(3) NOT NULL");
  }

  @Test public void testTimestampDiff() {
    tester.setFor(SqlStdOperatorTable.TIMESTAMP_DIFF);
    tester.checkScalar("timestampdiff(HOUR, "
        + "timestamp '2016-02-24 12:42:25', "
        + "timestamp '2016-02-24 15:42:25')",
        "3", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(MICROSECOND, "
        + "timestamp '2016-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:20')",
        "-5000000", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(SQL_TSI_FRAC_SECOND, "
        + "timestamp '2016-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:20')",
        "-5000000000", "BIGINT NOT NULL");
    tester.checkScalar("timestampdiff(NANOSECOND, "
        + "timestamp '2016-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:20')",
        "-5000000000", "BIGINT NOT NULL");
    tester.checkScalar("timestampdiff(YEAR, "
        + "timestamp '2014-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:25')",
        "2", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(WEEK, "
        + "timestamp '2014-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:25')",
        "104", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(WEEK, "
        + "timestamp '2014-02-19 12:42:25', "
        + "timestamp '2016-02-24 12:42:25')",
        "105", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(MONTH, "
        + "timestamp '2014-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:25')",
        "24", "INTEGER NOT NULL");
    tester.checkScalar("timestampdiff(QUARTER, "
        + "timestamp '2014-02-24 12:42:25', "
        + "timestamp '2016-02-24 12:42:25')",
        "8", "INTEGER NOT NULL");
    tester.checkFails("timestampdiff(CENTURY, "
        + "timestamp '2014-02-24 12:42:25', "
        + "timestamp '2614-02-24 12:42:25')",
        "(?s)Encountered \"CENTURY\" at .*", false);
    tester.checkScalar("timestampdiff(QUARTER, "
        + "timestamp '2014-02-24 12:42:25', "
        + "cast(null as timestamp))",
        null, "INTEGER");
    tester.checkScalar("timestampdiff(QUARTER, "
        + "cast(null as timestamp), "
        + "timestamp '2014-02-24 12:42:25')",
        null, "INTEGER");

    // timestampdiff with date
    tester.checkScalar(
        "timestampdiff(MONTH, date '2016-03-15', date '2016-06-14')",
        "2",
        "INTEGER NOT NULL");
    tester.checkScalar(
        "timestampdiff(DAY, date '2016-06-15', date '2016-06-14')",
        "-1",
        "INTEGER NOT NULL");
    tester.checkScalar(
        "timestampdiff(HOUR, date '2016-06-15', date '2016-06-14')",
        "-24",
        "INTEGER NOT NULL");
    tester.checkScalar(
        "timestampdiff(MINUTE, date '2016-06-15',  date '2016-06-15')",
        "0",
        "INTEGER NOT NULL");
    tester.checkScalar(
        "timestampdiff(SECOND, cast(null as date), date '2016-06-15')",
        null,
        "INTEGER");
    tester.checkScalar(
        "timestampdiff(DAY, date '2016-06-15', cast(null as date))",
        null,
        "INTEGER");
  }

  @Test public void testDenseRankFunc() {
    tester.setFor(
        SqlStdOperatorTable.DENSE_RANK, VM_FENNEL, VM_JAVA);
  }

  @Test public void testPercentRankFunc() {
    tester.setFor(
        SqlStdOperatorTable.PERCENT_RANK,
        VM_FENNEL,
        VM_JAVA);
  }

  @Test public void testRankFunc() {
    tester.setFor(SqlStdOperatorTable.RANK, VM_FENNEL, VM_JAVA);
  }

  @Test public void testCumeDistFunc() {
    tester.setFor(
        SqlStdOperatorTable.CUME_DIST,
        VM_FENNEL,
        VM_JAVA);
  }

  @Test public void testRowNumberFunc() {
    tester.setFor(
        SqlStdOperatorTable.ROW_NUMBER,
        VM_FENNEL,
        VM_JAVA);
  }

  @Test public void testCountFunc() {
    tester.setFor(SqlStdOperatorTable.COUNT, VM_EXPAND);
    tester.checkType("count(*)", "BIGINT NOT NULL");
    tester.checkType("count('name')", "BIGINT NOT NULL");
    tester.checkType("count(1)", "BIGINT NOT NULL");
    tester.checkType("count(1.2)", "BIGINT NOT NULL");
    tester.checkType("COUNT(DISTINCT 'x')", "BIGINT NOT NULL");
    tester.checkFails(
        "^COUNT()^",
        "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments",
        false);
    tester.checkType("count(1, 2)", "BIGINT NOT NULL");
    tester.checkType("count(1, 2, 'x', 'y')", "BIGINT NOT NULL");
    final String[] values = {"0", "CAST(null AS INTEGER)", "1", "0"};
    tester.checkAgg(
        "COUNT(x)",
        values,
        3,
        (double) 0);
    tester.checkAgg(
        "COUNT(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        2,
        (double) 0);
    tester.checkAgg(
        "COUNT(DISTINCT x)",
        values,
        2,
        (double) 0);

    // string values -- note that empty string is not null
    final String[] stringValues = {
        "'a'", "CAST(NULL AS VARCHAR(1))", "''"
    };
    tester.checkAgg("COUNT(*)", stringValues, 3, (double) 0);
    tester.checkAgg("COUNT(x)", stringValues, 2, (double) 0);
    tester.checkAgg("COUNT(DISTINCT x)", stringValues, 2, (double) 0);
    tester.checkAgg("COUNT(DISTINCT 123)", stringValues, 1, (double) 0);
  }

  @Test public void testApproxCountDistinctFunc() {
    tester.setFor(SqlStdOperatorTable.COUNT, VM_EXPAND);
    tester.checkFails("approx_count_distinct(^*^)", "Unknown identifier '\\*'",
        false);
    tester.checkType("approx_count_distinct('name')", "BIGINT NOT NULL");
    tester.checkType("approx_count_distinct(1)", "BIGINT NOT NULL");
    tester.checkType("approx_count_distinct(1.2)", "BIGINT NOT NULL");
    tester.checkType("APPROX_COUNT_DISTINCT(DISTINCT 'x')", "BIGINT NOT NULL");
    tester.checkFails("^APPROX_COUNT_DISTINCT()^",
        "Invalid number of arguments to function 'APPROX_COUNT_DISTINCT'. "
            + "Was expecting 1 arguments",
        false);
    tester.checkType("approx_count_distinct(1, 2)", "BIGINT NOT NULL");
    tester.checkType("approx_count_distinct(1, 2, 'x', 'y')",
        "BIGINT NOT NULL");
    final String[] values = {"0", "CAST(null AS INTEGER)", "1", "0"};
    // currently APPROX_COUNT_DISTINCT(x) returns the same as COUNT(DISTINCT x)
    tester.checkAgg(
        "APPROX_COUNT_DISTINCT(x)",
        values,
        2,
        (double) 0);
    tester.checkAgg(
        "APPROX_COUNT_DISTINCT(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        1,
        (double) 0);
    // DISTINCT keyword is allowed but has no effect
    tester.checkAgg(
        "APPROX_COUNT_DISTINCT(DISTINCT x)",
        values,
        2,
        (double) 0);

    // string values -- note that empty string is not null
    final String[] stringValues = {
        "'a'", "CAST(NULL AS VARCHAR(1))", "''"
    };
    tester.checkAgg("APPROX_COUNT_DISTINCT(x)", stringValues, 2, (double) 0);
    tester.checkAgg("APPROX_COUNT_DISTINCT(DISTINCT x)", stringValues, 2,
        (double) 0);
    tester.checkAgg("APPROX_COUNT_DISTINCT(DISTINCT 123)", stringValues, 1,
        (double) 0);
  }

  @Test public void testSumFunc() {
    tester.setFor(SqlStdOperatorTable.SUM, VM_EXPAND);
    tester.checkFails(
        "sum(^*^)", "Unknown identifier '\\*'", false);
    strictTester.checkFails(
        "^sum('name')^",
        "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<CHAR\\(4\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("sum('name')", "DECIMAL(19, 19)");
    checkAggType(tester, "sum(1)", "INTEGER NOT NULL");
    checkAggType(tester, "sum(1.2)", "DECIMAL(2, 1) NOT NULL");
    checkAggType(tester, "sum(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    tester.checkFails(
        "^sum()^",
        "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^sum(1, 2)^",
        "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
        false);
    strictTester.checkFails(
        "^sum(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'SUM' to arguments of type 'SUM\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'SUM\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("sum(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    tester.checkAgg("sum(x)", values, 4, (double) 0);
    Object result1 = -3;
    if (!enable) {
      return;
    }
    tester.checkAgg("sum(CASE x WHEN 0 THEN NULL ELSE -1 END)", values, result1,
        (double) 0);
    Object result = -1;
    tester.checkAgg("sum(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)", values,
        result, (double) 0);
    tester.checkAgg("sum(DISTINCT x)", values, 2, (double) 0);
  }

  /** Very similar to {@code tester.checkType}, but generates inside a SELECT
   * with a non-empty GROUP BY. Aggregate functions may be nullable if executed
   * in a SELECT with an empty GROUP BY.
   *
   * <p>Viz: {@code SELECT sum(1) FROM emp} has type "INTEGER",
   * {@code SELECT sum(1) FROM emp GROUP BY deptno} has type "INTEGER NOT NULL",
   */
  protected void checkAggType(SqlTester tester, String expr, String type) {
    tester.checkColumnType(AbstractSqlTester.buildQueryAgg(expr), type);
  }

  @Test public void testAvgFunc() {
    tester.setFor(SqlStdOperatorTable.AVG, VM_EXPAND);
    tester.checkFails(
        "avg(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^avg(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'AVG' to arguments of type 'AVG\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'AVG\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("avg(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("AVG(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "AVG(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    checkAggType(tester, "avg(1)", "INTEGER NOT NULL");
    checkAggType(tester, "avg(1.2)", "DECIMAL(2, 1) NOT NULL");
    checkAggType(tester, "avg(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    if (!enable) {
      return;
    }
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    tester.checkAgg("AVG(x)", values, 2d, 0d);
    tester.checkAgg("AVG(DISTINCT x)", values, 1.5d, 0d);
    Object result = -1;
    tester.checkAgg("avg(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)", values,
        result, 0d);
  }

  @Test public void testCovarPopFunc() {
    tester.setFor(SqlStdOperatorTable.COVAR_POP, VM_EXPAND);
    tester.checkFails("covar_pop(^*^)", "Unknown identifier '\\*'", false);
    strictTester.checkFails(
        "^covar_pop(cast(null as varchar(2)),cast(null as varchar(2)))^",
        "(?s)Cannot apply 'COVAR_POP' to arguments of type 'COVAR_POP\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'COVAR_POP\\(<NUMERIC>, <NUMERIC>\\)'.*",
        false);
    tester.checkType("covar_pop(cast(null as varchar(2)),cast(null as varchar(2)))",
        "DECIMAL(19, 19)");
    tester.checkType("covar_pop(CAST(NULL AS INTEGER),CAST(NULL AS INTEGER))",
        "INTEGER");
    checkAggType(tester, "covar_pop(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!enable) {
      return;
    }
    // with zero values
    tester.checkAgg("covar_pop(x)", new String[]{}, null, 0d);
  }

  @Test public void testCovarSampFunc() {
    tester.setFor(SqlStdOperatorTable.COVAR_SAMP, VM_EXPAND);
    tester.checkFails(
        "covar_samp(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^covar_samp(cast(null as varchar(2)),cast(null as varchar(2)))^",
        "(?s)Cannot apply 'COVAR_SAMP' to arguments of type 'COVAR_SAMP\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'COVAR_SAMP\\(<NUMERIC>, <NUMERIC>\\)'.*",
        false);
    tester.checkType("covar_samp(cast(null as varchar(2)),cast(null as varchar(2)))",
        "DECIMAL(19, 19)");
    tester.checkType("covar_samp(CAST(NULL AS INTEGER),CAST(NULL AS INTEGER))",
        "INTEGER");
    checkAggType(tester, "covar_samp(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!enable) {
      return;
    }
    // with zero values
    tester.checkAgg("covar_samp(x)", new String[]{}, null, 0d);
  }

  @Test public void testRegrSxxFunc() {
    tester.setFor(SqlStdOperatorTable.REGR_SXX, VM_EXPAND);
    tester.checkFails(
        "regr_sxx(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^regr_sxx(cast(null as varchar(2)), cast(null as varchar(2)))^",
        "(?s)Cannot apply 'REGR_SXX' to arguments of type 'REGR_SXX\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'REGR_SXX\\(<NUMERIC>, <NUMERIC>\\)'.*",
        false);
    tester.checkType("regr_sxx(cast(null as varchar(2)), cast(null as varchar(2)))",
        "DECIMAL(19, 19)");
    tester.checkType("regr_sxx(CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))",
        "INTEGER");
    checkAggType(tester, "regr_sxx(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!enable) {
      return;
    }
    // with zero values
    tester.checkAgg("regr_sxx(x)", new String[]{}, null, 0d);
  }

  @Test public void testRegrSyyFunc() {
    tester.setFor(SqlStdOperatorTable.REGR_SYY, VM_EXPAND);
    tester.checkFails(
        "regr_syy(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^regr_syy(cast(null as varchar(2)), cast(null as varchar(2)))^",
        "(?s)Cannot apply 'REGR_SYY' to arguments of type 'REGR_SYY\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'REGR_SYY\\(<NUMERIC>, <NUMERIC>\\)'.*",
        false);
    tester.checkType("regr_syy(cast(null as varchar(2)), cast(null as varchar(2)))",
        "DECIMAL(19, 19)");
    tester.checkType("regr_syy(CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))",
        "INTEGER");
    checkAggType(tester, "regr_syy(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!enable) {
      return;
    }
    // with zero values
    tester.checkAgg("regr_syy(x)", new String[]{}, null, 0d);
  }

  @Test public void testStddevPopFunc() {
    tester.setFor(SqlStdOperatorTable.STDDEV_POP, VM_EXPAND);
    tester.checkFails("stddev_pop(^*^)", "Unknown identifier '\\*'", false);
    strictTester.checkFails("^stddev_pop(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'STDDEV_POP' to arguments of type 'STDDEV_POP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'STDDEV_POP\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("stddev_pop(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("stddev_pop(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "stddev_pop(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (enable) {
      // verified on Oracle 10g
      tester.checkAgg("stddev_pop(x)", values, 1.414213562373095d,
          0.000000000000001d);
      // Oracle does not allow distinct
      tester.checkAgg("stddev_pop(DISTINCT x)", values, 1.5d, 0d);
      tester.checkAgg("stddev_pop(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
          values, 0, 0d);
    }
    // with one value
    tester.checkAgg("stddev_pop(x)", new String[]{"5"}, 0, 0d);
    // with zero values
    tester.checkAgg("stddev_pop(x)", new String[]{}, null, 0d);
  }

  @Test public void testStddevSampFunc() {
    tester.setFor(SqlStdOperatorTable.STDDEV_SAMP, VM_EXPAND);
    tester.checkFails(
        "stddev_samp(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^stddev_samp(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'STDDEV_SAMP' to arguments of type 'STDDEV_SAMP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'STDDEV_SAMP\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("stddev_samp(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("stddev_samp(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "stddev_samp(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (enable) {
      // verified on Oracle 10g
      tester.checkAgg("stddev_samp(x)", values, 1.732050807568877d,
          0.000000000000001d);
      // Oracle does not allow distinct
      tester.checkAgg("stddev_samp(DISTINCT x)", values, 2.121320343559642d,
          0.000000000000001d);
      tester.checkAgg(
          "stddev_samp(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
          values,
          null,
          0d);
    }
    // with one value
    tester.checkAgg(
        "stddev_samp(x)",
        new String[]{"5"},
        null,
        0d);
    // with zero values
    tester.checkAgg(
        "stddev_samp(x)",
        new String[]{},
        null,
        0d);
  }

  @Test public void testStddevFunc() {
    tester.setFor(SqlStdOperatorTable.STDDEV, VM_EXPAND);
    tester.checkFails(
        "stddev(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^stddev(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'STDDEV' to arguments of type 'STDDEV\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'STDDEV\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("stddev(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("stddev(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "stddev(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    // with one value
    tester.checkAgg(
        "stddev(x)",
        new String[]{"5"},
        null,
        0d);
    // with zero values
    tester.checkAgg(
        "stddev(x)",
        new String[]{},
        null,
        0d);
  }

  @Test public void testVarPopFunc() {
    tester.setFor(SqlStdOperatorTable.VAR_POP, VM_EXPAND);
    tester.checkFails(
        "var_pop(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^var_pop(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'VAR_POP' to arguments of type 'VAR_POP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'VAR_POP\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("var_pop(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("var_pop(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "var_pop(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "var_pop(x)",
        values,
        2d, // verified on Oracle 10g
        0d);
    tester.checkAgg(
        "var_pop(DISTINCT x)", // Oracle does not allow distinct
        values,
        2.25d,
        0.0001d);
    tester.checkAgg(
        "var_pop(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        0,
        0d);
    // with one value
    tester.checkAgg(
        "var_pop(x)",
        new String[]{"5"},
        0,
        0d);
    // with zero values
    tester.checkAgg(
        "var_pop(x)",
        new String[]{},
        null,
        0d);
  }

  @Test public void testVarSampFunc() {
    tester.setFor(SqlStdOperatorTable.VAR_SAMP, VM_EXPAND);
    tester.checkFails(
        "var_samp(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^var_samp(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'VAR_SAMP' to arguments of type 'VAR_SAMP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'VAR_SAMP\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("var_samp(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("var_samp(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "var_samp(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "var_samp(x)", values, 3d, // verified on Oracle 10g
        0d);
    tester.checkAgg(
        "var_samp(DISTINCT x)", // Oracle does not allow distinct
        values,
        4.5d,
        0.0001d);
    tester.checkAgg(
        "var_samp(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        null,
        0d);
    // with one value
    tester.checkAgg(
        "var_samp(x)",
        new String[]{"5"},
        null,
        0d);
    // with zero values
    tester.checkAgg(
        "var_samp(x)",
        new String[]{},
        null,
        0d);
  }

  @Test public void testVarFunc() {
    tester.setFor(SqlStdOperatorTable.VARIANCE, VM_EXPAND);
    tester.checkFails(
        "variance(^*^)",
        "Unknown identifier '\\*'",
        false);
    strictTester.checkFails(
        "^variance(cast(null as varchar(2)))^",
        "(?s)Cannot apply 'VARIANCE' to arguments of type 'VARIANCE\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): 'VARIANCE\\(<NUMERIC>\\)'.*",
        false);
    tester.checkType("variance(cast(null as varchar(2)))", "DECIMAL(19, 19)");
    tester.checkType("variance(CAST(NULL AS INTEGER))", "INTEGER");
    checkAggType(tester, "variance(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "variance(x)", values, 3d, // verified on Oracle 10g
        0d);
    tester.checkAgg(
        "variance(DISTINCT x)", // Oracle does not allow distinct
        values,
        4.5d,
        0.0001d);
    tester.checkAgg(
        "variance(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        null,
        0d);
    // with one value
    tester.checkAgg(
        "variance(x)",
        new String[]{"5"},
        null,
        0d);
    // with zero values
    tester.checkAgg(
        "variance(x)",
        new String[]{},
        null,
        0d);
  }

  @Test public void testMinFunc() {
    tester.setFor(SqlStdOperatorTable.MIN, VM_EXPAND);
    tester.checkFails(
        "min(^*^)",
        "Unknown identifier '\\*'",
        false);
    tester.checkType("min(1)", "INTEGER");
    tester.checkType("min(1.2)", "DECIMAL(2, 1)");
    tester.checkType("min(DISTINCT 1.5)", "DECIMAL(2, 1)");
    tester.checkFails(
        "^min()^",
        "Invalid number of arguments to function 'MIN'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^min(1, 2)^",
        "Invalid number of arguments to function 'MIN'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "min(x)",
        values,
        "0",
        0d);
    tester.checkAgg(
        "min(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "min(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "min(DISTINCT x)",
        values,
        "0",
        0d);
  }

  @Test public void testMaxFunc() {
    tester.setFor(SqlStdOperatorTable.MAX, VM_EXPAND);
    tester.checkFails(
        "max(^*^)",
        "Unknown identifier '\\*'",
        false);
    tester.checkType("max(1)", "INTEGER");
    tester.checkType("max(1.2)", "DECIMAL(2, 1)");
    tester.checkType("max(DISTINCT 1.5)", "DECIMAL(2, 1)");
    tester.checkFails(
        "^max()^",
        "Invalid number of arguments to function 'MAX'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^max(1, 2)^",
        "Invalid number of arguments to function 'MAX'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "max(x)",
        values,
        "2",
        0d);
    tester.checkAgg(
        "max(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "max(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "max(DISTINCT x)", values, "2", 0d);
  }

  @Test public void testLastValueFunc() {
    tester.setFor(SqlStdOperatorTable.LAST_VALUE, VM_EXPAND);
    final String[] values = {"0", "CAST(null AS INTEGER)", "3", "3"};
    if (!enable) {
      return;
    }
    tester.checkWinAgg("last_value(x)", values, "ROWS 3 PRECEDING", "INTEGER",
        Arrays.asList("3", "0"), 0d);
    final String[] values2 = {"1.6", "1.2"};
    tester.checkWinAgg(
        "last_value(x)",
        values2,
        "ROWS 3 PRECEDING",
        "DECIMAL(2, 1) NOT NULL",
        Arrays.asList("1.6", "1.2"),
        0d);
    final String[] values3 = {"'foo'", "'bar'", "'name'"};
    tester.checkWinAgg(
        "last_value(x)",
        values3,
        "ROWS 3 PRECEDING",
        "CHAR(4) NOT NULL",
        Arrays.asList("foo ", "bar ", "name"),
        0d);
  }

  @Test public void testFirstValueFunc() {
    tester.setFor(SqlStdOperatorTable.FIRST_VALUE, VM_EXPAND);
    final String[] values = {"0", "CAST(null AS INTEGER)", "3", "3"};
    if (!enable) {
      return;
    }
    tester.checkWinAgg("first_value(x)", values, "ROWS 3 PRECEDING", "INTEGER",
        Arrays.asList("0"), 0d);
    final String[] values2 = {"1.6", "1.2"};
    tester.checkWinAgg(
        "first_value(x)",
        values2,
        "ROWS 3 PRECEDING",
        "DECIMAL(2, 1) NOT NULL",
        Arrays.asList("1.6"),
        0d);
    final String[] values3 = {"'foo'", "'bar'", "'name'"};
    tester.checkWinAgg(
        "first_value(x)",
        values3,
        "ROWS 3 PRECEDING",
        "CHAR(4) NOT NULL",
        Arrays.asList("foo "),
        0d);
  }

  @Test public void testAnyValueFunc() {
    tester.setFor(SqlStdOperatorTable.ANY_VALUE, VM_EXPAND);
    tester.checkFails(
        "any_value(^*^)",
        "Unknown identifier '\\*'",
        false);
    tester.checkType("any_value(1)", "INTEGER");
    tester.checkType("any_value(1.2)", "DECIMAL(2, 1)");
    tester.checkType("any_value(DISTINCT 1.5)", "DECIMAL(2, 1)");
    tester.checkFails(
        "^any_value()^",
        "Invalid number of arguments to function 'ANY_VALUE'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^any_value(1, 2)^",
        "Invalid number of arguments to function 'ANY_VALUE'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    if (!enable) {
      return;
    }
    tester.checkAgg(
        "any_value(x)",
        values,
        "0",
        0d);
    tester.checkAgg(
        "any_value(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "any_value(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values,
        "-1",
        0d);
    tester.checkAgg(
        "any_value(DISTINCT x)",
        values,
        "0",
        0d);
  }

  @Test public void testBitAndFunc() {
    tester.setFor(SqlStdOperatorTable.BIT_AND, VM_FENNEL, VM_JAVA);
    tester.checkFails("bit_and(^*^)", "Unknown identifier '\\*'", false);
    tester.checkType("bit_and(1)", "INTEGER");
    tester.checkType("bit_and(CAST(2 AS TINYINT))", "TINYINT");
    tester.checkType("bit_and(CAST(2 AS SMALLINT))", "SMALLINT");
    tester.checkType("bit_and(distinct CAST(2 AS BIGINT))", "BIGINT");
    tester.checkFails("^bit_and(1.2)^",
        "Cannot apply 'BIT_AND' to arguments of type 'BIT_AND\\(<DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): 'BIT_AND\\(<INTEGER>\\)'",
        false);
    tester.checkFails(
        "^bit_and()^",
        "Invalid number of arguments to function 'BIT_AND'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^bit_and(1, 2)^",
        "Invalid number of arguments to function 'BIT_AND'. Was expecting 1 arguments",
        false);
    final String[] values = {"3", "2", "2"};
    tester.checkAgg("bit_and(x)", values, 2, 0);
  }

  @Test public void testBitOrFunc() {
    tester.setFor(SqlStdOperatorTable.BIT_OR, VM_FENNEL, VM_JAVA);
    tester.checkFails("bit_or(^*^)", "Unknown identifier '\\*'", false);
    tester.checkType("bit_or(1)", "INTEGER");
    tester.checkType("bit_or(CAST(2 AS TINYINT))", "TINYINT");
    tester.checkType("bit_or(CAST(2 AS SMALLINT))", "SMALLINT");
    tester.checkType("bit_or(distinct CAST(2 AS BIGINT))", "BIGINT");
    tester.checkFails("^bit_or(1.2)^",
        "Cannot apply 'BIT_OR' to arguments of type 'BIT_OR\\(<DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): 'BIT_OR\\(<INTEGER>\\)'",
        false);
    tester.checkFails(
        "^bit_or()^",
        "Invalid number of arguments to function 'BIT_OR'. Was expecting 1 arguments",
        false);
    tester.checkFails(
        "^bit_or(1, 2)^",
        "Invalid number of arguments to function 'BIT_OR'. Was expecting 1 arguments",
        false);
    final String[] values = {"1", "2", "2"};
    tester.checkAgg("bit_or(x)", values, 3, 0);
  }

  /**
   * Tests that CAST fails when given a value just outside the valid range for
   * that type. For example,
   *
   * <ul>
   * <li>CAST(-200 AS TINYINT) fails because the value is less than -128;
   * <li>CAST(1E-999 AS FLOAT) fails because the value underflows;
   * <li>CAST(123.4567891234567 AS FLOAT) fails because the value loses
   * precision.
   * </ul>
   */
  @Test public void testLiteralAtLimit() {
    tester.setFor(SqlStdOperatorTable.CAST);
    if (!enable) {
      return;
    }
    final List<RelDataType> types =
        SqlLimitsTest.getTypes(tester.getValidator().getTypeFactory());
    for (RelDataType type : types) {
      for (Object o : getValues((BasicSqlType) type, true)) {
        SqlLiteral literal =
            type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
        SqlString literalString =
            literal.toSqlString(AnsiSqlDialect.DEFAULT);
        final String expr =
            "CAST(" + literalString
                + " AS " + type + ")";
        try {
          tester.checkType(
              expr,
              type.getFullTypeString());

          if (type.getSqlTypeName() == SqlTypeName.BINARY) {
            // Casting a string/binary values may change the value.
            // For example, CAST(X'AB' AS BINARY(2)) yields
            // X'AB00'.
          } else {
            tester.checkScalar(
                expr + " = " + literalString,
                true,
                "BOOLEAN NOT NULL");
          }
        } catch (Error e) {
          System.out.println("Failed for expr=[" + expr + "]");
          throw e;
        } catch (RuntimeException e) {
          System.out.println("Failed for expr=[" + expr + "]");
          throw e;
        }
      }
    }
  }

  /**
   * Tests that CAST fails when given a value just outside the valid range for
   * that type. For example,
   *
   * <ul>
   * <li>CAST(-200 AS TINYINT) fails because the value is less than -128;
   * <li>CAST(1E-999 AS FLOAT) fails because the value underflows;
   * <li>CAST(123.4567891234567 AS FLOAT) fails because the value loses
   * precision.
   * </ul>
   */
  @Test public void testLiteralBeyondLimit() {
    tester.setFor(SqlStdOperatorTable.CAST);
    final List<RelDataType> types =
        SqlLimitsTest.getTypes(tester.getValidator().getTypeFactory());
    for (RelDataType type : types) {
      for (Object o : getValues((BasicSqlType) type, false)) {
        SqlLiteral literal =
            type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
        SqlString literalString =
            literal.toSqlString(AnsiSqlDialect.DEFAULT);

        if ((type.getSqlTypeName() == SqlTypeName.BIGINT)
            || ((type.getSqlTypeName() == SqlTypeName.DECIMAL)
            && (type.getPrecision() == 19))) {
          // Values which are too large to be literals fail at
          // validate time.
          tester.checkFails(
              "CAST(^" + literalString + "^ AS " + type + ")",
              "Numeric literal '.*' out of range",
              false);
        } else if (
            (type.getSqlTypeName() == SqlTypeName.CHAR)
                || (type.getSqlTypeName() == SqlTypeName.VARCHAR)
                || (type.getSqlTypeName() == SqlTypeName.BINARY)
                || (type.getSqlTypeName() == SqlTypeName.VARBINARY)) {
          // Casting overlarge string/binary values do not fail -
          // they are truncated. See testCastTruncates().
        } else {
          if (Bug.CALCITE_2539_FIXED) {
            // Value outside legal bound should fail at runtime (not
            // validate time).
            //
            // NOTE: Because Java and Fennel calcs give
            // different errors, the pattern hedges its bets.
            tester.checkFails(
                "CAST(" + literalString + " AS " + type + ")",
                "(?s).*(Overflow during calculation or cast\\.|Code=22003).*",
                true);
          }
        }
      }
    }
  }

  @Test public void testCastTruncates() {
    tester.setFor(SqlStdOperatorTable.CAST);
    tester.checkScalar("CAST('ABCD' AS CHAR(2))", "AB", "CHAR(2) NOT NULL");
    tester.checkScalar("CAST('ABCD' AS VARCHAR(2))", "AB",
        "VARCHAR(2) NOT NULL");
    tester.checkScalar("CAST('ABCD' AS VARCHAR)", "ABCD", "VARCHAR NOT NULL");
    tester.checkScalar("CAST(CAST('ABCD' AS VARCHAR) AS VARCHAR(3))", "ABC",
        "VARCHAR(3) NOT NULL");

    tester.checkScalar("CAST(x'ABCDEF12' AS BINARY(2))", "abcd",
        "BINARY(2) NOT NULL");
    tester.checkScalar("CAST(x'ABCDEF12' AS VARBINARY(2))", "abcd",
        "VARBINARY(2) NOT NULL");
    tester.checkScalar("CAST(x'ABCDEF12' AS VARBINARY)", "abcdef12",
        "VARBINARY NOT NULL");
    tester.checkScalar("CAST(CAST(x'ABCDEF12' AS VARBINARY) AS VARBINARY(3))",
        "abcdef", "VARBINARY(3) NOT NULL");

    if (!enable) {
      return;
    }
    tester.checkBoolean(
        "CAST(X'' AS BINARY(3)) = X'000000'",
        true);
    tester.checkBoolean("CAST(X'' AS BINARY(3)) = X''", false);
  }

  /** Test that calls all operators with all possible argument types, and for
   * each type, with a set of tricky values. */
  @Test public void testArgumentBounds() {
    if (!CalciteSystemProperty.TEST_SLOW.value()) {
      return;
    }
    final SqlValidatorImpl validator = (SqlValidatorImpl) tester.getValidator();
    final SqlValidatorScope scope = validator.getEmptyScope();
    final RelDataTypeFactory typeFactory = validator.getTypeFactory();
    final Builder builder = new Builder(typeFactory);
    builder.add0(SqlTypeName.BOOLEAN, true, false);
    builder.add0(SqlTypeName.TINYINT, 0, 1, -3, Byte.MAX_VALUE, Byte.MIN_VALUE);
    builder.add0(SqlTypeName.SMALLINT, 0, 1, -4, Short.MAX_VALUE,
        Short.MIN_VALUE);
    builder.add0(SqlTypeName.INTEGER, 0, 1, -2, Integer.MIN_VALUE,
        Integer.MAX_VALUE);
    builder.add0(SqlTypeName.BIGINT, 0, 1, -5, Integer.MAX_VALUE,
        Long.MAX_VALUE, Long.MIN_VALUE);
    builder.add1(SqlTypeName.VARCHAR, 11, "", " ", "hello world");
    builder.add1(SqlTypeName.CHAR, 5, "", "e", "hello");
    builder.add0(SqlTypeName.TIMESTAMP, 0L, DateTimeUtils.MILLIS_PER_DAY);
    for (SqlOperator op : SqlStdOperatorTable.instance().getOperatorList()) {
      switch (op.getKind()) {
      case TRIM: // can't handle the flag argument
      case EXISTS:
        continue;
      }
      switch (op.getSyntax()) {
      case SPECIAL:
        continue;
      }
      final SqlOperandTypeChecker typeChecker =
          op.getOperandTypeChecker();
      if (typeChecker == null) {
        continue;
      }
      final SqlOperandCountRange range =
          typeChecker.getOperandCountRange();
      for (int n = range.getMin(), max = range.getMax(); n <= max; n++) {
        final List<List<ValueType>> argValues =
            Collections.nCopies(n, builder.values);
        for (final List<ValueType> args : Linq4j.product(argValues)) {
          SqlNodeList nodeList = new SqlNodeList(SqlParserPos.ZERO);
          int nullCount = 0;
          for (ValueType arg : args) {
            if (arg.value == null) {
              ++nullCount;
            }
            nodeList.add(arg.node);
          }
          final SqlCall call = op.createCall(nodeList);
          final SqlCallBinding binding =
              new SqlCallBinding(validator, scope, call);
          if (!typeChecker.checkOperandTypes(binding, false)) {
            continue;
          }
          final SqlPrettyWriter writer =
              new SqlPrettyWriter(CalciteSqlDialect.DEFAULT);
          op.unparse(writer, call, 0, 0);
          final String s = writer.toSqlString().toString();
          if (s.startsWith("OVERLAY(")
              || s.contains(" / 0")
              || s.matches("MOD\\(.*, 0\\)")) {
            continue;
          }
          final Strong.Policy policy = Strong.policy(op.kind);
          try {
            if (nullCount > 0 && policy == Strong.Policy.ANY) {
              tester.checkNull(s);
            } else {
              final String query;
              if (op instanceof SqlAggFunction) {
                if (op.requiresOrder()) {
                  query = "SELECT " + s + " OVER () FROM (VALUES (1))";
                } else {
                  query = "SELECT " + s + " FROM (VALUES (1))";
                }
              } else {
                query = AbstractSqlTester.buildQuery(s);
              }
              tester.check(query, SqlTests.ANY_TYPE_CHECKER,
                  SqlTests.ANY_PARAMETER_CHECKER, result -> { });
            }
          } catch (Error e) {
            System.out.println(s + ": " + e.getMessage());
            throw e;
          } catch (Exception e) {
            System.out.println("Failed: " + s + ": " + e.getMessage());
          }
        }
      }
    }
  }

  private List<Object> getValues(BasicSqlType type, boolean inBound) {
    List<Object> values = new ArrayList<Object>();
    for (boolean sign : FALSE_TRUE) {
      for (SqlTypeName.Limit limit : SqlTypeName.Limit.values()) {
        Object o = type.getLimit(sign, limit, !inBound);
        if (o == null) {
          continue;
        }
        if (!values.contains(o)) {
          values.add(o);
        }
      }
    }
    return values;
  }

  // TODO: Test other stuff

  /**
   * Result checker that considers a test to have succeeded if it throws an
   * exception that matches one of a list of patterns.
   */
  private static class ExceptionResultChecker
      implements SqlTester.ResultChecker {
    private final Pattern[] patterns;

    ExceptionResultChecker(Pattern... patterns) {
      this.patterns = patterns;
    }

    public void checkResult(ResultSet result) throws Exception {
      Throwable thrown = null;
      try {
        result.next();
        fail("expected exception");
      } catch (SQLException e) {
        thrown = e;
      }
      final String stack = Throwables.getStackTraceAsString(thrown);
      for (Pattern pattern : patterns) {
        if (pattern.matcher(stack).matches()) {
          return;
        }
      }
      fail("Stack did not match any pattern; " + stack);
    }
  }

  /**
   * Result checker that considers a test to have succeeded if it returns a
   * particular value or throws an exception that matches one of a list of
   * patterns.
   *
   * <p>Sounds peculiar, but is necessary when eager and lazy behaviors are
   * both valid.
   */
  private static class ValueOrExceptionResultChecker
      implements SqlTester.ResultChecker {
    private final Object expected;
    private final Pattern[] patterns;

    ValueOrExceptionResultChecker(
        Object expected, Pattern... patterns) {
      this.expected = expected;
      this.patterns = patterns;
    }

    public void checkResult(ResultSet result) throws Exception {
      Throwable thrown = null;
      try {
        if (!result.next()) {
          // empty result is OK
          return;
        }
        final Object actual = result.getObject(1);
        assertEquals(expected, actual);
      } catch (SQLException e) {
        thrown = e;
      }
      if (thrown != null) {
        final String stack = Throwables.getStackTraceAsString(thrown);
        for (Pattern pattern : patterns) {
          if (pattern.matcher(stack).matches()) {
            return;
          }
        }
        fail("Stack did not match any pattern; " + stack);
      }
    }
  }

  public static SqlTester tester() {
    return new TesterImpl(SqlTestFactory.INSTANCE);
  }

  /**
   * Implementation of {@link org.apache.calcite.sql.test.SqlTester} based on a
   * JDBC connection.
   */
  protected static class TesterImpl extends SqlRuntimeTester {
    public TesterImpl(SqlTestFactory testFactory) {
      super(testFactory, UnaryOperator.identity());
    }

    @Override public void check(String query, TypeChecker typeChecker,
        ParameterChecker parameterChecker, ResultChecker resultChecker) {
      super.check(query, typeChecker, parameterChecker, resultChecker);
      //noinspection unchecked
      final CalciteAssert.ConnectionFactory connectionFactory =
          (CalciteAssert.ConnectionFactory)
              getFactory().get("connectionFactory");
      try (Connection connection = connectionFactory.createConnection();
           Statement statement = connection.createStatement()) {
        final ResultSet resultSet =
            statement.executeQuery(query);
        resultChecker.checkResult(resultSet);
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
      }
    }

    @Override protected SqlTester with(SqlTestFactory factory) {
      return new TesterImpl(factory);
    }
  }

  /** A type, a value, and its {@link SqlNode} representation. */
  static class ValueType {
    final RelDataType type;
    final Object value;
    final SqlNode node;

    ValueType(RelDataType type, Object value) {
      this.type = type;
      this.value = value;
      this.node = literal(type, value);
    }

    private SqlNode literal(RelDataType type, Object value) {
      if (value == null) {
        return SqlStdOperatorTable.CAST.createCall(
            SqlParserPos.ZERO,
            SqlLiteral.createNull(SqlParserPos.ZERO),
            SqlTypeUtil.convertTypeToSpec(type));
      }
      switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return SqlLiteral.createBoolean((Boolean) value, SqlParserPos.ZERO);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return SqlLiteral.createExactNumeric(
            value.toString(), SqlParserPos.ZERO);
      case CHAR:
      case VARCHAR:
        return SqlLiteral.createCharString(value.toString(), SqlParserPos.ZERO);
      case TIMESTAMP:
        TimestampString ts = TimestampString.fromMillisSinceEpoch((Long) value);
        return SqlLiteral.createTimestamp(ts, type.getPrecision(),
            SqlParserPos.ZERO);
      default:
        throw new AssertionError(type);
      }
    }
  }

  /** Builds lists of types and sample values. */
  static class Builder {
    final RelDataTypeFactory typeFactory;
    final List<RelDataType> types = new ArrayList<>();
    final List<ValueType> values = new ArrayList<>();

    Builder(RelDataTypeFactory typeFactory) {
      this.typeFactory = typeFactory;
    }

    public void add0(SqlTypeName typeName, Object... values) {
      add(typeFactory.createSqlType(typeName), values);
    }

    public void add1(SqlTypeName typeName, int precision, Object... values) {
      add(typeFactory.createSqlType(typeName, precision), values);
    }

    private void add(RelDataType type, Object[] values) {
      types.add(type);
      for (Object value : values) {
        this.values.add(new ValueType(type, value));
      }
      this.values.add(new ValueType(type, null));
    }
  }

  /** Runs an OVERLAPS test with a given set of literal values. */
  class OverlapChecker {
    final String[] values;

    OverlapChecker(String... values) {
      this.values = values;
    }

    public void isTrue(String s) {
      tester.checkBoolean(sub(s), Boolean.TRUE);
    }

    public void isFalse(String s) {
      tester.checkBoolean(sub(s), Boolean.FALSE);
    }

    private String sub(String s) {
      return s.replace("$0", values[0])
          .replace("$1", values[1])
          .replace("$2", values[2])
          .replace("$3", values[3]);
    }
  }
}

// End SqlOperatorBaseTest.java
