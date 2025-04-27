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

import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.avatica.util.DateTimeUtils;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.type.DelegatingTypeSystem;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.TimeFrameSet;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJdbcFunctionCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.fun.LibraryOperator;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.test.AbstractSqlTester;
import org.apache.calcite.sql.test.SqlOperatorFixture;
import org.apache.calcite.sql.test.SqlOperatorFixture.CastType;
import org.apache.calcite.sql.test.SqlOperatorFixture.VmName;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlTests;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.TryThreadLocal;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.apache.calcite.linq4j.tree.Expressions.list;
import static org.apache.calcite.rel.type.RelDataTypeImpl.NON_NULLABLE_SUFFIX;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PI;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.QUANTIFY_OPERATORS;
import static org.apache.calcite.sql.test.ResultCheckers.isDecimal;
import static org.apache.calcite.sql.test.ResultCheckers.isExactDateTime;
import static org.apache.calcite.sql.test.ResultCheckers.isExactTime;
import static org.apache.calcite.sql.test.ResultCheckers.isExactly;
import static org.apache.calcite.sql.test.ResultCheckers.isNullValue;
import static org.apache.calcite.sql.test.ResultCheckers.isSet;
import static org.apache.calcite.sql.test.ResultCheckers.isSingle;
import static org.apache.calcite.sql.test.ResultCheckers.isWithin;
import static org.apache.calcite.sql.test.SqlOperatorFixture.BAD_DATETIME_MESSAGE;
import static org.apache.calcite.sql.test.SqlOperatorFixture.DECIMAL_OVERFLOW;
import static org.apache.calcite.sql.test.SqlOperatorFixture.DIVISION_BY_ZERO_MESSAGE;
import static org.apache.calcite.sql.test.SqlOperatorFixture.INTEGER_OVERFLOW;
import static org.apache.calcite.sql.test.SqlOperatorFixture.INVALID_ARGUMENTS_NUMBER;
import static org.apache.calcite.sql.test.SqlOperatorFixture.INVALID_ARGUMENTS_TYPE_VALIDATION_ERROR;
import static org.apache.calcite.sql.test.SqlOperatorFixture.INVALID_CHAR_MESSAGE;
import static org.apache.calcite.sql.test.SqlOperatorFixture.LITERAL_OUT_OF_RANGE_MESSAGE;
import static org.apache.calcite.sql.test.SqlOperatorFixture.LONG_OVERFLOW;
import static org.apache.calcite.sql.test.SqlOperatorFixture.OUT_OF_RANGE_MESSAGE;
import static org.apache.calcite.sql.test.SqlOperatorFixture.WRONG_FORMAT_MESSAGE;
import static org.apache.calcite.util.DateTimeStringUtils.getDateFormatter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import static java.lang.Double.parseDouble;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Contains unit tests for all operators. Each of the methods is named after an
 * operator.
 *
 * <p>To run, you also need an execution mechanism: parse, validate, and execute
 * expressions on the operators. This is left to a {@link SqlTester} object
 * which is obtained via the {@link #fixture()} method. The default tester
 * merely validates calls to operators, but {@code CalciteSqlOperatorTest}
 * uses a tester that executes calls and checks that results are valid.
 *
 * <p>Different implementations of {@link SqlTester} are possible, such as:
 *
 * <ul>
 * <li>Execute against a JDBC database;
 * <li>Parse and validate but do not evaluate expressions;
 * <li>Generate a SQL script;
 * <li>Analyze which operators are adequately tested.
 * </ul>
 *
 * <p>A typical method will be named after the operator it is testing (say
 * <code>testSubstringFunc</code>). It first calls
 * {@link SqlOperatorFixture#setFor(SqlOperator, VmName...)}
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
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SuppressWarnings("MethodCanBeStatic")
public class SqlOperatorTest {
  //~ Static fields/initializers ---------------------------------------------

  public static final TesterImpl TESTER = new TesterImpl();

  private static final Logger LOGGER =
      CalciteTrace.getTestTracer(SqlOperatorTest.class);

  public static final boolean TODO = false;

  /**
   * Regular expression for a SQL TIME(0/1) value.
   */
  public static final Pattern TIME_PATTERN =
      Pattern.compile(
          "[0-9][0-9]:[0-9][0-9]:[0-9][0-9](.[0-9])?");

  /**
   * Regular expression for a SQL TIMESTAMP(0/1) value.
   */
  public static final Pattern TIMESTAMP_PATTERN =
      Pattern.compile(
          "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] "
              + "[0-9][0-9]:[0-9][0-9]:[0-9][0-9](.[0-9])?");

  /**
   * Regular expression for a SQL DATE value.
   */
  public static final Pattern DATE_PATTERN =
      Pattern.compile(
          "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]");

  public static final List<String> MICROSECOND_VARIANTS =
      Arrays.asList("FRAC_SECOND", "MICROSECOND", "SQL_TSI_MICROSECOND");
  public static final List<String> NANOSECOND_VARIANTS =
      Arrays.asList("NANOSECOND", "SQL_TSI_FRAC_SECOND");
  public static final List<String> SECOND_VARIANTS =
      Arrays.asList("SECOND", "SQL_TSI_SECOND");
  public static final List<String> MINUTE_VARIANTS =
      Arrays.asList("MINUTE", "SQL_TSI_MINUTE");
  public static final List<String> HOUR_VARIANTS =
      Arrays.asList("HOUR", "SQL_TSI_HOUR");
  public static final List<String> DAY_VARIANTS =
      Arrays.asList("DAY", "SQL_TSI_DAY");
  public static final List<String> WEEK_VARIANTS =
      Arrays.asList("WEEK", "SQL_TSI_WEEK");
  public static final List<String> MONTH_VARIANTS =
      Arrays.asList("MONTH", "SQL_TSI_MONTH");
  public static final List<String> QUARTER_VARIANTS =
      Arrays.asList("QUARTER", "SQL_TSI_QUARTER");
  public static final List<String> YEAR_VARIANTS =
      Arrays.asList("YEAR", "SQL_TSI_YEAR");

  /** Minimum and maximum values for each exact and approximate numeric
   * type. */
  enum Numeric {
    TINYINT("TINYINT", Long.toString(Byte.MIN_VALUE),
        Long.toString(Byte.MIN_VALUE - 1),
        Long.toString(Byte.MAX_VALUE),
        Long.toString(Byte.MAX_VALUE + 1)),
    SMALLINT("SMALLINT", Long.toString(Short.MIN_VALUE),
        Long.toString(Short.MIN_VALUE - 1),
        Long.toString(Short.MAX_VALUE),
        Long.toString(Short.MAX_VALUE + 1)),
    INTEGER("INTEGER", Long.toString(Integer.MIN_VALUE),
        Long.toString((long) Integer.MIN_VALUE - 1),
        Long.toString(Integer.MAX_VALUE),
        Long.toString((long) Integer.MAX_VALUE + 1)),
    BIGINT("BIGINT", Long.toString(Long.MIN_VALUE),
        new BigDecimal(Long.MIN_VALUE).subtract(BigDecimal.ONE).toString(),
        Long.toString(Long.MAX_VALUE),
        new BigDecimal(Long.MAX_VALUE).add(BigDecimal.ONE).toString()),
    DECIMAL5_2("DECIMAL(5, 2)", "-999.99",
        "-1000.00", "999.99", "1000.00"),
    REAL("REAL", "1E-37", // or Float.toString(Float.MIN_VALUE)
        "1e-46", "3.4028234E38", // or Float.toString(Float.MAX_VALUE)
        "1e39"),
    FLOAT("FLOAT", "2E-307", // or Double.toString(Double.MIN_VALUE)
        "1e-324", "1.79769313486231E308", // or Double.toString(Double.MAX_VALUE)
        "-1e309"),
    DOUBLE("DOUBLE", "2E-307", // or Double.toString(Double.MIN_VALUE)
        "1e-324", "1.79769313486231E308", // or Double.toString(Double.MAX_VALUE)
        "1e309");

    private final String typeName;

    /** For Float and Double Java types, MIN_VALUE
     * is the smallest positive value, not the smallest negative value.
     * For REAL, FLOAT, DOUBLE, Win32 takes smaller values from
     * win32_values.h. */
    private final String minNumericString;
    private final String minOverflowNumericString;

    /** For REAL, FLOAT and DOUBLE SQL types (Flaot and Double Java types), we
     * use something slightly less than MAX_VALUE because round-tripping string
     * to approx to string doesn't preserve MAX_VALUE on win32. */
    private final String maxNumericString;
    private final String maxOverflowNumericString;

    Numeric(String typeName, String minNumericString,
        String minOverflowNumericString, String maxNumericString,
        String maxOverflowNumericString) {
      this.typeName = typeName;
      this.minNumericString = minNumericString;
      this.minOverflowNumericString = minOverflowNumericString;
      this.maxNumericString = maxNumericString;
      this.maxOverflowNumericString = maxOverflowNumericString;
    }

    /** Calls a consumer for each value. Similar effect to a {@code for}
     * loop, but the calling line number will show up in the call stack. */
    static void forEach(Consumer<Numeric> consumer) {
      consumer.accept(TINYINT);
      consumer.accept(SMALLINT);
      consumer.accept(INTEGER);
      consumer.accept(BIGINT);
      consumer.accept(DECIMAL5_2);
      consumer.accept(REAL);
      consumer.accept(FLOAT);
      consumer.accept(DOUBLE);
    }

    double maxNumericAsDouble() {
      return parseDouble(maxNumericString);
    }

    double minNumericAsDouble() {
      return parseDouble(minNumericString);
    }
  }

  private static final boolean[] FALSE_TRUE = {false, true};
  private static final VmName VM_JAVA = VmName.JAVA;
  private static final VmName VM_EXPAND = VmName.EXPAND;
  protected static final TimeZone UTC_TZ = TimeZone.getTimeZone("GMT");
  // time zone for the LOCAL_{DATE,TIME,TIMESTAMP} functions
  protected static final TimeZone LOCAL_TZ = TimeZone.getDefault();
  // time zone for the CURRENT{DATE,TIME,TIMESTAMP} functions
  protected static final TimeZone CURRENT_TZ = LOCAL_TZ;

  private static final Pattern INVALID_ARG_FOR_POWER =
      Pattern.compile("(?s).*Invalid argument\\(s\\) for 'POWER' function.*");

  private static final Pattern CODE_2201F =
      Pattern.compile("(?s).*could not calculate results for the following "
          + "row.*PC=5 Code=2201F.*");

  /** Function object that returns a string with 2 copies of each character.
   * For example, {@code DOUBLER.apply("xy")} returns {@code "xxyy"}. */
  private static final UnaryOperator<String> DOUBLER =
      new UnaryOperator<String>() {
        final Pattern pattern = Pattern.compile("(.)");

        @Override public String apply(String s) {
          return pattern.matcher(s).replaceAll("$1$1");
        }
      };

  /** Sub-classes should override to run tests in a different environment. */
  protected SqlOperatorFixture fixture() {
    return SqlOperatorFixtureImpl.DEFAULT;
  }

  //--- Tests -----------------------------------------------------------

  @Test void testSqlOperatorOverloading() {
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
      if (routines.size() == 2) {
        // Some arithmetic operators looks like they are overloaded,
        // e.g. PLUS and CHECKED_PLUS
        assertTrue(SqlKind.CHECKED_ARITHMETIC.contains(routines.get(0).kind)
                || SqlKind.CHECKED_ARITHMETIC.contains(routines.get(1).kind));
      } else {
        assertThat(routines, hasSize(1));
        assertThat(sqlOperator, equalTo(routines.get(0)));
      }
    }
  }

  @Test void testBetween() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.BETWEEN, VmName.EXPAND);
    f.checkBoolean("2 between 1 and 3", true);
    f.checkBoolean("2 between 3 and 2", false);
    f.checkBoolean("2 between symmetric 3 and 2", true);
    f.checkBoolean("3 between 1 and 3", true);
    f.checkBoolean("4 between 1 and 3", false);
    f.checkBoolean("1 between 4 and -3", false);
    f.checkBoolean("1 between -1 and -3", false);
    f.checkBoolean("1 between -1 and 3", true);
    f.checkBoolean("1 between 1 and 1", true);
    f.checkBoolean("1.5 between 1 and 3", true);
    f.checkBoolean("1.2 between 1.1 and 1.3", true);
    f.checkBoolean("1.5 between 2 and 3", false);
    f.checkBoolean("1.5 between 1.6 and 1.7", false);
    f.checkBoolean("1.2e1 between 1.1 and 1.3", false);
    f.checkBoolean("1.2e0 between 1.1 and 1.3", true);
    f.checkBoolean("1.5e0 between 2 and 3", false);
    f.checkBoolean("1.5e0 between 2e0 and 3e0", false);
    f.checkBoolean("1.5e1 between 1.6e1 and 1.7e1", false);
    f.checkBoolean("x'' between x'' and x''", true);
    f.checkNull("cast(null as integer) between -1 and 2");
    f.checkNull("1 between -1 and cast(null as integer)");
    f.checkNull("1 between cast(null as integer) and cast(null as integer)");
    f.checkNull("1 between cast(null as integer) and 1");
    f.checkBoolean("x'0A00015A' between x'0A000130' and x'0A0001B0'",
        true);
    f.checkBoolean("x'0A00015A' between x'0A0001A0' and x'0A0001B0'",
        false);
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-3522">
   * Sql validator limits decimal literals to 64 bits</a>. */
  @Test void testLargeLiterals() {
    // Some of these literals were too large to be accepted previously, but
    // now are legal as decimal literals.
    SqlOperatorFixture f = fixture();
    f.checkCastFails("9223372036854775808", "INTEGER",
        OUT_OF_RANGE_MESSAGE, true, SqlOperatorFixture.CastType.CAST);
    f.checkCastFails("9223372036854775808.1", "INTEGER",
        "Numeric literal.*out of range", false, SqlOperatorFixture.CastType.CAST);
    f.checkCastFails("223372036854775808", "INTEGER",
        OUT_OF_RANGE_MESSAGE, true, SqlOperatorFixture.CastType.CAST);
    f.checkCastFails("9223372036854775808", "BIGINT",
        "Overflow", true, SqlOperatorFixture.CastType.CAST);
    f.checkCastFails("'" + Numeric.TINYINT.maxOverflowNumericString + "'",
        "TINYINT", OUT_OF_RANGE_MESSAGE, true, SqlOperatorFixture.CastType.CAST);
    String largePrecision = "1234567891011.0";
    String largeScale = "1.01234567891011";
    f.checkScalarExact(largePrecision, "DECIMAL(14, 1) NOT NULL", largePrecision);
    f.checkScalarExact(largeScale, "DECIMAL(15, 14) NOT NULL", largeScale);

    // Check that the type system can reject large decimal literals
    SqlOperatorFixture f0 = f.withFactory(tf ->
            tf.withTypeSystem(typeSystem ->
                new DelegatingTypeSystem(typeSystem) {
                  @Override public int getMaxNumericPrecision() {
                    return getMaxPrecision(SqlTypeName.DECIMAL);
                  }

                  @Override public int getMaxPrecision(SqlTypeName typeName) {
                    switch (typeName) {
                    case DECIMAL:
                      return 10;
                    default:
                      return super.getMaxPrecision(typeName);
                    }
                  }

                  @Override public int getMaxNumericScale() {
                    return getMaxScale(SqlTypeName.DECIMAL);
                  }

                  @Override public int getMaxScale(SqlTypeName typeName) {
                    switch (typeName) {
                    case DECIMAL:
                      return 10;
                    default:
                      return super.getMaxScale(typeName);
                    }
                  }
                }));
    f0.checkFails("^" + largePrecision + "^", OUT_OF_RANGE_MESSAGE, false);
    f0.checkFails("^" + largeScale + "^", OUT_OF_RANGE_MESSAGE, false);
  }

  @Test void testNotBetween() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.NOT_BETWEEN, VM_EXPAND);
    f.checkBoolean("2 not between 1 and 3", false);
    f.checkBoolean("3 not between 1 and 3", false);
    f.checkBoolean("4 not between 1 and 3", true);
    f.checkBoolean("1.2e0 not between 1.1 and 1.3", false);
    f.checkBoolean("1.2e1 not between 1.1 and 1.3", true);
    f.checkBoolean("1.5e0 not between 2 and 3", true);
    f.checkBoolean("1.5e0 not between 2e0 and 3e0", true);
    f.checkBoolean("x'0A00015A' not between x'0A000130' and x'0A0001B0'",
        false);
    f.checkBoolean("x'0A00015A' not between x'0A0001A0' and x'0A0001B0'",
        true);
    f.checkNull("cast(null as integer) not between -1 and 2");
    f.checkNull("1 not between -1 and cast(null as integer)");
    f.checkNull("1 not between cast(null as integer) and cast(null as integer)");
    f.checkNull("1 not between cast(null as integer) and 1");
  }

  /** Generates parameters to test both regular and safe cast. */
  @SuppressWarnings("unused")
  private Stream<Arguments> safeParameters() {
    SqlOperatorFixture f = fixture();
    SqlOperatorFixture f2 =
        SqlOperatorFixtures.safeCastWrapper(f.withLibrary(SqlLibrary.BIG_QUERY), "SAFE_CAST");
    SqlOperatorFixture f3 =
        SqlOperatorFixtures.safeCastWrapper(f.withLibrary(SqlLibrary.MSSQL), "TRY_CAST");
    return Stream.of(
        () -> new Object[] {CastType.CAST, f},
        () -> new Object[] {CastType.SAFE_CAST, f2},
        () -> new Object[] {CastType.TRY_CAST, f3});
  }

  /** Tests that CAST, SAFE_CAST and TRY_CAST are basically equivalent but SAFE_CAST is
   * only available in BigQuery library and TRY_CAST is only available in MSSQL library. */
  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCast(CastType castType, SqlOperatorFixture f) {
    // SAFE_CAST is available in BigQuery library but not by default.
    // TRY_CAST is available in MSSQL library but not by default.
    final SqlOperatorFixture f0 = fixture();
    if (castType != CastType.CAST) {
      f0.checkFails("^" + castType.name() + "(12 + 3 as varchar(10))^",
          "No match found for function signature " + castType.name().toUpperCase(Locale.ROOT)
              + "\\(<NUMERIC>, <CHARACTER>\\)", false);
    }

    f.checkScalar(castType.name() + "(12 + 3 as varchar(10))", "15", "VARCHAR(10) NOT NULL");
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastToString(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);
    f.checkCastToString("cast(cast('abc' as char(4)) as varchar(6))", null,
        "abc ", castType);

    // integer
    f.checkCastToString("123", "CHAR(3)", "123", castType);

    f.checkCastToString("0", "CHAR", "0", castType);
    f.checkCastToString("-123", "CHAR(4)", "-123", castType);

    // decimal
    f.checkCastToString("123.4", "CHAR(5)", "123.4", castType);
    f.checkCastToString("-0.0", "CHAR(2)", ".0", castType);
    f.checkCastToString("-123.4", "CHAR(6)", "-123.4", castType);

    f.checkString("cast(1.29 as varchar(10))", "1.29", "VARCHAR(10) NOT NULL");
    f.checkString("cast(.48 as varchar(10))", ".48", "VARCHAR(10) NOT NULL");
    f.checkString("cast(2.523 as char(2))", "2.", "CHAR(2) NOT NULL");

    f.checkString("cast(-0.29 as varchar(10))",
        "-.29", "VARCHAR(10) NOT NULL");
    f.checkString("cast(-1.29 as varchar(10))",
        "-1.29", "VARCHAR(10) NOT NULL");

    // approximate
    f.checkCastToString("1.23E45", "CHAR(7)", "1.23E45", castType);
    f.checkCastToString("CAST(0 AS DOUBLE)", "CHAR(3)", "0E0", castType);
    f.checkCastToString("-1.20e-07", "CHAR(7)", "-1.2E-7", castType);
    f.checkCastToString("cast(0e0 as varchar(5))", "CHAR(3)", "0E0", castType);
    if (TODO) {
      f.checkCastToString("cast(-45e-2 as varchar(17))", "CHAR(7)",
          "-4.5E-1", castType);
    }
    if (TODO) {
      f.checkCastToString("cast(4683442.3432498375e0 as varchar(20))",
          "CHAR(19)",
          "4.683442343249838E6", castType);
    }
    if (TODO) {
      f.checkCastToString("cast(-0.1 as real)", "CHAR(5)", "-1E-1", castType);
    }
    f.checkString("cast(1.3243232e0 as varchar(4))", "1.32",
        "VARCHAR(4) NOT NULL");
    f.checkString("cast(1.9e5 as char(4))", "1900", "CHAR(4) NOT NULL");

    // string
    f.checkCastToString("'abc'", "CHAR(1)", "a", castType);
    f.checkCastToString("'abc'", "CHAR(3)", "abc", castType);
    f.checkCastToString("cast('abc' as varchar(6))", "CHAR(3)", "abc", castType);
    f.checkCastToString("cast(' abc  ' as varchar(10))", null, " abc  ", castType);
    f.checkCastToString("cast(cast('abc' as char(4)) as varchar(6))", null,
        "abc ", castType);
    f.checkString("cast(cast('a' as char(2)) as varchar(3)) || 'x' ",
        "a x", "VARCHAR(4) NOT NULL");
    f.checkString("cast(cast('a' as char(3)) as varchar(5)) || 'x' ",
        "a  x", "VARCHAR(6) NOT NULL");
    f.checkString("cast('a' as char(3)) || 'x'", "a  x",
        "CHAR(4) NOT NULL");

    f.checkScalar("char_length(cast(' x ' as char(4)))", 4,
        "INTEGER NOT NULL");
    f.checkScalar("char_length(cast(' x ' as varchar(3)))", 3,
        "INTEGER NOT NULL");
    f.checkScalar("char_length(cast(' x ' as varchar(4)))", 3,
        "INTEGER NOT NULL");
    f.checkScalar("char_length(cast(cast(' x ' as char(4)) as varchar(5)))",
        4, "INTEGER NOT NULL");
    f.checkScalar("char_length(cast(' x ' as varchar(3)))", 3,
        "INTEGER NOT NULL");

    // date & time
    f.checkCastToString("date '2008-01-01'", "CHAR(10)", "2008-01-01", castType);
    f.checkCastToString("time '1:2:3'", "CHAR(8)", "01:02:03", castType);
    f.checkCastToString("timestamp '2008-1-1 1:2:3'", "CHAR(19)",
        "2008-01-01 01:02:03", castType);
    f.checkCastToString("timestamp '2008-1-1 1:2:3'", "VARCHAR(30)",
        "2008-01-01 01:02:03", castType);

    f.checkCastToString("interval '3-2' year to month", "CHAR(5)", "+3-02", castType);
    f.checkCastToString("interval '32' month", "CHAR(3)", "+32", castType);
    f.checkCastToString("interval '1 2:3:4' day to second", "CHAR(11)",
        "+1 02:03:04", castType);
    f.checkCastToString("interval '1234.56' second(4,2)", "CHAR(8)",
        "+1234.56", castType);
    f.checkCastToString("interval '60' day", "CHAR(8)", "+60     ", castType);

    // boolean
    f.checkCastToString("True", "CHAR(4)", "TRUE", castType);
    f.checkCastToString("True", "CHAR(6)", "TRUE  ", castType);
    f.checkCastToString("True", "VARCHAR(6)", "TRUE", castType);
    f.checkCastToString("False", "CHAR(5)", "FALSE", castType);

    f.checkString("cast(true as char(3))", "TRU", "CHAR(3) NOT NULL");
    f.checkString("cast(false as char(4))", "FALS", "CHAR(4) NOT NULL");
    f.checkString("cast(true as varchar(3))", "TRU", "VARCHAR(3) NOT NULL");
    f.checkString("cast(false as varchar(4))", "FALS", "VARCHAR(4) NOT NULL");
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastBooleanToNumeric(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);
    SqlOperatorFixture f0 = f.withConformance(SqlConformanceEnum.DEFAULT);
    f0.checkFails("^" + castType.name() + "(true as integer)^",
        "Cast function cannot convert value of type BOOLEAN NOT NULL to "
            + "type INTEGER NOT NULL", false);
    f0.checkFails("^" + castType.name() + "(true as decimal)^",
        "Cast function cannot convert value of type BOOLEAN NOT NULL to "
            + "type DECIMAL\\(19, 0\\) NOT NULL", false);

    SqlOperatorFixture f1 = f.withConformance(SqlConformanceEnum.BIG_QUERY);
    f1.checkString("cast(true as integer)", "1", "INTEGER NOT NULL");
    f1.checkString("cast(false as integer)", "0", "INTEGER NOT NULL");
    f1.checkString("cast(true as bigint)", "1", "BIGINT NOT NULL");
    f1.checkFails("^" + castType.name() + "(true as float)^",
        "Cast function cannot convert value of type BOOLEAN NOT NULL to "
            + "type FLOAT NOT NULL", false);
    f1.checkFails("^" + castType.name() + "(true as decimal)^",
        "Cast function cannot convert value of type BOOLEAN NOT NULL to "
            + "type DECIMAL\\(19, 0\\) NOT NULL", false);
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastExactNumericLimits(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);

    // Test casting for min,max, out of range for exact numeric types
    Numeric.forEach(numeric -> {
      final String type = numeric.typeName;
      switch (numeric) {
      case DOUBLE:
      case FLOAT:
      case REAL:
        // Skip approx types
        return;
      default:
        // fall through
      }

      // Convert from literal to type
      f.checkCastToScalarOkay(numeric.maxNumericString, type, castType);
      f.checkCastToScalarOkay(numeric.minNumericString, type, castType);

      // Overflow test
      if (numeric == Numeric.BIGINT) {
        // Overflow for casting decimals produces a different error
        f.checkCastFails(numeric.maxOverflowNumericString,
            type, "Overflow", true, castType);
        f.checkCastFails(numeric.minOverflowNumericString,
            type, "Overflow", true, castType);
      } else {
        if (numeric != Numeric.DECIMAL5_2) {
          // This condition is for bug [CALCITE-6078], not yet fixed
          f.checkCastFails(numeric.maxOverflowNumericString,
              type, OUT_OF_RANGE_MESSAGE, true, castType);
          f.checkCastFails(numeric.minOverflowNumericString,
              type, OUT_OF_RANGE_MESSAGE, true, castType);
        }
      }

      // Convert from string to type
      f.checkCastToScalarOkay("'" + numeric.maxNumericString + "'",
          type, numeric.maxNumericString, castType);
      f.checkCastToScalarOkay("'" + numeric.minNumericString + "'",
          type, numeric.minNumericString, castType);

      if (numeric != Numeric.DECIMAL5_2) {
        // The above condition is for bug CALCITE-6078
        f.checkCastFails("'" + numeric.maxOverflowNumericString + "'",
            type, WRONG_FORMAT_MESSAGE, true, castType);
        f.checkCastFails("'" + numeric.minOverflowNumericString + "'",
            type, WRONG_FORMAT_MESSAGE, true, castType);
      }

      // Convert from type to string
      f.checkCastToString(numeric.maxNumericString, null, null, castType);
      f.checkCastToString(numeric.maxNumericString, type, null, castType);

      f.checkCastToString(numeric.minNumericString, null, null, castType);
      f.checkCastToString(numeric.minNumericString, type, null, castType);

      f.checkCastFails("'notnumeric'", type, INVALID_CHAR_MESSAGE, true,
          castType);
    });
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6395">
   * [CALCITE-6395] Significant precision loss when representing REAL literals</a>. */
  @Test public void floatPrecisionTest() {
    SqlOperatorFixture f = fixture();
    f.checkScalar("CAST(CAST('36854775807.0' AS REAL) AS BIGINT)",
        "36854775808", "BIGINT NOT NULL");
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastToExactNumeric(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);

    f.checkCastToScalarOkay("1", "BIGINT", castType);
    f.checkCastToScalarOkay("1", "INTEGER", castType);
    f.checkCastToScalarOkay("1", "SMALLINT", castType);
    f.checkCastToScalarOkay("1", "TINYINT", castType);
    f.checkCastToScalarOkay("1", "DECIMAL(4, 0)", castType);
    f.checkCastToScalarOkay("-1", "BIGINT", castType);
    f.checkCastToScalarOkay("-1", "INTEGER", castType);
    f.checkCastToScalarOkay("-1", "SMALLINT", castType);
    f.checkCastToScalarOkay("-1", "TINYINT", castType);
    f.checkCastToScalarOkay("-1", "DECIMAL(4, 0)", castType);

    f.checkCastToScalarOkay("1.234E3", "INTEGER", "1234", castType);
    f.checkCastToScalarOkay("-9.99E2", "INTEGER", "-999", castType);
    f.checkCastToScalarOkay("'1'", "INTEGER", "1", castType);
    f.checkCastToScalarOkay("' 01 '", "INTEGER", "1", castType);
    f.checkCastToScalarOkay("'-1'", "INTEGER", "-1", castType);
    f.checkCastToScalarOkay("' -00 '", "INTEGER", "0", castType);

    // string to integer
    f.checkScalarExact("cast('6543' as integer)", 6543);
    f.checkScalarExact("cast(' -123 ' as int)", -123);
    f.checkScalarExact("cast('654342432412312' as bigint)",
        "BIGINT NOT NULL",
        "654342432412312");
  }

  /** Test cases for <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6322">
   * [CALCITE-6322] Casts to DECIMAL types are ignored</a>. */
  @Test public void testIssue6322() {
    SqlOperatorFixture f = fixture();
    f.checkScalar("CAST(1.123 AS DECIMAL(4, 0))", "1", "DECIMAL(4, 0) NOT NULL");
    f.checkScalar("CAST(100 AS DECIMAL(3, 0))", "100", "DECIMAL(3, 0) NOT NULL");
    f.checkScalar("CAST(-100 AS DECIMAL(3, 0))", "-100", "DECIMAL(3, 0) NOT NULL");
    f.checkScalar("CAST(100 AS DECIMAL(5, 2))", "100.00", "DECIMAL(5, 2) NOT NULL");
    f.checkScalar("CAST(-100 AS DECIMAL(5, 2))", "-100.00", "DECIMAL(5, 2) NOT NULL");
    f.checkFails("CAST(1000 AS DECIMAL(2, 0))",
        "Value 1000 cannot be represented as a DECIMAL\\(2, 0\\)", true);
    f.checkFails("CAST(-1000 AS DECIMAL(2, 0))",
        "Value -1000 cannot be represented as a DECIMAL\\(2, 0\\)", true);
    f.checkScalar("CAST(100.5e0 AS DECIMAL(4, 1))", "100.5", "DECIMAL(4, 1) NOT NULL");
    f.checkScalar("CAST(-100.5e0 AS DECIMAL(4, 1))", "-100.5", "DECIMAL(4, 1) NOT NULL");
    f.checkScalar("CAST(100.55e0 AS DECIMAL(4, 1))", "100.5", "DECIMAL(4, 1) NOT NULL");
    f.checkScalar("CAST(-100.55e0 AS DECIMAL(4, 1))", "-100.5", "DECIMAL(4, 1) NOT NULL");
    f.checkFails("CAST(100.5e0 AS DECIMAL(4, 2))",
        "Value 100.5 cannot be represented as a DECIMAL\\(4, 2\\)", true);
    f.checkFails("CAST(-100.5e0 AS DECIMAL(4, 2))",
        "Value -100.5 cannot be represented as a DECIMAL\\(4, 2\\)", true);
  }

  /**
   * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-5843">
   * Constant expression with nested casts causes a compiler crash</a>. */
  @Test public void testConstantCast() {
    SqlOperatorFixture f = fixture();
    f.checkScalarExact("CAST(CAST('32767.4' AS FLOAT) AS SMALLINT)",
        "SMALLINT NOT NULL", "32767");
    f.checkScalarExact("CAST(CAST('32767.4' AS FLOAT) AS CHAR)",
        "CHAR(1) NOT NULL", "3");
  }

  /**
   * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6210">
   * Cast to VARBINARY causes an assertion failure</a>. */
  @Test public void testVarbinaryCast() {
    SqlOperatorFixture f = fixture();
    f.checkScalar("CAST('00' AS VARBINARY)", "3030", "VARBINARY NOT NULL");
    f.checkScalar("CAST('help' AS VARBINARY)", "68656c70", "VARBINARY NOT NULL");
    f.checkScalar("CAST('help' AS VARBINARY(2))", "6865", "VARBINARY(2) NOT NULL");
    f.checkScalar("CAST('00' AS BINARY(1))", "30", "BINARY(1) NOT NULL");
    f.checkScalar("CAST('10' AS BINARY(2))", "3130", "BINARY(2) NOT NULL");
    f.checkScalar("CAST('10' AS BINARY(1))", "31", "BINARY(1) NOT NULL");
    f.checkScalar("CAST('10' AS BINARY(3))", "313000", "BINARY(3) NOT NULL");
    f.checkScalar("CAST(_UTF8'Hello ਸੰਸਾਰ!' AS VARBINARY)",
        "48656c6c6f20e0a8b8e0a9b0e0a8b8e0a8bee0a8b021", "VARBINARY NOT NULL");
    f.checkFails("CAST('Hello ਸੰਸਾਰ!' AS VARBINARY)",
        ".*Failed to encode .* in character set 'ISO-8859-1'", true);
    f.checkNull("CAST(CAST(NULL AS VARCHAR) AS VARBINARY)");
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastStringToDecimal(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);
    // string to decimal
    f.checkScalarExact("cast('1.29' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "1.2");
    f.checkScalarExact("cast(' 1.25 ' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "1.2");
    f.checkScalarExact("cast('1.21' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "1.2");
    f.checkScalarExact("cast(' -1.29 ' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "-1.2");
    f.checkScalarExact("cast('-1.25' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "-1.2");
    f.checkScalarExact("cast(' -1.21 ' as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "-1.2");
    String shouldFail = "cast(' -1.21e' as decimal(2,1))";
    if (castType == CastType.CAST) {
      f.checkFails(shouldFail, INVALID_CHAR_MESSAGE, true);
    } else {
      // safe casts never fail
      f.checkNull(shouldFail);
    }
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastIntervalToNumeric(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);

    // Interval to Decimal
    f.checkScalarExact("cast(INTERVAL '1.29' second(1,2) as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "1.2");
    f.checkScalarExact("cast(INTERVAL '1.25' second as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "1.2");
    f.checkScalarExact("cast(INTERVAL '-1.29' second as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "-1.2");
    f.checkScalarExact("cast(INTERVAL '-1.25' second as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "-1.2");
    f.checkScalarExact("cast(INTERVAL '-1.21' second as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        "-1.2");
    f.checkScalarExact("cast(INTERVAL '5' minute as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        isDecimal("5.0"));
    f.checkScalarExact("cast(INTERVAL '5' hour as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        isDecimal("5.0"));
    f.checkScalarExact("cast(INTERVAL '5' day as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        isDecimal("5.0"));
    f.checkScalarExact("cast(INTERVAL '5' month as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        isDecimal("5.0"));
    f.checkScalarExact("cast(INTERVAL '5' year as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        isDecimal("5.0"));
    f.checkScalarExact("cast(INTERVAL '-5' day as decimal(2,1))",
        "DECIMAL(2, 1) NOT NULL",
        isDecimal("-5.0"));

    // Interval to bigint
    f.checkScalarExact("cast(INTERVAL '1.25' second as bigint)",
        "BIGINT NOT NULL",
        "1");
    f.checkScalarExact("cast(INTERVAL '-1.29' second(1,2) as bigint)",
        "BIGINT NOT NULL",
        "-1");
    f.checkScalarExact("cast(INTERVAL '5' day as bigint)",
        "BIGINT NOT NULL",
        "5");

    // Interval to integer
    f.checkScalarExact("cast(INTERVAL '1.25' second as integer)",
        "INTEGER NOT NULL",
        "1");
    f.checkScalarExact("cast(INTERVAL '-1.29' second(1,2) as integer)",
        "INTEGER NOT NULL",
        "-1");
    f.checkScalarExact("cast(INTERVAL '5' day as integer)",
        "INTEGER NOT NULL",
        "5");

    f.checkScalarExact("cast(INTERVAL '1' year as integer)",
        "INTEGER NOT NULL",
        "1");
    f.checkScalarExact(
        "cast((INTERVAL '1' year - INTERVAL '2' year) as integer)",
        "INTEGER NOT NULL",
        "-1");
    f.checkScalarExact("cast(INTERVAL '1' month as integer)",
        "INTEGER NOT NULL",
        "1");
    f.checkScalarExact(
        "cast((INTERVAL '1' month - INTERVAL '2' month) as integer)",
        "INTEGER NOT NULL",
        "-1");
    f.checkScalarExact("cast(INTERVAL '1' day as integer)",
        "INTEGER NOT NULL",
        "1");
    f.checkScalarExact("cast((INTERVAL '1' day - INTERVAL '2' day) as integer)",
        "INTEGER NOT NULL",
        "-1");
    f.checkScalarExact("cast(INTERVAL '1' hour as integer)",
        "INTEGER NOT NULL",
        "1");
    f.checkScalarExact(
        "cast((INTERVAL '1' hour - INTERVAL '2' hour) as integer)",
        "INTEGER NOT NULL",
        "-1");
    f.checkScalarExact(
        "cast(INTERVAL '1' hour as integer)",
        "INTEGER NOT NULL",
        "1");
    f.checkScalarExact(
        "cast((INTERVAL '1' minute - INTERVAL '2' minute) as integer)",
        "INTEGER NOT NULL",
        "-1");
    f.checkScalarExact("cast(INTERVAL '1' minute as integer)",
        "INTEGER NOT NULL",
        "1");
    f.checkScalarExact(
        "cast((INTERVAL '1' second - INTERVAL '2' second) as integer)",
        "INTEGER NOT NULL",
        "-1");
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastToInterval(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);
    f.checkScalar(
        "cast(5 as interval second)",
        "+5.000000",
        "INTERVAL SECOND NOT NULL");
    f.checkScalar(
        "cast(5 as interval minute)",
        "+5",
        "INTERVAL MINUTE NOT NULL");
    f.checkScalar(
        "cast(5 as interval hour)",
        "+5",
        "INTERVAL HOUR NOT NULL");
    f.checkScalar(
        "cast(5 as interval day)",
        "+5",
        "INTERVAL DAY NOT NULL");
    f.checkScalar(
        "cast(5 as interval month)",
        "+5",
        "INTERVAL MONTH NOT NULL");
    f.checkScalar(
        "cast(5 as interval year)",
        "+5",
        "INTERVAL YEAR NOT NULL");
    f.checkScalar(
        "cast(5.7 as interval day)",
        "+6",
        "INTERVAL DAY NOT NULL");
    f.checkScalar(
        "cast(-5.7 as interval day)",
        "-6",
        "INTERVAL DAY NOT NULL");
    f.checkScalar(
        "cast(6.2 as interval day)",
        "+6",
        "INTERVAL DAY NOT NULL");
    f.checkScalar(
        "cast(3456 as interval month(4))",
        "+3456",
        "INTERVAL MONTH(4) NOT NULL");
    f.checkScalar(
        "cast(-5723 as interval minute(4))",
        "-5723",
        "INTERVAL MINUTE(4) NOT NULL");
    f.checkScalar(
        "cast(cast(1 as integer) as interval minute)",
        "+1",
        "INTERVAL MINUTE NOT NULL");
    f.checkScalar(
        "cast(cast(1 as decimal) as interval second)",
        "+1.000000",
        "INTERVAL SECOND NOT NULL");
    f.checkScalar(
        "cast(cast(5 as bigint) as interval month)",
        "+5",
        "INTERVAL MONTH NOT NULL");
    f.checkScalar(
        "cast(cast(5 as smallint) as interval hour)",
        "+5",
        "INTERVAL HOUR NOT NULL");
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastIntervalToInterval(CastType castType, SqlOperatorFixture f) {
    f.checkScalar("cast(interval '2 5' day to hour as interval hour to minute)",
        "+53:00",
        "INTERVAL HOUR TO MINUTE NOT NULL");
    f.checkScalar("cast(interval '2 5' day to hour as interval day to minute)",
        "+2 05:00",
        "INTERVAL DAY TO MINUTE NOT NULL");
    f.checkScalar("cast(interval '2 5' day to hour as interval hour to second)",
        "+53:00:00.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    f.checkScalar("cast(interval '2 5' day to hour as interval hour)",
        "+53",
        "INTERVAL HOUR NOT NULL");
    f.checkScalar("cast(interval '-29:15' hour to minute as interval day to hour)",
        "-1 05",
        "INTERVAL DAY TO HOUR NOT NULL");
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastWithRoundingToScalar(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);

    f.checkScalar("cast(1.25 as int)", 1, "INTEGER NOT NULL");
    f.checkScalar("cast(1.25E0 as int)", 1, "INTEGER NOT NULL");
    // Calcite's simplifier uses BigDecimal.intValue(), which rounds down
    f.checkScalar("cast(1.5 as int)", 1, "INTEGER NOT NULL");
    f.checkScalar("cast(5E-1 as int)", 0, "INTEGER NOT NULL");
    f.checkScalar("cast(1.75 as int)", 1, "INTEGER NOT NULL");
    f.checkScalar("cast(1.75E0 as int)", 1, "INTEGER NOT NULL");

    f.checkScalar("cast(-1.25 as int)", -1, "INTEGER NOT NULL");
    f.checkScalar("cast(-1.25E0 as int)", -1, "INTEGER NOT NULL");
    f.checkScalar("cast(-1.5 as int)", -1, "INTEGER NOT NULL");
    f.checkScalar("cast(-5E-1 as int)", 0, "INTEGER NOT NULL");
    f.checkScalar("cast(-1.75 as int)", -1, "INTEGER NOT NULL");
    f.checkScalar("cast(-1.75E0 as int)", -1, "INTEGER NOT NULL");

    f.checkScalar("cast(1.23454 as int)", 1, "INTEGER NOT NULL");
    f.checkScalar("cast(1.23454E0 as int)", 1, "INTEGER NOT NULL");
    f.checkScalar("cast(1.23455 as int)", 1, "INTEGER NOT NULL");
    f.checkScalar("cast(5E-5 as int)", 0, "INTEGER NOT NULL");
    f.checkScalar("cast(1.99995 as int)", 1, "INTEGER NOT NULL");
    f.checkScalar("cast(1.99995E0 as int)", 1, "INTEGER NOT NULL");

    f.checkScalar("cast(-1.23454 as int)", -1, "INTEGER NOT NULL");
    f.checkScalar("cast(-1.23454E0 as int)", -1, "INTEGER NOT NULL");
    f.checkScalar("cast(-1.23455 as int)", -1, "INTEGER NOT NULL");
    f.checkScalar("cast(-5E-5 as int)", 0, "INTEGER NOT NULL");
    f.checkScalar("cast(-1.99995 as int)", -1, "INTEGER NOT NULL");
    f.checkScalar("cast(-1.99995E0 as int)", -1, "INTEGER NOT NULL");

    f.checkScalar("cast(9.99 as decimal(2,1))", 9.9, "DECIMAL(2, 1) NOT NULL");
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastDecimalToDoubleToInteger(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);

    f.checkScalar("cast( cast(1.25 as double) as integer)", 1, "INTEGER NOT NULL");
    f.checkScalar("cast( cast(-1.25 as double) as integer)", -1, "INTEGER NOT NULL");
    f.checkScalar("cast( cast(1.75 as double) as integer)", 1, "INTEGER NOT NULL");
    f.checkScalar("cast( cast(-1.75 as double) as integer)", -1, "INTEGER NOT NULL");
    f.checkScalar("cast( cast(1.5 as double) as integer)", 1, "INTEGER NOT NULL");
    f.checkScalar("cast( cast(-1.5 as double) as integer)", -1, "INTEGER NOT NULL");
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastApproxNumericLimits(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);

    // Test casting for min, max, out of range for approx numeric types
    Numeric.forEach(numeric -> {
      String type = numeric.typeName;
      boolean isFloat;

      switch (numeric) {
      case DOUBLE:
      case FLOAT:
        isFloat = false;
        break;
      case REAL:
        isFloat = true;
        break;
      default:
        // Skip non-approx types
        return;
      }

      if (!f.brokenTestsEnabled()) {
        return;
      }

      // Convert from literal to type
      f.checkCastToApproxOkay(numeric.maxNumericString, type,
          isFloat
              ? isWithin(numeric.maxNumericAsDouble(), 1E32)
              : isExactly(numeric.maxNumericAsDouble()), castType);
      f.checkCastToApproxOkay(numeric.minNumericString, type,
          isExactly(numeric.minNumericString), castType);

      if (isFloat) {
        f.checkCastFails(numeric.maxOverflowNumericString, type,
            OUT_OF_RANGE_MESSAGE, true, castType);
      } else {
        // Double: Literal out of range
        f.checkCastFails(numeric.maxOverflowNumericString, type,
            LITERAL_OUT_OF_RANGE_MESSAGE, false, castType);
      }

      // Underflow: goes to 0
      f.checkCastToApproxOkay(numeric.minOverflowNumericString, type,
          isExactly(0), castType);

      // Convert from string to type
      f.checkCastToApproxOkay("'" + numeric.maxNumericString + "'", type,
          isFloat
              ? isWithin(numeric.maxNumericAsDouble(), 1E32)
              : isExactly(numeric.maxNumericAsDouble()), castType);
      f.checkCastToApproxOkay("'" + numeric.minNumericString + "'", type,
          isExactly(numeric.minNumericAsDouble()), castType);

      f.checkCastFails("'" + numeric.maxOverflowNumericString + "'", type,
          OUT_OF_RANGE_MESSAGE, true, castType);

      // Underflow: goes to 0
      f.checkCastToApproxOkay("'" + numeric.minOverflowNumericString + "'",
          type, isExactly(0), castType);

      // Convert from type to string

      // Treated as DOUBLE
      f.checkCastToString(numeric.maxNumericString, null,
          isFloat ? null : "1.79769313486231E308", castType);

      // TODO: The following tests are slightly different depending on
      // whether the java or fennel calc are used.
      // Try to make them the same
      if (false /* fennel calc*/) { // Treated as FLOAT or DOUBLE
        f.checkCastToString(numeric.maxNumericString, type,
            // Treated as DOUBLE
            isFloat ? "3.402824E38" : "1.797693134862316E308", castType);
        f.checkCastToString(numeric.minNumericString, null,
            // Treated as FLOAT or DOUBLE
            isFloat ? null : "4.940656458412465E-324", castType);
        f.checkCastToString(numeric.minNumericString, type,
            isFloat ? "1.401299E-45" : "4.940656458412465E-324", castType);
      } else if (false /* JavaCalc */) {
        // Treated as FLOAT or DOUBLE
        f.checkCastToString(numeric.maxNumericString, type,
            // Treated as DOUBLE
            isFloat ? "3.402823E38" : "1.797693134862316E308", castType);
        f.checkCastToString(numeric.minNumericString, null,
            isFloat ? null : null, castType); // Treated as FLOAT or DOUBLE
        f.checkCastToString(numeric.minNumericString, type,
            isFloat ? "1.401298E-45" : null, castType);
      }

      f.checkCastFails("'notnumeric'", type, INVALID_CHAR_MESSAGE, true, castType);
    });
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastToApproxNumeric(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);

    f.checkCastToApproxOkay("1", "DOUBLE", isExactly(1), castType);
    f.checkCastToApproxOkay("1.0", "DOUBLE", isExactly(1), castType);
    f.checkCastToApproxOkay("-2.3", "FLOAT", isWithin(-2.3, 0.000001), castType);
    f.checkCastToApproxOkay("'1'", "DOUBLE", isExactly(1), castType);
    f.checkCastToApproxOkay("'  -1e-37  '", "DOUBLE", isExactly("-1.0E-37"),
        castType);
    f.checkCastToApproxOkay("1e0", "DOUBLE", isExactly(1), castType);
    f.checkCastToApproxOkay("0e0", "REAL", isExactly(0), castType);
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastNull(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);

    // null
    f.checkNull("cast(null as integer)");
    f.checkNull("cast(null as decimal(4,3))");
    f.checkNull("cast(null as double)");
    f.checkNull("cast(null as varchar(10))");
    f.checkNull("cast(null as char(10))");
    f.checkNull("cast(null as date)");
    f.checkNull("cast(null as time)");
    f.checkNull("cast(null as timestamp)");
    f.checkNull("cast(null as interval year to month)");
    f.checkNull("cast(null as interval day to second(3))");
    f.checkNull("cast(null as boolean)");

    if (castType != CastType.CAST) {
      // In the following, 'cast' becomes 'safe_cast' or 'try_cast'
      f.checkNull("cast('a' as time)");
      f.checkNull("cast('a' as int)");
      f.checkNull("cast('2023-03-17a' as date)");
      f.checkNull("cast('12:12:11a' as time)");
      f.checkNull("cast('a' as interval year)");
      f.checkNull("cast('a' as interval minute to second)");
      f.checkNull("cast('True' as bigint)");
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1439">[CALCITE-1439]
   * Handling errors during constant reduction</a>. */
  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastInvalid(CastType castType, SqlOperatorFixture f) {
    // Before CALCITE-1439 was fixed, constant reduction would kick in and
    // generate Java constants that throw when the class is loaded, thus
    // ExceptionInInitializerError.
    f.checkScalarExact("cast('15' as integer)", "INTEGER NOT NULL", "15");
    if (castType == CastType.CAST) { // Safe casts should not fail
      f.checkFails("cast('15.4' as integer)", WRONG_FORMAT_MESSAGE, true);
      f.checkFails("cast('15.6' as integer)", WRONG_FORMAT_MESSAGE, true);
      f.checkFails("cast('ue' as boolean)", "Invalid character for cast.*", true);
      f.checkFails("cast('' as boolean)", "Invalid character for cast.*", true);
      f.checkFails("cast('' as integer)", WRONG_FORMAT_MESSAGE, true);
      f.checkFails("cast('' as real)", WRONG_FORMAT_MESSAGE, true);
      f.checkFails("cast('' as double)", WRONG_FORMAT_MESSAGE, true);
      f.checkFails("cast('' as smallint)", WRONG_FORMAT_MESSAGE, true);
    } else {
      f.checkNull("cast('15.4' as integer)");
      f.checkNull("cast('15.6' as integer)");
      f.checkNull("cast('ue' as boolean)");
      f.checkNull("cast('' as boolean)");
      f.checkNull("cast('' as integer)");
      f.checkNull("cast('' as real)");
      f.checkNull("cast('' as double)");
      f.checkNull("cast('' as smallint)");
    }
  }

  /** Test cast for DATE, TIME, TIMESTAMP types. */
  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastDateTime(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);

    f.checkScalar("cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIMESTAMP)",
        "1945-02-24 12:42:25", "TIMESTAMP(0) NOT NULL");

    f.checkScalar("cast(TIME '12:42:25.34' as TIME)",
        "12:42:25", "TIME(0) NOT NULL");

    // test rounding; uses truncation
    f.checkScalar("cast(TIME '12:42:25.9' as TIME)",
        "12:42:25", "TIME(0) NOT NULL");

    // test precision
    f.checkScalar("cast(TIME '12:42:25.34' as TIME(2))",
        "12:42:25.34", "TIME(2) NOT NULL");

    f.checkScalar("cast(DATE '1945-02-24' as DATE)",
        "1945-02-24", "DATE NOT NULL");

    // timestamp <-> time
    f.checkScalar("cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIME)",
        "12:42:25", "TIME(0) NOT NULL");

    // time <-> string
    f.checkCastToString("TIME '12:42:25'", null, "12:42:25", castType);
    if (TODO) {
      f.checkCastToString("TIME '12:42:25.34'", null, "12:42:25.34", castType);
    }

    // Generate the current date as a string, e.g. "2007-04-18". The value
    // is guaranteed to be good for at least 2 minutes, which should give
    // us time to run the rest of the tests.
    final String today =
        new SimpleDateFormat("yyyy-MM-dd", Locale.ROOT)
            .format(getCalendarNotTooNear(Calendar.DAY_OF_MONTH).getTime());

    f.checkScalar("cast(DATE '1945-02-24' as TIMESTAMP)",
        "1945-02-24 00:00:00", "TIMESTAMP(0) NOT NULL");

    // Note: Casting to time(0) should lose date info and fractional
    // seconds, then casting back to timestamp should initialize to
    // current_date.
    f.checkScalar(
        "cast(cast(TIMESTAMP '1945-02-24 12:42:25.34' as TIME) as TIMESTAMP)",
        today + " 12:42:25", "TIMESTAMP(0) NOT NULL");

    f.checkScalar("cast(TIME '12:42:25.34' as TIMESTAMP)",
        today + " 12:42:25", "TIMESTAMP(0) NOT NULL");

    // timestamp <-> date
    f.checkScalar("cast(TIMESTAMP '1945-02-24 12:42:25.34' as DATE)",
        "1945-02-24", "DATE NOT NULL");

    // Note: casting to Date discards Time fields
    f.checkScalar(
        "cast(cast(TIMESTAMP '1945-02-24 12:42:25.34' as DATE) as TIMESTAMP)",
        "1945-02-24 00:00:00", "TIMESTAMP(0) NOT NULL");
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastStringToDateTime(CastType castType, SqlOperatorFixture f) {
    f.checkScalar("cast('12:42:25' as TIME)",
        "12:42:25", "TIME(0) NOT NULL");
    f.checkScalar("cast('1:42:25' as TIME)",
        "01:42:25", "TIME(0) NOT NULL");
    f.checkScalar("cast('1:2:25' as TIME)",
        "01:02:25", "TIME(0) NOT NULL");
    f.checkScalar("cast('  12:42:25  ' as TIME)",
        "12:42:25", "TIME(0) NOT NULL");
    f.checkScalar("cast('12:42:25.34' as TIME)",
        "12:42:25", "TIME(0) NOT NULL");

    f.checkScalar("cast('12:42:25.34' as TIME(2))",
        "12:42:25.34", "TIME(2) NOT NULL");

    if (castType == CastType.CAST) {
      f.checkFails("cast('nottime' as TIME)", BAD_DATETIME_MESSAGE, true);
    } else {
      f.checkNull("cast('nottime' as TIME)");
    }

    if (Bug.CALCITE_6092_FIXED) {
      f.checkFails("cast('1241241' as TIME)", "Invalid TIME value, '1241241'", true);
      f.checkFails("cast('12:54:78' as TIME)", "Invalid TIME value, '12:54:78'", true);
    }
    f.checkScalar("cast('12:34:5' as TIME)", "12:34:05", "TIME(0) NOT NULL");
    f.checkScalar("cast('12:3:45' as TIME)", "12:03:45", "TIME(0) NOT NULL");
    f.checkScalar("cast('1:23:45' as TIME)", "01:23:45", "TIME(0) NOT NULL");

    // timestamp <-> string
    f.checkCastToString("TIMESTAMP '1945-02-24 12:42:25'", null,
        "1945-02-24 12:42:25", castType);

    if (TODO) {
      // TODO: casting allows one to discard precision without error
      f.checkCastToString("TIMESTAMP '1945-02-24 12:42:25.34'",
          null, "1945-02-24 12:42:25.34", castType);
    }

    f.checkScalar("cast('1945-02-24 12:42:25' as TIMESTAMP)",
        "1945-02-24 12:42:25", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("cast('  1945-02-24 12:42:25  ' as TIMESTAMP)",
        "1945-02-24 12:42:25", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("cast('1945-02-24 12:42:25.34' as TIMESTAMP)",
        "1945-02-24 12:42:25", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("cast('1945-12-31' as TIMESTAMP)",
        "1945-12-31 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("cast('2004-02-29' as TIMESTAMP)",
        "2004-02-29 00:00:00", "TIMESTAMP(0) NOT NULL");

    f.checkScalar("cast('1945-02-24 12:42:25.34' as TIMESTAMP(2))",
        "1945-02-24 12:42:25.34", "TIMESTAMP(2) NOT NULL");
    if (castType == CastType.CAST) {
      f.checkFails("cast('1945-2-2 12:2:5' as TIMESTAMP)",
          "Invalid DATE value, '1945-2-2 12:2:5'", true);
      f.checkFails("cast('1241241' as TIMESTAMP)",
          "Invalid DATE value, '1241241'", true);
      f.checkFails("cast('1945-20-24 12:42:25.34' as TIMESTAMP)",
          "Invalid DATE value, '1945-20-24 12:42:25.34'", true);
      f.checkFails("cast('1945-01-24 25:42:25.34' as TIMESTAMP)",
          "Value of HOUR field is out of range in '1945-01-24 25:42:25.34'", true);
      f.checkFails("cast('1945-1-24 12:23:34.454' as TIMESTAMP)",
          "Invalid DATE value, '1945-1-24 12:23:34.454'", true);
    } else {
      // test cases for 'SAFE_CAST' and 'TRY_CAST'
      f.checkNull("cast('1945-2-2 12:2:5' as TIMESTAMP)");
      f.checkNull("cast('1241241' as TIMESTAMP)");
      f.checkNull("cast('1945-20-24 12:42:25.34' as TIMESTAMP)");
      f.checkNull("cast('1945-01-24 25:42:25.34' as TIMESTAMP)");
      f.checkNull("cast('1945-1-24 12:23:34.454' as TIMESTAMP)");
    }
    if (castType == CastType.CAST) {
      f.checkFails("cast('nottime' as TIMESTAMP)", BAD_DATETIME_MESSAGE, true);
    } else {
      f.checkNull("cast('nottime' as TIMESTAMP)");
    }

    // date <-> string
    f.checkCastToString("DATE '1945-02-24'", null, "1945-02-24", castType);
    f.checkCastToString("DATE '1945-2-24'", null, "1945-02-24", castType);

    f.checkScalar("cast('1945-02-24' as DATE)", "1945-02-24", "DATE NOT NULL");
    f.checkScalar("cast(' 1945-2-4 ' as DATE)", "1945-02-04", "DATE NOT NULL");
    f.checkScalar("cast('  1945-02-24  ' as DATE)",
        "1945-02-24", "DATE NOT NULL");
    if (castType == CastType.CAST) {
      f.checkFails("cast('notdate' as DATE)", BAD_DATETIME_MESSAGE, true);
    } else {
      f.checkNull("cast('notdate' as DATE)");
    }

    if (castType == CastType.CAST) {
      f.checkFails("cast('52534253' as DATE)", BAD_DATETIME_MESSAGE, true);
      f.checkFails("cast('1945-30-24' as DATE)", BAD_DATETIME_MESSAGE, true);
    } else {
      f.checkNull("cast('52534253' as DATE)");
      f.checkNull("cast('1945-30-24' as DATE)");
    }

    // cast null
    f.checkNull("cast(null as date)");
    f.checkNull("cast(null as timestamp)");
    f.checkNull("cast(null as time)");
    f.checkNull("cast(cast(null as varchar(10)) as time)");
    f.checkNull("cast(cast(null as varchar(10)) as date)");
    f.checkNull("cast(cast(null as varchar(10)) as timestamp)");
    f.checkNull("cast(cast(null as date) as timestamp)");
    f.checkNull("cast(cast(null as time) as timestamp)");
    f.checkNull("cast(cast(null as timestamp) as date)");
    f.checkNull("cast(cast(null as timestamp) as time)");
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastFormatClauseDateTimeToString(CastType castType, SqlOperatorFixture f) {

    // Cast DATE to String
    f.checkString("cast(date '2018-01-30' as varchar format 'YYYY')",
        "2018",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '50-01-30' as varchar format 'YYYY')",
        "0050",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-30' as varchar format 'YYY')",
        "018",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-30' as varchar format 'YYY')",
        "018",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '50-01-30' as varchar format 'YYY')",
        "050",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-30' as varchar format 'Y')",
        "8",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-11-30' as varchar format 'Month')",
        "November",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-30' as varchar format 'MONTH')",
        "JANUARY",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-9-30' as varchar format 'mon')",
        "sep",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-12-30' as varchar format 'Mon')",
        "Dec",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-30' as varchar format 'MON')",
        "JAN",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-30' as varchar format 'MM')",
        "01",
        "VARCHAR NOT NULL");

    f.checkString("cast(date '2018-01-30' as varchar format 'DAY')",
        "TUESDAY",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-30' as varchar format 'Day')",
        "Tuesday",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-30' as varchar format 'day')",
        "tuesday",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-01' as varchar format 'DY')",
        "MON",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-01' as varchar format 'Dy')",
        "Mon",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-01' as varchar format 'dy')",
        "mon",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-30' as varchar format 'D')",
        "3",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-01-30' as varchar format 'DD')",
        "30",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2018-06-30' as varchar format 'DDD')",
        "181",
        "VARCHAR NOT NULL");

    f.checkString("cast(date '2018-01-30' as varchar format 'MM-DD-YY')",
        "01-30-18",
        "VARCHAR NOT NULL");
    f.checkString("cast(date '2021-12-21' as varchar format 'YY Q MON DD')",
        "21 4 DEC 21",
        "VARCHAR NOT NULL");

    // Cast TIME to String
    f.checkString("cast(time '21:30:00' as varchar format 'HH12')",
        "09",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '1:30:00' as varchar format 'HH24')",
        "01",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '11:24:00' as varchar format 'MI')",
        "24",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '21:30:25.16' as varchar format 'SS')",
        "25",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '15:45:10' as varchar format 'HH12:MI')",
        "03:45",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '21:30:25.16' as varchar format 'SSSSS')",
        "77425",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '00:00:00.23' as varchar format 'SSSSS')",
        "00000",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '01:59:59.99' as varchar format 'SSSSS')",
        "07199",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '23:30:55.43' as varchar format 'AM')",
        "PM",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '12:30:55' as varchar format 'PM')",
        "PM",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '08:45:12' as varchar format 'P.M.')",
        "A.M.",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '16:17:12' as varchar format 'am')",
        "pm",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '02:23:23' as varchar format 'p.m.')",
        "a.m.",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '23:30:55.4757' as varchar format 'FF2')",
        "47",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '23:30:55.1233793' as varchar format 'FF5')",
        "12300",
        "VARCHAR NOT NULL");
    f.checkString("cast(time '23:30:55.435712' as varchar format 'FF9')",
          "435000000",
          "VARCHAR NOT NULL");


    // Cast TIMESTAMP to String
    if (Bug.CALCITE_6367_FIXED) {
      // Query output cannot be validated as it's dependent on execution time zone
      f.checkQuery("cast(timestamp '2008-12-25 00:00:00+06:00' as varchar format 'TZH')");
      f.checkString("cast(timestamp '2008-12-25 00:00:00+00:00' as varchar format "
              + "'TZM' AT TIME ZONE 'Asia/Kolkata')",
          "30",
          "VARCHAR NOT NULL");
    }
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastFormatClauseStringToDateTime(CastType castType, SqlOperatorFixture f) {
    f.checkScalar("cast('18-12-03' as date format 'YY-MM-DD')",
        "2018-12-03",
        "DATE NOT NULL");
    f.checkScalar("cast('JUN 30, 2018' as date format 'MON DD, YYYY')",
        "2018-06-30",
        "DATE NOT NULL");
    f.checkScalar("cast('17:30' as time format 'HH12:MI')",
        "17:30:00",
        "TIME(0) NOT NULL");
    f.checkScalar("cast('01:05:07.16' as time format 'HH24:MI:SS.FF4')",
        "01:05:07",
        "TIME(0) NOT NULL");
    f.checkScalar("cast('2017-05-12' as timestamp format 'YYYY-MM-DD')",
        "2017-05-12 00:00:00",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("cast('2020.06.03 12:42:53' as timestamp format 'YYYY.MM.DD HH:MI:SS')",
        "2020-06-03 12:42:53",
        "TIMESTAMP(0) NOT NULL");

    if (Bug.CALCITE_6367_FIXED) {
      f.checkScalar("cast('2020.06.03 00:00:53+06:30' as timestamp format"
              + " 'YYYY.MM.DD HH24:MI:SSTZH:TZM')",
          "2020-06-02 17:30:53 UTC",
          "TIMESTAMP(0) NOT NULL");
      f.checkScalar("cast('03:30 P.M.' as time format 'HH:MI P.M.')",
          "15:30:00",
          "TIME(0) NOT NULL");
    }
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastFormatClauseByteToString(CastType castType, SqlOperatorFixture f) {
    if (Bug.CALCITE_6270_FIXED) {
      f.checkString("cast(b'\\x48\\x65\\x6c\\x6c\\x6f' as varchar format 'ASCII')",
          "Hello",
          "VARCHAR");
      f.checkScalar("cast('Hello' as varbinary format 'ASCII')",
          "\\x48\\x65\\x6c\\x6c\\x6f",
          "VARBINARY NOT NULL");
    }
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastFormatClauseNumericToString(CastType castType, SqlOperatorFixture f) {
    if (Bug.CALCITE_6270_FIXED) {
      f.checkString("cast(-12.23 as varchar FORMAT '999.999')",
          "-12.230",
          "VARCHAR NOT NULL");
      f.checkString("cast(1234.56 as varchar FORMAT '$999,999.999')",
          "$1,234.560",
          "VARCHAR NOT NULL");
      f.checkString("cast(123456 as varchar FORMAT '9.999EEEE')",
          "1.235E+05",
          "VARCHAR NOT NULL");
    }
  }

  @Test void testMssqlConvert() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.MSSQL_CONVERT, VmName.EXPAND);
    // happy-paths (no need to test all, proper functionality is tested by CAST already
    // just need to make sure it works at all
    f.checkScalar("convert(INTEGER, 45.4)", "45", "INTEGER NOT NULL");
    f.checkScalar("convert(DATE, '2000-01-01')", "2000-01-01", "DATE NOT NULL");

    // null-values
    f.checkNull("convert(DATE, NULL)");
  }

  @Test void testMssqlConvertWithStyle() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.MSSQL_CONVERT, VmName.EXPAND);
    // ensure 'style' argument is ignored
    f.checkScalar("convert(INTEGER, 45.4, 999)", "45", "INTEGER NOT NULL");
    f.checkScalar("convert(DATE, '2000-01-01', 999)", "2000-01-01", "DATE NOT NULL");
    // including 'NULL' style argument
    f.checkScalar("convert(DATE, '2000-01-01', NULL)", "2000-01-01", "DATE NOT NULL");

  }

  private static Calendar getFixedCalendar() {
    Calendar calendar = Util.calendar();
    calendar.set(Calendar.YEAR, 2014);
    calendar.set(Calendar.MONTH, 8);
    calendar.set(Calendar.DATE, 7);
    calendar.set(Calendar.HOUR_OF_DAY, 17);
    calendar.set(Calendar.MINUTE, 8);
    calendar.set(Calendar.SECOND, 48);
    calendar.set(Calendar.MILLISECOND, 15);
    return calendar;
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

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastToBoolean(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);

    // string to boolean
    f.checkBoolean("cast('true' as boolean)", true);
    f.checkBoolean("cast('false' as boolean)", false);
    f.checkBoolean("cast('  trUe' as boolean)", true);
    f.checkBoolean("cast('  tr' || 'Ue' as boolean)", true);
    f.checkBoolean("cast('  fALse' as boolean)", false);
    if (castType == CastType.CAST) {
      f.checkFails("cast('unknown' as boolean)", INVALID_CHAR_MESSAGE, true);
    } else {
      f.checkNull("cast('unknown' as boolean)");
    }

    f.checkBoolean("cast(cast('true' as varchar(10))  as boolean)", true);
    f.checkBoolean("cast(cast('false' as varchar(10)) as boolean)", false);
    if (castType == CastType.CAST) {
      f.checkFails("cast(cast('blah' as varchar(10)) as boolean)",
          INVALID_CHAR_MESSAGE, true);
    } else {
      f.checkNull("cast(cast('blah' as varchar(10)) as boolean)");
    }
  }

  @Test void testCastToDecimal() {
    final SqlOperatorFixture f = fixture();
    // test the minimum scale is 0
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);
    // cast integer to decimal
    f.checkFails("cast(123 as decimal(3, -1))",
        "DECIMAL scale -1 must be between 0 and 19", false);
    // cast float to decimal
    f.checkFails("cast(12.3 as decimal(3, -1))",
        "DECIMAL scale -1 must be between 0 and 19", false);
    // cast decimal to decimal
    f.checkFails("cast(cast(12.3 as decimal(3, 1)) as decimal(3, -1))",
        "DECIMAL scale -1 must be between 0 and 19", false);
    // cast string to decimal
    f.checkFails("cast('12.3' as decimal(3, -1))",
        "DECIMAL scale -1 must be between 0 and 19", false);
    // cast interval to decimal
    f.checkFails("cast(INTERVAL '5' hour as decimal(3, -1))",
        "DECIMAL scale -1 must be between 0 and 19", false);
    // test the minimum scale is -2
    final SqlOperatorFixture negativeScaleFixture = fixture()
        .withFactory(tf ->
            tf.withTypeSystem(typeSystem ->
                CustomTypeSystems.withMinScale(typeSystem, typeName -> -2)));
    // cast integer to decimal
    negativeScaleFixture.checkScalar("cast(123 as decimal(3, -1))",
        "120", "DECIMAL(3, -1) NOT NULL");
    // cast float to decimal
    negativeScaleFixture.checkScalar("cast(12.3 as decimal(3, -1))",
        "10", "DECIMAL(3, -1) NOT NULL");
    // cast decimal to decimal
    negativeScaleFixture.checkScalar("cast(cast(12.3 as decimal(3, 1)) as decimal(3, -1))",
        "10", "DECIMAL(3, -1) NOT NULL");
    // cast string to decimal
    negativeScaleFixture.checkScalar("cast('12.3' as decimal(3, -1))",
        "10", "DECIMAL(3, -1) NOT NULL");
    // cast interval to decimal
    negativeScaleFixture.checkScalar("cast(INTERVAL '5' hour as decimal(3, -1))",
        "0", "DECIMAL(3, -1) NOT NULL");
  }

  @Test void testCastRowType() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("cast((1, 2) as row(f0 integer, f1 bigint))",
        "{1, 2}",
        "RecordType(INTEGER NOT NULL F0, BIGINT NOT NULL F1) NOT NULL");
    f.checkScalar("cast((1, 2) as row(f0 integer, f1 decimal(2)))",
        "{1, 2}",
        "RecordType(INTEGER NOT NULL F0, DECIMAL(2, 0) NOT NULL F1) NOT NULL");
    f.checkScalar("cast((1, '2') as row(f0 integer, f1 varchar))",
        "{1, 2}",
        "RecordType(INTEGER NOT NULL F0, VARCHAR NOT NULL F1) NOT NULL");
    f.checkScalar("cast(('A', 'B') as row(f0 varchar, f1 varchar))",
        "{A, B}",
        "RecordType(VARCHAR NOT NULL F0, VARCHAR NOT NULL F1) NOT NULL");
    f.checkNull("cast(null as row(f0 integer, f1 bigint))");
    f.checkNull("cast(null as row(f0 integer, f1 decimal(2,0)))");
    f.checkNull("cast(null as row(f0 integer, f1 varchar))");
    f.checkNull("cast(null as row(f0 varchar, f1 varchar))");
  }

  /** Test cases for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4918">
   * [CALCITE-4918] Add a VARIANT data type</a>. */
  @Test public void testVariant() {
    SqlOperatorFixture f = fixture();
    f.checkScalar("cast(1 as VARIANT)", "1", "VARIANT NOT NULL");
    // String variants include quotes when output
    f.checkScalar("cast('abc' as VARIANT)", "\"abc\"", "VARIANT NOT NULL");
    f.checkScalar("cast(ARRAY[1,2,3] as VARIANT)", "[1, 2, 3]", "VARIANT NOT NULL");
    f.checkScalar("cast(MULTISET[1,2,3] as VARIANT)", "[1, 2, 3]", "VARIANT NOT NULL");
    f.checkScalar("cast(MAP['a',1,'b',2] as VARIANT)", "{\"a\"=1, \"b\"=2}", "VARIANT NOT NULL");
    f.checkScalar("cast((1, 2) as row(f0 integer, f1 bigint))", "{1, 2}",
        "RecordType(INTEGER NOT NULL F0, BIGINT NOT NULL F1) NOT NULL");
    f.checkScalar("cast(row(1, 2) AS VARIANT)", "{1, 2}", "VARIANT NOT NULL");
    f.checkNull("cast(NULL AS VARIANT)");

    // Converting a VARIANT back to the original type produces the original value
    f.checkScalar("cast(cast(1 as VARIANT) AS INTEGER)", "1", "INTEGER");
    // no quotes printed, since the result is a VARCHAR
    f.checkScalar("cast(cast(CAST('abc' AS VARCHAR) as VARIANT) AS VARCHAR)", "abc",
        "VARCHAR");
    f.checkScalar("cast(cast(ARRAY[1,2,3] as VARIANT) AS INTEGER ARRAY)", "[1, 2, 3]",
        "INTEGER ARRAY");
    f.checkScalar("cast(cast('abc' as VARIANT) AS VARCHAR)", "abc", "VARCHAR");
    f.checkScalar("cast(cast('abc' as VARIANT) AS CHAR(3))", "abc", "CHAR(3)");

    // Converting a variant to anything that does not match the runtime type returns null
    f.checkScalar("cast(cast(1 as VARIANT) as INTEGER)", "1", "INTEGER");
    f.checkNull("cast(cast(1 as VARIANT) as VARCHAR)");
    f.checkNull("cast(cast(1 as VARIANT) as INT ARRAY)");

    // Arrays of variant objects
    f.checkScalar("ARRAY[CAST(1 AS VARIANT), CAST('abc' AS VARIANT)]", "[1, \"abc\"]",
        "VARIANT NOT NULL ARRAY NOT NULL");
    // Arrays can even contain other arrays
    f.checkScalar("ARRAY[CAST(1 AS VARIANT), CAST('abc' AS VARIANT), CAST(ARRAY[2] AS VARIANT)]",
        "[1, \"abc\", [2]]", "VARIANT NOT NULL ARRAY NOT NULL");
    // Field access in a VARIANT ARRAY
    f.checkScalar("CAST(ARRAY[CAST(1 AS VARIANT), CAST('abc' AS VARIANT)][1] AS INTEGER)", "1",
        "INTEGER");
    // Field access in a VARIANT MAP
    f.checkScalar("cast(MAP['a',1,'b',2] as VARIANT)['a']", "1", "VARIANT");
    // Alternative field access in a VARIANT MAP.  Field names have to be quoted, though
    f.checkScalar("cast(MAP['a',1,'b',2] as VARIANT).\"a\"", "1", "VARIANT");

    // Here is a possible representation of a JSON document { "a": 1, "b": [ { "c": 2.3 }, 5 ] }
    f.checkScalar("MAP["
            + "CAST('a' AS VARIANT), CAST(1 AS VARIANT), "
            + "CAST('b' AS VARIANT), CAST(ARRAY["
            + "CAST(MAP[CAST('c' AS VARIANT), CAST(2.3 AS VARIANT)] AS VARIANT), CAST(5 AS VARIANT)]"
            + " AS VARIANT)]",
        "{\"a\"=1, \"b\"=[{\"c\"=2.3}, 5]}",
        "(VARIANT NOT NULL, VARIANT NOT NULL) MAP NOT NULL");
    // Another possible representation using CHAR instead of VARIANT for MAP keys
    f.checkScalar("MAP["
            + "'a', CAST(1 AS VARIANT), "
            + "'b', CAST(ARRAY["
            + "CAST(MAP['c', CAST(2.3 AS VARIANT)] AS VARIANT), CAST(5 AS VARIANT)]"
            + " AS VARIANT)]",
        "{a=1, b=[{\"c\"=2.3}, 5]}",
        "(CHAR(1) NOT NULL, VARIANT NOT NULL) MAP NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6095">
   * [CALCITE-6095] Arithmetic expression with VARBINARY value causes AssertionFailure</a>.
   */
  @Test public void testVarbitArithmetic() {
    SqlOperatorFixture f = fixture();
    String error = "Cannot apply '\\+' to arguments of type .*\\."
        + " Supported form\\(s\\): '<NUMERIC> \\+ <NUMERIC>'\n"
        + "'<DATETIME_INTERVAL> \\+ <DATETIME_INTERVAL>'\n"
        + "'<DATETIME> \\+ <DATETIME_INTERVAL>'\n"
        + "'<DATETIME_INTERVAL> \\+ <DATETIME>'";
    f.checkFails("SELECT ^x'31' + 0^", error, false);
    f.checkFails("SELECT ^x'31' + x'31'^", error, false);
    f.checkFails("SELECT ^0 + x'31'^", error, false);
    f.checkFails("SELECT ^'a' + x'31'^", error, false);
    f.checkFails("SELECT ^0.0 + x'31'^", error, false);
    f.checkFails("SELECT ^0e0 + x'31'^", error, false);
    f.checkFails("SELECT ^DATE '2000-01-01' + x'31'^", error, false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-4861">[CALCITE-4861]
   * Optimization of chained CAST calls leads to unexpected behavior</a>. */
  @Test void testChainedCast() {
    final SqlOperatorFixture f = fixture();
    f.checkFails("CAST(CAST(CAST(123456 AS TINYINT) AS INT) AS BIGINT)",
        ".*Value 123456 out of range", true);
  }

  @Test void testCase() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CASE, VmName.EXPAND);
    f.checkScalarExact("case when 'a'='a' then 1 end", 1);

    f.checkString("case 2 when 1 then 'a' when 2 then 'bcd' end",
        "bcd", "CHAR(3)");
    f.checkString("case 1 when 1 then 'a' when 2 then 'bcd' end",
        "a  ", "CHAR(3)");
    f.checkString("case 1 when 1 then cast('a' as varchar(1)) "
            + "when 2 then cast('bcd' as varchar(3)) end",
        "a", "VARCHAR(3)");
    f.checkScalarExact("case 2 when 1 then 11.2 "
            + "when 2 then 4.543 else null end",
        "DECIMAL(5, 3)", "4.543");
    f.checkScalarExact("case 1 when 1 then 11.2 "
            + "when 2 then 4.543 else null end",
        "DECIMAL(5, 3)", "11.200");
    f.checkScalarExact("case 'a' when 'a' then 1 end", 1);
    f.checkScalarApprox("case 1 when 1 then 11.2e0 "
            + "when 2 then cast(4 as bigint) else 3 end",
        "DOUBLE NOT NULL", isExactly("11.2"));
    f.checkScalarApprox("case 1 when 1 then 11.2e0 "
            + "when 2 then 4 else null end",
        "DOUBLE", isExactly("11.2"));
    f.checkScalarApprox("case 2 when 1 then 11.2e0 "
            + "when 2 then 4 else null end",
        "DOUBLE", isExactly(4));
    f.checkScalarApprox("case 1 when 1 then 11.2e0 "
            + "when 2 then 4.543 else null end",
        "DOUBLE", isExactly("11.2"));
    f.checkScalarApprox("case 2 when 1 then 11.2e0 "
            + "when 2 then 4.543 else null end",
        "DOUBLE", isExactly("4.543"));
    f.checkNull("case 'a' when 'b' then 1 end");

    // Per spec, 'case x when y then ...'
    // translates to 'case when x = y then ...'
    // so nulls do not match.
    // (Unlike Oracle's 'decode(null, null, ...)', by the way.)
    f.checkString("case cast(null as int)\n"
            + "when cast(null as int) then 'nulls match'\n"
            + "else 'nulls do not match' end",
        "nulls do not match",
        "CHAR(18) NOT NULL");

    f.checkScalarExact("case when 'a'=cast(null as varchar(1)) then 1 "
            + "else 2 end",
        2);

    // equivalent to "nullif('a',cast(null as varchar(1)))"
    f.checkString("case when 'a' = cast(null as varchar(1)) then null "
            + "else 'a' end",
        "a", "CHAR(1)");

    if (TODO) {
      f.checkScalar("case 1 when 1 then row(1,2) when 2 then row(2,3) end",
          "ROW(INTEGER NOT NULL, INTEGER NOT NULL)", "row(1,2)");
      f.checkScalar("case 1 when 1 then row('a','b') "
              + "when 2 then row('ab','cd') end",
          "ROW(CHAR(2) NOT NULL, CHAR(2) NOT NULL)", "row('a ','b ')");
    }

    // multiple values in some cases (introduced in SQL:2011)
    f.checkString("case 1 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "1 or 2           ",
        "CHAR(17) NOT NULL");
    f.checkString("case 2 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "1 or 2           ",
        "CHAR(17) NOT NULL");
    f.checkString("case 3 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "3                ",
        "CHAR(17) NOT NULL");
    f.checkString("case 4 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "none of the above",
        "CHAR(17) NOT NULL");

    // tests with SqlConformance
    final SqlOperatorFixture f2 =
        f.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    f2.checkString("case 2 when 1 then 'a' when 2 then 'bcd' end",
        "bcd", "VARCHAR(3)");
    f2.checkString("case 1 when 1 then 'a' when 2 then 'bcd' end",
        "a", "VARCHAR(3)");
    f2.checkString("case 1 when 1 then cast('a' as varchar(1)) "
            + "when 2 then cast('bcd' as varchar(3)) end",
        "a", "VARCHAR(3)");

    f2.checkString("case cast(null as int) when cast(null as int)"
            + " then 'nulls match'"
            + " else 'nulls do not match' end",
        "nulls do not match",
        "VARCHAR(18) NOT NULL");
    f2.checkScalarExact("case when 'a'=cast(null as varchar(1)) then 1 "
            + "else 2 end",
        2);

    // equivalent to "nullif('a',cast(null as varchar(1)))"
    f2.checkString("case when 'a' = cast(null as varchar(1)) then null "
            + "else 'a' end",
        "a", "CHAR(1)");

    // multiple values in some cases (introduced in SQL:2011)
    f2.checkString("case 1 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "1 or 2", "VARCHAR(17) NOT NULL");
    f2.checkString("case 2 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "1 or 2", "VARCHAR(17) NOT NULL");
    f2.checkString("case 3 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "3", "VARCHAR(17) NOT NULL");
    f2.checkString("case 4 "
            + "when 1, 2 then '1 or 2' "
            + "when 2 then 'not possible' "
            + "when 3, 2 then '3' "
            + "else 'none of the above' "
            + "end",
        "none of the above", "VARCHAR(17) NOT NULL");

    // TODO: Check case with multisets
  }

  @Test void testCaseNull() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CASE, VmName.EXPAND);
    f.checkScalarExact("case when 1 = 1 then 10 else null end", 10);
    f.checkNull("case when 1 = 2 then 10 else null end");
  }

  @Test void testCaseType() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CASE, VmName.EXPAND);
    f.checkType("case 1 when 1 then current_timestamp else null end",
        "TIMESTAMP(0)");
    f.checkType("case 1 when 1 then current_timestamp "
            + "else current_timestamp end",
        "TIMESTAMP(0) NOT NULL");
    f.checkType("case when true then current_timestamp else null end",
        "TIMESTAMP(0)");
    f.checkType("case when true then current_timestamp end",
        "TIMESTAMP(0)");
    f.checkType("case 'x' when 'a' then 3 when 'b' then null else 4.5 end",
        "DECIMAL(11, 1)");
  }

  /**
   * Tests support for JDBC functions.
   *
   * <p>See FRG-97 "Support for JDBC escape syntax is incomplete".
   */
  @Test void testJdbcFn() {
    final SqlOperatorFixture f = fixture();
    f.setFor(new SqlJdbcFunctionCall("dummy"), VmName.EXPAND);

    // There follows one test for each function in appendix C of the JDBC
    // 3.0 specification. The test is 'if-false'd out if the function is
    // not implemented or is broken.

    // Numeric Functions
    f.checkScalar("{fn ABS(-3)}", 3, "INTEGER NOT NULL");
    f.checkScalarApprox("{fn ACOS(0.2)}", "DOUBLE NOT NULL",
        isWithin(1.36943, 0.001));
    f.checkScalarApprox("{fn ASIN(0.2)}", "DOUBLE NOT NULL",
        isWithin(0.20135, 0.001));
    f.checkScalarApprox("{fn ATAN(0.2)}", "DOUBLE NOT NULL",
        isWithin(0.19739, 0.001));
    f.checkScalarApprox("{fn ATAN2(-2, 2)}", "DOUBLE NOT NULL",
        isWithin(-0.78539, 0.001));
    f.checkScalar("{fn CBRT(8)}", 2.0, "DOUBLE NOT NULL");
    f.checkScalar("{fn CEILING(-2.6)}", -2, "DECIMAL(2, 0) NOT NULL");
    f.checkScalarApprox("{fn COS(0.2)}", "DOUBLE NOT NULL",
        isWithin(0.98007, 0.001));
    f.checkScalarApprox("{fn COT(0.2)}", "DOUBLE NOT NULL",
        isWithin(4.93315, 0.001));
    f.checkScalarApprox("{fn DEGREES(-1)}", "DOUBLE NOT NULL",
        isWithin(-57.29578, 0.001));

    f.checkScalarApprox("{fn EXP(2)}", "DOUBLE NOT NULL",
        isWithin(7.389, 0.001));
    f.checkScalar("{fn FLOOR(2.6)}", 2, "DECIMAL(2, 0) NOT NULL");
    f.checkScalarApprox("{fn LOG(10)}", "DOUBLE NOT NULL",
        isWithin(2.30258, 0.001));
    f.checkScalarApprox("{fn LOG10(100)}", "DOUBLE NOT NULL", isExactly(2));
    f.checkScalar("{fn MOD(19, 4)}", 3, "INTEGER NOT NULL");
    f.checkScalarApprox("{fn PI()}", "DOUBLE NOT NULL",
        isWithin(3.14159, 0.0001));
    f.checkScalarApprox("{fn POWER(2, 3)}", "DOUBLE NOT NULL",
        isWithin(8.0, 0.001));
    f.checkScalarApprox("{fn RADIANS(90)}", "DOUBLE NOT NULL",
        isWithin(1.57080, 0.001));
    f.checkScalarApprox("{fn RAND(42)}", "DOUBLE NOT NULL",
        isWithin(0.63708, 0.001));
    f.checkScalar("{fn ROUND(1251, -2)}", 1300, "INTEGER NOT NULL");
    f.checkScalar("{fn ROUND(1251)}", 1251, "INTEGER NOT NULL");
    f.checkScalar("{fn SIGN(-1)}", -1, "INTEGER NOT NULL");
    f.checkScalarApprox("{fn SIN(0.2)}", "DOUBLE NOT NULL",
        isWithin(0.19867, 0.001));
    f.checkScalarApprox("{fn SQRT(4.2)}", "DOUBLE NOT NULL",
        isWithin(2.04939, 0.001));
    f.checkScalarApprox("{fn TAN(0.2)}", "DOUBLE NOT NULL",
        isWithin(0.20271, 0.001));
    f.checkScalar("{fn TRUNCATE(12.34, 1)}", 12.3, "DECIMAL(4, 2) NOT NULL");
    f.checkScalar("{fn TRUNCATE(-12.34, -1)}", -10, "DECIMAL(4, 2) NOT NULL");

    // String Functions
    f.checkScalar("{fn ASCII('a')}", 97, "INTEGER NOT NULL");
    f.checkScalar("{fn ASCII('ABC')}", "65", "INTEGER NOT NULL");
    f.checkNull("{fn ASCII(cast(null as varchar(1)))}");

    f.checkScalar("{fn CHAR(97)}", "a", "CHAR(1)");

    f.checkScalar("{fn CONCAT('foo', 'bar')}", "foobar", "CHAR(6) NOT NULL");

    f.checkScalar("{fn DIFFERENCE('Miller', 'miller')}", "4",
        "INTEGER NOT NULL");
    f.checkNull("{fn DIFFERENCE('muller', cast(null as varchar(1)))}");

    f.checkString("{fn REVERSE('abc')}", "cba", "VARCHAR(3) NOT NULL");
    f.checkNull("{fn REVERSE(cast(null as varchar(1)))}");

    f.checkString("{fn LEFT('abcd', 3)}", "abc", "VARCHAR(4) NOT NULL");
    f.checkString("{fn LEFT('abcd', 4)}", "abcd", "VARCHAR(4) NOT NULL");
    f.checkString("{fn LEFT('abcd', 5)}", "abcd", "VARCHAR(4) NOT NULL");
    f.checkNull("{fn LEFT(cast(null as varchar(1)), 3)}");
    f.checkString("{fn RIGHT('abcd', 3)}", "bcd", "VARCHAR(4) NOT NULL");
    f.checkString("{fn RIGHT('abcd', 4)}", "abcd", "VARCHAR(4) NOT NULL");
    f.checkString("{fn RIGHT('abcd', 5)}", "abcd", "VARCHAR(4) NOT NULL");
    f.checkNull("{fn RIGHT(cast(null as varchar(1)), 3)}");

    // REVIEW: is this result correct? I think it should be "abcCdef"
    f.checkScalar("{fn INSERT('abc', 1, 2, 'ABCdef')}",
        "ABCdefc", "VARCHAR(9) NOT NULL");
    f.checkScalar("{fn LCASE('foo' || 'bar')}",
        "foobar", "CHAR(6) NOT NULL");
    if (false) {
      f.checkScalar("{fn LENGTH(string)}", null, "");
    }
    f.checkScalar("{fn LOCATE('ha', 'alphabet')}", 4, "INTEGER NOT NULL");

    f.checkScalar("{fn LOCATE('ha', 'alphabet', 6)}", 0, "INTEGER NOT NULL");

    f.checkScalar("{fn LTRIM(' xxx  ')}", "xxx  ", "VARCHAR(6) NOT NULL");

    f.checkScalar("{fn REPEAT('a', -100)}", "", "VARCHAR NOT NULL");
    f.checkNull("{fn REPEAT('abc', cast(null as integer))}");
    f.checkNull("{fn REPEAT(cast(null as varchar(1)), cast(null as integer))}");

    f.checkString("{fn REPLACE('JACK and JUE','J','BL')}",
        "BLACK and BLUE", "VARCHAR NOT NULL");

    // REPLACE returns NULL in Oracle but not in Postgres or in Calcite.
    // When [CALCITE-815] is implemented and SqlConformance#emptyStringIsNull is
    // enabled, it will return empty string as NULL.
    f.checkString("{fn REPLACE('ciao', 'ciao', '')}", "",
        "VARCHAR NOT NULL");

    f.checkString("{fn REPLACE('hello world', 'o', '')}", "hell wrld",
        "VARCHAR NOT NULL");

    f.checkNull("{fn REPLACE(cast(null as varchar(5)), 'ciao', '')}");
    f.checkNull("{fn REPLACE('ciao', cast(null as varchar(3)), 'zz')}");
    f.checkNull("{fn REPLACE('ciao', 'bella', cast(null as varchar(3)))}");


    f.checkScalar(
        "{fn RTRIM(' xxx  ')}",
        " xxx",
        "VARCHAR(6) NOT NULL");

    f.checkScalar("{fn SOUNDEX('Miller')}", "M460", "VARCHAR(4) NOT NULL");
    f.checkNull("{fn SOUNDEX(cast(null as varchar(1)))}");

    f.checkScalar("{fn SPACE(-100)}", "", "VARCHAR NOT NULL");
    f.checkNull("{fn SPACE(cast(null as integer))}");

    f.checkScalar(
        "{fn SUBSTRING('abcdef', 2, 3)}",
        "bcd",
        "VARCHAR(6) NOT NULL");
    f.checkScalar("{fn UCASE('xxx')}", "XXX", "CHAR(3) NOT NULL");

    // Time and Date Functions
    f.checkType("{fn CURDATE()}", "DATE NOT NULL");
    f.checkType("{fn CURTIME()}", "TIME(0) NOT NULL");
    f.checkScalar("{fn DAYNAME(DATE '2014-12-10')}",
        // Day names in root locale changed from long to short in JDK 9
        TestUtil.getJavaMajorVersion() <= 8 ? "Wednesday" : "Wed",
        "VARCHAR(2000) NOT NULL");
    f.checkScalar("{fn DAYOFMONTH(DATE '2014-12-10')}", 10,
        "BIGINT NOT NULL");
    f.checkScalar("{fn DAYOFWEEK(DATE '2014-12-10')}", "4", "BIGINT NOT NULL");
    f.checkScalar("{fn DAYOFYEAR(DATE '2014-12-10')}", "344",
        "BIGINT NOT NULL");
    f.checkScalar("{fn HOUR(TIMESTAMP '2014-12-10 12:34:56')}", 12,
        "BIGINT NOT NULL");
    f.checkScalar("{fn MINUTE(TIMESTAMP '2014-12-10 12:34:56')}", 34,
        "BIGINT NOT NULL");
    f.checkScalar("{fn MONTH(DATE '2014-12-10')}", 12, "BIGINT NOT NULL");
    f.checkScalar("{fn MONTHNAME(DATE '2014-12-10')}",
        // Month names in root locale changed from long to short in JDK 9
        TestUtil.getJavaMajorVersion() <= 8 ? "December" : "Dec",
        "VARCHAR(2000) NOT NULL");
    f.checkType("{fn NOW()}", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("{fn QUARTER(DATE '2014-12-10')}", "4",
        "BIGINT NOT NULL");
    f.checkScalar("{fn SECOND(TIMESTAMP '2014-12-10 12:34:56')}", 56,
        "BIGINT NOT NULL");
    f.checkScalar("{fn TIMESTAMPADD(HOUR, 5,"
            + " TIMESTAMP '2014-03-29 12:34:56')}",
        "2014-03-29 17:34:56", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("{fn TIMESTAMPDIFF(HOUR,"
        + " TIMESTAMP '2014-03-29 12:34:56',"
        + " TIMESTAMP '2014-03-29 12:34:56')}", "0", "INTEGER NOT NULL");
    f.checkScalar("{fn TIMESTAMPDIFF(MONTH,"
        + " TIMESTAMP '2019-09-01 00:00:00',"
        + " TIMESTAMP '2020-03-01 00:00:00')}", "6", "INTEGER NOT NULL");

    f.checkScalar("{fn WEEK(DATE '2014-12-10')}", "50", "BIGINT NOT NULL");
    f.checkScalar("{fn YEAR(DATE '2014-12-10')}", 2014, "BIGINT NOT NULL");

    // System Functions
    f.checkType("{fn DATABASE()}", "VARCHAR(2000) NOT NULL");
    f.checkString("{fn IFNULL('a', 'b')}", "a", "CHAR(1) NOT NULL");
    f.checkString("{fn USER()}", "sa", "VARCHAR(2000) NOT NULL");


    // Conversion Functions
    // Legacy JDBC style
    f.checkScalar("{fn CONVERT('123', INTEGER)}", 123, "INTEGER NOT NULL");
    // ODBC/JDBC style
    f.checkScalar("{fn CONVERT('123', SQL_INTEGER)}", 123,
        "INTEGER NOT NULL");
    f.checkScalar("{fn CONVERT(INTERVAL '1' DAY, SQL_INTERVAL_DAY_TO_SECOND)}",
        "+1 00:00:00.000000", "INTERVAL DAY TO SECOND NOT NULL");

  }

  @Test void testChar() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.CHAR, VM_JAVA);
    f0.checkFails("^char(97)^",
        "No match found for function signature CHAR\\(<NUMERIC>\\)", false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("char(null)", isNullValue(), "CHAR(1)");
      f.checkScalar("char(-1)", isNullValue(), "CHAR(1)");
      f.checkScalar("char(97)", "a", "CHAR(1)");
      f.checkScalar("char(48)", "0", "CHAR(1)");
      f.checkScalar("char(0)", String.valueOf('\u0000'), "CHAR(1)");
      f.checkFails("^char(97.1)^",
          "Cannot apply 'CHAR' to arguments of type 'CHAR\\(<DECIMAL\\(3, 1\\)>\\)'\\. "
              + "Supported form\\(s\\): 'CHAR\\(<INTEGER>\\)'",
          false);
      f.checkNull("char(null)");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.MYSQL, SqlLibrary.SPARK);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testChr() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.CHR, VM_JAVA);
    f0.checkFails("^chr(97.1)^",
        "No match found for function signature CHR\\(<NUMERIC>\\)", false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("chr(97)", "a", "CHAR(1) NOT NULL");
      f.checkScalar("chr(48)", "0", "CHAR(1) NOT NULL");
      f.checkScalar("chr(0)", String.valueOf('\u0000'), "CHAR(1) NOT NULL");
      f.checkNull("chr(null)");
      // Test case for [CALCITE-6707] Type inference for CHR function is wrong
      // https://issues.apache.org/jira/browse/CALCITE-6707
      f.checkType("chr(CAST(null AS INTEGER))", "CHAR(1)");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.ORACLE, SqlLibrary.POSTGRESQL, SqlLibrary.REDSHIFT);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testCodePointsToBytes() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.CODE_POINTS_TO_BYTES, VM_JAVA)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkFails("^code_points_to_bytes('abc')^",
        "Cannot apply 'CODE_POINTS_TO_BYTES' to arguments of type "
            + "'CODE_POINTS_TO_BYTES\\(<CHAR\\(3\\)>\\)'\\. "
            + "Supported form\\(s\\): CODE_POINTS_TO_BYTES\\(<INTEGER ARRAY>\\)",
        false);
    f.checkFails("^code_points_to_bytes(array['abc'])^",
        "Cannot apply 'CODE_POINTS_TO_BYTES' to arguments of type "
            + "'CODE_POINTS_TO_BYTES\\(<CHAR\\(3\\) ARRAY>\\)'\\. "
            + "Supported form\\(s\\): CODE_POINTS_TO_BYTES\\(<INTEGER ARRAY>\\)",
        false);

    f.checkFails("code_points_to_bytes(array[-1])",
        "Input arguments of CODE_POINTS_TO_BYTES out of range: -1;"
            + " should be in the range of \\[0, 255\\]", true);
    f.checkFails("code_points_to_bytes(array[2147483648, 1])",
        "Input arguments of CODE_POINTS_TO_BYTES out of range: 2147483648;"
            + " should be in the range of \\[0, 255\\]", true);

    f.checkString("code_points_to_bytes(array[65, 66, 67, 68])", "41424344", "VARBINARY NOT NULL");
    f.checkString("code_points_to_bytes(array[255, 254, 65, 64])", "fffe4140",
        "VARBINARY NOT NULL");
    f.checkString("code_points_to_bytes(array[1+2, 3, 4])", "030304",
        "VARBINARY NOT NULL");

    f.checkNull("code_points_to_bytes(null)");
    f.checkNull("code_points_to_bytes(array[null])");
    f.checkNull("code_points_to_bytes(array[65, null])");
  }

  @Test void testCodePointsToString() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.CODE_POINTS_TO_STRING, VM_JAVA)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkFails("^code_points_to_string('abc')^",
        "Cannot apply 'CODE_POINTS_TO_STRING' to arguments of type "
            + "'CODE_POINTS_TO_STRING\\(<CHAR\\(3\\)>\\)'\\. "
            + "Supported form\\(s\\): CODE_POINTS_TO_STRING\\(<INTEGER ARRAY>\\)",
        false);
    f.checkFails("^code_points_to_string(array['abc'])^",
        "Cannot apply 'CODE_POINTS_TO_STRING' to arguments of type "
            + "'CODE_POINTS_TO_STRING\\(<CHAR\\(3\\) ARRAY>\\)'\\. "
            + "Supported form\\(s\\): CODE_POINTS_TO_STRING\\(<INTEGER ARRAY>\\)",
        false);

    f.checkFails("code_points_to_string(array[-1])",
        "Input arguments of CODE_POINTS_TO_STRING out of range: -1;"
            + " should be in the range of \\[0, 0xD7FF\\] and \\[0xE000, 0x10FFFF\\]", true);
    f.checkFails("code_points_to_string(array[2147483648, 1])",
        "Input arguments of CODE_POINTS_TO_STRING out of range: 2147483648;"
            + " should be in the range of \\[0, 0xD7FF\\] and \\[0xE000, 0x10FFFF\\]", true);

    f.checkString("code_points_to_string(array[65, 66, 67, 68])", "ABCD",
        "VARCHAR NOT NULL");
    f.checkString("code_points_to_string(array[255, 254, 1024, 70000])", "ÿþЀ\uD804\uDD70",
        "VARCHAR NOT NULL");
    f.checkString("code_points_to_string(array[1+2, 3])", "\u0003\u0003",
        "VARCHAR NOT NULL");

    f.checkNull("code_points_to_string(null)");
    f.checkNull("code_points_to_string(array[null])");
    f.checkNull("code_points_to_string(array[65, null])");
  }

  @Test void testToCodePoints() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.TO_CODE_POINTS, VM_JAVA)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkNull("to_code_points(null)");
    f.checkFails("^to_code_points(array[1,2,3])^",
        "Cannot apply 'TO_CODE_POINTS' to arguments of type "
            + "'TO_CODE_POINTS\\(<INTEGER ARRAY>\\)'\\. "
            + "Supported form\\(s\\): 'TO_CODE_POINTS\\(<STRING>\\)'\n"
            + "'TO_CODE_POINTS\\(<BINARY>\\)'", false);

    f.checkScalar("to_code_points(_UTF8'ÿþЀ\uD804\uDD70A')", "[255, 254, 1024, 70000, 65]",
        "INTEGER NOT NULL ARRAY");
    f.checkScalar("to_code_points('ABCD')", "[65, 66, 67, 68]",
        "INTEGER NOT NULL ARRAY");
    f.checkScalar("to_code_points(x'11223344')", "[17, 34, 51, 68]",
        "INTEGER NOT NULL ARRAY");
    f.checkScalar("to_code_points(code_points_to_string(array[255, 254, 1024, 70000, 65]))",
        "[255, 254, 1024, 70000, 65]", "INTEGER NOT NULL ARRAY");
    f.checkScalar("to_code_points(code_points_to_bytes(array[64, 65, 66, 67]))",
        "[64, 65, 66, 67]", "INTEGER NOT NULL ARRAY");

    f.checkNull("to_code_points(null)");
    f.checkNull("to_code_points('')");
    f.checkNull("to_code_points(x'')");
  }

  @Test void testSelect() {
    final SqlOperatorFixture f = fixture();
    f.check("select * from (values(1))", SqlTests.INTEGER_TYPE_CHECKER, 1);

    // Check return type on scalar sub-query in select list.  Note return
    // type is always nullable even if sub-query select value is NOT NULL.
    // Bug FRG-189 causes this test to fail only in SqlOperatorTest; not
    // in subtypes.
    if (Bug.FRG189_FIXED) {
      f.checkType("SELECT *,\n"
              + "  (SELECT * FROM (VALUES(1)))\n"
              + "FROM (VALUES(2))",
          "RecordType(INTEGER NOT NULL EXPR$0, INTEGER EXPR$1) NOT NULL");
      f.checkType("SELECT *,\n"
              + "  (SELECT * FROM (VALUES(CAST(10 as BIGINT))))\n"
              + "FROM (VALUES(CAST(10 as bigint)))",
          "RecordType(BIGINT NOT NULL EXPR$0, BIGINT EXPR$1) NOT NULL");
      f.checkType("SELECT *,\n"
              + "  (SELECT * FROM (VALUES(10.5)))\n"
              + "FROM (VALUES(10.5))",
          "RecordType(DECIMAL(3, 1) NOT NULL EXPR$0, DECIMAL(3, 1) EXPR$1) NOT NULL");
      f.checkType("SELECT *,\n"
              + "  (SELECT * FROM (VALUES('this is a char')))\n"
              + "FROM (VALUES('this is a char too'))",
          "RecordType(CHAR(18) NOT NULL EXPR$0, CHAR(14) EXPR$1) NOT NULL");
      f.checkType("SELECT *,\n"
              + "  (SELECT * FROM (VALUES(true)))\n"
              + "FROM (values(false))",
          "RecordType(BOOLEAN NOT NULL EXPR$0, BOOLEAN EXPR$1) NOT NULL");
      f.checkType(" SELECT *,\n"
              + "  (SELECT * FROM (VALUES(cast('abcd' as varchar(10)))))\n"
              + "FROM (VALUES(CAST('abcd' as varchar(10))))",
          "RecordType(VARCHAR(10) NOT NULL EXPR$0, VARCHAR(10) EXPR$1) NOT NULL");
      f.checkType("SELECT *,\n"
              + "  (SELECT * FROM (VALUES(TIMESTAMP '2006-01-01 12:00:05')))\n"
              + "FROM (VALUES(TIMESTAMP '2006-01-01 12:00:05'))",
          "RecordType(TIMESTAMP(0) NOT NULL EXPR$0, TIMESTAMP(0) EXPR$1) NOT NULL");
    }
  }

  @Test void testLiteralChain() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LITERAL_CHAIN, VM_EXPAND);
    f.checkString("'buttered'\n"
            + "' toast'",
        "buttered toast",
        "CHAR(14) NOT NULL");
    f.checkString("'corned'\n"
            + "' beef'\n"
            + "' on'\n"
            + "' rye'",
        "corned beef on rye",
        "CHAR(18) NOT NULL");
    f.checkString("_latin1'Spaghetti'\n"
            + "' all''Amatriciana'",
        "Spaghetti all'Amatriciana",
        "CHAR(25) NOT NULL");
    f.checkBoolean("x'1234'\n"
        + "'abcd' = x'1234abcd'", true);
    f.checkBoolean("x'1234'\n"
        + "'' = x'1234'", true);
    f.checkBoolean("x''\n"
        + "'ab' = x'ab'", true);
  }

  @Test void testComplexLiteral() {
    final SqlOperatorFixture f = fixture();
    f.check("select 2 * 2 * x from (select 2 as x)",
        SqlTests.INTEGER_TYPE_CHECKER, 8);
    f.check("select 1 * 2 * 3 * x from (select 2 as x)",
        SqlTests.INTEGER_TYPE_CHECKER, 12);
    f.check("select 1 + 2 + 3 + 4 + x from (select 2 as x)",
        SqlTests.INTEGER_TYPE_CHECKER, 12);
  }

  @Test void testRow() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ROW);
  }

  @Test void testAndOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.AND, VmName.EXPAND);
    f.checkBoolean("true and false", false);
    f.checkBoolean("true and true", true);
    f.checkBoolean("cast(null as boolean) and false", false);
    f.checkBoolean("false and cast(null as boolean)", false);
    f.checkNull("cast(null as boolean) and true");
    f.checkBoolean("true and (not false)", true);
  }

  @Test void testAndOperator2() {
    final SqlOperatorFixture f = fixture();
    f.checkBoolean("case when false then unknown else true end and true",
        true);
    f.checkBoolean("case when false then cast(null as boolean) "
            + "else true end and true",
        true);
    f.checkBoolean("case when false then null else true end and true",
        true);
  }

  @Test void testAndOperatorLazy() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.AND, VmName.EXPAND);

    // lazy eval returns FALSE;
    // eager eval executes RHS of AND and throws;
    // both are valid
    f.check("values 1 > 2 and sqrt(-4) = -2",
        SqlTests.BOOLEAN_TYPE_CHECKER, SqlTests.ANY_PARAMETER_CHECKER,
        new ValueOrExceptionResultChecker(false, INVALID_ARG_FOR_POWER,
            CODE_2201F));
  }

  @Test void testConcatOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CONCAT, VmName.EXPAND);
    f.checkString(" 'a'||'b' ", "ab", "CHAR(2) NOT NULL");
    f.checkNull(" 'a' || cast(null as char(2)) ");
    f.checkNull(" cast(null as char(2)) || 'b' ");
    f.checkNull(" cast(null as char(1)) || cast(null as char(2)) ");

    f.checkString(" x'fe'||x'df' ", "fedf", "BINARY(2) NOT NULL");
    f.checkString(" cast('fe' as char(2)) || cast('df' as varchar)",
        "fedf", "VARCHAR NOT NULL");
    // Precision is larger than VARCHAR allows, so result is unbounded
    f.checkString(" cast('fe' as char(2)) || cast('df' as varchar(65535))",
        "fedf", "VARCHAR NOT NULL");
    f.checkString(" cast('fe' as char(2)) || cast('df' as varchar(33333))",
        "fedf", "VARCHAR(33335) NOT NULL");
    f.checkNull("x'ff' || cast(null as varbinary)");
    f.checkNull(" cast(null as ANY) || cast(null as ANY) ");
    f.checkString("cast('a' as varchar) || cast('b' as varchar) "
        + "|| cast('c' as varchar)", "abc", "VARCHAR NOT NULL");

    f.checkScalar("array[1, 2] || array[2, 3]", "[1, 2, 2, 3]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array[1, 2] || array[2, null]", "[1, 2, 2, null]",
        "INTEGER ARRAY NOT NULL");
    f.checkScalar("array['hello', 'world'] || array['!'] || "
            + "array[cast(null as char)]",
        "[hello, world, !, null]", "CHAR(5) ARRAY NOT NULL");
    f.checkNull("cast(null as integer array) || array[1]");
  }

  @Test void testConcatFunc() {
    final SqlOperatorFixture f = fixture();
    checkConcatFunc(f.withLibrary(SqlLibrary.MYSQL));
    checkConcatFunc(f.withLibrary(SqlLibrary.BIG_QUERY));
    checkConcatFuncWithNull(f.withLibrary(SqlLibrary.POSTGRESQL));
    checkConcatFuncWithNull(f.withLibrary(SqlLibrary.MSSQL));
    checkConcat2Func(f.withLibrary(SqlLibrary.ORACLE));
    checkConcat2Func(f.withLibrary(SqlLibrary.REDSHIFT));
  }

  private static void checkConcatFunc(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.CONCAT_FUNCTION);
    f.checkString("concat('a', 'b', 'c')", "abc", "VARCHAR(3) NOT NULL");
    f.checkString("concat(cast('a' as varchar), cast('b' as varchar), "
        + "cast('c' as varchar))", "abc", "VARCHAR NOT NULL");
    f.checkNull("concat('a', 'b', cast(null as char(2)))");
    f.checkNull("concat(cast(null as ANY), 'b', cast(null as char(2)))");
    f.checkString("concat('', '', 'a')", "a", "VARCHAR(1) NOT NULL");
    f.checkString("concat('', '', '')", "", "VARCHAR(0) NOT NULL");
    f.checkFails("^concat()^", INVALID_ARGUMENTS_NUMBER, false);
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6518">
   * ClassCastException during validation when loading multiple libraries</a>. */
  @Test void testManyLibraries() {
    SqlOperatorFixture f =
        fixture().withLibraries(SqlLibrary.STANDARD, SqlLibrary.MYSQL, SqlLibrary.POSTGRESQL);
    f.checkScalar("substr('a', 1, 2)", "a", "VARCHAR(1) NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5771">[CALCITE-5771]
   * Apply two different NULL semantics for CONCAT function(enabled in MySQL,
   * Postgres, BigQuery and MSSQL)</a>. */
  private static void checkConcatFuncWithNull(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.CONCAT_FUNCTION_WITH_NULL);
    f.checkString("concat('a', 'b', 'c')", "abc", "VARCHAR(3) NOT NULL");
    f.checkString("concat(cast('a' as varchar), cast('b' as varchar), "
            + "cast('c' as varchar))", "abc", "VARCHAR NOT NULL");
    f.checkString("concat('a', 'b', cast(null as char(2)))", "ab", "VARCHAR(4) NOT NULL");
    f.checkString("concat(cast(null as ANY), 'b', cast(null as char(2)))", "b", "VARCHAR NOT NULL");
    f.checkString("concat('', '', 'a')", "a", "VARCHAR(1) NOT NULL");
    f.checkString("concat('', '', '')", "", "VARCHAR(0) NOT NULL");
    f.checkString("concat(null, null, null)", "", "VARCHAR NOT NULL");
    f.checkString("concat('', null, '')", "", "VARCHAR NOT NULL");
    f.checkFails("^concat()^", INVALID_ARGUMENTS_NUMBER, false);
  }

  private static void checkConcat2Func(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.CONCAT2);
    f.checkString("concat(cast('fe' as char(2)), cast('df' as varchar(65535)))",
        "fedf", "VARCHAR NOT NULL");
    f.checkString("concat(cast('fe' as char(2)), cast('df' as varchar))",
        "fedf", "VARCHAR NOT NULL");
    f.checkString("concat(cast('fe' as char(2)), cast('df' as varchar(33333)))",
        "fedf", "VARCHAR(33335) NOT NULL");
    f.checkString("concat('', '')", "", "VARCHAR(0) NOT NULL");
    f.checkString("concat('', 'a')", "a", "VARCHAR(1) NOT NULL");
    f.checkString("concat('a', 'b')", "ab", "VARCHAR(2) NOT NULL");
    // treat null value as empty string
    f.checkString("concat('a', cast(null as varchar))", "a", "VARCHAR NOT NULL");
    f.checkString("concat(null, 'b')", "b", "VARCHAR NOT NULL");
    // return null when both arguments are null
    f.checkNull("concat(null, cast(null as varchar))");
    f.checkNull("concat(null, null)");
    f.checkFails("^concat('a', 'b', 'c')^", INVALID_ARGUMENTS_NUMBER, false);
    f.checkFails("^concat('a')^", INVALID_ARGUMENTS_NUMBER, false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5741">[CALCITE-5741]
   * Add CONCAT_WS function (enabled in MSSQL, MySQL, Postgres
   * libraries)</a>. */
  @Test void testConcatWSFunc() {
    final SqlOperatorFixture f = fixture();
    checkConcatWithSeparator(f.withLibrary(SqlLibrary.MYSQL));
    checkConcatWithSeparator(f.withLibrary(SqlLibrary.POSTGRESQL));
    checkConcatWithSeparatorInPostgres(f.withLibrary(SqlLibrary.POSTGRESQL));
    checkConcatWithSeparatorInMSSQL(f.withLibrary(SqlLibrary.MSSQL));
  }

  private static void checkConcatWithSeparator(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.CONCAT_WS);
    f.checkString("concat_ws(',', 'a')", "a", "VARCHAR(1) NOT NULL");
    f.checkString("concat_ws(',', 'a', 'b', null, 'c')", "a,b,c",
        "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', cast('a' as varchar), cast('b' as varchar))",
        "a,b", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', cast('a' as varchar(2)), cast('b' as varchar(1)))",
        "a,b", "VARCHAR(4) NOT NULL");
    f.checkString("concat_ws(',', '', '', '')", ",,", "VARCHAR(2) NOT NULL");
    f.checkString("concat_ws(',', null, null, null)", "", "VARCHAR NOT NULL");
    // returns null if the separator is null
    f.checkNull("concat_ws(null, 'a', 'b')");
    f.checkNull("concat_ws(null, null, null)");
    f.checkFails("^concat_ws(',')^", INVALID_ARGUMENTS_NUMBER, false);
    // if the separator is empty string, it's equivalent to CONCAT
    f.checkString("concat_ws('', cast('a' as varchar(2)), cast('b' as varchar(1)))",
        "ab", "VARCHAR(3) NOT NULL");
    f.checkString("concat_ws('', '', '', '')", "", "VARCHAR(0) NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6450">[CALCITE-6450]
   * Postgres CONCAT_WS function </a>. */
  private static void checkConcatWithSeparatorInPostgres(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.CONCAT_WS_POSTGRESQL);
    f.checkFails("^concat_ws(array['a'])^", INVALID_ARGUMENTS_NUMBER, false);
    f.checkFails("^concat_ws(array['a'], 'd')^", INVALID_ARGUMENTS_TYPE_VALIDATION_ERROR, false);
    f.checkString("concat_ws(',', 'a', array['b', 'c'])", "a,[b, c]", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', 'a', 1, 4)", "a,1,4", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', 'a', array['b', 'c'], null, array[1,2])",
        "a,[b, c],[1, 2]", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', DATE '1945-02-24')",
        "1945-02-24", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', 'a', array['b', 'c'], DATE '1945-02-24')",
        "a,[b, c],1945-02-24", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', timestamp '2024-07-06 12:15:48.678')",
        "2024-07-06 12:15:48", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', time '12:34:56', time '13:00:00', 2, 'abc')",
        "12:34:56,13:00:00,2,abc", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', null, null)", "", "VARCHAR NOT NULL");
    f.checkNull("concat_ws(null, 'a', array['b', 'c'])");
  }

  private static void checkConcatWithSeparatorInMSSQL(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.CONCAT_WS_MSSQL);
    f.checkString("concat_ws(',', 'a', 'b', null, 'c')", "a,b,c",
        "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', cast('a' as varchar), cast('b' as varchar))",
        "a,b", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', cast('a' as varchar(2)), cast('b' as varchar(1)))",
        "a,b", "VARCHAR(4) NOT NULL");
    f.checkString("concat_ws(',', '', '', '')", ",,", "VARCHAR(2) NOT NULL");
    f.checkString("concat_ws(',', null, null, null)", "", "VARCHAR NOT NULL");
    f.checkString("concat_ws(null, 'a', 'b')", "ab", "VARCHAR NOT NULL");
    f.checkString("concat_ws(null, null, null)", "", "VARCHAR NOT NULL");
    f.checkFails("^concat_ws(',')^", INVALID_ARGUMENTS_NUMBER, false);
    f.checkFails("^concat_ws(',', 'a')^", INVALID_ARGUMENTS_NUMBER, false);
    // if the separator is empty string, it's equivalent to CONCAT
    f.checkString("concat_ws('', cast('a' as varchar(2)), cast('b' as varchar(1)))",
        "ab", "VARCHAR(3) NOT NULL");
    f.checkString("concat_ws('', '', '', '')", "", "VARCHAR(0) NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6446">[CALCITE-6446]
   * Add CONCAT_WS function (enabled in Spark library)</a>. */
  @Test void testConcatWSFuncInSpark() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.CONCAT_WS_SPARK)
        .withLibrary(SqlLibrary.SPARK);
    f.checkString("concat_ws(',')", "", "VARCHAR(0) NOT NULL");
    f.checkString("concat_ws(',', 'a')", "a", "VARCHAR(1) NOT NULL");
    f.checkString("concat_ws(',', 'a', 'b', null, 'c')", "a,b,c",
        "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', cast('a' as varchar), cast('b' as varchar))",
        "a,b", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', cast('a' as varchar(2)), cast('b' as varchar(1)))",
        "a,b", "VARCHAR(4) NOT NULL");
    f.checkString("concat_ws(',', '', '', '')", ",,", "VARCHAR(2) NOT NULL");
    f.checkString("concat_ws(',', null, null, null)", "", "VARCHAR NOT NULL");
    // returns null if the separator is null
    f.checkNull("concat_ws(null, 'a', 'b')");
    f.checkNull("concat_ws(null, null, null)");
    f.checkString("concat_ws(',')", "", "VARCHAR(0) NOT NULL");
    // if the separator is empty string, it's equivalent to CONCAT
    f.checkString("concat_ws('', cast('a' as varchar(2)), cast('b' as varchar(1)))",
        "ab", "VARCHAR(3) NOT NULL");
    f.checkString("concat_ws('', '', '', '')", "", "VARCHAR(0) NOT NULL");
    f.checkString("concat_ws(',', array('a', 'b'))", "a,b", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', null)", "", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', array('a', null, 'b'))", "a,b", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', 1, 'b')", "1,b", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', 100.0, 'b')", "100.0,b", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', 'a', 'b', array('c'))", "a,b,c", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', 'a', 'b', array('c'), array('d'))", "a,b,c,d",
        "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', '100', 'b', array('c'))", "100,b,c", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', 100, 'b', array('c'))", "100,b,c", "VARCHAR NOT NULL");
    f.checkString("concat_ws(',', 'a', 'b', array('100'))", "a,b,100", "VARCHAR NOT NULL");
    f.checkFails("^concat_ws(',', 'a', 'b', array(100))^",
        INVALID_ARGUMENTS_TYPE_VALIDATION_ERROR, false);
    f.checkFails("^concat_ws(array('a', 'b'))^",
        INVALID_ARGUMENTS_TYPE_VALIDATION_ERROR, false);
    f.checkFails("^concat_ws()^", INVALID_ARGUMENTS_NUMBER, false);
  }

  @Test void testModOperator() {
    // "%" is allowed under BIG_QUERY, MYSQL_5 SQL conformance levels
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlStdOperatorTable.PERCENT_REMAINDER);
    final List<SqlConformanceEnum> conformances =
        list(SqlConformanceEnum.BIG_QUERY, SqlConformanceEnum.MYSQL_5);
    f0.forEachConformance(conformances, this::checkModOperator);
    f0.forEachConformance(conformances, this::checkModPrecedence);
    f0.forEachConformance(conformances, this::checkModOperatorNull);
    f0.forEachConformance(conformances, this::checkModOperatorDivByZero);
  }

  void checkModOperator(SqlOperatorFixture f) {
    f.checkScalarExact("4%2", 0);
    f.checkScalarExact("8%5", 3);
    f.checkScalarExact("-12%7", -5);
    f.checkScalarExact("-12%-7", -5);
    f.checkScalarExact("12%-7", 5);
    f.checkScalarExact("cast(12 as tinyint) % cast(-7 as tinyint)",
        "TINYINT NOT NULL", "5");
    f.checkScalarExact("cast(9 as decimal(2, 0)) % 7",
        "INTEGER NOT NULL", "2");
    f.checkScalarExact("7 % cast(9 as decimal(2, 0))",
        "DECIMAL(2, 0) NOT NULL", "7");
    f.checkScalarExact("cast(-9 as decimal(2, 0)) % cast(7 as decimal(1, 0))",
        "DECIMAL(1, 0) NOT NULL", "-2");
  }

  void checkModPrecedence(SqlOperatorFixture f) {
    f.checkScalarExact("1 + 5 % 3 % 4 * 14 % 17", 12);
    f.checkScalarExact("(1 + 5 % 3) % 4 + 14 % 17", 17);
  }

  void checkModOperatorNull(SqlOperatorFixture f) {
    f.checkNull("cast(null as integer) % 2");
    f.checkNull("4 % cast(null as tinyint)");
    f.checkNull("4 % cast(null as decimal(12,0))");
  }

  void checkModOperatorDivByZero(SqlOperatorFixture f) {
    // The extra CASE expression is to fool Janino.  It does constant
    // reduction and will throw the divide by zero exception while
    // compiling the expression.  The test framework would then issue
    // unexpected exception occurred during "validation".  You cannot
    // submit as non-runtime because the janino exception does not have
    // error position information and the framework is unhappy with that.
    f.checkFails("3 % case 'a' when 'a' then 0 end",
        DIVISION_BY_ZERO_MESSAGE, true);
  }

  @Test void testDivideOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.DIVIDE, VmName.EXPAND);
    f.checkScalarExact("95.0 / 100", "DECIMAL(14, 6) NOT NULL", "0.95");
    f.checkScalarExact("95 / 100.0", "DECIMAL(17, 6) NOT NULL", "0.95");
    f.checkScalarExact("10 / 5", "INTEGER NOT NULL", "2");
    f.checkScalarExact("-10 / 5", "INTEGER NOT NULL", "-2");
    f.checkScalarExact("-10 / 5.0", "DECIMAL(17, 6) NOT NULL", "-2");
    f.checkScalarApprox(" cast(10.0 as double) / 5", "DOUBLE NOT NULL",
        isExactly(2));
    f.checkScalarApprox(" cast(10.0 as real) / 4", "REAL NOT NULL",
        isExactly("2.5"));
    f.checkScalarApprox(" 6.0 / cast(10.0 as real) ", "DOUBLE NOT NULL",
        isExactly("0.6"));
    f.checkScalarExact("10.0 / 5.0", "DECIMAL(9, 6) NOT NULL", "2");
    f.checkScalarExact("1.0 / 3.0", "DECIMAL(8, 6) NOT NULL", "0.3333333333333333");
    f.checkScalarExact("100.1 / 0.0001", "DECIMAL(14, 6) NOT NULL",
        "1.001E+6");
    f.checkScalarExact("100.1 / 0.00000001", "DECIMAL(19, 6) NOT NULL",
        "1.001E+10");
    f.checkNull("1e1 / cast(null as float)");
    f.checkScalarExact("100.1 / 0.00000000000000001", "DECIMAL(19, 6) NOT NULL",
        "1.001E+19");
    SqlOperatorFixture f0 = f.withFactory(tf ->
        tf.withTypeSystem(typeSystem ->
            new DelegatingTypeSystem(typeSystem) {
              @Override public int getMaxNumericPrecision() {
                return getMaxPrecision(SqlTypeName.DECIMAL);
              }

              @Override public int getMaxPrecision(SqlTypeName typeName) {
                switch (typeName) {
                case DECIMAL:
                  return 28;
                default:
                  return super.getMaxPrecision(typeName);
                }
              }

              @Override public int getMaxNumericScale() {
                return getMaxScale(SqlTypeName.DECIMAL);
              }

              @Override public int getMaxScale(SqlTypeName typeName) {
                switch (typeName) {
                case DECIMAL:
                  return 10;
                default:
                  return super.getMaxScale(typeName);
                }
              }
            }));
    f0.checkScalarExact("95.0 / 100", "DECIMAL(12, 10) NOT NULL", "0.95");
    f0.checkScalarExact("95 / 100.0", "DECIMAL(17, 6) NOT NULL", "0.95");
  }

  @Test void testDivideOperatorIntervals() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("interval '-2:2' hour to minute / 3",
        "-0:41", "INTERVAL HOUR TO MINUTE NOT NULL");
    f.checkScalar("interval '2:5:12' hour to second / 2 / -3",
        "-0:20:52.000000", "INTERVAL HOUR TO SECOND NOT NULL");
    f.checkNull("interval '2' day / cast(null as bigint)");
    f.checkNull("cast(null as interval month) / 2");
    f.checkScalar("interval '3-3' year to month / 15e-1",
        "+2-02", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkScalar("interval '3-4' year to month / 4.5",
        "+0-09", "INTERVAL YEAR TO MONTH NOT NULL");
  }

  @Test void testEqualsOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EQUALS, VmName.EXPAND);
    f.checkBoolean("1=1", true);
    f.checkBoolean("1=1.0", true);
    f.checkBoolean("1.34=1.34", true);
    f.checkBoolean("1=1.34", false);
    f.checkBoolean("1e2=100e0", true);
    f.checkBoolean("1e2=101", false);
    f.checkBoolean(
        "cast(1e2 as real)=cast(101 as bigint)",
        false);
    f.checkBoolean("'a'='b'", false);
    f.checkBoolean("true = true", true);
    f.checkBoolean("true = false", false);
    f.checkBoolean("false = true", false);
    f.checkBoolean("false = false", true);
    f.checkBoolean("cast('a' as varchar(30))=cast('a' as varchar(30))", true);
    f.checkBoolean("cast('a ' as varchar(30))=cast('a' as varchar(30))", false);
    f.checkBoolean("cast(' a' as varchar(30))=cast(' a' as varchar(30))", true);
    f.checkBoolean("cast('a ' as varchar(15))=cast('a ' as varchar(30))", true);
    f.checkBoolean("cast(' ' as varchar(3))=cast(' ' as varchar(2))", true);
    f.checkBoolean("cast('abcd' as varchar(2))='ab'", true);
    f.checkBoolean("cast('a' as varchar(30))=cast('b' as varchar(30))", false);
    f.checkBoolean("cast('a' as varchar(30))=cast('a' as varchar(15))", true);
    f.checkNull("cast(null as boolean)=cast(null as boolean)");
    f.checkNull("cast(null as integer)=1");
    f.checkNull("cast(null as varchar(10))='a'");
  }

  @Test void testEqualsOperatorInterval() {
    final SqlOperatorFixture f = fixture();
    f.checkBoolean("interval '2' day = interval '1' day", false);
    f.checkBoolean("interval '2' day = interval '2' day", true);
    f.checkBoolean("interval '2:2:2' hour to second = interval '2' hour",
        false);
    f.checkNull("cast(null as interval hour) = interval '2' minute");
  }

  @Test void testGreaterThanOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.GREATER_THAN, VmName.EXPAND);
    f.checkBoolean("1>2", false);
    f.checkBoolean("cast(-1 as TINYINT)>cast(1 as TINYINT)", false);
    f.checkBoolean("cast(1 as SMALLINT)>cast(1 as SMALLINT)", false);
    f.checkBoolean("2>1", true);
    f.checkBoolean("1.1>1.2", false);
    f.checkBoolean("-1.1>-1.2", true);
    f.checkBoolean("1.1>1.1", false);
    f.checkBoolean("1.2>1", true);
    f.checkBoolean("1.1e1>1.2e1", false);
    f.checkBoolean("cast(-1.1 as real) > cast(-1.2 as real)", true);
    f.checkBoolean("1.1e2>1.1e2", false);
    f.checkBoolean("1.2e0>1", true);
    f.checkBoolean("cast(1.2e0 as real)>1", true);
    f.checkBoolean("true>false", true);
    f.checkBoolean("true>true", false);
    f.checkBoolean("false>false", false);
    f.checkBoolean("false>true", false);
    f.checkNull("3.0>cast(null as double)");

    f.checkBoolean("DATE '2013-02-23' > DATE '1945-02-24'", true);
    f.checkBoolean("DATE '2013-02-23' > CAST(NULL AS DATE)", null);

    f.checkBoolean("x'0A000130'>x'0A0001B0'", false);
  }

  @Test void testGreaterThanOperatorIntervals() {
    final SqlOperatorFixture f = fixture();
    f.checkBoolean("interval '2' day > interval '1' day", true);
    f.checkBoolean("interval '2' day > interval '5' day", false);
    f.checkBoolean("interval '2 2:2:2' day to second > interval '2' day", true);
    f.checkBoolean("interval '2' day > interval '2' day", false);
    f.checkBoolean("interval '2' day > interval '-2' day", true);
    f.checkBoolean("interval '2' day > interval '2' hour", true);
    f.checkBoolean("interval '2' minute > interval '2' hour", false);
    f.checkBoolean("interval '2' second > interval '2' minute", false);
    f.checkNull("cast(null as interval hour) > interval '2' minute");
    f.checkNull(
        "interval '2:2' hour to minute > cast(null as interval second)");
  }

  @Test void testIsDistinctFromOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_DISTINCT_FROM, VM_EXPAND);
    f.checkBoolean("1 is distinct from 1", false);
    f.checkBoolean("1 is distinct from 1.0", false);
    f.checkBoolean("1 is distinct from 2", true);
    f.checkBoolean("cast(null as integer) is distinct from 2", true);
    f.checkBoolean(
        "cast(null as integer) is distinct from cast(null as integer)",
        false);
    f.checkBoolean("1.23 is distinct from 1.23", false);
    f.checkBoolean("1.23 is distinct from 5.23", true);
    f.checkBoolean("-23e0 is distinct from -2.3e1", false);

    // IS DISTINCT FROM not implemented for ROW yet
    if (false) {
      f.checkBoolean("row(1,1) is distinct from row(1,1)", true);
      f.checkBoolean("row(1,1) is distinct from row(1,2)", false);
    }

    // Intervals
    f.checkBoolean("interval '2' day is distinct from interval '1' day", true);
    f.checkBoolean("interval '10' hour is distinct from interval '10' hour",
        false);
  }

  @Test void testIsNotDistinctFromOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM, VM_EXPAND);
    f.checkBoolean("1 is not distinct from 1", true);
    f.checkBoolean("1 is not distinct from 1.0", true);
    f.checkBoolean("1 is not distinct from 2", false);
    f.checkBoolean("cast(null as integer) is not distinct from 2", false);
    f.checkBoolean(
        "cast(null as integer) is not distinct from cast(null as integer)",
        true);
    f.checkBoolean("1.23 is not distinct from 1.23", true);
    f.checkBoolean("1.23 is not distinct from 5.23", false);
    f.checkBoolean("-23e0 is not distinct from -2.3e1", true);

    // IS NOT DISTINCT FROM not implemented for ROW yet
    if (false) {
      f.checkBoolean("row(1,1) is not distinct from row(1,1)", false);
      f.checkBoolean("row(1,1) is not distinct from row(1,2)", true);
    }

    // Intervals
    f.checkBoolean("interval '2' day is not distinct from interval '1' day",
        false);
    f.checkBoolean("interval '10' hour is not distinct from interval '10' hour",
        true);
  }

  @Test void testGreaterThanOrEqualOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, VmName.EXPAND);
    f.checkBoolean("1>=2", false);
    f.checkBoolean("-1>=1", false);
    f.checkBoolean("1>=1", true);
    f.checkBoolean("2>=1", true);
    f.checkBoolean("1.1>=1.2", false);
    f.checkBoolean("-1.1>=-1.2", true);
    f.checkBoolean("1.1>=1.1", true);
    f.checkBoolean("1.2>=1", true);
    f.checkBoolean("1.2e4>=1e5", false);
    f.checkBoolean("1.2e4>=cast(1e5 as real)", false);
    f.checkBoolean("1.2>=cast(1e5 as double)", false);
    f.checkBoolean("120000>=cast(1e5 as real)", true);
    f.checkBoolean("true>=false", true);
    f.checkBoolean("true>=true", true);
    f.checkBoolean("false>=false", true);
    f.checkBoolean("false>=true", false);
    f.checkNull("cast(null as real)>=999");
    f.checkBoolean("x'0A000130'>=x'0A0001B0'", false);
    f.checkBoolean("x'0A0001B0'>=x'0A0001B0'", true);
  }

  @Test void testGreaterThanOrEqualOperatorIntervals() {
    final SqlOperatorFixture f = fixture();
    f.checkBoolean("interval '2' day >= interval '1' day", true);
    f.checkBoolean("interval '2' day >= interval '5' day", false);
    f.checkBoolean("interval '2 2:2:2' day to second >= interval '2' day",
        true);
    f.checkBoolean("interval '2' day >= interval '2' day", true);
    f.checkBoolean("interval '2' day >= interval '-2' day", true);
    f.checkBoolean("interval '2' day >= interval '2' hour", true);
    f.checkBoolean("interval '2' minute >= interval '2' hour", false);
    f.checkBoolean("interval '2' second >= interval '2' minute", false);
    f.checkNull("cast(null as interval hour) >= interval '2' minute");
    f.checkNull(
        "interval '2:2' hour to minute >= cast(null as interval second)");
  }

  @Test void testInOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IN, VM_EXPAND);
    f.checkBoolean("1 in (0, 1, 2)", true);
    f.checkBoolean("3 in (0, 1, 2)", false);
    f.checkBoolean("cast(null as integer) in (0, 1, 2)", null);
    f.checkBoolean("cast(null as integer) in (0, cast(null as integer), 2)",
        null);
    f.checkBoolean("cast(null as integer) in (0, null, 2)", null);
    f.checkBoolean("1 in (0, null, 2)", null);

    // AND has lower precedence than IN
    f.checkBoolean("false and true in (false, false)", false);

    if (!Bug.TODO_FIXED) {
      return;
    }
    f.checkFails("'foo' in (^)^", "(?s).*Encountered \"\\)\" at .*", false);
  }

  @Test void testNotInOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.NOT_IN, VM_EXPAND);
    f.checkBoolean("1 not in (0, 1, 2)", false);
    f.checkBoolean("3 not in (0, 1, 2)", true);
    f.checkBoolean("cast(null as integer) not in (0, 1, 2)", null);
    f.checkBoolean("cast(null as integer) not in (0, cast(null as integer), 2)",
        null);
    f.checkBoolean("cast(null as integer) not in (0, null, 2)", null);
    f.checkBoolean("1 not in (0, null, 2)", null);

    // AND has lower precedence than NOT IN
    f.checkBoolean("true and false not in (true, true)", true);

    if (!Bug.TODO_FIXED) {
      return;
    }
    f.checkFails("'foo' not in (^)^", "(?s).*Encountered \"\\)\" at .*", false);
  }

  @Test void testOverlapsOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.OVERLAPS, VM_EXPAND);
    f.checkBoolean("(date '1-2-3', date '1-2-3') "
        + "overlaps (date '1-2-3', interval '1' year)", true);
    f.checkBoolean("(date '1-2-3', date '1-2-3') "
        + "overlaps (date '4-5-6', interval '1' year)", false);
    f.checkBoolean("(date '1-2-3', date '4-5-6') "
        + "overlaps (date '2-2-3', date '3-4-5')", true);
    f.checkNull("(cast(null as date), date '1-2-3') "
        + "overlaps (date '1-2-3', interval '1' year)");
    f.checkNull("(date '1-2-3', date '1-2-3') overlaps "
        + "(date '1-2-3', cast(null as date))");

    f.checkBoolean("(time '1:2:3', interval '1' second) "
        + "overlaps (time '23:59:59', time '1:2:3')", true);
    f.checkBoolean("(time '1:2:3', interval '1' second) "
        + "overlaps (time '23:59:59', time '1:2:2')", true);
    f.checkBoolean("(time '1:2:3', interval '1' second) "
        + "overlaps (time '23:59:59', interval '2' hour)", false);
    f.checkNull("(time '1:2:3', cast(null as time)) "
        + "overlaps (time '23:59:59', time '1:2:3')");
    f.checkNull("(time '1:2:3', interval '1' second) "
        + "overlaps (time '23:59:59', cast(null as interval hour))");

    f.checkBoolean("(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) "
        + "overlaps (timestamp '1-2-3 4:5:6',"
        + " interval '1 2:3:4.5' day to second)", true);
    f.checkBoolean("(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) "
        + "overlaps (timestamp '2-2-3 4:5:6',"
        + " interval '1 2:3:4.5' day to second)", false);
    f.checkNull("(timestamp '1-2-3 4:5:6', cast(null as interval day) ) "
        + "overlaps (timestamp '1-2-3 4:5:6',"
        + " interval '1 2:3:4.5' day to second)");
    f.checkNull("(timestamp '1-2-3 4:5:6', timestamp '1-2-3 4:5:6' ) "
        + "overlaps (cast(null as timestamp),"
        + " interval '1 2:3:4.5' day to second)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-715">[CALCITE-715]
   * Add PERIOD type constructor and period operators (CONTAINS, PRECEDES,
   * etc.)</a>.
   *
   * <p>Tests OVERLAP and similar period operators CONTAINS, EQUALS, PRECEDES,
   * SUCCEEDS, IMMEDIATELY PRECEDES, IMMEDIATELY SUCCEEDS for DATE, TIME and
   * TIMESTAMP values. */
  @Test void testPeriodOperators() {
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
    final SqlOperatorFixture f = fixture();
    checkOverlaps(new OverlapChecker(f, times));
    checkOverlaps(new OverlapChecker(f, dates));
    checkOverlaps(new OverlapChecker(f, timestamps));
  }

  static void checkOverlaps(OverlapChecker c) {
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

  @Test void testLessThanOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LESS_THAN, VmName.EXPAND);
    f.checkBoolean("1<2", true);
    f.checkBoolean("-1<1", true);
    f.checkBoolean("1<1", false);
    f.checkBoolean("2<1", false);
    f.checkBoolean("1.1<1.2", true);
    f.checkBoolean("-1.1<-1.2", false);
    f.checkBoolean("1.1<1.1", false);
    f.checkBoolean("cast(1.1 as real)<1", false);
    f.checkBoolean("cast(1.1 as real)<1.1", false);
    f.checkBoolean("cast(1.1 as real)<cast(1.2 as real)", true);
    f.checkBoolean("-1.1e-1<-1.2e-1", false);
    f.checkBoolean("cast(1.1 as real)<cast(1.1 as double)", false);
    f.checkBoolean("true<false", false);
    f.checkBoolean("true<true", false);
    f.checkBoolean("false<false", false);
    f.checkBoolean("false<true", true);
    f.checkNull("123<cast(null as bigint)");
    f.checkNull("cast(null as tinyint)<123");
    f.checkNull("cast(null as integer)<1.32");
    f.checkBoolean("x'0A000130'<x'0A0001B0'", true);
  }

  @Test void testLessThanOperatorInterval() {
    final SqlOperatorFixture f = fixture();
    f.checkBoolean("interval '2' day < interval '1' day", false);
    f.checkBoolean("interval '2' day < interval '5' day", true);
    f.checkBoolean("interval '2 2:2:2' day to second < interval '2' day",
        false);
    f.checkBoolean("interval '2' day < interval '2' day", false);
    f.checkBoolean("interval '2' day < interval '-2' day", false);
    f.checkBoolean("interval '2' day < interval '2' hour", false);
    f.checkBoolean("interval '2' minute < interval '2' hour", true);
    f.checkBoolean("interval '2' second < interval '2' minute", true);
    f.checkNull("cast(null as interval hour) < interval '2' minute");
    f.checkNull("interval '2:2' hour to minute "
        + "< cast(null as interval second)");
  }

  @Test void testLessThanOrEqualOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
        VmName.EXPAND);
    f.checkBoolean("1<=2", true);
    f.checkBoolean("1<=1", true);
    f.checkBoolean("-1<=1", true);
    f.checkBoolean("2<=1", false);
    f.checkBoolean("1.1<=1.2", true);
    f.checkBoolean("-1.1<=-1.2", false);
    f.checkBoolean("1.1<=1.1", true);
    f.checkBoolean("1.2<=1", false);
    f.checkBoolean("1<=cast(1e2 as real)", true);
    f.checkBoolean("1000<=cast(1e2 as real)", false);
    f.checkBoolean("1.2e1<=1e2", true);
    f.checkBoolean("1.2e1<=cast(1e2 as real)", true);
    f.checkBoolean("true<=false", false);
    f.checkBoolean("true<=true", true);
    f.checkBoolean("false<=false", true);
    f.checkBoolean("false<=true", true);
    f.checkNull("cast(null as real)<=cast(1 as real)");
    f.checkNull("cast(null as integer)<=3");
    f.checkNull("3<=cast(null as smallint)");
    f.checkNull("cast(null as integer)<=1.32");
    f.checkBoolean("x'0A000130'<=x'0A0001B0'", true);
    f.checkBoolean("x'0A0001B0'<=x'0A0001B0'", true);
  }

  @Test void testLessThanOrEqualOperatorInterval() {
    final SqlOperatorFixture f = fixture();
    f.checkBoolean("interval '2' day <= interval '1' day", false);
    f.checkBoolean("interval '2' day <= interval '5' day", true);
    f.checkBoolean("interval '2 2:2:2' day to second <= interval '2' day",
        false);
    f.checkBoolean("interval '2' day <= interval '2' day", true);
    f.checkBoolean("interval '2' day <= interval '-2' day", false);
    f.checkBoolean("interval '2' day <= interval '2' hour", false);
    f.checkBoolean("interval '2' minute <= interval '2' hour", true);
    f.checkBoolean("interval '2' second <= interval '2' minute", true);
    f.checkNull("cast(null as interval hour) <= interval '2' minute");
    f.checkNull("interval '2:2' hour to minute "
        + "<= cast(null as interval second)");
  }

  @Test void testMinusOperator() {
    SqlOperatorFixture f = fixture()
        .setFor(SqlStdOperatorTable.MINUS, VmName.EXPAND)
        // BigQuery requires arithmetic overflows
        .withConformance(SqlConformanceEnum.BIG_QUERY);
    f.checkScalarExact("-2-1", -3);
    f.checkScalarExact("-2-1-5", -8);
    f.checkScalarExact("2-1", 1);
    f.checkScalarApprox("cast(2.0 as double) -1", "DOUBLE NOT NULL",
        isExactly(1));
    f.checkScalarApprox("cast(1 as smallint)-cast(2.0 as real)",
        "REAL NOT NULL", isExactly(-1));
    f.checkScalarApprox("2.4-cast(2.0 as real)", "DOUBLE NOT NULL",
        isWithin(0.4, 0.00000001));
    f.checkScalarExact("1-2", -1);
    f.checkScalarExact("10.0 - 5.0", "DECIMAL(4, 1) NOT NULL", "5.0");
    f.checkScalarExact("19.68 - 4.2", "DECIMAL(5, 2) NOT NULL", "15.48");
    f.checkNull("1e1-cast(null as double)");
    f.checkNull("cast(null as tinyint) - cast(null as smallint)");

    f.checkFails("cast(100 as tinyint) - cast(-100 as tinyint)",
        INTEGER_OVERFLOW, true);
    f.checkFails("cast(-20000 as smallint) - cast(20000 as smallint)",
        INTEGER_OVERFLOW, true);
    f.checkFails("cast(1.5e9 as integer) - cast(-1.5e9 as integer)",
        INTEGER_OVERFLOW, true);
    f.checkFails("cast(-5e18 as bigint) - cast(5e18 as bigint)",
        LONG_OVERFLOW, true);
    f.checkFails("cast(5e18 as decimal(19,0)) - cast(-5e18 as decimal(19,0))",
        DECIMAL_OVERFLOW, true);
    f.checkFails("cast(-6e8 as decimal(19,10)) - cast(6e8 as decimal(19,10))",
        DECIMAL_OVERFLOW, true);
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6706">[CALCITE-6706]
   * Checked arithmetic does not take effect in subqueries</a>. */
  @Test void testCastOverflow() {
    final SqlOperatorFixture f = fixture().withConformance(SqlConformanceEnum.BIG_QUERY);
    f.checkFails("SELECT -CAST(-32768 AS SMALLINT)",
        "integer overflow: Value 32768 does not fit in a SMALLINT", true);
    f.checkFails("SELECT CAST(32768 AS SMALLINT)",
        "Value 32768 out of range", true);
    f.checkFails("SELECT -CAST(32768 AS SMALLINT)",
        "Value 32768 out of range", true);
    final SqlOperatorFixture f0 = fixture();
    // This query does not fail if checked arithmetic is not used
    f0.checkScalar("SELECT -CAST(-32768 AS SMALLINT)",
        "-32768", "SMALLINT");
    // The last two queries should fail in any conformance level
    // because the value "32768" cannot be represented as a SMALLINT
    f0.checkFails("SELECT CAST(32768 AS SMALLINT)",
        "Value 32768 out of range", true);
    f0.checkFails("SELECT -CAST(32768 AS SMALLINT)",
        "Value 32768 out of range", true);
  }

  @Test void testMinusIntervalOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MINUS, VmName.EXPAND);
    f.checkScalar("interval '2' day - interval '1' day",
        "+1", "INTERVAL DAY NOT NULL");
    f.checkScalar("interval '2' day - interval '1' minute",
        "+1 23:59", "INTERVAL DAY TO MINUTE NOT NULL");
    f.checkScalar("interval '2' year - interval '1' month",
        "+1-11", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkScalar("interval '2' year - interval '1' month - interval '3' year",
        "-1-01", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkNull("cast(null as interval day) + interval '2' hour");

    // Datetime minus interval
    f.checkScalar("time '12:03:01' - interval '1:1' hour to minute",
        "11:02:01", "TIME(0) NOT NULL");
    // Per [CALCITE-1632] Return types of datetime + interval
    // make sure that TIME values say in range
    f.checkScalar("time '12:03:01' - interval '1' day",
        "12:03:01", "TIME(0) NOT NULL");
    f.checkScalar("time '12:03:01' - interval '25' hour",
        "11:03:01", "TIME(0) NOT NULL");
    f.checkScalar("time '12:03:03' - interval '25:0:1' hour to second",
        "11:03:02", "TIME(0) NOT NULL");
    f.checkScalar("date '2005-03-02' - interval '5' day",
        "2005-02-25", "DATE NOT NULL");
    f.checkScalar("date '2005-03-02' - interval '5' day",
        "2005-02-25", "DATE NOT NULL");
    f.checkScalar("date '2005-03-02' - interval '5' hour",
        "2005-03-02", "DATE NOT NULL");
    f.checkScalar("date '2005-03-02' - interval '25' hour",
        "2005-03-01", "DATE NOT NULL");
    f.checkScalar("date '2005-03-02' - interval '25:45' hour to minute",
        "2005-03-01", "DATE NOT NULL");
    f.checkScalar("date '2005-03-02' - interval '25:45:54' hour to second",
        "2005-03-01", "DATE NOT NULL");
    f.checkScalar("timestamp '2003-08-02 12:54:01' "
            + "- interval '-4 2:4' day to minute",
        "2003-08-06 14:58:01", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp '2003-08-02 12:54:01' "
            + "- interval '0.01' second(1, 3)",
        isExactDateTime(LocalDateTime.of(2003, 8, 2, 12, 54, 0, 990_000_000)),
        "TIMESTAMP(3) NOT NULL");
    f.checkScalar("timestamp '2003-08-02 12:54:01.000' "
            + "- interval '1' second",
        isExactDateTime(LocalDateTime.of(2003, 8, 2, 12, 54, 0, 0)),
        "TIMESTAMP(3) NOT NULL");
    f.checkScalar("timestamp '2003-08-02 12:54:01.123' "
            + "- interval '1' hour",
        isExactDateTime(LocalDateTime.of(2003, 8, 2, 11, 54, 1, 123_000_000)),
        "TIMESTAMP(3) NOT NULL");
    f.checkScalar("timestamp with local time zone '2003-08-02 12:54:01' "
            + "- interval '0.456' second(1, 3)",
        isExactDateTime(LocalDateTime.of(2003, 8, 2, 12, 54, 0, 544_000_000)),
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL");
    f.checkScalar("time '23:54:01' "
            + "- interval '0.01' second(1, 3)",
        isExactTime(LocalTime.of(23, 54, 0, 990_000_000)), "TIME(3) NOT NULL");
    f.checkScalar("time '23:54:01.123' "
            + "- interval '1' minute",
        isExactTime(LocalTime.of(23, 53, 1, 123_000_000)), "TIME(3) NOT NULL");
    f.checkScalar("date '2003-08-02' "
            + "- interval '1.123' second(1, 3)",
        "2003-08-02", "DATE NOT NULL");

    // Datetime minus year-month interval
    f.checkScalar("timestamp '2003-08-02 12:54:01' - interval '12' year",
        "1991-08-02 12:54:01", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("date '2003-08-02' - interval '12' year",
        "1991-08-02", "DATE NOT NULL");
    f.checkScalar("date '2003-08-02' - interval '12-3' year to month",
        "1991-05-02", "DATE NOT NULL");
  }

  @Test void testMinusDateOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MINUS_DATE, VmName.EXPAND);
    f.checkScalar("(time '12:03:34' - time '11:57:23') minute to second",
        "+6:11.000000", "INTERVAL MINUTE TO SECOND NOT NULL");
    f.checkScalar("(time '12:03:23' - time '11:57:23') minute",
        "+6", "INTERVAL MINUTE NOT NULL");
    f.checkScalar("(time '12:03:34' - time '11:57:23') minute",
        "+6", "INTERVAL MINUTE NOT NULL");
    f.checkScalar("(timestamp '2004-05-01 12:03:34'"
            + " - timestamp '2004-04-29 11:57:23') day to second",
        "+2 00:06:11.000000", "INTERVAL DAY TO SECOND NOT NULL");
    f.checkScalar("(timestamp '2004-05-01 12:03:34'"
            + " - timestamp '2004-04-29 11:57:23') day to hour",
        "+2 00", "INTERVAL DAY TO HOUR NOT NULL");
    f.checkScalar("(date '2004-12-02' - date '2003-12-01') day",
        "+367", "INTERVAL DAY NOT NULL");
    f.checkNull("(cast(null as date) - date '2003-12-01') day");

    // combine '<datetime> + <interval>' with '<datetime> - <datetime>'
    f.checkScalar("timestamp '1969-04-29 0:0:0' +"
            + " (timestamp '2008-07-15 15:28:00' - "
            + "  timestamp '1969-04-29 0:0:0') day to second / 2",
        "1988-12-06 07:44:00", "TIMESTAMP(0) NOT NULL");

    f.checkScalar("date '1969-04-29' +"
            + " (date '2008-07-15' - "
            + "  date '1969-04-29') day / 2",
        "1988-12-06", "DATE NOT NULL");

    f.checkScalar("time '01:23:44' +"
            + " (time '15:28:00' - "
            + "  time '01:23:44') hour to second / 2",
        "08:25:52", "TIME(0) NOT NULL");

    if (Bug.DT1684_FIXED) {
      f.checkBoolean("(date '1969-04-29' +"
              + " (CURRENT_DATE - "
              + "  date '1969-04-29') day / 2) is not null",
          true);
    }
    f.checkScalar("(date '2023-12-01' - date '2022-12-01') year",
        "+1", "INTERVAL YEAR NOT NULL");
    f.checkScalar("(date '2022-12-01' - date '2023-12-01') year",
        "-1", "INTERVAL YEAR NOT NULL");
    f.checkScalar("(date '2023-12-01' - date '2023-01-01') month",
        "+11", "INTERVAL MONTH NOT NULL");
    f.checkScalar("(date '2023-01-01' - date '2023-12-01') month",
        "-11", "INTERVAL MONTH NOT NULL");
  }

  @Test void testMultiplyOperator() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlStdOperatorTable.MULTIPLY, VmName.EXPAND)
        // BigQuery requires arithmetic overflows
        .withConformance(SqlConformanceEnum.BIG_QUERY);
    f.checkScalarExact("2*3", 6);
    f.checkScalarExact("2*-3", -6);
    f.checkScalarExact("+2*3", 6);
    f.checkScalarExact("2*0", 0);
    f.checkScalarApprox("cast(2.0 as float)*3",
        "FLOAT NOT NULL", isExactly(6));
    f.checkScalarApprox("3*cast(2.0 as real)",
        "REAL NOT NULL", isExactly(6));
    f.checkScalarApprox("cast(2.0 as real)*3.2",
        "DOUBLE NOT NULL", isExactly("6.4"));
    f.checkScalarExact("10.0 * 5.0",
        "DECIMAL(5, 2) NOT NULL", "50.00");
    f.checkScalarExact("19.68 * 4.2",
        "DECIMAL(6, 3) NOT NULL", "82.656");
    f.checkNull("cast(1 as real)*cast(null as real)");
    f.checkNull("2e-3*cast(null as integer)");
    f.checkNull("cast(null as tinyint) * cast(4 as smallint)");

    // Should throw out of range error
    f.checkFails("cast(100 as tinyint) * cast(-2 as tinyint)",
        INTEGER_OVERFLOW, true);
    f.checkFails("cast(200 as smallint) * cast(200 as smallint)",
        INTEGER_OVERFLOW, true);
    f.checkFails("cast(1.5e9 as integer) * cast(-2 as integer)",
        INTEGER_OVERFLOW, true);
    f.checkFails("cast(5e9 as bigint) * cast(2e9 as bigint)",
        LONG_OVERFLOW, true);
    f.checkFails("cast(2e9 as decimal(19,0)) * cast(-5e9 as decimal(19,0))",
        DECIMAL_OVERFLOW, true);
    f.checkFails("cast(5e4 as decimal(19,10)) * cast(2e4 as decimal(19,10))",
        DECIMAL_OVERFLOW, true);
  }

  /**
   * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6721">
   * Incorrect implementation of SqlFunction.checkedDivide</a>. */
  @Test void testCheckedDivideOperator() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlStdOperatorTable.MULTIPLY, VmName.EXPAND)
        // BigQuery requires arithmetic overflows
        .withConformance(SqlConformanceEnum.BIG_QUERY);
    f.checkFails("CAST(-32768 as SMALLINT) / CAST(0 AS SMALLINT)",
        "/ by zero", true);
  }

  @Test void testMultiplyIntervals() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("interval '2:2' hour to minute * 3",
        "+6:06", "INTERVAL HOUR TO MINUTE NOT NULL");
    f.checkScalar("3 * 2 * interval '2:5:12' hour to second",
        "+12:31:12.000000", "INTERVAL HOUR TO SECOND NOT NULL");
    f.checkNull("interval '2' day * cast(null as bigint)");
    f.checkNull("cast(null as interval month) * 2");
    if (TODO) {
      f.checkScalar("interval '3-2' year to month * 15e-1",
          "+04-09", "INTERVAL YEAR TO MONTH NOT NULL");
      f.checkScalar("interval '3-4' year to month * 4.5",
          "+15-00", "INTERVAL YEAR TO MONTH NOT NULL");
    }
  }

  @Test void testDatePlusInterval() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("date '2014-02-11' + interval '2' day",
        "2014-02-13", "DATE NOT NULL");
    // 60 days is more than 2^32 milliseconds
    f.checkScalar("date '2014-02-11' + interval '60' day",
        "2014-04-12", "DATE NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1864">[CALCITE-1864]
   * Allow NULL literal as argument</a>. */
  @Test void testNullOperand() {
    final SqlOperatorFixture f = fixture();
    checkNullOperand(f, "=");
    checkNullOperand(f, ">");
    checkNullOperand(f, "<");
    checkNullOperand(f, "<=");
    checkNullOperand(f, ">=");
    checkNullOperand(f, "<>");

    // "!=" is allowed under ORACLE_10 SQL conformance level
    final SqlOperatorFixture f1 =
        f.withConformance(SqlConformanceEnum.ORACLE_10);
    checkNullOperand(f1, "<>");
  }

  private static void checkNullOperand(SqlOperatorFixture f, String op) {
    f.checkBoolean("1 " + op + " null", null);
    f.checkBoolean("null " + op + " -3", null);
    f.checkBoolean("null " + op + " null", null);
  }

  @Test void testNotEqualsOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.NOT_EQUALS, VmName.EXPAND);
    f.checkBoolean("1<>1", false);
    f.checkBoolean("'a'<>'A'", true);
    f.checkBoolean("1e0<>1e1", true);
    f.checkNull("'a'<>cast(null as varchar(1))");

    // "!=" is not an acceptable alternative to "<>" under default SQL
    // conformance level
    f.checkFails("1 ^!=^ 1",
        "Bang equal '!=' is not allowed under the current SQL conformance level",
        false);

    final Consumer<SqlOperatorFixture> consumer = f1 -> {
      f1.checkBoolean("1 <> 1", false);
      f1.checkBoolean("1 != 1", false);
      f1.checkBoolean("2 != 1", true);
      f1.checkBoolean("1 != null", null);
    };

    // "!=" is allowed under BigQuery, ORACLE_10 SQL conformance levels
    final List<SqlConformanceEnum> conformances =
        list(SqlConformanceEnum.BIG_QUERY, SqlConformanceEnum.ORACLE_10);
    f.forEachConformance(conformances, consumer);
  }

  @Test void testNotEqualsOperatorIntervals() {
    final SqlOperatorFixture f = fixture();
    f.checkBoolean("interval '2' day <> interval '1' day", true);
    f.checkBoolean("interval '2' day <> interval '2' day", false);
    f.checkBoolean("interval '2:2:2' hour to second <> interval '2' hour",
        true);
    f.checkNull("cast(null as interval hour) <> interval '2' minute");
  }

  @Test void testOrOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.OR, VmName.EXPAND);
    f.checkBoolean("true or false", true);
    f.checkBoolean("false or false", false);
    f.checkBoolean("true or cast(null as boolean)", true);
    f.checkNull("false or cast(null as boolean)");
  }

  @Test void testOrOperatorLazy() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.OR, VmName.EXPAND);

    // need to evaluate 2nd argument if first evaluates to null, therefore
    // get error
    f.check("values 1 < cast(null as integer) or sqrt(-4) = -2",
        SqlTests.BOOLEAN_TYPE_CHECKER, SqlTests.ANY_PARAMETER_CHECKER,
        new ValueOrExceptionResultChecker(null, INVALID_ARG_FOR_POWER,
            CODE_2201F));

    // Do not need to evaluate 2nd argument if first evaluates to true.
    // In eager evaluation, get error;
    // lazy evaluation returns true;
    // both are valid.
    f.check("values 1 < 2 or sqrt(-4) = -2",
        SqlTests.BOOLEAN_TYPE_CHECKER, SqlTests.ANY_PARAMETER_CHECKER,
        new ValueOrExceptionResultChecker(true, INVALID_ARG_FOR_POWER,
            CODE_2201F));

    // NULL OR FALSE --> NULL
    // In eager evaluation, get error;
    // lazy evaluation returns NULL;
    // both are valid.
    f.check("values 1 < cast(null as integer) or sqrt(4) = -2",
        SqlTests.BOOLEAN_TYPE_CHECKER, SqlTests.ANY_PARAMETER_CHECKER,
        new ValueOrExceptionResultChecker(null, INVALID_ARG_FOR_POWER,
            CODE_2201F));

    // NULL OR TRUE --> TRUE
    f.checkBoolean("1 < cast(null as integer) or sqrt(4) = 2", true);
  }

  @Test void testPlusOperator() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlStdOperatorTable.PLUS, VmName.EXPAND)
        // BigQuery requires arithmetic overflows
        .withConformance(SqlConformanceEnum.BIG_QUERY);
    f.checkScalarExact("1+2", 3);
    f.checkScalarExact("-1+2", 1);
    f.checkScalarExact("1+2+3", 6);
    f.checkScalarApprox("1+cast(2.0 as double)", "DOUBLE NOT NULL",
        isExactly(3));
    f.checkScalarApprox("1+cast(2.0 as double)+cast(6.0 as float)",
        "DOUBLE NOT NULL", isExactly(9));
    f.checkScalarExact("10.0 + 5.0", "DECIMAL(4, 1) NOT NULL", "15.0");
    f.checkScalarExact("19.68 + 4.2", "DECIMAL(5, 2) NOT NULL", "23.88");
    f.checkScalarExact("19.68 + 4.2 + 6", "DECIMAL(13, 2) NOT NULL", "29.88");
    f.checkScalarApprox("19.68 + cast(4.2 as float)", "DOUBLE NOT NULL",
        isWithin(23.88, 0.02));
    f.checkNull("cast(null as tinyint)+1");
    f.checkNull("1e-2+cast(null as double)");

    // Should throw out of range error
    f.checkFails("cast(100 as tinyint) + cast(100 as tinyint)",
        INTEGER_OVERFLOW, true);
    f.checkFails("cast(-20000 as smallint) + cast(-20000 as smallint)",
        INTEGER_OVERFLOW, true);
    f.checkFails("cast(1.5e9 as integer) + cast(1.5e9 as integer)",
        INTEGER_OVERFLOW, true);
    f.checkFails("cast(5e18 as bigint) + cast(5e18 as bigint)",
        LONG_OVERFLOW, true);
    f.checkFails("cast(-5e18 as decimal(19,0))"
            + " + cast(-5e18 as decimal(19,0))",
        DECIMAL_OVERFLOW, true);
    f.checkFails("cast(5e8 as decimal(19,10)) + cast(5e8 as decimal(19,10))",
        DECIMAL_OVERFLOW, true);
  }

  @Test void testPlusOperatorAny() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.PLUS, VmName.EXPAND);
    f.checkScalar("1+CAST(2 AS ANY)", "3", "ANY NOT NULL");
  }

  @Test void testPlusIntervalOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.PLUS, VmName.EXPAND);
    f.checkScalar("interval '2' day + interval '1' day",
        "+3", "INTERVAL DAY NOT NULL");
    f.checkScalar("interval '2' day + interval '1' minute",
        "+2 00:01", "INTERVAL DAY TO MINUTE NOT NULL");
    f.checkScalar("interval '2' day + interval '5' minute"
            + " + interval '-3' second",
        "+2 00:04:57.000000", "INTERVAL DAY TO SECOND NOT NULL");
    f.checkScalar("interval '2' year + interval '1' month",
        "+2-01", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkNull("interval '2' year + cast(null as interval month)");

    // Datetime plus interval
    f.checkScalar("time '12:03:01' + interval '1:1' hour to minute",
        "13:04:01", "TIME(0) NOT NULL");
    // Per [CALCITE-1632] Return types of datetime + interval
    // make sure that TIME values say in range
    f.checkScalar("time '12:03:01' + interval '1' day",
        "12:03:01", "TIME(0) NOT NULL");
    f.checkScalar("time '12:03:01' + interval '25' day",
        "12:03:01", "TIME(0) NOT NULL");
    f.checkScalar("time '12:03:01' + interval '25' hour",
        "13:03:01", "TIME(0) NOT NULL");
    f.checkScalar("time '12:03:01' + interval '25:0:1' hour to second",
        "13:03:02", "TIME(0) NOT NULL");
    f.checkScalar("interval '5' day + date '2005-03-02'",
        "2005-03-07", "DATE NOT NULL");
    f.checkScalar("date '2005-03-02' + interval '5' day",
        "2005-03-07", "DATE NOT NULL");
    f.checkScalar("date '2005-03-02' + interval '5' hour",
        "2005-03-02", "DATE NOT NULL");
    f.checkScalar("date '2005-03-02' + interval '25' hour",
        "2005-03-03", "DATE NOT NULL");
    f.checkScalar("date '2005-03-02' + interval '25:45' hour to minute",
        "2005-03-03", "DATE NOT NULL");
    f.checkScalar("date '2005-03-02' + interval '25:45:54' hour to second",
        "2005-03-03", "DATE NOT NULL");
    f.checkScalar("timestamp '2003-08-02 12:54:01'"
            + " + interval '-4 2:4' day to minute",
        "2003-07-29 10:50:01", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("interval '0.003' SECOND(1, 3) + timestamp '2003-08-02 12:54:01.001'",
        isExactDateTime(LocalDateTime.of(2003, 8, 2, 12, 54, 1, 4_000_000)),
        "TIMESTAMP(3) NOT NULL");
    f.checkScalar("timestamp '2003-08-02 12:54:01.000' "
            + "+ interval '1' second",
        isExactDateTime(LocalDateTime.of(2003, 8, 2, 12, 54, 2, 0)),
        "TIMESTAMP(3) NOT NULL");
    f.checkScalar("timestamp '2003-08-02 12:54:01.123' "
            + "+ interval '1' hour",
        isExactDateTime(LocalDateTime.of(2003, 8, 2, 13, 54, 1, 123_000_000)),
        "TIMESTAMP(3) NOT NULL");
    f.checkScalar("timestamp with local time zone '2003-08-02 12:54:01' "
            + "+ interval '0.456' second(1, 3)",
        isExactDateTime(LocalDateTime.of(2003, 8, 2, 12, 54, 1, 456_000_000)),
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL");
    f.checkScalar("time '23:54:01' "
            + "+ interval '0.01' second(1, 3)",
        isExactTime(LocalTime.of(23, 54, 1, 10_000_000)), "TIME(3) NOT NULL");
    f.checkScalar("time '23:54:01.123' "
            + "+ interval '1' minute",
        isExactTime(LocalTime.of(23, 55, 1, 123_000_000)), "TIME(3) NOT NULL");
    f.checkScalar("date '2003-08-02' "
            + "+ interval '1.123' second(1, 3)",
        "2003-08-02", "DATE NOT NULL");

    // Datetime plus year-to-month interval
    f.checkScalar("interval '5-3' year to month + date '2005-03-02'",
        "2010-06-02", "DATE NOT NULL");
    f.checkScalar("timestamp '2003-08-02 12:54:01'"
            + " + interval '5-3' year to month",
        "2008-11-02 12:54:01", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("interval '5-3' year to month"
            + " + timestamp '2003-08-02 12:54:01'",
        "2008-11-02 12:54:01", "TIMESTAMP(0) NOT NULL");
  }

  @Test void testDescendingOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.DESC, VM_EXPAND);
  }

  @Test void testIsNotTrueOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_NOT_TRUE, VmName.EXPAND);
    f.checkBoolean("true is not true", false);
    f.checkBoolean("false is not true", true);
    f.checkBoolean("cast(null as boolean) is not true", true);
    f.checkFails("select ^'a string' is not true^ from (values (1))",
        "(?s)Cannot apply 'IS NOT TRUE' to arguments of type "
            + "'<CHAR\\(8\\)> IS NOT TRUE'. Supported form\\(s\\): "
            + "'<BOOLEAN> IS NOT TRUE'.*",
        false);
  }

  @Test void testIsTrueOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_TRUE, VmName.EXPAND);
    f.checkBoolean("true is true", true);
    f.checkBoolean("false is true", false);
    f.checkBoolean("cast(null as boolean) is true", false);
  }

  @Test void testIsNotFalseOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_NOT_FALSE, VmName.EXPAND);
    f.checkBoolean("false is not false", false);
    f.checkBoolean("true is not false", true);
    f.checkBoolean("cast(null as boolean) is not false", true);
  }

  @Test void testIsFalseOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_FALSE, VmName.EXPAND);
    f.checkBoolean("false is false", true);
    f.checkBoolean("true is false", false);
    f.checkBoolean("cast(null as boolean) is false", false);
  }

  @Test void testIsNotNullOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_NOT_NULL, VmName.EXPAND);
    checkIsNotNull(f, SqlStdOperatorTable.IS_NOT_NULL);
  }

  @Test void testIsNotUnknownOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_NOT_UNKNOWN, VM_EXPAND);
    f.checkFails("^'abc' IS NOT UNKNOWN^",
        "(?s).*Cannot apply 'IS NOT UNKNOWN'.*",
        false);
    checkIsNotNull(f, SqlStdOperatorTable.IS_NOT_UNKNOWN);
  }

  /** Tests the {@code IS NOT NULL} and {@code IS NOT UNKNOWN} operators. */
  void checkIsNotNull(SqlOperatorFixture f, SqlOperator operator) {
    final String fn = operator.getName();
    f.checkBoolean("false " + fn, true);
    f.checkBoolean("true " + fn, true);
    f.checkBoolean("cast(null as boolean) " + fn, false);
    f.checkBoolean("unknown " + fn, false);
  }

  @Test void testIsNullOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_NULL, VmName.EXPAND);
    checkIsNull(f, SqlStdOperatorTable.IS_NULL);
  }

  @Test void testIsUnknownOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_UNKNOWN, VmName.EXPAND);
    f.checkFails("0 = 1 AND ^2 IS UNKNOWN^ AND 3 > 4",
        "(?s).*Cannot apply 'IS UNKNOWN'.*",
        false);
    checkIsNull(f, SqlStdOperatorTable.IS_UNKNOWN);
  }

  /** Tests the {@code IS NULL} and {@code IS UNKNOWN} operators. */
  void checkIsNull(SqlOperatorFixture f, SqlOperator operator) {
    final String fn = operator.getName();
    f.checkBoolean("false " + fn, false);
    f.checkBoolean("true " + fn, false);
    f.checkBoolean("cast(null as boolean) " + fn, true);
  }

  @Test void testIsASetOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_A_SET, VM_EXPAND);
    f.checkBoolean("multiset[1] is a set", true);
    f.checkBoolean("multiset[1, 1] is a set", false);
    f.checkBoolean("multiset[cast(null as boolean), cast(null as boolean)]"
        + " is a set", false);
    f.checkBoolean("multiset[cast(null as boolean)] is a set", true);
    f.checkBoolean("multiset['a'] is a set", true);
    f.checkBoolean("multiset['a', 'b'] is a set", true);
    f.checkBoolean("multiset['a', 'b', 'a'] is a set", false);
  }

  @Test void testIsNotASetOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_NOT_A_SET, VM_EXPAND);
    f.checkBoolean("multiset[1] is not a set", false);
    f.checkBoolean("multiset[1, 1] is not a set", true);
    f.checkBoolean("multiset[cast(null as boolean), cast(null as boolean)]"
        + " is not a set", true);
    f.checkBoolean("multiset[cast(null as boolean)] is not a set", false);
    f.checkBoolean("multiset['a'] is not a set", false);
    f.checkBoolean("multiset['a', 'b'] is not a set", false);
    f.checkBoolean("multiset['a', 'b', 'a'] is not a set", true);
  }

  @Test void testIntersectOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MULTISET_INTERSECT, VM_EXPAND);
    f.checkScalar("multiset[1] multiset intersect multiset[1]",
        "[1]", "INTEGER NOT NULL MULTISET NOT NULL");
    f.checkScalar("multiset[2] multiset intersect all multiset[1]",
        "[]", "INTEGER NOT NULL MULTISET NOT NULL");
    f.checkScalar("multiset[2] multiset intersect distinct multiset[1]",
        "[]", "INTEGER NOT NULL MULTISET NOT NULL");
    f.checkScalar("multiset[1, 1] multiset intersect distinct multiset[1, 1]",
        "[1]", "INTEGER NOT NULL MULTISET NOT NULL");
    f.checkScalar("multiset[1, 1] multiset intersect all multiset[1, 1]",
        "[1, 1]", "INTEGER NOT NULL MULTISET NOT NULL");
    f.checkScalar("multiset[1, 1] multiset intersect distinct multiset[1, 1]",
        "[1]", "INTEGER NOT NULL MULTISET NOT NULL");
    f.checkScalar("multiset[cast(null as integer), cast(null as integer)] "
            + "multiset intersect distinct multiset[cast(null as integer)]",
        "[null]", "INTEGER MULTISET NOT NULL");
    f.checkScalar("multiset[cast(null as integer), cast(null as integer)] "
            + "multiset intersect all multiset[cast(null as integer)]",
        "[null]", "INTEGER MULTISET NOT NULL");
    f.checkScalar("multiset[cast(null as integer), cast(null as integer)] "
            + "multiset intersect distinct multiset[cast(null as integer)]",
        "[null]", "INTEGER MULTISET NOT NULL");
  }

  @Test void testExceptOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MULTISET_EXCEPT, VM_EXPAND);
    f.checkScalar("multiset[1] multiset except multiset[1]",
        "[]", "INTEGER NOT NULL MULTISET NOT NULL");
    f.checkScalar("multiset[1] multiset except distinct multiset[1]",
        "[]", "INTEGER NOT NULL MULTISET NOT NULL");
    f.checkScalar("multiset[2] multiset except multiset[1]",
        "[2]", "INTEGER NOT NULL MULTISET NOT NULL");
    f.checkScalar("multiset[1,2,3] multiset except multiset[1]",
        "[2, 3]", "INTEGER NOT NULL MULTISET NOT NULL");
    f.checkScalar("cardinality(multiset[1,2,3,2]"
            + " multiset except distinct multiset[1])",
        "2", "INTEGER NOT NULL");
    f.checkScalar("cardinality(multiset[1,2,3,2]"
            + " multiset except all multiset[1])",
        "3", "INTEGER NOT NULL");
    f.checkBoolean("(multiset[1,2,3,2] multiset except distinct multiset[1])"
        + " submultiset of multiset[2, 3]", true);
    f.checkBoolean("(multiset[1,2,3,2] multiset except distinct multiset[1])"
        + " submultiset of multiset[2, 3]", true);
    f.checkBoolean("(multiset[1,2,3,2] multiset except all multiset[1])"
        + " submultiset of multiset[2, 2, 3]", true);
    f.checkBoolean("(multiset[1,2,3] multiset except multiset[1]) is empty",
        false);
    f.checkBoolean("(multiset[1] multiset except multiset[1]) is empty", true);
  }

  @Test void testIsEmptyOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_EMPTY, VM_EXPAND);
    f.checkBoolean("multiset[1] is empty", false);
  }

  @Test void testIsNotEmptyOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.IS_NOT_EMPTY, VM_EXPAND);
    f.checkBoolean("multiset[1] is not empty", true);
  }

  @Test void testExistsOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EXISTS, VM_EXPAND);
  }

  @Test void testNotOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.NOT, VmName.EXPAND);
    f.checkBoolean("not true", false);
    f.checkBoolean("not false", true);
    f.checkBoolean("not unknown", null);
    f.checkNull("not cast(null as boolean)");
  }

  @Test void testPrefixMinusOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.UNARY_MINUS, VmName.EXPAND);
    f.enableTypeCoercion(false)
        .checkFails("'a' + ^- 'b'^ + 'c'",
            "(?s)Cannot apply '-' to arguments of type '-<CHAR\\(1\\)>'.*",
            false);
    f.checkType("'a' + - 'b' + 'c'", "DECIMAL(19, 9) NOT NULL");
    f.checkScalarExact("-1", -1);
    f.checkScalarExact("-1.23", "DECIMAL(3, 2) NOT NULL", "-1.23");
    f.checkScalarApprox("-1.0e0", "DOUBLE NOT NULL", isExactly(-1));
    f.checkNull("-cast(null as integer)");
    f.checkNull("-cast(null as tinyint)");
  }

  @Test void testPrefixMinusOperatorIntervals() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("-interval '-6:2:8' hour to second",
        "+6:02:08.000000", "INTERVAL HOUR TO SECOND NOT NULL");
    f.checkScalar("- -interval '-6:2:8' hour to second",
        "-6:02:08.000000", "INTERVAL HOUR TO SECOND NOT NULL");
    f.checkScalar("-interval '5' month",
        "-5", "INTERVAL MONTH NOT NULL");
    f.checkNull("-cast(null as interval day to minute)");
  }

  @Test void testPrefixPlusOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.UNARY_PLUS, VM_EXPAND);
    f.checkScalarExact("+1", 1);
    f.checkScalarExact("+1.23", "DECIMAL(3, 2) NOT NULL", "1.23");
    f.checkScalarApprox("+1.0e0", "DOUBLE NOT NULL", isExactly(1));
    f.checkNull("+cast(null as integer)");
    f.checkNull("+cast(null as tinyint)");
  }

  @Test void testPrefixPlusOperatorIntervals() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("+interval '-6:2:8' hour to second",
        "-6:02:08.000000", "INTERVAL HOUR TO SECOND NOT NULL");
    f.checkScalar("++interval '-6:2:8' hour to second",
        "-6:02:08.000000", "INTERVAL HOUR TO SECOND NOT NULL");
    if (Bug.FRG254_FIXED) {
      f.checkScalar("+interval '6:2:8.234' hour to second",
          "+06:02:08.234", "INTERVAL HOUR TO SECOND NOT NULL");
    }
    f.checkScalar("+interval '5' month",
        "+5", "INTERVAL MONTH NOT NULL");
    f.checkNull("+cast(null as interval day to minute)");
  }

  @Test void testExplicitTableOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EXPLICIT_TABLE, VM_EXPAND);
  }

  @Test void testValuesOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.VALUES, VM_EXPAND);
    f.check("select 'abc' from (values(true))",
        "CHAR(3) NOT NULL", "abc");
  }

  @Test void testNotLikeOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.NOT_LIKE, VM_EXPAND);
    f.checkBoolean("'abc' not like '_b_'", false);
    f.checkBoolean("'ab\ncd' not like 'ab%'", false);
    f.checkBoolean("'123\n\n45\n' not like '%'", false);
    f.checkBoolean("'ab\ncd\nef' not like '%cd%'", false);
    f.checkBoolean("'ab\ncd\nef' not like '%cde%'", true);
  }

  @Test void testRlikeOperator() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.RLIKE, VM_EXPAND);
    checkRlikeFunc(f, SqlLibrary.HIVE, SqlLibraryOperators.RLIKE);
    checkRlikeFunc(f, SqlLibrary.SPARK, SqlLibraryOperators.RLIKE);
    checkRlikeFunc(f, SqlLibrary.SPARK, SqlLibraryOperators.REGEXP);
    checkRlikeFunc(f, SqlLibrary.MYSQL, SqlLibraryOperators.RLIKE);
    checkNotRlikeFunc(f.withLibrary(SqlLibrary.HIVE));
    checkNotRlikeFunc(f.withLibrary(SqlLibrary.SPARK));
    checkNotRlikeFunc(f.withLibrary(SqlLibrary.MYSQL));
    checkRlikeFails(f.withLibrary(SqlLibrary.ORACLE));

    f.setFor(SqlLibraryOperators.REGEXP_LIKE, VM_EXPAND);
    checkRlikeFunc(f, SqlLibrary.SPARK, SqlLibraryOperators.REGEXP_LIKE);
    checkRlikeFunc(f, SqlLibrary.POSTGRESQL, SqlLibraryOperators.REGEXP_LIKE);
    checkRlikeFunc(f, SqlLibrary.MYSQL, SqlLibraryOperators.REGEXP_LIKE);
    checkRlikeFunc(f, SqlLibrary.ORACLE, SqlLibraryOperators.REGEXP_LIKE);
    checkRlikeFunc(f, SqlLibrary.REDSHIFT, SqlLibraryOperators.REGEXP_LIKE);
  }

  void checkRlikeFunc(SqlOperatorFixture f0, SqlLibrary library, SqlOperator operator) {
    // Note: carets were escaped (doubled) in some tests
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkBoolean(binaryExpression(operator, "'Merrisa@gmail.com'", "'.+@*\\.com'"), true);
      f.checkBoolean(binaryExpression(operator, "'Merrisa@gmail.com'", "'.com$'"), true);
      f.checkBoolean(binaryExpression(operator, "'acbd'", "'^^ac+'"), true);
      f.checkBoolean(binaryExpression(operator, "'acb'", "'acb|efg'"), true);
      f.checkBoolean(binaryExpression(operator, "'acb|efg'", "'acb\\|efg'"), true);
      f.checkBoolean(binaryExpression(operator, "'Acbd'", "'^^ac+'"), false);
      f.checkBoolean(binaryExpression(operator, "'Merrisa@gmail.com'", "'Merrisa_'"), false);
      f.checkBoolean(binaryExpression(operator, "'abcdef'", "'%cd%'"), false);
      f.checkBoolean(binaryExpression(operator, "'abc def ghi'", "'abc'"), true);
      f.checkBoolean(binaryExpression(operator, "'abc def ghi'", "'[a-z]+'"), true);
      f.checkBoolean(
          binaryExpression(operator, "'foo@bar.com'",
          "'@[a-zA-Z0-9-]+\\.[a-zA-Z0-9.]+'"), true);
      f.checkBoolean(
          binaryExpression(operator, "'foo@.com'",
          "'@[a-zA-Z0-9-]+\\.[a-zA-Z0-9.]+'"), false);
      f.checkBoolean(binaryExpression(operator, "'5556664422'", "'^^\\d{10}$'"), true);
      f.checkBoolean(binaryExpression(operator, "'11555666442233'", "'^^\\d{10}$'"), false);
      f.checkBoolean(binaryExpression(operator, "'55566644221133'", "'\\d{10}'"), true);
      f.checkBoolean(binaryExpression(operator, "'55as56664as422'", "'\\d{10}'"), false);
      f.checkBoolean(binaryExpression(operator, "'55as56664as422'", "''"), true);

      // test for string escaped
      f.checkBoolean(binaryExpression(operator, "'abc'", "'^\\abc$'"), false);
      f.checkBoolean(
          binaryExpression(operator, "'%SystemDrive%\\Users\\John'",
              "'%SystemDrive%\\\\Users.*'"), true);
      f.checkBoolean(
          binaryExpression(operator, "'%SystemDrive%\\\\Users\\\\John'",
          "'%SystemDrive%\\\\\\\\Users.*'"), true);
      f.checkFails(
          binaryExpression(operator, "'%SystemDrive%\\Users\\John'",
          "'%SystemDrive%\\Users.*'"),
          "(?s).*Illegal/unsupported escape sequence near index.*", true);

      f.checkQuery("select " + binaryExpression(operator, "'abc def ghi'", "'abc'"));
      f.checkQuery(
          "select " + binaryExpression(operator, "'foo@bar.com'",
          "'@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+'"));
      f.checkQuery("select " + binaryExpression(operator, "'55as56664as422'", "'\\d{10}'"));

      f.checkNull(binaryExpression(operator, "'abc def ghi'", "cast(null as varchar)"));
      f.checkNull(binaryExpression(operator, "cast(null as varchar)", "'abc'"));
      f.checkNull(binaryExpression(operator, "cast(null as varchar)", "cast(null as varchar)"));
    };
    f0.forEachLibrary(list(library), consumer);
  }

  void checkNotRlikeFunc(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.NOT_RLIKE, VM_EXPAND);
    f.checkBoolean("'Merrisagmail' not rlike '.+@*\\.com'", true);
    f.checkBoolean("'acbd' not rlike '^ac+'", false);
    f.checkBoolean("'acb|efg' not rlike 'acb\\|efg'", false);
    f.checkBoolean("'Merrisa@gmail.com' not rlike 'Merrisa_'", true);
  }

  private String binaryExpression(SqlOperator operator, String left, String right) {
    if (SqlLibraryOperators.RLIKE == operator || SqlLibraryOperators.NOT_RLIKE == operator) {
      return left + " " + operator.getName() + " " + right;
    } else {
      return operator.getName() + "( " + left + ", " + right + ")";
    }
  }

  static void checkRlikeFails(SqlOperatorFixture f) {
    final String noRlike = "(?s).*No match found for function signature RLIKE";
    f.checkFails("^'Merrisa@gmail.com' rlike '.+@*\\.com'^", noRlike, false);
    f.checkFails("^'acb' rlike 'acb|efg'^", noRlike, false);
    final String noNotRlike =
        "(?s).*No match found for function signature NOT RLIKE";
    f.checkFails("^'abcdef' not rlike '%cd%'^", noNotRlike, false);
    f.checkFails("^'Merrisa@gmail.com' not rlike 'Merrisa_'^", noNotRlike, false);
  }

  @Test void testLikeEscape() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LIKE, VmName.EXPAND);
    f.checkBoolean("'a_c' like 'a#_c' escape '#'", true);
    f.checkBoolean("'axc' like 'a#_c' escape '#'", false);
    f.checkBoolean("'a_c' like 'a\\_c' escape '\\'", true);
    f.checkBoolean("'axc' like 'a\\_c' escape '\\'", false);
    f.checkBoolean("'a%c' like 'a\\%c' escape '\\'", true);
    f.checkBoolean("'a%cde' like 'a\\%c_e' escape '\\'", true);
    f.checkBoolean("'abbc' like 'a%c' escape '\\'", true);
    f.checkBoolean("'abbc' like 'a\\%c' escape '\\'", false);
  }

  @Test void testIlikeEscape() {
    final SqlOperatorFixture f = fixture().setFor(SqlLibraryOperators.ILIKE, VmName.EXPAND)
        .withLibrary(SqlLibrary.POSTGRESQL);
    f.checkBoolean("'a_c' ilike 'a#_C' escape '#'", true);
    f.checkBoolean("'axc' ilike 'a#_C' escape '#'", false);
    f.checkBoolean("'a_c' ilike 'a\\_C' escape '\\'", true);
    f.checkBoolean("'axc' ilike 'a\\_C' escape '\\'", false);
    f.checkBoolean("'a%c' ilike 'a\\%C' escape '\\'", true);
    f.checkBoolean("'a%cde' ilike 'a\\%C_e' escape '\\'", true);
    f.checkBoolean("'abbc' ilike 'a%C' escape '\\'", true);
    f.checkBoolean("'abbc' ilike 'a\\%C' escape '\\'", false);
  }

  @Disabled("[CALCITE-525] Exception-handling in built-in functions")
  @Test void testLikeEscape2() {
    final SqlOperatorFixture f = fixture();
    f.checkBoolean("'x' not like 'x' escape 'x'", true);
    f.checkBoolean("'xyz' not like 'xyz' escape 'xyz'", true);
  }

  @Test void testLikeOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LIKE, VmName.EXPAND);
    f.checkBoolean("''  like ''", true);
    f.checkBoolean("'a' like 'a'", true);
    f.checkBoolean("'a' like 'b'", false);
    f.checkBoolean("'a' like 'A'", false);
    f.checkBoolean("'a' like 'a_'", false);
    f.checkBoolean("'a' like '_a'", false);
    f.checkBoolean("'a' like '%a'", true);
    f.checkBoolean("'a' like '%a%'", true);
    f.checkBoolean("'a' like 'a%'", true);
    f.checkBoolean("'ab'   like 'a_'", true);
    f.checkBoolean("'abc'  like 'a_'", false);
    f.checkBoolean("'abcd' like 'a%'", true);
    f.checkBoolean("'ab'   like '_b'", true);
    f.checkBoolean("'abcd' like '_d'", false);
    f.checkBoolean("'abcd' like '%d'", true);
    f.checkBoolean("'ab\ncd' like 'ab%'", true);
    f.checkBoolean("'abc\ncd' like 'ab%'", true);
    f.checkBoolean("'123\n\n45\n' like '%'", true);
    f.checkBoolean("'ab\ncd\nef' like '%cd%'", true);
    f.checkBoolean("'ab\ncd\nef' like '%cde%'", false);
  }

  @Test void testIlikeOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.ILIKE, VmName.EXPAND);
    final String noLike = "No match found for function signature ILIKE";
    f.checkFails("^'a' ilike 'b'^", noLike, false);
    f.checkFails("^'a' ilike 'b' escape 'c'^", noLike, false);
    final String noNotLike = "No match found for function signature NOT ILIKE";
    f.checkFails("^'a' not ilike 'b'^", noNotLike, false);
    f.checkFails("^'a' not ilike 'b' escape 'c'^", noNotLike, false);

    final SqlOperatorFixture f1 = f.withLibrary(SqlLibrary.POSTGRESQL);
    f1.checkBoolean("''  ilike ''", true);
    f1.checkBoolean("'a' ilike 'a'", true);
    f1.checkBoolean("'a' ilike 'b'", false);
    f1.checkBoolean("'a' ilike 'A'", true);
    f1.checkBoolean("'a' ilike 'a_'", false);
    f1.checkBoolean("'a' ilike '_a'", false);
    f1.checkBoolean("'a' ilike '%a'", true);
    f1.checkBoolean("'a' ilike '%A'", true);
    f1.checkBoolean("'a' ilike '%a%'", true);
    f1.checkBoolean("'a' ilike '%A%'", true);
    f1.checkBoolean("'a' ilike 'a%'", true);
    f1.checkBoolean("'a' ilike 'A%'", true);
    f1.checkBoolean("'ab'   ilike 'a_'", true);
    f1.checkBoolean("'ab'   ilike 'A_'", true);
    f1.checkBoolean("'abc'  ilike 'a_'", false);
    f1.checkBoolean("'abcd' ilike 'a%'", true);
    f1.checkBoolean("'abcd' ilike 'A%'", true);
    f1.checkBoolean("'ab'   ilike '_b'", true);
    f1.checkBoolean("'ab'   ilike '_B'", true);
    f1.checkBoolean("'abcd' ilike '_d'", false);
    f1.checkBoolean("'abcd' ilike '%d'", true);
    f1.checkBoolean("'abcd' ilike '%D'", true);
    f1.checkBoolean("'ab\ncd' ilike 'ab%'", true);
    f1.checkBoolean("'ab\ncd' ilike 'aB%'", true);
    f1.checkBoolean("'abc\ncd' ilike 'ab%'", true);
    f1.checkBoolean("'abc\ncd' ilike 'Ab%'", true);
    f1.checkBoolean("'123\n\n45\n' ilike '%'", true);
    f1.checkBoolean("'ab\ncd\nef' ilike '%cd%'", true);
    f1.checkBoolean("'ab\ncd\nef' ilike '%CD%'", true);
    f1.checkBoolean("'ab\ncd\nef' ilike '%cde%'", false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6309">[CALCITE-6309]
   * Add REGEXP_LIKE function (enabled in MySQL, Oracle, PostgreSQL and Spark libraries)</a>. */
  @Test void testRegexpLike3() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.REGEXP_LIKE, VmName.EXPAND);

    final Consumer<SqlOperatorFixture> consumer = f1 -> {
      f1.checkBoolean("REGEXP_LIKE('teststr', 'TEST', 'i')", true);
      f1.checkBoolean("REGEXP_LIKE('ateststr', 'TEST', 'c')", false);
      f1.checkBoolean("REGEXP_LIKE('atest\nstr', 'test.str', '')", false);
      f1.checkBoolean("REGEXP_LIKE('atest\nstr', 'test.str', 'n')", true);
      f1.checkBoolean("REGEXP_LIKE('atest\nstr', 'TEST.str', 'in')", true);
      f1.checkBoolean("REGEXP_LIKE('ateststring', 'teststr', '')", true);
      f1.checkBoolean("REGEXP_LIKE('ateststring', 'TESTstr', 'ic')", false);
      f1.checkBoolean("REGEXP_LIKE('ateststring', 'TESTstr', 'ci')", true);
      f1.checkBoolean("REGEXP_LIKE('atest\nstr', 'test.str', 's')", false);
      f1.checkBoolean("REGEXP_LIKE('atest\nstr', 'test.str', 'ns')", false);
      f1.checkBoolean("REGEXP_LIKE('atest\nstr', 'test.str', 'sn')", true);
      f1.checkNull("REGEXP_LIKE(NULL, 'test.str', 'sn')");
      f1.checkNull("REGEXP_LIKE('atest\nstr', NULL, 'sn')");
      f1.checkNull("REGEXP_LIKE('atest\nstr', 'test.str', NULL)");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.MYSQL, SqlLibrary.SPARK, SqlLibrary.POSTGRESQL,
            SqlLibrary.ORACLE);
    f.forEachLibrary(libraries, consumer);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-1898">[CALCITE-1898]
   * LIKE must match '.' (period) literally</a>. */
  @Test void testLikeDot() {
    final SqlOperatorFixture f = fixture();
    f.checkBoolean("'abc' like 'a.c'", false);
    f.checkBoolean("'abcde' like '%c.e'", false);
    f.checkBoolean("'abc.e' like '%c.e'", true);
  }

  @Test void testIlikeDot() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.ILIKE, VmName.EXPAND)
        .withLibrary(SqlLibrary.POSTGRESQL);
    f.checkBoolean("'abc' ilike 'a.c'", false);
    f.checkBoolean("'abcde' ilike '%c.e'", false);
    f.checkBoolean("'abc.e' ilike '%c.e'", true);
    f.checkBoolean("'abc.e' ilike '%c.E'", true);
  }

  @Test void testNotSimilarToOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.NOT_SIMILAR_TO, VM_EXPAND);
    f.checkBoolean("'ab' not similar to 'a_'", false);
    f.checkBoolean("'aabc' not similar to 'ab*c+d'", true);
    f.checkBoolean("'ab' not similar to 'a' || '_'", false);
    f.checkBoolean("'ab' not similar to 'ba_'", true);
    f.checkBoolean("cast(null as varchar(2)) not similar to 'a_'", null);
    f.checkBoolean("cast(null as varchar(3))"
        + " not similar to cast(null as char(2))", null);
  }

  @Test void testSimilarToOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.SIMILAR_TO, VmName.EXPAND);

    // like LIKE
    f.checkBoolean("''  similar to ''", true);
    f.checkBoolean("'a' similar to 'a'", true);
    f.checkBoolean("'a' similar to 'b'", false);
    f.checkBoolean("'a' similar to 'A'", false);
    f.checkBoolean("'a' similar to 'a_'", false);
    f.checkBoolean("'a' similar to '_a'", false);
    f.checkBoolean("'a' similar to '%a'", true);
    f.checkBoolean("'a' similar to '%a%'", true);
    f.checkBoolean("'a' similar to 'a%'", true);
    f.checkBoolean("'ab'   similar to 'a_'", true);
    f.checkBoolean("'abc'  similar to 'a_'", false);
    f.checkBoolean("'abcd' similar to 'a%'", true);
    f.checkBoolean("'ab'   similar to '_b'", true);
    f.checkBoolean("'abcd' similar to '_d'", false);
    f.checkBoolean("'abcd' similar to '%d'", true);
    f.checkBoolean("'ab\ncd' similar to 'ab%'", true);
    f.checkBoolean("'abc\ncd' similar to 'ab%'", true);
    f.checkBoolean("'123\n\n45\n' similar to '%'", true);
    f.checkBoolean("'ab\ncd\nef' similar to '%cd%'", true);
    f.checkBoolean("'ab\ncd\nef' similar to '%cde%'", false);

    // simple regular expressions
    // ab*c+d matches acd, abcd, acccd, abcccd but not abd, aabc
    f.checkBoolean("'acd'    similar to 'ab*c+d'", true);
    f.checkBoolean("'abcd'   similar to 'ab*c+d'", true);
    f.checkBoolean("'acccd'  similar to 'ab*c+d'", true);
    f.checkBoolean("'abcccd' similar to 'ab*c+d'", true);
    f.checkBoolean("'abd'    similar to 'ab*c+d'", false);
    f.checkBoolean("'aabc'   similar to 'ab*c+d'", false);

    // compound regular expressions
    // x(ab|c)*y matches xy, xccy, xababcy but not xbcy
    f.checkBoolean("'xy'      similar to 'x(ab|c)*y'", true);
    f.checkBoolean("'xccy'    similar to 'x(ab|c)*y'", true);
    f.checkBoolean("'xababcy' similar to 'x(ab|c)*y'", true);
    f.checkBoolean("'xbcy'    similar to 'x(ab|c)*y'", false);

    // x(ab|c)+y matches xccy, xababcy but not xy, xbcy
    f.checkBoolean("'xy'      similar to 'x(ab|c)+y'", false);
    f.checkBoolean("'xccy'    similar to 'x(ab|c)+y'", true);
    f.checkBoolean("'xababcy' similar to 'x(ab|c)+y'", true);
    f.checkBoolean("'xbcy'    similar to 'x(ab|c)+y'", false);

    f.checkBoolean("'ab' similar to 'a%' ", true);
    f.checkBoolean("'a' similar to 'a%' ", true);
    f.checkBoolean("'abcd' similar to 'a_' ", false);
    f.checkBoolean("'abcd' similar to 'a%' ", true);
    f.checkBoolean("'1a' similar to '_a' ", true);
    f.checkBoolean("'123aXYZ' similar to '%a%'", true);

    f.checkBoolean("'123aXYZ' similar to '_%_a%_' ", true);

    f.checkBoolean("'xy' similar to '(xy)' ", true);

    f.checkBoolean("'abd' similar to '[ab][bcde]d' ", true);

    f.checkBoolean("'bdd' similar to '[ab][bcde]d' ", true);

    f.checkBoolean("'abd' similar to '[ab]d' ", false);
    f.checkBoolean("'cd' similar to '[a-e]d' ", true);
    f.checkBoolean("'amy' similar to 'amy|fred' ", true);
    f.checkBoolean("'fred' similar to 'amy|fred' ", true);

    f.checkBoolean("'mike' similar to 'amy|fred' ", false);

    f.checkBoolean("'acd' similar to 'ab*c+d' ", true);
    f.checkBoolean("'accccd' similar to 'ab*c+d' ", true);
    f.checkBoolean("'abd' similar to 'ab*c+d' ", false);
    f.checkBoolean("'aabc' similar to 'ab*c+d' ", false);
    f.checkBoolean("'abb' similar to 'a(b{3})' ", false);
    f.checkBoolean("'abbb' similar to 'a(b{3})' ", true);

    f.checkBoolean("'abbbbb' similar to 'a(b{3})' ", false);

    f.checkBoolean("'abbbbb' similar to 'ab{3,6}' ", true);

    f.checkBoolean("'abbbbbbbb' similar to 'ab{3,6}' ", false);
    f.checkBoolean("'' similar to 'ab?' ", false);
    f.checkBoolean("'a' similar to 'ab?' ", true);
    f.checkBoolean("'a' similar to 'a(b?)' ", true);
    f.checkBoolean("'ab' similar to 'ab?' ", true);
    f.checkBoolean("'ab' similar to 'a(b?)' ", true);
    f.checkBoolean("'abb' similar to 'ab?' ", false);

    f.checkBoolean("'ab' similar to 'a\\_' ESCAPE '\\' ", false);
    f.checkBoolean("'ab' similar to 'a\\%' ESCAPE '\\' ", false);
    f.checkBoolean("'a_' similar to 'a\\_' ESCAPE '\\' ", true);
    f.checkBoolean("'a%' similar to 'a\\%' ESCAPE '\\' ", true);

    f.checkBoolean("'a(b{3})' similar to 'a(b{3})' ", false);
    f.checkBoolean("'a(b{3})' similar to 'a\\(b\\{3\\}\\)' ESCAPE '\\' ", true);

    f.checkBoolean("'yd' similar to '[a-ey]d'", true);
    // Note: carets were escaped (doubled) in some expressions
    f.checkBoolean("'yd' similar to '[^^a-ey]d'", false);
    f.checkBoolean("'yd' similar to '[^^a-ex-z]d'", false);
    f.checkBoolean("'yd' similar to '[a-ex-z]d'", true);
    f.checkBoolean("'yd' similar to '[x-za-e]d'", true);
    f.checkBoolean("'yd' similar to '[^^a-ey]?d'", false);
    f.checkBoolean("'yyyd' similar to '[a-ey]*d'", true);

    // range must be specified in []
    f.checkBoolean("'yd' similar to 'x-zd'", false);
    f.checkBoolean("'y' similar to 'x-z'", false);

    f.checkBoolean("'cd' similar to '([a-e])d'", true);
    f.checkBoolean("'xy' similar to 'x*?y'", true);
    f.checkBoolean("'y' similar to 'x*?y'", true);
    f.checkBoolean("'y' similar to '(x?)*y'", true);
    f.checkBoolean("'y' similar to 'x+?y'", false);

    f.checkBoolean("'y' similar to 'x?+y'", true);
    f.checkBoolean("'y' similar to 'x*+y'", true);

    // dot is a wildcard for SIMILAR TO but not LIKE
    f.checkBoolean("'abc' similar to 'a.c'", true);
    f.checkBoolean("'a.c' similar to 'a.c'", true);
    f.checkBoolean("'abcd' similar to 'a.*d'", true);
    f.checkBoolean("'abc' like 'a.c'", false);
    f.checkBoolean("'a.c' like 'a.c'", true);
    f.checkBoolean("'abcd' like 'a.*d'", false);

    // some negative tests
    f.checkFails("'y' similar to 'x+*y'", ".*Dangling meta character '\\*' near index 2\n"
        + "x\\+\\*y\n"
        + "  \\^.*", true);
    f.checkFails("'y' similar to 'x?*y'", ".*Dangling meta character '\\*' near index 2\n"
        + "x\\?\\*y\n"
        + "  \\^.*", true);

    f.checkFails("'yd' similar to '[x-ze-a]d'",
        ".*Illegal character range near index 6\n"
            + "\\[x-ze-a\\]d\n"
            + "      \\^",
        true);   // illegal range
    // Slightly different error message from JDK 13 onwards
    final String expectedError =
        TestUtil.getJavaMajorVersion() >= 13
            ? ".*Illegal repetition near index 22\n"
              + "\\[\\:LOWER\\:\\]\\{2\\}\\[\\:DIGIT\\:\\]\\{,5\\}\n"
              + "                      \\^"
            : ".*Illegal repetition near index 20\n"
                + "\\[\\:LOWER\\:\\]\\{2\\}\\[\\:DIGIT\\:\\]\\{,5\\}\n"
                + "                    \\^";
    f.checkFails("'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{,5}'",
        expectedError, true);
    f.checkFails("'cd' similar to '[(a-e)]d' ",
        ".*Invalid regular expression '\\[\\(a-e\\)\\]d', index 1",
        true);
    f.checkFails("'yd' similar to '[(a-e)]d' ",
        ".*Invalid regular expression '\\[\\(a-e\\)\\]d', index 1",
        true);

    // all the following tests wrong results due to missing functionality
    // or defect (FRG-375, 377).

    if (Bug.FRG375_FIXED) {
      f.checkBoolean("'cd' similar to '[a-e^c]d' ", false); // FRG-375
    }

    // following tests use regular character set identifiers.
    // Not implemented yet. FRG-377.
    if (Bug.FRG377_FIXED) {
      f.checkBoolean("'y' similar to '[:ALPHA:]*'", true);
      f.checkBoolean("'yd32' similar to '[:LOWER:]{2}[:DIGIT:]*'", true);
      f.checkBoolean("'yd32' similar to '[:ALNUM:]*'", true);
      f.checkBoolean("'yd32' similar to '[:ALNUM:]*[:DIGIT:]?'", true);
      f.checkBoolean("'yd32' similar to '[:ALNUM:]?[:DIGIT:]*'", false);
      f.checkBoolean("'yd3223' similar to '([:LOWER:]{2})[:DIGIT:]{2,5}'",
          true);
      f.checkBoolean("'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{2,}'", true);
      f.checkBoolean("'yd3223' similar to '[:LOWER:]{2}||[:DIGIT:]{4}'", true);
      f.checkBoolean("'yd3223' similar to '[:LOWER:]{2}[:DIGIT:]{3}'", false);
      f.checkBoolean("'yd  3223' similar to '[:UPPER:]{2}  [:DIGIT:]{3}'",
          false);
      f.checkBoolean("'YD  3223' similar to '[:UPPER:]{2}  [:DIGIT:]{3}'",
          false);
      f.checkBoolean("'YD  3223' similar to "
          + "'[:UPPER:]{2}||[:WHITESPACE:]*[:DIGIT:]{4}'", true);
      f.checkBoolean("'YD\t3223' similar to "
          + "'[:UPPER:]{2}[:SPACE:]*[:DIGIT:]{4}'", false);
      f.checkBoolean("'YD\t3223' similar to "
          + "'[:UPPER:]{2}[:WHITESPACE:]*[:DIGIT:]{4}'", true);
      f.checkBoolean("'YD\t\t3223' similar to "
          + "'([:UPPER:]{2}[:WHITESPACE:]+)||[:DIGIT:]{4}'", true);
    }
  }

  @Test void testEscapeOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ESCAPE, VM_EXPAND);
  }

  @Test void testConvertFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CONVERT, VM_JAVA);
    f.checkFails("convert('a', utf8, utf10)", "UTF10", false);
    f.checkFails("select ^convert(col, latin1, utf8)^\n"
            + "from (select 1 as col\n"
            + " from (values(true)))",
        "Invalid type 'INTEGER NOT NULL' in 'CONVERT' function\\. "
            + "Only 'CHARACTER' type is supported",
        false);
    f.check("select convert(col, latin1, utf8)\n"
            + "from (select 'a' as col\n"
            + " from (values(true)))",
        SqlTests.ANY_TYPE_CHECKER, 'a');

    f.checkType("convert('a', utf16, gbk)", "CHAR(1) NOT NULL");
    f.checkType("convert(null, utf16, gbk)", "NULL");
    f.checkType("convert(cast(1 as varchar(2)), utf8, latin1)", "VARCHAR(2) NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6146">[CALCITE-6146]
   * Target charset should be used when comparing two strings through
   * CONVERT/TRANSLATE function during validation</a>. */
  @Test void testStringComparisonWithConvertFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CONVERT, VM_JAVA);
    f.check("select 'a' as alia\n"
            + " from (values(true)) where convert('col', utf8, latin1)='col'",
        SqlTests.ANY_TYPE_CHECKER, 'a');
    f.check("select 'a' as alia\n"
            + " from (values(true)) where convert('col', utf8, gbk)=_GBK'col'",
        SqlTests.ANY_TYPE_CHECKER, 'a');
    f.check("select 'a' as alia\n"
            + " from (values(true)) where convert(null, utf8, gbk) is null",
        SqlTests.ANY_TYPE_CHECKER, 'a');
    f.checkFails("select 'a' as alia\n"
            + " from (values(true)) where ^convert('col', utf8, gbk)='col'^",
        "Cannot apply operation '=' to strings with "
            + "different charsets 'GBK' and 'ISO-8859-1'",
        false);

    // cast check
    f.check("select 'a' as alia\n"
            + " from (values(true)) where cast(convert('col', utf8, latin1) as char(3))='col'",
        SqlTests.ANY_TYPE_CHECKER, 'a');
    f.checkFails("select 'a' as alia\n"
            + " from (values(true)) where ^cast(convert('col', utf8, latin1) as char(3))=_GBK'col'^",
        "Cannot apply operation '=' to strings with "
            + "different charsets 'ISO-8859-1' and 'GBK'",
        false);
    // the result of convert('col', utf8, gbk) has GBK charset
    // while CHAR(3) has ISO-8859-1 charset, which is not allowed to cast
    f.checkFails("select 'a' as alia\n"
            + " from (values(true)) where ^cast(convert('col', utf8, gbk) as char(3))^=_GBK'col'",
        "Cast function cannot convert value of type "
            + "CHAR\\(3\\) CHARACTER SET \"GBK\" NOT NULL to type CHAR\\(3\\) NOT NULL",
        false);
  }

  @Test void testStringComparisonWithTranslateFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.TRANSLATE, VM_JAVA);
    f.check("select 'a' as alia\n"
            + " from (values(true)) where translate('col' using latin1)='col'",
        SqlTests.ANY_TYPE_CHECKER, 'a');
    f.check("select 'a' as alia\n"
            + " from (values(true)) where convert('col' using gbk)=_GBK'col'",
        SqlTests.ANY_TYPE_CHECKER, 'a');
    f.check("select 'a' as alia\n"
            + " from (values(true)) where convert(null using gbk) is null",
        SqlTests.ANY_TYPE_CHECKER, 'a');
    f.checkFails("select 'a' as alia\n"
            + " from (values(true)) where ^translate('col' using gbk)='col'^",
        "Cannot apply operation '=' to strings with "
            + "different charsets 'GBK' and 'ISO-8859-1'",
        false);

    // cast check
    f.check("select 'a' as alia\n"
            + " from (values(true)) where cast(convert('col' using latin1) as char(3))='col'",
        SqlTests.ANY_TYPE_CHECKER, 'a');
    f.checkFails("select 'a' as alia\n"
            + " from (values(true)) where ^cast(translate('col' using latin1) as char(3))=_GBK'col'^",
        "Cannot apply operation '=' to strings with "
            + "different charsets 'ISO-8859-1' and 'GBK'",
        false);
    // the result of translate('col' using gbk) has GBK charset
    // while CHAR(3) has ISO-8859-1 charset, which is not allowed to cast
    f.checkFails("select 'a' as alia\n"
            + " from (values(true)) where ^cast(translate('col' using gbk) as char(3))^=_GBK'col'",
        "Cast function cannot convert value of type "
            + "CHAR\\(3\\) CHARACTER SET \"GBK\" NOT NULL to type CHAR\\(3\\) NOT NULL",
        false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6730">[CALCITE-6730]
   * Add CONVERT function(enabled in Oracle library)</a>. */
  @Test void testConvertOracleFunc() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.CONVERT_ORACLE, VM_JAVA)
        .withLibrary(SqlLibrary.ORACLE);

    final Consumer<SqlOperatorFixture> consumer = f0 -> {
      f0.checkFails("convert('a', utf8, utf10)", "UTF10", false);
      f0.checkFails("convert('a', utf8, ^null^)",
          "(?s).*Encountered \\\"null\\\" at.*", false);
      f0.checkFails("convert('a', ^null^, utf8)",
          "(?s).*Encountered \\\"null\\\" at.*", false);
      f0.checkFails("^convert(1, utf8, gbk)^",
          "Invalid type 'INTEGER NOT NULL' in 'CONVERT' function\\. "
              + "Only 'CHARACTER' type is supported",
          false);
      f0.checkType("convert('a', utf16, gbk)", "CHAR(1) NOT NULL");
      f0.checkType("convert('a', utf16)", "CHAR(1) NOT NULL");
      f0.checkType("convert(null, utf16, gbk)", "NULL");
      f0.checkType("convert('', utf16, gbk)", "CHAR(0) NOT NULL");
      f0.checkType("convert(cast(1 as varchar(2)), utf8, latin1)", "VARCHAR(2) NOT NULL");

      // cast check
      f.check("select 'a' as alia\n"
              + " from (values(true)) where cast(convert('col', latin1) as char(3))='col'",
          SqlTests.ANY_TYPE_CHECKER, 'a');
      f.checkFails("select 'a' as alia\n"
              + " from (values(true)) where ^cast(convert('col', latin1) as char(3))=_GBK'col'^",
          "Cannot apply operation '=' to strings with "
              + "different charsets 'ISO-8859-1' and 'GBK'",
          false);
      // the result of convert('col', gbk) has GBK charset
      // while CHAR(3) has ISO-8859-1 charset, which is not allowed to cast
      f.checkFails("select 'a' as alia\n"
              + " from (values(true)) where ^cast(convert('col', gbk) as char(3))^=_GBK'col'",
          "Cast function cannot convert value of type "
              + "CHAR\\(3\\) CHARACTER SET \"GBK\" NOT NULL to type CHAR\\(3\\) NOT NULL",
          false);
    };

    final List<SqlConformanceEnum> conformances =
        list(SqlConformanceEnum.ORACLE_10, SqlConformanceEnum.ORACLE_12);
    f.forEachConformance(conformances, consumer);
  }

  @Test void testTranslateFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.TRANSLATE, VM_JAVA);
    f.checkFails("translate('a' using utf10)", "UTF10", false);
    f.checkFails("convert('a' using utf10)", "UTF10", false);

    f.checkFails("select ^translate(col using utf8)^\n"
            + "from (select 1 as col\n"
            + " from (values(true)))",
            "Invalid type 'INTEGER NOT NULL' in 'TRANSLATE' function\\. "
            + "Only 'CHARACTER' type is supported",
            false);
    f.checkFails("select ^convert(col using utf8)^\n"
            + "from (select 1 as col\n"
            + " from (values(true)))",
            "Invalid type 'INTEGER NOT NULL' in 'TRANSLATE' function\\. "
            + "Only 'CHARACTER' type is supported",
            false);

    f.check("select translate(col using utf8)\n"
            + "from (select 'a' as col\n"
            + " from (values(true)))",
        SqlTests.ANY_TYPE_CHECKER, 'a');
    f.check("select convert(col using utf8)\n"
            + "from (select 'a' as col\n"
            + " from (values(true)))",
        SqlTests.ANY_TYPE_CHECKER, 'a');

    f.checkType("translate('a' using gbk)", "CHAR(1) NOT NULL");
    f.checkType("convert('a' using gbk)", "CHAR(1) NOT NULL");
    f.checkType("translate(null using utf16)", "NULL");
    f.checkType("convert(null using utf16)", "NULL");
    f.checkType("translate(cast(1 as varchar(2)) using latin1)", "VARCHAR(2) NOT NULL");
    f.checkType("convert(cast(1 as varchar(2)) using latin1)", "VARCHAR(2) NOT NULL");
  }

  @Test void testTranslate3Func() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.TRANSLATE3);
    f0.checkFails("^translate('aabbcc', 'ab', '+-')^",
        "No match found for function signature "
            + "TRANSLATE3\\(<CHARACTER>, <CHARACTER>, <CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("translate('aabbcc', 'ab', '+-')",
          "++--cc", "VARCHAR(6) NOT NULL");
      f.checkString("translate('aabbcc', 'ab', 'ba')",
          "bbaacc", "VARCHAR(6) NOT NULL");
      f.checkString("translate('aabbcc', 'ab', '')",
          "cc", "VARCHAR(6) NOT NULL");
      f.checkString("translate('aabbcc', '', '+-')",
          "aabbcc", "VARCHAR(6) NOT NULL");
      f.checkString("translate(cast('aabbcc' as varchar(10)), 'ab', '+-')",
          "++--cc", "VARCHAR(10) NOT NULL");
      f.checkNull("translate(cast(null as varchar(7)), 'ab', '+-')");
      f.checkNull("translate('aabbcc', cast(null as varchar(2)), '+-')");
      f.checkNull("translate('aabbcc', 'ab', cast(null as varchar(2)))");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.ORACLE, SqlLibrary.POSTGRESQL,
            SqlLibrary.REDSHIFT, SqlLibrary.SPARK);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testOverlayFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.OVERLAY, VmName.EXPAND);
    f.checkString("overlay('ABCdef' placing 'abc' from 1)",
        "abcdef", "VARCHAR(9) NOT NULL");
    f.checkString("overlay('ABCdef' placing 'abc' from 1 for 2)",
        "abcCdef", "VARCHAR(9) NOT NULL");
    f.checkString("overlay(cast('ABCdef' as varchar(10)) placing "
            + "cast('abc' as char(5)) from 1 for 2)",
        "abc  Cdef", "VARCHAR(15) NOT NULL");
    f.checkString("overlay(cast('ABCdef' as char(10)) placing "
            + "cast('abc' as char(5)) from 1 for 2)",
        "abc  Cdef    ",
        "VARCHAR(15) NOT NULL");
    f.checkNull("overlay('ABCdef' placing 'abc'"
        + " from 1 for cast(null as integer))");
    f.checkNull("overlay(cast(null as varchar(1)) placing 'abc' from 1)");

    f.checkString("overlay(x'ABCdef' placing x'abcd' from 1)",
        "abcdef", "VARBINARY(5) NOT NULL");
    f.checkString("overlay(x'ABCDEF1234' placing x'2345' from 1 for 2)",
        "2345ef1234", "VARBINARY(7) NOT NULL");
    if (f.brokenTestsEnabled()) {
      f.checkString("overlay(cast(x'ABCdef' as varbinary(5)) placing "
              + "cast(x'abcd' as binary(3)) from 1 for 2)",
          "abc  Cdef", "VARBINARY(8) NOT NULL");
    }
    if (f.brokenTestsEnabled()) {
      f.checkString("overlay(cast(x'ABCdef' as binary(5)) placing "
              + "cast(x'abcd' as binary(3)) from 1 for 2)",
          "abc  Cdef    ", "VARBINARY(8) NOT NULL");
    }
    f.checkNull("overlay(x'ABCdef' placing x'abcd'"
        + " from 1 for cast(null as integer))");
    f.checkNull("overlay(cast(null as varbinary(1)) placing x'abcd' from 1)");
    f.checkNull("overlay(x'abcd' placing x'abcd' from cast(null as integer))");
  }

  @Test void testPositionFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.POSITION, VmName.EXPAND);
    f.checkScalarExact("position('b' in 'abc')", 2);
    f.checkScalarExact("position('' in 'abc')", 1);
    f.checkScalarExact("position('b' in 'abcabc' FROM 3)", 5);
    f.checkScalarExact("position('b' in 'abcabc' FROM 5)", 5);
    f.checkScalarExact("position('b' in 'abcabc' FROM 6)", 0);
    f.checkScalarExact("position('b' in 'abcabc' FROM -5)", 2);
    f.checkScalarExact("position('' in 'abc' FROM 3)", 3);
    f.checkScalarExact("position('' in 'abc' FROM 10)", 0);

    f.checkScalarExact("position(x'bb' in x'aabbcc')", 2);
    f.checkScalarExact("position(x'' in x'aabbcc')", 1);
    f.checkScalarExact("position(x'bb' in x'aabbccaabbcc' FROM 3)", 5);
    f.checkScalarExact("position(x'bb' in x'aabbccaabbcc' FROM 5)", 5);
    f.checkScalarExact("position(x'bb' in x'aabbccaabbcc' FROM 6)", 0);
    f.checkScalarExact("position(x'bb' in x'aabbccaabbcc' FROM -5)", 2);
    f.checkScalarExact("position(x'cc' in x'aabbccdd' FROM 2)", 3);
    f.checkScalarExact("position(x'' in x'aabbcc' FROM 3)", 3);
    f.checkScalarExact("position(x'' in x'aabbcc' FROM 10)", 0);

    // FRG-211
    f.checkScalarExact("position('tra' in 'fdgjklewrtra')", 10);

    f.checkNull("position(cast(null as varchar(1)) in '0010')");
    f.checkNull("position('a' in cast(null as varchar(1)))");

    f.checkScalar("position(cast('a' as char) in cast('bca' as varchar))",
        3, "INTEGER NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6774">[CALCITE-6774]
   * REPLACE function returns wrong result when search pattern is an empty string</a>. */
  @Test void testReplaceFunc() {
    final SqlOperatorFixture f = fixture();
    checkReplaceFunc(f);
    // case-sensitive
    f.checkString("REPLACE('ciAao', 'a', 'ciao')", "ciAciaoo",
        "VARCHAR NOT NULL");
    f.checkString("REPLACE('ciAao', 'A', 'ciao')", "ciciaoao",
        "VARCHAR NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6791">[CALCITE-6791]
   * Search pattern during matching in REPLACE function should be case insensitive
   * in MSSQL</a>. */
  @Test void testReplaceMSSQLFunc() {
    final SqlOperatorFixture f = fixture();
    checkReplaceFunc(f);
    // case-insensitive
    SqlOperatorFixture f1 = f.withConformance(SqlConformanceEnum.SQL_SERVER_2008);
    f1.checkString("REPLACE('ciAao', 'a', 'ciao')", "ciciaociaoo",
        "VARCHAR NOT NULL");
    f1.checkString("REPLACE('ciAao', 'A', 'ciao')", "ciciaociaoo",
        "VARCHAR NOT NULL");
  }

  private static void checkReplaceFunc(SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.REPLACE, VmName.EXPAND);
    f.checkString("REPLACE('ciao', 'ciao', '')", "",
        "VARCHAR NOT NULL");
    f.checkString("REPLACE('ciao', '', 'ciao')", "ciao",
        "VARCHAR NOT NULL");
    f.checkString("REPLACE('ci ao', ' ', 'ciao')", "ciciaoao",
        "VARCHAR NOT NULL");
    f.checkString("REPLACE('', 'ciao', 'ciao')", "",
        "VARCHAR NOT NULL");
    f.checkString("REPLACE('hello world', 'o', '')", "hell wrld",
        "VARCHAR NOT NULL");
    f.checkNull("REPLACE(cast(null as varchar(5)), 'ciao', '')");
    f.checkNull("REPLACE('ciao', cast(null as varchar(3)), 'zz')");
    f.checkNull("REPLACE('ciao', 'bella', cast(null as varchar(3)))");
  }

  @Test void testCharLengthFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CHAR_LENGTH, VmName.EXPAND);
    f.checkScalarExact("char_length('abc')", 3);
    f.checkNull("char_length(cast(null as varchar(1)))");
    checkCharLength(f, FunctionAlias.of(SqlStdOperatorTable.CHAR_LENGTH));
  }

  @Test void testCharacterLengthFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CHARACTER_LENGTH, VmName.EXPAND);
    f.checkScalarExact("CHARACTER_LENGTH('abc')", 3);
    f.checkNull("CHARACTER_LENGTH(cast(null as varchar(1)))");
    checkCharLength(f, FunctionAlias.of(SqlStdOperatorTable.CHARACTER_LENGTH));
  }

  @Test void testLenFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.LEN);
    f0.checkFails("^len('hello')^",
        "No match found for function signature LEN\\(<CHARACTER>\\)",
        false);
    checkCharLength(f0, FunctionAlias.of(SqlLibraryOperators.LEN));
  }

  @Test void testLengthFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.LENGTH);
    f0.checkFails("^length('hello')^",
        "No match found for function signature LENGTH\\(<CHARACTER>\\)",
        false);
    checkCharLength(f0, FunctionAlias.of(SqlLibraryOperators.LENGTH));
  }

  /** Common tests for {@code CHAR_LENGTH}, {@code CHARACTER_LENGTH}, {@code LEN},
   * and {@code LENGTH} functions. */
  void checkCharLength(SqlOperatorFixture f0, FunctionAlias functionAlias) {
    final SqlFunction function = functionAlias.function;
    final String fn = function.getName();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalarExact(fn + "('abc')", 3);
      f.checkNull(fn + "(cast(null as varchar(1)))");
      f.checkScalar(fn + "('')", "0", "INTEGER NOT NULL");
      f.checkScalar(fn + "(CAST('x' as CHAR(3)))", "3", "INTEGER NOT NULL");
      f.checkScalar(fn + "(CAST('x' as VARCHAR(4)))", "1", "INTEGER NOT NULL");
    };
    f0.forEachLibrary(functionAlias.libraries, consumer);
  }

  @Test void testOctetLengthFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.OCTET_LENGTH, VmName.EXPAND);
    f.checkScalarExact("OCTET_LENGTH(x'aabbcc')", 3);
    f.checkNull("OCTET_LENGTH(cast(null as varbinary(1)))");
  }

  @Test void testBitLengthFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.BIT_LENGTH, VmName.EXPAND);
    f0.checkFails("^bit_length('Apache Calcite')^",
        "No match found for function signature BIT_LENGTH\\(<CHARACTER>\\)", false);

    final SqlOperatorFixture f1 = f0.withLibrary(SqlLibrary.SPARK);
    // string
    f1.checkScalarExact("BIT_LENGTH('Apache Calcite')", 112);
    f1.checkNull("BIT_LENGTH(cast(null as varchar(1)))");
    // binary string
    f1.checkScalarExact("BIT_LENGTH(x'4170616368652043616C63697465')", 112);
    f1.checkNull("BIT_LENGTH(cast(null as binary))");
  }

  /** Generates parameters to test both BIT_GET and GETBIT functions. */
  static Stream<Arguments> bitGetParameters() {
    SqlOperatorFixture f0 = SqlOperatorFixtureImpl.DEFAULT.withTester(t -> TESTER);
    f0.setFor(SqlLibraryOperators.BIT_GET, VmName.EXPAND);
    SqlOperatorFixture f1 = SqlOperatorFixtureImpl.DEFAULT.withTester(t -> TESTER);
    f1.setFor(SqlLibraryOperators.GETBIT, VmName.EXPAND);
    return Stream.of(
        () -> new Object[] {f0, "BIT_GET"},
        () -> new Object[] {f1, "GETBIT"});
  }

  @ParameterizedTest
  @MethodSource("bitGetParameters")
  void testBitGetFunc(SqlOperatorFixture f, String functionName) {
    f.checkFails("^" + functionName + "(cast(11 as bigint), 1)^",
        "No match found for function signature " + functionName + "\\(<NUMERIC>, <NUMERIC>\\)",
        false);

    final SqlOperatorFixture f1 = f.withLibrary(SqlLibrary.SPARK);
    // test for bigint value
    f1.checkScalarExact(functionName + "(cast(11 as bigint), 3)",
        "TINYINT NOT NULL", "1");
    f1.checkScalarExact(functionName + "(cast(-1 as bigint), 0)",
        "TINYINT NOT NULL", "1");
    f1.checkScalarExact(functionName + "(cast(11 as bigint), 63)",
        "TINYINT NOT NULL", "0");
    f1.checkScalarExact(functionName + "(cast(-1 as bigint), 63)",
        "TINYINT NOT NULL", "1");
    f1.checkFails(functionName + "(cast(11 as bigint), 64)",
        "BIT_GET/GETBIT error: position 64 exceeds the bit upper limit 63",
        true);
    f1.checkFails(functionName + "(cast(11 as bigint), -1)",
        "BIT_GET/GETBIT error: negative position -1 not allowed",
        true);
    f1.checkNull(functionName + "(cast(null as bigint), 1)");
    f1.checkNull(functionName + "(cast(11 as bigint), cast(null as integer))");

    // test for integer value
    f1.checkScalarExact(functionName + "(cast(11 as integer), 3)",
        "TINYINT NOT NULL", "1");
    f1.checkScalarExact(functionName + "(cast(-1 as integer), 0)",
        "TINYINT NOT NULL", "1");
    f1.checkScalarExact(functionName + "(cast(11 as integer), 31)",
        "TINYINT NOT NULL", "0");
    f1.checkScalarExact(functionName + "(cast(-1 as integer), 31)",
        "TINYINT NOT NULL", "1");
    f1.checkFails(functionName + "(cast(11 as integer), 32)",
        "BIT_GET/GETBIT error: position 32 exceeds the bit upper limit 31",
        true);

    // test for smallint value
    f1.checkScalarExact(functionName + "(cast(11 as smallint), 3)",
        "TINYINT NOT NULL", "1");
    f1.checkScalarExact(functionName + "(cast(-1 as smallint), 0)",
        "TINYINT NOT NULL", "1");
    f1.checkScalarExact(functionName + "(cast(11 as smallint), 15)",
        "TINYINT NOT NULL", "0");
    f1.checkScalarExact(functionName + "(cast(-1 as smallint), 15)",
        "TINYINT NOT NULL", "1");
    f1.checkFails(functionName + "(cast(11 as smallint), 16)",
        "BIT_GET/GETBIT error: position 16 exceeds the bit upper limit 15",
        true);

    // test for tinyint value
    f1.checkScalarExact(functionName + "(cast(11 as tinyint), 3)",
        "TINYINT NOT NULL", "1");
    f1.checkScalarExact(functionName + "(cast(-1 as tinyint), 0)",
        "TINYINT NOT NULL", "1");
    f1.checkScalarExact(functionName + "(cast(11 as tinyint), 7)",
        "TINYINT NOT NULL", "0");
    f1.checkScalarExact(functionName + "(cast(-1 as tinyint), 7)",
        "TINYINT NOT NULL", "1");
    f1.checkFails(functionName + "(cast(11 as tinyint), 8)",
        "BIT_GET/GETBIT error: position 8 exceeds the bit upper limit 7",
        true);
  }

  @Test void testAsciiFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ASCII, VmName.EXPAND);
    f.checkScalarExact("ASCII('')", 0);
    f.checkScalarExact("ASCII('a')", 97);
    f.checkScalarExact("ASCII('1')", 49);
    f.checkScalarExact("ASCII('abc')", 97);
    f.checkScalarExact("ASCII('ABC')", 65);
    f.checkScalarExact("ASCII(_UTF8'\u0082')", 130);
    f.checkScalarExact("ASCII(_UTF8'\u5B57')", 23383);
    f.checkScalarExact("ASCII(_UTF8'\u03a9')", 937); // omega
    f.checkNull("ASCII(cast(null as varchar(1)))");
  }

  @Test void testParseUrl() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.PARSE_URL);
    f0.checkFails("^parse_url('http://calcite.apache.org/path1', 'HOST')^",
        "No match found for function signature PARSE_URL\\(<CHARACTER>, <CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      // test with each valid partToExtract
      f.checkString("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " 'HOST')",
          "calcite.apache.org",
          "VARCHAR");
      f.checkString("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " 'PATH')",
          "/path1/p.php",
          "VARCHAR");
      f.checkString("parse_url('http://calcite.apache.org/path1/%20p.php?k1=v1&k2=v2#Ref1',"
              + " 'PATH')",
          "/path1/%20p.php",
          "VARCHAR");
      f.checkString("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " 'QUERY')",
          "k1=v1&k2=v2",
          "VARCHAR");
      f.checkString("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " 'REF')",
          "Ref1",
          "VARCHAR");
      f.checkString("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " 'QUERY', 'k2')",
          "v2",
          "VARCHAR");
      f.checkString("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " 'QUERY', 'k1')",
          "v1",
          "VARCHAR");
      f.checkNull("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
          + " 'QUERY', 'k3')");
      f.checkString("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " 'FILE')",
          "/path1/p.php?k1=v1&k2=v2",
          "VARCHAR");
      f.checkString("parse_url('http://calcite.apache.org/path1/p.php',"
              + " 'FILE')",
          "/path1/p.php",
          "VARCHAR");
      f.checkString("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " 'PROTOCOL')",
          "http",
          "VARCHAR");
      f.checkString("parse_url('https://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " 'PROTOCOL')",
          "https",
          "VARCHAR");
      f.checkString("parse_url('http://bob@calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " 'USERINFO')",
          "bob",
          "VARCHAR");
      f.checkNull("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
          + " 'USERINFO')");
      f.checkString("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " 'AUTHORITY')",
          "calcite.apache.org",
          "VARCHAR");

      // test with invalid partToExtract
      f.checkNull("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
          + " 'INVALID_PART_TO_EXTRACT')");
      f.checkNull("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
          + " 'HOST', 'k1')");

      // test with invalid urlString
      f.checkNull("parse_url('http:calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST')");
      f.checkNull("parse_url('calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST')");
      f.checkNull("parse_url('/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST')");

      // test with operands with null values
      f.checkNull("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
              + " cast(null as varchar))");
      f.checkNull("parse_url('http://calcite.apache.org/path1/p.php?k1=v1&k2=v2#Ref1',"
          + " cast(null as varchar), cast(null as varchar))");
      f.checkNull("parse_url(cast(null as varchar), cast(null as varchar))");
      f.checkNull("parse_url(cast(null as varchar), cast(null as varchar), cast(null as varchar))");
    };
    f0.forEachLibrary(list(SqlLibrary.HIVE, SqlLibrary.SPARK), consumer);
  }

  @Test void testToBase64() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.MYSQL);
    f.setFor(SqlLibraryOperators.TO_BASE64);
    f.checkString("to_base64(x'546869732069732061207465737420537472696e672e')",
        "VGhpcyBpcyBhIHRlc3QgU3RyaW5nLg==",
        "VARCHAR NOT NULL");
    f.checkString("to_base64(x'546869732069732061207465737420537472696e672e20636865"
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
    f.checkString("to_base64('This is a test String.')",
        "VGhpcyBpcyBhIHRlc3QgU3RyaW5nLg==",
        "VARCHAR NOT NULL");
    f.checkString("to_base64('This is a test String. check resulte out of 76T"
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
    f.checkString("to_base64('')", "", "VARCHAR NOT NULL");
    f.checkString("to_base64('a')", "YQ==", "VARCHAR NOT NULL");
    f.checkString("to_base64(x'61')", "YQ==", "VARCHAR NOT NULL");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6812">[CALCITE-6812]
   * Add base64 function (enabled in Hive library)</a>. */
  @Test void testBase64() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.HIVE);
    f.setFor(SqlLibraryOperators.BASE64);
    f.checkString("base64(cast('a' as binary))", "YQ==", "VARCHAR NOT NULL");
    f.checkString("base64('')", "", "VARCHAR NOT NULL");
    f.checkNull("base64(null)");
    f.checkString("base64('This is a test String.')",
        "VGhpcyBpcyBhIHRlc3QgU3RyaW5nLg==",
        "VARCHAR NOT NULL");
  }

  @Test void testUnBase64() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.UN_BASE64);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("unbase64('VGhpcyBpcyBhIHRlc3QgU3RyaW5nLg==')",
          "546869732069732061207465737420537472696e672e",
          "VARBINARY");
      f.checkString("unbase64('VGhpcyBpcyBhIHRlc\t3QgU3RyaW5nLg==')",
          "546869732069732061207465737420537472696e672e",
          "VARBINARY");
      f.checkString("unbase64('VGhpcyBpcyBhIHRlc\t3QgU3\nRyaW5nLg==')",
          "546869732069732061207465737420537472696e672e",
          "VARBINARY");
      f.checkString("unbase64('VGhpcyB  pcyBhIHRlc3Qg\tU3Ry\naW5nLg==')",
          "546869732069732061207465737420537472696e672e",
          "VARBINARY");
      f.checkNull("unbase64('-1')");
      f.checkNull("unbase64('-100')");
      f.checkNull("unbase64(null)");
    };
    f0.forEachLibrary(list(SqlLibrary.HIVE), consumer);

  }

  @Test void testToChar() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.TO_CHAR);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString(
          "to_char(timestamp '2022-06-03 12:15:48.678', 'YYYY-MM-DD HH24:MI:SS.MS TZ')",
          "2022-06-03 12:15:48.678",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'Day')",
          "Friday",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '0001-01-01 00:00:00.000', 'Day')",
          "Monday",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'DY')",
          "FRI",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '0001-01-01 00:00:00.000', 'DY')",
          "MON",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'CC')",
          "21",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'HH12')",
          "01",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'HH24')",
          "13",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'MI')",
          "15",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'MS')",
          "678",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'Q')",
          "2",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'IW')",
          "22",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'YYYY')",
          "2022",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'YY')",
          "22",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'Month')",
          "June",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'Mon')",
          "Jun",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'MM')",
          "06",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'CC')",
          "21",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'DDD')",
          "154",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'DD')",
          "03",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'D')",
          "6",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'W')",
          "1",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'WW')",
          "23",
          "VARCHAR NOT NULL");
      f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'gggggg')",
          "gggggg",
          "VARCHAR NOT NULL");
      f.checkNull("to_char(timestamp '2022-06-03 12:15:48.678', NULL)");
      f.checkNull("to_char(cast(NULL as timestamp), NULL)");
      f.checkNull("to_char(cast(NULL as timestamp), 'Day')");
      f.checkNull("to_char(NULL, 'Day')");
      f.checkNull("to_char(NULL, NULL)");    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.MYSQL, SqlLibrary.ORACLE, SqlLibrary.REDSHIFT);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testToCharPg() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.POSTGRESQL);
    f.setFor(SqlLibraryOperators.TO_CHAR_PG);

    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'YYYY-MM-DD HH24:MI:SS.MS')",
        "2022-06-03 12:15:48.678",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'Day')",
        "Friday   ",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '0001-01-01 00:00:00.000', 'Day')",
        "Monday   ",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'DY')",
        "FRI",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '0001-01-01 00:00:00.000', 'DY')",
        "MON",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'CC')",
        "21",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'HH')",
        "12",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'HH12')",
        "01",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'HH24')",
        "13",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'MI')",
        "15",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'SS')",
        "48",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'MS')",
        "678",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'US')",
        "678000",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'FF1')",
        "6",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'FF2')",
        "67",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'FF3')",
        "678",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'FF4')",
        "6780",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'FF5')",
        "67800",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'FF6')",
        "678000",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'SSSS')",
        "44148",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'SSSSS')",
        "44148",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'AM')",
        "PM",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'am')",
        "pm",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 02:15:48.678', 'PM')",
        "AM",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 02:15:48.678', 'pm')",
        "am",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'A.M.')",
        "P.M.",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'a.m.')",
        "p.m.",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 02:15:48.678', 'P.M.')",
        "A.M.",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 02:15:48.678', 'p.m.')",
        "a.m.",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'Y,YYY')",
        "2,022",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'YYYY')",
        "2022",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'YYY')",
        "022",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'YY')",
        "22",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'Y')",
        "2",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2023-01-01 12:15:48.678', 'IYYY')",
        "2022",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2023-01-01 12:15:48.678', 'IYY')",
        "022",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2023-01-01 12:15:48.678', 'IY')",
        "22",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2023-01-01 12:15:48.678', 'I')",
        "2",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'BC')",
        "AD",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'bc')",
        "ad",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'AD')",
        "AD",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'ad')",
        "ad",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'B.C.')",
        "A.D.",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'b.c.')",
        "a.d.",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'A.D.')",
        "A.D.",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'a.d.')",
        "a.d.",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'MONTH')",
        "JUNE     ",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'Month')",
        "June     ",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'month')",
        "june     ",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'MON')",
        "JUN",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'Mon')",
        "Jun",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'mon')",
        "jun",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'DAY')",
        "FRIDAY   ",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'Day')",
        "Friday   ",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'day')",
        "friday   ",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'DY')",
        "FRI",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '0001-01-01 00:00:00.000', 'DY')",
        "MON",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'Dy')",
        "Fri",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'dy')",
        "fri",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'DDD')",
        "154",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'IDDD')",
        "152",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'DD')",
        "03",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'D')",
        "6",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'ID')",
        "5",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'W')",
        "1",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'WW')",
        "22",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'IW')",
        "22",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'CC')",
        "21",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 12:15:48.678', 'J')",
        "2459734",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'Q')",
        "2",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'RM')",
        "VI",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'rm')",
        "vi",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'YYYY')",
        "2022",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'YY')",
        "22",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'Month')",
        "June     ",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'Mon')",
        "Jun",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'MM')",
        "06",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'CC')",
        "21",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'DDD')",
        "154",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'DD')",
        "03",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'D')",
        "6",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'W')",
        "1",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'WW')",
        "22",
        "VARCHAR NOT NULL");
    f.checkString("to_char(timestamp '2022-06-03 13:15:48.678', 'gggggg')",
        "gggggg",
        "VARCHAR NOT NULL");
    f.checkNull("to_char(timestamp '2022-06-03 12:15:48.678', NULL)");
    f.checkNull("to_char(cast(NULL as timestamp), NULL)");
    f.checkNull("to_char(cast(NULL as timestamp), 'Day')");
    f.checkNull("to_char(NULL, 'Day')");
    f.checkNull("to_char(NULL, NULL)");
  }

  @Test void testToDate() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.TO_DATE);

    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("to_date('2022-06-03', 'YYYY-MM-DD')",
          "2022-06-03",
          "DATE NOT NULL");
      f.checkString("to_date('0001-01-01', 'YYYY-MM-DD')",
          "0001-01-01",
          "DATE NOT NULL");
      f.checkString("to_date('Jun 03, 2022', 'Mon DD, YYYY')",
          "2022-06-03",
          "DATE NOT NULL");
      f.checkString("to_date('2022-June-03', 'YYYY-Month-DD')",
          "2022-06-03",
          "DATE NOT NULL");
      f.checkString("to_date('2022-Jun-03', 'YYYY-Mon-DD')",
          "2022-06-03",
          "DATE NOT NULL");
      f.checkString("to_date('2022-154', 'YYYY-DDD')",
          "2022-06-03",
          "DATE NOT NULL");
      f.checkFails("to_date('ABCD', 'YYYY-MM-DD')",
          "java.sql.SQLException: Invalid format: 'YYYY-MM-DD' for datetime string: 'ABCD'.",
          true);
      f.checkFails("to_date('2022-06-03', 'I')",
          "Illegal pattern character 'I'",
          true);
      f.checkNull("to_date(NULL, 'YYYY-MM-DD')");
      f.checkNull("to_date('2022-06-03', NULL)");
      f.checkNull("to_date(NULL, NULL)");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.ORACLE, SqlLibrary.REDSHIFT, SqlLibrary.HIVE);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testToDatePg() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.POSTGRESQL)
        .setFor(SqlLibraryOperators.TO_DATE_PG);

    f.checkString("to_date('2022-06-03', 'YYYY-MM-DD')",
        "2022-06-03",
        "DATE NOT NULL");
    f.checkString("to_date('0001-01-01', 'YYYY-MM-DD')",
        "0001-01-01",
        "DATE NOT NULL");
    f.checkString("to_date('Jun 03, 2022', 'Mon DD, YYYY')",
        "2022-06-03",
        "DATE NOT NULL");
    f.checkString("to_date('2022-June-03', 'YYYY-Month-DD')",
        "2022-06-03",
        "DATE NOT NULL");
    f.checkString("to_date('2022-Jun-03', 'YYYY-Mon-DD')",
        "2022-06-03",
        "DATE NOT NULL");
    f.checkString("to_date('2022-154', 'YYYY-DDD')",
        "2022-06-03",
        "DATE NOT NULL");
    f.checkFails("to_date('ABCD', 'YYYY-MM-DD')",
        "java.sql.SQLException: Invalid format: 'YYYY-MM-DD' for datetime string: 'ABCD'.",
        true);
    f.checkNull("to_date(NULL, 'YYYY-MM-DD')");
    f.checkNull("to_date('2022-06-03', NULL)");
    f.checkNull("to_date(NULL, NULL)");
  }

  @Test void testToTimestamp() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.TO_TIMESTAMP);

    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("to_timestamp('2022-06-03 18:34:56', 'YYYY-MM-DD HH24:MI:SS')",
          "2022-06-03 18:34:56",
          "TIMESTAMP(0) NOT NULL");
      f.checkString("to_timestamp('0001-01-01 18:43:56', 'YYYY-MM-DD HH24:MI:SS')",
          "0001-01-01 18:43:56",
          "TIMESTAMP(0) NOT NULL");
      f.checkString("to_timestamp('18:34:56 Jun 03, 2022', 'HH24:MI:SS Mon DD, YYYY')",
          "2022-06-03 18:34:56",
          "TIMESTAMP(0) NOT NULL");
      f.checkString("to_timestamp('18:34:56 2022-June-03', 'HH24:MI:SS YYYY-Month-DD')",
          "2022-06-03 18:34:56",
          "TIMESTAMP(0) NOT NULL");
      f.checkString("to_timestamp('18:34:56 2022-Jun-03', 'HH24:MI:SS YYYY-Mon-DD')",
          "2022-06-03 18:34:56",
          "TIMESTAMP(0) NOT NULL");
      f.checkString("to_timestamp('18:34:56 2022-154', 'HH24:MI:SS YYYY-DDD')",
          "2022-06-03 18:34:56",
          "TIMESTAMP(0) NOT NULL");
      f.checkFails("to_timestamp('ABCD', 'YYYY-MM-DD HH24:MI:SS')",
          "java.sql.SQLException: Invalid format: 'YYYY-MM-DD HH24:MI:SS' for datetime string: 'ABCD'.",
          true);
      f.checkFails("to_timestamp('2022-06-03 18:34:56', 'I')",
          "Illegal pattern character 'I'",
          true);
      f.checkNull("to_timestamp(NULL, 'YYYY-MM-DD HH24:MI:SS')");
      f.checkNull("to_timestamp('2022-06-03 18:34:56', NULL)");
      f.checkNull("to_timestamp(NULL, NULL)");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.ORACLE, SqlLibrary.REDSHIFT);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testToTimestampPg() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.POSTGRESQL)
        .setFor(SqlLibraryOperators.TO_TIMESTAMP_PG);

    f.checkString("to_timestamp('2022-06-03 18:34:56', 'YYYY-MM-DD HH24:MI:SS')",
        "2022-06-03 18:34:56",
        "TIMESTAMP_TZ(0) NOT NULL");
    f.checkString("to_timestamp('0001-01-01 18:43:56', 'YYYY-MM-DD HH24:MI:SS')",
        "0001-01-01 18:43:56",
        "TIMESTAMP_TZ(0) NOT NULL");
    f.checkString("to_timestamp('18:34:56 Jun 03, 2022', 'HH24:MI:SS Mon DD, YYYY')",
        "2022-06-03 18:34:56",
        "TIMESTAMP_TZ(0) NOT NULL");
    f.checkString("to_timestamp('18:34:56 2022-June-03', 'HH24:MI:SS YYYY-Month-DD')",
        "2022-06-03 18:34:56",
        "TIMESTAMP_TZ(0) NOT NULL");
    f.checkString("to_timestamp('18:34:56 2022-Jun-03', 'HH24:MI:SS YYYY-Mon-DD')",
        "2022-06-03 18:34:56",
        "TIMESTAMP_TZ(0) NOT NULL");
    f.checkString("to_timestamp('18:34:56 2022-154', 'HH24:MI:SS YYYY-DDD')",
        "2022-06-03 18:34:56",
        "TIMESTAMP_TZ(0) NOT NULL");
    f.checkFails("to_timestamp('ABCD', 'YYYY-MM-DD HH24:MI:SS')",
        "java.sql.SQLException: Invalid format: 'YYYY-MM-DD HH24:MI:SS' for timestamp "
            + "string: 'ABCD'.",
        true);
    f.checkNull("to_timestamp(NULL, 'YYYY-MM-DD HH24:MI:SS')");
    f.checkNull("to_timestamp('2022-06-03 18:34:56', NULL)");
    f.checkNull("to_timestamp(NULL, NULL)");
  }

  @Test void testFromBase64() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.FROM_BASE64);
    f0.checkFails("^from_base64('2fjoeiwjfoj==')^",
        "No match found for function signature FROM_BASE64\\(<CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("from_base64('VGhpcyBpcyBhIHRlc3QgU3RyaW5nLg==')",
          "546869732069732061207465737420537472696e672e",
          "VARBINARY");
      f.checkString("from_base64('VGhpcyBpcyBhIHRlc\t3QgU3RyaW5nLg==')",
          "546869732069732061207465737420537472696e672e",
          "VARBINARY");
      f.checkString("from_base64('VGhpcyBpcyBhIHRlc\t3QgU3\nRyaW5nLg==')",
          "546869732069732061207465737420537472696e672e",
          "VARBINARY");
      f.checkString("from_base64('VGhpcyB  pcyBhIHRlc3Qg\tU3Ry\naW5nLg==')",
          "546869732069732061207465737420537472696e672e",
          "VARBINARY");
      f.checkNull("from_base64('-1')");
      f.checkNull("from_base64('-100')");
    };
    f0.forEachLibrary(list(SqlLibrary.BIG_QUERY, SqlLibrary.MYSQL), consumer);
  }

  @Test void testToBase32() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.TO_BASE32);
    f0.checkFails("^to_base32('')^",
        "No match found for function signature TO_BASE32\\(<CHARACTER>\\)",
        false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkString("to_base32(x'436f6e766572747320612073657175656e6365206f6620425954"
            + "455320696e746f2061206261736533322d656e636f64656420535452494e472e')",
        "INXW45TFOJ2HGIDBEBZWK4LVMVXGGZJAN5TCAQSZKRCVGIDJNZ2G6IDBEBRGC43FGMZC2ZLOMNXWIZ"
            + "LEEBJVIUSJJZDS4===",
        "VARCHAR NOT NULL");
    f.checkString("to_base32('Converts a sequence of BYTES into a base32-encoded STRING.')",
        "INXW45TFOJ2HGIDBEBZWK4LVMVXGGZJAN5TCAQSZKRCVGIDJNZ2G6IDBEBRGC43FGMZC2ZLOMNXWIZ"
            + "LEEBJVIUSJJZDS4===",
        "VARCHAR NOT NULL");
    f.checkNull("to_base32(cast (null as varchar))");
    f.checkString("to_base32(x'')", "", "VARCHAR NOT NULL");
    f.checkString("to_base32('')", "", "VARCHAR NOT NULL");
  }

  @Test void testFromBase32() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.FROM_BASE32);
    f0.checkFails("^from_base32('')^",
        "No match found for function signature FROM_BASE32\\(<CHARACTER>\\)",
        false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkString("from_base32('INXW45TFOJ2HGIDBEBZWK4LVMVXGGZJAN5TCAQSZKRCVGIDJNZ2"
            + "G6IDBEBRGC43FGMZC2ZLOMNXWIZLEEBJVIUSJJZDS4===')",
        "436f6e766572747320612073657175656e6365206f6620425954455320696e746f206120626173"
            + "6533322d656e636f64656420535452494e472e",
        "VARBINARY NOT NULL");

    f.checkString("from_base32('')", "", "VARBINARY NOT NULL");
    f.checkNull("from_base32(cast (null as varchar))");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6815">[CALCITE-6815]
   * Add bin function (enabled in Hive and Spark library)</a>. */
  @Test void testBin() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.BIN);
    f0.checkFails("^bin(x'')^",
        "No match found for function signature BIN\\(<BINARY>\\)",
        false);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.SPARK, SqlLibrary.HIVE);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("bin(12)",
          "1100",
          "VARCHAR NOT NULL");
      f.checkString("bin(1)",
          "1",
          "VARCHAR NOT NULL");
      f.checkString("bin(01)",
          "1",
          "VARCHAR NOT NULL");
      f.checkString("bin(000)",
          "0",
          "VARCHAR NOT NULL");
      f.checkString("bin(-000)",
          "0",
          "VARCHAR NOT NULL");
      f.checkString("bin(-11)",
          "1111111111111111111111111111111111111111111111111111111111110101",
          "VARCHAR NOT NULL");
      f.checkString("bin(-1)",
          "1111111111111111111111111111111111111111111111111111111111111111",
          "VARCHAR NOT NULL");
      f.checkString("bin(-001)",
          "1111111111111111111111111111111111111111111111111111111111111111",
          "VARCHAR NOT NULL");
      f.checkNull("bin(null)");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6821">[CALCITE-6821]
   * Add crc32 function (enabled in Hive and Spark library)</a>. */
  @Test void testCRC32() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.CRC32);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.SPARK, SqlLibrary.HIVE);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("crc32('ABC')",
          "2743272264",
          "BIGINT NOT NULL");
      f.checkString("crc32(x'414243')",
          "2743272264",
          "BIGINT NOT NULL");
      f.checkString("crc32('')",
          "0",
          "BIGINT NOT NULL");
      f.checkString("crc32(x'')",
          "0",
          "BIGINT NOT NULL");
      f.checkNull("crc32(null)");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testMd5() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.MD5);
    f0.checkFails("^md5(x'')^",
        "No match found for function signature MD5\\(<BINARY>\\)",
        false);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.BIG_QUERY, SqlLibrary.MYSQL,
            SqlLibrary.POSTGRESQL, SqlLibrary.REDSHIFT,
            SqlLibrary.SPARK, SqlLibrary.HIVE);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("md5(x'')",
          "d41d8cd98f00b204e9800998ecf8427e",
          "VARCHAR NOT NULL");
      f.checkString("md5('')",
          "d41d8cd98f00b204e9800998ecf8427e",
          "VARCHAR NOT NULL");
      f.checkString("md5('ABC')",
          "902fbdd2b1df0c4f70b4a5d23525e932",
          "VARCHAR NOT NULL");
      f.checkString("md5(x'414243')",
          "902fbdd2b1df0c4f70b4a5d23525e932",
          "VARCHAR NOT NULL");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testSha1() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SHA1);
    f0.checkFails("^sha1(x'')^",
        "No match found for function signature SHA1\\(<BINARY>\\)",
        false);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.BIG_QUERY, SqlLibrary.MYSQL,
            SqlLibrary.POSTGRESQL, SqlLibrary.REDSHIFT, SqlLibrary.SPARK, SqlLibrary.HIVE);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("sha1(x'')",
          "da39a3ee5e6b4b0d3255bfef95601890afd80709",
          "VARCHAR NOT NULL");
      f.checkString("sha1('')",
          "da39a3ee5e6b4b0d3255bfef95601890afd80709",
          "VARCHAR NOT NULL");
      f.checkString("sha1('ABC')",
          "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8",
          "VARCHAR NOT NULL");
      f.checkString("sha1(x'414243')",
          "3c01bdbb26f358bab27f267924aa2c9a03fcfdb8",
          "VARCHAR NOT NULL");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testSha256() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SHA1);
    f0.checkFails("^sha256(x'')^",
        "No match found for function signature SHA256\\(<BINARY>\\)",
        false);

    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.BIG_QUERY, SqlLibrary.POSTGRESQL);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("sha256('')",
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
          "VARCHAR NOT NULL");
      f.checkString("sha256(x'')",
          "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
          "VARCHAR NOT NULL");
      f.checkString("sha256('Hello World')",
          "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e",
          "VARCHAR NOT NULL");
      f.checkString("sha256(x'48656c6c6f20576f726c64')",
          "a591a6d40bf420404a011733cfb7b190d62c65bf0bcda32b57b277d9ad9f146e",
          "VARCHAR NOT NULL");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testSha512() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SHA1);
    f0.checkFails("^sha512(x'')^",
        "No match found for function signature SHA512\\(<BINARY>\\)",
        false);

    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.BIG_QUERY, SqlLibrary.POSTGRESQL);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("sha512('')",
          "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2"
              + "b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
          "VARCHAR NOT NULL");
      f.checkString("sha512(x'')",
          "cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c5d85f2b"
              + "0ff8318d2877eec2f63b931bd47417a81a538327af927da3e",
          "VARCHAR NOT NULL");
      f.checkString("sha512('Hello World')",
          "2c74fd17edafd80e8447b0d46741ee243b7eb74dd2149a0ab1b9246fb30382f27e853d8585719e0"
              + "e67cbda0daa8f51671064615d645ae27acb15bfb1447f459b",
          "VARCHAR NOT NULL");
      String hexString = new ByteString("Hello World".getBytes(UTF_8)).toString();
      f.checkString("sha512(x'" + hexString + "')",
          "2c74fd17edafd80e8447b0d46741ee243b7eb74dd2149a0ab1b9246fb30382f27e853d8585719e0"
              + "e67cbda0daa8f51671064615d645ae27acb15bfb1447f459b",
          "VARCHAR NOT NULL");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6805">[CALCITE-6805]
   * Add hex function (enabled in Hive, Spark library)</a>. */
  @Test void testHex() {
    SqlOperatorFixture sqlOperatorFixture = fixture();
    final SqlOperatorFixture f0 = sqlOperatorFixture.setFor(SqlLibraryOperators.HEX);
    f0.checkFails("^hex('')^",
        "No match found for function signature HEX\\(<CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("hex('abc')",
          "616263",
          "VARCHAR NOT NULL");
      f.checkString("hex('1')",
          "31",
          "VARCHAR NOT NULL");
      f.checkString("hex('0')",
          "30",
          "VARCHAR NOT NULL");
      f.checkString("hex('-1')",
          "2d31",
          "VARCHAR NOT NULL");
      f.checkString("hex('')", "", "VARCHAR NOT NULL");
      f.checkNull("hex(null)");
      f.checkNull("hex(cast(null as varbinary))");
    };
    f0.forEachLibrary(list(SqlLibrary.HIVE, SqlLibrary.SPARK), consumer);
  }

  @Test void testToHex() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.TO_HEX);
    f0.checkFails("^to_hex(x'')^",
        "No match found for function signature TO_HEX\\(<BINARY>\\)",
        false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkString("to_hex(x'00010203AAEEEFFF')",
        "00010203aaeeefff",
        "VARCHAR NOT NULL");
    f.checkString("to_hex(x'')", "", "VARCHAR NOT NULL");
    f.checkNull("to_hex(cast(null as varbinary))");
  }

  @Test void testFromHex() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.FROM_HEX);
    f0.checkFails("^from_hex('')^",
        "No match found for function signature FROM_HEX\\(<CHARACTER>\\)",
        false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkString("from_hex('00010203aaeeefff')",
        "00010203aaeeefff",
        "VARBINARY NOT NULL");

    f.checkString("from_hex('666f6f626172')",
        "666f6f626172",
        "VARBINARY NOT NULL");

    f.checkString("from_hex('')", "", "VARBINARY NOT NULL");
    f.checkNull("from_hex(cast(null as varchar))");
  }

  @Test void testRepeatFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.REPEAT);
    f0.checkFails("^repeat('a', -100)^",
        "No match found for function signature REPEAT\\(<CHARACTER>, <NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("REPEAT('a', -100)", "", "VARCHAR NOT NULL");
      f.checkString("REPEAT('a', -1)", "", "VARCHAR NOT NULL");
      f.checkString("REPEAT('a', 0)", "", "VARCHAR NOT NULL");
      f.checkString("REPEAT('a', 2)", "aa", "VARCHAR NOT NULL");
      f.checkString("REPEAT('abc', 3)", "abcabcabc", "VARCHAR NOT NULL");
      f.checkNull("REPEAT(cast(null as varchar(1)), -1)");
      f.checkNull("REPEAT(cast(null as varchar(1)), 2)");
      f.checkNull("REPEAT('abc', cast(null as integer))");
      f.checkNull("REPEAT(cast(null as varchar(1)), cast(null as integer))");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.MYSQL,
            SqlLibrary.POSTGRESQL, SqlLibrary.SPARK,
            SqlLibrary.REDSHIFT, SqlLibrary.HIVE);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testSpaceFunc() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.SPACE);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("SPACE(-100)", "", "VARCHAR NOT NULL");
      f.checkString("SPACE(-1)", "", "VARCHAR NOT NULL");
      f.checkString("SPACE(0)", "", "VARCHAR NOT NULL");
      f.checkString("SPACE(2)", "  ", "VARCHAR NOT NULL");
      f.checkString("SPACE(5)", "     ", "VARCHAR NOT NULL");
      f.checkNull("SPACE(cast(null as integer))");
    };
    f0.forEachLibrary(list(SqlLibrary.MYSQL, SqlLibrary.SPARK, SqlLibrary.HIVE), consumer);
  }

  @Test void testStrcmpFunc() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.STRCMP)
        .withLibrary(SqlLibrary.MYSQL);
    f.checkString("STRCMP('mytesttext', 'mytesttext')", "0", "INTEGER NOT NULL");
    f.checkString("STRCMP('mytesttext', 'mytest_text')", "-1", "INTEGER NOT NULL");
    f.checkString("STRCMP('mytest_text', 'mytesttext')", "1", "INTEGER NOT NULL");
    f.checkNull("STRCMP('mytesttext', cast(null as varchar(1)))");
    f.checkNull("STRCMP(cast(null as varchar(1)), 'mytesttext')");
  }

  @Test void testSoundexFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SOUNDEX);
    f0.checkFails("^soundex('tech on the net')^",
        "No match found for function signature SOUNDEX\\(<CHARACTER>\\)",
        false);
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.MYSQL,
            SqlLibrary.ORACLE, SqlLibrary.POSTGRESQL, SqlLibrary.REDSHIFT, SqlLibrary.HIVE);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("SOUNDEX('TECH ON THE NET')", "T253", "VARCHAR(4) NOT NULL");
      f.checkString("SOUNDEX('Miller')", "M460", "VARCHAR(4) NOT NULL");
      f.checkString("SOUNDEX('miler')", "M460", "VARCHAR(4) NOT NULL");
      f.checkString("SOUNDEX('myller')", "M460", "VARCHAR(4) NOT NULL");
      f.checkString("SOUNDEX('muller')", "M460", "VARCHAR(4) NOT NULL");
      f.checkString("SOUNDEX('m')", "M000", "VARCHAR(4) NOT NULL");
      f.checkString("SOUNDEX('mu')", "M000", "VARCHAR(4) NOT NULL");
      f.checkString("SOUNDEX('mile')", "M400", "VARCHAR(4) NOT NULL");
      f.checkNull("SOUNDEX(cast(null as varchar(1)))");
      f.checkFails("SOUNDEX(_UTF8'\u5B57\u5B57')", "The character is not mapped.*", true);
    };
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testSoundexSparkFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SOUNDEX_SPARK);
    f0.checkFails("^soundex('tech on the net')^",
        "No match found for function signature SOUNDEX\\(<CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("SOUNDEX('TECH ON THE NET')", "T253", "VARCHAR NOT NULL");
      f.checkString("SOUNDEX('Miller')", "M460", "VARCHAR NOT NULL");
      f.checkString("SOUNDEX('miler')", "M460", "VARCHAR NOT NULL");
      f.checkString("SOUNDEX('myller')", "M460", "VARCHAR NOT NULL");
      f.checkString("SOUNDEX('muller')", "M460", "VARCHAR NOT NULL");
      f.checkString("SOUNDEX('m')", "M000", "VARCHAR NOT NULL");
      f.checkString("SOUNDEX('mu')", "M000", "VARCHAR NOT NULL");
      f.checkString("SOUNDEX('mile')", "M400", "VARCHAR NOT NULL");
      // note: it's different with soundex for bigquery/mysql/oracle/pg
      f.checkString("SOUNDEX(_UTF8'\u5B57\u5B57')",
          "字字", "VARCHAR NOT NULL");
      f.checkString("SOUNDEX(_UTF8'\u5B57\u5B57\u5B57\u5B57')",
          "字字字字", "VARCHAR NOT NULL");
      f.checkNull("SOUNDEX(cast(null as varchar(1)))");
    };
    f0.forEachLibrary(list(SqlLibrary.SPARK), consumer);
  }

  @Test void testDifferenceFunc() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.DIFFERENCE);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalarExact("DIFFERENCE('Miller', 'miller')", 4);
      f.checkScalarExact("DIFFERENCE('Miller', 'myller')", 4);
      f.checkScalarExact("DIFFERENCE('muller', 'miller')", 4);
      f.checkScalarExact("DIFFERENCE('muller', 'miller')", 4);
      f.checkScalarExact("DIFFERENCE('muller', 'milk')", 2);
      f.checkScalarExact("DIFFERENCE('muller', 'mile')", 2);
      f.checkScalarExact("DIFFERENCE('muller', 'm')", 1);
      f.checkScalarExact("DIFFERENCE('muller', 'lee')", 0);
      f.checkNull("DIFFERENCE('muller', cast(null as varchar(1)))");
      f.checkNull("DIFFERENCE(cast(null as varchar(1)), 'muller')");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.POSTGRESQL, SqlLibrary.REDSHIFT);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testReverseFunc() {
    final SqlFunction func = SqlLibraryOperators.REVERSE;
    final SqlOperatorFixture f0 = fixture().setFor(func);
    f0.checkFails("^reverse('abc')^",
        "No match found for function signature REVERSE\\(<CHARACTER>\\)",
        false);
    checReverseFunc(f0, func, list(SqlLibrary.BIG_QUERY, SqlLibrary.MYSQL));
  }

  void checReverseFunc(SqlOperatorFixture f0, SqlFunction function,
      Iterable<? extends SqlLibrary> libraries) {
    final String fn = function.getName();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString(fn + "('')", "", "VARCHAR(0) NOT NULL");
      f.checkString(fn + "('123')", "321", "VARCHAR(3) NOT NULL");
      f.checkString(fn + "('abc')", "cba", "VARCHAR(3) NOT NULL");
      f.checkString(fn + "('ABC')", "CBA", "VARCHAR(3) NOT NULL");
      f.checkString(fn + "('Hello World')", "dlroW olleH",
          "VARCHAR(11) NOT NULL");
      f.checkString(fn + "(_UTF8'\u4F60\u597D')", "\u597D\u4F60",
          "VARCHAR(2) NOT NULL");
      f.checkString(fn + "('a!b@c#')", "#c@b!a", "VARCHAR(6) NOT NULL");
      f.checkNull(fn + "(cast(null as varchar(1)))");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testLevenshtein() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.LEVENSHTEIN);
    f0.checkFails("^levenshtein('abc', 'abc')^",
        "No match found for function signature LEVENSHTEIN\\(<CHARACTER>, <CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("levenshtein('', '')", 0, "INTEGER NOT NULL");
      f.checkScalar("levenshtein('abc', 'abc')", 0, "INTEGER NOT NULL");
      f.checkScalar("levenshtein('kitten', 'sitting')", 3, "INTEGER NOT NULL");
      f.checkScalar("levenshtein('frog', 'fog')", 1, "INTEGER NOT NULL");
      f.checkScalar("levenshtein(_UTF8'\u4F60\u597D', _UTF8'\u4F60\u5F88\u597D')",
          1, "INTEGER NOT NULL");
      f.checkNull("levenshtein(cast(null as varchar), 'abc')");
      f.checkNull("levenshtein('abc', cast(null as varchar))");
      f.checkNull("levenshtein(cast(null as varchar), cast(null as varchar))");
    };
    f0.forEachLibrary(list(SqlLibrary.HIVE, SqlLibrary.SPARK), consumer);
  }

  @Test void testFindInSetFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.FIND_IN_SET);
    f0.checkFails("^find_in_set('ab', 'abc,b,ab,c,def')^",
        "No match found for function signature FIND_IN_SET\\(<CHARACTER>, <CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("find_in_set('ab', 'abc,b,ab,c,def')",
          "3", "INTEGER NOT NULL");
      f.checkString("find_in_set('ab', ',,,ab,abc,b,ab,c,def')",
          "4", "INTEGER NOT NULL");
      f.checkString("find_in_set('def', ',,,ab,abc,c,def')",
          "7", "INTEGER NOT NULL");
      f.checkString("find_in_set(_UTF8'\u4F60\u597D', _UTF8'b,ab,c,def,\u4F60\u597D')",
          "5", "INTEGER NOT NULL");
      f.checkString("find_in_set('acd', ',,,ab,abc,c,def')",
          "0", "INTEGER NOT NULL");
      f.checkString("find_in_set('ab,', 'abc,b,ab,c,def')",
          "0", "INTEGER NOT NULL");
      f.checkNull("find_in_set(cast(null as varchar), 'abc,b,ab,c,def')");
      f.checkNull("find_in_set('ab', cast(null as varchar))");
      f.checkNull("find_in_set(cast(null as varchar), cast(null as varchar))");
    };
    f0.forEachLibrary(list(SqlLibrary.HIVE, SqlLibrary.SPARK), consumer);
  }

  @Test void testIfFunc() {
    final SqlOperatorFixture f = fixture();
    checkIf(f.withLibrary(SqlLibrary.BIG_QUERY));
    checkIf(f.withLibrary(SqlLibrary.HIVE));
    checkIf(f.withLibrary(SqlLibrary.SPARK));
  }

  private static void checkIf(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.IF);
    f.checkString("if(1 = 2, 1, 2)", "2", "INTEGER NOT NULL");
    f.checkString("if('abc'='xyz', 'abc', 'xyz')", "xyz",
        "CHAR(3) NOT NULL");
    f.checkString("if(substring('abc',1,2)='ab', 'abc', 'xyz')", "abc",
        "CHAR(3) NOT NULL");
    f.checkString("if(substring('abc',1,2)='ab', 'abc', 'wxyz')", "abc ",
        "CHAR(4) NOT NULL");
    // TRUE yields first arg, FALSE and UNKNOWN yield second arg
    f.checkScalar("if(nullif(true,false), 5, 10)", 5, "INTEGER NOT NULL");
    f.checkScalar("if(nullif(true,true), 5, 10)", 10, "INTEGER NOT NULL");
    f.checkScalar("if(nullif(true,true), 5, 10)", 10, "INTEGER NOT NULL");
  }

  @Test void testUpperFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.UPPER, VmName.EXPAND);
    f.checkString("upper('a')", "A", "CHAR(1) NOT NULL");
    f.checkString("upper('A')", "A", "CHAR(1) NOT NULL");
    f.checkString("upper('1')", "1", "CHAR(1) NOT NULL");
    f.checkString("upper('aa')", "AA", "CHAR(2) NOT NULL");
    f.checkNull("upper(cast(null as varchar(1)))");
  }

  @Test void testLeftFunc() {
    final SqlOperatorFixture f0 = fixture();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.setFor(SqlLibraryOperators.LEFT);
      f.checkString("left('abcd', 3)", "abc", "VARCHAR(4) NOT NULL");
      f.checkString("left('abcd', 0)", "", "VARCHAR(4) NOT NULL");
      f.checkString("left('abcd', 5)", "abcd", "VARCHAR(4) NOT NULL");
      f.checkString("left('abcd', -2)", "", "VARCHAR(4) NOT NULL");
      f.checkNull("left(cast(null as varchar(1)), -2)");
      f.checkNull("left('abcd', cast(null as Integer))");
      // Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-5859">[CALCITE-5859]
      // Compile-time evaluation of LEFT(NULL, n) should not throw RuntimeException</a>
      f.checkNull("left(null, 3)");

      // test for ByteString
      f.checkString("left(x'ABCdef', 1)", "ab", "VARBINARY(3) NOT NULL");
      f.checkString("left(x'ABCdef', 0)", "", "VARBINARY(3) NOT NULL");
      f.checkString("left(x'ABCdef', 4)", "abcdef",
          "VARBINARY(3) NOT NULL");
      f.checkString("left(x'ABCdef', -2)", "", "VARBINARY(3) NOT NULL");
      f.checkNull("left(cast(null as binary(1)), -2)");
      f.checkNull("left(x'ABCdef', cast(null as Integer))");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.MYSQL, SqlLibrary.POSTGRESQL,
            SqlLibrary.REDSHIFT, SqlLibrary.SPARK);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testRightFunc() {
    final SqlOperatorFixture f0 = fixture();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.setFor(SqlLibraryOperators.RIGHT);
      f.checkString("right('abcd', 3)", "bcd", "VARCHAR(4) NOT NULL");
      f.checkString("right('abcd', 0)", "", "VARCHAR(4) NOT NULL");
      f.checkString("right('abcd', 5)", "abcd", "VARCHAR(4) NOT NULL");
      f.checkString("right('abcd', -2)", "", "VARCHAR(4) NOT NULL");
      f.checkNull("right(cast(null as varchar(1)), -2)");
      f.checkNull("right('abcd', cast(null as Integer))");

      // test for ByteString
      f.checkString("right(x'ABCdef', 1)", "ef", "VARBINARY(3) NOT NULL");
      f.checkString("right(x'ABCdef', 0)", "", "VARBINARY(3) NOT NULL");
      f.checkString("right(x'ABCdef', 4)", "abcdef",
          "VARBINARY(3) NOT NULL");
      f.checkString("right(x'ABCdef', -2)", "", "VARBINARY(3) NOT NULL");
      f.checkNull("right(cast(null as binary(1)), -2)");
      f.checkNull("right(x'ABCdef', cast(null as Integer))");
    };

    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.MYSQL, SqlLibrary.POSTGRESQL,
            SqlLibrary.REDSHIFT, SqlLibrary.SPARK);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testRegexpContainsFunc() {
    final SqlOperatorFixture f = fixture().setFor(SqlLibraryOperators.REGEXP_CONTAINS)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkBoolean("regexp_contains('abc def ghi', 'abc')", true);
    f.checkBoolean("regexp_contains('abc def ghi', '[a-z]+')", true);
    f.checkBoolean("regexp_contains('foo@bar.com', '@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+')", true);
    f.checkBoolean("regexp_contains('foo@.com', '@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+')", false);
    // Note: Caret below is escaped (doubled)
    f.checkBoolean("regexp_contains('5556664422', '^^\\d{10}$')", true);
    f.checkBoolean("regexp_contains('11555666442233', '^^\\d{10}$')", false);
    f.checkBoolean("regexp_contains('55566644221133', '\\d{10}')", true);
    f.checkBoolean("regexp_contains('55as56664as422', '\\d{10}')", false);
    f.checkBoolean("regexp_contains('55as56664as422', '')", true);

    f.checkQuery("select regexp_contains('abc def ghi', 'abc')");
    f.checkQuery("select regexp_contains('foo@bar.com', '@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+')");
    f.checkQuery("select regexp_contains('55as56664as422', '\\d{10}')");

    f.checkNull("regexp_contains('abc def ghi', cast(null as varchar))");
    f.checkNull("regexp_contains(cast(null as varchar), 'abc')");
    f.checkNull("regexp_contains(cast(null as varchar), cast(null as varchar))");
  }

  @Test void testRegexpExtractAllFunc() {
    final SqlOperatorFixture f =
        fixture().setFor(SqlLibraryOperators.REGEXP_EXTRACT_ALL).withLibrary(SqlLibrary.BIG_QUERY);

    f.checkScalar("regexp_extract_all('a9cadca5c4aecghi', 'a[0-9]c')", "[a9c, a5c]", "CHAR(16) "
        + "NOT NULL ARRAY NOT NULL");
    f.checkScalar("regexp_extract_all('abcde', '.')", "[a, b, c, d, e]", "CHAR(5) NOT NULL ARRAY "
        + "NOT NULL");
    f.checkScalar("regexp_extract_all('abcadcabcaecghi', 'ac')", "[]", "CHAR(15) NOT NULL ARRAY "
        + "NOT NULL");
    f.checkScalar("regexp_extract_all('foo@bar.com, foo@gmail.com, foo@outlook.com', "
        + "'@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+')", "[@bar.com, @gmail.com, @outlook.com]", "CHAR(43) "
        + "NOT NULL ARRAY NOT NULL");

    f.checkNull("regexp_extract_all('abc def ghi', cast(null as varchar))");
    f.checkNull("regexp_extract_all(cast(null as varchar), 'abc')");
    f.checkNull("regexp_extract_all(cast(null as varchar), cast(null as varchar))");

    f.checkQuery("select regexp_extract_all('abc def ghi', 'abc')");
    f.checkQuery("select regexp_extract_all('foo@bar.com', '@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+')");
    f.checkQuery("select regexp_extract_all('55as56664as422', '\\d{10}')");
  }

  @Test void testRegexpInstrFunc() {
    final SqlOperatorFixture f =
        fixture().setFor(SqlLibraryOperators.REGEXP_INSTR).withLibrary(SqlLibrary.BIG_QUERY);

    f.checkScalar("regexp_instr('abc def ghi', 'def')", 5, "INTEGER NOT NULL");
    f.checkScalar("regexp_instr('abcadcaecghi', 'a.c', 2)", 4, "INTEGER NOT NULL");
    f.checkScalar("regexp_instr('abcadcaecghi', 'a.c', 1, 3)", 7, "INTEGER NOT NULL");
    f.checkScalar("regexp_instr('abcadcaecghi', 'a.c', 1, 3, 1)", 10, "INTEGER NOT NULL");
    f.checkScalar("regexp_instr('a9cadca513ca4cecghi', 'a([0-9]+)', 1, 2, 1)", 11,
        "INTEGER NOT NULL");
    f.checkScalar("regexp_instr('a9cadca513ca4cecghi', 'a([0-9]*)', 8, 1, 0)", 13,
        "INTEGER NOT NULL");
    f.checkScalar("regexp_instr('55as56664as422', '\\d{3}', 3, 2, 0)", 12, "INTEGER NOT NULL");
    f.checkScalar("regexp_instr('55as56664as422', '\\d{2}', 2, 2, 1)", 9, "INTEGER NOT NULL");
    f.checkScalar("regexp_instr('55as56664as422', '', 2, 2, 1)", 0, "INTEGER NOT NULL");

    f.checkNull("regexp_instr('abc def ghi', cast(null as varchar))");
    f.checkNull("regexp_instr(cast(null as varchar), 'abc')");
    f.checkNull("regexp_instr(cast(null as varchar), cast(null as varchar))");
    f.checkNull("regexp_instr('abc def ghi', 'abc', cast(null as integer))");
    f.checkNull("regexp_instr('abc def ghi', 'abc', 1, cast(null as integer))");
    f.checkNull("regexp_instr('abc def ghi', 'abc', 1, 3, cast(null as integer))");

    f.checkQuery("select regexp_instr('abc def ghi', 'abc')");
    f.checkQuery("select regexp_instr('foo@bar.com', '@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+')");
    f.checkQuery("select regexp_instr('55as56664as422', '\\d{10}')");
    f.checkQuery("select regexp_instr('abcadcabcaecghi', 'c(a.c)', 4)");
    f.checkQuery("select regexp_instr('a9cadca5c4aecghi', 'a[0-9]c', 1, 3)");
  }

  @Test void testRegexpReplace2Func() {
    final SqlOperatorFixture f0 = fixture();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.setFor(SqlLibraryOperators.REGEXP_REPLACE_2);

      // Tests for regexp replace generic functionality
      f.checkString("regexp_replace('a b c', 'b')", "a  c",
          "VARCHAR NOT NULL");
      f.checkString("regexp_replace('abc1 def2 ghi3', '[a-z]+')", "1 2 3",
          "VARCHAR NOT NULL");
      f.checkString("regexp_replace('100-200', '(\\d+)')", "-",
          "VARCHAR NOT NULL");
      f.checkString("regexp_replace('100-200', '(-)')", "100200",
          "VARCHAR NOT NULL");

      f.checkQuery("select regexp_replace('a b c', 'b')");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.REDSHIFT);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testRegexpReplace3Func() {
    final SqlOperatorFixture f0 = fixture();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.setFor(SqlLibraryOperators.REGEXP_REPLACE_3);

      // Tests for regexp replace generic functionality
      f.checkString("regexp_replace('a b c', 'b', 'X')", "a X c",
          "VARCHAR NOT NULL");
      f.checkString("regexp_replace('abc def ghi', '[a-z]+', 'X')", "X X X",
          "VARCHAR NOT NULL");
      f.checkString("regexp_replace('100-200', '(\\d+)', 'num')", "num-num",
          "VARCHAR NOT NULL");
      f.checkString("regexp_replace('100-200', '(-)', '###')", "100###200",
          "VARCHAR NOT NULL");
      f.checkNull("regexp_replace(cast(null as varchar), '(-)', '###')");
      f.checkNull("regexp_replace('100-200', cast(null as varchar), '###')");
      f.checkNull("regexp_replace('100-200', '(-)', cast(null as varchar))");
      f.checkString("regexp_replace('abc\t\ndef\t\nghi', '\t', '+')", "abc+\ndef+\nghi",
          "VARCHAR NOT NULL");
      f.checkString("regexp_replace('abc\t\ndef\t\nghi', '\t\n', '+')", "abc+def+ghi",
          "VARCHAR NOT NULL");
      f.checkString("regexp_replace('abc\t\ndef\t\nghi', '\\w+', '+')", "+\t\n+\t\n+",
          "VARCHAR NOT NULL");

      f.checkQuery("select regexp_replace('a b c', 'b', 'X')");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.MYSQL, SqlLibrary.ORACLE,
            SqlLibrary.REDSHIFT, SqlLibrary.HIVE);
    f0.forEachLibrary(libraries, consumer);

    // Tests to verify double-backslashes are ignored for indexing in other dialects
    final SqlOperatorFixture f2 =
        f0.withLibrary(SqlLibrary.MYSQL).withConformance(SqlConformanceEnum.MYSQL_5);
    f2.checkString("regexp_replace('abc16', 'b(.*)(\\d)', '\\\\2\\\\1X')", "a\\2\\1X",
        "VARCHAR NOT NULL");
    f2.checkString("regexp_replace('abcdefghijabc', 'abc(.)', '\\\\-11x')", "\\-11xefghijabc",
        "VARCHAR NOT NULL");
    f2.checkString("regexp_replace('abcdefghijabc', 'abc(.)', '$1x')", "dxefghijabc",
        "VARCHAR NOT NULL");
  }



  @Test void testRegexpReplace4Func() {
    final SqlOperatorFixture f0 = fixture();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.setFor(SqlLibraryOperators.REGEXP_REPLACE_4);

      // Tests for regexp replace generic functionality
      f.checkString("regexp_replace('abc def ghi', '[a-z]+', 'X', 2)", "aX X X",
          "VARCHAR NOT NULL");
      f.checkQuery("select regexp_replace('a b c', 'b', 'X', 1)");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.MYSQL, SqlLibrary.ORACLE, SqlLibrary.REDSHIFT);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testRegexpReplace5Func() {
    final SqlOperatorFixture f0 = fixture();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.setFor(SqlLibraryOperators.REGEXP_REPLACE_5);

      // Tests for regexp replace generic functionality
      f.checkString("regexp_replace('abc def ghi', '[a-z]+', 'X', 1, 3)", "abc def X",
          "VARCHAR NOT NULL");
      f.checkString("regexp_replace('ABC def ghi', '[a-z]+', 'X', 1, 'i')", "X X X",
          "VARCHAR NOT NULL");
      f.checkQuery("select regexp_replace('a b c', 'b', 'X', 1, 3)");
      f.checkQuery("select regexp_replace('a b c', 'b', 'X', 1, 'i')");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.MYSQL, SqlLibrary.REDSHIFT);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testRegexpReplace5OracleFunc() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.ORACLE);
    f.setFor(SqlLibraryOperators.REGEXP_REPLACE_5_ORACLE);

    // Tests for regexp replace generic functionality
    f.checkString("regexp_replace('abc def ghi', '[a-z]+', 'X', 1, 3)", "abc def X",
        "VARCHAR NOT NULL");
    f.checkQuery("select regexp_replace('a b c', 'b', 'X', 1, 1)");
  }

  @Test void testRegexpReplace6Func() {
    final SqlOperatorFixture f0 = fixture();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.setFor(SqlLibraryOperators.REGEXP_REPLACE_6);

      // Tests for regexp replace generic functionality
      f.checkString("regexp_replace('abc def GHI', '[a-z]+', 'X', 1, 3, 'c')", "abc def GHI",
          "VARCHAR NOT NULL");
      f.checkString("regexp_replace('abc def GHI', '[a-z]+', 'X', 1, 3, 'i')", "abc def X",
          "VARCHAR NOT NULL");

      f.checkQuery("select regexp_replace('a b c', 'b', 'X', 1, 3, 'i')");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.MYSQL, SqlLibrary.ORACLE, SqlLibrary.REDSHIFT);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testRegexpReplaceBigQuery3Func() {
    final SqlOperatorFixture f = fixture().setFor(SqlLibraryOperators.REGEXP_REPLACE_BIG_QUERY_3)
        .withLibrary(SqlLibrary.BIG_QUERY);

    // Tests for double-backslash indexed capturing groups for regexp_replace in BQ
    f.checkString("regexp_replace('abc16', 'b(.*)(\\d)', '\\\\2\\\\1X')", "a6c1X",
        "VARCHAR NOT NULL");
    f.checkString("regexp_replace('a\\bc56a\\bc37', 'b(.)(\\d)', '\\\\2\\\\0X')",
        "a\\5bc5X6a\\3bc3X7", "VARCHAR NOT NULL");
    f.checkString("regexp_replace('abcdefghijabc', 'abc(.)', '\\\\\\\\123xyz')",
        "\\123xyzefghijabc", "VARCHAR NOT NULL");
    f.checkString("regexp_replace('abcdefghijabc', 'abc(.)', '$1xy')",
        "$1xyefghijabc", "VARCHAR NOT NULL");
    f.checkString("regexp_replace('abc123', 'b(.*)(\\d)', '\\\\\\\\$ $\\\\\\\\')",
        "a\\$ $\\", "VARCHAR NOT NULL");

    f.checkQuery("select regexp_replace('a b c', 'b', 'X')");
  }

  @Test void testRegexpReplacePg3Func() {
    final SqlOperatorFixture f = fixture().setFor(SqlLibraryOperators.REGEXP_REPLACE_PG_3)
        .withLibrary(SqlLibrary.POSTGRESQL);

    // Tests for regexp replace generic functionality
    f.checkString("regexp_replace('a b c', 'b', 'X')", "a X c",
        "VARCHAR NOT NULL");
    f.checkString("regexp_replace('abc def ghi', '[a-z]+', 'X')", "X def ghi",
        "VARCHAR NOT NULL");
    f.checkString("regexp_replace('100-200', '(\\d+)', 'num')", "num-200",
        "VARCHAR NOT NULL");
    f.checkString("regexp_replace('100-200', '(-)', '###')", "100###200",
        "VARCHAR NOT NULL");
    f.checkNull("regexp_replace(cast(null as varchar), '(-)', '###')");
    f.checkNull("regexp_replace('100-200', cast(null as varchar), '###')");
    f.checkNull("regexp_replace('100-200', '(-)', cast(null as varchar))");
    f.checkString("regexp_replace('abc\t\ndef\t\nghi', '\t', '+')", "abc+\ndef\t\nghi",
        "VARCHAR NOT NULL");
    f.checkString("regexp_replace('abc\t\ndef\t\nghi', '\t\n', '+')", "abc+def\t\nghi",
        "VARCHAR NOT NULL");
    f.checkString("regexp_replace('abc\t\ndef\t\nghi', '\\w+', '+')", "+\t\ndef\t\nghi",
        "VARCHAR NOT NULL");
    f.checkString("regexp_replace('abc', 'a(.*)c', 'x\\1x')", "xbx",
        "VARCHAR NOT NULL");

    f.checkQuery("select regexp_replace('a b c', 'b', 'X')");
  }

  @Test void testRegexpReplacePg4Func() {
    final SqlOperatorFixture f = fixture().setFor(SqlLibraryOperators.REGEXP_REPLACE_PG_4)
        .withLibrary(SqlLibrary.POSTGRESQL);

    // Tests for regexp replace generic functionality
    f.checkString("regexp_replace('abc def GHI', '[a-z]+', 'X', 'c')", "X def GHI",
        "VARCHAR NOT NULL");
    f.checkString("regexp_replace('ABC def GHI', '[a-z]+', 'X', 'i')", "X def GHI",
        "VARCHAR NOT NULL");
    f.checkString("regexp_replace('abc def ghi', '[a-z]+', 'X', 'g')", "X X X",
        "VARCHAR NOT NULL");

    f.checkQuery("select regexp_replace('a b c', 'b', 'X', 'i')");
  }

  @Test void testRegexpExtractFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.REGEXP_EXTRACT, VmName.EXPAND);
    checkRegexpExtract(f, FunctionAlias.of(SqlLibraryOperators.REGEXP_EXTRACT));
  }

  @Test void testRegexpSubstrFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.REGEXP_SUBSTR, VmName.EXPAND);
    checkRegexpExtract(f, FunctionAlias.of(SqlLibraryOperators.REGEXP_SUBSTR));
  }

  /** Tests the {@code REGEXP_EXTRACT} and {@code REGEXP_SUBSTR} operators. */
  void checkRegexpExtract(SqlOperatorFixture f0, FunctionAlias functionAlias) {
    final SqlFunction function = functionAlias.function;
    final String fn = function.getName();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString(fn + "('abc def ghi', 'def')", "def", "VARCHAR");
      f.checkString(fn + "('abcadcaecghi', 'a.c', 1, 3)", "aec", "VARCHAR");
      f.checkString(fn + "('abcadcaecghi', 'abc(a.c)')", "adc", "VARCHAR");
      f.checkString(fn + "('55as56664as422', '\\d{3}')", "566", "VARCHAR");
      f.checkString(fn + "('abcadcabcaecghi', 'c(a.c)', 4)", "abc", "VARCHAR");
      f.checkString(fn + "('abcadcabcaecghi', 'a.c(a.c)', 1, 2)", "aec", "VARCHAR");
      f.checkString(fn + "('a9cadca5c4aecghi', 'a[0-9]c', 6)", "a5c", "VARCHAR");
      f.checkString(fn + "('a9cadca5ca4cecghi', 'a[0-9]c', 1, 3)", "a4c", "VARCHAR");

      f.checkNull(fn + "('abc def ghi', 'asd')");
      f.checkNull(fn + "('abc def ghi', 'abc', 25)");
      f.checkNull(fn + "('abc def ghi', 'abc', 1, 4)");
      f.checkNull(fn + "('abc def ghi', cast(null as varchar))");
      f.checkNull(fn + "(cast(null as varchar), 'abc')");
      f.checkNull(fn + "(cast(null as varchar), cast(null as varchar))");
      f.checkNull(fn + "('abc def ghi', 'abc', cast(null as integer))");
      f.checkNull(fn + "('abc def ghi', 'abc', 1, cast(null as integer))");

      f.checkQuery("select " + fn + "('abc def ghi', 'abc')");
      f.checkQuery("select " + fn + "('foo@bar.com', '@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+')");
      f.checkQuery("select " + fn + "('55as56664as422', '\\d{10}')");
      f.checkQuery("select " + fn + "('abcadcabcaecghi', 'c(a.c)', 4)");
      f.checkQuery("select " + fn + "('a9cadca5c4aecghi', 'a[0-9]c', 1, 3)");
    };
    f0.forEachLibrary(list(functionAlias.libraries), consumer);
  }

  @Test void testJsonExists() {
    // default pathmode the default is: strict mode
    final SqlOperatorFixture f = fixture();
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'$.foo')", true);

    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo' false on error)", true);
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo' true on error)", true);
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo' unknown on error)", true);
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo' false on error)", true);
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo' true on error)", true);
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo' unknown on error)", true);
    f.checkBoolean("json_exists('{}', "
        + "'invalid $.foo' false on error)", false);
    f.checkBoolean("json_exists('{}', "
        + "'invalid $.foo' true on error)", true);
    f.checkBoolean("json_exists('{}', "
        + "'invalid $.foo' unknown on error)", null);

    // not exists
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo1' false on error)", false);
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo1' true on error)", true);
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'strict $.foo1' unknown on error)", null);
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo1' true on error)", false);
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo1' false on error)", false);
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo1' error on error)", false);
    f.checkBoolean("json_exists('{\"foo\":\"bar\"}', "
        + "'lax $.foo1' unknown on error)", false);

    // nulls
    f.enableTypeCoercion(false)
        .checkFails("json_exists(^null^, "
            + "'lax $' unknown on error)", "(?s).*Illegal use of 'NULL'.*", false);
    f.checkString("json_exists(null, 'lax $' unknown on error)",
        null, "BOOLEAN");
    f.checkNull("json_exists(cast(null as varchar), "
        + "'lax $.foo1' unknown on error)");

  }

  @Test public void testJsonInsert() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.MYSQL);
    f.checkString("json_insert('10', '$.a', 10, '$.c', '[true]')",
        "10", "VARCHAR(2000)");
    f.checkString("json_insert('{ \"a\": 1, \"b\": [2]}', '$.a', 10, '$.c', '[true]')",
        "{\"a\":1,\"b\":[2],\"c\":\"[true]\"}", "VARCHAR(2000)");
    f.checkString("json_insert('{ \"a\": 1, \"b\": [2]}', '$', 10, '$', '[true]')",
        "{\"a\":1,\"b\":[2]}", "VARCHAR(2000)");
    f.checkString("json_insert('{ \"a\": 1, \"b\": [2]}', '$.b[1]', 10)",
        "{\"a\":1,\"b\":[2,10]}", "VARCHAR(2000)");
    f.checkString("json_insert('{\"a\": 1, \"b\": [2, 3, [true]]}', '$.b[3]', 'false')",
        "{\"a\":1,\"b\":[2,3,[true],\"false\"]}", "VARCHAR(2000)");
    f.checkFails("json_insert('{\"a\": 1, \"b\": [2, 3, [true]]}', 'a', 'false')",
        "(?s).*Invalid input for.*", true);
    // nulls
    f.checkNull("json_insert(cast(null as varchar), '$', 10)");
  }

  @Test public void testJsonReplace() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.MYSQL);
    f.checkString("json_replace('10', '$.a', 10, '$.c', '[true]')",
        "10", "VARCHAR(2000)");
    f.checkString("json_replace('{ \"a\": 1, \"b\": [2]}', '$.a', 10, '$.c', '[true]')",
        "{\"a\":10,\"b\":[2]}", "VARCHAR(2000)");
    f.checkString("json_replace('{ \"a\": 1, \"b\": [2]}', '$', 10, '$.c', '[true]')",
        "10", "VARCHAR(2000)");
    f.checkString("json_replace('{ \"a\": 1, \"b\": [2]}', '$.b', 10, '$.c', '[true]')",
        "{\"a\":1,\"b\":10}", "VARCHAR(2000)");
    f.checkString("json_replace('{ \"a\": 1, \"b\": [2, 3]}', '$.b[1]', 10)",
        "{\"a\":1,\"b\":[2,10]}", "VARCHAR(2000)");
    f.checkFails("json_replace('{\"a\": 1, \"b\": [2, 3, [true]]}', 'a', 'false')",
        "(?s).*Invalid input for.*", true);

    // nulls
    f.checkNull("json_replace(cast(null as varchar), '$', 10)");
  }

  @Test public void testJsonSet() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.MYSQL);
    f.checkString("json_set('10', '$.a', 10, '$.c', '[true]')",
        "10", "VARCHAR(2000)");
    f.checkString("json_set('{ \"a\": 1, \"b\": [2]}', '$.a', 10, '$.c', '[true]')",
        "{\"a\":10,\"b\":[2],\"c\":\"[true]\"}", "VARCHAR(2000)");
    f.checkString("json_set('{ \"a\": 1, \"b\": [2]}', '$', 10, '$.c', '[true]')",
        "10", "VARCHAR(2000)");
    f.checkString("json_set('{ \"a\": 1, \"b\": [2, 3]}', '$.b[1]', 10, '$.c', '[true]')",
        "{\"a\":1,\"b\":[2,10],\"c\":\"[true]\"}", "VARCHAR(2000)");
    f.checkFails("json_set('{\"a\": 1, \"b\": [2, 3, [true]]}', 'a', 'false')",
        "(?s).*Invalid input for.*", true);

    // nulls
    f.checkNull("json_set(cast(null as varchar), '$', 10)");
  }

  @Test void testJsonValue() {
    final SqlOperatorFixture f = fixture();
    if (false) {
      f.checkFails("json_value('{\"foo\":100}', 'lax $.foo1' error on empty)",
          "(?s).*Empty result of JSON_VALUE function is not allowed.*",
          true);
    }

    // default pathmode the default is: strict mode
    f.checkString("json_value('{\"foo\":100}', '$.foo')",
        "100", "VARCHAR(2000)");
    // type casting test
    f.checkString("json_value('{\"foo\":100}', 'strict $.foo')",
        "100", "VARCHAR(2000)");
    f.checkScalar("json_value('{\"foo\":100}', 'strict $.foo' returning integer)",
        100, "INTEGER");
    f.checkFails("json_value('{\"foo\":\"100\"}', 'strict $.foo' returning boolean)",
        INVALID_CHAR_MESSAGE, true);
    f.checkScalar("json_value('{\"foo\":100}', 'lax $.foo1' returning integer "
        + "null on empty)", isNullValue(), "INTEGER");
    f.checkScalar("json_value('{\"foo\":\"100\"}', 'strict $.foo1' returning boolean "
        + "null on error)", isNullValue(), "BOOLEAN");

    // lax test
    f.checkString("json_value('{\"foo\":100}', 'lax $.foo' null on empty)",
        "100", "VARCHAR(2000)");
    f.checkString("json_value('{\"foo\":100}', 'lax $.foo' error on empty)",
        "100", "VARCHAR(2000)");
    f.checkString("json_value('{\"foo\":100}', 'lax $.foo' default 'empty' on empty)",
        "100", "VARCHAR(2000)");
    f.checkString("json_value('{\"foo\":100}', 'lax $.foo1' null on empty)",
        null, "VARCHAR(2000)");
    f.checkFails("json_value('{\"foo\":100}', 'lax $.foo1' error on empty)",
        "(?s).*Empty result of JSON_VALUE function is not allowed.*", true);
    f.checkString("json_value('{\"foo\":100}', 'lax $.foo1' default 'empty' on empty)",
        "empty", "VARCHAR(2000)");
    f.checkString("json_value('{\"foo\":{}}', 'lax $.foo' null on empty)",
        null, "VARCHAR(2000)");
    f.checkFails("json_value('{\"foo\":{}}', 'lax $.foo' error on empty)",
        "(?s).*Empty result of JSON_VALUE function is not allowed.*", true);
    f.checkString("json_value('{\"foo\":{}}', 'lax $.foo' default 'empty' on empty)",
        "empty", "VARCHAR(2000)");
    f.checkString("json_value('{\"foo\":100}', 'lax $.foo' null on error)",
        "100", "VARCHAR(2000)");
    f.checkString("json_value('{\"foo\":100}', 'lax $.foo' error on error)",
        "100", "VARCHAR(2000)");
    f.checkString("json_value('{\"foo\":100}', 'lax $.foo' default 'empty' on error)",
        "100", "VARCHAR(2000)");

    // path error test
    f.checkString("json_value('{\"foo\":100}', 'invalid $.foo' null on error)",
        null, "VARCHAR(2000)");
    f.checkFails("json_value('{\"foo\":100}', 'invalid $.foo' error on error)",
        "(?s).*Illegal jsonpath spec.*", true);
    f.checkString("json_value('{\"foo\":100}', "
            + "'invalid $.foo' default 'empty' on error)",
        "empty", "VARCHAR(2000)");

    // strict test
    f.checkString("json_value('{\"foo\":100}', 'strict $.foo' null on empty)",
        "100", "VARCHAR(2000)");
    f.checkString("json_value('{\"foo\":100}', 'strict $.foo' error on empty)",
        "100", "VARCHAR(2000)");
    f.checkString("json_value('{\"foo\":100}', "
            + "'strict $.foo' default 'empty' on empty)",
        "100", "VARCHAR(2000)");
    f.checkString("json_value('{\"foo\":100}', 'strict $.foo1' null on error)",
        null, "VARCHAR(2000)");
    f.checkFails("json_value('{\"foo\":100}', 'strict $.foo1' error on error)",
        "(?s).*No results for path: \\$\\['foo1'\\].*", true);
    f.checkString("json_value('{\"foo\":100}', "
            + "'strict $.foo1' default 'empty' on error)",
        "empty", "VARCHAR(2000)");
    f.checkString("json_value('{\"foo\":{}}', 'strict $.foo' null on error)",
        null, "VARCHAR(2000)");
    f.checkFails("json_value('{\"foo\":{}}', 'strict $.foo' error on error)",
        "(?s).*Strict jsonpath mode requires scalar value, "
            + "and the actual value is: '\\{\\}'.*", true);
    f.checkString("json_value('{\"foo\":{}}', "
            + "'strict $.foo' default 'empty' on error)",
        "empty", "VARCHAR(2000)");

    // nulls
    f.enableTypeCoercion(false)
        .checkFails("json_value(^null^, 'strict $')",
            "(?s).*Illegal use of 'NULL'.*", false);
    f.checkString("json_value(null, 'strict $')", null, "VARCHAR(2000)");
    f.checkNull("json_value(cast(null as varchar), 'strict $')");
  }

  @Test void testJsonQuery() {
    final SqlOperatorFixture f = fixture();
    // default pathmode the default is: strict mode
    f.checkString("json_query('{\"foo\":100}', '$' null on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");

    // lax test
    f.checkString("json_query('{\"foo\":100}', 'lax $' null on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'lax $' error on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'lax $' empty array on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'lax $' empty object on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'lax $.foo' null on empty)",
        null, "VARCHAR(2000)");
    f.checkFails("json_query('{\"foo\":100}', 'lax $.foo' error on empty)",
        "(?s).*Empty result of JSON_QUERY function is not allowed.*", true);
    f.checkString("json_query('{\"foo\":100}', 'lax $.foo' empty array on empty)",
        "[]", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'lax $.foo' empty object on empty)",
        "{}", "VARCHAR(2000)");

    // path error test
    f.checkString("json_query('{\"foo\":100}', 'invalid $.foo' null on error)",
        null, "VARCHAR(2000)");
    f.checkFails("json_query('{\"foo\":100}', 'invalid $.foo' error on error)",
        "(?s).*Illegal jsonpath spec.*", true);
    f.checkString("json_query('{\"foo\":100}', "
            + "'invalid $.foo' empty array on error)",
        "[]", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', "
            + "'invalid $.foo' empty object on error)",
        "{}", "VARCHAR(2000)");

    // strict test
    f.checkString("json_query('{\"foo\":100}', 'strict $' null on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'strict $' error on empty)",
        "{\"foo\":100}", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'strict $' empty array on error)",
        "{\"foo\":100}", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'strict $' empty object on error)",
        "{\"foo\":100}", "VARCHAR(2000)");

    f.checkString("json_query('{\"foo\":100}', 'strict $.foo1' null on error)",
        null, "VARCHAR(2000)");
    f.checkFails("json_query('{\"foo\":100}', 'strict $.foo1' error on error)",
        "(?s).*No results for path: \\$\\['foo1'\\].*", true);
    f.checkString("json_query('{\"foo\":100}', 'strict $.foo1' empty array on error)",
        "[]", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'strict $.foo1' empty object on error)",
        "{}", "VARCHAR(2000)");

    f.checkString("json_query('{\"foo\":100}', 'strict $.foo' null on error)",
        null, "VARCHAR(2000)");
    f.checkFails("json_query('{\"foo\":100}', 'strict $.foo' error on error)",
        "(?s).*Strict jsonpath mode requires array or object value, "
            + "and the actual value is: '100'.*", true);
    f.checkString("json_query('{\"foo\":100}', 'strict $.foo' empty array on error)",
        "[]", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'strict $.foo' empty object on error)",
        "{}", "VARCHAR(2000)");

    // array wrapper test
    f.checkString("json_query('{\"foo\":100}', 'strict $.foo' without wrapper)",
        null, "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'strict $.foo' without array wrapper)",
        null, "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'strict $.foo' with wrapper)",
        "[100]", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'strict $.foo' "
            + "with unconditional wrapper)",
        "[100]", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":100}', 'strict $.foo' "
            + "with conditional wrapper)",
        "[100]", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":[100]}', 'strict $.foo' without wrapper)",
        "[100]", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":[100]}', 'strict $.foo' without array wrapper)",
        "[100]", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":[100]}', 'strict $.foo' with wrapper)",
        "[[100]]", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":[100]}', 'strict $.foo' "
            + "with unconditional wrapper)",
        "[[100]]", "VARCHAR(2000)");
    f.checkString("json_query('{\"foo\":[100]}', 'strict $.foo' "
            + "with conditional wrapper)",
        "[100]", "VARCHAR(2000)");


    // nulls
    f.enableTypeCoercion(false).checkFails("json_query(^null^, 'lax $')",
        "(?s).*Illegal use of 'NULL'.*", false);
    f.checkString("json_query(null, 'lax $')", null, "VARCHAR(2000)");
    f.checkNull("json_query(cast(null as varchar), 'lax $')");
  }

  @Test void testJsonPretty() {
    final SqlOperatorFixture f = fixture();
    f.checkString("json_pretty('{\"foo\":100}')",
        "{\n  \"foo\" : 100\n}", "VARCHAR(2000)");
    f.checkString("json_pretty('[1,2,3]')",
        "[ 1, 2, 3 ]", "VARCHAR(2000)");
    f.checkString("json_pretty('null')",
        "null", "VARCHAR(2000)");

    // nulls
    f.enableTypeCoercion(false).checkFails("json_pretty(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    f.checkString("json_pretty(null)", null, "VARCHAR(2000)");
    f.checkNull("json_pretty(cast(null as varchar))");
  }

  @Test void testJsonStorageSize() {
    final SqlOperatorFixture f = fixture();
    f.checkString("json_storage_size('[100, \"sakila\", [1, 3, 5], 425.05]')",
        "29", "INTEGER");
    f.checkString("json_storage_size('{\"a\": 1000,\"b\": \"aa\", \"c\": \"[1, 3, 5]\"}')",
        "35", "INTEGER");
    f.checkString("json_storage_size('{\"a\": 1000, \"b\": \"wxyz\", \"c\": \"[1, 3]\"}')",
        "34", "INTEGER");
    f.checkString("json_storage_size('[100, \"json\", [[10, 20, 30], 3, 5], 425.05]')",
        "36", "INTEGER");
    f.checkString("json_storage_size('12')",
        "2", "INTEGER");
    f.checkString("json_storage_size('12' format json)",
        "2", "INTEGER");
    f.checkString("json_storage_size('null')",
        "4", "INTEGER");

    // nulls
    f.enableTypeCoercion(false).checkFails("json_storage_size(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    f.checkString("json_storage_size(null)", null, "INTEGER");
    f.checkNull("json_storage_size(cast(null as varchar))");
  }

  @Test void testJsonType() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.JSON_TYPE, VmName.EXPAND);
    f.checkString("json_type('\"1\"')",
        "STRING", "VARCHAR(20)");
    f.checkString("json_type('1')",
        "INTEGER", "VARCHAR(20)");
    f.checkString("json_type('11.45')",
        "DOUBLE", "VARCHAR(20)");
    f.checkString("json_type('true')",
        "BOOLEAN", "VARCHAR(20)");
    f.checkString("json_type('null')",
        "NULL", "VARCHAR(20)");
    f.checkNull("json_type(cast(null as varchar(1)))");
    f.checkString("json_type('{\"a\": [10, true]}')",
        "OBJECT", "VARCHAR(20)");
    f.checkString("json_type('{}')",
        "OBJECT", "VARCHAR(20)");
    f.checkString("json_type('[10, true]')",
        "ARRAY", "VARCHAR(20)");
    f.checkString("json_type('\"2019-01-27 21:24:00\"')",
        "STRING", "VARCHAR(20)");

    // nulls
    f.enableTypeCoercion(false).checkFails("json_type(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    f.checkString("json_type(null)", null, "VARCHAR(20)");
    f.checkNull("json_type(cast(null as varchar))");
  }

  @Test void testJsonDepth() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.JSON_DEPTH, VmName.EXPAND);
    f.checkString("json_depth('1')",
        "1", "INTEGER");
    f.checkString("json_depth('11.45')",
        "1", "INTEGER");
    f.checkString("json_depth('true')",
        "1", "INTEGER");
    f.checkString("json_depth('\"2019-01-27 21:24:00\"')",
        "1", "INTEGER");
    f.checkString("json_depth('{}')",
        "1", "INTEGER");
    f.checkString("json_depth('[]')",
        "1", "INTEGER");
    f.checkString("json_depth('null')",
        null, "INTEGER");
    f.checkString("json_depth(cast(null as varchar(1)))",
        null, "INTEGER");
    f.checkString("json_depth('[10, true]')",
        "2", "INTEGER");
    f.checkString("json_depth('[[], {}]')",
        "2", "INTEGER");
    f.checkString("json_depth('{\"a\": [10, true]}')",
        "3", "INTEGER");
    f.checkString("json_depth('[10, {\"a\": [[1,2]]}]')",
        "5", "INTEGER");

    // nulls
    f.enableTypeCoercion(false).checkFails("json_depth(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    f.checkString("json_depth(null)", null, "INTEGER");
    f.checkNull("json_depth(cast(null as varchar))");
  }

  @Test void testJsonLength() {
    final SqlOperatorFixture f = fixture();
    // no path context
    f.checkString("json_length('{}')",
        "0", "INTEGER");
    f.checkString("json_length('[]')",
        "0", "INTEGER");
    f.checkString("json_length('{\"foo\":100}')",
        "1", "INTEGER");
    f.checkString("json_length('{\"a\": 1, \"b\": {\"c\": 30}}')",
        "2", "INTEGER");
    f.checkString("json_length('[1, 2, {\"a\": 3}]')",
        "3", "INTEGER");

    // default pathmode the default is: strict mode
    f.checkString("json_length('{\"foo\":100}', '$')",
        "1", "INTEGER");

    // lax test
    f.checkString("json_length('{}', 'lax $')",
        "0", "INTEGER");
    f.checkString("json_length('[]', 'lax $')",
        "0", "INTEGER");
    f.checkString("json_length('{\"foo\":100}', 'lax $')",
        "1", "INTEGER");
    f.checkString("json_length('{\"a\": 1, \"b\": {\"c\": 30}}', 'lax $')",
        "2", "INTEGER");
    f.checkString("json_length('[1, 2, {\"a\": 3}]', 'lax $')",
        "3", "INTEGER");
    f.checkString("json_length('{\"a\": 1, \"b\": {\"c\": 30}}', 'lax $.b')",
        "1", "INTEGER");
    f.checkString("json_length('{\"foo\":100}', 'lax $.foo1')",
        null, "INTEGER");

    // strict test
    f.checkString("json_length('{}', 'strict $')",
        "0", "INTEGER");
    f.checkString("json_length('[]', 'strict $')",
        "0", "INTEGER");
    f.checkString("json_length('{\"foo\":100}', 'strict $')",
        "1", "INTEGER");
    f.checkString("json_length('{\"a\": 1, \"b\": {\"c\": 30}}', 'strict $')",
        "2", "INTEGER");
    f.checkString("json_length('[1, 2, {\"a\": 3}]', 'strict $')",
        "3", "INTEGER");
    f.checkString("json_length('{\"a\": 1, \"b\": {\"c\": 30}}', 'strict $.b')",
        "1", "INTEGER");

    // catch error test
    f.checkFails("json_length('{\"foo\":100}', 'invalid $.foo')",
        "(?s).*Illegal jsonpath spec.*", true);
    f.checkFails("json_length('{\"foo\":100}', 'strict $.foo1')",
        "(?s).*No results for path.*", true);

    // nulls
    f.enableTypeCoercion(false).checkFails("json_length(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    f.checkString("json_length(null)", null, "INTEGER");
    f.checkNull("json_length(cast(null as varchar))");
  }

  @Test void testJsonKeys() {
    final SqlOperatorFixture f = fixture();
    // no path context
    f.checkString("json_keys('{}')",
        "[]", "VARCHAR(2000)");
    f.checkString("json_keys('[]')",
        "null", "VARCHAR(2000)");
    f.checkString("json_keys('{\"foo\":100}')",
        "[\"foo\"]", "VARCHAR(2000)");
    f.checkString("json_keys('{\"a\": 1, \"b\": {\"c\": 30}}')",
        "[\"a\",\"b\"]", "VARCHAR(2000)");
    f.checkString("json_keys('[1, 2, {\"a\": 3}]')",
        "null", "VARCHAR(2000)");

    // lax test
    f.checkString("json_keys('{}', 'lax $')",
        "[]", "VARCHAR(2000)");
    f.checkString("json_keys('[]', 'lax $')",
        "null", "VARCHAR(2000)");
    f.checkString("json_keys('{\"foo\":100}', 'lax $')",
        "[\"foo\"]", "VARCHAR(2000)");
    f.checkString("json_keys('{\"a\": 1, \"b\": {\"c\": 30}}', 'lax $')",
        "[\"a\",\"b\"]", "VARCHAR(2000)");
    f.checkString("json_keys('[1, 2, {\"a\": 3}]', 'lax $')",
        "null", "VARCHAR(2000)");
    f.checkString("json_keys('{\"a\": 1, \"b\": {\"c\": 30}}', 'lax $.b')",
        "[\"c\"]", "VARCHAR(2000)");
    f.checkString("json_keys('{\"foo\":100}', 'lax $.foo1')",
        "null", "VARCHAR(2000)");

    // strict test
    f.checkString("json_keys('{}', 'strict $')",
        "[]", "VARCHAR(2000)");
    f.checkString("json_keys('[]', 'strict $')",
        "null", "VARCHAR(2000)");
    f.checkString("json_keys('{\"foo\":100}', 'strict $')",
        "[\"foo\"]", "VARCHAR(2000)");
    f.checkString("json_keys('{\"a\": 1, \"b\": {\"c\": 30}}', 'strict $')",
        "[\"a\",\"b\"]", "VARCHAR(2000)");
    f.checkString("json_keys('[1, 2, {\"a\": 3}]', 'strict $')",
        "null", "VARCHAR(2000)");
    f.checkString("json_keys('{\"a\": 1, \"b\": {\"c\": 30}}', 'strict $.b')",
        "[\"c\"]", "VARCHAR(2000)");

    // catch error test
    f.checkFails("json_keys('{\"foo\":100}', 'invalid $.foo')",
        "(?s).*Illegal jsonpath spec.*", true);
    f.checkFails("json_keys('{\"foo\":100}', 'strict $.foo1')",
        "(?s).*No results for path.*", true);

    // nulls
    f.enableTypeCoercion(false).checkFails("json_keys(^null^)",
        "(?s).*Illegal use of 'NULL'.*", false);
    f.checkString("json_keys(null)", null, "VARCHAR(2000)");
    f.checkNull("json_keys(cast(null as varchar))");
  }

  @Test void testJsonRemove() {
    final SqlOperatorFixture f = fixture();
    f.checkString("json_remove('{\"foo\":100}', '$.foo')",
        "{}", "VARCHAR(2000)");
    f.checkString("json_remove('{\"foo\":100, \"foo1\":100}', '$.foo')",
        "{\"foo1\":100}", "VARCHAR(2000)");
    f.checkString("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$[1][0]')",
        "[\"a\",[\"c\"],\"d\"]", "VARCHAR(2000)");
    f.checkString("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$[1]')",
        "[\"a\",\"d\"]", "VARCHAR(2000)");
    f.checkString("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$[0]', '$[0]')",
        "[\"d\"]", "VARCHAR(2000)");
    f.checkFails("json_remove('[\"a\", [\"b\", \"c\"], \"d\"]', '$')",
        "(?s).*Invalid input for.*", true);

    // nulls
    f.enableTypeCoercion(false).checkFails("json_remove(^null^, '$')",
        "(?s).*Illegal use of 'NULL'.*", false);
    f.checkString("json_remove(null, '$')", null, "VARCHAR(2000)");
    f.checkNull("json_remove(cast(null as varchar), '$')");
  }

  @Test void testJsonObject() {
    final SqlOperatorFixture f = fixture();
    f.checkString("json_object()", "{}", "VARCHAR(2000) NOT NULL");
    f.checkString("json_object('foo': 'bar')",
        "{\"foo\":\"bar\"}", "VARCHAR(2000) NOT NULL");
    f.checkString("json_object('foo': 'bar', 'foo2': 'bar2')",
        "{\"foo\":\"bar\",\"foo2\":\"bar2\"}", "VARCHAR(2000) NOT NULL");
    f.checkString("json_object('foo': null)",
        "{\"foo\":null}", "VARCHAR(2000) NOT NULL");
    f.checkString("json_object('foo': null null on null)",
        "{\"foo\":null}", "VARCHAR(2000) NOT NULL");
    f.checkString("json_object('foo': null absent on null)",
        "{}", "VARCHAR(2000) NOT NULL");
    f.checkString("json_object('foo': 100)",
        "{\"foo\":100}", "VARCHAR(2000) NOT NULL");
    f.checkString("json_object('foo': json_object('foo': 'bar'))",
        "{\"foo\":{\"foo\":\"bar\"}}", "VARCHAR(2000) NOT NULL");
    f.checkString("json_object('foo': json_object('foo': 'bar') format json)",
        "{\"foo\":{\"foo\":\"bar\"}}", "VARCHAR(2000) NOT NULL");
  }

  @Test void testJsonObjectAgg() {
    final SqlOperatorFixture f = fixture();
    f.checkAggType("json_objectagg('foo': 'bar')", "VARCHAR(2000) NOT NULL");
    f.checkAggType("json_objectagg('foo': null)", "VARCHAR(2000) NOT NULL");
    f.checkAggType("json_objectagg(100: 'bar')", "VARCHAR(2000) NOT NULL");
    f.enableTypeCoercion(false).checkFails("^json_objectagg(100: 'bar')^",
        "(?s).*Cannot apply.*", false);
    final String[][] values = {
        {"'foo'", "'bar'"},
        {"'foo2'", "cast(null as varchar(2000))"},
        {"'foo3'", "'bar3'"}
    };
    f.checkAggWithMultipleArgs("json_objectagg(x: x2)",
        values,
        isSingle("{\"foo \":\"bar\",\"foo2\":null,\"foo3\":\"bar3\"}"));
    f.checkAggWithMultipleArgs("json_objectagg(x: x2 null on null)",
        values,
        isSingle("{\"foo \":\"bar\",\"foo2\":null,\"foo3\":\"bar3\"}"));
    f.checkAggWithMultipleArgs("json_objectagg(x: x2 absent on null)",
        values,
        isSingle("{\"foo \":\"bar\",\"foo3\":\"bar3\"}"));
  }

  @Test void testJsonValueExpressionOperator() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("'{}' format json", "{}", "ANY NOT NULL");
    f.checkScalar("'[1, 2, 3]' format json", "[1,2,3]", "ANY NOT NULL");
    f.checkNull("cast(null as varchar) format json");
    f.checkScalar("'null' format json", "null", "ANY NOT NULL");
    f.enableTypeCoercion(false)
        .checkFails("^null^ format json", "(?s).*Illegal use of .NULL.*",
            false);
  }

  @Test void testJsonArray() {
    final SqlOperatorFixture f = fixture();
    f.checkString("json_array()", "[]", "VARCHAR(2000) NOT NULL");
    f.checkString("json_array('foo')",
        "[\"foo\"]", "VARCHAR(2000) NOT NULL");
    f.checkString("json_array('foo', 'bar')",
        "[\"foo\",\"bar\"]", "VARCHAR(2000) NOT NULL");
    f.checkString("json_array(null)",
        "[]", "VARCHAR(2000) NOT NULL");
    f.checkString("json_array(null null on null)",
        "[null]", "VARCHAR(2000) NOT NULL");
    f.checkString("json_array(null absent on null)",
        "[]", "VARCHAR(2000) NOT NULL");
    f.checkString("json_array(100)",
        "[100]", "VARCHAR(2000) NOT NULL");
    f.checkString("json_array(json_array('foo'))",
        "[[\"foo\"]]", "VARCHAR(2000) NOT NULL");
    f.checkString("json_array(json_array('foo') format json)",
        "[[\"foo\"]]", "VARCHAR(2000) NOT NULL");
  }

  @Test void testJsonArrayAgg() {
    final SqlOperatorFixture f = fixture();
    f.checkAggType("json_arrayagg('foo')", "VARCHAR(2000) NOT NULL");
    f.checkAggType("json_arrayagg(null)", "VARCHAR(2000) NOT NULL");
    final String[] values = {
        "'foo'",
        "cast(null as varchar(2000))",
        "'foo3'"
    };
    f.checkAgg("json_arrayagg(x)", values, isSingle("[\"foo\",\"foo3\"]"));
    f.checkAgg("json_arrayagg(x null on null)", values,
        isSingle("[\"foo\",null,\"foo3\"]"));
    f.checkAgg("json_arrayagg(x absent on null)", values,
        isSingle("[\"foo\",\"foo3\"]"));
  }

  @Test void testJsonPredicate() {
    final SqlOperatorFixture f = fixture();
    f.checkBoolean("'{}' is json value", true);
    f.checkBoolean("'{]' is json value", false);
    f.checkBoolean("'{}' is json object", true);
    f.checkBoolean("'[]' is json object", false);
    f.checkBoolean("'{}' is json array", false);
    f.checkBoolean("'[]' is json array", true);
    f.checkBoolean("'100' is json scalar", true);
    f.checkBoolean("'[]' is json scalar", false);
    f.checkBoolean("'{}' is not json value", false);
    f.checkBoolean("'{]' is not json value", true);
    f.checkBoolean("'{}' is not json object", false);
    f.checkBoolean("'[]' is not json object", true);
    f.checkBoolean("'{}' is not json array", true);
    f.checkBoolean("'[]' is not json array", false);
    f.checkBoolean("'100' is not json scalar", false);
    f.checkBoolean("'[]' is not json scalar", true);
    f.checkNull("null is json value");
    f.checkNull("null is json object");
    f.checkNull("null is json array");
    f.checkNull("null is json scalar");
    f.checkNull("null is not json value");
    f.checkNull("null is not json object");
    f.checkNull("null is not json array");
    f.checkNull("null is not json scalar");
  }

  @Test void testCompress() {
    SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.MYSQL);
    f.checkNull("COMPRESS(NULL)");
    f.checkString("COMPRESS('')", "",
        "VARBINARY NOT NULL");

    f.checkString("COMPRESS(REPEAT('a',1000))",
        "e8030000789c4b4c1c05a360140c770000f9d87af8", "VARBINARY NOT NULL");
    f.checkString("COMPRESS(REPEAT('a',16))",
        "10000000789c4b4c44050033980611", "VARBINARY NOT NULL");

    f.checkString("COMPRESS('sample')",
        "06000000789c2b4ecc2dc849050008de0283", "VARBINARY NOT NULL");
    f.checkString("COMPRESS('example')",
        "07000000789c4bad48cc2dc84905000bc002ed", "VARBINARY NOT NULL");
  }

  @Test void testUrlDecode() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.URL_DECODE);
    f0.checkFails("^URL_DECODE('https://calcite.apache.org')^",
        "No match found for function signature URL_DECODE\\(<CHARACTER>\\)",
        false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkString("URL_DECODE('https%3A%2F%2Fcalcite.apache.org')",
        "https://calcite.apache.org",
        "VARCHAR NOT NULL");
    f.checkString("URL_DECODE('http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd')",
        "http://test?a=b&c=d",
        "VARCHAR NOT NULL");
    f.checkString("URL_DECODE('http%3A%2F%2F%E4%BD%A0%E5%A5%BD')",
        "http://\u4F60\u597D",
        "VARCHAR NOT NULL");
    f.checkString("URL_DECODE('test')",
        "test",
        "VARCHAR NOT NULL");
    f.checkString("URL_DECODE('')",
        "",
        "VARCHAR NOT NULL");
    f.checkString("URL_DECODE('https%%3A%2F%2Fcalcite.apache.org')",
        "https%%3A%2F%2Fcalcite.apache.org",
        "VARCHAR NOT NULL");
    f.checkString("URL_DECODE('https%3A%2F%2Fcalcite.apache.org%')",
        "https%3A%2F%2Fcalcite.apache.org%",
        "VARCHAR NOT NULL");
    f.checkNull("URL_DECODE(cast(null as varchar))");
  }

  @Test void testUrlEncode() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.URL_ENCODE);
    f0.checkFails("^URL_ENCODE('https://calcite.apache.org')^",
        "No match found for function signature URL_ENCODE\\(<CHARACTER>\\)",
        false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkString("URL_ENCODE('https://calcite.apache.org')",
        "https%3A%2F%2Fcalcite.apache.org",
        "VARCHAR NOT NULL");
    f.checkString("URL_ENCODE('http://test?a=b&c=d')",
        "http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd",
        "VARCHAR NOT NULL");
    f.checkString("URL_ENCODE(_UTF8'http://\u4F60\u597D')",
        "http%3A%2F%2F%E4%BD%A0%E5%A5%BD",
        "VARCHAR NOT NULL");
    f.checkString("URL_ENCODE('test')",
        "test",
        "VARCHAR NOT NULL");
    f.checkString("URL_ENCODE('')",
        "",
        "VARCHAR NOT NULL");
    f.checkNull("URL_ENCODE(cast(null as varchar))");
  }

  @Test void testExtractValue() {
    SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.MYSQL);
    f.checkNull("ExtractValue(NULL, '//b')");
    f.checkNull("ExtractValue('', NULL)");
    f.checkFails("ExtractValue('<a><b/></a>', '#/a/b')",
        "Invalid input for EXTRACTVALUE: xml: '.*", true);
    f.checkFails("ExtractValue('<a><b/></a></a>', '/b')",
        "Invalid input for EXTRACTVALUE: xml: '.*", true);

    f.checkString("ExtractValue('<a>c</a>', '//a')",
        "c", "VARCHAR(2000)");
    f.checkString("ExtractValue('<a>ccc<b>ddd</b></a>', '/a')",
        "ccc", "VARCHAR(2000)");
    f.checkString("ExtractValue('<a>ccc<b>ddd</b></a>', '/a/b')",
        "ddd", "VARCHAR(2000)");
    f.checkString("ExtractValue('<a>ccc<b>ddd</b></a>', '/b')",
        "", "VARCHAR(2000)");
    f.checkString("ExtractValue('<a>ccc<b>ddd</b><b>eee</b></a>', '//b')",
        "ddd eee", "VARCHAR(2000)");
    f.checkString("ExtractValue('<a><b/></a>', 'count(/a/b)')",
        "1", "VARCHAR(2000)");
  }

  @Test void testXmlTransform() {
    SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.ORACLE);
    f.checkNull("XMLTRANSFORM('', NULL)");
    f.checkNull("XMLTRANSFORM(NULL,'')");

    f.checkFails("XMLTRANSFORM('', '<')",
        "Illegal xslt specified : '.*", true);
    final String sql = "XMLTRANSFORM('<', '<?xml version=\"1.0\"?>\n"
        + "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
        + "</xsl:stylesheet>')";
    f.checkFails(sql,
        "Invalid input for XMLTRANSFORM xml: '.*", true);

    final String sql2 = "XMLTRANSFORM("
        + "'<?xml version=\"1.0\"?>\n"
        + "<Article>\n"
        + "  <Title>My Article</Title>\n"
        + "  <Authors>\n"
        + "    <Author>Mr. Foo</Author>\n"
        + "    <Author>Mr. Bar</Author>\n"
        + "  </Authors>\n"
        + "  <Body>This is my article text.</Body>\n"
        + "</Article>'"
        + ","
        + "'<?xml version=\"1.0\"?>\n"
        + "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3"
        + ".org/1999/XSL/Transform\">"
        + "  <xsl:output method=\"text\"/>"
        + "  <xsl:template match=\"/\">"
        + "    Article - <xsl:value-of select=\"/Article/Title\"/>"
        + "    Authors: <xsl:apply-templates select=\"/Article/Authors/Author\"/>"
        + "  </xsl:template>"
        + "  <xsl:template match=\"Author\">"
        + "    - <xsl:value-of select=\".\" />"
        + "  </xsl:template>"
        + "</xsl:stylesheet>')";
    f.checkString(sql2,
        "    Article - My Article    Authors:     - Mr. Foo    - Mr. Bar",
        "VARCHAR");

    // Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-5813">[CALCITE-5813]
    // Type inference for REPEAT sql function is incorrect</a>. This test shows that the
    // output of the XML_TRANSFORM function can exceed 2000 characters. */
    StringBuilder sql3 = new StringBuilder();
    StringBuilder expected = new StringBuilder();
    sql3.append("XMLTRANSFORM("
        + "'<?xml version=\"1.0\"?>\n"
        + "<Article>\n"
        + "  <Title>My Article</Title>\n"
        + "  <Authors>\n"
        + "    <Author>Mr. Foo</Author>\n"
        + "    <Author>Mr. Bar</Author>\n");
    expected.append("    Article - My Article    Authors:     - Mr. Foo    - Mr. Bar");
    for (int i = 0; i < 40; i++) {
      final String row = "Mr. Bar                                                        " + i;
      sql3.append("    <Author>")
          .append(row)
          .append("</Author>\n");
      expected.append("    - ").append(row);
    }
    sql3.append("  </Authors>\n"
        + "  <Body>This is my article text.</Body>\n"
        + "</Article>'"
        + ","
        + "'<?xml version=\"1.0\"?>\n"
        + "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3"
        + ".org/1999/XSL/Transform\">"
        + "  <xsl:output method=\"text\"/>"
        + "  <xsl:template match=\"/\">"
        + "    Article - <xsl:value-of select=\"/Article/Title\"/>"
        + "    Authors: <xsl:apply-templates select=\"/Article/Authors/Author\"/>"
        + "  </xsl:template>"
        + "  <xsl:template match=\"Author\">"
        + "    - <xsl:value-of select=\".\" />"
        + "  </xsl:template>"
        + "</xsl:stylesheet>')");
    f.checkString(sql3.toString(), expected.toString(), "VARCHAR");
  }

  @Test void testExtractXml() {
    SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.ORACLE);

    f.checkFails("\"EXTRACT\"('', '<','a')",
        "Invalid input for EXTRACT xpath: '.*", true);
    f.checkFails("\"EXTRACT\"('', '<')",
        "Invalid input for EXTRACT xpath: '.*", true);
    f.checkNull("\"EXTRACT\"('', NULL)");
    f.checkNull("\"EXTRACT\"(NULL,'')");

    f.checkString("\"EXTRACT\"("
            + "'<Article>"
            + "<Title>Article1</Title>"
            + "<Authors>"
            + "<Author>Foo</Author>"
            + "<Author>Bar</Author>"
            + "</Authors>"
            + "<Body>article text.</Body>"
            + "</Article>', '/Article/Title')",
        "<Title>Article1</Title>",
        "VARCHAR");

    f.checkString("\"EXTRACT\"('"
            + "<Article>"
            + "<Title>Article1</Title>"
            + "<Title>Article2</Title>"
            + "<Authors><Author>Foo</Author><Author>Bar</Author></Authors>"
            + "<Body>article text.</Body>"
            + "</Article>', '/Article/Title')",
        "<Title>Article1</Title><Title>Article2</Title>",
        "VARCHAR");

    // Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-5813">[CALCITE-5813]
    // Type inference for REPEAT sql function is incorrect</a>. This test shows that
    // the output of the 'XML_EXTRACT' function can exceed 2000 characters. */
    StringBuilder sql = new StringBuilder();
    StringBuilder expected = new StringBuilder();
    sql.append("\"EXTRACT\"('"
        + "<Article>"
        + "<Title>Article1</Title>"
        + "<Title>Article2");
    expected.append("<Title>Article1</Title><Title>Article2");
    final String spaces =
        "                                                                               ";
    for (int i = 0; i < 40; i++) {
      sql.append(spaces);
      expected.append(spaces);
    }
    sql.append("Long</Title>"
        + "<Authors><Author>Foo</Author><Author>Bar</Author></Authors>"
        + "<Body>article text.</Body>"
        + "</Article>', '/Article/Title')");
    expected.append("Long</Title>");
    f.checkString(sql.toString(), expected.toString(), "VARCHAR");

    f.checkString("\"EXTRACT\"(\n"
            + "'<books xmlns=\"http://www.contoso.com/books\">"
            + "<book><title>Title</title>"
            + "<author>Author Name</author>"
            + "<price>5.50</price>"
            + "</book>"
            + "</books>', "
            + "'/books:books/books:book', "
            + "'books=\"http://www.contoso.com/books\"')",
        "<book xmlns=\"http://www.contoso.com/books\"><title>Title</title><author>Author "
            + "Name</author><price>5.50</price></book>",
        "VARCHAR");
  }

  @Test void testExistsNode() {
    SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.ORACLE);

    f.checkFails("EXISTSNODE('', '<','a')",
        "Invalid input for EXISTSNODE xpath: '.*", true);
    f.checkFails("EXISTSNODE('', '<')",
        "Invalid input for EXISTSNODE xpath: '.*", true);
    f.checkNull("EXISTSNODE('', NULL)");
    f.checkNull("EXISTSNODE(NULL,'')");

    f.checkString("EXISTSNODE('<Article>"
            + "<Title>Article1</Title>"
            + "<Authors><Author>Foo</Author><Author>Bar</Author></Authors>"
            + "<Body>article text.</Body>"
            + "</Article>', '/Article/Title')",
        "1",
        "INTEGER");

    f.checkString("EXISTSNODE('<Article>"
            + "<Title>Article1</Title>"
            + "<Authors><Author>Foo</Author><Author>Bar</Author></Authors>"
            + "<Body>article text.</Body></Article>', '/Article/Title/Books')",
        "0",
        "INTEGER");

    f.checkString("EXISTSNODE('<Article>"
            + "<Title>Article1</Title>"
            + "<Title>Article2</Title>"
            + "<Authors><Author>Foo</Author><Author>Bar</Author></Authors>"
            + "<Body>article text.</Body></Article>', '/Article/Title')",
        "1",
        "INTEGER");

    f.checkString("EXISTSNODE(\n"
            + "'<books xmlns=\"http://www.contoso.com/books\">"
            + "<book>"
            + "<title>Title</title>"
            + "<author>Author Name</author>"
            + "<price>5.50</price>"
            + "</book>"
            + "</books>', "
            + "'/books:books/books:book', "
            + "'books=\"http://www.contoso.com/books\"')",
        "1",
        "INTEGER");
    f.checkString("EXISTSNODE(\n"
            + "'<books xmlns=\"http://www.contoso.com/books\">"
            + "<book><title>Title</title>"
            + "<author>Author Name</author>"
            + "<price>5.50</price></book></books>', "
            + "'/books:books/books:book/books:title2', "
            + "'books=\"http://www.contoso.com/books\"'"
            + ")",
        "0",
        "INTEGER");
  }

  @Test void testLowerFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LOWER, VmName.EXPAND);

    // SQL:2003 6.29.8 The type of lower is the type of its argument
    f.checkString("lower('A')", "a", "CHAR(1) NOT NULL");
    f.checkString("lower('a')", "a", "CHAR(1) NOT NULL");
    f.checkString("lower('1')", "1", "CHAR(1) NOT NULL");
    f.checkString("lower('AA')", "aa", "CHAR(2) NOT NULL");
    f.checkNull("lower(cast(null as varchar(1)))");
  }

  @Test void testInitcapFunc() {
    // Note: the initcap function is an Oracle defined function and is not
    // defined in the SQL:2003 standard
    // todo: implement in fennel
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.INITCAP);

    f.checkString("initcap('aA')", "Aa", "CHAR(2) NOT NULL");
    f.checkString("initcap('Aa')", "Aa", "CHAR(2) NOT NULL");
    f.checkString("initcap('1a')", "1a", "CHAR(2) NOT NULL");
    f.checkString("initcap('ab cd Ef 12')",
        "Ab Cd Ef 12",
        "CHAR(11) NOT NULL");
    f.checkNull("initcap(cast(null as varchar(1)))");

    // dtbug 232
    f.enableTypeCoercion(false)
        .checkFails("^initcap(cast(null as date))^",
            "Cannot apply 'INITCAP' to arguments of type "
                + "'INITCAP\\(<DATE>\\)'\\. Supported form\\(s\\): "
                + "'INITCAP\\(<CHARACTER>\\)'",
            false);
    f.checkType("initcap(cast(null as date))", "VARCHAR");
  }

  @Test void testPowerFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.POWER, VmName.EXPAND);
    f.checkScalarApprox("power(2,-2)", "DOUBLE NOT NULL", isExactly("0.25"));
    f.checkScalarApprox("power(cast(2 as decimal), cast(-2 as decimal))",
        "DOUBLE NOT NULL",
        isExactly("0.25"));
    f.checkNull("power(cast(null as integer),2)");
    f.checkNull("power(2,cast(null as double))");

    // 'pow' is an obsolete form of the 'power' function
    f.checkFails("^pow(2,-2)^",
        "No match found for function signature POW\\(<NUMERIC>, <NUMERIC>\\)",
        false);
  }

  @Test void testPowerDecimalFunc() {
    final SqlOperatorFixture f = fixture()
        .withOperatorTable(
            SqlOperatorTables.chain(
            SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
                ImmutableList.of(SqlLibrary.POSTGRESQL, SqlLibrary.ALL),
                false),
            SqlStdOperatorTable.instance()))
        .setFor(SqlLibraryOperators.POWER_PG);
    f.checkScalarApprox("power(cast(2 as decimal), cast(-2 as decimal))",
        "DECIMAL(17, 0) NOT NULL",
        isExactly("0.25"));
    f.checkScalarApprox("power(cast(2 as decimal), -2)",
        "DECIMAL(17, 0) NOT NULL",
        isExactly("0.25"));
    f.checkScalarApprox("power(2, cast(-2 as decimal))",
        "DECIMAL(17, 0) NOT NULL",
        isExactly("0.25"));
    f.checkScalarApprox("power(2, -2)", "DOUBLE NOT NULL", isExactly("0.25"));
    f.checkScalarApprox("power(CAST(0.25 AS DOUBLE), CAST(0.5 AS DOUBLE))",
        "DOUBLE NOT NULL", isExactly("0.5"));
    f.checkNull("power(null, -2)");
    f.checkNull("power(2, null)");
  }

  @Test void testSqrtFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.SQRT, VmName.EXPAND);
    f.checkType("sqrt(2)", "DOUBLE NOT NULL");
    f.checkType("sqrt(cast(2 as float))", "DOUBLE NOT NULL");
    f.checkType("sqrt(case when false then 2 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^sqrt('abc')^",
            "Cannot apply 'SQRT' to arguments of type "
                + "'SQRT\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'SQRT\\(<NUMERIC>\\)'",
            false);
    f.checkType("sqrt('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("sqrt(2)", "DOUBLE NOT NULL",
        isWithin(1.4142d, 0.0001d));
    f.checkScalarApprox("sqrt(cast(2 as decimal(2, 0)))", "DOUBLE NOT NULL",
        isWithin(1.4142d, 0.0001d));
    f.checkScalarApprox("sqrt(0)", "DOUBLE NOT NULL",
        isWithin(0, 0.0001d));
    f.checkScalarApprox("sqrt(0.1)", "DOUBLE NOT NULL",
        isWithin(0.31622776601683794, 0.0001d));
    f.checkScalarApprox("sqrt(2.0/3)", "DOUBLE NOT NULL",
        isWithin(0.816496580927726, 0.0001d));
    f.checkScalarApprox("sqrt(cast(10e8 as integer))", "DOUBLE NOT NULL",
        isWithin(31622.776601683792, 0.0001d));
    f.checkScalarApprox("sqrt(cast(10e8 as double))", "DOUBLE NOT NULL",
        isWithin(31622.776601683792, 0.0001d));
    f.checkScalarApprox("sqrt(-1)", "DOUBLE NOT NULL",
        "NaN");
    f.checkNull("sqrt(cast(null as integer))");
    f.checkNull("sqrt(cast(null as double))");
  }

  @Test void testExpFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EXP);
    f.checkScalarApprox("exp(2)", "DOUBLE NOT NULL",
        isWithin(7.389056, 0.000001));
    f.checkScalarApprox("exp(-2)", "DOUBLE NOT NULL",
        isWithin(0.1353, 0.0001));
    f.checkNull("exp(cast(null as integer))");
    f.checkNull("exp(cast(null as double))");
  }

  @Test void testModFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MOD, VmName.EXPAND);
    f.checkScalarExact("mod(4,2)", 0);
    f.checkScalarExact("mod(8,5)", 3);
    f.checkScalarExact("mod(-12,7)", -5);
    f.checkScalarExact("mod(-12,-7)", -5);
    f.checkScalarExact("mod(12,-7)", 5);
    f.checkScalarExact("mod(cast(12 as tinyint), cast(-7 as tinyint))",
        "TINYINT NOT NULL", "5");

    f.checkScalarExact("mod(cast(9 as decimal(2, 0)), 7)",
        "INTEGER NOT NULL", "2");
    f.checkScalarExact("mod(7, cast(9 as decimal(2, 0)))",
        "DECIMAL(2, 0) NOT NULL", "7");
    f.checkScalarExact("mod(cast(-9 as decimal(2, 0)), "
            + "cast(7 as decimal(1, 0)))",
        "DECIMAL(1, 0) NOT NULL", "-2");
  }

  @Test void testModFuncNull() {
    final SqlOperatorFixture f = fixture();
    f.checkNull("mod(cast(null as integer),2)");
    f.checkNull("mod(4,cast(null as tinyint))");
    f.checkNull("mod(4,cast(null as decimal(12,0)))");
  }

  @Test void testModFuncDivByZero() {
    // The extra CASE expression is to fool Janino.  It does constant
    // reduction and will throw the divide by zero exception while
    // compiling the expression.  The test framework would then issue
    // unexpected exception occurred during "validation".  You cannot
    // submit as non-runtime because the janino exception does not have
    // error position information and the framework is unhappy with that.
    final SqlOperatorFixture f = fixture();
    f.checkFails("mod(3,case 'a' when 'a' then 0 end)",
        DIVISION_BY_ZERO_MESSAGE, true);
  }

  @Test void testLnFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LN, VmName.EXPAND);
    f.checkScalarApprox("ln(2.71828)", "DOUBLE NOT NULL",
        isWithin(1.0, 0.000001));
    f.checkScalarApprox("ln(2.71828)", "DOUBLE NOT NULL",
        isWithin(0.999999327, 0.0000001));
    f.checkNull("ln(cast(null as tinyint))");
  }

  @Test void testLog10Func() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LOG10, VmName.EXPAND);
    f.checkScalarApprox("log10(10)", "DOUBLE NOT NULL",
        isWithin(1.0, 0.000001));
    f.checkScalarApprox("log10(100.0)", "DOUBLE NOT NULL",
        isWithin(2.0, 0.000001));
    f.checkScalarApprox("log10(cast(10e8 as double))", "DOUBLE NOT NULL",
        isWithin(9.0, 0.000001));
    f.checkScalarApprox("log10(cast(10e2 as float))", "DOUBLE NOT NULL",
        isWithin(3.0, 0.000001));
    f.checkScalarApprox("log10(cast(10e-3 as real))", "DOUBLE NOT NULL",
        isWithin(-2.0, 0.000001));
    f.checkNull("log10(cast(null as real))");
  }

  @Test void testLogFunc() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.LOG, VmName.EXPAND);
    f0.checkFails("^log(100, 10)^",
        "No match found for function signature LOG\\(<NUMERIC>, <NUMERIC>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    // Test case for https://issues.apache.org/jira/browse/CALCITE-6984
    // FamilyOperandTypeChecker with a Predicate describing optional arguments does not
    // reject mistyped expressions
    f.checkFails("^log(x'')^",
        "Cannot apply 'LOG' to arguments of type 'LOG\\(<BINARY\\(0\\)>\\)'.*\\n.*", false);
    f.checkScalarApprox("log(10, 10)", "DOUBLE NOT NULL",
        isWithin(1.0, 0.000001));
    f.checkScalarApprox("log(64, 8)", "DOUBLE NOT NULL",
        isWithin(2.0, 0.000001));
    f.checkScalarApprox("log(27,3)", "DOUBLE NOT NULL",
        isWithin(3.0, 0.000001));
    f.checkScalarApprox("log(100, 10)", "DOUBLE NOT NULL",
        isWithin(2.0, 0.000001));
    f.checkScalarApprox("log(10, 100)", "DOUBLE NOT NULL",
        isWithin(0.5, 0.000001));
    f.checkScalarApprox("log(cast(1e7 as double), 10)", "DOUBLE NOT NULL",
        isWithin(7.0, 0.000001));
    f.checkScalarApprox("log(cast(10e8 as float), 10)", "DOUBLE NOT NULL",
        isWithin(9.0, 0.000001));
    f.checkScalarApprox("log(cast(10e-3 as real), 10)", "DOUBLE NOT NULL",
        isWithin(-2.0, 0.000001));
    f.checkScalarApprox("log(10)", "DOUBLE NOT NULL",
        isWithin(2.302585092994046, 0.000001));
    f.checkNull("log(cast(null as real), 10)");
    f.checkNull("log(10, cast(null as real))");
    f.checkFails("log(0)",
        "Cannot take logarithm of zero or negative number", true);
    f.checkFails("log(0, 64)",
        "Cannot take logarithm of zero or negative number", true);
    f.checkFails("log(64, 0)",
        "Cannot take logarithm of zero or negative number", true);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6224">[CALCITE-6224]
   * Add LOG2 function (enabled in MYSQL, Spark library)</a>. */
  @Test void testLog2Func() {
    final SqlOperatorFixture f0 = fixture();
    f0.checkFails("^log2(4)^",
        "No match found for function signature LOG2\\(<NUMERIC>\\)", false);
    f0.setFor(SqlLibraryOperators.LOG2);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalarApprox("log2(2)", "DOUBLE",
          isWithin(1.0, 0.000001));
      f.checkScalarApprox("log2(4)", "DOUBLE",
          isWithin(2.0, 0.000001));
      f.checkScalarApprox("log2(65536)", "DOUBLE",
          isWithin(16.0, 0.000001));
      f.checkScalarApprox("log2(2.0/3)", "DOUBLE",
          isWithin(-0.5849625007211561, 0.000001));
      f.checkScalarApprox("log2(4.0/3)", "DOUBLE",
          isWithin(0.4150374992788435, 0.000001));
      f.checkScalarApprox("log2(0.5)", "DOUBLE",
          isWithin(-1.0, 0.000001));
      f.checkScalarApprox("log2(cast(10e8 as double))", "DOUBLE",
          isWithin(29.897352853986263, 0.000001));
      f.checkScalarApprox("log2(cast(10e8 as float))", "DOUBLE",
          isWithin(29.897352853986263, 0.000001));
      f.checkScalarApprox("log2(1e+52)", "DOUBLE",
          isWithin(172.74026093414284, 0.000001));
      f.checkNull("log2(0)");
      f.checkNull("log2(-2)");
      f.checkNull("log2(+0.0)");
      f.checkNull("log2(-0.0)");
      f.checkNull("log2(null)");
      f.checkNull("log2(cast(null as real))");
    };
    f0.forEachLibrary(list(SqlLibrary.MYSQL, SqlLibrary.SPARK), consumer);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6259">[CALCITE-6259]
   * Add LOG function (enabled in MYSQL, Spark library)</a>. */
  @Test void testLogMysqlSparkFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.checkFails("^log(100, 10)^",
        "No match found for function signature LOG\\(<NUMERIC>, <NUMERIC>\\)", false);
    f0.setFor(SqlLibraryOperators.LOG_MYSQL);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalarApprox("log(10, 10)", "DOUBLE",
          isWithin(1.0, 0.000001));
      f.checkScalarApprox("log(8, 64)", "DOUBLE",
          isWithin(2.0, 0.000001));
      f.checkScalarApprox("log(3,27)", "DOUBLE",
          isWithin(3.0, 0.000001));
      f.checkScalarApprox("log(10, 100)", "DOUBLE",
          isWithin(2.0, 0.000001));
      f.checkScalarApprox("log(100, 10)", "DOUBLE",
          isWithin(0.5, 0.000001));
      f.checkScalarApprox("log(10, cast(1e7 as double))", "DOUBLE",
          isWithin(7.0, 0.000001));
      f.checkScalarApprox("log(10, cast(1e9 as float))", "DOUBLE",
          isWithin(9.0, 0.000001));
      // real type is equivalent to double type
      f.checkScalarApprox("log(10, cast(1e-2 as real))", "DOUBLE",
          isWithin(-2.0, 0.000001));
      f.checkScalarApprox("log(10)", "DOUBLE",
          isWithin(2.302585092994046, 0.000001));
      f.checkNull("log(cast(null as real), 10)");
      f.checkNull("log(10, cast(null as real))");
      f.checkNull("log(2, 0)");
      f.checkNull("log(-2,0)");
      f.checkNull("log(0, +0.0)");
      f.checkNull("log(0, 0.0)");
      f.checkNull("log(null)");
      f.checkNull("log(cast(null as real))");
      f.checkNull("log(0)");
      f.checkNull("log(-1)");
    };
    f0.forEachLibrary(list(SqlLibrary.MYSQL, SqlLibrary.SPARK, SqlLibrary.HIVE), consumer);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6312">[CALCITE-6312]
   * Add LOG function (enabled in PostgreSQL library)</a>. */
  @Test void testPostgresLogFunc() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.LOG_POSTGRES, VmName.EXPAND);
    f0.checkFails("^log(100, 10)^",
        "No match found for function signature LOG\\(<NUMERIC>, <NUMERIC>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.POSTGRESQL);
    f.checkScalar("log(10, 10)", 1.0,
        "DOUBLE NOT NULL");
    f.checkScalar("log(8, 64)", 2.0,
        "DOUBLE NOT NULL");
    f.checkScalar("log(10, 100)", 2.0,
        "DOUBLE NOT NULL");
    f.checkScalar("log(100, 10)", 0.5,
        "DOUBLE NOT NULL");
    f.checkScalar("log(10, cast(1e7 as double))", 7.0,
        "DOUBLE NOT NULL");
    f.checkScalar("log(10)", 1.0,
        "DOUBLE NOT NULL");
    f.checkNull("log(cast(null as real), 10)");
    f.checkNull("log(10, cast(null as real))");
    f.checkFails("log(0)",
        "Cannot take logarithm of zero or negative number", true);
    f.checkFails("log(0, 64)",
        "Cannot take logarithm of zero or negative number", true);
    f.checkFails("log(64, 0)",
        "Cannot take logarithm of zero or negative number", true);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6549">[CALCITE-6549]
   * Add LOG1P function (enabled in Spark library)</a>. */
  @Test void testLog1PFunc() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.LOG1P, VmName.EXPAND);
    f0.checkFails("^log1p(4)^",
        "No match found for function signature LOG1P\\(<NUMERIC>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalarApprox("log1p(0)", "DOUBLE",
        isWithin(0.0, 0.000001));
    f.checkScalarApprox("log1p(1)", "DOUBLE",
        isWithin(0.6931471805599453, 0.000001));
    f.checkScalarApprox("log1p(1e+22)", "DOUBLE",
        isWithin(50.65687204586901, 0.000001));
    f.checkScalarApprox("log1p(1.2)", "DOUBLE",
        isWithin(0.7884573603642702, 0.000001));
    f.checkScalarApprox("log1p(2.0/3)", "DOUBLE",
        isWithin(0.5108256237659907, 0.000001));
    f.checkNull("log1p(cast(null as real))");
    f.checkNull("log1p(-1)");
    f.checkNull("log1p(null)");
    f.checkFails("^log1p()^", INVALID_ARGUMENTS_NUMBER, false);
  }

  @Test void testRandFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.RAND, VmName.EXPAND);
    f.checkFails("^rand^", "Column 'RAND' not found in any table", false);
    for (int i = 0; i < 100; i++) {
      // Result must always be between 0 and 1, inclusive.
      f.checkScalarApprox("rand()", "DOUBLE NOT NULL", isWithin(0.5, 0.5));
    }
  }

  @Test void testRandSeedFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.RAND, VmName.EXPAND);
    f.checkScalarApprox("rand(1)", "DOUBLE NOT NULL", isWithin(0.6016, 0.0001));
    f.checkScalarApprox("rand(2)", "DOUBLE NOT NULL", isWithin(0.4728, 0.0001));
  }

  @Test void testRandIntegerFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.RAND_INTEGER, VmName.EXPAND);
    for (int i = 0; i < 100; i++) {
      // Result must always be between 0 and 10, inclusive.
      f.checkScalarApprox("rand_integer(11)", "INTEGER NOT NULL",
          isWithin(5.0, 5.0));
    }
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6314">[CALCITE-6314]
   * Add RANDOM function (enabled in Postgres library)</a>. */
  @Test void testRandomFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.RANDOM, VmName.EXPAND);
    f.checkFails("^random^", "Column 'RANDOM' not found in any table", false);
    Consumer<SqlOperatorFixture> consumer = fixture -> {
      for (int i = 0; i < 100; i++) {
        // Result must always be between 0 and 1, inclusive.
        fixture.checkScalarApprox("random()", "DOUBLE NOT NULL", isWithin(0.5, 0.5));
      }
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.POSTGRESQL, SqlLibrary.REDSHIFT);
    f.forEachLibrary(libraries, consumer);
  }

  @Test void testRandIntegerSeedFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.RAND_INTEGER, VmName.EXPAND);
    f.checkScalar("rand_integer(1, 11)", 4, "INTEGER NOT NULL");
    f.checkScalar("rand_integer(2, 11)", 1, "INTEGER NOT NULL");
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6283">
   * [CALCITE-6283] Function array_append with a NULL array argument crashes with
   * NullPointerException</a>. */
  @Test void testArrayNullFunc() {
    final String expected = "Illegal use of 'NULL'";
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.SPARK);
    f.checkFails("array_append(^null^, 2)", expected, false);
    f.checkFails("array_prepend(^null^, 2)", expected, false);
    f.checkFails("array_remove(^null^, 2)", expected, false);
    f.checkFails("array_contains(^null^, 2)", expected, false);
    f.checkFails("array_position(^null^, 2)", expected, false);
    f.checkFails("^array_min(null)^",
        "Cannot apply 'ARRAY_MIN' to arguments of type 'ARRAY_MIN\\(<NULL>\\)'."
            + " Supported form\\(s\\): 'ARRAY_MIN\\(<ARRAY>\\)'", false);
    f.checkFails("^array_max(null)^",
        "Cannot apply 'ARRAY_MAX' to arguments of type 'ARRAY_MAX\\(<NULL>\\)'."
        + " Supported form\\(s\\): 'ARRAY_MAX\\(<ARRAY>\\)'", false);
  }

  /** Tests {@code ARRAY_APPEND} function from Spark. */
  @Test void testArrayAppendFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ARRAY_APPEND);
    f0.checkFails("^array_append(array[1], 2)^",
        "No match found for function signature ARRAY_APPEND\\("
            + "<INTEGER ARRAY>, <NUMERIC>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("array_append(array[1], 2)", "[1, 2]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array[1], null)", "[1, null]",
        "INTEGER ARRAY NOT NULL");
    f.checkScalar("array_append(array(null), null)", "[null, null]",
        "NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array(), null)", "[null]",
        "UNKNOWN ARRAY NOT NULL");
    f.checkScalar("array_append(array(), 1)", "[1]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array[array[1, 2]], array[3, 4])", "[[1, 2], [3, 4]]",
        "INTEGER NOT NULL ARRAY NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array[map[1, 'a']], map[2, 'b'])", "[{1=a}, {2=b}]",
        "(INTEGER NOT NULL, CHAR(1) NOT NULL) MAP NOT NULL ARRAY NOT NULL");
    f.checkNull("array_append(cast(null as integer array), 1)");
    f.checkType("array_append(cast(null as integer array), 1)", "INTEGER ARRAY");
    f.checkFails("^array_append(array[1, 2], true)^",
        "INTEGER is not comparable to BOOLEAN", false);

    // element cast to the biggest type
    f.checkScalar("array_append(array(cast(1 as tinyint)), 2)", "[1, 2]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array(cast(1 as double)), cast(2 as float))", "[1.0, 2.0]",
        "DOUBLE NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array(1), cast(2 as float))", "[1.0, 2.0]",
        "FLOAT NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array(1), cast(2 as double))", "[1.0, 2.0]",
        "DOUBLE NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array(1), cast(2 as bigint))", "[1, 2]",
        "BIGINT NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array(1, 2), cast(3 as double))", "[1.0, 2.0, 3.0]",
        "DOUBLE NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array(1, 2), cast(3 as float))", "[1.0, 2.0, 3.0]",
        "FLOAT NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array(1, 2), cast(3 as bigint))", "[1, 2, 3]",
        "BIGINT NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array(1, 2), cast(null as double))", "[1.0, 2.0, null]",
        "DOUBLE ARRAY NOT NULL");
    f.checkScalar("array_append(array(1, 2), cast(null as float))", "[1.0, 2.0, null]",
        "FLOAT ARRAY NOT NULL");
    f.checkScalar("array_append(array(1), cast(null as bigint))", "[1, null]",
        "BIGINT ARRAY NOT NULL");
    f.checkScalar("array_append(array(1), cast(100 as decimal))", "[1, 100]",
        "DECIMAL(19, 0) NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array(1), 10e6)", "[1.0, 1.0E7]",
        "DOUBLE NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_append(array(), cast(null as double))", "[null]",
        "DOUBLE ARRAY NOT NULL");
    f.checkScalar("array_append(array(), cast(null as float))", "[null]",
        "FLOAT ARRAY NOT NULL");
    f.checkScalar("array_append(array(), cast(null as tinyint))", "[null]",
        "TINYINT ARRAY NOT NULL");
    f.checkScalar("array_append(array(), cast(null as bigint))", "[null]",
        "BIGINT ARRAY NOT NULL");
  }

  /** Tests {@code ARRAY_COMPACT} function from Spark. */
  @Test void testArrayCompactFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ARRAY_COMPACT);
    f0.checkFails("^array_compact(array[null, 1, null, 2])^",
        "No match found for function signature ARRAY_COMPACT\\(<INTEGER ARRAY>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("array_compact(array[null, 1, null, 2])", "[1, 2]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_compact(array[1, 2])", "[1, 2]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_compact(array[null, 'hello', null, 'world'])", "[hello, world]",
        "CHAR(5) NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_compact(array['hello', 'world'])", "[hello, world]",
        "CHAR(5) NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_compact(array[null])", "[]",
        "NULL ARRAY NOT NULL");
    f.checkScalar("array_compact(array())", "[]",
        "UNKNOWN NOT NULL ARRAY NOT NULL");
    f.checkNull("array_compact(null)");
    // elements cast
    f.checkScalar("array_compact(array[null, 1, null, cast(2 as tinyint)])", "[1, 2]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_compact(array[null, 1, null, cast(2 as bigint)])", "[1, 2]",
        "BIGINT NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_compact(array[null, 1, null, cast(2 as decimal)])", "[1, 2]",
        "DECIMAL(19, 0) NOT NULL ARRAY NOT NULL");
  }

  /** Tests {@code ARRAY_CONCAT} function from BigQuery. */
  @Test void testArrayConcat() {
    SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.ARRAY_CONCAT)
        .withLibrary(SqlLibrary.BIG_QUERY);
    // Test case for [CALCITE-7105] ARRAY_CONCAT should only accept arguments with type ARRAY
    f.checkFails("^array_concat('a')^", "Cannot apply 'ARRAY_CONCAT'.*", false);
    f.checkFails("^array_concat()^", INVALID_ARGUMENTS_NUMBER, false);
    f.checkScalar("array_concat(array[1, 2], array[2, 3])", "[1, 2, 2, 3]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_concat(array[1, 2], array[2, null])", "[1, 2, 2, null]",
        "INTEGER ARRAY NOT NULL");
    f.checkScalar("array_concat(array['hello', 'world'], array['!'], "
            + "array[cast(null as char)])",
        "[hello, world, !, null]", "CHAR(5) ARRAY NOT NULL");
    f.checkNull("array_concat(cast(null as integer array), array[1])");
  }

  /** Tests {@code ARRAY_CONTAINS} function from Spark. */
  @Test void testArrayContainsFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ARRAY_CONTAINS);
    f0.checkFails("^array_contains(array[1, 2], 1)^",
        "No match found for function signature "
            + "ARRAY_CONTAINS\\(<INTEGER ARRAY>, <NUMERIC>\\)", false);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.SPARK);

    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("array_contains(array[1, 2], 1)", true,
          "BOOLEAN NOT NULL");
      f.checkScalar("array_contains(array[1], 1)", true,
          "BOOLEAN NOT NULL");
      f.checkScalar("array_contains(array(), 1)", false,
          "BOOLEAN NOT NULL");
      f.checkScalar("array_contains(array[array[1, 2], array[3, 4]], array[1, 2])", true,
          "BOOLEAN NOT NULL");
      f.checkScalar("array_contains(array[map[1, 'a'], map[2, 'b']], map[1, 'a'])", true,
          "BOOLEAN NOT NULL");
      f.checkNull("array_contains(cast(null as integer array), 1)");
      f.checkType("array_contains(cast(null as integer array), 1)", "BOOLEAN");
      // Flink and Spark differ on the following. The expression
      //   array_contains(array[1, null], cast(null as integer))
      // returns TRUE in Flink, and returns UNKNOWN in Spark. The current
      // function has Spark behavior, but if we supported a Flink function
      // library (i.e. "fun=flink") we could add a function with Flink behavior.
      f.checkNull("array_contains(array[1, null], cast(null as integer))");
      f.checkType("array_contains(array[1, null], cast(null as integer))", "BOOLEAN");
      f.checkFails("^array_contains(array[1, 2], true)^",
          "INTEGER is not comparable to BOOLEAN", false);

      // check null without cast
      f.checkNull("array_contains(array[1, 2], null)");
      f.checkFails("array_contains(^null^, array[1, 2])", "Illegal use of 'NULL'", false);
      f.checkFails("array_contains(^null^, null)", "Illegal use of 'NULL'", false);
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Tests {@code ARRAY_DISTINCT} function from Spark, Hive. */
  @Test void testArrayDistinctFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ARRAY_DISTINCT);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.HIVE, SqlLibrary.SPARK);
    f0.checkFails("^array_distinct(array['foo'])^",
        "No match found for function signature ARRAY_DISTINCT\\(<CHAR\\(3\\) ARRAY>\\)", false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("array_distinct(array[1, 2, 2, 1])", "[1, 2]",
          "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkScalar("array_distinct(array[null, 1, null])", "[null, 1]",
          "INTEGER ARRAY NOT NULL");
      f.checkNull("array_distinct(null)");
      // elements cast
      f.checkScalar("array_distinct(array[null, cast(1 as tinyint), 1, cast(2 as smallint)])",
          "[null, 1, 2]", "INTEGER ARRAY NOT NULL");
      f.checkScalar("array_distinct(array[null, cast(1 as tinyint), 1, cast(2 as bigint)])",
          "[null, 1, 2]", "BIGINT ARRAY NOT NULL");
      f.checkScalar("array_distinct(array[null, cast(1 as tinyint), 1, cast(2 as decimal)])",
          "[null, 1, 2]", "DECIMAL(19, 0) ARRAY NOT NULL");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testArrayJoinFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ARRAY_JOIN);
    f0.checkFails("^array_join(array['aa', 'b', 'c'], '-')^", "No match found for function"
        + " signature ARRAY_JOIN\\(<CHAR\\(2\\) ARRAY>, <CHARACTER>\\)", false);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.HIVE, SqlLibrary.SPARK);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("array_join(array['aa', 'b', 'c'], '-')", "aa-b -c ",
          "VARCHAR NOT NULL");
      f.checkScalar("array_join(array[null, 'aa', null, 'b', null], '-', 'empty')",
          "empty-aa-empty-b -empty", "VARCHAR NOT NULL");
      f.checkScalar("array_join(array[null, 'aa', null, 'b', null], '-')", "aa-b ",
          "VARCHAR NOT NULL");
      f.checkScalar("array_join(array[null, x'aa', null, x'bb', null], '-')", "aa-bb",
          "VARCHAR NOT NULL");
      f.checkScalar("array_join(array['', 'b'], '-')", " -b", "VARCHAR NOT NULL");
      f.checkScalar("array_join(array['', ''], '-')", "-", "VARCHAR NOT NULL");

      final SqlOperatorFixture f1 =
          f.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
      f1.checkScalar("array_join(array['aa', 'b', 'c'], '-')", "aa-b-c",
          "VARCHAR NOT NULL");
      f1.checkScalar("array_join(array[null, 'aa', null, 'b', null], '-', 'empty')",
          "empty-aa-empty-b-empty", "VARCHAR NOT NULL");
      f1.checkScalar("array_join(array[null, 'aa', null, 'b', null], '-')", "aa-b",
          "VARCHAR NOT NULL");
      f1.checkScalar("array_join(array[null, x'aa', null, x'bb', null], '-')", "aa-bb",
          "VARCHAR NOT NULL");
      f1.checkScalar("array_join(array['', 'b'], '-')", "-b", "VARCHAR NOT NULL");
      f1.checkScalar("array_join(array['', ''], '-')", "-", "VARCHAR NOT NULL");

      f.checkNull("array_join(null, '-')");
      f.checkNull("array_join(array['a', 'b', null], null)");
      f.checkFails("^array_join(array[1, 2, 3], '-', ' ')^",
          "Cannot apply 'ARRAY_JOIN' to arguments of type 'ARRAY_JOIN\\("
              + "<INTEGER ARRAY>, <CHAR\\(1\\)>, <CHAR\\(1\\)>\\)'\\. Supported form\\(s\\):"
              + " ARRAY_JOIN\\(<STRING ARRAY>, <CHARACTER>\\[, <CHARACTER>\\]\\)", false);
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Tests {@code ARRAY_MAX} function from Spark, Hive. */
  @Test void testArrayMaxFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ARRAY_MAX);
    f0.checkFails("^array_max(array[1, 2])^",
        "No match found for function signature ARRAY_MAX\\(<INTEGER ARRAY>\\)", false);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.HIVE, SqlLibrary.SPARK);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("array_max(array[1, 2])", "2", "INTEGER");
      f.checkScalar("array_max(array[1, 2, null])", "2", "INTEGER");
      f.checkScalar("array_max(array[1])", "1", "INTEGER");
      f.checkType("array_max(array())", "UNKNOWN");
      f.checkNull("array_max(array())");
      f.checkNull("array_max(cast(null as integer array))");
      // elements cast
      f.checkScalar("array_max(array[null, 1, cast(2 as tinyint)])", "2",
          "INTEGER");
      f.checkScalar("array_max(array[null, 1, cast(2 as bigint)])", "2",
          "BIGINT");
      f.checkScalar("array_max(array[null, 1, cast(2 as decimal)])", "2",
          "DECIMAL(19, 0)");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Tests {@code ARRAY_MIN} function from Spark, Hive. */
  @Test void testArrayMinFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ARRAY_MIN);
    f0.checkFails("^array_min(array[1, 2])^",
        "No match found for function signature ARRAY_MIN\\(<INTEGER ARRAY>\\)", false);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.HIVE, SqlLibrary.SPARK);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("array_min(array[1, 2])", "1", "INTEGER");
      f.checkScalar("array_min(array[1, 2, null])", "1", "INTEGER");
      f.checkType("array_min(array())", "UNKNOWN");
      f.checkNull("array_min(array())");
      f.checkNull("array_min(cast(null as integer array))");
      // elements cast
      f.checkScalar("array_min(array[null, 1, cast(2 as tinyint)])", "1",
          "INTEGER");
      f.checkScalar("array_min(array[null, 1, cast(2 as bigint)])", "1",
          "BIGINT");
      f.checkScalar("array_min(array[null, 1, cast(2 as decimal)])", "1",
          "DECIMAL(19, 0)");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6831">[CALCITE-6831]
   * Add ARRAR_SLICE function (enabled in Hive library)</a>. */
  @Test void testArraySlice() {
    SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ARRAY_SLICE);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.HIVE);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("array_slice(array[1,2,3], 1, 2)",
          "[2, 3]",
          "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkString("array_slice(array[1,null,3], 1, 2)",
          "[null, 3]",
          "INTEGER ARRAY NOT NULL");
      f.checkString("array_slice(array[1,2,3], 1, 10)",
          "[]",
          "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkString("array_slice(array['a','b','c','d'], 1, 3)",
          "[b, c, d]",
          "CHAR(1) NOT NULL ARRAY NOT NULL");
      f.checkString("array_slice(array[null,null,null], 1, 2)",
          "[null, null]",
          "NULL ARRAY NOT NULL");
      f.checkString("array_slice(array[1,2.2,3], 1, 2)",
          "[2.2, 3.0]",
          "DECIMAL(11, 1) NOT NULL ARRAY NOT NULL");
      f.checkString("array_slice(array[1,cast(2 as double),3], 1, 2)",
          "[2.0, 3.0]",
          "DOUBLE NOT NULL ARRAY NOT NULL");
      f.checkString("array_slice(array[1,2.2,3,null], 1, 3)",
          "[2.2, 3.0, null]",
                "DECIMAL(11, 1) ARRAY NOT NULL");
      f.checkFails("array_slice(^array[1,2.2,'c']^, 1, 2)",
                "Parameters must be of the same type", false);
      f.checkFails("array_slice(array[null,null,null], -1, 2)",
                "fromIndex = -1", true);
      f.checkNull("array_slice(array[1,2,3], null, 2)");
      f.checkNull("array_slice(array[1,2,3], null, null)");
      f.checkNull("array_slice(array[1,2,3], 1, null)");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Tests {@code ARRAY_POSITION} function from Spark. */
  @Test void testArrayPositionFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ARRAY_POSITION);
    f0.checkFails("^array_position(array[1], 1)^",
        "No match found for function signature ARRAY_POSITION\\("
            + "<INTEGER ARRAY>, <NUMERIC>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("array_position(array[1], 1)", "1",
        "BIGINT NOT NULL");
    f.checkScalar("array_position(array[1, 2, 2], 2)", "2",
        "BIGINT NOT NULL");
    f.checkScalar("array_position(array[1], 2)", "0",
        "BIGINT NOT NULL");
    f.checkScalar("array_position(array(), 1)", "0",
        "BIGINT NOT NULL");
    f.checkScalar("array_position(array[array[1, 2]], array[1, 2])", "1",
        "BIGINT NOT NULL");
    f.checkScalar("array_position(array[map[1, 'a']], map[1, 'a'])", "1",
        "BIGINT NOT NULL");
    f.checkNull("array_position(cast(null as integer array), 1)");
    f.checkType("array_position(cast(null as integer array), 1)", "BIGINT");
    f.checkNull("array_position(array[1], null)");
    f.checkType("array_position(array[1], null)", "BIGINT");
    f.checkFails("^array_position(array[1, 2], true)^",
        "INTEGER is not comparable to BOOLEAN", false);
  }

  /** Tests {@code ARRAY_PREPEND} function from Spark. */
  @Test void testArrayPrependFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ARRAY_PREPEND);
    f0.checkFails("^array_prepend(array[1], 2)^",
        "No match found for function signature ARRAY_PREPEND\\("
            + "<INTEGER ARRAY>, <NUMERIC>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("array_prepend(array[1], 2)", "[2, 1]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array[1], null)", "[null, 1]",
        "INTEGER ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(null), null)", "[null, null]",
        "NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(), null)", "[null]",
        "UNKNOWN ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(), 1)", "[1]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array[array[1, 2]], array[3, 4])", "[[3, 4], [1, 2]]",
        "INTEGER NOT NULL ARRAY NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array[map[1, 'a']], map[2, 'b'])", "[{2=b}, {1=a}]",
        "(INTEGER NOT NULL, CHAR(1) NOT NULL) MAP NOT NULL ARRAY NOT NULL");
    f.checkNull("array_prepend(cast(null as integer array), 1)");
    f.checkType("array_prepend(cast(null as integer array), 1)", "INTEGER ARRAY");
    f.checkFails("^array_prepend(array[1, 2], true)^",
        "INTEGER is not comparable to BOOLEAN", false);

    // element cast to the biggest type
    f.checkScalar("array_prepend(array(1), cast(3 as float))", "[3.0, 1.0]",
        "FLOAT NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(1), cast(3 as bigint))", "[3, 1]",
        "BIGINT NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(2), cast(3 as double))", "[3.0, 2.0]",
        "DOUBLE NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(1, 2), cast(3 as float))", "[3.0, 1.0, 2.0]",
        "FLOAT NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(2, 1), cast(3 as double))", "[3.0, 2.0, 1.0]",
        "DOUBLE NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(1, 2), cast(3 as tinyint))", "[3, 1, 2]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(1, 2), cast(3 as bigint))", "[3, 1, 2]",
        "BIGINT NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(1, 2), cast(null as double))", "[null, 1.0, 2.0]",
        "DOUBLE ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(1, 2), cast(null as float))", "[null, 1.0, 2.0]",
        "FLOAT ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(1), cast(null as bigint))", "[null, 1]",
        "BIGINT ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(1), cast(100 as decimal))", "[100, 1]",
        "DECIMAL(19, 0) NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(1), 10e6)", "[1.0E7, 1.0]",
        "DOUBLE NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(), cast(null as double))", "[null]",
        "DOUBLE ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(), cast(null as float))", "[null]",
        "FLOAT ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(), cast(null as tinyint))", "[null]",
        "TINYINT ARRAY NOT NULL");
    f.checkScalar("array_prepend(array(), cast(null as bigint))", "[null]",
        "BIGINT ARRAY NOT NULL");
  }

  /** Tests {@code ARRAY_REMOVE} function from Spark, Hive. */
  @Test void testArrayRemoveFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ARRAY_REMOVE);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.HIVE, SqlLibrary.SPARK);
    f0.checkFails("^array_remove(array[1], 1)^",
        "No match found for function signature ARRAY_REMOVE\\("
            + "<INTEGER ARRAY>, <NUMERIC>\\)", false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("array_remove(array[1], 1)", "[]",
          "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkScalar("array_remove(array[1, 2, 1], 1)", "[2]",
          "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkScalar("array_remove(array[1, 2, null], 1)", "[2, null]",
          "INTEGER ARRAY NOT NULL");
      f.checkScalar("array_remove(array[1, 2, null], 3)", "[1, 2, null]",
          "INTEGER ARRAY NOT NULL");
      f.checkScalar("array_remove(array(null), 1)", "[null]",
          "NULL ARRAY NOT NULL");
      f.checkScalar("array_remove(array(), 1)", "[]",
          "UNKNOWN NOT NULL ARRAY NOT NULL");
      f.checkScalar("array_remove(array[array[1, 2]], array[1, 2])", "[]",
          "INTEGER NOT NULL ARRAY NOT NULL ARRAY NOT NULL");
      f.checkScalar("array_remove(array[map[1, 'a']], map[1, 'a'])", "[]",
          "(INTEGER NOT NULL, CHAR(1) NOT NULL) MAP NOT NULL ARRAY NOT NULL");
      f.checkNull("array_remove(cast(null as integer array), 1)");
      f.checkType("array_remove(cast(null as integer array), 1)", "INTEGER ARRAY");

      // Flink and Spark differ on the following. The expression
      //   array_remove(array[1, null], cast(null as integer))
      // returns [1] in Flink, and returns null in Spark. The current
      // function has Spark behavior, but if we supported a Flink function
      // library (i.e. "fun=flink") we could add a function with Flink behavior.
      f.checkNull("array_remove(array[1, null], cast(null as integer))");
      f.checkType("array_remove(array[1, null], cast(null as integer))", "INTEGER ARRAY");
      f.checkFails("^array_remove(array[1, 2], true)^",
          "INTEGER is not comparable to BOOLEAN", false);
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Tests {@code ARRAY_REPEAT} function from Spark. */
  @Test void testArrayRepeatFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ARRAY_REPEAT);
    f0.checkFails("^array_repeat(1, 2)^",
        "No match found for function signature ARRAY_REPEAT\\(<NUMERIC>, <NUMERIC>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("array_repeat('1', 2)", "[1, 1]",
        "CHAR(1) NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_repeat(1, 2)", "[1, 1]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_repeat(1, -2)", "[]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_repeat(array[1, 2], 2)", "[[1, 2], [1, 2]]",
        "INTEGER NOT NULL ARRAY NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_repeat(map[1, 'a', 2, 'b'], 2)", "[{1=a, 2=b}, {1=a, 2=b}]",
        "(INTEGER NOT NULL, CHAR(1) NOT NULL) MAP NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_repeat(cast(null as integer), 2)", "[null, null]",
        "INTEGER ARRAY");
    // elements cast
    f.checkScalar("array_repeat(cast(1 as tinyint), 2)", "[1, 1]",
        "TINYINT NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_repeat(cast(1 as bigint), 2)", "[1, 1]",
        "BIGINT NOT NULL ARRAY NOT NULL");
    f.checkScalar("array_repeat(cast(1 as decimal), 2)", "[1, 1]",
        "DECIMAL(19, 0) NOT NULL ARRAY NOT NULL");
    f.checkNull("array_repeat(1, null)");
  }

  /** Tests {@code ARRAY_REVERSE} function from BigQuery. */
  @Test void testArrayReverseFunc() {
    final SqlFunction func = SqlLibraryOperators.ARRAY_REVERSE;
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(func);
    f0.checkFails("^array_reverse(array[1])^",
        "No match found for function signature ARRAY_REVERSE\\(<INTEGER ARRAY>\\)", false);
    checkArrayReverseFunc(f0, func, list(SqlLibrary.BIG_QUERY));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6445">[CALCITE-6445]
   * Add REVERSE function (enabled in Spark library)</a>. */
  @Test void testReverseSparkFunc() {
    final SqlFunction func = SqlLibraryOperators.REVERSE_SPARK;
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(func);
    Iterable<SqlLibrary> libraries = list(SqlLibrary.SPARK);
    checkArrayReverseFunc(f0, func, libraries);
    checReverseFunc(f0, func, libraries);
  }

  void checkArrayReverseFunc(SqlOperatorFixture f0, SqlFunction function,
      Iterable<? extends SqlLibrary> libraries) {
    final String fn = function.getName();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar(fn + "(array[1])", "[1]",
          "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkScalar(fn + "(array[1, 2])", "[2, 1]",
          "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkScalar(fn + "(array[null, 1])", "[1, null]",
          "INTEGER ARRAY NOT NULL");
      // elements cast
      f.checkScalar(fn + "(array[cast(1 as tinyint), 2])", "[2, 1]",
          "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkScalar(fn + "(array[null, 1, cast(2 as tinyint)])", "[2, 1, null]",
          "INTEGER ARRAY NOT NULL");
      f.checkScalar(fn + "(array[cast(1 as bigint), 2])", "[2, 1]",
          "BIGINT NOT NULL ARRAY NOT NULL");
      f.checkScalar(fn + "(array[null, 1, cast(2 as bigint)])", "[2, 1, null]",
          "BIGINT ARRAY NOT NULL");
      f.checkScalar(fn + "(array[cast(1 as decimal), 2])", "[2, 1]",
          "DECIMAL(19, 0) NOT NULL ARRAY NOT NULL");
      f.checkScalar(fn + "(array[null, 1, cast(2 as decimal)])", "[2, 1, null]",
          "DECIMAL(19, 0) ARRAY NOT NULL");
      f.checkScalar(fn + "(array[CAST(2.1 as decimal(17)), 2.1111111111111119])",
          "[2.11, 2.00]", "DECIMAL(19, 2) NOT NULL ARRAY NOT NULL");
      f.checkScalar(fn + "(array[CAST(2.1 as double), 2.1111111111111119])",
          "[2.111111111111112, 2.1]", "DOUBLE NOT NULL ARRAY NOT NULL");
      f.checkScalar(fn + "(array[null])", "[null]", "NULL ARRAY NOT NULL");
      f.checkNull(fn + "(cast(null as integer array))");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Tests {@code ARRAY_SIZE} function from Spark. */
  @Test void testArraySizeFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ARRAY_SIZE);
    f0.checkFails("^array_size(array[1])^",
        "No match found for function signature ARRAY_SIZE\\(<INTEGER ARRAY>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("array_size(array[1])", "1",
        "INTEGER NOT NULL");
    f.checkScalar("array_size(array[1, 2, null])", "3",
        "INTEGER NOT NULL");
    f.checkNull("array_size(null)");
    // elements cast
    f.checkScalar("array_size(array[cast(1 as tinyint), 2])", "2",
        "INTEGER NOT NULL");
    f.checkScalar("array_size(array[null, 1, cast(2 as tinyint)])", "3",
        "INTEGER NOT NULL");
    f.checkScalar("array_size(array[cast(1 as bigint), 2])", "2",
        "INTEGER NOT NULL");
  }

  /** Tests {@code ARRAY_LENGTH} function from BigQuery. */
  @Test void testArrayLengthFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ARRAY_LENGTH);
    f0.checkFails("^array_length(array[1])^",
        "No match found for function signature ARRAY_LENGTH\\(<INTEGER ARRAY>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("array_length(array[1])", "1",
        "INTEGER NOT NULL");
    f.checkScalar("array_length(array[1, 2, null])", "3",
        "INTEGER NOT NULL");
    f.checkNull("array_length(null)");
    // elements cast
    f.checkScalar("array_length(array[cast(1 as tinyint), 2])", "2",
        "INTEGER NOT NULL");
    f.checkScalar("array_length(array[null, 1, cast(2 as tinyint)])", "3",
        "INTEGER NOT NULL");
    f.checkScalar("array_length(array[cast(1 as bigint), 2])", "2",
        "INTEGER NOT NULL");
  }

  @Test void testArrayToStringFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ARRAY_TO_STRING);
    f0.checkFails("^array_to_string(array['aa', 'b', 'c'], '-')^", "No match found for function"
        + " signature ARRAY_TO_STRING\\(<CHAR\\(2\\) ARRAY>, <CHARACTER>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("array_to_string(array['aa', 'b', 'c'], '-')", "aa-b -c ",
        "VARCHAR NOT NULL");
    f.checkScalar("array_to_string(array[null, 'aa', null, 'b', null], '-', 'empty')",
        "empty-aa-empty-b -empty", "VARCHAR NOT NULL");
    f.checkScalar("array_to_string(array[null, 'aa', null, 'b', null], '-')", "aa-b ",
        "VARCHAR NOT NULL");
    f.checkScalar("array_to_string(array[null, x'aa', null, x'bb', null], '-')", "aa-bb",
        "VARCHAR NOT NULL");
    f.checkScalar("array_to_string(array['', 'b'], '-')", " -b", "VARCHAR NOT NULL");
    f.checkScalar("array_to_string(array['', ''], '-')", "-", "VARCHAR NOT NULL");
    f.checkNull("array_to_string(null, '-')");
    f.checkNull("array_to_string(array['a', 'b', null], null)");
    f.checkNull("array_to_string(array['1','2','3'], ',', null)");
    f.checkFails("^array_to_string(array[1, 2, 3], '-', ' ')^",
        "Cannot apply 'ARRAY_TO_STRING' to arguments of type 'ARRAY_TO_STRING"
            + "\\(<INTEGER ARRAY>, <CHAR\\(1\\)>, <CHAR\\(1\\)>\\)'\\. Supported form\\(s\\):"
            + " ARRAY_TO_STRING\\(<STRING ARRAY>, <CHARACTER>\\[, <CHARACTER>\\]\\)", false);

    final SqlOperatorFixture f1 =
        f.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    f1.checkScalar("array_to_string(array['aa', 'b', 'c'], '-')", "aa-b-c",
        "VARCHAR NOT NULL");
    f1.checkScalar("array_to_string(array[null, 'aa', null, 'b', null], '-', 'empty')",
        "empty-aa-empty-b-empty", "VARCHAR NOT NULL");
    f1.checkScalar("array_to_string(array[null, 'aa', null, 'b', null], '-')", "aa-b",
        "VARCHAR NOT NULL");
    f1.checkScalar("array_to_string(array[null, x'aa', null, x'bb', null], '-')", "aa-bb",
        "VARCHAR NOT NULL");
    f1.checkScalar("array_to_string(array['', 'b'], '-')", "-b", "VARCHAR NOT NULL");
    f1.checkScalar("array_to_string(array['', ''], '-')", "-", "VARCHAR NOT NULL");
  }

  /** Tests {@code ARRAY_EXCEPT} function from Spark, Hive. */
  @Test void testArrayExceptFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ARRAY_EXCEPT);
    f0.checkFails("^array_except(array[2, null, 3, 3], array[1, 2, null])^",
        "No match found for function signature "
            + "ARRAY_EXCEPT\\(<INTEGER ARRAY>, <INTEGER ARRAY>\\)", false);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.HIVE, SqlLibrary.SPARK);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("array_except(array[2, 3, 3], array[2])",
          "[3]", "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkScalar("array_except(array[2], array[2, 3])",
          "[]", "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkScalar("array_except(array[2, null, 3, 3], array[1, 2, null])",
          "[3]", "INTEGER ARRAY NOT NULL");
      f.checkNull("array_except(cast(null as integer array), array[1])");
      f.checkNull("array_except(array[1], cast(null as integer array))");
      f.checkNull("array_except(cast(null as integer array), cast(null as integer array))");

      // check null without cast
      f.checkFails("^array_except(array[1, 2], null)^",
          "Cannot apply 'ARRAY_EXCEPT' to arguments of type 'ARRAY_EXCEPT\\(<INTEGER ARRAY>, "
              + "<NULL>\\)'\\. Supported form\\(s\\): 'ARRAY_EXCEPT\\(<EQUIVALENT_TYPE>, "
              + "<EQUIVALENT_TYPE>\\)'", false);
      f.checkFails("^array_except(null, array[1, 2])^",
          "Cannot apply 'ARRAY_EXCEPT' to arguments of type 'ARRAY_EXCEPT\\(<NULL>, "
              + "<INTEGER ARRAY>\\)'\\. Supported form\\(s\\): 'ARRAY_EXCEPT\\(<EQUIVALENT_TYPE>, "
              + "<EQUIVALENT_TYPE>\\)'", false);
      f.checkFails("^array_except(null, null)^",
          "Cannot apply 'ARRAY_EXCEPT' to arguments of type 'ARRAY_EXCEPT\\(<NULL>, "
              + "<NULL>\\)'\\. Supported form\\(s\\): 'ARRAY_EXCEPT\\(<EQUIVALENT_TYPE>, "
              + "<EQUIVALENT_TYPE>\\)'", false);
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Tests {@code ARRAY_INSERT} function from Spark. */
  @Test void testArrayInsertFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ARRAY_INSERT);
    f0.checkFails("^array_insert(null, 3, 4)^",
        "No match found for function signature "
            + "ARRAY_INSERT\\(<NULL>, <NUMERIC>, <NUMERIC>\\)", false);
    f0.checkFails("^array_insert(array[1], null, 4)^",
        "No match found for function signature "
            + "ARRAY_INSERT\\(<INTEGER ARRAY>, <NULL>, <NUMERIC>\\)", false);
    f0.checkFails("^array_insert(array[1], 3, null)^",
        "No match found for function signature "
            + "ARRAY_INSERT\\(<INTEGER ARRAY>, <NUMERIC>, <NULL>\\)", false);

    final SqlOperatorFixture f1 = f0.withLibrary(SqlLibrary.SPARK);

    // can't be NULL
    f1.checkFails("array_insert(^null^, 3, 4)",
        "Argument to function 'ARRAY_INSERT' must not be NULL", false);
    f1.checkFails("array_insert(array[1], ^null^, 4)",
        "Argument to function 'ARRAY_INSERT' must not be NULL", false);
    f1.checkFails("array_insert(array[1], 3, ^null^)",
        "Argument to function 'ARRAY_INSERT' must not be NULL", false);

    // return null
    f1.checkNull("array_insert(cast(null as integer array), 3, 4)");
    f1.checkNull("array_insert(array[1], cast(null as integer), 4)");

    // op1 must be Integer type
    f1.checkFails("^array_insert(array[1, 2, 3], cast(3 as tinyint), 4)^",
        "TINYINT is not comparable to INTEGER", false);
    f1.checkFails("^array_insert(array[1, 2, 3], cast(3 as smallint), 4)^",
        "SMALLINT is not comparable to INTEGER", false);
    f1.checkFails("^array_insert(array[1, 2, 3], cast(3 as bigint), 4)^",
        "BIGINT is not comparable to INTEGER", false);
    f1.checkFails("^array_insert(array[1, 2, 3], 3.0, 4)^",
        "DECIMAL is not comparable to INTEGER", false);
    f1.checkFails("^array_insert(array[1, 2, 3], '3', 4)^",
        "CHAR is not comparable to INTEGER", false);
    // op1 can't be 0
    f1.checkFails("array_insert(array[2, 3, 4], 0, 1)",
        "The index 0 is invalid. "
            + "An index shall be either < 0 or > 0 \\(the first element has index 1\\) "
            + "and not exceeds the allowed limit.", true);
    // op1 overflow
    f1.checkFails("array_insert(array[2, 3, 4], 2147483647, 1)",
        "The index 0 is invalid. "
            + "An index shall be either < 0 or > 0 \\(the first element has index 1\\) "
            + "and not exceeds the allowed limit.", true);
    f1.checkFails("array_insert(array[2, 3, 4], -2147483648, 1)",
        "The index 0 is invalid. "
            + "An index shall be either < 0 or > 0 \\(the first element has index 1\\) "
            + "and not exceeds the allowed limit.", true);

    f1.checkScalar("array_insert(array[1, 2, 3], 3, 4)",
        "[1, 2, 4, 3]", "INTEGER ARRAY NOT NULL");
    f1.checkScalar("array_insert(array[1, 2, 3], 3, cast(null as integer))",
        "[1, 2, null, 3]", "INTEGER ARRAY NOT NULL");
    f1.checkScalar("array_insert(array[2, 3, 4], 1, 1)",
        "[1, 2, 3, 4]", "INTEGER ARRAY NOT NULL");
    f1.checkScalar("array_insert(array[1, 3, 4], -1, 2)",
        "[1, 3, 4, 2]", "INTEGER ARRAY NOT NULL");
    f1.checkScalar("array_insert(array[1, 3, 4], -3, 2)",
        "[1, 2, 3, 4]", "INTEGER ARRAY NOT NULL");
    f1.checkScalar("array_insert(array[2, 3, null, 4], -6, 1)",
        "[1, null, 2, 3, null, 4]", "INTEGER ARRAY NOT NULL");
    // check complex type
    f1.checkScalar("array_insert(array[array[1,2]], 1, array[1])",
        "[[1], [1, 2]]", "INTEGER NOT NULL ARRAY ARRAY NOT NULL");
    f1.checkScalar("array_insert(array[array[1,2]], -1, array[1])",
        "[[1, 2], [1]]", "INTEGER NOT NULL ARRAY ARRAY NOT NULL");
    f1.checkScalar("array_insert(array[map[1, 'a']], 1, map[2, 'b'])", "[{2=b}, {1=a}]",
        "(INTEGER NOT NULL, CHAR(1) NOT NULL) MAP ARRAY NOT NULL");
    f1.checkScalar("array_insert(array[map[1, 'a']], -1, map[2, 'b'])", "[{1=a}, {2=b}]",
        "(INTEGER NOT NULL, CHAR(1) NOT NULL) MAP ARRAY NOT NULL");

    // element cast to the biggest type
    f1.checkScalar("array_insert(array(1, 2, 3), 3, cast(4 as tinyint))",
        "[1, 2, 4, 3]", "INTEGER ARRAY NOT NULL");
    f1.checkScalar("array_insert(array(1, 2, 3), 3, cast(4 as double))",
        "[1.0, 2.0, 4.0, 3.0]", "DOUBLE ARRAY NOT NULL");
    f1.checkScalar("array_insert(array(1, 2, 3), 3, cast(4 as float))",
        "[1.0, 2.0, 4.0, 3.0]", "FLOAT ARRAY NOT NULL");
    f1.checkScalar("array_insert(array(1, 2, 3), 3, cast(4 as bigint))",
        "[1, 2, 4, 3]", "BIGINT ARRAY NOT NULL");
    f1.checkScalar("array_insert(array(1, 2, 3), 3, cast(null as bigint))",
        "[1, 2, null, 3]", "BIGINT ARRAY NOT NULL");
    f1.checkScalar("array_insert(array(1, 2, 3), 3, cast(null as float))",
        "[1.0, 2.0, null, 3.0]", "FLOAT ARRAY NOT NULL");
    f1.checkScalar("array_insert(array(1, 2, 3), 3, cast(null as tinyint))",
        "[1, 2, null, 3]", "INTEGER ARRAY NOT NULL");
  }

  /** Tests {@code ARRAY_INTERSECT} function from Spark, Hive. */
  @Test void testArrayIntersectFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ARRAY_INTERSECT);
    f0.checkFails("^array_intersect(array[2, null, 2], array[1, 2, null])^",
        "No match found for function signature "
            + "ARRAY_INTERSECT\\(<INTEGER ARRAY>, <INTEGER ARRAY>\\)", false);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.HIVE, SqlLibrary.SPARK);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("array_intersect(array[2, 3, 3], array[3])",
          "[3]", "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkScalar("array_intersect(array[1], array[2, 3])",
          "[]", "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkScalar("array_intersect(array[2, null, 2], array[1, 2, null])",
          "[2, null]", "INTEGER ARRAY NOT NULL");
      f.checkNull("array_intersect(cast(null as integer array), array[1])");
      f.checkNull("array_intersect(array[1], cast(null as integer array))");
      f.checkNull("array_intersect(cast(null as integer array), cast(null as integer array))");

      // check null without cast
      f.checkFails("^array_intersect(array[1, 2], null)^",
          "Cannot apply 'ARRAY_INTERSECT' to arguments of type 'ARRAY_INTERSECT\\(<INTEGER ARRAY>, "
              + "<NULL>\\)'\\. Supported form\\(s\\): 'ARRAY_INTERSECT\\(<EQUIVALENT_TYPE>, "
              + "<EQUIVALENT_TYPE>\\)'", false);
      f.checkFails("^array_intersect(null, array[1, 2])^",
          "Cannot apply 'ARRAY_INTERSECT' to arguments of type 'ARRAY_INTERSECT\\(<NULL>, "
              + "<INTEGER ARRAY>\\)'\\. Supported form\\(s\\): 'ARRAY_INTERSECT\\(<EQUIVALENT_TYPE>, "
              + "<EQUIVALENT_TYPE>\\)'", false);
      f.checkFails("^array_intersect(null, null)^",
          "Cannot apply 'ARRAY_INTERSECT' to arguments of type 'ARRAY_INTERSECT\\(<NULL>, "
              + "<NULL>\\)'\\. Supported form\\(s\\): 'ARRAY_INTERSECT\\(<EQUIVALENT_TYPE>, "
              + "<EQUIVALENT_TYPE>\\)'", false);
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Tests {@code ARRAY_UNION} function from Spark, Hive. */
  @Test void testArrayUnionFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ARRAY_UNION);
    f0.checkFails("^array_union(array[2, null, 2], array[1, 2, null])^",
        "No match found for function signature "
            + "ARRAY_UNION\\(<INTEGER ARRAY>, <INTEGER ARRAY>\\)", false);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.HIVE, SqlLibrary.SPARK);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("array_intersect(array[2, 3, 3], array[3])",
          "[3]", "INTEGER NOT NULL ARRAY NOT NULL");
      f.checkScalar("array_union(array[2, null, 2], array[1, 2, null])",
          "[2, null, 1]", "INTEGER ARRAY NOT NULL");
      f.checkNull("array_union(cast(null as integer array), array[1])");
      f.checkNull("array_union(array[1], cast(null as integer array))");
      f.checkNull("array_union(cast(null as integer array), cast(null as integer array))");
    };
    f0.forEachLibrary(libraries, consumer);
  }

  /** Tests {@code ARRAYS_OVERLAP} function from Spark. */
  @Test void testArraysOverlapFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ARRAYS_OVERLAP);
    f0.checkFails("^arrays_overlap(array[1, 2], array[2])^",
        "No match found for function signature ARRAYS_OVERLAP\\("
            + "<INTEGER ARRAY>, <INTEGER ARRAY>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("arrays_overlap(array[1, 2], array[2])", true,
        "BOOLEAN NOT NULL");
    f.checkScalar("arrays_overlap(array[1, 2], array[3])", false,
        "BOOLEAN NOT NULL");
    f.checkScalar("arrays_overlap(array[1, null], array[1])", true,
        "BOOLEAN");
    f.checkScalar("arrays_overlap(array(), array(2))", false,
        "BOOLEAN NOT NULL");
    f.checkScalar("arrays_overlap(array(), array())", false,
        "BOOLEAN NOT NULL");
    f.checkScalar("arrays_overlap(array(), array(1, null))", false,
        "BOOLEAN");
    f.checkScalar("arrays_overlap(array[array[1, 2], array[3, 4]], array[array[1, 2]])", true,
        "BOOLEAN NOT NULL");
    f.checkScalar("arrays_overlap(array[map[1, 'a'], map[2, 'b']], array[map[1, 'a']])", true,
        "BOOLEAN NOT NULL");
    f.checkNull("arrays_overlap(cast(null as integer array), array[1, 2])");
    f.checkType("arrays_overlap(cast(null as integer array), array[1, 2])", "BOOLEAN");
    f.checkNull("arrays_overlap(array[1, 2], cast(null as integer array))");
    f.checkType("arrays_overlap(array[1, 2], cast(null as integer array))", "BOOLEAN");
    f.checkNull("arrays_overlap(array[1], array[2, null])");
    f.checkType("arrays_overlap(array[2, null], array[1])", "BOOLEAN");
    final String expected = "Cannot apply 'ARRAYS_OVERLAP' to arguments of type 'ARRAYS_OVERLAP\\("
        + "<.*>, <.*>\\)'. Supported form\\(s\\): 'ARRAYS_OVERLAP\\("
        + "<EQUIVALENT_TYPE>, <EQUIVALENT_TYPE>\\)'";
    f.checkFails("^arrays_overlap(array[1, 2], true)^", expected, false);
    f.checkFails("^arrays_overlap(null, null)^", expected, false);
    f.checkFails("^arrays_overlap(null, array[1])^", expected, false);
    f.checkFails("^arrays_overlap(array[1], null)^", expected, false);
  }

  /** Tests {@code ARRAYS_ZIP} function from Spark. */
  @Test void testArraysZipFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ARRAYS_ZIP);
    f0.checkFails("^arrays_zip(array[1, 2], array[2])^",
        "No match found for function signature ARRAYS_ZIP\\("
            + "<INTEGER ARRAY>, <INTEGER ARRAY>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("arrays_zip(array[1, 2], array[2, 3], array[3, 4])", "[{1, 2, 3}, {2, 3, 4}]",
        "RecordType(INTEGER NOT NULL 0, INTEGER NOT NULL 1, INTEGER NOT NULL 2) "
            + "NOT NULL ARRAY NOT NULL");
    f.checkScalar("arrays_zip(array[1, 2], array[2])", "[{1, 2}, {2, null}]",
        "RecordType(INTEGER NOT NULL 0, INTEGER NOT NULL 1) NOT NULL ARRAY NOT NULL");
    f.checkScalar("arrays_zip(array[1], array[2, null])", "[{1, 2}, {null, null}]",
        "RecordType(INTEGER NOT NULL 0, INTEGER 1) NOT NULL ARRAY NOT NULL");
    f.checkScalar("arrays_zip(array[1, 2])", "[{1}, {2}]",
        "RecordType(INTEGER NOT NULL 0) NOT NULL ARRAY NOT NULL");
    f.checkScalar("arrays_zip(array(), array(1, 2))", "[{null, 1}, {null, 2}]",
        "RecordType(UNKNOWN NOT NULL 0, INTEGER NOT NULL 1) NOT NULL ARRAY NOT NULL");
    f.checkScalar("arrays_zip(array(), array())", "[]",
        "RecordType(UNKNOWN NOT NULL 0, UNKNOWN NOT NULL 1) NOT NULL ARRAY NOT NULL");
    f.checkScalar("arrays_zip(array())", "[]",
        "RecordType(UNKNOWN NOT NULL 0) NOT NULL ARRAY NOT NULL");
    f.checkScalar("arrays_zip()", "[]",
        "RecordType() NOT NULL ARRAY NOT NULL");
    f.checkScalar("arrays_zip(array(null), array(1))", "[{null, 1}]",
        "RecordType(NULL 0, INTEGER NOT NULL 1) NOT NULL ARRAY NOT NULL");
    f.checkScalar("arrays_zip(array[array[1, 2], array[3, 4]], array[array[1, 2]])",
        "[{[1, 2], [1, 2]}, {[3, 4], null}]",
        "RecordType(INTEGER NOT NULL ARRAY NOT NULL 0, INTEGER NOT NULL ARRAY NOT NULL 1) "
            + "NOT NULL ARRAY NOT NULL");
    f.checkScalar("arrays_zip(array[map[1, 'a'], map[2, 'b']], array[map[1, 'a']])",
        "[{{1=a}, {1=a}}, {{2=b}, null}]",
        "RecordType((INTEGER NOT NULL, CHAR(1) NOT NULL) MAP NOT NULL 0, "
            + "(INTEGER NOT NULL, CHAR(1) NOT NULL) MAP NOT NULL 1) NOT NULL ARRAY NOT NULL");

    f.checkNull("arrays_zip(cast(null as integer array), array[1, 2])");
    f.checkType("arrays_zip(cast(null as integer array), array[1, 2])",
        "RecordType(INTEGER 0, INTEGER NOT NULL 1) NOT NULL ARRAY");
    f.checkNull("arrays_zip(array[1, 2], cast(null as integer array))");
    f.checkType("arrays_zip(array[1, 2], cast(null as integer array))",
        "RecordType(INTEGER NOT NULL 0, INTEGER 1) NOT NULL ARRAY");
    f.checkFails("^arrays_zip(array[1, 2], true)^",
        "Parameters must be of the same type", false);
  }

  /** Tests {@code SORT_ARRAY} function from Spark. */
  @Test void testSortArrayFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.SORT_ARRAY);
    f0.checkFails("^sort_array(array[null, 1, null, 2])^",
        "No match found for function signature SORT_ARRAY\\(<INTEGER ARRAY>\\)", false);
    f0.checkFails("^sort_array(array[null, 1, null, 2], true)^",
        "No match found for function signature SORT_ARRAY\\(<INTEGER ARRAY>, <BOOLEAN>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("sort_array(array[2, null, 1])", "[null, 1, 2]",
        "INTEGER ARRAY NOT NULL");
    f.checkScalar("sort_array(array(2, null, 1), false)", "[2, 1, null]",
        "INTEGER ARRAY NOT NULL");
    f.checkScalar("sort_array(array[true, false, null])", "[null, false, true]",
        "BOOLEAN ARRAY NOT NULL");
    f.checkScalar("sort_array(array[true, false, null], false)", "[true, false, null]",
        "BOOLEAN ARRAY NOT NULL");
    f.checkScalar("sort_array(array[null])", "[null]",
        "NULL ARRAY NOT NULL");
    f.checkScalar("sort_array(array())", "[]",
        "UNKNOWN NOT NULL ARRAY NOT NULL");
    f.checkNull("sort_array(null)");

    // elements cast
    f.checkScalar("sort_array(array[cast(1 as tinyint), 2])", "[1, 2]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("sort_array(array[null, 1, cast(2 as tinyint)])", "[null, 1, 2]",
        "INTEGER ARRAY NOT NULL");
    f.checkScalar("sort_array(array[cast(1 as bigint), 2])", "[1, 2]",
        "BIGINT NOT NULL ARRAY NOT NULL");
    f.checkScalar("sort_array(array[cast(1 as decimal), 2])", "[1, 2]",
        "DECIMAL(19, 0) NOT NULL ARRAY NOT NULL");

    f.checkFails("^sort_array(array[2, null, 1], cast(1 as boolean))^",
        "Argument to function 'SORT_ARRAY' must be a literal", false);
    f.checkFails("^sort_array(array[2, null, 1], 1)^",
        "Cannot apply 'SORT_ARRAY' to arguments of type "
            + "'SORT_ARRAY\\(<INTEGER ARRAY>, <INTEGER>\\)'\\."
            + " Supported form\\(s\\): 'SORT_ARRAY\\(<ARRAY>\\)'\n"
            + "'SORT_ARRAY\\(<ARRAY>, <BOOLEAN>\\)'", false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6116">[CALCITE-6116]
   * Add EXISTS function (enabled in Spark library)</a>. */
  @Test void testExistsFunc() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.EXISTS)
        .withLibrary(SqlLibrary.SPARK);
    // wrong return type in function
    f.withValidatorConfig(t -> t.withTypeCoercionEnabled(false))
        .checkFails("^\"EXISTS\"(array[1, 2, 3], x -> x + 1)^",
            "Cannot apply 'EXISTS' to arguments of type "
                + "'EXISTS\\(<INTEGER ARRAY>, <FUNCTION\\(INTEGER\\) -> INTEGER>\\)'. "
                + "Supported form\\(s\\): "
                + "EXISTS\\(<ARRAY>, <FUNCTION\\(ARRAY_ELEMENT_TYPE\\)->BOOLEAN>\\)",
            false);

    // bad number of arguments
    f.checkFails("^\"EXISTS\"(array[1, 2, 3])^",
        "Invalid number of arguments to function 'EXISTS'\\. Was expecting 2 arguments",
        false);
    f.checkFails("^\"EXISTS\"(array[1, 2, 3], x -> x > 2, x -> x > 2)^",
        "Invalid number of arguments to function 'EXISTS'\\. Was expecting 2 arguments",
        false);

    // function should not be null
    f.checkFails("^\"EXISTS\"(array[1, 2, 3], null)^",
        "Cannot apply 'EXISTS' to arguments of type 'EXISTS\\(<INTEGER ARRAY>, <NULL>\\)'. "
            + "Supported form\\(s\\): "
            + "EXISTS\\(<ARRAY>, <FUNCTION\\(ARRAY_ELEMENT_TYPE\\)->BOOLEAN>\\)",
        false);

    // bad type
    f.checkFails("^\"EXISTS\"(1, x -> x > 2)^",
        "Cannot apply 'EXISTS' to arguments of type 'EXISTS\\(<INTEGER>, "
            + "<FUNCTION\\(ANY\\) -> BOOLEAN>\\)'. "
            + "Supported form\\(s\\): "
            + "EXISTS\\(<ARRAY>, <FUNCTION\\(ARRAY_ELEMENT_TYPE\\)->BOOLEAN>\\)",
        false);
    f.checkFails("^\"EXISTS\"(array[1, 2, 3], 1)^",
        "Cannot apply 'EXISTS' to arguments of type 'EXISTS\\(<INTEGER ARRAY>, <INTEGER>\\)'. "
            + "Supported form\\(s\\): "
            + "EXISTS\\(<ARRAY>, <FUNCTION\\(ARRAY_ELEMENT_TYPE\\)->BOOLEAN>\\)",
        false);

    // simple expression
    f.checkScalar("\"EXISTS\"(array[1, 2, 3], x -> x > 2)", true, "BOOLEAN");
    f.checkScalar("\"EXISTS\"(array[1, 2, 3], x -> x > 3)", false, "BOOLEAN");
    f.checkScalar("\"EXISTS\"(array[1, 2, 3], x -> false)", false, "BOOLEAN");
    f.checkScalar("\"EXISTS\"(array[1, 2, 3], x -> true)", true, "BOOLEAN");

    // empty array
    f.checkScalar("\"EXISTS\"(array(), x -> true)", false, "BOOLEAN");
    f.checkScalar("\"EXISTS\"(array(), x -> false)", false, "BOOLEAN");
    f.checkScalar("\"EXISTS\"(array(), x -> cast(x as int) = 1)", false, "BOOLEAN");

    // complex expression
    f.checkScalar("\"EXISTS\"(array[-1, 2, 3], y -> abs(y) = 1)", true, "BOOLEAN");
    f.checkScalar("\"EXISTS\"(array[-1, 2, 3], y -> abs(y) = 4)", false, "BOOLEAN");
    f.checkScalar("\"EXISTS\"(array[1, 2, 3], x -> x > 2 and x < 4)", true, "BOOLEAN");

    // complex array
    f.checkScalar("\"EXISTS\"(array[array[1, 2], array[3, 4]], x -> x[1] = 1)", true, "BOOLEAN");
    f.checkScalar("\"EXISTS\"(array[array[1, 2], array[3, 4]], x -> x[1] = 5)", false, "BOOLEAN");

    // test for null
    f.checkScalar("\"EXISTS\"(array[null, 3], x -> x > 2 or x < 4)", true, "BOOLEAN");
    f.checkScalar("\"EXISTS\"(array[null, 3], x -> x is null)", true, "BOOLEAN");
    f.checkNull("\"EXISTS\"(array[null, 3], x -> cast(null as boolean))");
    f.checkNull("\"EXISTS\"(array[null, 3], x -> x = null)");
    f.checkNull("\"EXISTS\"(cast(null as integer array), x -> x > 2)");
  }

  /** Tests {@code MAP_CONCAT} function from Spark. */
  @Test void testMapConcatFunc() {
    // 1. check with std map constructor, map[k, v ...]
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.MAP_CONCAT);
    f0.checkFails("^map_concat(map['foo', 1], map['bar', 2])^",
        "No match found for function signature MAP_CONCAT\\("
            + "<\\(CHAR\\(3\\), INTEGER\\) MAP>, <\\(CHAR\\(3\\), INTEGER\\) MAP>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("map_concat(map['foo', 1], map['bar', 2])", "{foo=1, bar=2}",
        "(CHAR(3) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f.checkScalar("map_concat(map['foo', 1], map['bar', 2], map['foo', 2])", "{foo=2, bar=2}",
        "(CHAR(3) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f.checkScalar("map_concat(map[null, 1], map[null, 2])", "{null=2}",
        "(NULL, INTEGER NOT NULL) MAP NOT NULL");
    f.checkScalar("map_concat(map[1, 2], map[1, null])", "{1=null}",
        "(INTEGER NOT NULL, INTEGER) MAP NOT NULL");
    // test zero arg, but it should return empty map.
    f.checkScalar("map_concat()", "{}",
        "(VARCHAR NOT NULL, VARCHAR NOT NULL) MAP");

    // after calcite supports cast(null as map<string, int>), it should add these tests.
    if (TODO) {
      f.checkNull("map_concat(map['foo', 1], cast(null as map<string, int>))");
      f.checkType("map_concat(map['foo', 1], cast(null as map<string, int>))",
          "(VARCHAR NOT NULL, INTEGER NOT NULL) MAP");
      f.checkNull("map_concat(cast(null as map<string, int>), map['foo', 1])");
      f.checkType("map_concat(cast(null as map<string, int>), map['foo', 1])",
          "(VARCHAR NOT NULL, INTEGER NOT NULL) MAP");
    }

    // test only has one operand, but it is not map type.
    f.checkFails("^map_concat(1)^",
        "Function 'MAP_CONCAT' should all be of type map, but it is 'INTEGER NOT NULL'", false);
    f.checkFails("^map_concat(null)^",
        "Function 'MAP_CONCAT' should all be of type map, but it is 'NULL'", false);
    // test operands in same type family, but it is not map type.
    f.checkFails("^map_concat(array[1], array[1])^",
        "Function 'MAP_CONCAT' should all be of type map, "
            + "but it is 'INTEGER NOT NULL ARRAY NOT NULL'", false);
    f.checkFails("^map_concat(map['foo', 1], null)^",
        "Function 'MAP_CONCAT' should all be of type map, "
            + "but it is 'NULL'", false);
    // test operands not in same type family.
    f.checkFails("^map_concat(map[1, null], array[1])^",
        "Parameters must be of the same type", false);

    // 2. check with map function, map(k, v ...)
    final SqlOperatorFixture f1 = fixture()
        .setFor(SqlLibraryOperators.MAP_CONCAT)
        .withLibrary(SqlLibrary.SPARK);
    f1.checkScalar("map_concat(map('foo', 1), map('bar', 2))", "{foo=1, bar=2}",
        "(CHAR(3) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f1.checkScalar("map_concat(map('foo', 1), map('bar', 2), map('foo', 2))", "{foo=2, bar=2}",
        "(CHAR(3) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f1.checkScalar("map_concat(map(null, 1), map(null, 2))", "{null=2}",
        "(NULL, INTEGER NOT NULL) MAP NOT NULL");
    f1.checkScalar("map_concat(map(1, 2), map(1, null))", "{1=null}",
        "(INTEGER NOT NULL, INTEGER) MAP NOT NULL");
    f1.checkScalar("map_concat(map('foo', 1), map())", "{foo=1}",
        "(UNKNOWN NOT NULL, UNKNOWN NOT NULL) MAP NOT NULL");

    // test operand is null map
    f1.checkNull("map_concat(map('foo', 1), cast(null as map<varchar, int>))");
    f1.checkType("map_concat(map('foo', 1), cast(null as map<varchar, int>))",
        "(VARCHAR NOT NULL, INTEGER) MAP");
    f1.checkNull("map_concat(cast(null as map<varchar, int>), map['foo', 1])");
    f1.checkType("map_concat(cast(null as map<varchar, int>), map['foo', 1])",
        "(VARCHAR NOT NULL, INTEGER) MAP");

    f1.checkFails("^map_concat(map('foo', 1), null)^",
        "Function 'MAP_CONCAT' should all be of type map, "
            + "but it is 'NULL'", false);
    // test operands not in same type family.
    f1.checkFails("^map_concat(map(1, null), array[1])^",
        "Parameters must be of the same type", false);
  }


  /** Tests {@code MAP_ENTRIES} function from Spark. */
  @Test void testMapEntriesFunc() {
    // 1. check with std map constructor, map[k, v ...]
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.MAP_ENTRIES);
    f0.checkFails("^map_entries(map['foo', 1, 'bar', 2])^",
        "No match found for function signature MAP_ENTRIES\\(<\\(CHAR\\(3\\), INTEGER\\) "
            + "MAP>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("map_entries(map['foo', 1, 'bar', 2])", "[{foo, 1}, {bar, 2}]",
        "RecordType(CHAR(3) NOT NULL f0, INTEGER NOT NULL f1) NOT NULL ARRAY NOT NULL");

    // elements cast
    // key cast
    f.checkScalar("map_entries(map[cast(1 as tinyint), 1, 2, 2])", "[{1, 1}, {2, 2}]",
        "RecordType(INTEGER NOT NULL f0, INTEGER NOT NULL f1) NOT NULL ARRAY NOT NULL");

    // value cast
    f.checkScalar("map_entries(map[1, cast(1 as tinyint), 2, 2])", "[{1, 1}, {2, 2}]",
        "RecordType(INTEGER NOT NULL f0, INTEGER NOT NULL f1) NOT NULL ARRAY NOT NULL");


    // 2. check with map function, map(k, v ...)
    final SqlOperatorFixture f1 = fixture()
        .setFor(SqlLibraryOperators.MAP_ENTRIES)
        .withLibrary(SqlLibrary.SPARK);
    f1.checkScalar("map_entries(map())", "[]",
        "RecordType(UNKNOWN NOT NULL f0, UNKNOWN NOT NULL f1) NOT NULL ARRAY NOT NULL");
    f1.checkScalar("map_entries(map('foo', 1, 'bar', 2))", "[{foo, 1}, {bar, 2}]",
        "RecordType(CHAR(3) NOT NULL f0, INTEGER NOT NULL f1) NOT NULL ARRAY NOT NULL");

    // 3. check key is not allowed to be null
    f.checkFails("map_entries(map[cast(1 as decimal), 1, null, 2])",
        "Illegal arguments for MAP_ENTRIES function: using a map with a null key is not allowed",
        true);
    f.checkFails("map_entries(map[1, cast(1 as bigint), null, 2])",
        "Illegal arguments for MAP_ENTRIES function: using a map with a null key is not allowed",
        true);
    f.checkFails("map_entries(map[1, cast(1 as decimal), null, 2])",
        "Illegal arguments for MAP_ENTRIES function: using a map with a null key is not allowed",
        true);
    f.checkFails("map_entries(map['foo', 1, null, 2])",
        "Illegal arguments for MAP_ENTRIES function: using a map with a null key is not allowed",
        true);
  }

  /** Tests {@code MAP_KEYS} function from Spark. */
  @Test void testMapKeysFunc() {
    // 1. check with std map constructor, map[k, v ...]
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.MAP_KEYS);
    f0.checkFails("^map_keys(map['foo', 1, 'bar', 2])^",
        "No match found for function signature MAP_KEYS\\(<\\(CHAR\\(3\\), INTEGER\\) "
            + "MAP>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("map_keys(map['foo', 1, 'bar', 2])", "[foo, bar]",
        "CHAR(3) NOT NULL ARRAY NOT NULL");

    // elements cast
    // key cast
    f.checkScalar("map_keys(map[cast(1 as tinyint), 1, 2, 2])", "[1, 2]",
        "INTEGER NOT NULL ARRAY NOT NULL");

    // value cast
    f.checkScalar("map_keys(map[1, cast(1 as tinyint), 2, 2])", "[1, 2]",
        "INTEGER NOT NULL ARRAY NOT NULL");

    // 2. check with map function, map(k, v ...)
    final SqlOperatorFixture f1 = fixture()
        .setFor(SqlLibraryOperators.MAP_KEYS)
        .withLibrary(SqlLibrary.SPARK);
    f1.checkScalar("map_keys(map())", "[]",
        "UNKNOWN NOT NULL ARRAY NOT NULL");
    f1.checkScalar("map_keys(map('foo', 1, 'bar', 2))", "[foo, bar]",
        "CHAR(3) NOT NULL ARRAY NOT NULL");

    f.checkFails("map_keys(map['foo', 1, null, 2])",
        "Illegal arguments for MAP_KEYS function: using a map with a null key is not allowed",
        true);
    f.checkFails("map_keys(map[1, cast(1 as decimal), null, 2])",
        "Illegal arguments for MAP_KEYS function: using a map with a null key is not allowed",
        true);
    f.checkFails("map_keys(map[1, cast(1 as bigint), null, 2])",
        "Illegal arguments for MAP_KEYS function: using a map with a null key is not allowed",
        true);
    f.checkFails("map_keys(map[cast(1 as decimal), 1, null, 2])",
        "Illegal arguments for MAP_KEYS function: using a map with a null key is not allowed",
        true);
  }

  /** Tests {@code MAP_VALUES} function from Spark. */
  @Test void testMapValuesFunc() {
    // 1. check with std map constructor, map[k, v ...]
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.MAP_VALUES);
    f0.checkFails("^map_values(map['foo', 1, 'bar', 2])^",
        "No match found for function signature MAP_VALUES\\(<\\(CHAR\\(3\\), INTEGER\\) "
            + "MAP>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("map_values(map['foo', 1, 'bar', 2])", "[1, 2]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("map_values(map['foo', 1, 'bar', cast(null as integer)])", "[1, null]",
        "INTEGER ARRAY NOT NULL");

    // 2. check with map function, map(k, v ...)
    final SqlOperatorFixture f1 = fixture()
        .setFor(SqlLibraryOperators.MAP_VALUES)
        .withLibrary(SqlLibrary.SPARK);
    f1.checkScalar("map_values(map())", "[]",
        "UNKNOWN NOT NULL ARRAY NOT NULL");
    f1.checkScalar("map_values(map('foo', 1, 'bar', 2))", "[1, 2]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f1.checkScalar("map_values(map('foo', 1, 'bar', cast(null as integer)))", "[1, null]",
        "INTEGER ARRAY NOT NULL");

    f.checkFails("map_values(map['foo', 1, null, 2])",
        "Illegal arguments for MAP_VALUES function: using a map with a null key is not allowed",
        true);
    f.checkFails("map_values(map[1, cast(1 as decimal), null, 2])",
        "Illegal arguments for MAP_VALUES function: using a map with a null key is not allowed",
        true);
    f.checkFails("map_values(map[1, cast(1 as bigint), null, 2])",
        "Illegal arguments for MAP_VALUES function: using a map with a null key is not allowed",
        true);
    f.checkFails("map_values(map[cast(1 as decimal), 1, null, 2])",
        "Illegal arguments for MAP_VALUES function: using a map with a null key is not allowed",
        true);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6223">[CALCITE-6223]
   * Add MAP_CONTAINS_KEY function (enabled in SPARK library)</a>.
   */
  @Test void testMapContainsKeyFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.MAP_CONTAINS_KEY);
    f0.checkFails("^map_contains_key(map[1, 'a'], 1)^",
        "No match found for function signature "
            + "MAP_CONTAINS_KEY\\(<\\(INTEGER, CHAR\\(1\\)\\) MAP\\>, <NUMERIC>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("map_contains_key(map[1, 'a', 2, 'b'], 1)", "true",
        "BOOLEAN NOT NULL");
    f.checkScalar("map_contains_key(map[1, 'a'], 1)", "true",
        "BOOLEAN NOT NULL");
    f.checkScalar("map_contains_key(map[1, 'a'], 2)", "false",
        "BOOLEAN NOT NULL");
    f.checkScalar("map_contains_key(map['foo', 1], 'foo')", "true",
        "BOOLEAN NOT NULL");
    f.checkScalar("map_contains_key(map['foo', 1], 'bar')", "false",
        "BOOLEAN NOT NULL");
    f.checkScalar("map_contains_key(map(cast(1 as double), 2), cast(1 as double))", "true",
        "BOOLEAN NOT NULL");
    f.checkScalar("map_contains_key(map(array(1), array(2)), array(1))", "true",
        "BOOLEAN NOT NULL");
    f.checkType("map_contains_key(cast(null as map<int, varchar>), 1)", "BOOLEAN");
    f.checkNull("map_contains_key(map[1, 'a'], cast(null as integer))");
    f.checkNull("map_contains_key(cast(null as map<int, varchar>), cast(null as integer))");
    f.checkFails("^map_contains_key(map['foo', 1], 1)^",
        "CHAR\\(3\\) is not comparable to INTEGER",
        false);
    f.checkFails("^map_contains_key(map[1, 1], 'foo')^",
        "INTEGER is not comparable to CHAR\\(3\\)",
        false);
  }

  /** Tests {@code MAP_FROM_ARRAYS} function from Spark. */
  @Test void testMapFromArraysFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.MAP_FROM_ARRAYS);
    f0.checkFails("^map_from_arrays(array[1, 2], array['foo', 'bar'])^",
        "No match found for function signature MAP_FROM_ARRAYS\\(<INTEGER ARRAY>, "
            + "<CHAR\\(3\\) ARRAY>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("map_from_arrays(array[1, 2], array['foo', 'bar'])", "{1=foo, 2=bar}",
        "(INTEGER NOT NULL, CHAR(3) NOT NULL) MAP NOT NULL");
    f.checkScalar("map_from_arrays(array[1, 1, null], array['foo', 'bar', 'name'])",
        "{1=bar , null=name}", "(INTEGER, CHAR(4) NOT NULL) MAP NOT NULL");

    final SqlOperatorFixture f1 =
        f.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    f1.checkScalar("map_from_arrays(array[1, 1, null], array['foo', 'bar', 'name'])",
        "{1=bar, null=name}", "(INTEGER, VARCHAR(4) NOT NULL) MAP NOT NULL");

    f.checkScalar("map_from_arrays(array(), array())",
        "{}", "(UNKNOWN NOT NULL, UNKNOWN NOT NULL) MAP NOT NULL");
    f.checkType("map_from_arrays(cast(null as integer array), array['foo', 'bar'])",
        "(INTEGER, CHAR(3) NOT NULL) MAP");
    f.checkNull("map_from_arrays(cast(null as integer array), array['foo', 'bar'])");

    f.checkFails("^map_from_arrays(array[1, 2], 2)^",
        "Cannot apply 'MAP_FROM_ARRAYS' to arguments of type 'MAP_FROM_ARRAYS\\(<INTEGER ARRAY>,"
            + " <INTEGER>\\)'. Supported form\\(s\\): 'MAP_FROM_ARRAYS\\(<ARRAY>, <ARRAY>\\)'",
        false);
    f.checkFails("^map_from_arrays(2, array[1, 2])^",
        "Cannot apply 'MAP_FROM_ARRAYS' to arguments of type 'MAP_FROM_ARRAYS\\(<INTEGER>,"
            + " <INTEGER ARRAY>\\)'. Supported form\\(s\\): 'MAP_FROM_ARRAYS\\(<ARRAY>, <ARRAY>\\)'",
        false);
    f.checkFails("map_from_arrays(^array[1, '1', true]^, array['a', 'b', 'c'])",
        "Parameters must be of the same type",
        false);
    f.checkFails("map_from_arrays(array['a', 'b', 'c'], ^array[1, '1', true]^)",
        "Parameters must be of the same type",
        false);
    f.checkFails("map_from_arrays(array[1, 2], array['foo'])",
        "Illegal arguments: The length of the keys array 2 is not equal to the length "
            + "of the values array 1 in MAP_FROM_ARRAYS function",
        true);
  }

  /** Tests {@code MAP_FROM_ENTRIES} function from Spark. */
  @Test void testMapFromEntriesFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.MAP_FROM_ENTRIES);
    f0.checkFails("^map_from_entries(array[row(1, 'a'), row(2, 'b')])^",
        "No match found for function signature MAP_FROM_ENTRIES\\("
            + "<RecordType\\(INTEGER EXPR\\$0, CHAR\\(1\\) EXPR\\$1\\) ARRAY>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("map_from_entries(array[row(1, 'a'), row(2, 'b')])", "{1=a, 2=b}",
        "(INTEGER NOT NULL, CHAR(1) NOT NULL) MAP NOT NULL");
    f.checkScalar("map_from_entries(array[row(1, 'a'), row(1, 'b')])", "{1=b}",
        "(INTEGER NOT NULL, CHAR(1) NOT NULL) MAP NOT NULL");
    f.checkScalar("map_from_entries(array[row(null, 'a'), row(null, 'b')])", "{null=b}",
        "(NULL, CHAR(1) NOT NULL) MAP NOT NULL");
    f.checkScalar("map_from_entries(array[row(1, 'a'), row(1, null)])", "{1=null}",
        "(INTEGER NOT NULL, CHAR(1)) MAP NOT NULL");
    f.checkScalar("map_from_entries(array[row(array['a'], 1), row(array['b'], 2)])",
        "{[a]=1, [b]=2}",
        "(CHAR(1) NOT NULL ARRAY NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f.checkScalar("map_from_entries(array[row(map['a', 1], 2), row(map['a', 1], 3)])",
        "{{a=1}=3}",
        "((CHAR(1) NOT NULL, INTEGER NOT NULL) MAP NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f.checkType("map_from_entries(cast(null as row(f0 int, f1 varchar) array))",
        "(INTEGER NOT NULL, VARCHAR NOT NULL) MAP");
    f.checkNull("map_from_entries(cast(null as row(f0 int, f1 varchar) array))");
    f.checkNull("map_from_entries(array[row(1, 'a'), null])");
    f.checkType("map_from_entries(array[row(1, 'a'), null])",
        "(INTEGER, CHAR(1)) MAP");

    f.checkFails("^map_from_entries(array[1])^",
        "Cannot apply 'MAP_FROM_ENTRIES' to arguments of type 'MAP_FROM_ENTRIES\\("
            + "<INTEGER ARRAY>\\)'. Supported form\\(s\\): 'MAP_FROM_ENTRIES\\("
            + "<ARRAY<RECORDTYPE\\(TWO FIELDS\\)>>\\)'",
        false);
    f.checkFails("^map_from_entries(array[row(1, 'a', 2)])^",
        "Cannot apply 'MAP_FROM_ENTRIES' to arguments of type 'MAP_FROM_ENTRIES\\("
            + "<RECORDTYPE\\(INTEGER EXPR\\$0, CHAR\\(1\\) EXPR\\$1, INTEGER EXPR\\$2\\) ARRAY>\\)'. "
            + "Supported form\\(s\\): 'MAP_FROM_ENTRIES\\(<ARRAY<RECORDTYPE\\(TWO FIELDS\\)>>\\)'",
        false);
  }

  /** Tests {@code STR_TO_MAP} function from Spark. */
  @Test void testStrToMapFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.STR_TO_MAP);
    f0.checkFails("^str_to_map('a=1,b=2', ',', '=')^",
        "No match found for function signature STR_TO_MAP\\("
            + "<CHARACTER>, <CHARACTER>, <CHARACTER>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkScalar("str_to_map('a=1,b=2', ',', '=')", "{a=1, b=2}",
        "(CHAR(7) NOT NULL, CHAR(7) NOT NULL) MAP NOT NULL");
    f.checkScalar("str_to_map('a:1,b:2')", "{a=1, b=2}",
        "(CHAR(7) NOT NULL, CHAR(7) NOT NULL) MAP NOT NULL");
    f.checkScalar("str_to_map('a:1,b:2', ',')", "{a=1, b=2}",
        "(CHAR(7) NOT NULL, CHAR(7) NOT NULL) MAP NOT NULL");
    f.checkScalar("str_to_map('a=1&b=2', '&', '=')", "{a=1, b=2}",
        "(CHAR(7) NOT NULL, CHAR(7) NOT NULL) MAP NOT NULL");
    f.checkScalar("str_to_map('k#2%v#3', '%', '#')", "{k=2, v=3}",
        "(CHAR(7) NOT NULL, CHAR(7) NOT NULL) MAP NOT NULL");
    f.checkScalar("str_to_map('a:1&b:2', '&')", "{a=1, b=2}",
        "(CHAR(7) NOT NULL, CHAR(7) NOT NULL) MAP NOT NULL");
    f.checkScalar("str_to_map('k:2%v:3', '%')", "{k=2, v=3}",
        "(CHAR(7) NOT NULL, CHAR(7) NOT NULL) MAP NOT NULL");
    f.checkScalar("str_to_map('a')", "{a=null}",
        "(CHAR(1) NOT NULL, CHAR(1) NOT NULL) MAP NOT NULL");
    f.checkScalar("str_to_map('a,b,c')", "{a=null, b=null, c=null}",
        "(CHAR(5) NOT NULL, CHAR(5) NOT NULL) MAP NOT NULL");
    f.checkScalar("str_to_map('a-b--c', '--')", "{a-b=null, c=null}",
        "(CHAR(6) NOT NULL, CHAR(6) NOT NULL) MAP NOT NULL");
    f.checkType("str_to_map(cast(null as varchar))",
        "(VARCHAR, VARCHAR) MAP");
    f.checkNull("str_to_map(cast(null as varchar))");
    f.checkNull("str_to_map(null, ',', ':')");
    f.checkNull("str_to_map('a:1,b:2,c:3', null, ':')");
    f.checkNull("str_to_map('a:1,b:2,c:3', ',',null)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5807">[CALCITE-5807]
   * Add SUBSTRING_INDEX function (enabled in Spark library).</a>.
   */
  @Test void testSubstringIndexFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.SUBSTRING_INDEX);
    f0.checkFails("^substring_index('a', ',')^",
        "No match found for function signature SUBSTRING_INDEX\\("
            + "<CHARACTER>, <CHARACTER>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.SPARK);
    f.checkString("substring_index('www.apache.org', '.', 2)",
        "www.apache", "VARCHAR(14) NOT NULL");
    f.checkString("substring_index('www.apache.org', '.', 1)",
        "www", "VARCHAR(14) NOT NULL");
    f.checkString("substring_index('www.apache.org', '.', 3)",
        "www.apache.org", "VARCHAR(14) NOT NULL");
    f.checkString("substring_index('www.apache.org', '.', -1)",
        "org", "VARCHAR(14) NOT NULL");

    f.checkString("substring_index('aBc', 'B', -1)",
        "c", "VARCHAR(3) NOT NULL");
    f.checkString("substring_index('aBc', 'b', -1)",
        "aBc", "VARCHAR(3) NOT NULL");

    f.checkString("substring_index('aBc', 'B', 0)",
        "", "VARCHAR(3) NOT NULL");
    f.checkString("substring_index('aBc', 'b', 0)",
        "", "VARCHAR(3) NOT NULL");

    f.checkNull("substring_index(cast(null as varchar(1)),"
        + " cast(null as varchar(1)), cast(null as integer))");
    f.checkNull("substring_index(cast(null as varchar(1)),"
        + " cast(null as varchar(1)), 2)");
    f.checkNull("substring_index('abc', cast(null as varchar(1)),"
        + " cast(null as integer))");
    f.checkNull("substring_index(cast(null as varchar(1)), '.',"
        + " cast(null as integer))");
    f.checkNull("substring_index('abc', '.', cast(null as integer))");
    f.checkNull("substring_index('abc', cast(null as varchar(1)), 2)");
  }

  /** Tests {@code UNIX_SECONDS} and other datetime functions from BigQuery. */
  @Test void testUnixSecondsFunc() {
    SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.UNIX_SECONDS)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("unix_seconds(timestamp '1970-01-01 00:00:00')", 0,
        "BIGINT NOT NULL");
    f.checkNull("unix_seconds(cast(null as timestamp))");
    f.checkNull("unix_millis(cast(null as timestamp))");
    f.checkNull("unix_micros(cast(null as timestamp))");
    f.checkScalar("timestamp_seconds(0)", "1970-01-01 00:00:00",
        "TIMESTAMP(0) NOT NULL");
    f.checkNull("timestamp_seconds(cast(null as bigint))");
    f.checkNull("timestamp_millis(cast(null as bigint))");
    f.checkNull("timestamp_micros(cast(null as bigint))");
    f.checkScalar("date_from_unix_date(0)", "1970-01-01", "DATE NOT NULL");

    // DATE is a reserved keyword, but the parser has special treatment to
    // allow it as a function.
    f.checkNull("DATE(null)");
    f.checkScalar("DATE('1985-12-06')", "1985-12-06", "DATE NOT NULL");
  }

  /** Tests that the {@code CURRENT_DATETIME} function is defined in the
   * BigQuery library, is not available in the default library,
   * and can be called with and without parentheses. */
  @Test void testCurrentDatetimeFunc() {
    SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.CURRENT_DATETIME);

    // In default conformance, with BigQuery operator table,
    // CURRENT_DATETIME is valid only without parentheses.
    final SqlOperatorFixture f1 =
        f0.withLibrary(SqlLibrary.BIG_QUERY);
    f1.checkType("CURRENT_DATETIME", "TIMESTAMP(0) NOT NULL");
    f1.checkFails("^CURRENT_DATETIME()^",
        "No match found for function signature CURRENT_DATETIME\\(\\)",
        false);

    // In BigQuery conformance, with BigQuery operator table,
    // CURRENT_DATETIME should be valid with and without parentheses.
    // We cannot execute it because results are non-deterministic.
    SqlOperatorFixture f =
        f1.withConformance(SqlConformanceEnum.BIG_QUERY);
    f.checkType("CURRENT_DATETIME", "TIMESTAMP(0) NOT NULL");
    f.checkType("CURRENT_DATETIME()", "TIMESTAMP(0) NOT NULL");
    f.checkType("CURRENT_DATETIME('America/Los_Angeles')",
        "TIMESTAMP(0) NOT NULL");
    f.checkType("CURRENT_DATETIME(CAST(NULL AS VARCHAR(20)))", "TIMESTAMP(0)");
    f.checkNull("CURRENT_DATETIME(CAST(NULL AS VARCHAR(20)))");

    // In BigQuery conformance, but with the default operator table,
    // CURRENT_DATETIME is not found.
    final SqlOperatorFixture f2 =
        f0.withConformance(SqlConformanceEnum.BIG_QUERY);
    f2.checkFails("^CURRENT_DATETIME^",
        "Column 'CURRENT_DATETIME' not found in any table", false);
    f2.checkFails("^CURRENT_DATETIME()^",
        "No match found for function signature CURRENT_DATETIME\\(\\)",
        false);
  }

  @Test void testAbsFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ABS, VmName.EXPAND);
    f.checkScalarExact("abs(-1)", 1);
    f.checkScalarExact("abs(cast(10 as TINYINT))", "TINYINT NOT NULL", "10");
    f.checkScalarExact("abs(cast(-20 as SMALLINT))", "SMALLINT NOT NULL", "20");
    f.checkScalarExact("abs(cast(-100 as INT))", "INTEGER NOT NULL", "100");
    f.checkScalarExact("abs(cast(1000 as BIGINT))", "BIGINT NOT NULL", "1000");
    f.checkScalarExact("abs(54.4)", "DECIMAL(3, 1) NOT NULL", "54.4");
    f.checkScalarExact("abs(-54.4)", "DECIMAL(3, 1) NOT NULL", "54.4");
    f.checkScalarApprox("abs(-9.32E-2)", "DOUBLE NOT NULL",
        isExactly("0.0932"));
    f.checkScalarApprox("abs(cast(-3.5 as double))", "DOUBLE NOT NULL",
        isExactly("3.5"));
    f.checkScalarApprox("abs(cast(-3.5 as float))", "FLOAT NOT NULL",
        isExactly("3.5"));
    f.checkScalarApprox("abs(cast(3.5 as real))", "REAL NOT NULL",
        isExactly("3.5"));
    f.checkNull("abs(cast(null as double))");
  }

  @Test void testAbsFuncIntervals() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("abs(interval '-2' day)", "+2", "INTERVAL DAY NOT NULL");
    f.checkScalar("abs(interval '-5-03' year to month)",
        "+5-03", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkNull("abs(cast(null as interval hour))");
  }

  @Test void testAcosFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ACOS, VmName.EXPAND);
    f.checkType("acos(0)", "DOUBLE NOT NULL");
    f.checkType("acos(cast(1 as float))", "DOUBLE NOT NULL");
    f.checkType("acos(case when false then 0.5 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^acos('abc')^",
            "Cannot apply 'ACOS' to arguments of type "
                + "'ACOS\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'ACOS\\(<NUMERIC>\\)'",
            false);
    f.checkType("acos('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("acos(0.5)", "DOUBLE NOT NULL",
        isWithin(1.0472d, 0.0001d));
    f.checkScalarApprox("acos(cast(0.5 as decimal(1, 1)))", "DOUBLE NOT NULL",
        isWithin(1.0472d, 0.0001d));
    f.checkNull("acos(cast(null as integer))");
    f.checkNull("acos(cast(null as double))");
  }

  @Test void testAcosdFunc() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.POSTGRESQL);
    f.setFor(SqlLibraryOperators.ACOSD, VmName.EXPAND);
    f.checkType("acosd(0)", "DOUBLE NOT NULL");
    f.checkType("acosd(cast(1 as float))", "DOUBLE NOT NULL");
    f.checkType("acosd(case when false then 0.5 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^acosd('abc')^",
            "Cannot apply 'ACOSD' to arguments of type "
                + "'ACOSD\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'ACOSD\\(<NUMERIC>\\)'",
            false);
    f.checkType("acosd('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("acosd(0.5)", "DOUBLE NOT NULL",
        isWithin(60.0d, 0.01d));
    f.checkScalarApprox("acosd(cast(0.5 as decimal(3, 1)))", "DOUBLE NOT NULL",
        isWithin(60.0d, 0.1d));
    f.checkFails("acosd(-2.0)", "input is out of range", true);
    f.checkScalarExact("acosd(cast('NaN' as double))", "DOUBLE NOT NULL", "NaN");
    f.checkFails("acosd(cast('Infinity' as double))", "input is out of range", true);
    f.checkNull("acosd(cast(null as integer))");
    f.checkNull("acosd(cast(null as double))");
  }

  @Test void testAsinFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ASIN, VmName.EXPAND);
    f.checkType("asin(0)", "DOUBLE NOT NULL");
    f.checkType("asin(cast(1 as float))", "DOUBLE NOT NULL");
    f.checkType("asin(case when false then 0.5 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^asin('abc')^",
            "Cannot apply 'ASIN' to arguments of type "
                + "'ASIN\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'ASIN\\(<NUMERIC>\\)'",
            false);
    f.checkType("asin('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("asin(0.5)", "DOUBLE NOT NULL",
        isWithin(0.5236d, 0.0001d));
    f.checkScalarApprox("asin(cast(0.5 as decimal(1, 1)))", "DOUBLE NOT NULL",
        isWithin(0.5236d, 0.0001d));
    f.checkNull("asin(cast(null as integer))");
    f.checkNull("asin(cast(null as double))");
  }

  @Test void testAsindFunc() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.POSTGRESQL);
    f.setFor(SqlLibraryOperators.ASIND, VmName.EXPAND);
    f.checkType("asind(0)", "DOUBLE NOT NULL");
    f.checkType("asind(cast(1 as float))", "DOUBLE NOT NULL");
    f.checkType("asind(case when false then 0.5 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^asind('abc')^",
            "Cannot apply 'ASIND' to arguments of type "
                + "'ASIND\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'ASIND\\(<NUMERIC>\\)'",
            false);
    f.checkType("asind('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("asind(0.5)", "DOUBLE NOT NULL",
        isWithin(30.0d, 0.01d));
    f.checkScalarApprox("asind(cast(0.5 as decimal(3, 1)))", "DOUBLE NOT NULL",
        isWithin(30.0d, 0.01d));
    f.checkFails("asind(-2.0)", "input is out of range", true);
    f.checkScalarExact("asind(cast('NaN' as double))", "DOUBLE NOT NULL", "NaN");
    f.checkFails("asind(cast('Infinity' as double))", "input is out of range", true);
    f.checkNull("asind(cast(null as integer))");
    f.checkNull("asind(cast(null as double))");
  }

  @Test void testAtanFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ATAN, VmName.EXPAND);
    f.checkType("atan(2)", "DOUBLE NOT NULL");
    f.checkType("atan(cast(2 as float))", "DOUBLE NOT NULL");
    f.checkType("atan(case when false then 2 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^atan('abc')^",
            "Cannot apply 'ATAN' to arguments of type "
                + "'ATAN\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'ATAN\\(<NUMERIC>\\)'",
            false);
    f.checkType("atan('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("atan(2)", "DOUBLE NOT NULL",
        isWithin(1.1071d, 0.0001d));
    f.checkScalarApprox("atan(cast(2 as decimal(1, 0)))", "DOUBLE NOT NULL",
        isWithin(1.1071d, 0.0001d));
    f.checkNull("atan(cast(null as integer))");
    f.checkNull("atan(cast(null as double))");
  }

  @Test void testAtan2Func() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ATAN2, VmName.EXPAND);
    f.checkType("atan2(2, -2)", "DOUBLE NOT NULL");
    f.checkScalarApprox("atan2(cast(1 as float), -1)", "DOUBLE NOT NULL",
        isWithin(2.3562d, 0.0001d));
    f.checkType("atan2(case when false then 0.5 else null end, -1)",
        "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^atan2('abc', 'def')^",
            "Cannot apply 'ATAN2' to arguments of type "
                + "'ATAN2\\(<CHAR\\(3\\)>, <CHAR\\(3\\)>\\)'\\. "
                + "Supported form\\(s\\): 'ATAN2\\(<NUMERIC>, <NUMERIC>\\)'",
            false);
    f.checkType("atan2('abc', 'def')", "DOUBLE NOT NULL");
    f.checkScalarApprox("atan2(0.5, -0.5)", "DOUBLE NOT NULL",
        isWithin(2.3562d, 0.0001d));
    f.checkScalarApprox("atan2(cast(0.5 as decimal(1, 1)),"
            + " cast(-0.5 as decimal(1, 1)))", "DOUBLE NOT NULL",
        isWithin(2.3562d, 0.0001d));
    f.checkNull("atan2(cast(null as integer), -1)");
    f.checkNull("atan2(1, cast(null as double))");
  }

  @Test void testAtandFunc() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.POSTGRESQL);
    f.setFor(SqlLibraryOperators.ATAND, VmName.EXPAND);
    f.checkType("atand(2)", "DOUBLE NOT NULL");
    f.checkType("atand(cast(2 as float))", "DOUBLE NOT NULL");
    f.checkType("atand(case when false then 2 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^atand('abc')^",
            "Cannot apply 'ATAND' to arguments of type "
                + "'ATAND\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'ATAND\\(<NUMERIC>\\)'",
            false);
    f.checkType("atand('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("atand(1.73)", "DOUBLE NOT NULL",
        isWithin(60.0d, 0.1d));
    f.checkScalarApprox("atand(cast(1.73 as decimal(3, 2)))", "DOUBLE NOT NULL",
        isWithin(60.0d, 0.1d));
    f.checkScalarApprox("atand(-2000.0)", "DOUBLE NOT NULL", isWithin(-89.97d, 0.1d));
    f.checkScalarExact("atand(cast('NaN' as double))", "DOUBLE NOT NULL", "NaN");
    f.checkScalarApprox("atand(cast('Infinity' as double))", "DOUBLE NOT NULL",
        isWithin(90.0d, 0.01d));
    f.checkScalarApprox("atand(cast('-Infinity' as double))", "DOUBLE NOT NULL",
        isWithin(-90.0d, 0.01d));
    f.checkNull("atand(cast(null as integer))");
    f.checkNull("atand(cast(null as double))");
  }

  @Test void testAcoshFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ACOSH);
    f0.checkFails("^acosh(1)^",
        "No match found for function signature ACOSH\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkType("acosh(1)", "DOUBLE NOT NULL");
      f.checkType("acosh(cast(1 as Decimal))", "DOUBLE NOT NULL");
      f.checkType("acosh(case when false then 1 else null end)", "DOUBLE");
      f.checkType("acosh('abc')", "DOUBLE NOT NULL");
      f.checkScalarApprox("acosh(1)", "DOUBLE NOT NULL",
          isWithin(0d, 0.0001d));
      f.checkScalarApprox("acosh(cast(10 as decimal))", "DOUBLE NOT NULL",
          isWithin(2.9932d, 0.0001d));
      f.checkNull("acosh(cast(null as integer))");
      f.checkNull("acosh(cast(null as double))");
      f.checkFails("acosh(0.1)",
          "Input parameter of acosh cannot be less than 1!",
          true);
    };
    f0.forEachLibrary(list(SqlLibrary.ALL), consumer);
  }

  @Test void testAsinhFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ASINH);
    f0.checkFails("^asinh(1)^",
        "No match found for function signature ASINH\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkType("asinh(1)", "DOUBLE NOT NULL");
      f.checkType("asinh(cast(1 as Decimal))", "DOUBLE NOT NULL");
      f.checkType("asinh(case when false then 1 else null end)", "DOUBLE");
      f.checkType("asinh('abc')", "DOUBLE NOT NULL");
      f.checkScalarApprox("asinh(-2.5)", "DOUBLE NOT NULL",
          isWithin(-1.6472d, 0.0001d));
      f.checkScalarApprox("asinh(cast(10 as decimal))", "DOUBLE NOT NULL",
          isWithin(2.9982, 0.0001d));
      f.checkNull("asinh(cast(null as integer))");
      f.checkNull("asinh(cast(null as double))");
    };
    f0.forEachLibrary(list(SqlLibrary.ALL), consumer);
  }

  @Test void testAtanhFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.ATANH);
    f0.checkFails("^atanh(1)^",
        "No match found for function signature ATANH\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkType("atanh(0.1)", "DOUBLE NOT NULL");
      f.checkType("atanh(cast(0 as Decimal))", "DOUBLE NOT NULL");
      f.checkType("atanh(case when false then 0 else null end)", "DOUBLE");
      f.checkType("atanh('abc')", "DOUBLE NOT NULL");
      f.checkScalarApprox("atanh(0.76159416)", "DOUBLE NOT NULL",
          isWithin(1d, 0.0001d));
      f.checkScalarApprox("atanh(cast(-0.1 as decimal(2,1)))", "DOUBLE NOT NULL",
          isWithin(-0.1003d, 0.0001d));
      f.checkNull("atanh(cast(null as integer))");
      f.checkNull("atanh(cast(null as double))");
      f.checkFails("atanh(1)",
          "Input arguments of ATANH out of range: 1; should be in the range of \\(-1, 1\\)",
          true);
      f.checkFails("atanh(-1)",
          "Input arguments of ATANH out of range: -1; should be in the range of \\(-1, 1\\)",
          true);
      f.checkFails("atanh(-1.5)",
          "Input arguments of ATANH out of range: -1.5; should be in the range of \\(-1, 1\\)",
          true);
    };
    f0.forEachLibrary(list(SqlLibrary.ALL), consumer);
  }

  @Test void testCbrtFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CBRT, VmName.EXPAND);
    f.checkType("cbrt(1)", "DOUBLE NOT NULL");
    f.checkType("cbrt(cast(1 as float))", "DOUBLE NOT NULL");
    f.checkType("cbrt(case when false then 1 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^cbrt('abc')^",
            "Cannot apply 'CBRT' to arguments of type "
                + "'CBRT\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'CBRT\\(<NUMERIC>\\)'",
            false);
    f.checkType("cbrt('abc')", "DOUBLE NOT NULL");
    f.checkScalar("cbrt(8)", "2.0", "DOUBLE NOT NULL");
    f.checkScalar("cbrt(-8)", "-2.0", "DOUBLE NOT NULL");
    f.checkScalar("cbrt(cast(1 as decimal(1, 0)))", "1.0",
        "DOUBLE NOT NULL");
    f.checkNull("cbrt(cast(null as integer))");
    f.checkNull("cbrt(cast(null as double))");
  }

  @Test void testCosFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.COS, VmName.EXPAND);
    f.checkType("cos(1)", "DOUBLE NOT NULL");
    f.checkType("cos(cast(1 as float))", "DOUBLE NOT NULL");
    f.checkType("cos(case when false then 1 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^cos('abc')^",
            "Cannot apply 'COS' to arguments of type "
                + "'COS\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'COS\\(<NUMERIC>\\)'",
            false);
    f.checkType("cos('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("cos(1)", "DOUBLE NOT NULL",
        isWithin(0.5403d, 0.0001d));
    f.checkScalarApprox("cos(cast(1 as decimal(1, 0)))", "DOUBLE NOT NULL",
        isWithin(0.5403d, 0.0001d));
    f.checkNull("cos(cast(null as integer))");
    f.checkNull("cos(cast(null as double))");
  }

  @Test void testCosdFunc() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.POSTGRESQL);
    f.setFor(SqlLibraryOperators.COSD, VmName.EXPAND);
    f.checkType("cosd(60)", "DOUBLE NOT NULL");
    f.checkType("cosd(cast(60 as float))", "DOUBLE NOT NULL");
    f.checkType("cosd(case when false then 60 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^cosd('abc')^",
            "Cannot apply 'COSD' to arguments of type "
                + "'COSD\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'COSD\\(<NUMERIC>\\)'",
            false);
    f.checkType("cosd('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("cosd(60)", "DOUBLE NOT NULL",
        isWithin(0.5d, 0.01d));
    f.checkScalarApprox("cosd(420)", "DOUBLE NOT NULL",
        isWithin(0.5d, 0.01d));
    f.checkScalarApprox("cosd(-60)", "DOUBLE NOT NULL",
        isWithin(0.5d, 0.01d));
    f.checkScalarApprox("cosd(cast(60 as decimal(2, 0)))", "DOUBLE NOT NULL",
        isWithin(0.5d, 0.01d));
    f.checkScalarExact("cosd(cast('NaN' as double))", "DOUBLE NOT NULL",
        "NaN");
    f.checkFails("cosd(cast('Infinity' as double))", "input is out of range", true);
    f.checkNull("cosd(cast(null as integer))");
    f.checkNull("cosd(cast(null as double))");
  }

  @Test void testCoshFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.COSH);
    f0.checkFails("^cosh(1)^",
        "No match found for function signature COSH\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkType("cosh(1)", "DOUBLE NOT NULL");
      f.checkType("cosh(cast(1 as float))", "DOUBLE NOT NULL");
      f.checkType("cosh(case when false then 1 else null end)", "DOUBLE");
      f.checkType("cosh('abc')", "DOUBLE NOT NULL");
      f.checkScalarApprox("cosh(1)", "DOUBLE NOT NULL",
          isWithin(1.5430d, 0.0001d));
      f.checkScalarApprox("cosh(cast(1 as decimal(1, 0)))", "DOUBLE NOT NULL",
          isWithin(1.5430d, 0.0001d));
      f.checkNull("cosh(cast(null as integer))");
      f.checkNull("cosh(cast(null as double))");
    };
    f0.forEachLibrary(list(SqlLibrary.ALL), consumer);
  }

  @Test void testCotFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.COT, VmName.EXPAND);
    f.checkType("cot(1)", "DOUBLE NOT NULL");
    f.checkType("cot(cast(1 as float))", "DOUBLE NOT NULL");
    f.checkType("cot(case when false then 1 else null end)", "DOUBLE");
    f.enableTypeCoercion(false).checkFails("^cot('abc')^",
        "Cannot apply 'COT' to arguments of type "
            + "'COT\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
            + "'COT\\(<NUMERIC>\\)'",
        false);
    f.checkType("cot('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("cot(1)", "DOUBLE NOT NULL",
        isWithin(0.6421d, 0.0001d));
    f.checkScalarApprox("cot(cast(1 as decimal(1, 0)))", "DOUBLE NOT NULL",
        isWithin(0.6421d, 0.0001d));
    f.checkNull("cot(cast(null as integer))");
    f.checkNull("cot(cast(null as double))");
  }

  @Test void testCothFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.COTH);
    f0.checkFails("^coth(1)^",
        "No match found for function signature COTH\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkType("coth(1)", "DOUBLE NOT NULL");
      f.checkType("coth(cast(1 as float))", "DOUBLE NOT NULL");
      f.checkType("coth(case when false then 1 else null end)", "DOUBLE");
      f.checkType("coth('abc')", "DOUBLE NOT NULL");
      f.checkScalarApprox("coth(1)", "DOUBLE NOT NULL",
          isWithin(1.3130d, 0.0001d));
      f.checkScalarApprox(" coth(cast(1 as decimal(1, 0)))", "DOUBLE NOT NULL",
          isWithin(1.3130d, 0.0001d));
      f.checkNull("coth(cast(null as integer))");
      f.checkNull("coth(cast(null as double))");
    };
    f0.forEachLibrary(list(SqlLibrary.BIG_QUERY, SqlLibrary.ORACLE), consumer);
  }

  @Test void testCschFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.CSCH);
    f0.checkFails("^csch(1)^",
        "No match found for function signature CSCH\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkType("csch(1)", "DOUBLE NOT NULL");
      f.checkType("csch(cast(1 as float))", "DOUBLE NOT NULL");
      f.checkType("csch(case when false then 1 else null end)", "DOUBLE");
      f.checkType("csch('abc')", "DOUBLE NOT NULL");
      f.checkScalarApprox("csch(1)", "DOUBLE NOT NULL",
          isWithin(0.8509d, 0.0001d));
      f.checkScalarApprox(" csch(cast(1 as decimal(1, 0)))", "DOUBLE NOT NULL",
          isWithin(0.8509d, 0.0001d));
      f.checkNull("csch(cast(null as integer))");
      f.checkNull("csch(cast(null as double))");
    };
    f0.forEachLibrary(list(SqlLibrary.BIG_QUERY, SqlLibrary.ORACLE), consumer);
  }

  @Test void testCscFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.CSC);
    f0.checkFails("^csc(1)^",
        "No match found for function signature CSC\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkType("csc(cast (1 as float))", "DOUBLE NOT NULL");
      f.checkType("csc('a')", "DOUBLE NOT NULL");
      f.checkScalarApprox("csc(100)", "DOUBLE NOT NULL",
          isWithin(-1.9748d, 0.0001d));
      f.checkScalarApprox("csc(cast(10 as float))", "DOUBLE NOT NULL",
          isWithin(-1.8381d, 0.0001d));
      f.checkScalarApprox("csc(cast(10 as decimal))", "DOUBLE NOT NULL",
          isWithin(-1.8381d, 0.0001d));
      f.checkScalarApprox("csc(-1)", "DOUBLE NOT NULL",
          isWithin(-1.1883d, 0.0001d));
      f.checkNull("csc(cast(null as double))");
      f.checkNull("csc(cast(null as integer))");
    };
    f0.forEachLibrary(list(SqlLibrary.ALL), consumer);
  }

  @Test void testSecFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SEC);
    f0.checkFails("^sec(1)^",
        "No match found for function signature SEC\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkType("sec(cast (1 as float))", "DOUBLE NOT NULL");
      f.checkType("sec('a')", "DOUBLE NOT NULL");
      f.checkScalarApprox("sec(100)", "DOUBLE NOT NULL",
          isWithin(1.1596d, 0.0001d));
      f.checkScalarApprox("sec(cast(10 as float))", "DOUBLE NOT NULL",
          isWithin(-1.1917d, 0.0001d));
      f.checkScalarApprox("sec(cast(10 as decimal))", "DOUBLE NOT NULL",
          isWithin(-1.1917d, 0.0001d));
      f.checkScalarApprox("sec(-1)", "DOUBLE NOT NULL",
          isWithin(1.8508d, 0.0001d));
      f.checkNull("sec(cast(null as double))");
      f.checkNull("sec(cast(null as integer))");
    };
    f0.forEachLibrary(list(SqlLibrary.ALL), consumer);
  }

  @Test void testDegreesFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.DEGREES, VmName.EXPAND);
    f.checkType("degrees(1)", "DOUBLE NOT NULL");
    f.checkType("degrees(cast(1 as float))", "DOUBLE NOT NULL");
    f.checkType("degrees(case when false then 1 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^degrees('abc')^",
            "Cannot apply 'DEGREES' to arguments of type "
                + "'DEGREES\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'DEGREES\\(<NUMERIC>\\)'",
            false);
    f.checkType("degrees('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("degrees(1)", "DOUBLE NOT NULL",
        isWithin(57.2958d, 0.0001d));
    f.checkScalarApprox("degrees(cast(1 as decimal(1, 0)))", "DOUBLE NOT NULL",
        isWithin(57.2958d, 0.0001d));
    f.checkNull("degrees(cast(null as integer))");
    f.checkNull("degrees(cast(null as double))");
  }

  @Test void testFactorialFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.FACTORIAL);
    f0.checkFails("^factorial(5)^",
        "No match found for function signature FACTORIAL\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("factorial(0)", "1",
          "BIGINT");
      f.checkScalar("factorial(1)", "1",
          "BIGINT");
      f.checkScalar("factorial(5)", "120",
          "BIGINT");
      f.checkScalar("factorial(20)", "2432902008176640000",
          "BIGINT");
      f.checkNull("factorial(21)");
      f.checkNull("factorial(-1)");
      f.checkNull("factorial(cast(null as integer))");
    };
    f0.forEachLibrary(list(SqlLibrary.HIVE, SqlLibrary.SPARK), consumer);
  }

  @Test void testPiFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.PI, VmName.EXPAND);
    f.checkScalarApprox("PI", "DOUBLE NOT NULL", isWithin(3.1415d, 0.0001d));
    f.checkScalarApprox("PI()", "DOUBLE NOT NULL", isWithin(3.1415d, 0.0001d));

    // assert that PI function is not dynamic [CALCITE-2750]
    assertThat("PI operator should not be identified as dynamic function",
        PI.isDynamicFunction(), is(false));
  }

  @Test void testRadiansFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.RADIANS, VmName.EXPAND);
    f.checkType("radians(42)", "DOUBLE NOT NULL");
    f.checkType("radians(cast(42 as float))", "DOUBLE NOT NULL");
    f.checkType("radians(case when false then 42 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^radians('abc')^",
            "Cannot apply 'RADIANS' to arguments of type "
                + "'RADIANS\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'RADIANS\\(<NUMERIC>\\)'",
            false);
    f.checkType("radians('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("radians(42)", "DOUBLE NOT NULL",
        isWithin(0.7330d, 0.0001d));
    f.checkScalarApprox("radians(cast(42 as decimal(2, 0)))", "DOUBLE NOT NULL",
        isWithin(0.7330d, 0.0001d));
    f.checkNull("radians(cast(null as integer))");
    f.checkNull("radians(cast(null as double))");
  }

  @Test void testPowFunc() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.POW);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalarApprox("pow(2,3)", "DOUBLE NOT NULL", isExactly("8.0"));
      f.checkNull("pow(2, cast(null as integer))");
      f.checkNull("pow(cast(null as integer), 2)");
    };
    f0.forEachLibrary(list(SqlLibrary.BIG_QUERY, SqlLibrary.SPARK), consumer);
  }

  @Test void testInfinity() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("cast('Infinity' as double)", "Infinity",
        "DOUBLE NOT NULL");
    f.checkScalar("cast('-Infinity' as double)", "-Infinity",
        "DOUBLE NOT NULL");
    f.checkScalar("cast('Infinity' as real)", "Infinity",
        "REAL NOT NULL");
    f.checkScalar("cast('-Infinity' as real)", "-Infinity",
        "REAL NOT NULL");
  }

  @Test void testNaN() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("cast('NaN' as double)", "NaN",
        "DOUBLE NOT NULL");
    f.checkScalar("cast('NaN' as real)", "NaN",
        "REAL NOT NULL");
  }

  @Test void testIsInfFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.IS_INF);
    f0.checkFails("^is_inf(3)^",
        "No match found for function signature IS_INF\\(<NUMERIC>\\)",
        false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkBoolean("is_inf(3)", false);
    f.checkBoolean("is_inf(1.2345)", false);
    f.checkBoolean("is_inf(cast('NaN' as double))", false);
    f.checkBoolean("is_inf(cast('NaN' as real))", false);
    f.checkBoolean("is_inf(cast('Infinity' as double))", true);
    f.checkBoolean("is_inf(cast('Infinity' as float))", true);
    f.checkBoolean("is_inf(cast('Infinity' as real))", true);
    f.checkBoolean("is_inf(cast('-Infinity' as double))", true);
    f.checkBoolean("is_inf(cast('-Infinity' as float))", true);
    f.checkBoolean("is_inf(cast('-Infinity' as real))", true);
    f.checkNull("is_inf(cast(null as double))");
  }

  @Test void testIsNaNFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.IS_NAN);
    f0.checkFails("^is_nan(3)^",
        "No match found for function signature IS_NAN\\(<NUMERIC>\\)",
        false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkBoolean("is_nan(3)", false);
    f.checkBoolean("is_nan(1.2345)", false);
    f.checkBoolean("is_nan(cast('Infinity' as double))", false);
    f.checkBoolean("is_nan(cast('Infinity' as float))", false);
    f.checkBoolean("is_nan(cast('Infinity' as real))", false);
    f.checkBoolean("is_nan(cast('-Infinity' as double))", false);
    f.checkBoolean("is_nan(cast('-Infinity' as float))", false);
    f.checkBoolean("is_nan(cast('-Infinity' as real))", false);
    f.checkBoolean("is_nan(cast('NaN' as double))", true);
    f.checkBoolean("is_nan(cast('NaN' as real))", true);
    f.checkNull("is_nan(cast(null as double))");
  }

  @Test void testRoundFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ROUND, VmName.EXPAND);
    f.checkType("round(42, -1)", "INTEGER NOT NULL");
    f.checkType("round(cast(42 as float), 1)", "FLOAT NOT NULL");
    f.checkType("round(case when false then 42 else null end, -1)",
        "INTEGER");
    f.enableTypeCoercion(false)
        .checkFails("^round('abc', 'def')^",
            "Cannot apply 'ROUND' to arguments of type 'ROUND\\(<CHAR\\(3\\)>, <CHAR\\(3\\)>\\)'\\."
                + " Supported form\\(s\\): 'ROUND\\(<NUMERIC>\\)'\n"
                + "ROUND\\(<NUMERIC>, <INTEGER>\\)",
            false);
    f.checkType("round('abc', 'def')", "DECIMAL(19, 9) NOT NULL");
    f.checkScalar("round(42, -1)", 40, "INTEGER NOT NULL");
    f.checkScalar("round(cast(42.346 as decimal(5, 3)), 2)",
        BigDecimal.valueOf(4235, 2), "DECIMAL(5, 3) NOT NULL");
    f.checkScalar("round(cast(-42.346 as decimal(5, 3)), 2)",
        BigDecimal.valueOf(-4235, 2), "DECIMAL(5, 3) NOT NULL");
    f.checkNull("round(cast(null as integer), 1)");
    f.checkNull("round(cast(null as double), 1)");
    f.checkNull("round(43.21, cast(null as integer))");

    f.checkNull("round(cast(null as double))");
    f.checkScalar("round(42)", 42, "INTEGER NOT NULL");
    f.checkScalar("round(cast(42.346 as decimal(5, 3)))",
        BigDecimal.valueOf(42, 0), "DECIMAL(5, 3) NOT NULL");
    f.checkScalar("round(42.324)",
        BigDecimal.valueOf(42, 0), "DECIMAL(5, 3) NOT NULL");
    f.checkScalar("round(42.724)",
        BigDecimal.valueOf(43, 0), "DECIMAL(5, 3) NOT NULL");
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6238">
   * [CALCITE-6238] Exception while evaluating ROUND/TRUNCATE functions</a>. */
  @Test void testRoundFail() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ROUND, VmName.EXPAND);
    f.checkFails("^round(42, CAST(2 as BIGINT))^",
        "Cannot apply 'ROUND' to arguments of type 'ROUND\\(<INTEGER>, <BIGINT>\\)'\\. "
            + "Supported form\\(s\\): 'ROUND\\(<NUMERIC>\\)'\nROUND\\(<NUMERIC>, <INTEGER>\\)",
        false);
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6238">
   * [CALCITE-6238] Exception while evaluating ROUND/TRUNCATE functions</a>. */
  @Test void testTruncFail() {
    SqlOperatorFixture f = fixture();
    f = f.setFor(SqlStdOperatorTable.TRUNCATE, VmName.EXPAND)
        .setFor(SqlLibraryOperators.TRUNC_BIG_QUERY)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkFails("^truncate(42, CAST(2 as BIGINT))^",
        "Cannot apply 'TRUNCATE' to arguments of type 'TRUNCATE\\(<INTEGER>, <BIGINT>\\)'\\. "
            + "Supported form\\(s\\): 'TRUNCATE\\(<NUMERIC>\\)'\n"
            + "TRUNCATE\\(<NUMERIC>, <INTEGER>\\)",
        false);
    f.checkFails("^trunc(42, CAST(2 as BIGINT))^",
        "Cannot apply 'TRUNC' to arguments of type 'TRUNC\\(<INTEGER>, <BIGINT>\\)'\\. "
            + "Supported form\\(s\\): 'TRUNC\\(<NUMERIC>\\)'\nTRUNC\\(<NUMERIC>, <INTEGER>\\)",
        false);
  }


  @Test void testSignFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.SIGN, VmName.EXPAND);
    f.checkType("sign(1)", "INTEGER NOT NULL");
    f.checkType("sign(cast(1 as float))", "FLOAT NOT NULL");
    f.checkType("sign(case when false then 1 else null end)", "INTEGER");
    f.enableTypeCoercion(false)
        .checkFails("^sign('abc')^",
            "Cannot apply 'SIGN' to arguments of type "
                + "'SIGN\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'SIGN\\(<NUMERIC>\\)'",
            false);
    f.checkType("sign('abc')", "DECIMAL(19, 9) NOT NULL");
    f.checkScalar("sign(1)", 1, "INTEGER NOT NULL");
    f.checkScalar("sign(cast(-1 as decimal(1, 0)))",
        BigDecimal.valueOf(-1), "DECIMAL(1, 0) NOT NULL");
    f.checkScalar("sign(cast(0 as float))", 0d, "FLOAT NOT NULL");
    f.checkNull("sign(cast(null as integer))");
    f.checkNull("sign(cast(null as double))");
  }

  @Test void testSechFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SECH);
    f0.checkFails("^sech(1)^",
        "No match found for function signature SECH\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkType("sech(1)", "DOUBLE NOT NULL");
      f.checkType("sech(cast(1 as float))", "DOUBLE NOT NULL");
      f.checkType("sech(case when false then 1 else null end)", "DOUBLE");
      f.checkType("sech('abc')", "DOUBLE NOT NULL");
      f.checkScalarApprox("sech(1)", "DOUBLE NOT NULL",
          isWithin(0.6481d, 0.0001d));
      f.checkScalarApprox(" sech(cast(1 as decimal(1, 0)))", "DOUBLE NOT NULL",
          isWithin(0.6481d, 0.0001d));
      f.checkNull("sech(cast(null as integer))");
      f.checkNull("sech(cast(null as double))");
    };
    f0.forEachLibrary(list(SqlLibrary.BIG_QUERY, SqlLibrary.ORACLE), consumer);
  }

  @Test void testSinFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.SIN, VmName.EXPAND);
    f.checkType("sin(1)", "DOUBLE NOT NULL");
    f.checkType("sin(cast(1 as float))", "DOUBLE NOT NULL");
    f.checkType("sin(case when false then 1 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^sin('abc')^",
            "Cannot apply 'SIN' to arguments of type "
                + "'SIN\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'SIN\\(<NUMERIC>\\)'",
            false);
    f.checkType("sin('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("sin(1)", "DOUBLE NOT NULL",
        isWithin(0.8415d, 0.0001d));
    f.checkScalarApprox("sin(cast(1 as decimal(1, 0)))", "DOUBLE NOT NULL",
        isWithin(0.8415d, 0.0001d));
    f.checkNull("sin(cast(null as integer))");
    f.checkNull("sin(cast(null as double))");
  }

  @Test void testSindFunc() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.POSTGRESQL);
    f.setFor(SqlLibraryOperators.SIND, VmName.EXPAND);
    f.checkType("sind(30)", "DOUBLE NOT NULL");
    f.checkType("sind(cast(30 as float))", "DOUBLE NOT NULL");
    f.checkType("sind(case when false then 30 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^sind('abc')^",
            "Cannot apply 'SIND' to arguments of type "
                + "'SIND\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'SIND\\(<NUMERIC>\\)'",
            false);
    f.checkType("sind('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("sind(30)", "DOUBLE NOT NULL",
        isWithin(0.5d, 0.01d));
    f.checkScalarApprox("sind(390)", "DOUBLE NOT NULL",
        isWithin(0.5d, 0.01d));
    f.checkScalarApprox("sind(-30)", "DOUBLE NOT NULL",
        isWithin(-0.5d, 0.01d));
    f.checkScalarApprox("sind(cast(30 as decimal(2, 0)))", "DOUBLE NOT NULL",
        isWithin(0.5d, 0.01d));
    f.checkScalarExact("sin(cast('NaN' as double))", "DOUBLE NOT NULL",
        "NaN");
    f.checkScalarExact("sin(cast('Infinity' as double))", "DOUBLE NOT NULL",
        "NaN");
    f.checkNull("sind(cast(null as integer))");
    f.checkNull("sind(cast(null as double))");
  }

  @Test void testSinhFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SINH);
    f0.checkFails("^sinh(1)^",
        "No match found for function signature SINH\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkType("sinh(1)", "DOUBLE NOT NULL");
      f.checkType("sinh(cast(1 as float))", "DOUBLE NOT NULL");
      f.checkType("sinh(case when false then 1 else null end)", "DOUBLE");
      f.checkType("sinh('abc')", "DOUBLE NOT NULL");
      f.checkScalarApprox("sinh(1)", "DOUBLE NOT NULL",
          isWithin(1.1752d, 0.0001d));
      f.checkScalarApprox("sinh(cast(1 as decimal(1, 0)))", "DOUBLE NOT NULL",
          isWithin(1.1752d, 0.0001d));
      f.checkNull("sinh(cast(null as integer))");
      f.checkNull("sinh(cast(null as double))");
    };
    f0.forEachLibrary(list(SqlLibrary.ALL), consumer);
  }

  @Test void testTanFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.TAN, VmName.EXPAND);
    f.checkType("tan(1)", "DOUBLE NOT NULL");
    f.checkType("tan(cast(1 as float))", "DOUBLE NOT NULL");
    f.checkType("tan(case when false then 1 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^tan('abc')^",
            "Cannot apply 'TAN' to arguments of type "
                + "'TAN\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'TAN\\(<NUMERIC>\\)'",
            false);
    f.checkType("tan('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("tan(1)", "DOUBLE NOT NULL",
        isWithin(1.5574d, 0.0001d));
    f.checkScalarApprox("tan(cast(1 as decimal(1, 0)))", "DOUBLE NOT NULL",
        isWithin(1.5574d, 0.0001d));
    f.checkNull("tan(cast(null as integer))");
    f.checkNull("tan(cast(null as double))");
  }

  @Test void testTandFunc() {
    final SqlOperatorFixture f = fixture().withLibrary(SqlLibrary.POSTGRESQL);
    f.setFor(SqlLibraryOperators.TAND, VmName.EXPAND);
    f.checkType("tand(60)", "DOUBLE NOT NULL");
    f.checkType("tand(cast(60 as float))", "DOUBLE NOT NULL");
    f.checkType("tand(case when false then 30 else null end)", "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^tand('abc')^",
            "Cannot apply 'TAND' to arguments of type "
                + "'TAND\\(<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
                + "'TAND\\(<NUMERIC>\\)'",
            false);
    f.checkType("tand('abc')", "DOUBLE NOT NULL");
    f.checkScalarApprox("tand(60)", "DOUBLE NOT NULL",
        isWithin(1.73d, 1.74d));
    f.checkScalarApprox("cosd(420)", "DOUBLE NOT NULL",
        isWithin(1.73d, 1.74d));
    f.checkScalarApprox("cosd(-60)", "DOUBLE NOT NULL",
        isWithin(1.73d, 1.74d));
    f.checkScalarApprox("tand(cast(60 as decimal(3, 0)))", "DOUBLE NOT NULL",
        isWithin(1.73d, 1.74d));
    f.checkScalarExact("tand(cast('NaN' as double))", "DOUBLE NOT NULL",
        "NaN");
    f.checkFails("tand(cast('Infinity' as double))", "input is out of range", true);
    f.checkNull("tand(cast(null as integer))");
    f.checkNull("tand(cast(null as double))");
  }

  @Test void testTanhFunc() {
    SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.TANH);
    f0.checkFails("^tanh(1)^",
        "No match found for function signature TANH\\(<NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkType("tanh(1)", "DOUBLE NOT NULL");
      f.checkType("tanh(cast(1 as float))", "DOUBLE NOT NULL");
      f.checkType("tanh(case when false then 1 else null end)", "DOUBLE");
      f.checkType("tanh('abc')", "DOUBLE NOT NULL");
      f.checkScalarApprox("tanh(1)", "DOUBLE NOT NULL",
          isWithin(0.7615d, 0.0001d));
      f.checkScalarApprox("tanh(cast(1 as decimal(1, 0)))", "DOUBLE NOT NULL",
          isWithin(0.7615d, 0.0001d));
      f.checkNull("tanh(cast(null as integer))");
      f.checkNull("tanh(cast(null as double))");
    };
    f0.forEachLibrary(list(SqlLibrary.ALL), consumer);
  }

  @Test void testTruncFunc() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.TRUNC_BIG_QUERY)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkType("trunc(42, -1)", "DOUBLE NOT NULL");
    f.checkType("trunc(cast(42 as float), 1)", "FLOAT NOT NULL");
    f.checkType("trunc(case when false then 42 else null end, -1)",
        "DOUBLE");
    f.enableTypeCoercion(false)
        .checkFails("^trunc('abc', 'def')^",
            "Cannot apply 'TRUNC' to arguments of type 'TRUNC\\(<CHAR\\(3\\)>, <CHAR\\(3\\)>\\)'\\."
                + " Supported form\\(s\\): 'TRUNC\\(<NUMERIC>\\)'\n"
                + "TRUNC\\(<NUMERIC>, <INTEGER>\\)",
            false);
    f.checkType("trunc('abc', 'def')", "DECIMAL(19, 9) NOT NULL");
    f.checkScalar("trunc(42, -1)", 40.0, "DOUBLE NOT NULL");
    f.checkScalar("trunc(cast(42.345 as decimal(5, 3)), 2)",
        BigDecimal.valueOf(4234, 2), "DECIMAL(5, 3) NOT NULL");
    f.checkScalar("trunc(cast(-42.345 as decimal(5, 3)), 2)",
        BigDecimal.valueOf(-4234, 2), "DECIMAL(5, 3) NOT NULL");
    f.checkNull("trunc(cast(null as integer), 1)");
    f.checkNull("trunc(cast(null as double), 1)");
    f.checkNull("trunc(43.21, cast(null as integer))");

    f.checkScalar("trunc(42)", 42.0, "DOUBLE NOT NULL");
    f.checkScalar("trunc(42.324)",
        BigDecimal.valueOf(42, 0), "DECIMAL(5, 3) NOT NULL");
    f.checkScalar("trunc(cast(42.324 as float))", 42F,
        "FLOAT NOT NULL");
    f.checkScalar("trunc(cast(42.345 as decimal(5, 3)))",
        BigDecimal.valueOf(42, 0), "DECIMAL(5, 3) NOT NULL");
    f.checkNull("trunc(cast(null as integer))");
    f.checkNull("trunc(cast(null as double))");
  }

  @Test void testTruncateFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.TRUNCATE, VmName.EXPAND);
    f.checkType("truncate(42, -1)", "INTEGER NOT NULL");
    f.checkType("truncate(cast(42 as float), 1)", "FLOAT NOT NULL");
    f.checkType("truncate(case when false then 42 else null end, -1)",
        "INTEGER");
    f.enableTypeCoercion(false)
        .checkFails("^truncate('abc', 'def')^",
            "Cannot apply 'TRUNCATE' to arguments of type "
                + "'TRUNCATE\\(<CHAR\\(3\\)>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\):"
                + " 'TRUNCATE\\(<NUMERIC>\\)'\n"
                + "TRUNCATE\\(<NUMERIC>, <INTEGER>\\)",
            false);
    f.checkType("truncate('abc', 'def')", "DECIMAL(19, 9) NOT NULL");
    f.checkScalar("truncate(42, -1)", 40, "INTEGER NOT NULL");
    f.checkScalar("truncate(cast(42.345 as decimal(5, 3)), 2)",
        BigDecimal.valueOf(4234, 2), "DECIMAL(5, 3) NOT NULL");
    f.checkScalar("truncate(cast(-42.345 as decimal(5, 3)), 2)",
        BigDecimal.valueOf(-4234, 2), "DECIMAL(5, 3) NOT NULL");
    f.checkNull("truncate(cast(null as integer), 1)");
    f.checkNull("truncate(cast(null as double), 1)");
    f.checkNull("truncate(43.21, cast(null as integer))");

    f.checkScalar("truncate(42)", 42, "INTEGER NOT NULL");
    f.checkScalar("truncate(42.324)",
        BigDecimal.valueOf(42, 0), "DECIMAL(5, 3) NOT NULL");
    f.checkScalar("truncate(cast(42.324 as float))", 42F,
        "FLOAT NOT NULL");
    f.checkScalar("truncate(cast(42.345 as decimal(5, 3)))",
        BigDecimal.valueOf(42, 0), "DECIMAL(5, 3) NOT NULL");
    f.checkNull("truncate(cast(null as integer))");
    f.checkNull("truncate(cast(null as double))");
  }

  @Test void testSafeAddFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SAFE_ADD);
    f0.checkFails("^safe_add(2, 3)^",
        "No match found for function signature "
        + "SAFE_ADD\\(<NUMERIC>, <NUMERIC>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    // Basic test for each of the 9 2-permutations of BIGINT, DECIMAL, and FLOAT
    f.checkScalar("safe_add(cast(20 as bigint), cast(20 as bigint))",
        "40", "BIGINT");
    f.checkScalar("safe_add(cast(20 as bigint), cast(1.2345 as decimal(5,4)))",
        "21.2345", "DECIMAL(19, 4)");
    f.checkScalar("safe_add(cast(1.2345 as decimal(5,4)), cast(20 as bigint))",
        "21.2345", "DECIMAL(19, 4)");
    f.checkScalar("safe_add(cast(1.2345 as decimal(5,4)), cast(2.0 as decimal(2, 1)))",
        "3.2345", "DECIMAL(6, 4)");
    f.checkScalar("safe_add(cast(3 as double), cast(3 as bigint))",
        "6.0", "DOUBLE");
    f.checkScalar("safe_add(cast(3 as bigint), cast(3 as double))",
        "6.0", "DOUBLE");
    f.checkScalar("safe_add(cast(3 as double), cast(1.2345 as decimal(5, 4)))",
        "4.2345", "DOUBLE");
    f.checkScalar("safe_add(cast(1.2345 as decimal(5, 4)), cast(3 as double))",
        "4.2345", "DOUBLE");
    f.checkScalar("safe_add(cast(3 as double), cast(3 as double))",
        "6.0", "DOUBLE");
    // Tests for + and - Infinity
    f.checkScalar("safe_add(cast('Infinity' as double), cast(3 as double))",
        "Infinity", "DOUBLE");
    f.checkScalar("safe_add(cast('-Infinity' as double), cast(3 as double))",
        "-Infinity", "DOUBLE");
    f.checkScalar("safe_add(cast('-Infinity' as double), "
        + "cast('Infinity' as double))", "NaN", "DOUBLE");
    // Tests for NaN
    f.checkScalar("safe_add(cast('NaN' as double), cast(3 as bigint))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_add(cast('NaN' as double), cast(1.23 as decimal(3, 2)))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_add(cast('NaN' as double), cast('Infinity' as double))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_add(cast(3 as bigint), cast('NaN' as double))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_add(cast(1.23 as decimal(3, 2)), cast('NaN' as double))",
        "NaN", "DOUBLE");
    // Overflow test for each pairing
    f.checkNull("safe_add(cast(20 as bigint), "
        + "cast(9223372036854775807 as bigint))");
    f.checkNull("safe_add(cast(-20 as bigint), "
        + "cast(-9223372036854775807 as bigint))");
    if (Bug.CALCITE_6328_FIXED) {
      f.checkNull("safe_add(9, cast(9.999999999999999999e75 as DECIMAL(38, 19)))");
      f.checkNull("safe_add(-9, cast(-9.999999999999999999e75 as DECIMAL(38, 19)))");
      f.checkNull("safe_add(cast(9.999999999999999999e75 as DECIMAL(38, 19)), 9)");
      f.checkNull("safe_add(cast(-9.999999999999999999e75 as DECIMAL(38, 19)), -9)");
      f.checkNull("safe_add(cast(9.9e75 as DECIMAL(76, 0)), "
          + "cast(9.9e75 as DECIMAL(76, 0)))");
      f.checkNull("safe_add(cast(-9.9e75 as DECIMAL(76, 0)), "
          + "cast(-9.9e75 as DECIMAL(76, 0)))");
    }
    f.checkNull("safe_add(cast(1.7976931348623157e308 as double), "
        + "cast(9.9e7 as decimal(76, 0)))");
    f.checkNull("safe_add(cast(-1.7976931348623157e308 as double), "
        + "cast(-9.9e7 as decimal(76, 0)))");
    f.checkNull("safe_add(cast(9.9e7 as decimal(76, 0)), "
        + "cast(1.7976931348623157e308 as double))");
    f.checkNull("safe_add(cast(-9.9e7 as decimal(76, 0)), "
        + "cast(-1.7976931348623157e308 as double))");
    f.checkNull("safe_add(cast(1.7976931348623157e308 as double), cast(3 as bigint))");
    f.checkNull("safe_add(cast(-1.7976931348623157e308 as double), "
        + "cast(-3 as bigint))");
    f.checkNull("safe_add(cast(3 as bigint), cast(1.7976931348623157e308 as double))");
    f.checkNull("safe_add(cast(-3 as bigint), "
        + "cast(-1.7976931348623157e308 as double))");
    f.checkNull("safe_add(cast(3 as double), cast(1.7976931348623157e308 as double))");
    f.checkNull("safe_add(cast(-3 as double), "
        + "cast(-1.7976931348623157e308 as double))");
    // Check that null argument retuns null
    f.checkNull("safe_add(cast(null as double), cast(3 as bigint))");
    f.checkNull("safe_add(cast(3 as double), cast(null as bigint))");
  }

  @Test void testSafeDivideFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SAFE_DIVIDE);
    f0.checkFails("^safe_divide(2, 3)^",
        "No match found for function signature "
        + "SAFE_DIVIDE\\(<NUMERIC>, <NUMERIC>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    // Basic test for each of the 9 2-permutations of BIGINT, DECIMAL, and FLOAT
    f.checkScalar("safe_divide(cast(2 as bigint), cast(4 as bigint))",
        "0.5", "DOUBLE");
    f.checkScalar("safe_divide(cast(15 as bigint), cast(1.2 as decimal(2,1)))",
        "12.5", "DECIMAL(19, 6)");
    f.checkScalar("safe_divide(cast(4.5 as decimal(2,1)), cast(3 as bigint))",
        "1.5", "DECIMAL(19, 6)");
    f.checkScalar("safe_divide(cast(4.5 as decimal(2,1)), "
        + "cast(1.5 as decimal(2, 1)))", "3", "DECIMAL(8, 6)");
    f.checkScalar("safe_divide(cast(3 as double), cast(3 as bigint))",
        "1.0", "DOUBLE");
    f.checkScalar("safe_divide(cast(3 as bigint), cast(3 as double))",
        "1.0", "DOUBLE");
    f.checkScalar("safe_divide(cast(3 as double), cast(1.5 as decimal(5, 4)))",
        "2.0", "DOUBLE");
    f.checkScalar("safe_divide(cast(1.5 as decimal(5, 4)), cast(3 as double))",
        "0.5", "DOUBLE");
    f.checkScalar("safe_divide(cast(3 as double), cast(3 as double))",
        "1.0", "DOUBLE");
    // Tests for + and - Infinity
    f.checkScalar("safe_divide(cast('Infinity' as double), cast(3 as double))",
        "Infinity", "DOUBLE");
    f.checkScalar("safe_divide(cast('-Infinity' as double), cast(3 as double))",
        "-Infinity", "DOUBLE");
    f.checkScalar("safe_divide(cast('-Infinity' as double), "
        + "cast('Infinity' as double))", "NaN", "DOUBLE");
    // Tests for NaN
    f.checkScalar("safe_divide(cast('NaN' as double), cast(3 as bigint))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_divide(cast('NaN' as double), cast(1.23 as decimal(3, 2)))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_divide(cast('NaN' as double), cast('Infinity' as double))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_divide(cast(3 as bigint), cast('NaN' as double))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_divide(cast(1.23 as decimal(3, 2)), cast('NaN' as double))",
        "NaN", "DOUBLE");
    f.checkNull("safe_divide(cast(0 as bigint), cast(0 as bigint))");
    f.checkNull("safe_divide(cast(0 as bigint), cast(0 as double))");
    f.checkNull("safe_divide(cast(0 as bigint), cast(0 as decimal(1, 0)))");
    f.checkNull("safe_divide(cast(0 as double), cast(0 as bigint))");
    f.checkNull("safe_divide(cast(0 as double), cast(0 as double))");
    f.checkNull("safe_divide(cast(0 as double), cast(0 as decimal(1, 0)))");
    if (Bug.CALCITE_6328_FIXED) {
      f.checkNull("safe_divide(cast(1.5 as decimal(2, 1)), cast(0 as bigint))");
      f.checkNull("safe_divide(cast(1.5 as decimal(2, 1)), cast(0 as double))");
      f.checkNull("safe_divide(cast(1.5 as decimal(2, 1)), cast(0 as decimal(1, 0)))");
      // Overflow test for each pairing
      f.checkNull("safe_divide(cast(10 as bigint), cast(3.5e-75 as DECIMAL(76, 0)))");
      f.checkNull("safe_divide(cast(10 as bigint), cast(-3.5e75 as DECIMAL(76, 0)))");
      f.checkNull("safe_divide(cast(3.5e75 as DECIMAL(76, 0)), "
          + "cast(1.5 as DECIMAL(2, 1)))");
      f.checkNull("safe_divide(cast(-3.5e75 as DECIMAL(76, 0)), "
          + "cast(1.5 as DECIMAL(2, 1)))");
      f.checkNull("safe_divide(cast(5e20 as decimal(1, 0)), cast(1.7e-309 as double))");
      f.checkNull("safe_divide(cast(5e20 as decimal(1, 0)), cast(-1.7e-309 as double))");
    }
    f.checkNull("safe_divide(cast(1.7e308 as double), cast(0.5 as decimal(3, 2)))");
    f.checkNull("safe_divide(cast(-1.7e308 as double), cast(0.5 as decimal(2, 1)))");
    f.checkNull("safe_divide(cast(3 as bigint), cast(1.7e-309 as double))");
    f.checkNull("safe_divide(cast(3 as bigint), cast(-1.7e-309 as double))");
    f.checkNull("safe_divide(cast(3 as double), cast(1.7e-309 as double))");
    f.checkNull("safe_divide(cast(3 as double), cast(-1.7e-309 as double))");
    // Check that null argument retuns null
    f.checkNull("safe_divide(cast(null as double), cast(3 as bigint))");
    f.checkNull("safe_divide(cast(3 as double), cast(null as bigint))");
  }

  @Test void testSafeMultiplyFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SAFE_MULTIPLY);
    f0.checkFails("^safe_multiply(2, 3)^",
        "No match found for function signature "
        + "SAFE_MULTIPLY\\(<NUMERIC>, <NUMERIC>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    // Basic test for each of the 9 2-permutations of BIGINT, DECIMAL, and FLOAT
    f.checkScalar("safe_multiply(cast(20 as bigint), cast(20 as bigint))",
        "400", "BIGINT");
    f.checkScalar("safe_multiply(cast(20 as bigint), cast(1.2345 as decimal(5,4)))",
        "24.6900", "DECIMAL(19, 4)");
    f.checkScalar("safe_multiply(cast(1.2345 as decimal(5,4)), cast(20 as bigint))",
        "24.6900", "DECIMAL(19, 4)");
    f.checkScalar("safe_multiply(cast(1.2345 as decimal(5,4)), "
        + "cast(2.0 as decimal(2, 1)))", "2.46900", "DECIMAL(7, 5)");
    f.checkScalar("safe_multiply(cast(3 as double), cast(3 as bigint))",
        "9.0", "DOUBLE");
    f.checkScalar("safe_multiply(cast(3 as bigint), cast(3 as double))",
        "9.0", "DOUBLE");
    f.checkScalar("safe_multiply(cast(3 as double), cast(1.2345 as decimal(5, 4)))",
        "3.7035", "DOUBLE");
    f.checkScalar("safe_multiply(cast(1.2345 as decimal(5, 4)), cast(3 as double))",
        "3.7035", "DOUBLE");
    f.checkScalar("safe_multiply(cast(3 as double), cast(3 as double))",
        "9.0", "DOUBLE");
    // Tests for + and - Infinity
    f.checkScalar("safe_multiply(cast('Infinity' as double), cast(3 as double))",
        "Infinity", "DOUBLE");
    f.checkScalar("safe_multiply(cast('-Infinity' as double), cast(3 as double))",
        "-Infinity", "DOUBLE");
    f.checkScalar("safe_multiply(cast('-Infinity' as double), "
        + "cast('Infinity' as double))", "-Infinity", "DOUBLE");
    // Tests for NaN
    f.checkScalar("safe_multiply(cast('NaN' as double), cast(3 as bigint))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_multiply(cast('NaN' as double), cast(1.23 as decimal(3, 2)))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_multiply(cast('NaN' as double), cast('Infinity' as double))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_multiply(cast(3 as bigint), cast('NaN' as double))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_multiply(cast(1.23 as decimal(3, 2)), cast('NaN' as double))",
        "NaN", "DOUBLE");
    // Overflow test for each pairing
    f.checkNull("safe_multiply(cast(20 as bigint), "
        + "cast(9223372036854775807 as bigint))");
    f.checkNull("safe_multiply(cast(20 as bigint), "
        + "cast(-9223372036854775807 as bigint))");
    if (Bug.CALCITE_6328_FIXED) {
      f.checkNull("safe_multiply(cast(10 as bigint), cast(3.5e75 as DECIMAL(76, 0)))");
      f.checkNull("safe_multiply(cast(10 as bigint), cast(-3.5e75 as DECIMAL(76, 0)))");
      f.checkNull("safe_multiply(cast(3.5e75 as DECIMAL(76, 0)), cast(10 as bigint))");
      f.checkNull("safe_multiply(cast(-3.5e75 as DECIMAL(76, 0)), cast(10 as bigint))");
      f.checkNull("safe_multiply(cast(3.5e75 as DECIMAL(76, 0)), "
          + "cast(1.5 as DECIMAL(2, 1)))");
      f.checkNull("safe_multiply(cast(-3.5e75 as DECIMAL(76, 0)), "
          + "cast(1.5 as DECIMAL(2, 1)))");
    }
    f.checkNull("safe_multiply(cast(1.7e308 as double), cast(1.23 as decimal(3, 2)))");
    f.checkNull("safe_multiply(cast(-1.7e308 as double), cast(1.2 as decimal(2, 1)))");
    f.checkNull("safe_multiply(cast(1.2 as decimal(2, 1)), cast(1.7e308 as double))");
    f.checkNull("safe_multiply(cast(1.2 as decimal(2, 1)), cast(-1.7e308 as double))");
    f.checkNull("safe_multiply(cast(1.7e308 as double), cast(3 as bigint))");
    f.checkNull("safe_multiply(cast(-1.7e308 as double), cast(3 as bigint))");
    f.checkNull("safe_multiply(cast(3 as bigint), cast(1.7e308 as double))");
    f.checkNull("safe_multiply(cast(3 as bigint), cast(-1.7e308 as double))");
    f.checkNull("safe_multiply(cast(3 as double), cast(1.7e308 as double))");
    f.checkNull("safe_multiply(cast(3 as double), cast(-1.7e308 as double))");
    // Check that null argument retuns null
    f.checkNull("safe_multiply(cast(null as double), cast(3 as bigint))");
    f.checkNull("safe_multiply(cast(3 as double), cast(null as bigint))");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5770">[CALCITE-5770]
   * Add SAFE_NEGATE function (enabled in BigQuery library)</a>.
   */
  @Test void testSafeNegateFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SAFE_NEGATE);
    f0.checkFails("^safe_negate(2)^",
        "No match found for function signature "
        + "SAFE_NEGATE\\(<NUMERIC>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("safe_negate(cast(20 as bigint))", "-20",
        "BIGINT");
    f.checkScalar("safe_negate(cast(-20 as bigint))", "20",
        "BIGINT");
    f.checkScalar("safe_negate(cast(1.5 as decimal(2, 1)))", "-1.5",
        "DECIMAL(2, 1)");
    f.checkScalar("safe_negate(cast(-1.5 as decimal(2, 1)))", "1.5",
        "DECIMAL(2, 1)");
    f.checkScalar("safe_negate(cast(12.3456 as double))", "-12.3456",
        "DOUBLE");
    f.checkScalar("safe_negate(cast(-12.3456 as double))", "12.3456",
        "DOUBLE");
    // Infinity and NaN tests
    f.checkScalar("safe_negate(cast('Infinity' as double))",
        "-Infinity", "DOUBLE");
    f.checkScalar("safe_negate(cast('-Infinity' as double))",
        "Infinity", "DOUBLE");
    f.checkScalar("safe_negate(cast('NaN' as double))",
        "NaN", "DOUBLE");
    // Null cases are rarer for SAFE_NEGATE
    f.checkNull("safe_negate(-9223372036854775808)");
    f.checkNull("safe_negate(-1 + -9223372036854775807)");
    f.checkNull("safe_negate(cast(null as bigint))");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5770">[CALCITE-5770]
   * Add SAFE_SUBTRACT function (enabled in BigQuery library)</a>.
   */
  @Test void testSafeSubtractFunc() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SAFE_SUBTRACT);
    f0.checkFails("^safe_subtract(2, 3)^",
        "No match found for function signature "
        + "SAFE_SUBTRACT\\(<NUMERIC>, <NUMERIC>\\)", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    // Basic test for each of the 9 2-permutations of BIGINT, DECIMAL, and FLOAT
    f.checkScalar("safe_subtract(cast(20 as bigint), cast(20 as bigint))",
        "0", "BIGINT");
    f.checkScalar("safe_subtract(cast(20 as bigint), cast(-1.2345 as decimal(5,4)))",
        "21.2345", "DECIMAL(19, 4)");
    f.checkScalar("safe_subtract(cast(1.2345 as decimal(5,4)), cast(-20 as bigint))",
        "21.2345", "DECIMAL(19, 4)");
    f.checkScalar("safe_subtract(cast(1.23 as decimal(3,2)), "
        + "cast(-2.0 as decimal(2, 1)))", "3.23", "DECIMAL(4, 2)");
    f.checkScalar("safe_subtract(cast(3 as double), cast(-3 as bigint))",
        "6.0", "DOUBLE");
    f.checkScalar("safe_subtract(cast(3 as bigint), cast(-3 as double))",
        "6.0", "DOUBLE");
    f.checkScalar("safe_subtract(cast(3 as double), cast(-1.2345 as decimal(5, 4)))",
        "4.2345", "DOUBLE");
    f.checkScalar("safe_subtract(cast(1.2345 as decimal(5, 4)), cast(-3 as double))",
        "4.2345", "DOUBLE");
    f.checkScalar("safe_subtract(cast(3 as double), cast(3 as double))",
        "0.0", "DOUBLE");
    // Tests for + and - Infinity
    f.checkScalar("safe_subtract(cast('Infinity' as double), cast(3 as double))",
        "Infinity", "DOUBLE");
    f.checkScalar("safe_subtract(cast('-Infinity' as double), cast(3 as double))",
        "-Infinity", "DOUBLE");
    f.checkScalar("safe_subtract(cast('Infinity' as double), "
        + "cast('Infinity' as double))", "NaN", "DOUBLE");
    // Tests for NaN
    f.checkScalar("safe_subtract(cast('NaN' as double), cast(3 as bigint))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_subtract(cast('NaN' as double), cast(1.23 as decimal(3, 2)))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_subtract(cast('NaN' as double), cast('Infinity' as double))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_subtract(cast(3 as bigint), cast('NaN' as double))",
        "NaN", "DOUBLE");
    f.checkScalar("safe_subtract(cast(1.23 as decimal(3, 2)), cast('NaN' as double))",
        "NaN", "DOUBLE");
    // Overflow test for each pairing
    f.checkNull("safe_subtract(cast(20 as bigint), "
        + "cast(-9223372036854775807 as bigint))");
    f.checkNull("safe_subtract(cast(-20 as bigint), "
        + "cast(9223372036854775807 as bigint))");
    if (Bug.CALCITE_6328_FIXED) {
      f.checkNull("safe_subtract(9, cast(-9.999999999999999999e75 as DECIMAL(38, 19)))");
      f.checkNull("safe_subtract(-9, cast(9.999999999999999999e75 as DECIMAL(38, 19)))");
      f.checkNull("safe_subtract(cast(-9.999999999999999999e75 as DECIMAL(38, 19)), 9)");
      f.checkNull("safe_subtract(cast(9.999999999999999999e75 as DECIMAL(38, 19)), -9)");
      f.checkNull("safe_subtract(cast(-9.9e75 as DECIMAL(76, 0)), "
          + "cast(9.9e75 as DECIMAL(76, 0)))");
      f.checkNull("safe_subtract(cast(9.9e75 as DECIMAL(76, 0)), "
          + "cast(-9.9e75 as DECIMAL(76, 0)))");
    }
    f.checkNull("safe_subtract(cast(1.7976931348623157e308 as double), "
        + "cast(-9.9e7 as decimal(76, 0)))");
    f.checkNull("safe_subtract(cast(-1.7976931348623157e308 as double), "
        + "cast(9.9e7 as decimal(76, 0)))");
    f.checkNull("safe_subtract(cast(9.9e7 as decimal(76, 0)), "
        + "cast(-1.7976931348623157e308 as double))");
    f.checkNull("safe_subtract(cast(-9.9e7 as decimal(76, 0)), "
        + "cast(1.7976931348623157e308 as double))");
    f.checkNull("safe_subtract(cast(1.7976931348623157e308 as double), "
        + "cast(-3 as bigint))");
    f.checkNull("safe_subtract(cast(-1.7976931348623157e308 as double), "
        + "cast(3 as bigint))");
    f.checkNull("safe_subtract(cast(3 as bigint), "
        + "cast(-1.7976931348623157e308 as double))");
    f.checkNull("safe_subtract(cast(-3 as bigint), "
        + "cast(1.7976931348623157e308 as double))");
    f.checkNull("safe_subtract(cast(3 as double), "
        + "cast(-1.7976931348623157e308 as double))");
    f.checkNull("safe_subtract(cast(-3 as double), "
        + "cast(1.7976931348623157e308 as double))");
    // Check that null argument retuns null
    f.checkNull("safe_subtract(cast(null as double), cast(3 as bigint))");
    f.checkNull("safe_subtract(cast(3 as double), cast(null as bigint))");
  }

  @Test void testNullifFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.NULLIF, VM_EXPAND);
    f.checkNull("nullif(1,1)");
    f.checkScalarExact("nullif(1.5, 13.56)", "DECIMAL(2, 1)", "1.5");
    f.checkScalarExact("nullif(13.56, 1.5)", "DECIMAL(4, 2)", "13.56");
    f.checkScalarExact("nullif(1.5, 3)", "DECIMAL(2, 1)", "1.5");
    f.checkScalarExact("nullif(3, 1.5)", "INTEGER", "3");
    f.checkScalarApprox("nullif(1.5e0, 3e0)", "DOUBLE", isExactly("1.5"));
    f.checkScalarApprox("nullif(1.5, cast(3e0 as REAL))", "DECIMAL(2, 1)",
        isExactly("1.5"));
    f.checkScalarExact("nullif(3, 1.5e0)", "INTEGER", "3");
    f.checkScalarExact("nullif(3, cast(1.5e0 as REAL))", "INTEGER", "3");
    f.checkScalarApprox("nullif(1.5e0, 3.4)", "DOUBLE", isExactly("1.5"));
    f.checkScalarExact("nullif(3.4, 1.5e0)", "DECIMAL(2, 1)", "3.4");
    f.checkString("nullif('a','bc')", "a", "CHAR(1)");
    f.checkString("nullif('a',cast(null as varchar(1)))", "a", "CHAR(1)");
    f.checkNull("nullif(cast(null as varchar(1)),'a')");
    f.checkNull("nullif(cast(null as numeric(4,3)), 4.3)");

    // Error message reflects the fact that Nullif is expanded before it is
    // validated (like a C macro). Not perfect, but good enough.
    f.checkFails("1 + ^nullif(1, date '2005-8-4')^ + 2",
        "(?s)Cannot apply '=' to arguments of type '<INTEGER> = <DATE>'\\..*",
        false);

    f.checkFails("1 + ^nullif(1, 2, 3)^ + 2",
        "Invalid number of arguments to function 'NULLIF'\\. "
            + "Was expecting 2 arguments",
        false);
  }

  @Test void testNullIfOperatorIntervals() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("nullif(interval '2' month, interval '3' year)", "+2",
        "INTERVAL MONTH");
    f.checkScalar("nullif(interval '2 5' day to hour,"
            + " interval '5' second)",
        "+2 05", "INTERVAL DAY TO HOUR");
    f.checkNull("nullif(interval '3' day, interval '3' day)");
  }

  @Test void testCoalesceFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.COALESCE, VM_EXPAND);
    f.checkString("coalesce('a','b')", "a", "CHAR(1) NOT NULL");
    f.checkScalarExact("coalesce(null,null,3)", 3);
    f.enableTypeCoercion(false)
        .checkFails("1 + ^coalesce('a', 'b', 1, null)^ + 2",
            "Illegal mixing of types in CASE or COALESCE statement",
            false);
    f.checkType("1 + coalesce('a', 'b', 1, null) + 2",
        "INTEGER");
  }

  @Test void testUserFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.USER);
    f.checkString("USER", "sa", "VARCHAR(2000) NOT NULL");
  }

  @Test void testCurrentUserFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CURRENT_USER);
    f.checkString("CURRENT_USER", "sa", "VARCHAR(2000) NOT NULL");
  }

  @Test void testSessionUserFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.SESSION_USER);
    f.checkString("SESSION_USER", "sa", "VARCHAR(2000) NOT NULL");
  }

  @Test void testSystemUserFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.SYSTEM_USER);
    String user = System.getProperty("user.name"); // e.g. "jhyde"
    f.checkString("SYSTEM_USER", user, "VARCHAR(2000) NOT NULL");
  }

  @Test void testCurrentPathFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CURRENT_PATH);
    f.checkString("CURRENT_PATH", "", "VARCHAR(2000) NOT NULL");
  }

  @Test void testCurrentRoleFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CURRENT_ROLE);
    // By default, the CURRENT_ROLE function returns
    // the empty string because a role has to be set explicitly.
    f.checkString("CURRENT_ROLE", "", "VARCHAR(2000) NOT NULL");
  }

  @Test void testCurrentCatalogFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CURRENT_CATALOG);
    // By default, the CURRENT_CATALOG function returns
    // the empty string because a catalog has to be set explicitly.
    f.checkString("CURRENT_CATALOG", "", "VARCHAR(2000) NOT NULL");
  }

  @Tag("slow")
  @Test void testLocalTimeFuncWithCurrentTime() {
    testLocalTimeFunc(currentTimeString(LOCAL_TZ));
  }

  @Test void testLocalTimeFuncWithFixedTime() {
    testLocalTimeFunc(fixedTimeString(LOCAL_TZ));
  }

  private void testLocalTimeFunc(Pair<String, Hook.Closeable> pair) {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LOCALTIME, VmName.EXPAND);
    f.checkScalar("LOCALTIME", TIME_PATTERN, "TIME(0) NOT NULL");
    f.checkFails("^LOCALTIME()^",
        "No match found for function signature LOCALTIME\\(\\)",
        false);
    f.checkScalar("LOCALTIME(1)", TIME_PATTERN, "TIME(1) NOT NULL");

    f.checkScalar("CAST(LOCALTIME AS VARCHAR(30))",
        Pattern.compile(pair.left.substring(11) + "[0-9][0-9]:[0-9][0-9]"),
        "VARCHAR(30) NOT NULL");
    f.checkScalar("LOCALTIME",
        Pattern.compile(pair.left.substring(11) + "[0-9][0-9]:[0-9][0-9]"),
        "TIME(0) NOT NULL");
    pair.right.close();
  }

  @Tag("slow")
  @Test void testLocalTimestampFuncWithCurrentTime() {
    testLocalTimestampFunc(currentTimeString(LOCAL_TZ));
  }

  @Test void testLocalTimestampFuncWithFixedTime() {
    testLocalTimestampFunc(fixedTimeString(LOCAL_TZ));
  }

  private void testLocalTimestampFunc(Pair<String, Hook.Closeable> pair) {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LOCALTIMESTAMP, VmName.EXPAND);
    f.checkScalar("LOCALTIMESTAMP", TIMESTAMP_PATTERN,
        "TIMESTAMP(0) NOT NULL");
    f.checkFails("^LOCALTIMESTAMP()^",
        "No match found for function signature LOCALTIMESTAMP\\(\\)",
        false);
    f.checkFails("^LOCALTIMESTAMP(4000000000)^",
        LITERAL_OUT_OF_RANGE_MESSAGE, false);
    f.checkFails("^LOCALTIMESTAMP(9223372036854775807)^",
        LITERAL_OUT_OF_RANGE_MESSAGE, false);
    f.checkScalar("LOCALTIMESTAMP(1)", TIMESTAMP_PATTERN,
        "TIMESTAMP(1) NOT NULL");

    // Check that timestamp is being generated in the right timezone by
    // generating a specific timestamp.
    f.checkScalar("CAST(LOCALTIMESTAMP AS VARCHAR(30))",
        Pattern.compile(pair.left + "[0-9][0-9]:[0-9][0-9]"),
        "VARCHAR(30) NOT NULL");
    f.checkScalar("LOCALTIMESTAMP",
        Pattern.compile(pair.left + "[0-9][0-9]:[0-9][0-9]"),
        "TIMESTAMP(0) NOT NULL");
    pair.right.close();
  }

  @Tag("slow")
  @Test void testCurrentTimeFuncWithCurrentTime() {
    testCurrentTimeFunc(currentTimeString(CURRENT_TZ));
  }

  @Test void testCurrentTimeFuncWithFixedTime() {
    testCurrentTimeFunc(fixedTimeString(CURRENT_TZ));
  }

  private void testCurrentTimeFunc(Pair<String, Hook.Closeable> pair) {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CURRENT_TIME, VmName.EXPAND);
    f.checkScalar("CURRENT_TIME", TIME_PATTERN, "TIME(0) NOT NULL");
    f.checkFails("^CURRENT_TIME()^",
        "No match found for function signature CURRENT_TIME\\(\\)",
        false);
    f.checkScalar("CURRENT_TIME(1)", TIME_PATTERN, "TIME(1) NOT NULL");

    f.checkScalar("CAST(CURRENT_TIME AS VARCHAR(30))",
        Pattern.compile(pair.left.substring(11) + "[0-9][0-9]:[0-9][0-9]"),
        "VARCHAR(30) NOT NULL");
    f.checkScalar("CURRENT_TIME",
        Pattern.compile(pair.left.substring(11) + "[0-9][0-9]:[0-9][0-9]"),
        "TIME(0) NOT NULL");
    pair.right.close();
  }

  @Tag("slow")
  @Test void testCurrentTimestampFuncWithCurrentTime() {
    testCurrentTimestampFunc(currentTimeString(CURRENT_TZ));
  }

  @Test void testCurrentTimestampFuncWithFixedTime() {
    testCurrentTimestampFunc(fixedTimeString(CURRENT_TZ));
  }

  private void testCurrentTimestampFunc(Pair<String, Hook.Closeable> pair) {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CURRENT_TIMESTAMP,
        VmName.EXPAND);
    f.checkScalar("CURRENT_TIMESTAMP", TIMESTAMP_PATTERN,
        "TIMESTAMP(0) NOT NULL");
    f.checkFails("^CURRENT_TIMESTAMP()^",
        "No match found for function signature CURRENT_TIMESTAMP\\(\\)",
        false);
    f.checkFails("^CURRENT_TIMESTAMP(4000000000)^",
        LITERAL_OUT_OF_RANGE_MESSAGE, false);
    f.checkScalar("CURRENT_TIMESTAMP(1)", TIMESTAMP_PATTERN,
        "TIMESTAMP(1) NOT NULL");

    f.checkScalar("CAST(CURRENT_TIMESTAMP AS VARCHAR(30))",
        Pattern.compile(pair.left + "[0-9][0-9]:[0-9][0-9]"),
        "VARCHAR(30) NOT NULL");
    f.checkScalar("CURRENT_TIMESTAMP",
        Pattern.compile(pair.left + "[0-9][0-9]:[0-9][0-9]"),
        "TIMESTAMP(0) NOT NULL");
    pair.right.close();
  }

  @Tag("slow")
  @Test void testSysTimestampFuncWithCurrentTime() {
    testSysTimestampFunc(currentTimeString(CURRENT_TZ));
  }

  @Test void testSysTimestampFuncWithFixedTime() {
    testSysTimestampFunc(fixedTimeString(CURRENT_TZ));
  }

  private void testSysTimestampFunc(Pair<String, Hook.Closeable> pair) {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.SYSTIMESTAMP, VmName.EXPAND)
        .withLibrary(SqlLibrary.ORACLE);

    f.checkType("SYSTIMESTAMP", "TIMESTAMP_TZ(0) NOT NULL");
    f.checkFails("^SYSTIMESTAMP()^",
        "No match found for function signature SYSTIMESTAMP\\(\\)",
        false);
    // check systimestamp function within cast function
    f.checkType("CAST(SYSTIMESTAMP AS VARCHAR(30))", "VARCHAR(30) NOT NULL");
    f.checkFails("CAST(^SYSTIMESTAMP()^ AS VARCHAR(30))",
        "No match found for function signature SYSTIMESTAMP\\(\\)",
        false);
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
    final Calendar calendar = getCalendarNotTooNear(Calendar.HOUR_OF_DAY);
    final Hook.Closeable closeable = () -> { };
    return Pair.of(toTimeString(tz, calendar), closeable);
  }

  private static Pair<String, Hook.Closeable> fixedTimeString(TimeZone tz) {
    final Calendar calendar = getFixedCalendar();
    final long timeInMillis = calendar.getTimeInMillis();
    final Consumer<Holder<Long>> consumer = o -> o.set(timeInMillis);
    final Hook.Closeable closeable = Hook.CURRENT_TIME.addThread(consumer);
    return Pair.of(toTimeString(tz, calendar), closeable);
  }

  private static String toTimeString(TimeZone tz, Calendar cal) {
    SimpleDateFormat sdf = getDateFormatter("yyyy-MM-dd HH:", tz);
    return sdf.format(cal.getTime());
  }

  @Tag("slow")
  @Test void testCurrentDateFuncWithCurrentTime() {
    testCurrentDateFunc(currentTimeString(LOCAL_TZ));
  }

  @Test void testCurrentDateFuncWithFixedTime() {
    testCurrentDateFunc(fixedTimeString(LOCAL_TZ));
  }

  private void testCurrentDateFunc(Pair<String, Hook.Closeable> pair) {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CURRENT_DATE);

    // A tester with a lenient conformance that allows parentheses.
    final SqlOperatorFixture f1 = f.withConformance(SqlConformanceEnum.LENIENT);

    f.checkScalar("CURRENT_DATE", DATE_PATTERN, "DATE NOT NULL");
    f.checkScalar(
        "(CURRENT_DATE - CURRENT_DATE) DAY",
        "+0",
        "INTERVAL DAY NOT NULL");
    f.checkBoolean("CURRENT_DATE IS NULL", false);
    f.checkBoolean("CURRENT_DATE IS NOT NULL", true);
    f.checkBoolean("NOT (CURRENT_DATE IS NULL)", true);
    f.checkFails("^CURRENT_DATE()^",
        "No match found for function signature CURRENT_DATE\\(\\)",
        false);

    f1.checkBoolean("CURRENT_DATE() IS NULL", false);
    f1.checkBoolean("CURRENT_DATE IS NOT NULL", true);
    f1.checkBoolean("NOT (CURRENT_DATE() IS NULL)", true);
    f1.checkType("CURRENT_DATE", "DATE NOT NULL");
    f1.checkType("CURRENT_DATE()", "DATE NOT NULL");
    f1.checkType("CURRENT_TIMESTAMP()", "TIMESTAMP(0) NOT NULL");
    f1.checkType("CURRENT_TIME()", "TIME(0) NOT NULL");

    // Check the actual value.
    final String dateString = pair.left;
    try (Hook.Closeable ignore = pair.right) {
      f.checkScalar("CAST(CURRENT_DATE AS VARCHAR(30))",
          dateString.substring(0, 10),
          "VARCHAR(30) NOT NULL");
      f.checkScalar("CURRENT_DATE",
          dateString.substring(0, 10),
          "DATE NOT NULL");

      f1.checkScalar("CAST(CURRENT_DATE AS VARCHAR(30))",
          dateString.substring(0, 10),
          "VARCHAR(30) NOT NULL");
      f1.checkScalar("CAST(CURRENT_DATE() AS VARCHAR(30))",
          dateString.substring(0, 10),
          "VARCHAR(30) NOT NULL");
      f1.checkScalar("CURRENT_DATE",
          dateString.substring(0, 10),
          "DATE NOT NULL");
      f1.checkScalar("CURRENT_DATE()",
          dateString.substring(0, 10),
          "DATE NOT NULL");
    }
  }

  @Tag("slow")
  @Test void testSysDateFuncWithCurrentTime() {
    testSysDateFunc(currentTimeString(LOCAL_TZ));
  }

  @Test void testSysDateFuncWithFixedTime() {
    testSysDateFunc(fixedTimeString(LOCAL_TZ));
  }

  private void testSysDateFunc(Pair<String, Hook.Closeable> pair) {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.SYSDATE)
        .withLibrary(SqlLibrary.ORACLE);

    f.checkType("SYSDATE", "DATE NOT NULL");
    f.checkType("(SYSDATE - SYSDATE) DAY", "INTERVAL DAY NOT NULL");
    f.checkFails("^SYSDATE()^",
        "No match found for function signature SYSDATE\\(\\)",
        false);
    // check sysdate function within cast function
    f.checkType("CAST(SYSDATE AS VARCHAR(30))", "VARCHAR(30) NOT NULL");
    f.checkFails("CAST(^SYSDATE()^ AS VARCHAR(30))",
        "No match found for function signature SYSDATE\\(\\)",
        false);
    pair.right.close();
  }

  @Test void testLastDayFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LAST_DAY, VmName.EXPAND);
    f.checkScalar("last_day(DATE '2019-02-10')",
        "2019-02-28", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '2019-06-10')",
        "2019-06-30", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '2019-07-10')",
        "2019-07-31", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '2019-09-10')",
        "2019-09-30", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '2019-12-10')",
        "2019-12-31", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '9999-12-10')",
        "9999-12-31", "DATE NOT NULL");

    // Edge tests
    f.checkScalar("last_day(DATE '1900-01-01')",
        "1900-01-31", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '1935-02-01')",
        "1935-02-28", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '1965-09-01')",
        "1965-09-30", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '1970-01-01')",
        "1970-01-31", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '2019-02-28')",
        "2019-02-28", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '2019-12-31')",
        "2019-12-31", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '2019-01-01')",
        "2019-01-31", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '2019-06-30')",
        "2019-06-30", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '2020-02-20')",
        "2020-02-29", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '2020-02-29')",
        "2020-02-29", "DATE NOT NULL");
    f.checkScalar("last_day(DATE '9999-12-31')",
        "9999-12-31", "DATE NOT NULL");

    f.checkNull("last_day(cast(null as date))");

    f.checkScalar("last_day(TIMESTAMP '2019-02-10 02:10:12')",
        "2019-02-28", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '2019-06-10 06:10:16')",
        "2019-06-30", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '2019-07-10 07:10:17')",
        "2019-07-31", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '2019-09-10 09:10:19')",
        "2019-09-30", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '2019-12-10 12:10:22')",
        "2019-12-31", "DATE NOT NULL");

    // Edge tests
    f.checkScalar("last_day(TIMESTAMP '1900-01-01 01:01:02')",
        "1900-01-31", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '1935-02-01 02:01:03')",
        "1935-02-28", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '1970-01-01 01:01:02')",
        "1970-01-31", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '2019-02-28 02:28:30')",
        "2019-02-28", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '2019-12-31 12:31:43')",
        "2019-12-31", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '2019-01-01 01:01:02')",
        "2019-01-31", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '2019-06-30 06:30:36')",
        "2019-06-30", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '2020-02-20 02:20:33')",
        "2020-02-29", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '2020-02-29 02:29:31')",
        "2020-02-29", "DATE NOT NULL");
    f.checkScalar("last_day(TIMESTAMP '9999-12-31 12:31:43')",
        "9999-12-31", "DATE NOT NULL");

    f.checkNull("last_day(cast(null as timestamp))");
  }

  @Test void testLpadFunction() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.LPAD);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.check("select lpad('12345', 8, 'a')", "VARCHAR NOT NULL", "aaa12345");
      f.checkString("lpad('12345', 8)", "   12345", "VARCHAR NOT NULL");
      f.checkString("lpad('12345', 8, 'ab')", "aba12345", "VARCHAR NOT NULL");
      f.checkString("lpad('12345', 3, 'a')", "123", "VARCHAR NOT NULL");
      f.checkFails("lpad('12345', -3, 'a')",
          "Second argument for LPAD/RPAD must not be negative", true);
      f.checkFails("lpad('12345', -3)",
          "Second argument for LPAD/RPAD must not be negative", true);
      f.checkFails("lpad('12345', 3, '')",
          "Third argument \\(pad pattern\\) for LPAD/RPAD must not be empty", true);
      f.checkString("lpad(x'aa', 4, x'bb')", "bbbbbbaa", "VARBINARY NOT NULL");
      f.checkString("lpad(x'aa', 4)", "202020aa", "VARBINARY NOT NULL");
      f.checkString("lpad(x'aaaaaa', 2)", "aaaa", "VARBINARY NOT NULL");
      f.checkString("lpad(x'aaaaaa', 2, x'bb')", "aaaa", "VARBINARY NOT NULL");
      f.checkFails("lpad(x'aa', -3, x'bb')",
          "Second argument for LPAD/RPAD must not be negative", true);
      f.checkFails("lpad(x'aa', -3)",
          "Second argument for LPAD/RPAD must not be negative", true);
      f.checkFails("lpad(x'aa', 3, x'')",
          "Third argument \\(pad pattern\\) for LPAD/RPAD must not be empty", true);
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.ORACLE, SqlLibrary.POSTGRESQL,
            SqlLibrary.REDSHIFT, SqlLibrary.SPARK, SqlLibrary.HIVE);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testRpadFunction() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.RPAD);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.check("select rpad('12345', 8, 'a')", "VARCHAR NOT NULL", "12345aaa");
      f.checkString("rpad('12345', 8)", "12345   ", "VARCHAR NOT NULL");
      f.checkString("rpad('12345', 8, 'ab')", "12345aba", "VARCHAR NOT NULL");
      f.checkString("rpad('12345', 3, 'a')", "123", "VARCHAR NOT NULL");
      f.checkFails("rpad('12345', -3, 'a')",
          "Second argument for LPAD/RPAD must not be negative", true);
      f.checkFails("rpad('12345', -3)",
          "Second argument for LPAD/RPAD must not be negative", true);
      f.checkFails("rpad('12345', 3, '')",
          "Third argument \\(pad pattern\\) for LPAD/RPAD must not be empty", true);

      f.checkString("rpad(x'aa', 4, x'bb')", "aabbbbbb", "VARBINARY NOT NULL");
      f.checkString("rpad(x'aa', 4)", "aa202020", "VARBINARY NOT NULL");
      f.checkString("rpad(x'aaaaaa', 2)", "aaaa", "VARBINARY NOT NULL");
      f.checkString("rpad(x'aaaaaa', 2, x'bb')", "aaaa", "VARBINARY NOT NULL");
      f.checkFails("rpad(x'aa', -3, x'bb')",
          "Second argument for LPAD/RPAD must not be negative", true);
      f.checkFails("rpad(x'aa', -3)",
          "Second argument for LPAD/RPAD must not be negative", true);
      f.checkFails("rpad(x'aa', 3, x'')",
          "Third argument \\(pad pattern\\) for LPAD/RPAD must not be empty", true);
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.ORACLE, SqlLibrary.POSTGRESQL,
            SqlLibrary.REDSHIFT, SqlLibrary.SPARK, SqlLibrary.HIVE);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testContainsSubstrFunc() {
    final SqlOperatorFixture f = fixture().setFor(SqlLibraryOperators.CONTAINS_SUBSTR)
        .withLibrary(SqlLibrary.BIG_QUERY);
    // Strings
    f.checkBoolean("CONTAINS_SUBSTR('abc', 'a')", true);
    f.checkBoolean("CONTAINS_SUBSTR('abc', 'd')", false);
    f.checkBoolean("CONTAINS_SUBSTR('ABC', 'a')", true);
    f.checkBoolean("CONTAINS_SUBSTR('abc', '')", true);
    f.checkBoolean("CONTAINS_SUBSTR('', '')", true);
    // Only the values of ROWs and ARRAYs should be searched, their opening/closing
    // parentheses/brackets should not be included
    f.checkBoolean("CONTAINS_SUBSTR((23, 35, 41), '35')", true);
    f.checkBoolean("CONTAINS_SUBSTR((23, 35, 41), '1)')", false);
    // Arrays and nested rows
    f.checkBoolean("CONTAINS_SUBSTR(('abc', ('def', 'ghi', 'jkl'), 'mno'), 'jk')",
        true);
    f.checkBoolean("CONTAINS_SUBSTR(('abc', ARRAY['def', 'ghi', 'jkl'], 'mno'), 'jk')",
        true);
    f.checkBoolean("CONTAINS_SUBSTR(('abc', ARRAY['def', 'ghi', 'jkl'], 'mno'), 'xyz')",
        false);
    f.checkBoolean("CONTAINS_SUBSTR(('abc', ARRAY['def', 'ghi', 'jkl'], (1, 2, 3)), 'xyz')",
        false);
    // Boolean types
    f.checkBoolean("CONTAINS_SUBSTR(false, 'fal')", true);
    f.checkBoolean("CONTAINS_SUBSTR(false, 'true')", false);
    // DATETIME types should lose keyword (DATE '2008-12-25' should be searched as '2008-12-25')
    f.checkBoolean("CONTAINS_SUBSTR(DATE '2008-12-25', '2008-12-25')",
        true);
    f.checkBoolean("CONTAINS_SUBSTR(TIME '15:30:00', '15:30:00')",
        true);
    f.checkBoolean("CONTAINS_SUBSTR(TIMESTAMP '2008-12-25 15:30:00', '5 1')",
        true);
    f.checkBoolean("CONTAINS_SUBSTR(TIMESTAMP '2008-12-25 15:30:00', 'TI')",
        false);
    // Numeric types
    f.checkBoolean("CONTAINS_SUBSTR(cast(1 as tinyint), '1')", true);
    f.checkBoolean("CONTAINS_SUBSTR(cast(1 as smallint), '1')", true);
    f.checkBoolean("CONTAINS_SUBSTR(cast(1 as integer), '1')", true);
    f.checkBoolean("CONTAINS_SUBSTR(cast(1 as bigint), '1')", true);
    f.checkBoolean("CONTAINS_SUBSTR(cast(1.2345 as decimal(5, 4)), '1')",
        true);
    // JSON
    f.checkBoolean("CONTAINS_SUBSTR('{\"foo\":\"bar\"}', 'bar')",
        true);
    f.checkBoolean("CONTAINS_SUBSTR('{\"foo\":\"bar\"}', 'BAR')",
        true);
    f.checkBoolean("CONTAINS_SUBSTR('{\"foo\":\"bar\"}', 'bar', json_scope=>'JSON_KEYS')",
        false);
    f.checkBoolean("CONTAINS_SUBSTR('{\"foo\":\"bar\"}', 'bar', "
            + "json_scope=>'JSON_VALUES')", true);
    f.checkBoolean("CONTAINS_SUBSTR('{\"foo\":\"bar\"}', 'bar', "
            + "json_scope=>'JSON_KEYS_AND_VALUES')", true);
    f.checkFails("CONTAINS_SUBSTR('{\"foo\":\"bar\"}', 'bar', json_scope=>'JSON_JSON')",
        "json_scope argument must be one of: \"JSON_KEYS\", \"JSON_VALUES\", "
            + "\"JSON_KEYS_AND_VALUES\".", true);
    // Null behavior
    f.checkNull("CONTAINS_SUBSTR(cast(null as integer), 'hello')");
    f.checkNull("CONTAINS_SUBSTR(null, 'a')");
  }

  @Test void testStrposFunction() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.STRPOS);
    f0.checkFails("^strpos('abc', 'a')^",
        "No match found for function signature STRPOS\\(<CHARACTER>, <CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("STRPOS('abc', 'a')", "1", "INTEGER NOT NULL");
      f.checkScalar("STRPOS('abcabc', 'bc')", "2", "INTEGER NOT NULL");
      f.checkScalar("STRPOS('abcabc', 'd')", "0", "INTEGER NOT NULL");
      f.checkScalar("STRPOS('abc', '')", "1", "INTEGER NOT NULL");
      f.checkScalar("STRPOS('', 'a')", "0", "INTEGER NOT NULL");
      f.checkNull("STRPOS(null, 'a')");
      f.checkNull("STRPOS('a', null)");

      // test for BINARY
      f.checkScalar("STRPOS(x'2212', x'12')", "2", "INTEGER NOT NULL");
      f.checkScalar("STRPOS(x'2122', x'12')", "0", "INTEGER NOT NULL");
      f.checkScalar("STRPOS(x'1222', x'12')", "1", "INTEGER NOT NULL");
      f.checkScalar("STRPOS(x'1111', x'22')", "0", "INTEGER NOT NULL");
      f.checkScalar("STRPOS(x'2122', x'')", "1", "INTEGER NOT NULL");
      f.checkScalar("STRPOS(x'', x'12')", "0", "INTEGER NOT NULL");
      f.checkNull("STRPOS(null, x'')");
      f.checkNull("STRPOS(x'', null)");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.POSTGRESQL, SqlLibrary.REDSHIFT);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testInstrFunction() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.INSTR, VM_JAVA);
    f0.checkFails("^INSTR('abc', 'a', 1, 1)^",
        "No match found for function signature INSTR\\(<CHARACTER>, <CHARACTER>,"
            + " <NUMERIC>, <NUMERIC>\\)", false);

    final Consumer<SqlOperatorFixture> consumer = f -> {
      // test for CHAR
      f.checkScalar("INSTR('abc', 'a', 1, 1)", "1", "INTEGER NOT NULL");
      f.checkScalar("INSTR('abcabc', 'bc', 1, 2)", "5", "INTEGER NOT NULL");
      f.checkScalar("INSTR('abcabc', 'd', 1, 1)", "0", "INTEGER NOT NULL");
      f.checkScalar("INSTR('dabcabcd', 'd', 4, 1)", "8", "INTEGER NOT NULL");
      f.checkScalar("INSTR('abc', '', 1, 1)", "1", "INTEGER NOT NULL");
      f.checkScalar("INSTR('', 'a', 1, 1)", "0", "INTEGER NOT NULL");
      f.checkNull("INSTR(null, 'a', 1, 1)");
      f.checkNull("INSTR('a', null, 1, 1)");

      // test for BINARY
      f.checkScalar("INSTR(x'2212', x'12', -1, 1)", "2", "INTEGER NOT NULL");
      f.checkScalar("INSTR(x'2122', x'12', 1, 1)", "0", "INTEGER NOT NULL");
      f.checkScalar("INSTR(x'122212', x'12', -1, 2)", "1", "INTEGER NOT NULL");
      f.checkScalar("INSTR(x'1111', x'22', 1, 1)", "0", "INTEGER NOT NULL");
      f.checkScalar("INSTR(x'2122', x'', 1, 1)", "1", "INTEGER NOT NULL");
      f.checkScalar("INSTR(x'', x'12', 1, 1)", "0", "INTEGER NOT NULL");
      f.checkNull("INSTR(null, x'', 1, 1)");
      f.checkNull("INSTR(x'', null, 1, 1)");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.HIVE, SqlLibrary.MYSQL, SqlLibrary.ORACLE);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testSnowflakeStartsWithFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.STARTSWITH, VmName.EXPAND);
    checkStartsWith(f, FunctionAlias.of(SqlLibraryOperators.STARTSWITH));
  }

  @Test void testStartsWithFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.STARTS_WITH, VmName.EXPAND);
    checkStartsWith(f, FunctionAlias.of(SqlLibraryOperators.STARTS_WITH));
  }

  /** Tests the {@code STARTS_WITH} and {@code STARTSWITH} operators. */
  void checkStartsWith(SqlOperatorFixture f0, FunctionAlias functionAlias) {
    final SqlFunction function = functionAlias.function;
    final String fn = function.getName();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkBoolean(fn + "('12345', '123')", true);
      f.checkBoolean(fn + "('12345', '1243')", false);
      f.checkBoolean(fn + "(x'11', x'11')", true);
      f.checkBoolean(fn + "(x'112211', x'33')", false);
      f.checkFails("^" + fn + "('aabbcc', x'aa')^",
          "Cannot apply '" + fn + "' to arguments of type "
              + "'" + fn + "\\(<CHAR\\(6\\)>, <BINARY\\(1\\)>\\)'\\. Supported "
              + "form\\(s\\): '" + fn + "\\(<STRING>, <STRING>\\)'",
          false);
      f.checkNull(fn + "(null, null)");
      f.checkNull(fn + "('12345', null)");
      f.checkNull(fn + "(null, '123')");
      f.checkBoolean(fn + "('', '123')", false);
      f.checkBoolean(fn + "('', '')", true);
      f.checkNull(fn + "(x'aa', null)");
      f.checkNull(fn + "(null, x'aa')");
      f.checkBoolean(fn + "(x'1234', x'')", true);
      f.checkBoolean(fn + "(x'', x'123456')", false);
      f.checkBoolean(fn + "(x'', x'')", true);
    };
    f0.forEachLibrary(list(functionAlias.libraries), consumer);
  }

  @Test void testSnowflakeEndsWithFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.ENDSWITH, VmName.EXPAND);
    checkEndsWith(f, FunctionAlias.of(SqlLibraryOperators.ENDSWITH));
  }

  @Test void testEndsWithFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.ENDS_WITH, VmName.EXPAND);
    checkEndsWith(f, FunctionAlias.of(SqlLibraryOperators.ENDS_WITH));
  }

  /** Tests the {@code ENDS_WITH} and {@code ENDSWITH} operators. */
  void checkEndsWith(SqlOperatorFixture f0, FunctionAlias functionAlias) {
    final SqlFunction function = functionAlias.function;
    final String fn = function.getName();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkBoolean(fn + "('12345', '345')", true);
      f.checkBoolean(fn + "('12345', '123')", false);
      f.checkBoolean(fn + "(x'11', x'11')", true);
      f.checkBoolean(fn + "(x'112211', x'33')", false);
      f.checkFails("^" + fn + "('aabbcc', x'aa')^",
          "Cannot apply '" + fn + "' to arguments of type "
              + "'" + fn + "\\(<CHAR\\(6\\)>, <BINARY\\(1\\)>\\)'\\. Supported "
              + "form\\(s\\): '" + fn + "\\(<STRING>, <STRING>\\)'",
          false);
      f.checkNull(fn + "(null, null)");
      f.checkNull(fn + "('12345', null)");
      f.checkNull(fn + "(null, '123')");
      f.checkBoolean(fn + "('', '123')", false);
      f.checkBoolean(fn + "('', '')", true);
      f.checkNull(fn + "(x'aa', null)");
      f.checkNull(fn + "(null, x'aa')");
      f.checkBoolean(fn + "(x'1234', x'')", true);
      f.checkBoolean(fn + "(x'', x'123456')", false);
      f.checkBoolean(fn + "(x'', x'')", true);
    };
    f0.forEachLibrary(list(functionAlias.libraries), consumer);
  }

  /** Tests the {@code SPLIT} operator. */
  @Test void testSplitFunction() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SPLIT);
    f0.checkFails("^split('hello')^",
        "No match found for function signature SPLIT\\(<CHARACTER>\\)",
        false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("SPLIT('h,e,l,l,o')", "[h, e, l, l, o]",
        "VARCHAR NOT NULL ARRAY NOT NULL");
    f.checkScalar("SPLIT('h-e-l-l-o', '-')", "[h, e, l, l, o]",
        "VARCHAR NOT NULL ARRAY NOT NULL");
    f.checkScalar("SPLIT('hello', '-')", "[hello]",
        "VARCHAR NOT NULL ARRAY NOT NULL");
    f.checkScalar("SPLIT('')", "[]",
        "VARCHAR NOT NULL ARRAY NOT NULL");
    f.checkScalar("SPLIT('', '-')", "[]",
        "VARCHAR NOT NULL ARRAY NOT NULL");
    f.checkNull("SPLIT(null)");
    f.checkNull("SPLIT('hello', null)");

    // In ASCII, x'41' = 'A', x'42' = 'B', x'43' = 'C'
    f.checkScalar("SPLIT(x'414243', x'ff')", "[ABC]",
        "VARBINARY NOT NULL ARRAY NOT NULL");
    f.checkScalar("SPLIT(x'414243', x'41')", "[, BC]",
        "VARBINARY NOT NULL ARRAY NOT NULL");
    f.checkScalar("SPLIT(x'414243', x'42')", "[A, C]",
        "VARBINARY NOT NULL ARRAY NOT NULL");
    f.checkScalar("SPLIT(x'414243', x'43')", "[AB, ]",
        "VARBINARY NOT NULL ARRAY NOT NULL");
    f.checkFails("^SPLIT(x'aabbcc')^",
        "Call to function 'SPLIT' with argument of type 'BINARY\\(3\\)' "
            + "requires extra delimiter argument", false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6663">[CALCITE-6663]
   * Support SPLIT_PART function for PostgreSql  </a>.
   */
  @Test void testSplitPartFunction() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.SPLIT_PART);
    f0.checkFails("^split_part('hello', ',', 1)^",
        "No match found for function signature SPLIT_PART\\(<CHARACTER>, <CHARACTER>, <NUMERIC>\\)",
        false);

    final SqlOperatorFixture f1 = fixture().withLibrary(SqlLibrary.REDSHIFT);
    f1.checkFails("^split_part('hello', ',', 1)^",
        "No match found for function signature SPLIT_PART\\(<CHARACTER>, <CHARACTER>, <NUMERIC>\\)",
        false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.POSTGRESQL);

    f.checkScalar("SPLIT_PART('abc~@~def~@~ghi', '~@~', 2)", "def", "VARCHAR NOT NULL");
    f.checkScalar("SPLIT_PART('abc,def,ghi,jkl', ',', 3)", "ghi", "VARCHAR NOT NULL");

    f.checkScalar("SPLIT_PART('abc~@~def~@~ghi', '~@~', -1)", "ghi", "VARCHAR NOT NULL");
    f.checkScalar("SPLIT_PART('abc,def,ghi,jkl', ',', -2)", "ghi", "VARCHAR NOT NULL");

    f.checkScalar("SPLIT_PART('h,e,l,l,o', ',', 7)", "", "VARCHAR NOT NULL");
    f.checkScalar("SPLIT_PART('h,e,l,l,o', ',', -7)", "", "VARCHAR NOT NULL");

    f.checkScalar("SPLIT_PART('abc,,ghi', ',', 2)", "", "VARCHAR NOT NULL");
    f.checkScalar("SPLIT_PART('', ',', 1)", "", "VARCHAR NOT NULL");

    f.checkNull("SPLIT_PART(null, ',', 1)");
    f.checkNull("SPLIT_PART('abc', null, 1)");
    f.checkNull("SPLIT_PART('a,b,c', ',', null)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5811">[CALCITE-5811]
   * Error messages produced for constant out-of-bounds arguments are confusing</a>. */
  @Test void testIndexOutOfBounds() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("substring('abc' from 2 for 2147483650)",
        "bc", "VARCHAR(3) NOT NULL");
    f.checkScalar("substring('abc' from 2147483650)",
        "", "VARCHAR(3) NOT NULL");
    f.checkScalar("substring('abc' from 2147483650 for 2147483650)",
        "", "VARCHAR(3) NOT NULL");
    f.checkScalar("substring('abc' from 2147483650 for 2)",
        "", "VARCHAR(3) NOT NULL");
    f.checkFails("^substring('abc' from 2 for 2147483650.0)^",
        "Cannot apply 'SUBSTRING' to arguments of type "
            + "'SUBSTRING\\(<CHAR\\(3\\)> FROM <INTEGER> FOR <DECIMAL\\(11, 1\\)>\\)'\\. "
            + "Supported form\\(s\\): 'SUBSTRING\\(<CHAR> FROM <INTEGER>\\)'\n"
            + "'SUBSTRING\\(<CHAR> FROM <INTEGER> FOR <INTEGER>\\)'\n"
            + "'SUBSTRING\\(<VARCHAR> FROM <INTEGER>\\)'\n"
            + "'SUBSTRING\\(<VARCHAR> FROM <INTEGER> FOR <INTEGER>\\)'\n"
            + "'SUBSTRING\\(<BINARY> FROM <INTEGER>\\)'\n"
            + "'SUBSTRING\\(<BINARY> FROM <INTEGER> FOR <INTEGER>\\)'\n"
            + "'SUBSTRING\\(<VARBINARY> FROM <INTEGER>\\)'\n"
            + "'SUBSTRING\\(<VARBINARY> FROM <INTEGER> FOR <INTEGER>\\)'", false);
  }

  /** Tests the {@code SUBSTRING} operator. Many test cases that used to be
   * have been moved to {@link SubFunChecker#assertSubFunReturns}, and are
   * called for both {@code SUBSTRING} and {@code SUBSTR}. */
  @Test void testSubstringFunction() {
    final SqlOperatorFixture f = fixture();
    checkSubstringFunction(f);
    checkSubstringFunction(f.withConformance(SqlConformanceEnum.BIG_QUERY));
    checkSubstringFunctionOverflow(f);
    checkSubstringFunctionOverflow(f.withConformance(SqlConformanceEnum.BIG_QUERY));
  }

  /**
   * Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6211">
   * SUBSTRING with Integer.MIN_VALUE as a second parameter raise unexpected exception</a>. */
  private static void checkSubstringFunctionOverflow(SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.SUBSTRING);
    f.checkScalar(
        String.format(Locale.ROOT, "{fn SUBSTRING('abcdef', %d)}", Integer.MIN_VALUE),
        "abcdef", "VARCHAR(6) NOT NULL");

    f.checkScalar(
        String.format(Locale.ROOT, "{fn SUBSTRING('abcdef', %d, %d)}", Integer.MIN_VALUE,
            Integer.MAX_VALUE + 10L), "abcdef", "VARCHAR(6) NOT NULL");

    f.checkScalar(
        String.format(Locale.ROOT, "{fn SUBSTRING('abcdef', CAST(%d AS BIGINT))}",
            Integer.MIN_VALUE), "abcdef", "VARCHAR(6) NOT NULL");
  }

  private static void checkSubstringFunction(SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.SUBSTRING);
    f.checkString("substring('abc' from 1 for 2)",
        "ab", "VARCHAR(3) NOT NULL");
    f.checkString("substring(x'aabbcc' from 1 for 2)",
        "aabb", "VARBINARY(3) NOT NULL");
    f.checkString("substring('abc' from 2 for 2147483646)",
        "bc", "VARCHAR(3) NOT NULL");
    f.checkString(
        String.format(Locale.ROOT, "substring('string', CAST(%d AS TINYINT), %d)",
        Byte.MIN_VALUE, Byte.MAX_VALUE + 10), "string", "VARCHAR(6) NOT NULL");

    switch (f.conformance().semantics()) {
    case BIG_QUERY:
      f.checkString("substring('abc' from 1 for -1)", "",
          "VARCHAR(3) NOT NULL");
      f.checkString("substring(x'aabbcc' from 1 for -1)", "",
          "VARBINARY(3) NOT NULL");
      break;
    default:
      f.checkFails(
          String.format(Locale.ROOT, "^substring('string', CAST(%d AS DOUBLE), "
              + "CAST(%d AS DOUBLE))^", Byte.MIN_VALUE, Byte.MAX_VALUE + 10),
          "Cannot apply 'SUBSTRING' to arguments of type "
            + ".*\\n.*\\n.*\\n.*\\n.*\\n.*\\n.*\\n.*", false);
      f.checkFails(
          String.format(Locale.ROOT, "^substring('string', CAST(%d AS DECIMAL), "
              + "CAST(%d AS DECIMAL))^", Byte.MIN_VALUE, Byte.MAX_VALUE + 10),
          "Cannot apply 'SUBSTRING' to arguments of type "
              + ".*\\n.*\\n.*\\n.*\\n.*\\n.*\\n.*\\n.*",
          false);
      f.checkFails("substring('abc' from 1 for -1)",
          "Substring error: negative substring length not allowed",
          true);
      f.checkFails("substring(x'aabbcc' from 1 for -1)",
          "Substring error: negative substring length not allowed",
          true);
      f.checkFails("{fn SUBSTRING('abc', 2, -1)}",
          "Substring error: negative substring length not allowed",
          true);
    }

    if (Bug.FRG296_FIXED) {
      // substring regexp not supported yet
      f.checkString("substring('foobar' from '%#\"o_b#\"%' for'#')",
          "oob", "xx");
    }
    f.checkNull("substring(cast(null as varchar(1)),1,2)");
    f.checkNull("substring(cast(null as varchar(1)) FROM 1 FOR 2)");
    f.checkNull("substring('abc' FROM cast(null as integer) FOR 2)");
    f.checkNull("substring('abc' FROM cast(null as integer))");
    f.checkNull("substring('abc' FROM 2 FOR cast(null as integer))");
  }

  /** Tests the non-standard SUBSTR function, that has syntax
   * "SUBSTR(value, start [, length ])", as used in BigQuery. */
  @Test void testBigQuerySubstrFunction() {
    substrChecker(SqlLibrary.BIG_QUERY, SqlLibraryOperators.SUBSTR_BIG_QUERY)
        .check();
  }

  /** Tests the non-standard SUBSTR function, that has syntax
   * "SUBSTR(value, start [, length ])", as used in Oracle. */
  @Test void testMysqlSubstrFunction() {
    substrChecker(SqlLibrary.MYSQL, SqlLibraryOperators.SUBSTR_MYSQL)
        .check();
  }

  /** Tests the non-standard SUBSTR function, that has syntax
   * "SUBSTR(value, start [, length ])", as used in Oracle. */
  @Test void testOracleSubstrFunction() {
    substrChecker(SqlLibrary.ORACLE, SqlLibraryOperators.SUBSTR_ORACLE)
        .check();
  }

  /** Tests the non-standard SUBSTR function, that has syntax
   * "SUBSTR(value, start [, length ])", as used in PostgreSQL. */
  @Test void testPostgresqlSubstrFunction() {
    substrChecker(SqlLibrary.POSTGRESQL, SqlLibraryOperators.SUBSTR_POSTGRESQL)
        .check();
  }

  /** Tests the standard {@code SUBSTRING} function in the mode that has
   * BigQuery's non-standard semantics. */
  @Test void testBigQuerySubstringFunction() {
    substringChecker(SqlConformanceEnum.BIG_QUERY, SqlLibrary.BIG_QUERY)
        .check();
  }

  /** Tests the standard {@code SUBSTRING} function in ISO standard
   * semantics. */
  @Test void testStandardSubstringFunction() {
    substringChecker(SqlConformanceEnum.STRICT_2003, SqlLibrary.POSTGRESQL)
        .check();
  }

  SubFunChecker substringChecker(SqlConformanceEnum conformance,
      SqlLibrary library) {
    final SqlOperatorFixture f = fixture();
    return new SubFunChecker(
        f.withConnectionFactory(cf ->
            cf.with(ConnectionFactories.add(CalciteAssert.SchemaSpec.HR))
                .with(CalciteConnectionProperty.CONFORMANCE, conformance)),
        library,
        SqlStdOperatorTable.SUBSTRING);
  }

  SubFunChecker substrChecker(SqlLibrary library, SqlFunction function) {
    return new SubFunChecker(fixture().withLibrary(library), library, function);
  }

  /** Tests various configurations of {@code SUBSTR} and {@code SUBSTRING}
   * functions. */
  static class SubFunChecker {
    final SqlOperatorFixture f;
    final SqlLibrary library;
    final SqlFunction function;

    SubFunChecker(SqlOperatorFixture f, SqlLibrary library,
        SqlFunction function) {
      this.f = f;
      f.setFor(function);
      this.library = library;
      this.function = function;
    }

    void check() {
      // The following tests have been checked on Oracle 11g R2, PostgreSQL 9.6,
      // MySQL 5.6, Google BigQuery.
      //
      // PostgreSQL and MySQL have a standard SUBSTRING(x FROM s [FOR l])
      // operator, and its behavior is identical to their SUBSTRING(x, s [, l]).
      // Oracle and BigQuery do not have SUBSTRING.
      assertReturns("abc", 1, "abc");
      assertReturns("abc", 2, "bc");
      assertReturns("abc", 3, "c");
      assertReturns("abc", 4, "");
      assertReturns("abc", 5, "");

      switch (library) {
      case BIG_QUERY:
      case ORACLE:
        assertReturns("abc", 0, "abc");
        assertReturns("abc", 0, 5, "abc");
        assertReturns("abc", 0, 4, "abc");
        assertReturns("abc", 0, 3, "abc");
        assertReturns("abc", 0, 2, "ab");
        break;
      case POSTGRESQL:
        assertReturns("abc", 0, "abc");
        assertReturns("abc", 0, 5, "abc");
        assertReturns("abc", 0, 4, "abc");
        assertReturns("abc", 0, 3, "ab");
        assertReturns("abc", 0, 2, "a");
        break;
      case MYSQL:
        assertReturns("abc", 0, "");
        assertReturns("abc", 0, 5, "");
        assertReturns("abc", 0, 4, "");
        assertReturns("abc", 0, 3, "");
        assertReturns("abc", 0, 2, "");
        break;
      default:
        throw new AssertionError(library);
      }
      assertReturns("abc", 0, 0, "");
      assertReturns("abc", 2, 8, "bc");
      assertReturns("abc", 1, 0, "");
      assertReturns("abc", 1, 2, "ab");
      assertReturns("abc", 1, 3, "abc");
      assertReturns("abc", 4, 3, "");
      assertReturns("abc", 4, 4, "");
      assertReturns("abc", 8, 2, "");

      switch (library) {
      case POSTGRESQL:
        assertReturns("abc", 1, -1, null);
        assertReturns("abc", 4, -1, null);
        break;
      default:
        assertReturns("abc", 1, -1, "");
        assertReturns("abc", 4, -1, "");
        break;
      }

      // For negative start, BigQuery matches Oracle.
      switch (library) {
      case BIG_QUERY:
      case MYSQL:
      case ORACLE:
        // BIG_QUERY has different implementation, check SubstrConvertlet
        if (library == SqlLibrary.BIG_QUERY) {
          assertReturns("abc", Integer.MIN_VALUE, "abc");
        } else {
          assertReturns("abc", Integer.MIN_VALUE, "");
        }
        assertReturns("abc", -2, "bc");
        assertReturns("abc", -1, "c");
        assertReturns("abc", -2, 1, "b");
        assertReturns("abc", -2, 2, "bc");
        assertReturns("abc", -2, 3, "bc");
        assertReturns("abc", -2, 4, "bc");
        assertReturns("abc", -2, 5, "bc");
        assertReturns("abc", -2, 6, "bc");
        assertReturns("abc", -2, 7, "bc");
        assertReturns("abcde", -3, 2, "cd");
        assertReturns("abc", -3, 3, "abc");
        assertReturns("abc", -3, 8, "abc");
        assertReturns("abc", -1, 4, "c");
        break;
      case POSTGRESQL:
        assertReturns("abc", Integer.MIN_VALUE, "abc");
        assertReturns("abc", -2, "abc");
        assertReturns("abc", -1, "abc");
        assertReturns("abc", -2, 1, "");
        assertReturns("abc", -2, 2, "");
        assertReturns("abc", -2, 3, "");
        assertReturns("abc", -2, 4, "a");
        assertReturns("abc", -2, 5, "ab");
        assertReturns("abc", -2, 6, "abc");
        assertReturns("abc", -2, 7, "abc");
        assertReturns("abcde", -3, 2, "");
        assertReturns("abc", -3, 3, "");
        assertReturns("abc", -3, 8, "abc");
        assertReturns("abc", -1, 4, "ab");
        break;
      default:
        throw new AssertionError(library);
      }

      // For negative start and start + length between 0 and actual-length,
      // confusion reigns.
      switch (library) {
      case BIG_QUERY:
        assertReturns("abc", -4, 6, "abc");
        break;
      case MYSQL:
      case ORACLE:
        assertReturns("abc", -4, 6, "");
        break;
      case POSTGRESQL:
        assertReturns("abc", -4, 6, "a");
        break;
      default:
        throw new AssertionError(library);
      }
      // For very negative start, BigQuery differs from Oracle and PostgreSQL.
      switch (library) {
      case BIG_QUERY:
        assertReturns("abc", -4, 3, "abc");
        assertReturns("abc", -5, 1, "abc");
        assertReturns("abc", -10, 2, "abc");
        assertReturns("abc", -500, 1, "abc");
        break;
      case MYSQL:
      case ORACLE:
      case POSTGRESQL:
        assertReturns("abc", -4, 3, "");
        assertReturns("abc", -5, 1, "");
        assertReturns("abc", -10, 2, "");
        assertReturns("abc", -500, 1, "");
        break;
      default:
        throw new AssertionError(library);
      }
    }

    void assertReturns(String s, int start, String expected) {
      assertSubFunReturns(false, s, start, null, expected);
      assertSubFunReturns(true, s, start, null, expected);
    }

    void assertReturns(String s, int start, @Nullable Integer end,
        @Nullable String expected) {
      assertSubFunReturns(false, s, start, end, expected);
      assertSubFunReturns(true, s, start, end, expected);
    }

    void assertSubFunReturns(boolean binary, String s, int start,
        @Nullable Integer end, @Nullable String expected) {
      final String v = binary
          ? "x'" + DOUBLER.apply(s) + "'"
          : "'" + s + "'";
      final String type =
          (binary ? "VARBINARY" : "VARCHAR") + "(" + s.length() + ")";
      final String value = "CAST(" + v + " AS " + type + ")";
      final String expression;
      if (function == SqlStdOperatorTable.SUBSTRING) {
        expression = "substring(" + value + " FROM " + start
            + (end == null ? "" : (" FOR " + end)) + ")";
      } else {
        expression = "substr(" + value + ", " + start
            + (end == null ? "" : (", " + end)) + ")";
      }
      if (expected == null) {
        f.checkFails(expression,
            "Substring error: negative substring length not allowed", true);
      } else {
        if (binary) {
          expected = DOUBLER.apply(expected);
        }
        f.checkString(expression, expected, type + NON_NULLABLE_SUFFIX);
      }
    }
  }

  @Test void testFormatNumber() {
    final SqlOperatorFixture f0 = fixture().setFor(SqlLibraryOperators.FORMAT_NUMBER);
    f0.checkFails("^format_number(123, 2)^",
        "No match found for function signature FORMAT_NUMBER\\(<NUMERIC>, <NUMERIC>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      // test with tinyint type
      f.checkString("format_number(cast(1 as tinyint), 4)", "1.0000",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(1 as tinyint), '#,###,###,###,###,###,##0.0000')",
          "1.0000",
          "VARCHAR NOT NULL");

      // test with smallint type
      f.checkString("format_number(cast(1 as smallint), 4)", "1.0000",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(1234 as smallint), '#,###,###,###,###,###,##0.0000000')",
          "1,234.0000000",
          "VARCHAR NOT NULL");

      // test with integer type
      f.checkString("format_number(cast(1 as integer), 4)", "1.0000",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(1234 as integer), '#,###,###,###,###,###,##0.0000000')",
          "1,234.0000000",
          "VARCHAR NOT NULL");

      // test with bigint type
      f.checkString("format_number(cast(0 as bigint), 0)", "0",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(1 as bigint), 4)", "1.0000",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(1234 as bigint), 7)", "1,234.0000000",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(1234 as bigint), '#,###,###,###,###,###,##0.0000000')",
          "1,234.0000000",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(-1 as bigint), 4)", "-1.0000",
          "VARCHAR NOT NULL");

      // test with float type
      f.checkString("format_number(cast(12332.123456 as float), 4)", "12,332.1235",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(123456.123456789 as float), '########.###')",
          "123456.123",
          "VARCHAR NOT NULL");

      // test with double type
      f.checkString("format_number(cast(1234567.123456789 as double), 7)", "1,234,567.1234568",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(1234567.123456789 as double), '##,###,###.##')",
          "1,234,567.12",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(-0.123456789 as double), 15)", "-0.123456789000000",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(-0.123456789 as double),"
              + " '#,###,###,###,###,###,##0.000000000000000')",
          "-0.123456789000000",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(0.000000 as double), 1)", "0.0",
          "VARCHAR NOT NULL");
      f.checkString("format_number(cast(0.000000 as double), '#,###,###,###,###,###,##0.0')",
          "0.0",
          "VARCHAR NOT NULL");

      // test with decimal type
      f.checkString("format_number(1234567.123456789, 7)", "1,234,567.1234568",
          "VARCHAR NOT NULL");
      f.checkString("format_number(1234567.123456789, '##,###,###.##')",
          "1,234,567.12",
          "VARCHAR NOT NULL");
      f.checkString("format_number(-0.123456789, 15)", "-0.123456789000000",
          "VARCHAR NOT NULL");
      f.checkString("format_number(-0.123456789,"
              + " '#,###,###,###,###,###,##0.000000000000000')",
          "-0.123456789000000",
          "VARCHAR NOT NULL");
      f.checkString("format_number(0.000000, 1)", "0.0",
          "VARCHAR NOT NULL");
      f.checkString("format_number(0.0, '#,###,###,###,###,###,##0.0000')",
          "0.0000",
          "VARCHAR NOT NULL");

      // test with illegal argument
      f.checkFails("format_number(12332.123456, -1)",
          "Illegal arguments for FORMAT_NUMBER function:"
              + " negative decimal value not allowed",
          true);

      // test with null values
      f.checkNull("format_number(cast(null as integer), 1)");
      f.checkNull("format_number(0, cast(null as integer))");
      f.checkNull("format_number(0, cast(null as varchar))");
      f.checkNull("format_number(cast(null as integer), cast(null as integer))");
      f.checkNull("format_number(cast(null as integer), cast(null as varchar))");
    };
    f0.forEachLibrary(list(SqlLibrary.HIVE, SqlLibrary.SPARK), consumer);
  }

  @Test void testTrimFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.TRIM, VmName.EXPAND);

    // SQL:2003 6.29.11 Trimming a CHAR yields a VARCHAR
    f.checkString("trim('a' from 'aAa')", "A", "VARCHAR(3) NOT NULL");
    f.checkString("trim(both 'a' from 'aAa')", "A", "VARCHAR(3) NOT NULL");
    f.checkString("trim(' aAa ')", "aAa", "VARCHAR(5) NOT NULL");
    f.checkString("trim(both ' ' from ' aAa ')", "aAa", "VARCHAR(5) NOT NULL");
    f.checkString("trim(leading 'a' from 'aAa')", "Aa", "VARCHAR(3) NOT NULL");
    f.checkString("trim(trailing 'a' from 'aAa')", "aA", "VARCHAR(3) NOT NULL");
    f.checkNull("trim(null)");
    f.checkNull("trim(cast(null as varchar(1)) from 'a')");
    f.checkNull("trim('a' from cast(null as varchar(1)))");

    // SQL:2003 6.29.9 and SQL:2016 6.30.11: trim string must have length=1.
    // Failure occurs at runtime.
    f.checkFails("trim('xy' from 'abcde')",
        "Invalid argument 'xy': the length of the string describing "
            + "the trimmed character must be 1",
        true);
    f.checkFails("trim('' from 'abcde')",
        "Invalid argument '': the length of the string describing "
            + "the trimmed character must be 1",
        true);

    final Consumer<SqlOperatorFixture> consumer = f1 -> {
      f1.checkString("trim(leading 'eh' from 'hehe__hehe')", "__hehe",
          "VARCHAR(10) NOT NULL");
      f1.checkString("trim(trailing 'eh' from 'hehe__hehe')", "hehe__",
          "VARCHAR(10) NOT NULL");
      f1.checkString("trim('eh' from 'hehe__hehe')", "__", "VARCHAR(10) NOT NULL");
    };
    final List<SqlConformanceEnum> conformanceEnums =
        list(SqlConformanceEnum.MYSQL_5, SqlConformanceEnum.SQL_SERVER_2008);
    f.forEachConformance(conformanceEnums, consumer);
  }

  @Test void testRtrimFunc() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.RTRIM, VmName.EXPAND);
    f0.checkFails("^rtrim(' aaa')^",
        "No match found for function signature RTRIM\\(<CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("rtrim(' aAa  ')", " aAa", "VARCHAR(6) NOT NULL");
      f.checkNull("rtrim(CAST(NULL AS VARCHAR(6)))");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.ORACLE, SqlLibrary.POSTGRESQL,
            SqlLibrary.REDSHIFT, SqlLibrary.SPARK, SqlLibrary.HIVE);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testLtrimFunc() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.LTRIM, VmName.EXPAND);
    f0.checkFails("^ltrim('  aa')^",
        "No match found for function signature LTRIM\\(<CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("ltrim(' aAa  ')", "aAa  ", "VARCHAR(6) NOT NULL");
      f.checkNull("ltrim(CAST(NULL AS VARCHAR(6)))");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.ORACLE, SqlLibrary.POSTGRESQL,
            SqlLibrary.REDSHIFT, SqlLibrary.SPARK, SqlLibrary.HIVE);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testGreatestFunc() {
    final SqlOperatorFixture f0 =
        fixture().setFor(SqlLibraryOperators.GREATEST, VmName.EXPAND);
    f0.checkFails("^greatest('on', 'earth')^",
        "No match found for function signature GREATEST\\(<CHARACTER>, <CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("greatest('on', 'earth')", "on   ", "CHAR(5) NOT NULL");
      f.checkString("greatest('show', 'on', 'earth')", "show ",
          "CHAR(5) NOT NULL");
      f.checkScalar("greatest(12, CAST(NULL AS INTEGER), 3)", isNullValue(),
          "INTEGER");
      f.checkScalar("greatest(false, true)", true, "BOOLEAN NOT NULL");

      final SqlOperatorFixture f12 = f.forOracle(SqlConformanceEnum.ORACLE_12);
      f12.checkString("greatest('on', 'earth')", "on", "VARCHAR(5) NOT NULL");
      f12.checkString("greatest('show', 'on', 'earth')", "show",
          "VARCHAR(5) NOT NULL");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.ORACLE, SqlLibrary.HIVE);
    f0.forEachLibrary(libraries, consumer);
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-5634">
   * [CALCITE-5634] Enable GREATEST, LEAST functions in PostgreSQL library</a>. */
  @Test void testGreatestPgFunc() {
    final SqlOperatorFixture f0 =
        fixture().setFor(SqlLibraryOperators.GREATEST_PG, VmName.EXPAND);
    f0.checkFails("^greatest('on', 'earth')^",
        "No match found for function signature GREATEST\\(<CHARACTER>, <CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("greatest('on', 'earth')", "on   ", "CHAR(5) NOT NULL");
      f.checkString("greatest('show', 'on', 'earth')", "show ",
          "CHAR(5) NOT NULL");
      f.checkScalar("greatest(12, CAST(NULL AS INTEGER), 3)", "12",
          "INTEGER");
      f.checkScalar("greatest(false, true)", true, "BOOLEAN NOT NULL");
      f.checkScalar("greatest(CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))", isNullValue(),
          "INTEGER");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.POSTGRESQL, SqlLibrary.REDSHIFT, SqlLibrary.SPARK);
    f0.forEachLibrary(libraries, consumer);
  }

  @Test void testLeastFunc() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.LEAST, VmName.EXPAND);
    f0.checkFails("^least('on', 'earth')^",
        "No match found for function signature LEAST\\(<CHARACTER>, <CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("least('on', 'earth')", "earth", "CHAR(5) NOT NULL");
      f.checkString("least('show', 'on', 'earth')", "earth",
          "CHAR(5) NOT NULL");
      f.checkScalar("least(12, CAST(NULL AS INTEGER), 3)", isNullValue(),
          "INTEGER");
      f.checkScalar("least(false, true)", false, "BOOLEAN NOT NULL");

      final SqlOperatorFixture f12 = f.forOracle(SqlConformanceEnum.ORACLE_12);
      f12.checkString("least('on', 'earth')", "earth", "VARCHAR(5) NOT NULL");
      f12.checkString("least('show', 'on', 'earth')", "earth",
          "VARCHAR(5) NOT NULL");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.BIG_QUERY, SqlLibrary.ORACLE, SqlLibrary.HIVE);
    f0.forEachLibrary(libraries, consumer);
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7210">[CALCITE-7210]
   * BINARY literal values may not match their type</a>. */
  @Test void testLeastBinary() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.LEAST, VmName.EXPAND)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("least(x'01', x'0202')", "0100", "BINARY(2) NOT NULL");

    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.LEAST, VmName.EXPAND)
        // for shouldConvertRaggedUnionTypesToVarying
        .withConformance(SqlConformanceEnum.MYSQL_5)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f0.checkScalar("least(x'01', x'0202')", "01", "VARBINARY(2) NOT NULL");
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-6939">
   * [CALCITE-6939] Add support for Lateral Column Alias</a>. */
  @Test void testAliasInSelect() {
    final SqlOperatorFixture f = fixture()
        // Babel sets isSelectAlias to ANY
        .withConformance(SqlConformanceEnum.BABEL);
    // Y uses the local X
    f.check("select Y from (select 1 AS x, x as Y)",
        "INTEGER NOT NULL", "1");
    // X used twice
    f.check("select Y from (select 1 AS x, x+x as Y)",
        "INTEGER NOT NULL", "2");
    // Chain of uses
    f.check("select W from (select 1 AS x, x+x as Y, y+y as Z, z+z as W)",
        "INTEGER NOT NULL", "8");
    // Out of order lookup
    f.check("SELECT W FROM (select z+z AS W, y+y AS Z, x+x AS Y, 1 AS x)",
        "INTEGER NOT NULL", "8");
    // Duplicate local column definition
    f.checkFails("SELECT ^x^ FROM (select 1 AS x, 2 as x)",
        "Column 'X' is ambiguous", false);
    // Duplicate local column definition
    f.checkFails("SELECT W FROM (select 1 AS x, 2 as x, ^x^ as W)",
        "Column 'X' is ambiguous", false);
    // Inner columns are not used if scope includes outer columns
    f.check("SELECT W FROM (SELECT 1 AS X, X AS W FROM (SELECT 2 AS X))",
        "INTEGER NOT NULL", "2");
    // Circular dependency
    f.checkFails("SELECT X FROM (SELECT ^X^ AS Y, Y AS X)",
        "The definition of column 'X' depends on itself through "
            + "the following columns: 'X', 'Y'", false);
    // Circular dependency
    f.checkFails("SELECT X FROM (SELECT ^X^ + 1 AS Y, Y + 1 AS X)",
        "The definition of column 'X' depends on itself through "
            + "the following columns: 'X', 'Y'", false);
    // No circular dependency if a column is defined in an outer scope
    f.check("SELECT X FROM (SELECT X + 1 AS Y, Y + 1 AS X FROM (SELECT 2 AS X))",
        "INTEGER NOT NULL", "4");
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-5634">
   * [CALCITE-5634] Enable GREATEST, LEAST functions in PostgreSQL library</a>. */
  @Test void testLeastPgFunc() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.LEAST_PG, VmName.EXPAND);
    f0.checkFails("^least('on', 'earth')^",
        "No match found for function signature LEAST\\(<CHARACTER>, <CHARACTER>\\)",
        false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkString("least('on', 'earth')", "earth", "CHAR(5) NOT NULL");
      f.checkString("least('show', 'on', 'earth')", "earth",
          "CHAR(5) NOT NULL");
      f.checkScalar("least(12, CAST(NULL AS INTEGER), 3)", "3",
          "INTEGER");
      f.checkScalar("least(false, true)", false, "BOOLEAN NOT NULL");
      f.checkScalar("least(CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))", isNullValue(),
          "INTEGER");
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.POSTGRESQL, SqlLibrary.REDSHIFT, SqlLibrary.SPARK);
    f0.forEachLibrary(libraries, consumer);
  }

  /** Test case for <a href="https://issues.apache.org/jira/browse/CALCITE-7216">[CALCITE-7216]
   * SqlOperator.inferReturnType throws the wrong exception on error</a>. */
  @Test void testLeastMismatch() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.LEAST, VmName.EXPAND)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkFails("^least(DATE '2020-01-01', 'x')^",
        "Cannot infer return type for LEAST; operand types: \\[DATE, CHAR\\(1\\)\\]", false);
  }

  @Test void testIfNullFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.IFNULL, VmName.EXPAND);
    checkNvl(f, FunctionAlias.of(SqlLibraryOperators.IFNULL));
  }

  @Test void testNvlFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.NVL, VmName.EXPAND);
    SqlOperatorFixture f12 = f
        .withLibrary(SqlLibrary.ORACLE)
        .forOracle(SqlConformanceEnum.ORACLE_12);
    f12.checkString("nvl('abc', 'de')", "abc", "VARCHAR(3) NOT NULL");
    f12.checkString("nvl('abc', 'defg')", "abc", "VARCHAR(4) NOT NULL");
    f12.checkString("nvl('abc', CAST(NULL AS VARCHAR(20)))", "abc",
        "VARCHAR(20) NOT NULL");
    f12.checkString("nvl(CAST(NULL AS VARCHAR(20)), 'abc')", "abc",
        "VARCHAR(20) NOT NULL");
    f12.checkNull("nvl(CAST(NULL AS VARCHAR(6)), cast(NULL AS VARCHAR(4)))");
    checkNvl(f, FunctionAlias.of(SqlLibraryOperators.NVL));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6397">[CALCITE-6397]
   * Add NVL2 function (enabled in Oracle, Spark library) </a>.
   */
  @Test void testNvl2Func() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.NVL2, VmName.EXPAND);
    f.checkFails("^nvl2(NULL, 2, 1)^",
        "No match found for function signature "
            + "NVL2\\(<NULL>, <NUMERIC>, <NUMERIC>\\)", false);

    final Consumer<SqlOperatorFixture> consumer = f12 -> {
      f12.checkScalar("nvl2(NULL, 2, 1)", "1", "INTEGER NOT NULL");
      f12.checkScalar("nvl2(true, true, false)", true, "BOOLEAN NOT NULL");
      f12.checkScalar("nvl2(false, true, false)", true, "BOOLEAN NOT NULL");
      f12.checkScalar("nvl2(NULL, true, false)", false, "BOOLEAN NOT NULL");
      f12.checkScalar("nvl2(3, 2, 1)", "2", "INTEGER NOT NULL");
      f12.checkScalar("nvl2(3, 'a', 'b')", "a", "CHAR(1) NOT NULL");
      f12.checkScalar("nvl2(NULL, 'a', 'b')", "b", "CHAR(1) NOT NULL");
      f12.checkScalar("nvl2(NULL, 'ab', 'de')", "de", "CHAR(2) NOT NULL");
      f12.checkScalar("nvl2('ab', 'abc', 'def')", "abc", "CHAR(3) NOT NULL");
      f12.checkScalar("nvl2('a', 3, 2)", "3", "INTEGER NOT NULL");
      f12.checkScalar("NVL2(NULL, 3.0, 4.0)", "4.0", "DECIMAL(2, 1) NOT NULL");
      f12.checkScalar("NVL2('abc', 3.0, 4.0)", "3.0", "DECIMAL(2, 1) NOT NULL");
      f12.checkScalar("NVL2(1, 3.0, 2.111)", "3.000", "DECIMAL(4, 3) NOT NULL");
      f12.checkScalar("NVL2(NULL, 3.0, 2.111)", "2.111", "DECIMAL(4, 3) NOT NULL");
      f12.checkScalar("NVL2(3.111, 3.1415926, 2.111)", "3.1415926", "DECIMAL(8, 7) NOT NULL");

      f12.checkNull("nvl2('ab', CAST(NULL AS VARCHAR(6)), 'def')");
      f12.checkNull("nvl2(NULL, 'abc', NULL)");
      f12.checkNull("nvl2(NULL, NULL, NULL)");

      f12.checkFails("^NVL2(2.0, 2.0, true)^", "Parameters must be of the same type", false);
      f12.checkFails("^NVL2(NULL, 2.0, true)^", "Parameters must be of the same type", false);
      f12.checkFails("^NVL2(2.0, 1, true)^", "Parameters must be of the same type", false);
      f12.checkFails("^NVL2(NULL, 1, true)^", "Parameters must be of the same type", false);
    };
    final List<SqlLibrary> libraries =
        list(SqlLibrary.ORACLE, SqlLibrary.REDSHIFT, SqlLibrary.SPARK);
    f.forEachLibrary(libraries, consumer);
  }

  /** Tests the {@code NVL} and {@code IFNULL} operators. */
  void checkNvl(SqlOperatorFixture f0, FunctionAlias functionAlias) {
    final SqlFunction function = functionAlias.function;
    final String fn = function.getName();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar(fn + "(1, 2)", "1", "INTEGER NOT NULL");
      f.checkFails("^" + fn + "(1, true)^",
                   "Parameters must be of the same type", false);
      f.checkScalar(fn + "(true, false)", true, "BOOLEAN NOT NULL");
      f.checkScalar(fn + "(false, true)", false, "BOOLEAN NOT NULL");
      f.checkString(fn + "('abc', 'de')", "abc", "CHAR(3) NOT NULL");
      f.checkString(fn + "('abc', 'defg')", "abc ", "CHAR(4) NOT NULL");
      f.checkString(fn + "('abc', CAST(NULL AS VARCHAR(20)))", "abc",
                    "VARCHAR(20) NOT NULL");
      f.checkString(fn + "(CAST(NULL AS VARCHAR(20)), 'abc')", "abc",
                    "VARCHAR(20) NOT NULL");
      f.checkNull(fn + "(CAST(NULL AS VARCHAR(6)), cast(NULL AS VARCHAR(4)))");
    };
    f0.forEachLibrary(list(functionAlias.libraries), consumer);
  }

  @Test void testDecodeFunc() {
    checkDecodeFunc(fixture().withLibrary(SqlLibrary.ORACLE));
    checkDecodeFunc(fixture().withLibrary(SqlLibrary.REDSHIFT));
    checkDecodeFunc(fixture().withLibrary(SqlLibrary.SPARK));
    checkDecodeFunc(fixture().withLibrary(SqlLibrary.HIVE));
  }

  private static void checkDecodeFunc(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.DECODE, VmName.EXPAND);
    f.checkScalar("decode(0, 0, 'a', 1, 'b', 2, 'c')", "a", "CHAR(1)");
    f.checkScalar("decode(1, 0, 'a', 1, 'b', 2, 'c')", "b", "CHAR(1)");
    // if there are duplicates, take the first match
    f.checkScalar("decode(1, 0, 'a', 1, 'b', 1, 'z', 2, 'c')", "b",
        "CHAR(1)");
    // if there's no match, and no "else", return null
    f.checkScalar("decode(3, 0, 'a', 1, 'b', 2, 'c')", isNullValue(),
        "CHAR(1)");
    // if there's no match, return the "else" value
    f.checkScalar("decode(3, 0, 'a', 1, 'b', 2, 'c', 'd')", "d",
        "CHAR(1) NOT NULL");
    f.checkScalar("decode(1, 0, 'a', 1, 'b', 2, 'c', 'd')", "b",
        "CHAR(1) NOT NULL");
    // nulls match
    f.checkScalar("decode(cast(null as integer), 0, 'a',\n"
            + " cast(null as integer), 'b', 2, 'c', 'd')", "b",
        "CHAR(1) NOT NULL");
  }

  @Test void testWindow() {
    final SqlOperatorFixture f = fixture();
    f.check("select sum(1) over (order by x)\n"
            + "from (select 1 as x, 2 as y\n"
            + "  from (values (true)))",
        SqlTests.INTEGER_TYPE_CHECKER, 1);
  }

  @Test void testElementFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ELEMENT, VM_JAVA);
    f.checkString("element(multiset['abc'])", "abc", "CHAR(3)");
    f.checkNull("element(multiset[cast(null as integer)])");
  }

  @Test void testCardinalityFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CARDINALITY, VM_JAVA);
    f.checkScalarExact("cardinality(multiset[cast(null as integer),2])", 2);
    // applied to array
    f.checkScalarExact("cardinality(array['foo', 'bar'])", 2);
    // applied to map
    f.checkScalarExact("cardinality(map['foo', 1, 'bar', 2])", 2);
  }

  @Test void testMemberOfOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MEMBER_OF, VM_JAVA);
    f.checkBoolean("1 member of multiset[1]", true);
    f.checkBoolean("'2' member of multiset['1']", false);
    f.checkBoolean("cast(null as double) member of"
        + " multiset[cast(null as double)]", true);
    f.checkBoolean("cast(null as double) member of multiset[1.1]", false);
    f.checkBoolean("1.1 member of multiset[cast(null as double)]", false);
  }

  @Test void testMultisetUnionOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MULTISET_UNION_DISTINCT, VM_JAVA);
    f.checkBoolean("multiset[1,2] submultiset of "
        + "(multiset[2] multiset union multiset[1])", true);
    f.checkScalar("cardinality(multiset[1, 2, 3, 4, 2] "
            + "multiset union distinct multiset[1, 4, 5, 7, 8])",
        "7",
        "INTEGER NOT NULL");
    f.checkScalar("cardinality(multiset[1, 2, 3, 4, 2] "
            + "multiset union distinct multiset[1, 4, 5, 7, 8])",
        "7",
        "INTEGER NOT NULL");
    f.checkBoolean("(multiset[1, 2, 3, 4, 2] "
            + "multiset union distinct multiset[1, 4, 5, 7, 8]) "
            + "submultiset of multiset[1, 2, 3, 4, 5, 7, 8]",
        true);
    f.checkBoolean("(multiset[1, 2, 3, 4, 2] "
            + "multiset union distinct multiset[1, 4, 5, 7, 8]) "
            + "submultiset of multiset[1, 2, 3, 4, 5, 7, 8]",
        true);
    f.checkScalar("cardinality(multiset['a', 'b', 'c'] "
            + "multiset union distinct multiset['c', 'd', 'e'])",
        "5",
        "INTEGER NOT NULL");
    f.checkScalar("cardinality(multiset['a', 'b', 'c'] "
            + "multiset union distinct multiset['c', 'd', 'e'])",
        "5",
        "INTEGER NOT NULL");
    f.checkBoolean("(multiset['a', 'b', 'c'] "
            + "multiset union distinct multiset['c', 'd', 'e'])"
            + " submultiset of multiset['a', 'b', 'c', 'd', 'e']",
        true);
    f.checkBoolean("(multiset['a', 'b', 'c'] "
            + "multiset union distinct multiset['c', 'd', 'e'])"
            + " submultiset of multiset['a', 'b', 'c', 'd', 'e']",
        true);
    f.checkScalar("multiset[cast(null as double)] "
            + "multiset union multiset[cast(null as double)]",
        "[null, null]",
        "DOUBLE MULTISET NOT NULL");
    f.checkScalar("multiset[cast(null as boolean)] "
            + "multiset union multiset[cast(null as boolean)]",
        "[null, null]",
        "BOOLEAN MULTISET NOT NULL");
  }

  @Test void testMultisetUnionAllOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MULTISET_UNION, VM_JAVA);
    f.checkScalar("cardinality(multiset[1, 2, 3, 4, 2] "
            + "multiset union all multiset[1, 4, 5, 7, 8])",
        "10",
        "INTEGER NOT NULL");
    f.checkBoolean("(multiset[1, 2, 3, 4, 2] "
            + "multiset union all multiset[1, 4, 5, 7, 8]) "
            + "submultiset of multiset[1, 2, 3, 4, 5, 7, 8]",
        false);
    f.checkBoolean("(multiset[1, 2, 3, 4, 2] "
            + "multiset union all multiset[1, 4, 5, 7, 8]) "
            + "submultiset of multiset[1, 1, 2, 2, 3, 4, 4, 5, 7, 8]",
        true);
    f.checkScalar("cardinality(multiset['a', 'b', 'c'] "
            + "multiset union all multiset['c', 'd', 'e'])",
        "6",
        "INTEGER NOT NULL");
    f.checkBoolean("(multiset['a', 'b', 'c'] "
            + "multiset union all multiset['c', 'd', 'e']) "
            + "submultiset of multiset['a', 'b', 'c', 'd', 'e']",
        false);
    f.checkBoolean("(multiset['a', 'b', 'c'] "
            + "multiset union distinct multiset['c', 'd', 'e']) "
            + "submultiset of multiset['a', 'b', 'c', 'd', 'e', 'c']",
        true);
    f.checkScalar("multiset[cast(null as double)] "
            + "multiset union all multiset[cast(null as double)]",
        "[null, null]",
        "DOUBLE MULTISET NOT NULL");
    f.checkScalar("multiset[cast(null as boolean)] "
            + "multiset union all multiset[cast(null as boolean)]",
        "[null, null]",
        "BOOLEAN MULTISET NOT NULL");
  }

  @Test void testSubMultisetOfOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.SUBMULTISET_OF, VM_JAVA);
    f.checkBoolean("multiset[2] submultiset of multiset[1]", false);
    f.checkBoolean("multiset[1] submultiset of multiset[1]", true);
    f.checkBoolean("multiset[1, 2] submultiset of multiset[1]", false);
    f.checkBoolean("multiset[1] submultiset of multiset[1, 2]", true);
    f.checkBoolean("multiset[1, 2] submultiset of multiset[1, 2]", true);
    f.checkBoolean("multiset['a', 'b'] submultiset of "
        + "multiset['c', 'd', 's', 'a']", false);
    f.checkBoolean("multiset['a', 'd'] submultiset of "
        + "multiset['c', 's', 'a', 'w', 'd']", true);
    f.checkBoolean("multiset['q', 'a'] submultiset of multiset['a', 'q']",
        true);
  }

  @Test void testNotSubMultisetOfOperator() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.NOT_SUBMULTISET_OF, VM_JAVA);
    f.checkBoolean("multiset[2] not submultiset of multiset[1]", true);
    f.checkBoolean("multiset[1] not submultiset of multiset[1]", false);
    f.checkBoolean("multiset[1, 2] not submultiset of multiset[1]", true);
    f.checkBoolean("multiset[1] not submultiset of multiset[1, 2]", false);
    f.checkBoolean("multiset[1, 2] not submultiset of multiset[1, 2]", false);
    f.checkBoolean("multiset['a', 'b'] not submultiset of "
        + "multiset['c', 'd', 's', 'a']", true);
    f.checkBoolean("multiset['a', 'd'] not submultiset of "
        + "multiset['c', 's', 'a', 'w', 'd']", false);
    f.checkBoolean("multiset['q', 'a'] not submultiset of "
        + "multiset['a', 'q']", false);
  }

  @Test void testCollectFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.COLLECT, VM_JAVA);
    f.checkFails("collect(^*^)", "Unknown identifier '\\*'", false);
    f.checkAggType("collect(1)", "INTEGER NOT NULL MULTISET NOT NULL");
    f.checkAggType("collect(1.2)", "DECIMAL(2, 1) NOT NULL MULTISET NOT NULL");
    f.checkAggType("collect(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL MULTISET NOT NULL");
    f.checkFails("^collect()^",
        "Invalid number of arguments to function 'COLLECT'. Was expecting 1 arguments",
        false);
    f.checkFails("^collect(1, 2)^",
        "Invalid number of arguments to function 'COLLECT'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    f.checkAgg("collect(x)", values, isSet("[0, 2, 2]"));
    f.checkAgg("collect(x) within group(order by x desc)", values,
        isSet("[2, 2, 0]"));
    if (!f.brokenTestsEnabled()) {
      return;
    }
    f.checkAgg("collect(CASE x WHEN 0 THEN NULL ELSE -1 END)", values,
        isSingle(-3));
    f.checkAgg("collect(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values, isSingle(-1));
    f.checkAgg("collect(DISTINCT x)", values, isSingle(2));
  }

  @Test void testListAggFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LISTAGG, VM_JAVA);
    f.checkFails("listagg(^*^)", "Unknown identifier '\\*'", false);
    f.checkAggType("listagg(12)", "VARCHAR NOT NULL");
    f.enableTypeCoercion(false)
        .checkFails("^listagg(12)^",
            "Cannot apply 'LISTAGG' to arguments of type .*'\n.*'", false);
    f.checkAggType("listagg(cast(12 as double))", "VARCHAR NOT NULL");
    f.enableTypeCoercion(false)
        .checkFails("^listagg(cast(12 as double))^",
            "Cannot apply 'LISTAGG' to arguments of type .*'\n.*'", false);
    f.checkFails("^listagg()^",
        "Invalid number of arguments to function 'LISTAGG'. Was expecting 1 arguments",
        false);
    f.checkFails("^listagg('1', '2', '3')^",
        "Invalid number of arguments to function 'LISTAGG'. Was expecting 1 arguments",
        false);
    f.checkAggType("listagg('test')", "CHAR(4) NOT NULL");
    f.checkAggType("listagg('test', ', ')", "CHAR(4) NOT NULL");
    final String[] values1 = {"'hello'", "CAST(null AS CHAR)", "'world'", "'!'"};
    f.checkAgg("listagg(x)", values1, isSingle("hello,world,!    "));
    final String[] values2 = {"0", "1", "2", "3"};
    f.checkAgg("listagg(cast(x as CHAR))", values2, isSingle("0,1,2,3"));
  }

  @Test void testStringAggFunc() {
    final SqlOperatorFixture f = fixture();
    checkStringAggFunc(f.withLibrary(SqlLibrary.POSTGRESQL));
    checkStringAggFunc(f.withLibrary(SqlLibrary.BIG_QUERY));
    checkStringAggFuncFails(f.withLibrary(SqlLibrary.MYSQL));
  }

  private static void checkStringAggFunc(SqlOperatorFixture f) {
    final String[] values = {"'x'", "null", "'yz'"};
    f.checkAgg("string_agg(x)", values, isSingle("x ,yz"));
    f.checkAgg("string_agg(x,':')", values, isSingle("x :yz"));
    f.checkAgg("string_agg(x,':' order by x)", values, isSingle("x :yz"));
    f.checkAgg("string_agg(x order by char_length(x) desc)", values, isSingle("x ,yz"));
    f.checkAggFails("^string_agg(x respect nulls order by x desc)^", values,
        "Cannot specify IGNORE NULLS or RESPECT NULLS following 'STRING_AGG'",
        false);
    f.checkAggFails("^string_agg(x order by x desc)^ respect nulls", values,
        "Cannot specify IGNORE NULLS or RESPECT NULLS following 'STRING_AGG'",
        false);
  }

  private static void checkStringAggFuncFails(SqlOperatorFixture f) {
    final String[] values = {"'x'", "'y'"};
    f.checkAggFails("^string_agg(x)^", values,
        "No match found for function signature STRING_AGG\\(<CHARACTER>\\)",
        false);
    f.checkAggFails("^string_agg(x, ',')^", values,
        "No match found for function signature STRING_AGG\\(<CHARACTER>, "
            + "<CHARACTER>\\)",
        false);
    f.checkAggFails("^string_agg(x, ',' order by x desc)^", values,
        "No match found for function signature STRING_AGG\\(<CHARACTER>, "
            + "<CHARACTER>\\)",
        false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6951">[CALCITE-6951]
   * Add STRING_TO_ARRAY function(enabled in PostgreSQL Library)</a>. */
  @Test void testStringToArrayFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.STRING_TO_ARRAY);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.POSTGRESQL);
    f.checkNull("string_to_array(NULL, ' ')");
    f.checkScalar("string_to_array('', '')", "[]",
        "CHAR(0) NOT NULL ARRAY NOT NULL");
    f.checkScalar("string_to_array('ab', NULL)",
        "[a, b]", "CHAR(2) NOT NULL ARRAY");
    f.checkScalar("string_to_array('www apache org', ' ')",
        "[www, apache, org]", "CHAR(14) NOT NULL ARRAY NOT NULL");
    f.checkScalar("string_to_array('www apache org', ' ', 'apache')",
        "[www, null, org]", "CHAR(14) NOT NULL ARRAY NOT NULL");
    f.checkFails("^string_to_array(NULL)^",
        "Invalid number of arguments to function 'STRING_TO_ARRAY'.*", false);
    f.checkFails("^string_to_array(NULL, NULL, NULL, NULL)^",
        "Invalid number of arguments to function 'STRING_TO_ARRAY'.*", false);
  }

  @Test void testGroupConcatFunc() {
    final SqlOperatorFixture f = fixture();
    checkGroupConcatFunc(f.withLibrary(SqlLibrary.MYSQL));
    checkGroupConcatFuncFails(f.withLibrary(SqlLibrary.BIG_QUERY));
    checkGroupConcatFuncFails(f.withLibrary(SqlLibrary.POSTGRESQL));
  }

  private static void checkGroupConcatFunc(SqlOperatorFixture f) {
    final String[] values = {"'x'", "null", "'yz'"};
    f.checkAgg("group_concat(x)", values, isSingle("x ,yz"));
    f.checkAgg("group_concat(x,':')", values, isSingle("x :yz"));
    f.checkAgg("group_concat(x,':' order by x)", values, isSingle("x :yz"));
    f.checkAgg("group_concat(x order by x separator '|')", values,
        isSingle("x |yz"));
    f.checkAgg("group_concat(x order by char_length(x) desc)", values,
        isSingle("x ,yz"));
    f.checkAggFails("^group_concat(x respect nulls order by x desc)^", values,
        "Cannot specify IGNORE NULLS or RESPECT NULLS following 'GROUP_CONCAT'",
        false);
    f.checkAggFails("^group_concat(x order by x desc)^ respect nulls", values,
        "Cannot specify IGNORE NULLS or RESPECT NULLS following 'GROUP_CONCAT'",
        false);
  }

  private static void checkGroupConcatFuncFails(SqlOperatorFixture t) {
    final String[] values = {"'x'", "'y'"};
    t.checkAggFails("^group_concat(x)^", values,
        "No match found for function signature GROUP_CONCAT\\(<CHARACTER>\\)",
        false);
    t.checkAggFails("^group_concat(x, ',')^", values,
        "No match found for function signature GROUP_CONCAT\\(<CHARACTER>, "
            + "<CHARACTER>\\)",
        false);
    t.checkAggFails("^group_concat(x, ',' order by x desc)^", values,
        "No match found for function signature GROUP_CONCAT\\(<CHARACTER>, "
            + "<CHARACTER>\\)",
        false);
  }

  @Test void testArrayAggFunc() {
    final SqlOperatorFixture f = fixture();
    checkArrayAggFunc(f.withLibrary(SqlLibrary.POSTGRESQL));
    checkArrayAggFunc(f.withLibrary(SqlLibrary.BIG_QUERY));
    checkArrayAggFuncFails(f.withLibrary(SqlLibrary.MYSQL));
  }

  private static void checkArrayAggFunc(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.ARRAY_AGG, VM_JAVA);
    final String[] values = {"'x'", "null", "'yz'"};
    f.checkAggType("array_agg(x)", "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkAgg("array_agg(x)", values, "CHAR(2) ARRAY", isSingle("[x , yz]"));
    f.checkAgg("array_agg(x ignore nulls)", values, "CHAR(2) ARRAY", isSingle("[x , yz]"));
    f.checkAgg("array_agg(x respect nulls)", values, "CHAR(2) ARRAY", isSingle("[x , yz]"));
    final String expectedError = "Invalid number of arguments "
        + "to function 'ARRAY_AGG'. Was expecting 1 arguments";
    f.checkAggFails("^array_agg(x,':')^", values, expectedError, false);
    f.checkAggFails("^array_agg(x,':' order by x)^", values, expectedError,
        false);
    f.checkAgg("array_agg(x order by char_length(x) desc)", values,
        isSingle("[x , yz]"));
  }

  private static void checkArrayAggFuncFails(SqlOperatorFixture t) {
    t.setFor(SqlLibraryOperators.ARRAY_AGG, VM_JAVA);
    final String[] values = {"'x'", "'y'"};
    final String expectedError = "No match found for function signature "
        + "ARRAY_AGG\\(<CHARACTER>\\)";
    final String expectedError2 = "No match found for function signature "
        + "ARRAY_AGG\\(<CHARACTER>, <CHARACTER>\\)";
    t.checkAggFails("^array_agg(x)^", values, expectedError, false);
    t.checkAggFails("^array_agg(x, ',')^", values, expectedError2, false);
    t.checkAggFails("^array_agg(x, ',' order by x desc)^", values,
        expectedError2, false);
  }

  @Test void testArrayConcatAggFunc() {
    final SqlOperatorFixture f = fixture();
    checkArrayConcatAggFunc(f.withLibrary(SqlLibrary.POSTGRESQL));
    checkArrayConcatAggFunc(f.withLibrary(SqlLibrary.BIG_QUERY));
    checkArrayConcatAggFuncFails(f.withLibrary(SqlLibrary.MYSQL));
  }

  private static void checkArrayConcatAggFunc(SqlOperatorFixture t) {
    t.setFor(SqlLibraryOperators.ARRAY_CONCAT_AGG, VM_JAVA);
    t.checkFails("array_concat_agg(^*^)",
        "(?s)Encountered \"\\*\" at .*", false);
    t.checkAggType("array_concat_agg(ARRAY[1,2,3])",
        "INTEGER NOT NULL ARRAY NOT NULL");

    final String expectedError = "Cannot apply 'ARRAY_CONCAT_AGG' to arguments "
        + "of type 'ARRAY_CONCAT_AGG\\(<INTEGER MULTISET>\\)'. Supported "
        + "form\\(s\\): 'ARRAY_CONCAT_AGG\\(<ARRAY>\\)'";
    t.checkFails("^array_concat_agg(multiset[1,2])^", expectedError, false);

    final String expectedError1 = "Cannot apply 'ARRAY_CONCAT_AGG' to "
        + "arguments of type 'ARRAY_CONCAT_AGG\\(<INTEGER>\\)'\\. Supported "
        + "form\\(s\\): 'ARRAY_CONCAT_AGG\\(<ARRAY>\\)'";
    t.checkFails("^array_concat_agg(12)^", expectedError1, false);

    final String[] values1 = {"ARRAY[0]", "ARRAY[1]", "ARRAY[2]", "ARRAY[3]"};
    t.checkAgg("array_concat_agg(x)", values1, isSingle("[0, 1, 2, 3]"));

    final String[] values2 = {"ARRAY[0,1]", "ARRAY[1, 2]"};
    t.checkAgg("array_concat_agg(x)", values2, isSingle("[0, 1, 1, 2]"));
  }

  private static void checkArrayConcatAggFuncFails(SqlOperatorFixture t) {
    t.setFor(SqlLibraryOperators.ARRAY_CONCAT_AGG, VM_JAVA);
    final String[] values = {"'x'", "'y'"};
    final String expectedError = "No match found for function signature "
        + "ARRAY_CONCAT_AGG\\(<CHARACTER>\\)";
    final String expectedError2 = "No match found for function signature "
        + "ARRAY_CONCAT_AGG\\(<CHARACTER>, <CHARACTER>\\)";
    t.checkAggFails("^array_concat_agg(x)^", values, expectedError, false);
    t.checkAggFails("^array_concat_agg(x, ',')^", values, expectedError2, false);
    t.checkAggFails("^array_concat_agg(x, ',' order by x desc)^", values,
        expectedError2, false);
  }

  @Test void testFusionFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.FUSION, VM_JAVA);
    f.checkFails("fusion(^*^)", "Unknown identifier '\\*'", false);
    f.checkAggType("fusion(MULTISET[1,2,3])", "INTEGER NOT NULL MULTISET NOT NULL");
    f.enableTypeCoercion(false).checkFails("^fusion(12)^",
        "Cannot apply 'FUSION' to arguments of type .*", false);
    final String[] values1 = {"MULTISET[0]", "MULTISET[1]", "MULTISET[2]", "MULTISET[3]"};
    f.checkAgg("fusion(x)", values1, isSingle("[0, 1, 2, 3]"));
    final String[] values2 = {"MULTISET[0,1]", "MULTISET[1, 2]"};
    f.checkAgg("fusion(x)", values2, isSingle("[0, 1, 1, 2]"));
  }

  @Test void testIntersectionFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.INTERSECTION, VM_JAVA);
    f.checkFails("intersection(^*^)", "Unknown identifier '\\*'", false);
    f.checkAggType("intersection(MULTISET[1,2,3])",
        "INTEGER NOT NULL MULTISET NOT NULL");
    f.enableTypeCoercion(false).checkFails("^intersection(12)^",
        "Cannot apply 'INTERSECTION' to arguments of type .*", false);
    final String[] values1 = {"MULTISET[0]", "MULTISET[1]", "MULTISET[2]",
        "MULTISET[3]"};
    f.checkAgg("intersection(x)", values1, isSingle("[]"));
    final String[] values2 = {"MULTISET[0, 1]", "MULTISET[1, 2]"};
    f.checkAgg("intersection(x)", values2, isSingle("[1]"));
    final String[] values3 = {"MULTISET[0, 1, 1]", "MULTISET[0, 1, 2]"};
    f.checkAgg("intersection(x)", values3, isSingle("[0, 1, 1]"));
  }

  @Test void testModeFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MODE, VM_EXPAND);
    f.checkFails("mode(^*^)", "Unknown identifier '\\*'", false);
    f.enableTypeCoercion(false)
        .checkFails("^mode()^",
            "Invalid number of arguments to function 'MODE'. "
                + "Was expecting 1 arguments",
            false);
    f.enableTypeCoercion(false)
        .checkFails("^mode(1,2)^",
            "Invalid number of arguments to function 'MODE'. "
                + "Was expecting 1 arguments",
            false);
    f.enableTypeCoercion(false)
        .checkFails("mode(^null^)", "Illegal use of 'NULL'", false);

    f.checkType("mode('name')", "CHAR(4)");
    f.checkAggType("mode(1)", "INTEGER NOT NULL");
    f.checkAggType("mode(1.2)", "DECIMAL(2, 1) NOT NULL");
    f.checkAggType("mode(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    f.checkType("mode(cast(null as varchar(2)))", "VARCHAR(2)");

    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2", "3", "3", "3" };
    f.checkAgg("mode(x)", values, isSingle("3"));
    final String[] values2 = {"0", null, null, null, "2", "2"};
    f.checkAgg("mode(x)", values2, isSingle("2"));
    final String[] values3 = {};
    f.checkAgg("mode(x)", values3, isNullValue());
    f.checkAgg("mode(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values, isSingle(-1));
    f.checkAgg("mode(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values, isSingle(-1));
    f.checkAgg("mode(DISTINCT x)", values, isSingle(0));
  }

  @Test void testYear() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.YEAR, VM_JAVA);

    f.checkScalar("year(date '2008-1-23')", "2008", "BIGINT NOT NULL");
    f.checkNull("year(cast(null as date))");
  }

  @Test void testQuarter() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.QUARTER, VM_JAVA);

    f.checkScalar("quarter(date '2008-1-23')", "1", "BIGINT NOT NULL");
    f.checkScalar("quarter(date '2008-2-23')", "1", "BIGINT NOT NULL");
    f.checkScalar("quarter(date '2008-3-23')", "1", "BIGINT NOT NULL");
    f.checkScalar("quarter(date '2008-4-23')", "2", "BIGINT NOT NULL");
    f.checkScalar("quarter(date '2008-5-23')", "2", "BIGINT NOT NULL");
    f.checkScalar("quarter(date '2008-6-23')", "2", "BIGINT NOT NULL");
    f.checkScalar("quarter(date '2008-7-23')", "3", "BIGINT NOT NULL");
    f.checkScalar("quarter(date '2008-8-23')", "3", "BIGINT NOT NULL");
    f.checkScalar("quarter(date '2008-9-23')", "3", "BIGINT NOT NULL");
    f.checkScalar("quarter(date '2008-10-23')", "4", "BIGINT NOT NULL");
    f.checkScalar("quarter(date '2008-11-23')", "4", "BIGINT NOT NULL");
    f.checkScalar("quarter(date '2008-12-23')", "4", "BIGINT NOT NULL");
    f.checkNull("quarter(cast(null as date))");
  }

  @Test void testMonth() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MONTH, VM_JAVA);

    f.checkScalar("month(date '2008-1-23')", "1", "BIGINT NOT NULL");
    f.checkNull("month(cast(null as date))");
  }

  @Test void testWeek() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.WEEK, VM_JAVA);
    f.checkScalar("week(date '2008-1-23')", "4", "BIGINT NOT NULL");
    f.checkNull("week(cast(null as date))");
  }

  @Test void testDayOfYear() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.DAYOFYEAR, VM_JAVA);
    f.checkScalar("dayofyear(date '2008-01-23')", "23", "BIGINT NOT NULL");
    f.checkNull("dayofyear(cast(null as date))");
  }

  @Test void testDayOfMonth() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.DAYOFMONTH, VM_JAVA);
    f.checkScalar("dayofmonth(date '2008-1-23')", "23", "BIGINT NOT NULL");
    f.checkNull("dayofmonth(cast(null as date))");
  }

  @Test void testDayOfWeek() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.DAYOFWEEK, VM_JAVA);
    f.checkScalar("dayofweek(date '2008-1-23')", "4", "BIGINT NOT NULL");
    f.checkNull("dayofweek(cast(null as date))");
  }

  @Test void testHour() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.HOUR, VM_JAVA);

    f.checkScalar("hour(timestamp '2008-1-23 12:34:56')", "12",
        "BIGINT NOT NULL");
    f.checkNull("hour(cast(null as timestamp))");
  }

  @Test void testMinute() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MINUTE, VM_JAVA);

    f.checkScalar("minute(timestamp '2008-1-23 12:34:56')", "34",
        "BIGINT NOT NULL");
    f.checkNull("minute(cast(null as timestamp))");
  }

  @Test void testSecond() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.SECOND, VM_JAVA);

    f.checkScalar("second(timestamp '2008-1-23 12:34:56')", "56",
        "BIGINT NOT NULL");
    f.checkNull("second(cast(null as timestamp))");
  }

  @Test void testExtractIntervalYearMonth() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EXTRACT, VM_JAVA);

    if (TODO) {
      f.checkScalar("extract(epoch from interval '4-2' year to month)",
          // number of seconds elapsed since timestamp
          // '1970-01-01 00:00:00' + input interval
          "131328000", "BIGINT NOT NULL");
    }
    f.checkScalar("extract(second from interval '4-2' year to month)",
        "0", "BIGINT NOT NULL");
    f.checkScalar("extract(millisecond from "
        + "interval '4-2' year to month)", "0", "BIGINT NOT NULL");
    f.checkScalar("extract(microsecond "
        + "from interval '4-2' year to month)", "0", "BIGINT NOT NULL");
    f.checkScalar("extract(nanosecond from "
        + "interval '4-2' year to month)", "0", "BIGINT NOT NULL");
    f.checkScalar("extract(minute from interval '4-2' year to month)",
        "0", "BIGINT NOT NULL");
    f.checkScalar("extract(hour from interval '4-2' year to month)",
        "0", "BIGINT NOT NULL");
    f.checkScalar("extract(day from interval '4-2' year to month)",
        "0", "BIGINT NOT NULL");

    final String fail = "Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<.*> "
        + "FROM <INTERVAL YEAR TO MONTH>\\)'\\. Supported form\\(s\\): "
        + ".*\\n.*\\n.*";

    f.checkFails("^extract(doy from interval '4-2' year to month)^", fail, false);
    f.checkFails("^extract(dow from interval '4-2' year to month)^", fail, false);
    f.checkFails("^extract(isodow from interval '4-2' year to month)^", fail, false);
    f.checkFails("^extract(week from interval '4-2' year to month)^", fail, false);

    f.checkScalar("extract(month from interval '4-2' year to month)",
        "2", "BIGINT NOT NULL");
    f.checkScalar("extract(quarter from interval '4-2' year to month)",
        "1", "BIGINT NOT NULL");
    f.checkScalar("extract(year from interval '4-2' year to month)",
        "4", "BIGINT NOT NULL");
    f.checkScalar("extract(decade from "
        + "interval '426-3' year(3) to month)", "42", "BIGINT NOT NULL");
    f.checkScalar("extract(century from "
        + "interval '426-3' year(3) to month)", "4", "BIGINT NOT NULL");
    f.checkScalar("extract(millennium from "
        + "interval '2005-3' year(4) to month)", "2", "BIGINT NOT NULL");
  }

  @Test void testExtractIntervalDayTime() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EXTRACT, VM_JAVA);

    f.checkScalar("extract(epoch from interval '2 3:4:5.678' day to second)",
        // number of seconds elapsed since timestamp
        // '1970-01-01 00:00:00' + input interval
        "183845",
        "BIGINT NOT NULL");

    f.checkScalar("extract(millisecond from "
            + "interval '2 3:4:5.678' day to second)",
        "5678",
        "BIGINT NOT NULL");

    f.checkScalar("extract(microsecond from "
            + "interval '2 3:4:5.678' day to second)",
        "5678000",
        "BIGINT NOT NULL");

    f.checkScalar("extract(nanosecond from "
            + "interval '2 3:4:5.678' day to second)",
        "5678000000",
        "BIGINT NOT NULL");

    f.checkScalar(
        "extract(second from interval '2 3:4:5.678' day to second)",
        "5",
        "BIGINT NOT NULL");

    f.checkScalar(
        "extract(minute from interval '2 3:4:5.678' day to second)",
        "4",
        "BIGINT NOT NULL");

    f.checkScalar(
        "extract(hour from interval '2 3:4:5.678' day to second)",
        "3",
        "BIGINT NOT NULL");

    f.checkScalar(
        "extract(day from interval '2 3:4:5.678' day to second)",
        "2",
        "BIGINT NOT NULL");

    final String fail = "Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<.*> "
        + "FROM <INTERVAL DAY TO SECOND>\\)'\\. Supported form\\(s\\): .*\\n.*\\n.*";

    f.checkFails("^extract(doy from interval '2 3:4:5.678' day to second)^", fail, false);
    f.checkFails("^extract(dow from interval '2 3:4:5.678' day to second)^", fail, false);
    f.checkFails("^extract(week from interval '2 3:4:5.678' day to second)^", fail, false);
    f.checkFails("^extract(isodow from interval '2 3:4:5.678' day to second)^", fail, false);
    f.checkFails("^extract(month from interval '2 3:4:5.678' day to second)^", fail, false);
    f.checkFails("^extract(quarter from interval '2 3:4:5.678' day to second)^", fail, false);
    f.checkFails("^extract(year from interval '2 3:4:5.678' day to second)^", fail, false);
    f.checkFails("^extract(isoyear from interval '2 3:4:5.678' day to second)^", fail, false);
    f.checkFails("^extract(century from interval '2 3:4:5.678' day to second)^", fail, false);
    f.checkFails("^extract(millennium from interval '2 3:4:5.678' day to second)^", fail, false);
  }

  @Test void testExtractDate() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EXTRACT, VM_JAVA);

    f.checkFails("extract(^a^ from date '2008-2-23')",
        "'A' is not a valid time frame", false);
    f.checkScalar("extract(epoch from date '2008-2-23')",
        "1203724800", // number of seconds elapsed since timestamp
        // '1970-01-01 00:00:00' for given date
        "BIGINT NOT NULL");

    f.checkScalar("extract(second from date '2008-2-23')",
        "0", "BIGINT NOT NULL");
    f.checkScalar("extract(millisecond from date '2008-2-23')",
        "0", "BIGINT NOT NULL");
    f.checkScalar("extract(microsecond from date '2008-2-23')",
        "0", "BIGINT NOT NULL");
    f.checkScalar("extract(nanosecond from date '2008-2-23')",
        "0", "BIGINT NOT NULL");
    f.checkScalar("extract(minute from date '9999-2-23')",
        "0", "BIGINT NOT NULL");
    f.checkScalar("extract(minute from date '0001-1-1')",
        "0", "BIGINT NOT NULL");
    f.checkScalar("extract(minute from date '2008-2-23')",
        "0", "BIGINT NOT NULL");
    f.checkScalar("extract(hour from date '2008-2-23')",
        "0", "BIGINT NOT NULL");
    f.checkScalar("extract(day from date '2008-2-23')",
        "23", "BIGINT NOT NULL");
    f.checkScalar("extract(month from date '2008-2-23')",
        "2", "BIGINT NOT NULL");
    f.checkScalar("extract(quarter from date '2008-4-23')",
        "2", "BIGINT NOT NULL");
    f.checkScalar("extract(year from date '2008-2-23')",
        "2008", "BIGINT NOT NULL");
    f.checkScalar("extract(isoyear from date '2008-2-23')",
        "2008", "BIGINT NOT NULL");

    f.checkScalar("extract(doy from date '2008-2-23')",
        "54", "BIGINT NOT NULL");
    f.checkScalar("extract(dayofyear from date '2008-2-23')",
        "54", "BIGINT NOT NULL");
    f.checkScalar("extract(dow from date '2008-2-23')",
        "7", "BIGINT NOT NULL");
    f.checkScalar("extract(dow from date '2008-2-24')",
        "1", "BIGINT NOT NULL");
    f.checkScalar("extract(dayofweek from date '2008-2-23')",
        "7", "BIGINT NOT NULL");
    f.checkScalar("extract(isodow from date '2008-2-23')",
        "6", "BIGINT NOT NULL");
    f.checkScalar("extract(isodow from date '2008-2-24')",
        "7", "BIGINT NOT NULL");
    f.checkScalar("extract(week from date '2008-2-23')",
        "8", "BIGINT NOT NULL");
    f.checkScalar("extract(week from timestamp '2008-2-23 01:23:45')",
        "8", "BIGINT NOT NULL");
    f.checkScalar("extract(week from cast(null as date))",
        isNullValue(), "BIGINT");

    f.checkScalar("extract(decade from date '2008-2-23')",
        "200", "BIGINT NOT NULL");

    f.checkScalar("extract(century from date '2008-2-23')",
        "21", "BIGINT NOT NULL");
    f.checkScalar("extract(century from date '2001-01-01')",
        "21", "BIGINT NOT NULL");
    f.checkScalar("extract(century from date '2000-12-31')",
        "20", "BIGINT NOT NULL");
    f.checkScalar("extract(century from date '1852-06-07')",
        "19", "BIGINT NOT NULL");
    f.checkScalar("extract(century from date '0001-02-01')",
        "1", "BIGINT NOT NULL");

    f.checkScalar("extract(millennium from date '2000-2-23')",
        "2", "BIGINT NOT NULL");
    f.checkScalar("extract(millennium from date '1969-2-23')",
        "2", "BIGINT NOT NULL");
    f.checkScalar("extract(millennium from date '2000-12-31')",
        "2", "BIGINT NOT NULL");
    f.checkScalar("extract(millennium from date '2001-01-01')",
        "3", "BIGINT NOT NULL");
  }

  @Test void testExtractTime() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EXTRACT, VM_JAVA);

    final String fail = "Cannot apply 'EXTRACT' to arguments of type 'EXTRACT\\(<.*> "
        + "FROM <TIME\\(0\\)>\\)'\\. "
        + "Supported form\\(s\\): .*\\n.*\\n.*";

    f.checkFails("extract(^a^ from time '12:34:56')",
        "'A' is not a valid time frame", false);
    f.checkFails("^extract(epoch from time '12:34:56')^",
        fail, false);

    f.checkScalar("extract(second from time '12:34:56')",
        "56", "BIGINT NOT NULL");
    f.checkScalar("extract(millisecond from time '12:34:56')",
        "56000", "BIGINT NOT NULL");
    f.checkScalar("extract(microsecond from time '12:34:56')",
        "56000000", "BIGINT NOT NULL");
    f.checkScalar("extract(nanosecond from time '12:34:56')",
        "56000000000", "BIGINT NOT NULL");
    f.checkScalar("extract(minute from time '12:34:56')",
        "34", "BIGINT NOT NULL");
    f.checkScalar("extract(hour from time '12:34:56')",
        "12", "BIGINT NOT NULL");
    f.checkFails("^extract(day from time '12:34:56')^", fail, false);
    f.checkFails("^extract(month from time '12:34:56')^", fail, false);
    f.checkFails("^extract(quarter from time '12:34:56')^", fail, false);
    f.checkFails("^extract(year from time '12:34:56')^", fail, false);
    f.checkFails("^extract(isoyear from time '12:34:56')^", fail, false);
    f.checkFails("^extract(doy from time '12:34:56')^", fail, false);
    f.checkFails("^extract(dow from time '12:34:56')^", fail, false);
    f.checkFails("^extract(week from time '12:34:56')^", fail, false);
    f.checkFails("^extract(decade from time '12:34:56')^", fail, false);
    f.checkFails("^extract(century from time '12:34:56')^", fail, false);
    f.checkFails("^extract(millennium from time '12:34:56')^", fail, false);
  }

  @Test void testExtractTimestamp() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EXTRACT, VM_JAVA);

    f.checkFails("extract(^a^ from timestamp '2008-2-23 12:34:56')",
        "'A' is not a valid time frame", false);
    f.checkScalar("extract(epoch from timestamp '2008-2-23 12:34:56')",
        "1203770096", // number of seconds elapsed since timestamp
        // '1970-01-01 00:00:00' for given date
        "BIGINT NOT NULL");

    f.checkScalar("extract(second from timestamp '2008-2-23 12:34:56')",
        "56", "BIGINT NOT NULL");
    f.checkScalar("extract(millisecond from timestamp '2008-2-23 12:34:56')",
        "56000", "BIGINT NOT NULL");
    f.checkScalar("extract(microsecond from timestamp '2008-2-23 12:34:56')",
        "56000000", "BIGINT NOT NULL");
    f.checkScalar("extract(nanosecond from timestamp '2008-2-23 12:34:56')",
        "56000000000", "BIGINT NOT NULL");
    f.checkScalar("extract(minute from timestamp '2008-2-23 12:34:56')",
        "34", "BIGINT NOT NULL");
    f.checkScalar("extract(hour from timestamp '2008-2-23 12:34:56')",
        "12", "BIGINT NOT NULL");
    f.checkScalar("extract(day from timestamp '2008-2-23 12:34:56')",
        "23", "BIGINT NOT NULL");
    f.checkScalar("extract(month from timestamp '2008-2-23 12:34:56')",
        "2", "BIGINT NOT NULL");
    f.checkScalar("extract(quarter from timestamp '2008-7-23 12:34:56')",
        "3", "BIGINT NOT NULL");
    f.checkScalar("extract(year from timestamp '2008-2-23 12:34:56')",
        "2008", "BIGINT NOT NULL");
    f.checkScalar("extract(isoyear from timestamp '2008-2-23 12:34:56')",
        "2008", "BIGINT NOT NULL");
    f.checkScalar("extract(doy from timestamp '2008-2-23 12:34:56')",
        "54", "BIGINT NOT NULL");
    f.checkScalar("extract(dow from timestamp '2008-2-23 12:34:56')",
        "7", "BIGINT NOT NULL");
    f.checkScalar("extract(week from timestamp '2008-2-23 12:34:56')",
        "8", "BIGINT NOT NULL");
    f.checkScalar("extract(decade from timestamp '2008-2-23 12:34:56')",
        "200", "BIGINT NOT NULL");
    f.checkScalar("extract(century from timestamp '2008-2-23 12:34:56')",
        "21", "BIGINT NOT NULL");
    f.checkScalar("extract(century from timestamp '2001-01-01 12:34:56')",
        "21", "BIGINT NOT NULL");
    f.checkScalar("extract(century from timestamp '2000-12-31 12:34:56')",
        "20", "BIGINT NOT NULL");
    f.checkScalar("extract(millennium from timestamp '2008-2-23 12:34:56')",
        "3", "BIGINT NOT NULL");
    f.checkScalar("extract(millennium from timestamp '2000-2-23 12:34:56')",
        "2", "BIGINT NOT NULL");
  }

  @Test void testExtractInterval() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EXTRACT, VM_JAVA);

    f.checkFails("extract(^a^ from interval '2 3:4:5.678' day to second)",
        "'A' is not a valid time frame", false);
    f.checkScalar("extract(day from interval '2 3:4:5.678' day to second)",
        "2", "BIGINT NOT NULL");
    f.checkScalar("extract(day from interval -'2 3:4:5.678' day to second)",
        "-2", "BIGINT NOT NULL");
    f.checkScalar("extract(day from interval '23456 3:4:5.678' day(5) to second)",
        "23456", "BIGINT NOT NULL");
    f.checkScalar("extract(hour from interval '2 3:4:5.678' day to second)",
        "3", "BIGINT NOT NULL");
    f.checkScalar("extract(minute from interval '2 3:4:5.678' day to second)",
        "4", "BIGINT NOT NULL");

    // TODO: Seconds should include precision
    f.checkScalar("extract(second from interval '2 3:4:5.678' day to second)",
        "5", "BIGINT NOT NULL");
    f.checkScalar("extract(millisecond from"
            + " interval '2 3:4:5.678' day to second)",
        "5678", "BIGINT NOT NULL");
    f.checkScalar("extract(microsecond from"
            + " interval '2 3:4:5.678' day to second)",
        "5678000", "BIGINT NOT NULL");
    f.checkScalar("extract(microsecond from"
            + " interval -'2 3:4:5.678' day to second)",
        "-5678000", "BIGINT NOT NULL");
    f.checkScalar("extract(nanosecond from"
            + " interval '2 3:4:5.678' day to second)",
        "5678000000", "BIGINT NOT NULL");
    f.checkNull("extract(month from cast(null as interval year))");
  }

  @Test void testExtractFuncFromDateTime() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EXTRACT, VM_JAVA);
    f.checkScalar("extract(year from date '2008-2-23')",
        "2008", "BIGINT NOT NULL");
    f.checkScalar("extract(isoyear from date '2008-2-23')",
        "2008", "BIGINT NOT NULL");
    f.checkScalar("extract(month from date '2008-2-23')",
        "2", "BIGINT NOT NULL");
    f.checkScalar("extract(month from timestamp '2008-2-23 12:34:56')",
        "2", "BIGINT NOT NULL");
    f.checkScalar("extract(minute from timestamp '2008-2-23 12:34:56')",
        "34", "BIGINT NOT NULL");
    f.checkScalar("extract(minute from time '12:23:34')",
        "23", "BIGINT NOT NULL");
    f.checkNull("extract(month from cast(null as timestamp))");
    f.checkNull("extract(month from cast(null as date))");
    f.checkNull("extract(second from cast(null as time))");
    f.checkNull("extract(millisecond from cast(null as time))");
    f.checkNull("extract(microsecond from cast(null as time))");
    f.checkNull("extract(nanosecond from cast(null as time))");
  }

  @Test void testExtractWithDatesBeforeUnixEpoch() {
    final SqlOperatorFixture f = fixture();
    f.checkScalar("extract(millisecond from"
            + " TIMESTAMP '1969-12-31 21:13:17.357')",
        "17357", "BIGINT NOT NULL");
    f.checkScalar("extract(year from TIMESTAMP '1970-01-01 00:00:00')",
        "1970", "BIGINT NOT NULL");
    f.checkScalar("extract(year from TIMESTAMP '1969-12-31 10:13:17')",
        "1969", "BIGINT NOT NULL");
    f.checkScalar("extract(quarter from TIMESTAMP '1969-12-31 08:13:17')",
        "4", "BIGINT NOT NULL");
    f.checkScalar("extract(quarter from TIMESTAMP '1969-5-31 21:13:17')",
        "2", "BIGINT NOT NULL");
    f.checkScalar("extract(month from TIMESTAMP '1969-12-31 00:13:17')",
        "12", "BIGINT NOT NULL");
    f.checkScalar("extract(day from TIMESTAMP '1969-12-31 12:13:17')",
        "31", "BIGINT NOT NULL");
    f.checkScalar("extract(week from TIMESTAMP '1969-2-23 01:23:45')",
        "8", "BIGINT NOT NULL");
    f.checkScalar("extract(doy from TIMESTAMP '1969-12-31 21:13:17.357')",
        "365", "BIGINT NOT NULL");
    f.checkScalar("extract(dow from TIMESTAMP '1969-12-31 01:13:17.357')",
        "4", "BIGINT NOT NULL");
    f.checkScalar("extract(decade from TIMESTAMP '1969-12-31 21:13:17.357')",
        "196", "BIGINT NOT NULL");
    f.checkScalar("extract(century from TIMESTAMP '1969-12-31 21:13:17.357')",
        "20", "BIGINT NOT NULL");
    f.checkScalar("extract(hour from TIMESTAMP '1969-12-31 21:13:17.357')",
        "21", "BIGINT NOT NULL");
    f.checkScalar("extract(minute from TIMESTAMP '1969-12-31 21:13:17.357')",
        "13", "BIGINT NOT NULL");
    f.checkScalar("extract(second from TIMESTAMP '1969-12-31 21:13:17.357')",
        "17", "BIGINT NOT NULL");
    f.checkScalar("extract(millisecond from"
            + " TIMESTAMP '1969-12-31 21:13:17.357')",
        "17357", "BIGINT NOT NULL");
    f.checkScalar("extract(microsecond from"
            + " TIMESTAMP '1969-12-31 21:13:17.357')",
        "17357000", "BIGINT NOT NULL");
  }

  @Test void testArrayValueConstructor() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, VmName.EXPAND);
    f.checkScalar("Array['foo']",
        "[foo]", "CHAR(3) NOT NULL ARRAY NOT NULL");
    f.checkScalar("Array['foo', 'bar']",
        "[foo, bar]", "CHAR(3) NOT NULL ARRAY NOT NULL");
    f.checkScalar("Array['foo', null]",
        "[foo, null]", "CHAR(3) ARRAY NOT NULL");
    f.checkScalar("Array[null]",
        "[null]", "NULL ARRAY NOT NULL");
    // element cast
    f.checkScalar("Array[cast(1 as tinyint), cast(2 as smallint)]",
        "[1, 2]", "SMALLINT NOT NULL ARRAY NOT NULL");
    f.checkScalar("Array[1, cast(2 as tinyint), cast(3 as smallint)]",
        "[1, 2, 3]", "INTEGER NOT NULL ARRAY NOT NULL");
    f.checkScalar("Array[1, cast(2 as tinyint), cast(3 as smallint), cast(4 as bigint)]",
        "[1, 2, 3, 4]", "BIGINT NOT NULL ARRAY NOT NULL");
    // empty array is illegal per SQL spec. presumably because one can't
    // infer type
    f.checkFails("^Array[]^", "Require at least 1 argument", false);
    f.checkFails("^array[1, '1', true]^", "Parameters must be of the same type", false);
  }

  /** Test case for {@link SqlLibraryOperators#ARRAY} (Spark, Hive). */
  @Test void testArrayFunction() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.ARRAY, VmName.EXPAND);
    final List<SqlLibrary> libraries =
        ImmutableList.of(SqlLibrary.HIVE, SqlLibrary.SPARK);
    f.checkFails("^array()^",
        "No match found for function signature ARRAY\\(\\)", false);
    f.checkFails("^array('foo')^",
        "No match found for function signature ARRAY\\(<CHARACTER>\\)", false);
    final Consumer<SqlOperatorFixture> consumer = f2 -> {
      f2.checkScalar("array('foo')",
          "[foo]", "CHAR(3) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array('foo', 'bar')",
          "[foo, bar]", "CHAR(3) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array()",
          "[]", "UNKNOWN NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array('foo', null)",
          "[foo, null]", "CHAR(3) ARRAY NOT NULL");
      f2.checkScalar("array(null, 'foo')",
          "[null, foo]", "CHAR(3) ARRAY NOT NULL");
      f2.checkScalar("array(null)",
          "[null]", "NULL ARRAY NOT NULL");
      // check complex type
      f2.checkScalar("array(row(1))", "[{1}]",
          "RecordType(INTEGER NOT NULL EXPR$0) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array(row(1, null))", "[{1, null}]",
          "RecordType(INTEGER NOT NULL EXPR$0, NULL EXPR$1) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array(row(null, 1))", "[{null, 1}]",
          "RecordType(NULL EXPR$0, INTEGER NOT NULL EXPR$1) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array(row(1, 2))", "[{1, 2}]",
          "RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array(row(1, 2), null)",
          "[{1, 2}, null]", "RecordType(INTEGER EXPR$0, INTEGER EXPR$1) ARRAY NOT NULL");
      f2.checkScalar("array(null, row(1, 2))",
          "[null, {1, 2}]", "RecordType(INTEGER EXPR$0, INTEGER EXPR$1) ARRAY NOT NULL");
      f2.checkScalar("array(row(1, null), row(2, null))", "[{1, null}, {2, null}]",
          "RecordType(INTEGER NOT NULL EXPR$0, NULL EXPR$1) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array(row(null, 1), row(null, 2))", "[{null, 1}, {null, 2}]",
          "RecordType(NULL EXPR$0, INTEGER NOT NULL EXPR$1) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array(row(1, null), row(null, 2))", "[{1, null}, {null, 2}]",
          "RecordType(INTEGER EXPR$0, INTEGER EXPR$1) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array(row(null, 1), row(2, null))", "[{null, 1}, {2, null}]",
          "RecordType(INTEGER EXPR$0, INTEGER EXPR$1) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array(row(1, 2), row(3, 4))", "[{1, 2}, {3, 4}]",
          "RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1) NOT NULL ARRAY NOT NULL");
      // checkFails
      f2.checkFails("^array(row(1), row(2, 3))^",
          "Parameters must be of the same type", false);
      f2.checkFails("^array(row(1), row(2, 3), null)^",
          "Parameters must be of the same type", false);
      // calcite default cast char type will fill extra spaces
      f2.checkScalar("array(1, 2, 'Hi')",
          "[1 , 2 , Hi]", "CHAR(2) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array(1, 2, 'Hi', 'Hello')",
          "[1    , 2    , Hi   , Hello]", "CHAR(5) NOT NULL ARRAY NOT NULL");
      f2.checkScalar("array(1, 2, 'Hi', null)",
          "[1 , 2 , Hi, null]", "CHAR(2) ARRAY NOT NULL");
      f2.checkScalar("array(1, 2, 'Hi', cast(null as char(10)))",
          "[1         , 2         , Hi        , null]", "CHAR(10) ARRAY NOT NULL");
    };
    f.forEachLibrary(libraries, consumer);
  }

  @Test void testArrayQueryConstructor() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ARRAY_QUERY, SqlOperatorFixture.VmName.EXPAND);

    // Test case for [CALCITE-4999] ARRAY, MULTISET functions should
    // return a collection of scalars if a sub-query returns 1 column
    f.checkScalar("array(select 1)", "[1]",
        "INTEGER NOT NULL ARRAY NOT NULL");
    f.check("select array(select ROW(1,2))",
        "RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1) NOT NULL ARRAY NOT NULL",
        "[{1, 2}]");

    // single sub-clause test
    f.check("select array(select x from unnest(array[1,2,3,4]) as t(x))",
        "INTEGER NOT NULL ARRAY NOT NULL", "[1, 2, 3, 4]");
    f.check("select array(select x from unnest(array[1,2,3,4]) as t(x) where x > 1)",
        "INTEGER NOT NULL ARRAY NOT NULL", "[2, 3, 4]");
    f.check("select array(select x from unnest(array[1,2,3,4]) as t(x) limit 2)",
        "INTEGER NOT NULL ARRAY NOT NULL", "[1, 2]");
    f.check("select array(select x from unnest(array[1,2,3,4]) as t(x) where x > 1 limit 2)",
        "INTEGER NOT NULL ARRAY NOT NULL", "[2, 3]");

    // combined tests
    f.check("select array(select x from unnest(array[1,2,3,4]) as t(x) "
        + "order by x asc)", "INTEGER NOT NULL ARRAY NOT NULL", "[1, 2, 3, 4]");
    f.check("select array(select x from unnest(array[1,2,3,4]) as t(x) "
        + "where x > 1 order by x asc)", "INTEGER NOT NULL ARRAY NOT NULL", "[2, 3, 4]");
    f.check("select array(select x from unnest(array[1,2,3,4]) as t(x) "
        + "where x > 1 order by x asc limit 2)", "INTEGER NOT NULL ARRAY NOT NULL", "[2, 3]");
    f.check("select array(select x from unnest(array[1,2,3,4]) as t(x) "
        + "order by x desc)", "INTEGER NOT NULL ARRAY NOT NULL", "[4, 3, 2, 1]");
    f.check("select array(select x from unnest(array[1,2,3,4]) as t(x) "
        + "where x > 1 order by x desc)", "INTEGER NOT NULL ARRAY NOT NULL", "[4, 3, 2]");
    f.check("select array(select x from unnest(array[1,2,3,4]) as t(x) "
        + "where x > 1 order by x desc limit 2)", "INTEGER NOT NULL ARRAY NOT NULL", "[4, 3]");
  }

  @Test void testMultisetQueryConstructor() {
    final SqlOperatorFixture f = fixture();

    // Test case for [CALCITE-4999] ARRAY, MULTISET functions should
    // return an collection of scalars if a sub-query returns 1 column
    f.setFor(SqlStdOperatorTable.MULTISET_QUERY, SqlOperatorFixture.VmName.EXPAND);
    f.checkScalar("multiset(select 1)", "[1]", "INTEGER NOT NULL MULTISET NOT NULL");
    f.check("select multiset(select ROW(1,2))",
        "RecordType(INTEGER NOT NULL EXPR$0, INTEGER NOT NULL EXPR$1) NOT NULL MULTISET NOT NULL",
        "[{1, 2}]");

    // test filter, orderby, limit
    // multiset subquery not support orderby and limit sub-clause
    f.check("select multiset(select x from unnest(array[1,2,3,4]) as t(x))",
        "INTEGER NOT NULL MULTISET NOT NULL", "[1, 2, 3, 4]");
    f.check("select multiset(select x from unnest(array[1,2,3,4]) as t(x) where x > 1)",
        "INTEGER NOT NULL MULTISET NOT NULL", "[2, 3, 4]");

    f.checkFails("select multiset(select x from unnest(array[1,2,3,4]) as t(x) ^order^ by x)",
        "(?s).*Encountered \"order\" at .*", false);
    f.checkFails("select multiset(select x from unnest(array[1,2,3,4]) as t(x) ^limit^ 2)",
        "(?s).*Encountered \"limit\" at .*", false);
    f.checkFails("select multiset(select x from unnest(array[1,2,3,4]) as t(x) "
        + "^order^ by x limit 2)", "(?s).*Encountered \"order\" at .*", false);
    f.checkFails("select multiset(select x from unnest(array[1,2,3,4]) as t(x) where x > 1 "
        + "^order^ by x limit 2)", "(?s).*Encountered \"order\" at .*", false);
  }

  @Test void testItemOp() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ITEM, VmName.EXPAND);
    f.checkScalar("ARRAY ['foo', 'bar'][1]", "foo", "CHAR(3)");
    f.checkScalar("ARRAY ['foo', 'bar'][0]", isNullValue(), "CHAR(3)");
    f.checkScalar("ARRAY ['foo', 'bar'][2]", "bar", "CHAR(3)");
    f.checkScalar("ARRAY ['foo', 'bar'][3]", isNullValue(), "CHAR(3)");
    f.checkNull("ARRAY ['foo', 'bar'][1 + CAST(NULL AS INTEGER)]");
    f.checkFails("^ARRAY ['foo', 'bar']['baz']^",
        "Cannot apply 'ITEM' to arguments of type 'ITEM\\(<CHAR\\(3\\) ARRAY>, "
            + "<CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): <ARRAY>\\[<INTEGER>\\]\n"
            + "<MAP>\\[<ANY>\\]\n"
            + "<ROW>\\[<CHARACTER>\\|<INTEGER>\\]\n"
            + "<VARIANT>\\[<ANY>\\]",
        false);

    // Array of INTEGER NOT NULL is interesting because we might be tempted
    // to represent the result as Java "int".
    f.checkScalar("ARRAY [2, 4, 6][2]", "4", "INTEGER");
    f.checkScalar("ARRAY [2, 4, 6][4]", isNullValue(), "INTEGER");

    // Map item
    f.checkScalarExact("map['foo', 3, 'bar', 7]['bar']", "INTEGER", "7");
    f.checkScalarExact("map['foo', CAST(NULL AS INTEGER), 'bar', 7]"
        + "['bar']", "INTEGER", "7");
    f.checkScalarExact("map['foo', CAST(NULL AS INTEGER), 'bar', 7]['baz']",
        "INTEGER", isNullValue());
    f.checkColumnType("select cast(null as any)['x'] from (values(1))",
        "ANY");

    // Row item
    final String intStructQuery = "select \"T\".\"X\"[1] "
        + "from (VALUES (ROW(ROW(3, 7), ROW(4, 8)))) as T(x, y)";
    f.check(intStructQuery, SqlTests.INTEGER_TYPE_CHECKER, 3);
    f.checkColumnType(intStructQuery, "INTEGER NOT NULL");

    f.check("select \"T\".\"X\"[1] "
            + "from (VALUES (ROW(ROW(3, CAST(NULL AS INTEGER)), ROW(4, 8)))) as T(x, y)",
        SqlTests.INTEGER_TYPE_CHECKER, 3);
    f.check("select \"T\".\"X\"[2] "
            + "from (VALUES (ROW(ROW(3, CAST(NULL AS INTEGER)), ROW(4, 8)))) as T(x, y)",
        SqlTests.ANY_TYPE_CHECKER, isNullValue());
    f.checkFails("select ^\"T\".\"X\"[1 + CAST(NULL AS INTEGER)]^ "
            + "from (VALUES (ROW(ROW(3, CAST(NULL AS INTEGER)), ROW(4, 8)))) as T(x, y)",
        "Index in ROW type does not have a constant integer or string value", false);
  }

  @Test void testOffsetOperator() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.OFFSET);
    f0.checkFails("^ARRAY[2,4,6][OFFSET(2)]^",
        "No match found for function signature OFFSET", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("ARRAY[2,4,6][OFFSET(2)]", "6", "INTEGER");
    f.checkScalar("ARRAY[2,4,6][OFFSET(0)]", "2", "INTEGER");
    f.checkScalar("ARRAY[2,4,6,8,10][OFFSET(1+2)]", "8", "INTEGER");
    f.checkNull("ARRAY[2,4,6][OFFSET(null)]");
    f.checkFails("ARRAY[2,4,6][OFFSET(-1)]",
        "Array index -1 is out of bounds", true);
    f.checkFails("ARRAY[2,4,6][OFFSET(5)]",
        "Array index 5 is out of bounds", true);
    f.checkFails("^map['foo', 3, 'bar', 7][offset('bar')]^",
        "Cannot apply 'OFFSET' to arguments of type 'OFFSET\\(<\\(CHAR\\(3\\)"
            + ", INTEGER\\) MAP>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
            + "<ARRAY>\\[OFFSET\\(<INTEGER>\\)\\]", false);
  }

  @Test void testOrdinalOperator() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.ORDINAL);
    f0.checkFails("^ARRAY[2,4,6][ORDINAL(2)]^",
        "No match found for function signature ORDINAL", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("ARRAY[2,4,6][ORDINAL(3)]", "6", "INTEGER");
    f.checkScalar("ARRAY[2,4,6][ORDINAL(1)]", "2", "INTEGER");
    f.checkScalar("ARRAY[2,4,6,8,10][ORDINAL(1+2)]", "6", "INTEGER");
    f.checkNull("ARRAY[2,4,6][ORDINAL(null)]");
    f.checkFails("ARRAY[2,4,6][ORDINAL(-1)]",
        "Array index -1 is out of bounds", true);
    f.checkFails("ARRAY[2,4,6][ORDINAL(5)]",
        "Array index 5 is out of bounds", true);
    f.checkFails("^map['foo', 3, 'bar', 7][ordinal('bar')]^",
        "Cannot apply 'ORDINAL' to arguments of type 'ORDINAL\\(<\\(CHAR\\(3\\)"
            + ", INTEGER\\) MAP>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
            + "<ARRAY>\\[ORDINAL\\(<INTEGER>\\)\\]", false);
  }

  @Test void testSafeOffsetOperator() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.SAFE_OFFSET);
    f0.checkFails("^ARRAY[2,4,6][SAFE_OFFSET(2)]^",
        "No match found for function signature SAFE_OFFSET", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("ARRAY[2,4,6][SAFE_OFFSET(2)]", "6", "INTEGER");
    f.checkScalar("ARRAY[2,4,6][SAFE_OFFSET(0)]", "2", "INTEGER");
    f.checkScalar("ARRAY[2,4,6,8,10][SAFE_OFFSET(1+2)]", "8", "INTEGER");
    f.checkScalar("ARRAY[2,4,6][SAFE_OFFSET(-1)]", isNullValue(), "INTEGER");
    f.checkScalar("ARRAY[2,4,6][SAFE_OFFSET(5)]", isNullValue(), "INTEGER");
    f.checkNull("ARRAY[2,4,6][SAFE_OFFSET(null)]");
    f.checkFails("^map['foo', 3, 'bar', 7][safe_offset('bar')]^",
        "Cannot apply 'SAFE_OFFSET' to arguments of type 'SAFE_OFFSET\\(<\\(CHAR\\(3\\)"
            + ", INTEGER\\) MAP>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
            + "<ARRAY>\\[SAFE_OFFSET\\(<INTEGER>\\)\\]", false);
  }

  @Test void testSafeOrdinalOperator() {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(SqlLibraryOperators.SAFE_ORDINAL);
    f0.checkFails("^ARRAY[2,4,6][SAFE_ORDINAL(2)]^",
        "No match found for function signature SAFE_ORDINAL", false);
    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("ARRAY[2,4,6][SAFE_ORDINAL(3)]", "6", "INTEGER");
    f.checkScalar("ARRAY[2,4,6][SAFE_ORDINAL(1)]", "2", "INTEGER");
    f.checkScalar("ARRAY[2,4,6,8,10][SAFE_ORDINAL(1+2)]", "6", "INTEGER");
    f.checkScalar("ARRAY[2,4,6][SAFE_ORDINAL(-1)]", isNullValue(), "INTEGER");
    f.checkScalar("ARRAY[2,4,6][SAFE_ORDINAL(5)]", isNullValue(), "INTEGER");
    f.checkNull("ARRAY[2,4,6][SAFE_ORDINAL(null)]");
    f.checkFails("^map['foo', 3, 'bar', 7][safe_ordinal('bar')]^",
        "Cannot apply 'SAFE_ORDINAL' to arguments of type 'SAFE_ORDINAL\\(<\\(CHAR\\(3\\)"
            + ", INTEGER\\) MAP>, <CHAR\\(3\\)>\\)'\\. Supported form\\(s\\): "
            + "<ARRAY>\\[SAFE_ORDINAL\\(<INTEGER>\\)\\]", false);
  }

  @Test void testMapValueConstructor() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, VM_JAVA);

    f.checkFails("^Map[]^", "Map requires at least 2 arguments", false);
    f.checkFails("^Map[1, 'x', 2]^",
        "Map requires an even number of arguments", false);
    f.checkFails("^map[1, 1, 2, 'x']^",
        "Parameters must be of the same type", false);
    f.checkScalar("map['washington', 1, 'obama', 44]",
        "{washington=1, obama     =44}",
        "(CHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");

    // check null key or null value
    f.checkScalar("map['foo', null]",
        "{foo=null}",
        "(CHAR(3) NOT NULL, NULL) MAP NOT NULL");
    f.checkScalar("map[null, 'foo']",
        "{null=foo}",
        "(NULL, CHAR(3) NOT NULL) MAP NOT NULL");
    f.checkScalar("map[1, 'foo', 2, null]",
        "{1=foo, 2=null}",
        "(INTEGER NOT NULL, CHAR(3)) MAP NOT NULL");
    f.checkScalar("map[1, null, 2, 'foo']",
        "{1=null, 2=foo}",
        "(INTEGER NOT NULL, CHAR(3)) MAP NOT NULL");
    f.checkScalar("map[1, null, 2, null]",
        "{1=null, 2=null}",
        "(INTEGER NOT NULL, NULL) MAP NOT NULL");
    f.checkScalar("map[null, 1, null, 2]",
        "{null=2}",
        "(NULL, INTEGER NOT NULL) MAP NOT NULL");

    // elements cast
    f.checkScalar("map['A', 1, 'ABC', 2]", "{A  =1, ABC=2}",
        "(CHAR(3) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f.checkScalar("Map[cast(1 as tinyint), 1, cast(2 as smallint), 2]",
        "{1=1, 2=2}", "(SMALLINT NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f.checkScalar("Map[1, cast(1 as tinyint), 2, cast(2 as smallint)]",
        "{1=1, 2=2}", "(INTEGER NOT NULL, SMALLINT NOT NULL) MAP NOT NULL");
    f.checkScalar("Map[1, cast(1 as tinyint), cast(2 as bigint), cast(2 as smallint)]",
        "{1=1, 2=2}", "(BIGINT NOT NULL, SMALLINT NOT NULL) MAP NOT NULL");
    f.checkScalar("Map[cast(1 as bigint), cast(1 as tinyint), 2, cast(2 as smallint)]",
        "{1=1, 2=2}", "(BIGINT NOT NULL, SMALLINT NOT NULL) MAP NOT NULL");

    final SqlOperatorFixture f1 =
        f.withConformance(SqlConformanceEnum.PRAGMATIC_2003);
    f1.checkScalar("map['washington', 1, 'obama', 44]",
        "{washington=1, obama=44}",
        "(VARCHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    // elements cast
    f1.checkScalar("map['A', 1, 'ABC', 2]", "{A=1, ABC=2}",
        "(VARCHAR(3) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f1.checkScalar("Map[cast(1 as tinyint), 1, cast(2 as smallint), 2]",
        "{1=1, 2=2}", "(SMALLINT NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f1.checkScalar("Map[1, cast(1 as tinyint), 2, cast(2 as smallint)]",
        "{1=1, 2=2}", "(INTEGER NOT NULL, SMALLINT NOT NULL) MAP NOT NULL");
    f1.checkScalar("Map[1, cast(1 as tinyint), cast(2 as bigint), cast(2 as smallint)]",
        "{1=1, 2=2}", "(BIGINT NOT NULL, SMALLINT NOT NULL) MAP NOT NULL");
    f1.checkScalar("Map[cast(1 as bigint), cast(1 as tinyint), 2, cast(2 as smallint)]",
        "{1=1, 2=2}", "(BIGINT NOT NULL, SMALLINT NOT NULL) MAP NOT NULL");
  }

  @Test void testMapFunction() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.MAP, VmName.EXPAND);

    f.checkFails("^Map()^",
        "No match found for function signature "
            + "MAP\\(\\)", false);
    f.checkFails("^Map(1, 'x')^",
        "No match found for function signature "
            + "MAP\\(<NUMERIC>, <CHARACTER>\\)", false);
    f.checkFails("^map(1, 'x', 2, 'x')^",
        "No match found for function signature "
            + "MAP\\(<NUMERIC>, <CHARACTER>, <NUMERIC>, <CHARACTER>\\)", false);

    final SqlOperatorFixture f1 = f.withLibrary(SqlLibrary.SPARK);
    f1.checkFails("^Map(1)^",
        "Map requires an even number of arguments", false);
    f1.checkFails("^Map(1, 'x', 2)^",
        "Map requires an even number of arguments", false);
    f1.checkFails("^map(1, 1, 2, 'x')^",
        "Parameters must be of the same type", false);
    // leastRestrictive for key
    f1.checkFails("^MAP('k1', 1, 1, 2.0, 'k3', 1)^",
        "Parameters must be of the same type", false);
    // leastRestrictive for value
    f1.checkFails("^MAP('k1', 1, 'k2', 2.0, 'k3', '3')^",
        "Parameters must be of the same type", false);

    // this behavior is different from std MapValueConstructor
    f1.checkScalar("map()",
        "{}",
        "(UNKNOWN NOT NULL, UNKNOWN NOT NULL) MAP NOT NULL");
    f1.checkScalar("map('washington', null)",
        "{washington=null}",
        "(CHAR(10) NOT NULL, NULL) MAP NOT NULL");
    f1.checkScalar("map('washington', 1)",
        "{washington=1}",
        "(CHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f1.checkScalar("map('washington', 1, 'washington', 2)",
        "{washington=2}",
        "(CHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f1.checkScalar("map('washington', 1, 'obama', 44)",
        "{washington=1, obama=44}",
        "(CHAR(10) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f1.checkScalar("map('k1', 1, 'k2', 2.0)",
        "{k1=1, k2=2.0}",
        "(CHAR(2) NOT NULL, DECIMAL(11, 1) NOT NULL) MAP NOT NULL");
  }

  @Test void testMapQueryConstructor() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MAP_QUERY, VmName.EXPAND);
    // must be 2 fields
    f.checkFails("map(select 1)", "MAP requires exactly two fields, got 1; "
        + "row type RecordType\\(INTEGER EXPR\\$0\\)", false);
    f.checkFails("map(select 1, 2, 3)", "MAP requires exactly two fields, got 3; "
        + "row type RecordType\\(INTEGER EXPR\\$0, INTEGER EXPR\\$1, "
        + "INTEGER EXPR\\$2\\)", false);
    f.checkFails("map(select 1, 'x', 2, 'x')", "MAP requires exactly two fields, got 4; "
        + "row type RecordType\\(INTEGER EXPR\\$0, CHAR\\(1\\) EXPR\\$1, INTEGER EXPR\\$2, "
        + "CHAR\\(1\\) EXPR\\$3\\)", false);
    f.checkScalar("map(select 1, 2)", "{1=2}",
          "(INTEGER NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f.checkScalar("map(select 1, 2.0)", "{1=2.0}",
        "(INTEGER NOT NULL, DECIMAL(2, 1) NOT NULL) MAP NOT NULL");
    f.checkScalar("map(select 1, true)", "{1=true}",
        "(INTEGER NOT NULL, BOOLEAN NOT NULL) MAP NOT NULL");
    f.checkScalar("map(select 'x', 1)", "{x=1}",
        "(CHAR(1) NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    // element cast
    f.checkScalar("map(select cast(1 as bigint), 2)", "{1=2}",
        "(BIGINT NOT NULL, INTEGER NOT NULL) MAP NOT NULL");
    f.checkScalar("map(select 1, cast(2 as varchar))", "{1=2}",
        "(INTEGER NOT NULL, VARCHAR NOT NULL) MAP NOT NULL");
    // null key or value
    f.checkScalar("map(select 1, null)", "{1=null}",
        "(INTEGER NOT NULL, NULL) MAP NOT NULL");
    f.checkScalar("map(select null, 1)", "{null=1}",
        "(NULL, INTEGER NOT NULL) MAP NOT NULL");
    f.checkScalar("map(select null, null)", "{null=null}",
        "(NULL, NULL) MAP NOT NULL");

    // single sub-clause test for filter/limit/orderby
    f.check("select map(select x,y from (values(1,2),(3,4)) as t(x,y))",
        "(INTEGER NOT NULL, INTEGER NOT NULL) MAP NOT NULL", "{1=2, 3=4}");
    f.check("select map(select x,y from (values(1,2),(3,4)) as t(x,y) where x > 1)",
        "(INTEGER NOT NULL, INTEGER NOT NULL) MAP NOT NULL", "{3=4}");
    f.check("select map(select x,y from (values(1,2),(3,4),(5,6)) as t(x,y) limit 1)",
        "(INTEGER NOT NULL, INTEGER NOT NULL) MAP NOT NULL", "{1=2}");
    f.check("select map(select x,y from (values(1,2),(3,4),(5,6)) as t(x,y) where x > 1 limit 1)",
        "(INTEGER NOT NULL, INTEGER NOT NULL) MAP NOT NULL", "{3=4}");

    // combined tests for filter/limit/orderby
    // note: map subquery is not sql standard, it has limitations below:
    // case-1: use order by without limit
    // it has no sorting effect in runtime (sort will be removed)
    // case-2: use order by and limit
    // the order by will take effect (sort will be reserved),
    // but we do not guarantee the order of the final map result
    f.check("select map(select x,y from (values(1,2),(3,4)) as t(x,y) order by x asc)",
        "(INTEGER NOT NULL, INTEGER NOT NULL) MAP NOT NULL", "{1=2, 3=4}");
    f.check("select map(select x,y from (values(1,2),(3,4)) as t(x,y) "
            + "where x > 1 order by x asc)",
        "(INTEGER NOT NULL, INTEGER NOT NULL) MAP NOT NULL", "{3=4}");
    f.check("select map(select x,y from (values(1,2),(3,4),(5,6)) as t(x,y) "
        + "where x > 1 order by x asc limit 1)",
        "(INTEGER NOT NULL, INTEGER NOT NULL) MAP NOT NULL", "{3=4}");
    f.check("select map(select x,y from (values(1,2),(3,4)) as t(x,y) order by x desc)",
        "(INTEGER NOT NULL, INTEGER NOT NULL) MAP NOT NULL", "{1=2, 3=4}");
    f.check("select map(select x,y from (values(1,2),(3,4)) as t(x,y) "
            + "where x > 1 order by x desc)",
        "(INTEGER NOT NULL, INTEGER NOT NULL) MAP NOT NULL", "{3=4}");
    f.check("select map(select x,y from (values(1,2),(3,4),(5,6)) as t(x,y) "
            + "where x > 1 order by x desc limit 1)",
        "(INTEGER NOT NULL, INTEGER NOT NULL) MAP NOT NULL", "{5=6}");
  }

  @Test void testCeilFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CEIL);
    f.checkScalarApprox("ceil(10.1e0)", "DOUBLE NOT NULL", isExactly(11));
    f.checkScalarApprox("ceil(cast(-11.2e0 as real))", "REAL NOT NULL",
        isExactly(-11));
    f.checkScalarExact("ceil(100)", "INTEGER NOT NULL", "100");
    f.checkScalarExact("ceil(1.3)", "DECIMAL(2, 0) NOT NULL", "2");
    f.checkScalarExact("ceil(-1.7)", "DECIMAL(2, 0) NOT NULL", "-1");
    f.checkNull("ceiling(cast(null as decimal(2,0)))");
    f.checkNull("ceiling(cast(null as double))");
  }

  @Test void testCeilFuncInterval() {
    final SqlOperatorFixture f = fixture();
    if (!f.brokenTestsEnabled()) {
      return;
    }
    f.checkScalar("ceil(interval '3:4:5' hour to second)",
        "+4:00:00.000000", "INTERVAL HOUR TO SECOND NOT NULL");
    f.checkScalar("ceil(interval '-6.3' second)",
        "-6.000000", "INTERVAL SECOND NOT NULL");
    f.checkScalar("ceil(interval '5-1' year to month)",
        "+6-00", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkScalar("ceil(interval '-5-1' year to month)",
        "-5-00", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkNull("ceil(cast(null as interval year))");
  }

  @Test void testFloorFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.FLOOR);
    f.checkScalarApprox("floor(2.5e0)", "DOUBLE NOT NULL", isExactly(2));
    f.checkScalarApprox("floor(cast(-1.2e0 as real))", "REAL NOT NULL",
        isExactly(-2));
    f.checkScalarExact("floor(100)", "INTEGER NOT NULL", "100");
    f.checkScalarExact("floor(1.7)", "DECIMAL(2, 0) NOT NULL", "1");
    f.checkScalarExact("floor(-1.7)", "DECIMAL(2, 0) NOT NULL", "-2");
    f.checkNull("floor(cast(null as decimal(2,0)))");
    f.checkNull("floor(cast(null as real))");
  }

  @Test void testBigQueryCeilFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.checkType("ceil(cast(3 as tinyint))", "TINYINT NOT NULL");
    final SqlOperatorFixture f = f0.setFor(SqlLibraryOperators.FLOOR_BIG_QUERY)
        .withLibrary(SqlLibrary.BIG_QUERY).withConformance(SqlConformanceEnum.BIG_QUERY);
    f.checkScalarExact("ceil(cast(3 as tinyint))", "DOUBLE NOT NULL", "3.0");
    f.checkScalarExact("ceil(cast(3 as smallint))", "DOUBLE NOT NULL", "3.0");
    f.checkScalarExact("ceil(cast(3 as integer))", "DOUBLE NOT NULL", "3.0");
    f.checkScalarExact("ceil(cast(3 as bigint))", "DOUBLE NOT NULL", "3.0");
    f.checkScalarExact("ceil(cast(3.5 as double))", "DOUBLE NOT NULL", "4.0");
    f.checkScalarExact("ceil(cast(3.45 as decimal(19, 1)))",
        "DECIMAL(19, 1) NOT NULL", "4.0");
    f.checkScalarExact("ceil(cast(3.45 as float))", "FLOAT NOT NULL", "4.0");
    f.checkNull("ceil(cast(null as tinyint))");
  }

  @Test void testBigQueryFloorFunc() {
    final SqlOperatorFixture f0 = fixture();
    f0.checkType("floor(cast(3 as tinyint))", "TINYINT NOT NULL");
    final SqlOperatorFixture f = f0.setFor(SqlLibraryOperators.FLOOR_BIG_QUERY)
        .withLibrary(SqlLibrary.BIG_QUERY).withConformance(SqlConformanceEnum.BIG_QUERY);
    f.checkScalarExact("floor(cast(3 as tinyint))", "DOUBLE NOT NULL", "3.0");
    f.checkScalarExact("floor(cast(3 as smallint))", "DOUBLE NOT NULL", "3.0");
    f.checkScalarExact("floor(cast(3 as integer))", "DOUBLE NOT NULL", "3.0");
    f.checkScalarExact("floor(cast(3 as bigint))", "DOUBLE NOT NULL", "3.0");
    f.checkScalarExact("floor(cast(3.5 as double))", "DOUBLE NOT NULL", "3.0");
    f.checkScalarExact("floor(cast(3.45 as decimal))",
        "DECIMAL(19, 0) NOT NULL", "3");
    f.checkScalarExact("floor(cast(3.45 as float))", "FLOAT NOT NULL", "3.0");
    f.checkNull("floor(cast(null as tinyint))");
  }

  @Test void testFloorFuncDateTime() {
    final SqlOperatorFixture f = fixture();
    f.enableTypeCoercion(false)
        .checkFails("^floor('12:34:56')^",
            "Cannot apply 'FLOOR' to arguments of type "
                + "'FLOOR\\(<CHAR\\(8\\)>\\)'\\. Supported form\\(s\\): "
                + "'FLOOR\\(<NUMERIC>\\)'\n"
                + "'FLOOR\\(<DATETIME_INTERVAL>\\)'\n"
                + "'FLOOR\\(<DATE> TO <TIME_UNIT>\\)'\n"
                + "'FLOOR\\(<TIME> TO <TIME_UNIT>\\)'\n"
                + "'FLOOR\\(<TIMESTAMP> TO <TIME_UNIT>\\)'",
            false);
    f.checkType("floor('12:34:56')", "DECIMAL(19, 0) NOT NULL");
    f.checkFails("^floor(time '12:34:56')^",
        "(?s)Cannot apply 'FLOOR' to arguments .*", false);
    f.checkFails("^floor(123.45 to minute)^",
        "(?s)Cannot apply 'FLOOR' to arguments .*", false);
    f.checkFails("^floor('abcde' to minute)^",
        "(?s)Cannot apply 'FLOOR' to arguments .*", false);
    f.checkScalar("floor(time '12:34:56' to minute)",
        "12:34:00", "TIME(0) NOT NULL");
    f.checkScalar("floor(timestamp '2015-02-19 12:34:56.78' to second)",
        "2015-02-19 12:34:56.00", "TIMESTAMP(2) NOT NULL");
    f.checkScalar("floor(timestamp '2015-02-19 12:34:56.78' to millisecond)",
        "2015-02-19 12:34:56.78", "TIMESTAMP(2) NOT NULL");
    f.checkScalar("floor(timestamp '2015-02-19 12:34:56.78' to microsecond)",
        "2015-02-19 12:34:56.78", "TIMESTAMP(2) NOT NULL");
    f.checkScalar("floor(timestamp '2015-02-19 12:34:56.78' to nanosecond)",
        "2015-02-19 12:34:56.78", "TIMESTAMP(2) NOT NULL");
    f.checkScalar("floor(timestamp '2015-02-19 12:34:56' to minute)",
        "2015-02-19 12:34:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("floor(timestamp '2015-02-19 12:34:56' to year)",
        "2015-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("floor(date '2015-02-19' to year)",
        "2015-01-01", "DATE NOT NULL");
    f.checkScalar("floor(timestamp '2015-02-19 12:34:56' to month)",
        "2015-02-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("floor(date '2015-02-19' to month)",
        "2015-02-01", "DATE NOT NULL");
    f.checkNull("floor(cast(null as timestamp) to month)");
    f.checkNull("floor(cast(null as date) to month)");
  }

  @Test void testCeilFuncDateTime() {
    final SqlOperatorFixture f = fixture();
    f.enableTypeCoercion(false)
        .checkFails("^ceil('12:34:56')^",
            "Cannot apply 'CEIL' to arguments of type "
                + "'CEIL\\(<CHAR\\(8\\)>\\)'\\. Supported form\\(s\\): "
                + "'CEIL\\(<NUMERIC>\\)'\n"
                + "'CEIL\\(<DATETIME_INTERVAL>\\)'\n"
                + "'CEIL\\(<DATE> TO <TIME_UNIT>\\)'\n"
                + "'CEIL\\(<TIME> TO <TIME_UNIT>\\)'\n"
                + "'CEIL\\(<TIMESTAMP> TO <TIME_UNIT>\\)'",
            false);
    f.checkType("ceil('12:34:56')", "DECIMAL(19, 0) NOT NULL");
    f.checkFails("^ceil(time '12:34:56')^",
        "(?s)Cannot apply 'CEIL' to arguments .*", false);
    f.checkFails("^ceil(123.45 to minute)^",
        "(?s)Cannot apply 'CEIL' to arguments .*", false);
    f.checkFails("^ceil('abcde' to minute)^",
        "(?s)Cannot apply 'CEIL' to arguments .*", false);
    f.checkScalar("ceil(time '12:34:56' to minute)",
        "12:35:00", "TIME(0) NOT NULL");
    f.checkScalar("ceil(time '12:59:56' to minute)",
        "13:00:00", "TIME(0) NOT NULL");
    f.checkScalar("ceil(timestamp '2015-02-19 12:34:56.78' to second)",
        "2015-02-19 12:34:57.00", "TIMESTAMP(2) NOT NULL");
    f.checkScalar("ceil(timestamp '2015-02-19 12:34:56.78' to millisecond)",
        "2015-02-19 12:34:56.78", "TIMESTAMP(2) NOT NULL");
    f.checkScalar("ceil(timestamp '2015-02-19 12:34:56.78' to microsecond)",
        "2015-02-19 12:34:56.78", "TIMESTAMP(2) NOT NULL");
    f.checkScalar("ceil(timestamp '2015-02-19 12:34:56.78' to nanosecond)",
        "2015-02-19 12:34:56.78", "TIMESTAMP(2) NOT NULL");
    f.checkScalar("ceil(timestamp '2015-02-19 12:34:56.00' to second)",
        "2015-02-19 12:34:56.00", "TIMESTAMP(2) NOT NULL");
    f.checkScalar("ceil(timestamp '2015-02-19 12:34:56' to minute)",
        "2015-02-19 12:35:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("ceil(timestamp '2015-02-19 12:34:56' to year)",
        "2016-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("ceil(date '2015-02-19' to year)",
        "2016-01-01", "DATE NOT NULL");
    f.checkScalar("ceil(timestamp '2015-02-19 12:34:56' to month)",
        "2015-03-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("ceil(date '2015-02-19' to month)",
        "2015-03-01", "DATE NOT NULL");
    f.checkNull("ceil(cast(null as timestamp) to month)");
    f.checkNull("ceil(cast(null as date) to month)");

    // ceiling alias
    f.checkScalar("ceiling(timestamp '2015-02-19 12:34:56' to month)",
        "2015-03-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("ceiling(date '2015-02-19' to month)",
        "2015-03-01", "DATE NOT NULL");
    f.checkNull("ceiling(cast(null as timestamp) to month)");
  }

  /** Tests {@code FLOOR}, {@code CEIL}, {@code TIMESTAMPADD},
   * {@code TIMESTAMPDIFF} functions with custom time frames. */
  @Test void testCustomTimeFrame() {
    final SqlOperatorFixture f = fixture()
        .withFactory(tf ->
            tf.withTypeSystem(typeSystem ->
                new DelegatingTypeSystem(typeSystem) {
                  @Override public TimeFrameSet deriveTimeFrameSet(
                      TimeFrameSet frameSet) {
                    return TimeFrameSet.builder()
                        .addAll(frameSet)
                        .addDivision("minute15", 4, "HOUR")
                        .addMultiple("month4", 4, "MONTH")
                        .build();
                  }
                }));
    f.checkScalar("floor(timestamp '2020-06-27 12:34:56' to \"minute15\")",
        "2020-06-27 12:30:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("floor(timestamp '2020-06-27 12:34:56' to \"month4\")",
        "2020-05-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("floor(date '2020-06-27' to \"month4\")",
        "2020-05-01", "DATE NOT NULL");

    f.checkScalar("ceil(timestamp '2020-06-27 12:34:56' to \"minute15\")",
        "2020-06-27 12:45:00", "TIMESTAMP(0) NOT NULL");
    f.checkFails("ceil(timestamp '2020-06-27 12:34:56' to ^\"minute25\"^)",
        "'minute25' is not a valid time frame", false);

    f.checkScalar(
        "timestampadd(\"minute15\", 7, timestamp '2016-02-24 12:42:25')",
        "2016-02-24 14:27:25", "TIMESTAMP(0) NOT NULL");
    f.checkScalar(
        "timestampadd(\"month4\", 7, timestamp '2016-02-24 12:42:25')",
        "2018-06-24 12:42:25", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestampadd(\"month4\", 7, date '2016-02-24')",
        "2018-06-24", "DATE NOT NULL");

    f.checkScalar("timestampdiff(\"minute15\", "
            + "timestamp '2016-02-24 12:42:25', "
            + "timestamp '2016-02-24 15:42:25')",
        "12", "INTEGER NOT NULL");
    f.checkScalar("timestampdiff(\"month4\", "
            + "timestamp '2016-02-24 12:42:25', "
            + "timestamp '2016-02-24 15:42:25')",
        "0", "INTEGER NOT NULL");
    f.checkScalar("timestampdiff(\"month4\", "
            + "timestamp '2016-02-24 12:42:25', "
            + "timestamp '2018-02-24 15:42:25')",
        "6", "INTEGER NOT NULL");
    f.checkScalar("timestampdiff(\"month4\", "
            + "timestamp '2016-02-24 12:42:25', "
            + "timestamp '2018-02-23 15:42:25')",
        "5", "INTEGER NOT NULL");
    f.checkScalar("timestampdiff(\"month4\", date '2016-02-24', "
            + "date '2020-03-24')",
        "12", "INTEGER NOT NULL");
    f.checkScalar("timestampdiff(\"month4\", date '2016-02-24', "
            + "date '2016-06-23')",
        "0", "INTEGER NOT NULL");
    f.checkScalar("timestampdiff(\"month4\", date '2016-02-24', "
            + "date '2016-06-24')",
        "1", "INTEGER NOT NULL");
    f.checkScalar("timestampdiff(\"month4\", date '2016-02-24', "
            + "date '2015-10-24')",
        "-1", "INTEGER NOT NULL");
    f.checkScalar("timestampdiff(\"month4\", date '2016-02-24', "
            + "date '2016-02-23')",
        "0", "INTEGER NOT NULL");
    f.withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.TIMESTAMP_DIFF3)
        .checkScalar("timestamp_diff(timestamp '2008-12-25 15:30:00', "
                + "timestamp '2008-12-25 16:30:00', \"minute15\")",
            "-4", "INTEGER NOT NULL");
  }

  @Test void testFloorFuncInterval() {
    final SqlOperatorFixture f = fixture();
    if (!f.brokenTestsEnabled()) {
      return;
    }
    f.checkScalar("floor(interval '3:4:5' hour to second)",
        "+3:00:00.000000",
        "INTERVAL HOUR TO SECOND NOT NULL");
    f.checkScalar("floor(interval '-6.3' second)",
        "-7.000000", "INTERVAL SECOND NOT NULL");
    f.checkScalar("floor(interval '5-1' year to month)",
        "+5-00", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkScalar("floor(interval '-5-1' year to month)",
        "-6-00", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkScalar("floor(interval '-6.3' second to second)",
        "-7.000000", "INTERVAL SECOND NOT NULL");
    f.checkScalar("floor(interval '6-3' minute to second to minute)",
        "-7-0", "INTERVAL MINUTE TO SECOND NOT NULL");
    f.checkScalar("floor(interval '6-3' hour to minute to hour)",
        "7-0", "INTERVAL HOUR TO MINUTE NOT NULL");
    f.checkScalar("floor(interval '6 3' day to hour to day)",
        "7 00", "INTERVAL DAY TO HOUR NOT NULL");
    f.checkScalar("floor(interval '102-7' year to month to month)",
        "102-07", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkScalar("floor(interval '102-7' year to month to quarter)",
        "102-10", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkScalar("floor(interval '102-1' year to month to century)",
        "201", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkScalar("floor(interval '1004-1' year to month to millennium)",
        "2001-00", "INTERVAL YEAR TO MONTH NOT NULL");
    f.checkNull("floor(cast(null as interval year))");
  }

  @Test void testTimestampAdd() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.TIMESTAMP_ADD, VmName.EXPAND);
    MICROSECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s
                + ", 2000000, timestamp '2016-02-24 12:42:25')",
            "2016-02-24 12:42:27.000",
            "TIMESTAMP(3) NOT NULL"));
    SECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s
                + ", 2, timestamp '2016-02-24 12:42:25')",
            "2016-02-24 12:42:27",
            "TIMESTAMP(0) NOT NULL"));
    NANOSECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s
                + ", 3000000000, timestamp '2016-02-24 12:42:25')",
            "2016-02-24 12:42:28.000",
            "TIMESTAMP(3) NOT NULL"));
    NANOSECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s
                + ", 2000000000, timestamp '2016-02-24 12:42:25')",
            "2016-02-24 12:42:27.000",
            "TIMESTAMP(3) NOT NULL"));
    MINUTE_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s
                + ", 2, timestamp '2016-02-24 12:42:25')",
            "2016-02-24 12:44:25",
            "TIMESTAMP(0) NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s
                + ", -2000, timestamp '2016-02-24 12:42:25')",
            "2015-12-03 04:42:25",
            "TIMESTAMP(0) NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkNull("timestampadd(" + s + ", CAST(NULL AS INTEGER),"
            + " timestamp '2016-02-24 12:42:25')"));
    HOUR_VARIANTS.forEach(s ->
        f.checkNull("timestampadd(" + s + ", -200, CAST(NULL AS TIMESTAMP))"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s
                + ", 3, timestamp '2016-02-24 12:42:25')",
            "2016-05-24 12:42:25", "TIMESTAMP(0) NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 3, cast(null as timestamp))",
            isNullValue(), "TIMESTAMP(0)"));

    // TIMESTAMPADD with DATE; returns a TIMESTAMP value for sub-day intervals.
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, date '2016-06-15')",
            "2016-07-15", "DATE NOT NULL"));
    DAY_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, date '2016-06-15')",
            "2016-06-16", "DATE NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", -1, date '2016-06-15')",
            "2016-06-14 23:00:00", "TIMESTAMP(0) NOT NULL"));
    MINUTE_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, date '2016-06-15')",
            "2016-06-15 00:01:00", "TIMESTAMP(0) NOT NULL"));
    SECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", -1, date '2016-06-15')",
            "2016-06-14 23:59:59", "TIMESTAMP(0) NOT NULL"));
    SECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, date '2016-06-15')",
            "2016-06-15 00:00:01", "TIMESTAMP(0) NOT NULL"));
    SECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, cast(null as date))",
            isNullValue(), "TIMESTAMP(0)"));
    DAY_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, cast(null as date))",
            isNullValue(), "DATE"));

    // Round to the last day of previous month
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, date '2016-05-31')",
            "2016-06-30", "DATE NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 5, date '2016-01-31')",
            "2016-06-30", "DATE NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", -1, date '2016-03-31')",
            "2016-02-29", "DATE NOT NULL"));

    // TIMESTAMPADD with time; returns a time value.The interval is positive.
    SECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, time '23:59:59')",
            "00:00:00", "TIME(0) NOT NULL"));
    MINUTE_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, time '00:00:00')",
            "00:01:00", "TIME(0) NOT NULL"));
    MINUTE_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, time '23:59:59')",
            "00:00:59", "TIME(0) NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, time '23:59:59')",
            "00:59:59", "TIME(0) NOT NULL"));
    DAY_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 15, time '23:59:59')",
            "23:59:59", "TIME(0) NOT NULL"));
    WEEK_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 3, time '23:59:59')",
            "23:59:59", "TIME(0) NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 6, time '23:59:59')",
            "23:59:59", "TIME(0) NOT NULL"));
    QUARTER_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 1, time '23:59:59')",
            "23:59:59", "TIME(0) NOT NULL"));
    YEAR_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", 10, time '23:59:59')",
            "23:59:59", "TIME(0) NOT NULL"));
    // TIMESTAMPADD with time; returns a time value .The interval is negative.
    SECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", -1, time '00:00:00')",
            "23:59:59", "TIME(0) NOT NULL"));
    MINUTE_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", -1, time '00:00:00')",
            "23:59:00", "TIME(0) NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", -1, time '00:00:00')",
            "23:00:00", "TIME(0) NOT NULL"));
    DAY_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", -1, time '23:59:59')",
            "23:59:59", "TIME(0) NOT NULL"));
    WEEK_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", -1, time '23:59:59')",
            "23:59:59", "TIME(0) NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", -1, time '23:59:59')",
            "23:59:59", "TIME(0) NOT NULL"));
    QUARTER_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", -1, time '23:59:59')",
            "23:59:59", "TIME(0) NOT NULL"));
    YEAR_VARIANTS.forEach(s ->
        f.checkScalar("timestampadd(" + s + ", -1, time '23:59:59')",
            "23:59:59", "TIME(0) NOT NULL"));
  }

  @Test void testTimestampAddFractionalSeconds() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.TIMESTAMP_ADD, VmName.EXPAND);
    f.checkType("timestampadd(SQL_TSI_FRAC_SECOND, 2, "
            + "timestamp '2016-02-24 12:42:25.000000')",
        // "2016-02-24 12:42:25.000002",
        "TIMESTAMP(3) NOT NULL");

    f.checkType("timestampadd(SQL_TSI_FRAC_SECOND, 2, "
            + "timestamp with local time zone '2016-02-24 12:42:25.000000')",
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL");

    f.checkType("timestampadd(SECOND, 2, timestamp '2016-02-24 12:42:25.000')",
        "TIMESTAMP(3) NOT NULL");

    f.checkType("timestampadd(HOUR, 2, time '12:42:25.000')",
        "TIME(3) NOT NULL");

    f.checkType("timestampadd(MINUTE, 2, time '12:42:25')",
        "TIME(0) NOT NULL");

    // The following test would correctly return "TIMESTAMP(6) NOT NULL" if max
    // precision were 6 or higher
    assumeTrue(f.getFactory().getTypeFactory().getTypeSystem()
        .getMaxPrecision(SqlTypeName.TIMESTAMP) == 3);
    f.checkType(
        "timestampadd(MICROSECOND, 2, timestamp '2016-02-24 12:42:25.000000')",
        // "2016-02-24 12:42:25.000002",
        "TIMESTAMP(3) NOT NULL");
  }

  /** Tests {@code TIMESTAMP_ADD}, BigQuery's 2-argument variant of the
   * 3-argument {@code TIMESTAMPADD} function. */
  @Test void testTimestampAdd2() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.TIMESTAMP_ADD2);
    f0.checkFails("^timestamp_add(timestamp '2008-12-25 15:30:00', "
            + "interval 5 minute)^",
        "No match found for function signature "
            + "TIMESTAMP_ADD\\(<TIMESTAMP>, <INTERVAL_DAY_TIME>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    if (Bug.CALCITE_5422_FIXED) {
      f.checkScalar("timestamp_add(timestamp '2008-12-25 15:30:00', "
              + "interval 100000000000 microsecond)",
          "2008-12-26 19:16:40",
          "TIMESTAMP(3) NOT NULL");
      f.checkScalar("timestamp_add(timestamp '2008-12-25 15:30:00', "
              + "interval 100000000 millisecond)",
          "2008-12-26 19:16:40",
          "TIMESTAMP(3) NOT NULL");
    }

    f.checkScalar("timestamp_add(timestamp '2016-02-24 12:42:25', interval 2 second)",
        "2016-02-24 12:42:27",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_add(timestamp '2016-02-24 12:42:25', interval 2 minute)",
        "2016-02-24 12:44:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_add(timestamp '2016-02-24 12:42:25', interval -2000 hour)",
        "2015-12-03 04:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_add(timestamp '2016-02-24 12:42:25', interval 1 day)",
        "2016-02-25 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_add(timestamp '2016-02-24 12:42:25', interval 1 month)",
        "2016-03-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_add(timestamp '2016-02-24 12:42:25', interval 1 year)",
        "2017-02-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkNull("timestamp_add(CAST(NULL AS TIMESTAMP), interval 5 minute)");
  }

  /** Tests BigQuery's {@code DATETIME_ADD(timestamp, interval)} function.
   * When Calcite runs in BigQuery mode, {@code DATETIME} is a type alias for
   * {@code TIMESTAMP} and this function follows the same behavior as
   * {@code TIMESTAMP_ADD(timestamp, interval)}. The tests below use
   * {@code TIMESTAMP} values rather than the {@code DATETIME} alias because the
   * operator fixture does not currently support type aliases. */
  @Test void testDatetimeAdd() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.DATETIME_ADD);
    f0.checkFails("^datetime_add(timestamp '2008-12-25 15:30:00', "
            + "interval 5 minute)^",
        "No match found for function signature "
            + "DATETIME_ADD\\(<TIMESTAMP>, <INTERVAL_DAY_TIME>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    if (Bug.CALCITE_5422_FIXED) {
      f.checkScalar("datetime_add(timestamp '2008-12-25 15:30:00', "
              + "interval 100000000000 microsecond)",
          "2008-12-26 19:16:40",
          "TIMESTAMP(3) NOT NULL");
      f.checkScalar("datetime_add(timestamp '2008-12-25 15:30:00', "
              + "interval 100000000 millisecond)",
          "2008-12-26 19:16:40",
          "TIMESTAMP(3) NOT NULL");
    }

    f.checkScalar("datetime_add(timestamp '2016-02-24 12:42:25', interval 2 second)",
        "2016-02-24 12:42:27",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_add(timestamp '2016-02-24 12:42:25', interval 2 minute)",
        "2016-02-24 12:44:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_add(timestamp '2016-02-24 12:42:25', interval -2000 hour)",
        "2015-12-03 04:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_add(timestamp '2016-02-24 12:42:25', interval 1 day)",
        "2016-02-25 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_add(timestamp '2016-02-24 12:42:25', interval 1 month)",
        "2016-03-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_add(timestamp '2016-02-24 12:42:25', interval 1 year)",
        "2017-02-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkNull("datetime_add(CAST(NULL AS TIMESTAMP), interval 5 minute)");
  }

  /** Tests {@code TIMESTAMP_DIFF}, BigQuery's variant of the
   * {@code TIMESTAMPDIFF} function, which differs in the ordering
   * of the parameters and the ordering of the subtraction between
   * the two timestamps. In {@code TIMESTAMPDIFF} it is (t2 - t1)
   * while for {@code TIMESTAMP_DIFF} is is (t1 - t2). */
  @Test void testTimestampDiff3() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.TIMESTAMP_DIFF3);
    f0.checkFails("^timestamp_diff(timestamp '2008-12-25 15:30:00', "
            + "timestamp '2008-12-25 16:30:00', "
            + "minute)^",
        "No match found for function signature "
            + "TIMESTAMP_DIFF\\(<TIMESTAMP>, <TIMESTAMP>, <INTERVAL_DAY_TIME>\\)", false);

    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.TIMESTAMP_DIFF3);
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(timestamp '2016-02-24 12:42:25', "
                + "timestamp '2016-02-24 15:42:25', "
                + s + ")",
            "-3", "INTEGER NOT NULL"));
    MICROSECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(timestamp '2016-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:20', "
                + s + ")",
            "5000000", "INTEGER NOT NULL"));
    YEAR_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25', "
                + s + ")",
            "-2", "INTEGER NOT NULL"));
    WEEK_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25', "
                + s + ")",
            "-104", "INTEGER NOT NULL"));
    WEEK_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(timestamp '2014-02-19 12:42:25', "
                + "timestamp '2016-02-24 12:42:25', "
                + s + ")",
            "-105", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25', "
                + s + ")",
            "-24", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(timestamp '2019-09-01 12:42:25', "
                + "timestamp '2020-03-01 12:42:25', "
                + s + ")",
            "-6", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(timestamp '2019-09-01 12:42:25', "
                + "timestamp '2016-08-01 12:42:25', "
                + s + ")",
            "37", "INTEGER NOT NULL"));
    QUARTER_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25', "
                + s + ")",
            "-8", "INTEGER NOT NULL"));
    f.checkScalar("timestamp_diff(timestamp '2014-02-24 12:42:25', "
            + "timestamp '2614-02-24 12:42:25', "
            + "CENTURY)",
        "-6", "INTEGER NOT NULL");
    QUARTER_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(timestamp '2016-02-24 12:42:25', "
                + "cast(null as timestamp), "
                + s + ")",
            isNullValue(), "INTEGER"));

    // timestamp_diff with date
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(date '2016-03-15', "
                + "date '2016-06-14', "
                + s + ")",
            "-3", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(date '2019-09-01', "
                + "date '2020-03-01', "
                + s + ")",
            "-6", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(date '2019-09-01', "
                + "date '2016-08-01', "
                + s + ")",
            "37", "INTEGER NOT NULL"));
    DAY_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(date '2016-06-15', "
                + "date '2016-06-14', "
                + s + ")",
            "1", "INTEGER NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(date '2016-06-15', "
                + "date '2016-06-14', "
                + s + ")",
            "24", "INTEGER NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(date '2016-06-15',  "
                + "date '2016-06-15', "
                + s + ")",
            "0", "INTEGER NOT NULL"));
    MINUTE_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(date '2016-06-15', "
                + "date '2016-06-14', "
                + s + ")",
            "1440", "INTEGER NOT NULL"));
    DAY_VARIANTS.forEach(s ->
        f.checkScalar("timestamp_diff(date '2016-06-15', "
                + "cast(null as date), "
                + s + ")",
            isNullValue(), "INTEGER"));
  }

  /** Tests BigQuery's {@code DATETIME_DIFF(timestamp, timestamp2, timeUnit)}
   * function. When Calcite runs in BigQuery mode, {@code DATETIME} is a type
   * alias for {@code TIMESTAMP} and this function follows the same behavior as
   * {@code TIMESTAMP_DIFF(timestamp, timestamp2, timeUnit)}. The tests below
   * use {@code TIMESTAMP} values rather than the {@code DATETIME} alias because
   * the operator fixture does not currently support type aliases. */
  @Test void testDatetimeDiff() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.DATETIME_DIFF);
    f0.checkFails("^datetime_diff(timestamp '2008-12-25 15:30:00', "
            + "timestamp '2008-12-25 16:30:00', "
            + "minute)^",
        "No match found for function signature "
            + "DATETIME_DIFF\\(<TIMESTAMP>, <TIMESTAMP>, <INTERVAL_DAY_TIME>\\)", false);

    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.DATETIME_DIFF);
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(timestamp '2016-02-24 12:42:25', "
                + "timestamp '2016-02-24 15:42:25', "
                + s + ")",
            "-3", "INTEGER NOT NULL"));
    MICROSECOND_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(timestamp '2016-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:20', "
                + s + ")",
            "5000000", "INTEGER NOT NULL"));
    YEAR_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25', "
                + s + ")",
            "-2", "INTEGER NOT NULL"));
    WEEK_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25', "
                + s + ")",
            "-104", "INTEGER NOT NULL"));
    WEEK_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(timestamp '2014-02-19 12:42:25', "
                + "timestamp '2016-02-24 12:42:25', "
                + s + ")",
            "-105", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25', "
                + s + ")",
            "-24", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(timestamp '2019-09-01 12:42:25', "
                + "timestamp '2020-03-01 12:42:25', "
                + s + ")",
            "-6", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(timestamp '2019-09-01 12:42:25', "
                + "timestamp '2016-08-01 12:42:25', "
                + s + ")",
            "37", "INTEGER NOT NULL"));
    QUARTER_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25', "
                + s + ")",
            "-8", "INTEGER NOT NULL"));
    f.checkScalar("datetime_diff(timestamp '2014-02-24 12:42:25', "
            + "timestamp '2614-02-24 12:42:25', "
            + "CENTURY)",
        "-6", "INTEGER NOT NULL");
    QUARTER_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(timestamp '2016-02-24 12:42:25', "
                + "cast(null as timestamp), "
                + s + ")",
            isNullValue(), "INTEGER"));

    // datetime_diff with date
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(date '2016-03-15', "
                + "date '2016-06-14', "
                + s + ")",
            "-3", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(date '2019-09-01', "
                + "date '2020-03-01', "
                + s + ")",
            "-6", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(date '2019-09-01', "
                + "date '2016-08-01', "
                + s + ")",
            "37", "INTEGER NOT NULL"));
    DAY_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(date '2016-06-15', "
                + "date '2016-06-14', "
                + s + ")",
            "1", "INTEGER NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(date '2016-06-15', "
                + "date '2016-06-14', "
                + s + ")",
            "24", "INTEGER NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(date '2016-06-15',  "
                + "date '2016-06-15', "
                + s + ")",
            "0", "INTEGER NOT NULL"));
    MINUTE_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(date '2016-06-15', "
                + "date '2016-06-14', "
                + s + ")",
            "1440", "INTEGER NOT NULL"));
    DAY_VARIANTS.forEach(s ->
        f.checkScalar("datetime_diff(date '2016-06-15', "
                + "cast(null as date), "
                + s + ")",
            isNullValue(), "INTEGER"));
  }

  @ValueSource(booleans = {true, false})
  @ParameterizedTest(name = "CoercionEnabled: {0}")
  void testTimestampDiff(boolean coercionEnabled) {
    final SqlOperatorFixture f = fixture()
        .withValidatorConfig(c -> c.withTypeCoercionEnabled(coercionEnabled));
    f.setFor(SqlStdOperatorTable.TIMESTAMP_DIFF, VmName.EXPAND);
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "timestamp '2016-02-24 12:42:25', "
                + "timestamp '2016-02-24 15:42:25')",
            "3", "INTEGER NOT NULL"));
    MICROSECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "timestamp '2016-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:20')",
            "-5000000", "INTEGER NOT NULL"));
    NANOSECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "timestamp '2016-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:20')",
            "-5000000000", "BIGINT NOT NULL"));
    YEAR_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25')",
            "2", "INTEGER NOT NULL"));
    WEEK_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25')",
            "104", "INTEGER NOT NULL"));
    WEEK_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "timestamp '2014-02-19 12:42:25', "
                + "timestamp '2016-02-24 12:42:25')",
            "105", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25')",
            "24", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "timestamp '2019-09-01 00:00:00', "
                + "timestamp '2020-03-01 00:00:00')",
            "6", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "timestamp '2019-09-01 00:00:00', "
                + "timestamp '2016-08-01 00:00:00')",
            "-37", "INTEGER NOT NULL"));
    QUARTER_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "timestamp '2014-02-24 12:42:25', "
                + "timestamp '2016-02-24 12:42:25')",
            "8", "INTEGER NOT NULL"));
    // Until 1.33, CENTURY was an invalid time frame for TIMESTAMPDIFF
    f.checkScalar("timestampdiff(CENTURY, "
            + "timestamp '2014-02-24 12:42:25', "
            + "timestamp '2614-02-24 12:42:25')",
        "6", "INTEGER NOT NULL");
    QUARTER_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "timestamp '2014-02-24 12:42:25', "
                + "cast(null as timestamp))",
            isNullValue(), "INTEGER"));
    QUARTER_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "cast(null as timestamp), "
                + "timestamp '2014-02-24 12:42:25')",
            isNullValue(), "INTEGER"));

    // timestampdiff with date
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "date '2016-03-15', date '2016-06-14')",
            "2", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "date '2019-09-01', date '2020-03-01')",
            "6", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "date '2019-09-01', date '2016-08-01')",
            "-37", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "time '12:42:25', time '12:42:25')",
            "0", "INTEGER NOT NULL"));
    // 2 test cases for [CALCITE-7146] TIMESTAMPDIFF accepts arguments with mismatched types
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "time '12:42:25', date '2016-06-14')",
            "557", "INTEGER NOT NULL"));
    MONTH_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "date '2016-06-14', time '12:42:25')",
            "-557", "INTEGER NOT NULL"));
    DAY_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "date '2016-06-15', date '2016-06-14')",
            "-1", "INTEGER NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "date '2016-06-15', date '2016-06-14')",
            "-24", "INTEGER NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "date '2016-06-15',  date '2016-06-15')",
            "0", "INTEGER NOT NULL"));
    MINUTE_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "date '2016-06-15', date '2016-06-14')",
            "-1440", "INTEGER NOT NULL"));
    SECOND_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "cast(null as date), date '2016-06-15')",
            isNullValue(), "INTEGER"));
    DAY_VARIANTS.forEach(s ->
        f.checkScalar("timestampdiff(" + s + ", "
                + "date '2016-06-15', cast(null as date))",
            isNullValue(), "INTEGER"));
  }

  @Test void testTimestampSub() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.TIMESTAMP_SUB);
    f0.checkFails("^timestamp_sub(timestamp '2008-12-25 15:30:00', "
            + "interval 5 minute)^",
        "No match found for function signature "
            + "TIMESTAMP_SUB\\(<TIMESTAMP>, <INTERVAL_DAY_TIME>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    if (Bug.CALCITE_5422_FIXED) {
      f.checkScalar("timestamp_sub(timestamp '2008-12-25 15:30:00', "
              + "interval 100000000000 microsecond)",
          "2008-12-24 11:44:20",
          "TIMESTAMP(3) NOT NULL");
      f.checkScalar("timestamp_sub(timestamp '2008-12-25 15:30:00', "
              + "interval 100000000 millisecond)",
          "2008-12-24 11:44:20",
          "TIMESTAMP(3) NOT NULL");
    }

    f.checkScalar("timestamp_sub(timestamp '2016-02-24 12:42:25', interval 2 second)",
        "2016-02-24 12:42:23",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_sub(timestamp '2016-02-24 12:42:25', interval 2 minute)",
        "2016-02-24 12:40:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_sub(timestamp '2016-02-24 12:42:25', interval 2000 hour)",
        "2015-12-03 04:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_sub(timestamp '2016-02-24 12:42:25', interval 1 day)",
        "2016-02-23 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_sub(timestamp '2016-02-24 12:42:25', interval 2 week)",
        "2016-02-10 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_sub(timestamp '2016-02-24 12:42:25', interval 2 weeks)",
        "2016-02-10 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_sub(timestamp '2016-02-24 12:42:25', interval 1 month)",
        "2016-01-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_sub(timestamp '2016-02-24 12:42:25', interval 1 quarter)",
        "2015-11-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_sub(timestamp '2016-02-24 12:42:25', interval 1 quarters)",
        "2015-11-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_sub(timestamp '2016-02-24 12:42:25', interval 1 year)",
        "2015-02-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkNull("timestamp_sub(CAST(NULL AS TIMESTAMP), interval 5 minute)");
  }

  @Test void testTimeSub() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.TIME_SUB);
    f0.checkFails("^time_sub(time '15:30:00', "
            + "interval 5 minute)^",
        "No match found for function signature "
            + "TIME_SUB\\(<TIME>, <INTERVAL_DAY_TIME>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    if (Bug.CALCITE_5422_FIXED) {
      f.checkScalar("time_sub(time '15:30:00', "
              + "interval 100000000000 microsecond)",
          "11:44:20",
          "TIME(3) NOT NULL");
      f.checkScalar("time_sub(time '15:30:00', "
              + "interval 100000000 millisecond)",
          "11:44:20",
          "TIME(3) NOT NULL");
    }

    f.checkScalar("time_sub(time '12:42:25', interval 2 second)",
        "12:42:23",
        "TIME(0) NOT NULL");
    f.checkScalar("time_sub(time '12:42:25', interval 2 minute)",
        "12:40:25",
        "TIME(0) NOT NULL");
    f.checkScalar("time_sub(time '12:42:25', interval 0 minute)",
        "12:42:25",
        "TIME(0) NOT NULL");
    f.checkScalar("time_sub(time '12:42:25', interval 20 hour)",
        "16:42:25",
        "TIME(0) NOT NULL");
    f.checkScalar("time_sub(time '12:34:45', interval -5 second)",
        "12:34:50",
        "TIME(0) NOT NULL");
    f.checkNull("time_sub(CAST(NULL AS TIME), interval 5 minute)");
  }

  @Test void testDateAdd() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.DATE_ADD);
    f0.checkFails("^date_add(date '2008-12-25', "
            + "interval 5 day)^",
        "No match found for function signature "
            + "DATE_ADD\\(<DATE>, <INTERVAL_DAY_TIME>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("date_add(date '2016-02-22', interval 2 day)",
        "2016-02-24",
        "DATE NOT NULL");
    f.checkScalar("date_add(date '2016-02-17', interval 1 week)",
        "2016-02-24",
        "DATE NOT NULL");
    f.checkScalar("date_add(date '2016-02-10', interval 2 weeks)",
        "2016-02-24",
        "DATE NOT NULL");
    f.checkScalar("date_add(date '2020-10-17', interval 0 week)",
        "2020-10-17",
        "DATE NOT NULL");
    f.checkScalar("date_add(date '2016-11-24', interval 3 month)",
        "2017-02-24",
        "DATE NOT NULL");
    f.checkScalar("date_add(date '2015-11-24', interval 1 quarter)",
        "2016-02-24",
        "DATE NOT NULL");
    f.checkScalar("date_add(date '2015-08-24', interval 2 quarters)",
        "2016-02-24",
        "DATE NOT NULL");
    f.checkScalar("date_add(date '2011-02-24', interval 5 year)",
        "2016-02-24",
        "DATE NOT NULL");
    f.checkNull("date_add(CAST(NULL AS DATE), interval 5 day)");
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6527">[CALCITE-6527]
   * Add DATE_ADD function (enabled in Spark library)</a>.
   */
  @Test void testDateAddSpark() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.DATE_ADD_SPARK);
    f0.checkFails("^date_add(date '2008-12-25', "
            + "5)^",
        "No match found for function signature "
            + "DATE_ADD\\(<DATE>, <NUMERIC>\\)", false);

    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("date_add(date '2016-02-22', 2)",
          "2016-02-24",
          "DATE NOT NULL");
      f.checkScalar("date_add(date '2016-02-28', 2)",
          "2016-03-01",
          "DATE NOT NULL");
      f.checkScalar("date_add(date '2016-03-01', -2)",
          "2016-02-28",
          "DATE NOT NULL");
      f.checkScalar("date_add(date '2016-03-01', -2)",
          "2016-02-28",
          "DATE NOT NULL");
      f.checkScalar("date_add(date '2016-03-01', -2.0)",
          "2016-02-28",
          "DATE NOT NULL");
      f.checkScalar("date_add(date '2016-02-28', 2.0)",
          "2016-03-01",
          "DATE NOT NULL");
      f.checkScalar("date_add(date '2016-02-28', '2.0')",
          "2016-03-01",
          "DATE NOT NULL");
      f.checkScalar("date_add(timestamp '2016-02-22 13:00:01', '-2.0')",
          "2016-02-20",
          "DATE NOT NULL");
      f.checkScalar("date_add(timestamp '2016-02-22 13:00:01', -2)",
          "2016-02-20",
          "DATE NOT NULL");
      f.checkScalar("date_add(timestamp '2016-02-22 13:00:01', -2.0)",
          "2016-02-20",
          "DATE NOT NULL");
      f.checkScalar("date_add(date '0001-01-01', '-2.0')",
          "0000-12-30",
          "DATE NOT NULL");
      f.checkScalar("date_add(date '0001-01-01', '-367')",
          "000/-12-31",
          "DATE NOT NULL");
      f.checkScalar("date_add(date '0001-01-01', '-3')",
          "0000-12-29",
          "DATE NOT NULL");
      f.checkNull("date_add(CAST(NULL AS DATE), 5)");
      f.checkNull("date_add(date '2016-02-22', CAST(NULL AS INTEGER))");
      f.checkNull("date_add(CAST(NULL AS DATE), CAST(NULL AS INTEGER))");
      f.checkFails("^date_add(time '13:00:01', -2)^", INVALID_ARGUMENTS_TYPE_VALIDATION_ERROR,
          false);
      f.checkFails("^date_add(1, -2)^", INVALID_ARGUMENTS_TYPE_VALIDATION_ERROR,
          false);
    };

    f0.forEachLibrary(list(SqlLibrary.SPARK, SqlLibrary.HIVE), consumer);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6612">[CALCITE-6612]
   * Add DATE_SUB function (enabled in Spark library)</a>.
   */
  @Test void testDateSubSpark() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.DATE_SUB_SPARK);

    f0.checkFails("^date_sub(date '2008-12-25', "
            + "5)^",
        "No match found for function signature "
            + "DATE_SUB\\(<DATE>, <NUMERIC>\\)", false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("date_sub(date '2016-02-22', 2)",
          "2016-02-20",
          "DATE NOT NULL");
      f.checkScalar("date_sub(date '2016-03-01', 2)",
          "2016-02-28",
          "DATE NOT NULL");
      f.checkScalar("date_sub(timestamp '2016-02-22 13:00:01', '-2.0')",
          "2016-02-24",
          "DATE NOT NULL");
      f.checkScalar("date_sub(timestamp '2016-02-22 13:00:01', -2)",
          "2016-02-24",
          "DATE NOT NULL");
      f.checkNull("date_sub(CAST(NULL AS DATE), 5)");
      f.checkNull("date_sub(date '2016-02-22', CAST(NULL AS INTEGER))");
      f.checkNull("date_sub(CAST(NULL AS DATE), CAST(NULL AS INTEGER))");
      f.checkFails("^date_sub(time '13:00:01', -2)^", INVALID_ARGUMENTS_TYPE_VALIDATION_ERROR,
          false);
      f.checkFails("^date_sub(1, -2)^", INVALID_ARGUMENTS_TYPE_VALIDATION_ERROR,
          false);
    };
    f0.forEachLibrary(list(SqlLibrary.SPARK, SqlLibrary.HIVE), consumer);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6396">[CALCITE-6396]
   * Add ADD_MONTHS function (enabled in Oracle, Spark library)</a>.
   */
  @Test void testAddMonths() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.ADD_MONTHS);
    f0.checkFails("^add_months(date '2008-12-25', "
            + "5)^",
        "No match found for function signature "
            + "ADD_MONTHS\\(<DATE>, <NUMERIC>\\)", false);
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkScalar("add_months(date '2016-02-22', 2)",
          "2016-04-22",
          "DATE NOT NULL");
      f.checkScalar("add_months(date '2016-02-22', -2)",
          "2015-12-22",
          "DATE NOT NULL");
      f.checkScalar("add_months('2016-08-31',1)",
          "2016-09-30",
          "DATE NOT NULL");
      f.checkScalar("add_months('2016-08-31',1.0)",
          "2016-09-30",
          "DATE NOT NULL");
      f.checkScalar("add_months('2016-08-31','1.0')",
          "2016-09-30",
          "DATE NOT NULL");
      f.checkScalar("add_months('2016-08-31','1.1')",
          "2016-09-30",
          "DATE NOT NULL");
      f.checkScalar("add_months('2016-08-31','1.6')",
          "2016-09-30",
          "DATE NOT NULL");
      f.checkScalar("add_months(date '2016-02-22', -2.0)",
          "2015-12-22",
          "DATE NOT NULL");
      f.checkScalar("add_months(date '2016-02-22', 2.0)",
          "2016-04-22",
          "DATE NOT NULL");
      f.checkScalar("add_months(date '2016-02-22', '2')",
          "2016-04-22",
          "DATE NOT NULL");
      f.checkScalar("add_months(date '2016-02-22', '-2')",
          "2015-12-22",
          "DATE NOT NULL");
      f.checkScalar("add_months(timestamp '2016-02-22 13:00:01', '-2')",
          "2015-12-22",
          "DATE NOT NULL");
      f.checkScalar("add_months(timestamp '2016-02-22 13:00:01', '-2.0')",
          "2015-12-22",
          "DATE NOT NULL");
      f.checkScalar("add_months(timestamp '2016-02-22 13:00:01', -2)",
          "2015-12-22",
          "DATE NOT NULL");
      f.checkScalar("add_months(timestamp '2016-02-22 13:00:01', -2.0)",
          "2015-12-22",
          "DATE NOT NULL");
      f.checkFails("add_months(date '2016-02-22', '1e+1000')",
          "Value Infinity out of range",
          true);
      f.checkNull("add_months(CAST(NULL AS DATE), 5)");
      f.checkNull("add_months(date '2016-02-22', CAST(NULL AS INTEGER))");
      f.checkNull("add_months(CAST(NULL AS DATE), CAST(NULL AS INTEGER))");
    };
    f0.forEachLibrary(list(SqlLibrary.ORACLE, SqlLibrary.SPARK), consumer);
  }

  @Test void testDateSub() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.DATE_SUB);
    f0.checkFails("^date_sub(date '2008-12-25', "
            + "interval 5 day)^",
        "No match found for function signature "
            + "DATE_SUB\\(<DATE>, <INTERVAL_DAY_TIME>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("date_sub(date '2016-02-24', interval 2 day)",
        "2016-02-22",
        "DATE NOT NULL");
    f.checkScalar("date_sub(date '2016-02-24', interval 1 week)",
        "2016-02-17",
        "DATE NOT NULL");
    f.checkScalar("date_sub(date '2016-02-24', interval 2 weeks)",
        "2016-02-10",
        "DATE NOT NULL");
    f.checkScalar("date_sub(date '2020-10-17', interval 0 week)",
        "2020-10-17",
        "DATE NOT NULL");
    f.checkScalar("date_sub(date '2016-02-24', interval 3 month)",
        "2015-11-24",
        "DATE NOT NULL");
    f.checkScalar("date_sub(date '2016-02-24', interval 1 quarter)",
        "2015-11-24",
        "DATE NOT NULL");
    f.checkScalar("date_sub(date '2016-02-24', interval 2 quarters)",
        "2015-08-24",
        "DATE NOT NULL");
    f.checkScalar("date_sub(date '2016-02-24', interval 5 year)",
        "2011-02-24",
        "DATE NOT NULL");
    f.checkNull("date_sub(CAST(NULL AS DATE), interval 5 day)");
  }

  /** Tests for BigQuery's DATETIME_SUB() function. Because the operator
   * fixture does not currently support type aliases, TIMESTAMPs are used
   * in place of DATETIMEs (a Calcite alias of TIMESTAMP) for the function's
   * first argument. */
  @Test void testDatetimeSub() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.DATETIME_SUB);
    f0.checkFails("^datetime_sub(timestamp '2008-12-25 15:30:00', "
            + "interval 5 minute)^",
        "No match found for function signature "
            + "DATETIME_SUB\\(<TIMESTAMP>, <INTERVAL_DAY_TIME>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    if (Bug.CALCITE_5422_FIXED) {
      f.checkScalar("datetime_sub(timestamp '2008-12-25 15:30:00', "
              + "interval 100000000000 microsecond)",
          "2008-12-24 11:44:20",
          "TIMESTAMP(3) NOT NULL");
      f.checkScalar("datetime_sub(timestamp '2008-12-25 15:30:00', "
              + "interval 100000000 millisecond)",
          "2008-12-24 11:44:20",
          "TIMESTAMP(3) NOT NULL");
    }

    f.checkScalar("datetime_sub(timestamp '2016-02-24 12:42:25', interval 2 second)",
        "2016-02-24 12:42:23",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_sub(timestamp '2016-02-24 12:42:25', interval 2 minute)",
        "2016-02-24 12:40:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_sub(timestamp '2016-02-24 12:42:25', interval 2000 hour)",
        "2015-12-03 04:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_sub(timestamp '2016-02-24 12:42:25', interval 1 day)",
        "2016-02-23 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_sub(timestamp '2016-02-24 12:42:25', interval 1 month)",
        "2016-01-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_sub(timestamp '2016-02-24 12:42:25', interval 1 year)",
        "2015-02-24 12:42:25",
        "TIMESTAMP(0) NOT NULL");
    f.checkNull("datetime_sub(CAST(NULL AS TIMESTAMP), interval 5 minute)");
  }

  /** The {@code DATEDIFF} function is implemented in the Babel parser but not
   * the Core parser, and therefore gives validation errors. */
  @Test void testDateDiff() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.DATEDIFF);
    f.checkFails("datediff(^\"MONTH\"^, '2019-09-14',  '2019-09-15')",
        "(?s)Column 'MONTH' not found in any table",
        false);
    final SqlOperatorFixture f0 = f.withLibrary(SqlLibrary.BIG_QUERY);
    f0.checkScalar("date_diff(DATE '2010-07-07', DATE '2008-12-25', DAY)",
        "559",
        "INTEGER NOT NULL");
    f0.checkScalar("date_diff(DATE '2010-07-14', DATE '2010-07-07', WEEK)",
        "1",
        "INTEGER NOT NULL");
    f0.checkScalar("date_diff(DATE '2011-12-14', DATE '2011-07-14', MONTH)",
        "5",
        "INTEGER NOT NULL");
    f0.checkScalar("date_diff(DATE '2011-10-14', DATE '2011-07-14', QUARTER)",
        "1",
        "INTEGER NOT NULL");
    f0.checkScalar("date_diff(DATE '2021-07-14', DATE '2011-07-14', YEAR)",
        "10",
        "INTEGER NOT NULL");
    f0.checkNull("date_diff(CAST(NULL AS DATE), CAST(NULL AS DATE), DAY)");
    f0.checkNull("date_diff(DATE '2008-12-25', CAST(NULL AS DATE), DAY)");
    f0.checkNull("date_diff(CAST(NULL AS DATE), DATE '2008-12-25', DAY)");
  }

  /** Tests BigQuery's {@code TIME_ADD}, which adds an interval to a time
   * expression. */
  @Test void testTimeAdd() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.TIME_ADD);
    f0.checkFails("^time_add(time '15:30:00', interval 5 minute)^",
        "No match found for function signature TIME_ADD\\(<TIME>, <INTERVAL_DAY_TIME>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    if (Bug.CALCITE_5422_FIXED) {
      f.checkScalar("time_add(time '15:30:00', interval 5000000 millisecond)",
          "15:30:05", "TIME(3) NOT NULL");
      f.checkScalar("time_add(time '15:30:00', interval 5000000000 microsecond)",
          "15:30:05", "TIME(3) NOT NULL");
    }
    f.checkScalar("time_add(time '23:59:59', interval 2 second)",
        "00:00:01", "TIME(0) NOT NULL");
    f.checkScalar("time_add(time '23:59:59', interval 86402 second)",
        "00:00:01", "TIME(0) NOT NULL");
    f.checkScalar("time_add(time '15:30:00', interval 5 minute)",
        "15:35:00", "TIME(0) NOT NULL");
    f.checkScalar("time_add(time '15:30:00', interval 1445 minute)",
        "15:35:00", "TIME(0) NOT NULL");
    f.checkScalar("time_add(time '15:30:00', interval 3 hour)",
        "18:30:00", "TIME(0) NOT NULL");
    f.checkScalar("time_add(time '15:30:00', interval 27 hour)",
        "18:30:00", "TIME(0) NOT NULL");
    f.checkNull("time_add(cast(null as time), interval 5 minute)");
  }

  @Test void testTimeDiff() {
    final SqlOperatorFixture f0 = fixture()
        .setFor(SqlLibraryOperators.TIME_DIFF);
    f0.checkFails("^time_diff(time '15:30:00', "
            + "time '16:30:00', "
            + "minute)^",
        "No match found for function signature "
            + "TIME_DIFF\\(<TIME>, <TIME>, <INTERVAL_DAY_TIME>\\)", false);

    final SqlOperatorFixture f = f0.withLibrary(SqlLibrary.BIG_QUERY);
    f.checkScalar("time_diff(time '15:30:00', "
            + "time '15:30:05', "
            + "millisecond)",
        "-5000", "INTEGER NOT NULL");
    MICROSECOND_VARIANTS.forEach(s ->
        f.checkScalar("time_diff(time '15:30:00', "
                + "time '15:30:05', "
                + s + ")",
            "-5000000", "INTEGER NOT NULL"));
    SECOND_VARIANTS.forEach(s ->
        f.checkScalar("time_diff(time '15:30:00', "
                + "time '15:29:00', "
                + s + ")",
            "60", "INTEGER NOT NULL"));
    MINUTE_VARIANTS.forEach(s ->
        f.checkScalar("time_diff(time '15:30:00', "
                + "time '15:29:00', "
                + s + ")",
            "1", "INTEGER NOT NULL"));
    HOUR_VARIANTS.forEach(s ->
        f.checkScalar("time_diff(time '15:30:00', "
                + "time '16:30:00', "
                + s + ")",
            "-1", "INTEGER NOT NULL"));
    MINUTE_VARIANTS.forEach(s ->
        f.checkScalar("time_diff(time '15:30:00', "
                + "cast(null as time), "
                + s + ")",
            isNullValue(), "INTEGER"));
  }

  @Test void testTimeTrunc() {
    SqlOperatorFixture nonBigQuery = fixture()
        .setFor(SqlLibraryOperators.TIME_TRUNC);
    nonBigQuery.checkFails("^time_trunc(time '15:30:00', hour)^",
        "No match found for function signature "
            + "TIME_TRUNC\\(<TIME>, <INTERVAL_DAY_TIME>\\)",
        false);

    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.TIME_TRUNC);
    f.checkFails("time_trunc(time '12:34:56', ^year^)",
        "'YEAR' is not a valid time frame", false);
    f.checkFails("^time_trunc(123.45, minute)^",
        "Cannot apply 'TIME_TRUNC' to arguments of type "
            + "'TIME_TRUNC\\(<DECIMAL\\(5, 2\\)>, <INTERVAL MINUTE>\\)'\\. "
            + "Supported form\\(s\\): 'TIME_TRUNC\\(<TIME>, <DATETIME_INTERVAL>\\)'", false);
    f.checkScalar("time_trunc(time '12:34:56', second)",
        "12:34:56", "TIME(0) NOT NULL");
    f.checkScalar("time_trunc(time '12:34:56', minute)",
        "12:34:00", "TIME(0) NOT NULL");
    f.checkScalar("time_trunc(time '12:34:56', hour)",
        "12:00:00", "TIME(0) NOT NULL");
    f.checkNull("time_trunc(cast(null as time), second)");
    f.checkNull("time_trunc(cast(null as time), minute)");
  }

  @Test void testTimestampTrunc() {
    SqlOperatorFixture nonBigQuery = fixture()
        .setFor(SqlLibraryOperators.TIMESTAMP_TRUNC);
    nonBigQuery.checkFails("^timestamp_trunc(timestamp '2012-05-02 15:30:00', hour)^",
        "No match found for function signature "
            + "TIMESTAMP_TRUNC\\(<TIMESTAMP>, <INTERVAL_DAY_TIME>\\)",
        false);

    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.TIMESTAMP_TRUNC);
    f.checkFails("^timestamp_trunc(100, hour)^",
        "Cannot apply 'TIMESTAMP_TRUNC' to arguments of type "
            + "'TIMESTAMP_TRUNC\\(<INTEGER>, <INTERVAL HOUR>\\)'\\. "
            + "Supported form\\(s\\): 'TIMESTAMP_TRUNC\\(<TIMESTAMP>, <DATETIME_INTERVAL>\\)'",
        false);
    f.checkFails("^timestamp_trunc(100, foo)^",
        "Cannot apply 'TIMESTAMP_TRUNC' to arguments of type "
            + "'TIMESTAMP_TRUNC\\(<INTEGER>, <INTERVAL `FOO`>\\)'\\. "
            + "Supported form\\(s\\): 'TIMESTAMP_TRUNC\\(<TIMESTAMP>, <DATETIME_INTERVAL>\\)'",
        false);

    f.checkFails("timestamp_trunc(timestamp '2015-02-19 12:34:56.78', ^microsecond^)",
        "'MICROSECOND' is not a valid time frame", false);
    f.checkFails("timestamp_trunc(timestamp '2015-02-19 12:34:56.78', ^nanosecond^)",
        "'NANOSECOND' is not a valid time frame", false);
    f.checkFails("timestamp_trunc(timestamp '2015-02-19 12:34:56.78', ^millisecond^)",
        "'MILLISECOND' is not a valid time frame", false);
    f.checkScalar("timestamp_trunc(timestamp '2015-02-19 12:34:56.78', second)",
        "2015-02-19 12:34:56", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_trunc(timestamp '2015-02-19 12:34:56', minute)",
        "2015-02-19 12:34:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_trunc(timestamp '2015-02-19 12:34:56', hour)",
        "2015-02-19 12:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_trunc(timestamp '2015-02-19 12:34:56', day)",
        "2015-02-19 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_trunc(timestamp '2015-02-19 12:34:56', week)",
        "2015-02-15 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_trunc(timestamp '2015-02-19 12:34:56', month)",
        "2015-02-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_trunc(timestamp '2015-02-19 12:34:56', year)",
        "2015-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    // verify return type for dates
    f.checkScalar("timestamp_trunc(date '2008-12-25', month)",
        "2008-12-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("timestamp_trunc(timestamp '2015-02-19 12:34:56', decade)",
        "2010-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    // It may be surprising that this returns 2001 (and not 2000),
    // but the definition requires the "first day of the century".
    // See DateTimeUtils.julianDateFloor in Calcite Avatica.
    f.checkScalar("timestamp_trunc(timestamp '2015-02-19 12:34:56', century)",
        "2001-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    // The comment above for century applies to millennium too.
    f.checkScalar("timestamp_trunc(timestamp '2015-02-19 12:34:56', millennium)",
        "2001-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkFails("^timestamp_trunc(time '15:30:00', hour)^",
        "Cannot apply 'TIMESTAMP_TRUNC' to arguments of type "
            + "'TIMESTAMP_TRUNC\\(<TIME\\(0\\)>, <INTERVAL HOUR>\\)'\\. "
            + "Supported form\\(s\\): 'TIMESTAMP_TRUNC\\(<TIMESTAMP>, <DATETIME_INTERVAL>\\)'",
        false);
    f.checkNull("timestamp_trunc(CAST(NULL AS TIMESTAMP), second)");
  }

  @Test void testDatetimeTrunc() {
    SqlOperatorFixture nonBigQuery = fixture()
        .setFor(SqlLibraryOperators.DATETIME_TRUNC);
    nonBigQuery.checkFails("^datetime_trunc(timestamp '2012-05-02 15:30:00', hour)^",
        "No match found for function signature "
            + "DATETIME_TRUNC\\(<TIMESTAMP>, <INTERVAL_DAY_TIME>\\)",
        false);

    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.DATETIME_TRUNC);
    f.checkFails("^datetime_trunc(100, hour)^",
        "Cannot apply 'DATETIME_TRUNC' to arguments of type "
            + "'DATETIME_TRUNC\\(<INTEGER>, <INTERVAL HOUR>\\)'\\. "
            + "Supported form\\(s\\): 'DATETIME_TRUNC\\(<TIMESTAMP>, <DATETIME_INTERVAL>\\)'",
        false);
    f.checkFails("^datetime_trunc(100, foo)^",
        "Cannot apply 'DATETIME_TRUNC' to arguments of type "
            + "'DATETIME_TRUNC\\(<INTEGER>, <INTERVAL `FOO`>\\)'\\. "
            + "Supported form\\(s\\): 'DATETIME_TRUNC\\(<TIMESTAMP>, <DATETIME_INTERVAL>\\)'",
        false);

    f.checkScalar("datetime_trunc(timestamp '2015-02-19 12:34:56.78', second)",
        "2015-02-19 12:34:56", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_trunc(timestamp '2015-02-19 12:34:56', minute)",
        "2015-02-19 12:34:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_trunc(timestamp '2015-02-19 12:34:56', hour)",
        "2015-02-19 12:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_trunc(timestamp '2015-02-19 12:34:56', day)",
        "2015-02-19 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_trunc(timestamp '2015-02-19 12:34:56', week)",
        "2015-02-15 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_trunc(timestamp '2015-02-19 12:34:56', month)",
        "2015-02-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_trunc(timestamp '2015-02-19 12:34:56', year)",
        "2015-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    // verify return type for dates
    f.checkScalar("datetime_trunc(date '2008-12-25', month)",
        "2008-12-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkScalar("datetime_trunc(date '2015-02-19', decade)",
        "2010-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    // It may be surprising that this returns 2001 (and not 2000),
    // but the definition requires the "first day of the century".
    // See DateTimeUtils.julianDateFloor in Calcite Avatica.
    f.checkScalar("datetime_trunc(date '2015-02-19', century)",
        "2001-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    // The comment above for century applies to millennium too.
    f.checkScalar("datetime_trunc(date '2015-02-19', millennium)",
        "2001-01-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkFails("^datetime_trunc(time '15:30:00', hour)^",
        "Cannot apply 'DATETIME_TRUNC' to arguments of type "
            + "'DATETIME_TRUNC\\(<TIME\\(0\\)>, <INTERVAL HOUR>\\)'\\. "
            + "Supported form\\(s\\): 'DATETIME_TRUNC\\(<TIMESTAMP>, <DATETIME_INTERVAL>\\)'",
        false);
    f.checkNull("datetime_trunc(CAST(NULL AS TIMESTAMP), second)");
  }

  @Test void testDateTrunc() {
    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.DATE_TRUNC);
    f.checkFails("date_trunc(date '2015-02-19', ^foo^)",
        "Column 'FOO' not found in any table", false);
    f.checkScalar("date_trunc(date '2015-02-19', day)",
        "2015-02-19", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', week)",
        "2015-02-15", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', isoweek)",
        "2015-02-16", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', week(sunday))",
        "2015-02-15", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', week(monday))",
        "2015-02-16", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', week(tuesday))",
        "2015-02-17", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', week(wednesday))",
        "2015-02-18", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', week(thursday))",
        "2015-02-19", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', week(friday))",
        "2015-02-13", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', week(saturday))",
        "2015-02-14", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', month)",
        "2015-02-01", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', quarter)",
        "2015-01-01", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', year)",
        "2015-01-01", "DATE NOT NULL");
    f.checkScalar("date_trunc(date '2015-02-19', isoyear)",
        "2014-12-29", "DATE NOT NULL");
    // verifies return type for TIME & TIMESTAMP
    f.checkScalar("date_trunc(timestamp '2008-12-25 15:30:00', month)",
        "2008-12-01 00:00:00", "TIMESTAMP(0) NOT NULL");
    f.checkFails("^date_trunc(time '15:30:00', hour)^",
        "Cannot apply 'DATE_TRUNC' to arguments of type "
            + "'DATE_TRUNC\\(<TIME\\(0\\)>, <INTERVAL HOUR>\\)'\\. "
            + "Supported form\\(s\\): 'DATE_TRUNC\\(<DATE>, <DATETIME_INTERVAL>\\)'",
        false);
    f.checkScalar("date_trunc(date '2015-02-19', decade)",
        "2010-01-01", "DATE NOT NULL");
    // It may be surprising that this returns 2001 (and not 2000),
    // but the definition requires the "first day of the century".
    // See DateTimeUtils.julianDateFloor in Calcite Avatica.
    f.checkScalar("date_trunc(date '2015-02-19', century)",
        "2001-01-01", "DATE NOT NULL");
    // The comment above for century applies to millennium too.
    f.checkScalar("date_trunc(date '2015-02-19', millennium)",
        "2001-01-01", "DATE NOT NULL");
    f.checkNull("date_trunc(CAST(NULL AS DATE) , day)");
  }

  @Test void testFormatTime() {
    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.FORMAT_TIME);
    f.checkFails("^FORMAT_TIME('%x', timestamp '2008-12-25 15:30:00')^",
        "Cannot apply 'FORMAT_TIME' to arguments of type "
            + "'FORMAT_TIME\\(<CHAR\\(2\\)>, <TIMESTAMP\\(0\\)>\\)'\\. "
            + "Supported form\\(s\\): "
            + "'FORMAT_TIME\\(<CHARACTER>, <TIME>\\)'",
        false);
    f.checkScalar("FORMAT_TIME('%H', TIME '12:34:33')",
        "12",
        "VARCHAR NOT NULL");
    f.checkScalar("FORMAT_TIME('%R', TIME '12:34:33')",
        "12:34",
        "VARCHAR NOT NULL");
    f.checkScalar("FORMAT_TIME('The time is %M-%S', TIME '12:34:33')",
        "The time is 34-33",
        "VARCHAR NOT NULL");
  }

  @Test void testFormatDate() {
    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.FORMAT_DATE);
    // Test case for [CALCITE-6252] https://issues.apache.org/jira/browse/CALCITE-6252
    // BigQuery FORMAT_DATE uses the wrong calendar for Julian dates
    f.checkScalar("FORMAT_DATE('%A %a %d %B %Y', '0001-01-01')",
        "Monday Mon 01 January 1",
        "VARCHAR NOT NULL");
    f.checkScalar("FORMAT_DATE('%A %a %d %B %Y', '2024-02-08')",
        "Thursday Thu 08 February 2024",
        "VARCHAR NOT NULL");

    f.checkFails("^FORMAT_DATE('%x', 123)^",
        "Cannot apply 'FORMAT_DATE' to arguments of type "
            + "'FORMAT_DATE\\(<CHAR\\(2\\)>, <INTEGER>\\)'\\. "
            + "Supported form\\(s\\): "
            + "'FORMAT_DATE\\(<CHARACTER>, <DATE>\\)'",
        false);
    // Can implicitly cast TIMESTAMP to DATE
    f.checkScalar("FORMAT_DATE('%x', timestamp '2008-12-25 15:30:00')",
        "12/25/08",
        "VARCHAR NOT NULL");
    f.checkScalar("FORMAT_DATE('%b-%d-%Y', DATE '2008-12-25')",
        "Dec-25-2008",
        "VARCHAR NOT NULL");
    f.checkScalar("FORMAT_DATE('%b %Y', DATE '2008-12-25')",
        "Dec 2008",
        "VARCHAR NOT NULL");
    // Test case for [CALCITE-6247] https://issues.apache.org/jira/browse/CALCITE-6247
    // BigQuery FORMAT_DATE function handles incorrectly the %e format specifier
    f.checkScalar("FORMAT_DATE('*%e*', DATE '2008-12-02')",
        "* 2*",
        "VARCHAR NOT NULL");
    f.checkScalar("FORMAT_DATE('%x', DATE '2008-12-25')",
        "12/25/08",
        "VARCHAR NOT NULL");
    f.checkScalar("FORMAT_DATE('%x', DATE '2008-12-25')",
        "12/25/08",
        "VARCHAR NOT NULL");
    f.checkScalar("FORMAT_DATE('%g-%V', DATE '2001-01-01')",
        "01-01",
        "VARCHAR NOT NULL");
    // Test case for [CALCITE-6226] https://issues.apache.org/jira/browse/CALCITE-6226
    f.checkScalar("FORMAT_DATE('%G-%V', DATE '2023-01-01')",
        "2022-52",
        "VARCHAR NOT NULL");
    f.checkScalar("FORMAT_DATE('%g-%V', DATE '2023-01-01')",
        "22-52",
        "VARCHAR NOT NULL");
    // For Julian dates - motivated by [CALCITE-6252]
    f.checkScalar("FORMAT_DATE('%G-%V', DATE '0005-01-01')",
        "4-53",
        "VARCHAR NOT NULL");
    f.checkScalar("FORMAT_DATE('%g-%V', DATE '0005-01-01')",
        "04-53",
        "VARCHAR NOT NULL");
    // End Test case for
    f.checkScalar("FORMAT_DATE('%u', DATE '2024-01-01')",
        "1",
        "VARCHAR NOT NULL");
    f.checkScalar("FORMAT_DATE('%u', DATE '2024-01-07')",
        "7",
        "VARCHAR NOT NULL");
    f.checkNull("FORMAT_DATE('%x', CAST(NULL AS DATE))");
    f.checkNull("FORMAT_DATE('%b-%d-%Y', CAST(NULL AS DATE))");
    f.checkNull("FORMAT_DATE('%b %Y', CAST(NULL AS DATE))");
    f.checkNull("FORMAT_DATE(NULL, CAST(NULL AS DATE))");
  }

  @Test void testFormatTimestamp() {
    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.FORMAT_TIMESTAMP);
    f.checkFails("^FORMAT_TIMESTAMP('%x', 123)^",
        "Cannot apply 'FORMAT_TIMESTAMP' to arguments of type "
            + "'FORMAT_TIMESTAMP\\(<CHAR\\(2\\)>, <INTEGER>\\)'\\. "
            + "Supported form\\(s\\): "
            + "FORMAT_TIMESTAMP\\(<CHARACTER>, "
            + "<TIMESTAMP WITH LOCAL TIME ZONE>\\)\n"
            + "FORMAT_TIMESTAMP\\(<CHARACTER>, "
            + "<TIMESTAMP WITH LOCAL TIME ZONE>, <CHARACTER>\\)",
        false);
    f.checkScalar("FORMAT_TIMESTAMP('%c',"
            + " TIMESTAMP WITH LOCAL TIME ZONE '2008-12-25 15:30:00')",
        "Thu Dec 25 15:30:00 2008",
        "VARCHAR(2000) NOT NULL");
    f.checkScalar("FORMAT_TIMESTAMP('%b-%d-%Y',"
            + " TIMESTAMP WITH LOCAL TIME ZONE '2008-12-25 15:30:00')",
        "Dec-25-2008",
        "VARCHAR(2000) NOT NULL");
    f.checkScalar("FORMAT_TIMESTAMP('%b %Y',"
            + " TIMESTAMP WITH LOCAL TIME ZONE '2008-12-25 15:30:00')",
        "Dec 2008",
        "VARCHAR(2000) NOT NULL");
    f.checkScalar("FORMAT_TIMESTAMP('%x',"
            + " TIMESTAMP WITH LOCAL TIME ZONE '2008-12-25 15:30:00')",
        "12/25/08",
        "VARCHAR(2000) NOT NULL");
    f.checkScalar("FORMAT_TIMESTAMP('The time is: %R',"
            + " TIMESTAMP WITH LOCAL TIME ZONE '2008-12-25 15:30:00')",
        "The time is: 15:30",
        "VARCHAR(2000) NOT NULL");
    f.checkScalar("FORMAT_TIMESTAMP('The time is: %R.%E2S',"
            + " TIMESTAMP WITH LOCAL TIME ZONE '2008-12-25 15:30:00.1235456')",
        "The time is: 15:30.12",
        "VARCHAR(2000) NOT NULL");
  }

  @Test void testParseDate() {
    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.PARSE_DATE);
    f.checkScalar("PARSE_DATE('%A %b %e %Y', 'Thursday Dec 25 2008')",
        "2008-12-25",
        "DATE NOT NULL");
    f.checkScalar("PARSE_DATE('%x', '12/25/08')",
        "2008-12-25",
        "DATE NOT NULL");
    f.checkScalar("PARSE_DATE('%F', '2000-12-30')",
        "2000-12-30",
        "DATE NOT NULL");
    f.checkScalar("PARSE_DATE('%x', '12/25/08')",
        "2008-12-25",
        "DATE NOT NULL");
    f.checkScalar("PARSE_DATE('%Y%m%d', '20081225')",
        "2008-12-25",
        "DATE NOT NULL");
    f.checkScalar("PARSE_DATE('%F', '2022-06-01')",
        "2022-06-01",
        "DATE NOT NULL");
    f.checkNull("PARSE_DATE('%F', CAST(NULL AS DATE))");
    f.checkNull("PARSE_DATE('%Y%m%d', CAST(NULL AS DATE))");
    f.checkNull("PARSE_DATE('%x', CAST(NULL AS DATE))");
    f.checkNull("PARSE_DATE(NULL, CAST(NULL AS DATE))");
  }

  @Test void testParseDatetime() {
    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.PARSE_DATETIME);
    f.checkScalar("PARSE_DATETIME('%a %b %e %I:%M:%S %Y', 'Thu Dec 25 07:30:00 2008')",
        "2008-12-25 07:30:00",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("PARSE_DATETIME('%c', 'Thu Dec 25 07:30:00 2008')",
        "2008-12-25 07:30:00",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '1998-10-18 13:45:55')",
        "1998-10-18 13:45:55",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', '8/30/2018 2:23:38 pm')",
        "2018-08-30 14:23:38",
        "TIMESTAMP(0) NOT NULL");
    f.checkScalar("PARSE_DATETIME('%A, %B %e, %Y', 'Wednesday, December 19, 2018')",
        "2018-12-19 00:00:00",
        "TIMESTAMP(0) NOT NULL");
    f.checkNull("PARSE_DATETIME('%a %b %e %I:%M:%S %Y', CAST(NULL AS TIMESTAMP))");
    f.checkNull("PARSE_DATETIME('%c', CAST(NULL AS TIMESTAMP))");
    f.checkNull("PARSE_DATETIME('%Y-%m-%d %H:%M:%S', CAST(NULL AS TIMESTAMP))");
    f.checkNull("PARSE_DATETIME('%m/%d/%Y %I:%M:%S %p', CAST(NULL AS TIMESTAMP))");
    f.checkNull("PARSE_DATETIME(NULL, CAST(NULL AS TIMESTAMP))");
  }

  @Test void testParseTime() {
    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.PARSE_TIME);
    f.checkScalar("PARSE_TIME('%I:%M:%S', '07:30:00')",
        "07:30:00",
        "TIME(0) NOT NULL");
    f.checkScalar("PARSE_TIME('%T', '07:30:00')",
        "07:30:00",
        "TIME(0) NOT NULL");
    f.checkScalar("PARSE_TIME('%H', '15')",
        "15:00:00",
        "TIME(0) NOT NULL");
    f.checkScalar("PARSE_TIME('%I:%M:%S %p', '2:23:38 pm')",
        "14:23:38",
        "TIME(0) NOT NULL");
    f.checkNull("PARSE_TIME('%I:%M:%S', CAST(NULL AS TIME))");
    f.checkNull("PARSE_TIME('%T', CAST(NULL AS TIME))");
    f.checkNull("PARSE_TIME('%H', CAST(NULL AS TIME))");
    f.checkNull("PARSE_TIME('%I:%M:%S %p', CAST(NULL AS TIME))");
    f.checkNull("PARSE_TIME(NULL, CAST(NULL AS TIME))");
  }

  @Test void testParseTimestamp() {
    final SqlOperatorFixture f = fixture()
        .withLibrary(SqlLibrary.BIG_QUERY)
        .setFor(SqlLibraryOperators.PARSE_TIMESTAMP);
    f.checkScalar("PARSE_TIMESTAMP('%a %b %e %I:%M:%S %Y', 'Thu Dec 25 07:30:00 2008')",
        "2008-12-25 07:30:00",
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE(0) NOT NULL");
    f.checkScalar("PARSE_TIMESTAMP('%c', 'Thu Dec 25 07:30:00 2008')",
        "2008-12-25 07:30:00",
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE(0) NOT NULL");
    f.checkScalar("PARSE_TIMESTAMP('%c', 'Thu Dec 25 07:30:00 2008')",
        "2008-12-25 07:30:00",
        "TIMESTAMP_WITH_LOCAL_TIME_ZONE(0) NOT NULL");
    f.checkNull("PARSE_TIME('%a %b %e %I:%M:%S %Y', CAST(NULL AS TIMESTAMP))");
    f.checkNull("PARSE_TIMESTAMP(NULL, CAST(NULL AS TIMESTAMP))");
  }

  @Test void testDenseRankFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.DENSE_RANK, VM_JAVA);
  }

  @Test void testPercentRankFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.PERCENT_RANK, VM_JAVA);
  }

  @Test void testRankFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.RANK, VM_JAVA);
  }

  @Test void testCumeDistFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CUME_DIST, VM_JAVA);
  }

  @Test void testRowNumberFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ROW_NUMBER, VM_JAVA);
  }

  @Test void testPercentileContFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.PERCENTILE_CONT, VM_JAVA);
    f.checkType("percentile_cont(0.25) within group (order by 1)",
        "INTEGER NOT NULL");
    f.checkFails("percentile_cont(0.25) within group (^order by 'a'^)",
        "Invalid type 'CHAR' in ORDER BY clause of 'PERCENTILE_CONT' function. "
            + "Only NUMERIC types are supported", false);
    f.checkFails("percentile_cont(0.25) within group (^order by 1, 2^)",
        "'PERCENTILE_CONT' requires precisely one ORDER BY key", false);
    f.checkFails(" ^percentile_cont(2 + 3)^ within group (order by 1)",
        "Argument to function 'PERCENTILE_CONT' must be a literal", false);
    f.checkFails(" ^percentile_cont(2)^ within group (order by 1)",
        "Argument to function 'PERCENTILE_CONT' must be a numeric literal "
            + "between 0 and 1", false);
  }

  @Test void testPercentileDiscFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.PERCENTILE_DISC, VM_JAVA);
    f.checkType("percentile_disc(0.25) within group (order by 1)",
        "INTEGER NOT NULL");
    f.checkFails("percentile_disc(0.25) within group (^order by 'a'^)",
        "Invalid type 'CHAR' in ORDER BY clause of 'PERCENTILE_DISC' function. "
            + "Only NUMERIC types are supported", false);
    f.checkFails("percentile_disc(0.25) within group (^order by 1, 2^)",
        "'PERCENTILE_DISC' requires precisely one ORDER BY key", false);
    f.checkFails(" ^percentile_disc(2 + 3)^ within group (order by 1)",
        "Argument to function 'PERCENTILE_DISC' must be a literal", false);
    f.checkFails(" ^percentile_disc(2)^ within group (order by 1)",
        "Argument to function 'PERCENTILE_DISC' must be a numeric literal "
            + "between 0 and 1", false);
  }

  @Test void testPercentileContBigQueryFunc() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.PERCENTILE_CONT2, SqlOperatorFixture.VmName.EXPAND)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkType("percentile_cont(1, .5)",
        "DOUBLE NOT NULL");
    f.checkType("percentile_cont(.5, .5 RESPECT NULLS)", "DOUBLE NOT NULL");
    f.checkType("percentile_cont(1, .5 IGNORE NULLS)", "DOUBLE NOT NULL");
    f.checkType("percentile_cont(2+3, .5 IGNORE NULLS)", "DOUBLE NOT NULL");
    f.checkFails("^percentile_cont(1, 1.5)^",
        "Argument to function 'PERCENTILE_CONT' must be a numeric literal "
            + "between 0 and 1", false);
  }

  @Test void testPercentileDiscBigQueryFunc() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.PERCENTILE_DISC2, SqlOperatorFixture.VmName.EXPAND)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkType("percentile_disc(1, .5)",
        "INTEGER NOT NULL");
    f.checkType("percentile_disc(1, .5 RESPECT NULLS)", "INTEGER NOT NULL");
    f.checkType("percentile_disc(0.75, .5 IGNORE NULLS)", "DECIMAL(3, 2) NOT NULL");
    f.checkType("percentile_disc(2+3, .5 IGNORE NULLS)", "INTEGER NOT NULL");
    f.checkFails("^percentile_disc(1, 1.5)^",
        "Argument to function 'PERCENTILE_DISC' must be a numeric literal "
            + "between 0 and 1", false);
  }

  @Test void testCountFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.COUNT, VM_EXPAND);
    f.checkType("count(*)", "BIGINT NOT NULL");
    f.checkType("count('name')", "BIGINT NOT NULL");
    f.checkType("count(1)", "BIGINT NOT NULL");
    f.checkType("count(1.2)", "BIGINT NOT NULL");
    f.checkType("COUNT(DISTINCT 'x')", "BIGINT NOT NULL");
    f.checkFails("^COUNT()^",
        "Invalid number of arguments to function 'COUNT'. Was expecting 1 arguments",
        false);
    f.checkType("count(1, 2)", "BIGINT NOT NULL");
    f.checkType("count(1, 2, 'x', 'y')", "BIGINT NOT NULL");
    final String[] values = {"0", "CAST(null AS INTEGER)", "1", "0"};
    f.checkAgg("COUNT(x)", values, isSingle(3));
    f.checkAgg("COUNT(CASE x WHEN 0 THEN NULL ELSE -1 END)", values,
        isSingle(2));
    f.checkAgg("COUNT(DISTINCT x)", values, isSingle(2));

    // string values -- note that empty string is not null
    final String[] stringValues = {
        "'a'", "CAST(NULL AS VARCHAR(1))", "''"
    };
    f.checkAgg("COUNT(*)", stringValues, isSingle(3));
    f.checkAgg("COUNT(x)", stringValues, isSingle(2));
    f.checkAgg("COUNT(DISTINCT x)", stringValues, isSingle(2));
    f.checkAgg("COUNT(DISTINCT 123)", stringValues, isSingle(1));
  }

  @Test void testCountifFunc() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.COUNTIF, VM_JAVA)
        .withLibrary(SqlLibrary.BIG_QUERY);
    f.checkType("countif(true)", "BIGINT NOT NULL");
    f.checkType("countif(nullif(true,true))", "BIGINT NOT NULL");
    f.checkType("countif(false) filter (where true)", "BIGINT NOT NULL");

    final String expectedError = "Invalid number of arguments to function "
        + "'COUNTIF'. Was expecting 1 arguments";
    f.checkFails("^COUNTIF()^", expectedError, false);
    f.checkFails("^COUNTIF(true, false)^", expectedError, false);
    final String expectedError2 = "Cannot apply 'COUNTIF' to arguments of "
        + "type 'COUNTIF\\(<INTEGER>\\)'\\. Supported form\\(s\\): "
        + "'COUNTIF\\(<BOOLEAN>\\)'";
    f.checkFails("^COUNTIF(1)^", expectedError2, false);

    final String[] values = {"1", "2", "CAST(NULL AS INTEGER)", "1"};
    f.checkAgg("countif(x > 0)", values, isSingle(3));
    f.checkAgg("countif(x < 2)", values, isSingle(2));
    f.checkAgg("countif(x is not null) filter (where x < 2)",
        values, isSingle(2));
    f.checkAgg("countif(x < 2) filter (where x is not null)",
        values, isSingle(2));
    f.checkAgg("countif(x between 1 and 2)", values, isSingle(3));
    f.checkAgg("countif(x < 0)", values, isSingle(0));
  }

  @Test void testApproxCountDistinctFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.COUNT, VM_EXPAND);
    f.checkFails("approx_count_distinct(^*^)", "Unknown identifier '\\*'",
        false);
    f.checkType("approx_count_distinct('name')", "BIGINT NOT NULL");
    f.checkType("approx_count_distinct(1)", "BIGINT NOT NULL");
    f.checkType("approx_count_distinct(1.2)", "BIGINT NOT NULL");
    f.checkType("APPROX_COUNT_DISTINCT(DISTINCT 'x')", "BIGINT NOT NULL");
    f.checkFails("^APPROX_COUNT_DISTINCT()^",
        "Invalid number of arguments to function 'APPROX_COUNT_DISTINCT'. "
            + "Was expecting 1 arguments",
        false);
    f.checkType("approx_count_distinct(1, 2)", "BIGINT NOT NULL");
    f.checkType("approx_count_distinct(1, 2, 'x', 'y')",
        "BIGINT NOT NULL");
    final String[] values = {"0", "CAST(null AS INTEGER)", "1", "0"};
    // currently APPROX_COUNT_DISTINCT(x) returns the same as COUNT(DISTINCT x)
    f.checkAgg("APPROX_COUNT_DISTINCT(x)", values, isSingle(2));
    f.checkAgg("APPROX_COUNT_DISTINCT(CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values, isSingle(1));
    // DISTINCT keyword is allowed but has no effect
    f.checkAgg("APPROX_COUNT_DISTINCT(DISTINCT x)", values, isSingle(2));

    // string values -- note that empty string is not null
    final String[] stringValues = {
        "'a'", "CAST(NULL AS VARCHAR(1))", "''"
    };
    f.checkAgg("APPROX_COUNT_DISTINCT(x)", stringValues, isSingle(2));
    f.checkAgg("APPROX_COUNT_DISTINCT(DISTINCT x)", stringValues, isSingle(2));
    f.checkAgg("APPROX_COUNT_DISTINCT(DISTINCT 123)", stringValues, isSingle(1));
  }

  @Test void testSumFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.SUM, VM_EXPAND);
    f.checkFails("sum(^*^)", "Unknown identifier '\\*'", false);
    f.enableTypeCoercion(false)
        .checkFails("^sum('name')^",
            "(?s)Cannot apply 'SUM' to arguments of type "
                + "'SUM\\(<CHAR\\(4\\)>\\)'\\. Supported form\\(s\\): "
                + "'SUM\\(<NUMERIC>\\)'.*",
            false);
    f.checkType("sum('name')", "DECIMAL(19, 9)");
    f.checkAggType("sum(1)", "INTEGER NOT NULL");
    f.checkAggType("sum(1.2)", "DECIMAL(19, 1) NOT NULL");
    f.checkAggType("sum(DISTINCT 1.5)", "DECIMAL(19, 1) NOT NULL");
    f.checkFails("^sum()^",
        "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
        false);
    f.checkFails("^sum(1, 2)^",
        "Invalid number of arguments to function 'SUM'. Was expecting 1 arguments",
        false);
    f.enableTypeCoercion(false)
        .checkFails("^sum(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'SUM' to arguments of type "
                + "'SUM\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): "
                + "'SUM\\(<NUMERIC>\\)'.*",
            false);
    f.checkType("sum(cast(null as varchar(2)))", "DECIMAL(19, 9)");
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    f.checkAgg("sum(x)", values, isSingle(4));
    f.checkAgg("sum(CASE x WHEN 0 THEN NULL ELSE -1 END)", values,
        isSingle(-3));
    f.checkAgg("sum(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)", values,
        isSingle(-1));
    f.checkAgg("sum(DISTINCT x)", values, isSingle(2));
  }

  @Test void testAvgFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.AVG, VM_EXPAND);
    f.checkFails("avg(^*^)",
        "Unknown identifier '\\*'",
        false);
    f.enableTypeCoercion(false)
        .checkFails("^avg(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'AVG' to arguments of type "
                + "'AVG\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): "
                + "'AVG\\(<NUMERIC>\\)'.*",
            false);
    f.checkType("avg(cast(null as varchar(2)))", "DECIMAL(19, 9)");
    f.checkType("AVG(CAST(NULL AS INTEGER))", "INTEGER");
    f.checkAggType("AVG(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    f.checkAggType("avg(1)", "INTEGER NOT NULL");
    f.checkAggType("avg(1.2)", "DECIMAL(2, 1) NOT NULL");
    f.checkAggType("avg(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    if (!f.brokenTestsEnabled()) {
      return;
    }
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    f.checkAgg("AVG(x)", values, isExactly(2d));
    f.checkAgg("AVG(DISTINCT x)", values, isExactly(1.5d));
    f.checkAgg("avg(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)", values,
        isSingle(-1));
  }

  @Test void testCovarPopFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.COVAR_POP, VM_EXPAND);
    f.checkFails("covar_pop(^*^)", "Unknown identifier '\\*'", false);
    f.enableTypeCoercion(false)
        .checkFails("^covar_pop(cast(null as varchar(2)),"
                + " cast(null as varchar(2)))^",
            "(?s)Cannot apply 'COVAR_POP' to arguments of type "
                + "'COVAR_POP\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. "
                + "Supported form\\(s\\): "
                + "'COVAR_POP\\(<NUMERIC>, <NUMERIC>\\)'.*",
            false);
    f.checkType("covar_pop(cast(null as varchar(2)),cast(null as varchar(2)))",
        "DECIMAL(19, 9)");
    f.checkType("covar_pop(CAST(NULL AS INTEGER),CAST(NULL AS INTEGER))",
        "INTEGER");
    f.checkAggType("covar_pop(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!f.brokenTestsEnabled()) {
      return;
    }
    // with zero values
    f.checkAgg("covar_pop(x)", new String[]{}, isNullValue());
  }

  @Test void testCovarSampFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.COVAR_SAMP, VM_EXPAND);
    f.checkFails("covar_samp(^*^)", "Unknown identifier '\\*'", false);
    f.enableTypeCoercion(false)
        .checkFails("^covar_samp(cast(null as varchar(2)),"
                + " cast(null as varchar(2)))^",
            "(?s)Cannot apply 'COVAR_SAMP' to arguments of type "
                + "'COVAR_SAMP\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. "
                + "Supported form\\(s\\): "
                + "'COVAR_SAMP\\(<NUMERIC>, <NUMERIC>\\)'.*",
            false);
    f.checkType("covar_samp(cast(null as varchar(2)),cast(null as varchar(2)))",
        "DECIMAL(19, 9)");
    f.checkType("covar_samp(CAST(NULL AS INTEGER),CAST(NULL AS INTEGER))",
        "INTEGER");
    f.checkAggType("covar_samp(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!f.brokenTestsEnabled()) {
      return;
    }
    // with zero values
    f.checkAgg("covar_samp(x)", new String[]{}, isNullValue());
  }

  @Test void testRegrSxxFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.REGR_SXX, VM_EXPAND);
    f.checkFails("regr_sxx(^*^)", "Unknown identifier '\\*'", false);
    f.enableTypeCoercion(false)
        .checkFails("^regr_sxx(cast(null as varchar(2)),"
                + " cast(null as varchar(2)))^",
            "(?s)Cannot apply 'REGR_SXX' to arguments of type "
                + "'REGR_SXX\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. "
                + "Supported form\\(s\\): "
                + "'REGR_SXX\\(<NUMERIC>, <NUMERIC>\\)'.*",
            false);
    f.checkType("regr_sxx(cast(null as varchar(2)), cast(null as varchar(2)))",
        "DECIMAL(19, 9)");
    f.checkType("regr_sxx(CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))",
        "INTEGER");
    f.checkAggType("regr_sxx(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!f.brokenTestsEnabled()) {
      return;
    }
    // with zero values
    f.checkAgg("regr_sxx(x)", new String[]{}, isNullValue());
  }

  @Test void testRegrSyyFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.REGR_SYY, VM_EXPAND);
    f.checkFails("regr_syy(^*^)", "Unknown identifier '\\*'", false);
    f.enableTypeCoercion(false)
        .checkFails("^regr_syy(cast(null as varchar(2)),"
                + " cast(null as varchar(2)))^",
            "(?s)Cannot apply 'REGR_SYY' to arguments of type "
                + "'REGR_SYY\\(<VARCHAR\\(2\\)>, <VARCHAR\\(2\\)>\\)'\\. "
                + "Supported form\\(s\\): "
                + "'REGR_SYY\\(<NUMERIC>, <NUMERIC>\\)'.*",
            false);
    f.checkType("regr_syy(cast(null as varchar(2)), cast(null as varchar(2)))",
        "DECIMAL(19, 9)");
    f.checkType("regr_syy(CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))",
        "INTEGER");
    f.checkAggType("regr_syy(1.5, 2.5)", "DECIMAL(2, 1) NOT NULL");
    if (!f.brokenTestsEnabled()) {
      return;
    }
    // with zero values
    f.checkAgg("regr_syy(x)", new String[]{}, isNullValue());
  }

  @Test void testStddevPopFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.STDDEV_POP, VM_EXPAND);
    f.checkFails("stddev_pop(^*^)", "Unknown identifier '\\*'", false);
    f.enableTypeCoercion(false)
        .checkFails("^stddev_pop(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'STDDEV_POP' to arguments of type "
                + "'STDDEV_POP\\(<VARCHAR\\(2\\)>\\)'\\. "
                + "Supported form\\(s\\): 'STDDEV_POP\\(<NUMERIC>\\)'.*",
            false);
    f.checkType("stddev_pop(cast(null as varchar(2)))", "DECIMAL(19, 9)");
    f.checkType("stddev_pop(CAST(NULL AS INTEGER))", "INTEGER");
    f.checkAggType("stddev_pop(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (f.brokenTestsEnabled()) {
      // verified on Oracle 10g
      f.checkAgg("stddev_pop(x)", values,
          isWithin(1.414213562373095d, 0.000000000000001d));
      // Oracle does not allow distinct
      f.checkAgg("stddev_pop(DISTINCT x)", values, isExactly(1.5d));
      f.checkAgg("stddev_pop(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
          values, isExactly(0));
    }
    // with one value
    f.checkAgg("stddev_pop(x)", new String[]{"5"}, isSingle(0));
    // with zero values
    f.checkAgg("stddev_pop(x)", new String[]{}, isNullValue());
  }

  @Test void testStddevSampFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.STDDEV_SAMP, VM_EXPAND);
    f.checkFails("stddev_samp(^*^)",
        "Unknown identifier '\\*'",
        false);
    f.enableTypeCoercion(false)
        .checkFails("^stddev_samp(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'STDDEV_SAMP' to arguments of type "
                + "'STDDEV_SAMP\\(<VARCHAR\\(2\\)>\\)'\\. "
                + "Supported form\\(s\\): 'STDDEV_SAMP\\(<NUMERIC>\\)'.*",
            false);
    f.checkType("stddev_samp(cast(null as varchar(2)))", "DECIMAL(19, 9)");
    f.checkType("stddev_samp(CAST(NULL AS INTEGER))", "INTEGER");
    f.checkAggType("stddev_samp(DISTINCT 1.5)", "DECIMAL(2, 1)");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (f.brokenTestsEnabled()) {
      // verified on Oracle 10g
      f.checkAgg("stddev_samp(x)", values,
          isWithin(1.732050807568877d, 0.000000000000001d));
      // Oracle does not allow distinct
      f.checkAgg("stddev_samp(DISTINCT x)", values,
          isWithin(2.121320343559642d, 0.000000000000001d));
      f.checkAgg("stddev_samp(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
          values, isNullValue());
    }
    // with one value
    f.checkAgg("stddev_samp(x)", new String[]{"5"}, isNullValue());
    // with zero values
    f.checkAgg("stddev_samp(x)", new String[]{}, isNullValue());
  }

  @Test void testStddevFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.STDDEV, VM_EXPAND);
    f.checkFails("stddev(^*^)",
        "Unknown identifier '\\*'",
        false);
    f.enableTypeCoercion(false)
        .checkFails("^stddev(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'STDDEV' to arguments of type "
                + "'STDDEV\\(<VARCHAR\\(2\\)>\\)'\\. "
                + "Supported form\\(s\\): 'STDDEV\\(<NUMERIC>\\)'.*",
            false);
    f.checkType("stddev(cast(null as varchar(2)))", "DECIMAL(19, 9)");
    f.checkType("stddev(CAST(NULL AS INTEGER))", "INTEGER");
    f.checkAggType("stddev(DISTINCT 1.5)", "DECIMAL(2, 1)");
    // with one value
    f.checkAgg("stddev(x)", new String[]{"5"}, isNullValue());
    // with zero values
    f.checkAgg("stddev(x)", new String[]{}, isNullValue());
    // NaN fix check
    final String[] values = {"cast(23.79 as double)", "23.79", "23.79"};
    f.checkAgg("stddev(x)", values, isExactly(0));
  }

  @Test void testVarPopFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.VAR_POP, VM_EXPAND);
    f.checkFails("var_pop(^*^)", "Unknown identifier '\\*'", false);
    f.enableTypeCoercion(false)
        .checkFails("^var_pop(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'VAR_POP' to arguments of type "
                + "'VAR_POP\\(<VARCHAR\\(2\\)>\\)'\\. "
                + "Supported form\\(s\\): 'VAR_POP\\(<NUMERIC>\\)'.*",
            false);
    f.checkType("var_pop(cast(null as varchar(2)))", "DECIMAL(19, 9)");
    f.checkType("var_pop(CAST(NULL AS INTEGER))", "INTEGER");
    f.checkAggType("var_pop(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (!f.brokenTestsEnabled()) {
      return;
    }
    f.checkAgg("var_pop(x)", values, isExactly(2d)); // verified on Oracle 10g
    f.checkAgg("var_pop(DISTINCT x)", // Oracle does not allow distinct
        values, isWithin(2.25d, 0.0001d));
    f.checkAgg("var_pop(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values, isExactly(0));
    // with one value
    f.checkAgg("var_pop(x)", new String[]{"5"}, isExactly(0));
    // with zero values
    f.checkAgg("var_pop(x)", new String[]{}, isNullValue());
  }

  @Test void testVarSampFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.VAR_SAMP, VM_EXPAND);
    f.checkFails("var_samp(^*^)", "Unknown identifier '\\*'", false);
    f.enableTypeCoercion(false)
        .checkFails("^var_samp(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'VAR_SAMP' to arguments of type "
                + "'VAR_SAMP\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): "
                + "'VAR_SAMP\\(<NUMERIC>\\)'.*",
            false);
    f.checkType("var_samp(cast(null as varchar(2)))", "DECIMAL(19, 9)");
    f.checkType("var_samp(CAST(NULL AS INTEGER))", "INTEGER");
    f.checkAggType("var_samp(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (!f.brokenTestsEnabled()) {
      return;
    }
    f.checkAgg("var_samp(x)", values, isExactly(3d)); // verified on Oracle 10g
    f.checkAgg("var_samp(DISTINCT x)", // Oracle does not allow distinct
        values, isWithin(4.5d, 0.0001d));
    f.checkAgg("var_samp(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values, isNullValue());
    // with one value
    f.checkAgg("var_samp(x)", new String[]{"5"}, isNullValue());
    // with zero values
    f.checkAgg("var_samp(x)", new String[]{}, isNullValue());
  }

  @Test void testVarFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.VARIANCE, VM_EXPAND);
    f.checkFails("variance(^*^)", "Unknown identifier '\\*'", false);
    f.enableTypeCoercion(false)
        .checkFails("^variance(cast(null as varchar(2)))^",
            "(?s)Cannot apply 'VARIANCE' to arguments of type "
                + "'VARIANCE\\(<VARCHAR\\(2\\)>\\)'\\. Supported form\\(s\\): "
                + "'VARIANCE\\(<NUMERIC>\\)'.*",
            false);
    f.checkType("variance(cast(null as varchar(2)))", "DECIMAL(19, 9)");
    f.checkType("variance(CAST(NULL AS INTEGER))", "INTEGER");
    f.checkAggType("variance(DISTINCT 1.5)", "DECIMAL(2, 1) NOT NULL");

    final String[] values2 = {"cast(64.34 as double)", "64.34", "64.34"};
    f.checkAgg("variance(x)", values2, isExactly(0));
    final String[] values = {"0", "CAST(null AS FLOAT)", "3", "3"};
    if (!f.brokenTestsEnabled()) {
      return;
    }
    f.checkAgg("variance(x)", values, isExactly(3d)); // verified on Oracle 10g
    f.checkAgg("variance(DISTINCT x)", // Oracle does not allow distinct
        values, isWithin(4.5d, 0.0001d));
    f.checkAgg("variance(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)",
        values, isNullValue());
    // with one value
    f.checkAgg("variance(x)", new String[]{"5"}, isNullValue());
    // with zero values
    f.checkAgg("variance(x)", new String[]{}, isNullValue());
  }

  @Test void testMinFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MIN, VM_EXPAND);
    f.checkFails("min(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("min(1)", "INTEGER");
    f.checkType("min(1.2)", "DECIMAL(2, 1)");
    f.checkType("min(DISTINCT 1.5)", "DECIMAL(2, 1)");
    f.checkFails("^min()^",
        "Invalid number of arguments to function 'MIN'. "
            + "Was expecting 1 arguments",
        false);
    f.checkFails("^min(1, 2)^",
        "Invalid number of arguments to function 'MIN'. "
            + "Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    f.checkAgg("min(x)", values, isSingle("0"));
    f.checkAgg("min(CASE x WHEN 0 THEN NULL ELSE -1 END)", values, isSingle("-1"));
    f.checkAgg("min(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)", values, isSingle("-1"));
    f.checkAgg("min(DISTINCT x)", values, isSingle("0"));
  }

  @Test void testMaxFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.MAX, VM_EXPAND);
    f.checkFails("max(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("max(1)", "INTEGER");
    f.checkType("max(1.2)", "DECIMAL(2, 1)");
    f.checkType("max(DISTINCT 1.5)", "DECIMAL(2, 1)");
    f.checkFails("^max()^",
        "Invalid number of arguments to function 'MAX'. Was expecting 1 arguments",
        false);
    f.checkFails("^max(1, 2)^",
        "Invalid number of arguments to function 'MAX'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    f.checkAgg("max(x)", values, isSingle("2"));
    f.checkAgg("max(CASE x WHEN 0 THEN NULL ELSE -1 END)", values, isSingle("-1"));
    f.checkAgg("max(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)", values, isSingle("-1"));
    f.checkAgg("max(DISTINCT x)", values, isSingle("2"));
  }

  @Test void testLastValueFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.LAST_VALUE, VM_EXPAND);
    final String[] values = {"0", "CAST(null AS INTEGER)", "3", "3"};
    if (!f.brokenTestsEnabled()) {
      return;
    }
    f.checkWinAgg("last_value(x)", values, "ROWS 3 PRECEDING", "INTEGER",
        isSet("3", "0"));
    final String[] values2 = {"1.6", "1.2"};
    f.checkWinAgg("last_value(x)", values2, "ROWS 3 PRECEDING",
        "DECIMAL(2, 1) NOT NULL", isSet("1.6", "1.2"));
    final String[] values3 = {"'foo'", "'bar'", "'name'"};
    f.checkWinAgg("last_value(x)", values3, "ROWS 3 PRECEDING",
        "CHAR(4) NOT NULL", isSet("foo ", "bar ", "name"));
  }

  @Test void testFirstValueFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.FIRST_VALUE, VM_EXPAND);
    final String[] values = {"0", "CAST(null AS INTEGER)", "3", "3"};
    if (!f.brokenTestsEnabled()) {
      return;
    }
    f.checkWinAgg("first_value(x)", values, "ROWS 3 PRECEDING", "INTEGER",
        isSet("0"));
    final String[] values2 = {"1.6", "1.2"};
    f.checkWinAgg("first_value(x)", values2, "ROWS 3 PRECEDING",
        "DECIMAL(2, 1) NOT NULL", isSet("1.6"));
    final String[] values3 = {"'foo'", "'bar'", "'name'"};
    f.checkWinAgg("first_value(x)", values3, "ROWS 3 PRECEDING",
        "CHAR(4) NOT NULL", isSet("foo "));
  }

  @Test void testEveryFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.EVERY, VM_EXPAND);
    f.checkFails("every(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("every(1 = 1)", "BOOLEAN");
    f.checkType("every(1.2 = 1.2)", "BOOLEAN");
    f.checkType("every(1.5 = 1.4)", "BOOLEAN");
    f.checkFails("^every()^",
        "Invalid number of arguments to function 'EVERY'. Was expecting 1 arguments",
        false);
    f.checkFails("^every(1, 2)^",
        "Invalid number of arguments to function 'EVERY'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    f.checkAgg("every(x = 2)", values, isSingle("false"));
  }

  @Test void testSomeAggFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.SOME, VM_EXPAND);
    f.checkFails("some(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("some(1 = 1)", "BOOLEAN");
    f.checkType("some(1.2 = 1.2)", "BOOLEAN");
    f.checkType("some(1.5 = 1.4)", "BOOLEAN");
    f.checkFails("^some()^",
        "Invalid number of arguments to function 'SOME'. Was expecting 1 arguments",
        false);
    f.checkFails("^some(1, 2)^",
        "Invalid number of arguments to function 'SOME'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    f.checkAgg("some(x = 2)", values, isSingle("true"));
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-5160">[CALCITE-5160]
   * ANY/SOME, ALL operators should support collection expressions</a>. */
  @Test void testQuantifyCollectionOperators() {
    final SqlOperatorFixture f = fixture();
    QUANTIFY_OPERATORS.forEach(operator -> f.setFor(operator, SqlOperatorFixture.VmName.EXPAND));

    Function2<String, Boolean, Void> checkBoolean = (sql, result) -> {
      f.checkBoolean(sql.replace("COLLECTION", "ARRAY"), result);
      f.checkBoolean(sql.replace("COLLECTION", "MULTISET"), result);
      return null;
    };

    Function1<String, Void> checkNull = sql -> {
      f.checkNull(sql.replace("COLLECTION", "ARRAY"));
      f.checkNull(sql.replace("COLLECTION", "MULTISET"));
      return null;
    };

    checkNull.apply("1 = some (COLLECTION[2,3,null])");
    checkNull.apply("null = some (COLLECTION[1,2,3])");
    checkNull.apply("null = some (COLLECTION[1,2,null])");
    checkNull.apply("1 = some (COLLECTION[null,null,null])");
    checkNull.apply("null = some (COLLECTION[null,null,null])");

    checkBoolean.apply("1 = some (COLLECTION[1,2,null])", true);
    checkBoolean.apply("3 = some (COLLECTION[1,2])", false);

    checkBoolean.apply("1 <> some (COLLECTION[1])", false);
    checkBoolean.apply("2 <> some (COLLECTION[1,2,null])", true);
    checkBoolean.apply("3 <> some (COLLECTION[1,2,null])", true);

    checkBoolean.apply("1 < some (COLLECTION[1,2,null])", true);
    checkBoolean.apply("0 < some (COLLECTION[1,2,null])", true);
    checkBoolean.apply("2 < some (COLLECTION[1,2])", false);

    checkBoolean.apply("2 <= some (COLLECTION[1,2,null])", true);
    checkBoolean.apply("0 <= some (COLLECTION[1,2,null])", true);
    checkBoolean.apply("3 <= some (COLLECTION[1,2])", false);

    checkBoolean.apply("2 > some (COLLECTION[1,2,null])", true);
    checkBoolean.apply("3 > some (COLLECTION[1,2,null])", true);
    checkBoolean.apply("1 > some (COLLECTION[1,2])", false);

    checkBoolean.apply("2 >= some (COLLECTION[1,2,null])", true);
    checkBoolean.apply("3 >= some (COLLECTION[1,2,null])", true);
    checkBoolean.apply("0 >= some (COLLECTION[1,2])", false);

    f.check("SELECT 3 = some(x.t) FROM (SELECT ARRAY[1,2,3,null] as t) as x",
        "BOOLEAN", true);
    f.check("SELECT 4 = some(x.t) FROM (SELECT ARRAY[1,2,3] as t) as x",
        "BOOLEAN NOT NULL", false);
    f.check("SELECT 4 = some(x.t) FROM (SELECT ARRAY[1,2,3,null] as t) as x",
        "BOOLEAN", isNullValue());
    f.check("SELECT (SELECT * FROM UNNEST(ARRAY[3]) LIMIT 1) = "
            + "some(x.t) FROM (SELECT ARRAY[1,2,3,null] as t) as x",
        "BOOLEAN", true);


    checkNull.apply("1 = all (COLLECTION[1,1,null])");
    checkNull.apply("null = all (COLLECTION[1,2,3])");
    checkNull.apply("null = all (COLLECTION[1,2,null])");
    checkNull.apply("1 = all (COLLECTION[null,null,null])");
    checkNull.apply("null = all (COLLECTION[null,null,null])");

    checkBoolean.apply("1 = all (COLLECTION[1,1])", true);
    checkBoolean.apply("3 = all (COLLECTION[1,3,null])", false);

    checkBoolean.apply("1 <> all (COLLECTION[2,3,4])", true);
    checkBoolean.apply("2 <> all (COLLECTION[2,null])", false);

    checkBoolean.apply("1 < all (COLLECTION[2,3,4])", true);
    checkBoolean.apply("2 < all (COLLECTION[1,2,null])", false);

    checkBoolean.apply("2 <= all (COLLECTION[2,3,4])", true);
    checkBoolean.apply("1 <= all (COLLECTION[0,1,null])", false);

    checkBoolean.apply("2 > all (COLLECTION[0,1])", true);
    checkBoolean.apply("3 > all (COLLECTION[1,3,null])", false);

    checkBoolean.apply("2 >= all (COLLECTION[0,1,2])", true);
    checkBoolean.apply("3 >= all (COLLECTION[3,4,null])", false);

    f.check("SELECT 3 >= all(x.t) FROM (SELECT ARRAY[1,2,3] as t) as x",
        "BOOLEAN NOT NULL", true);
    f.check("SELECT 4 = all(x.t) FROM (SELECT ARRAY[1,2,3,null] as t) as x",
        "BOOLEAN", false);
    f.check("SELECT 4 = all(x.t) FROM (SELECT ARRAY[4,4,null] as t) as x",
        "BOOLEAN", isNullValue());
    f.check("SELECT (SELECT * FROM UNNEST(ARRAY[3]) LIMIT 1) = "
            + "all(x.t) FROM (SELECT ARRAY[3,3] as t) as x",
        "BOOLEAN", true);
  }

  @Test void testQuantifyOperatorsWithTypeException() {
    final SqlOperatorFixture f = fixture();
    QUANTIFY_OPERATORS.forEach(operator -> f.setFor(operator, SqlOperatorFixture.VmName.EXPAND));
    // some(List value)
    f.checkFails("SELECT ^cast(true as boolean) = some(1, 2, 3)^",
        "Values passed to = SOME operator must have compatible types", false);
    // some(Collection value)
    f.checkFails("^cast(true as boolean) = some (ARRAY[2,3,null])^",
        "Values passed to = SOME operator must have compatible types", false);
    f.checkFails("^cast(true as boolean) <> some (ARRAY[1,2,null])^",
        "Values passed to <> SOME operator must have compatible types", false);
    f.checkFails("^cast(true as boolean) < some (ARRAY[1,2,null])^",
        "Values passed to < SOME operator must have compatible types", false);
    f.checkFails("^cast(true as boolean) <= some (ARRAY[1,2,null])^",
        "Values passed to <= SOME operator must have compatible types", false);
    f.checkFails("^cast(true as boolean) > some (ARRAY[1,2,null])^",
        "Values passed to > SOME operator must have compatible types", false);
    f.checkFails("^cast(true as boolean) >= some (ARRAY[1,2,null])^",
        "Values passed to >= SOME operator must have compatible types", false);
    f.checkFails(
        "SELECT ^cast(true as boolean) = some(x.t)^ FROM (SELECT ARRAY[1,2,3,null] as t) as x",
        "Values passed to = SOME operator must have compatible types", false);
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6950">[CALCITE-6950]
   * Use ANY operator to check if an element exists in an array throws exception</a>. */
  @Test void testQuantifyOperatorsWithTypeCoercion() {
    final SqlOperatorFixture f = fixture();
    QUANTIFY_OPERATORS.forEach(operator -> f.setFor(operator, SqlOperatorFixture.VmName.EXPAND));

    f.checkNull("1.0 = some (ARRAY[2,3,null])");
    f.checkNull("1.0 = some (ARRAY[2,null,3])");
    f.enableTypeCoercion(false).checkFails(
        "^1.0 = some (ARRAY[2,3,null])^",
        "Values passed to = SOME operator must have compatible types",
        false);

    f.checkBoolean("1.0 = some (ARRAY[1,2,null])", true);
    f.checkBoolean("3.0 = some (ARRAY[1,2])", false);
    f.enableTypeCoercion(false).checkFails(
        "^3.0 = some (ARRAY[1,2])^",
        "Values passed to = SOME operator must have compatible types",
        false);

    f.checkBoolean(
        "'1970-01-01 01:23:45' = any (array[timestamp '1970-01-01 01:23:45',"
            + "timestamp '1970-01-01 01:23:46'])", true);
    f.checkBoolean(
        "'1970-01-01 01:23:47' = any (array[timestamp '1970-01-01 01:23:45',"
            + "timestamp '1970-01-01 01:23:46'])", false);
    f.enableTypeCoercion(false).checkFails(
        "^'1970-01-01 01:23:47' = any (array[timestamp '1970-01-01 01:23:45',"
            + "timestamp '1970-01-01 01:23:46'])^",
        "Values passed to = SOME operator must have compatible types",
        false);

    f.checkBoolean(
        "cast('1970-01-01 01:23:45' as timestamp) = any (array['1970-01-01 01:23:45',"
            + "'1970-01-01 01:23:46'])", true);
    f.checkBoolean(
        "cast('1970-01-01 01:23:47' as timestamp) = any (array['1970-01-01 01:23:45',"
            + "'1970-01-01 01:23:46'])", false);
    f.enableTypeCoercion(false).checkFails(
        "^cast('1970-01-01 01:23:47' as timestamp) = any (array['1970-01-01 01:23:45',"
            + "'1970-01-01 01:23:46'])^",
        "Values passed to = SOME operator must have compatible types",
        false);
  }

  @Test void testAnyValueFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.ANY_VALUE, VM_EXPAND);
    f.checkFails("any_value(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("any_value(1)", "INTEGER");
    f.checkType("any_value(1.2)", "DECIMAL(2, 1)");
    f.checkType("any_value(DISTINCT 1.5)", "DECIMAL(2, 1)");
    f.checkFails("^any_value()^",
        "Invalid number of arguments to function 'ANY_VALUE'. Was expecting 1 arguments",
        false);
    f.checkFails("^any_value(1, 2)^",
        "Invalid number of arguments to function 'ANY_VALUE'. Was expecting 1 arguments",
        false);
    final String[] values = {"0", "CAST(null AS INTEGER)", "2", "2"};
    if (!f.brokenTestsEnabled()) {
      return;
    }
    f.checkAgg("any_value(x)", values, isSingle("0"));
    f.checkAgg("any_value(CASE x WHEN 0 THEN NULL ELSE -1 END)", values, isSingle("-1"));
    f.checkAgg("any_value(DISTINCT CASE x WHEN 0 THEN NULL ELSE -1 END)", values, isSingle("-1"));
    f.checkAgg("any_value(DISTINCT x)", values, isSingle("0"));
  }

  @Test void testBoolAndFunc() {
    final SqlOperatorFixture f = fixture();
    // not in standard dialect
    final String[] values = {"true", "true", "null"};
    f.checkAggFails("^bool_and(x)^", values,
        "No match found for function signature BOOL_AND\\(<BOOLEAN>\\)", false);
    f.checkAggFails("^booland_agg(x)^", values,
        "No match found for function signature BOOLAND_AGG\\(<BOOLEAN>\\)", false);

    checkBoolAndFunc(f.withLibrary(SqlLibrary.POSTGRESQL));
    checkBoolAndFunc(f.withLibrary(SqlLibrary.REDSHIFT));
    checkBoolAndFunc(f.withLibrary(SqlLibrary.SPARK));
    checkBoolAndAggFunc(f.withLibrary(SqlLibrary.SNOWFLAKE));
  }

  private static void checkBoolAndFunc(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.BOOL_AND, VM_EXPAND);

    f.checkFails("bool_and(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("bool_and(true)", "BOOLEAN");
    f.checkFails("^bool_and(1)^",
        "Cannot apply 'BOOL_AND' to arguments of type 'BOOL_AND\\(<INTEGER>\\)'\\. "
            + "Supported form\\(s\\): 'BOOL_AND\\(<BOOLEAN>\\)'",
        false);
    f.checkFails("^bool_and()^",
        "Invalid number of arguments to function 'BOOL_AND'. Was expecting 1 arguments",
        false);
    f.checkFails("^bool_and(true, true)^",
        "Invalid number of arguments to function 'BOOL_AND'. Was expecting 1 arguments",
        false);

    final String[] values1 = {"true", "true", "null"};
    f.checkAgg("bool_and(x)", values1, isSingle(true));
    String[] values2 = {"true", "false", "null"};
    f.checkAgg("bool_and(x)", values2, isSingle(false));
    String[] values3 = {"true", "false", "false"};
    f.checkAgg("bool_and(x)", values3, isSingle(false));
    String[] values4 = {"null"};
    f.checkAgg("bool_and(x)", values4, isNullValue());
  }

  private static void checkBoolAndAggFunc(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.BOOLAND_AGG, VM_EXPAND);

    f.checkFails("booland_agg(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("booland_agg(true)", "BOOLEAN");
    f.checkFails("^booland_agg(1)^",
        "Cannot apply 'BOOLAND_AGG' to arguments of type 'BOOLAND_AGG\\(<INTEGER>\\)'\\. "
            + "Supported form\\(s\\): 'BOOLAND_AGG\\(<BOOLEAN>\\)'",
        false);
    f.checkFails("^booland_agg()^",
        "Invalid number of arguments to function 'BOOLAND_AGG'. Was expecting 1 arguments",
        false);
    f.checkFails("^booland_agg(true, true)^",
        "Invalid number of arguments to function 'BOOLAND_AGG'. Was expecting 1 arguments",
        false);

    final String[] values1 = {"true", "true", "null"};
    f.checkAgg("booland_agg(x)", values1, isSingle(true));
    String[] values2 = {"true", "false", "null"};
    f.checkAgg("booland_agg(x)", values2, isSingle(false));
    String[] values3 = {"true", "false", "false"};
    f.checkAgg("booland_agg(x)", values3, isSingle(false));
    String[] values4 = {"null"};
    f.checkAgg("booland_agg(x)", values4, isNullValue());
  }

  /** Test case for
   * <a href="https://issues.apache.org/jira/projects/CALCITE/issues/CALCITE-6094">
   * Linq4j.ConstantExpression.write crashes on special FP values</a>. */
  @Test void testInfinityExpression() {
    final SqlOperatorFixture f = fixture();
    f.check("SELECT CAST(10e70 AS REAL)", "REAL NOT NULL", "Infinity");
    f.check("SELECT CAST(-10e70 AS REAL)", "REAL NOT NULL", "-Infinity");
    // I could not write a test that generates NaN and triggers this issue.
    // I could not write tests with double that trigger this issue.
  }

  @Test void testBoolOrFunc() {
    final SqlOperatorFixture f = fixture();
    // not in standard dialect
    final String[] values = {"true", "true", "null"};
    f.checkAggFails("^bool_or(x)^", values,
        "No match found for function signature BOOL_OR\\(<BOOLEAN>\\)", false);
    f.checkAggFails("^boolor_agg(x)^", values,
        "No match found for function signature BOOLOR_AGG\\(<BOOLEAN>\\)", false);

    checkBoolOrFunc(f.withLibrary(SqlLibrary.POSTGRESQL));
    checkBoolOrFunc(f.withLibrary(SqlLibrary.REDSHIFT));
    checkBoolOrFunc(f.withLibrary(SqlLibrary.SPARK));
    checkBoolOrAggFunc(f.withLibrary(SqlLibrary.SNOWFLAKE));
  }

  private static void checkBoolOrFunc(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.BOOL_OR, VM_EXPAND);

    f.checkFails("bool_or(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("bool_or(true)", "BOOLEAN");
    f.checkFails("^bool_or(1)^",
        "Cannot apply 'BOOL_OR' to arguments of type 'BOOL_OR\\(<INTEGER>\\)'\\. "
            + "Supported form\\(s\\): 'BOOL_OR\\(<BOOLEAN>\\)'",
        false);
    f.checkFails("^bool_or()^",
        "Invalid number of arguments to function 'BOOL_OR'. Was expecting 1 arguments",
        false);
    f.checkFails("^bool_or(true, true)^",
        "Invalid number of arguments to function 'BOOL_OR'. Was expecting 1 arguments",
        false);

    final String[] values1 = {"true", "true", "null"};
    f.checkAgg("bool_or(x)", values1, isSingle(true));
    String[] values2 = {"true", "false", "null"};
    f.checkAgg("bool_or(x)", values2, isSingle(true));
    String[] values3 = {"false", "false", "false"};
    f.checkAgg("bool_or(x)", values3, isSingle(false));
    String[] values4 = {"null"};
    f.checkAgg("bool_or(x)", values4, isNullValue());
  }

  private static void checkBoolOrAggFunc(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.BOOLOR_AGG, VM_EXPAND);

    f.checkFails("boolor_agg(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("boolor_agg(true)", "BOOLEAN");
    f.checkFails("^boolor_agg(1)^",
        "Cannot apply 'BOOLOR_AGG' to arguments of type 'BOOLOR_AGG\\(<INTEGER>\\)'\\. "
            + "Supported form\\(s\\): 'BOOLOR_AGG\\(<BOOLEAN>\\)'",
        false);
    f.checkFails("^boolor_agg()^",
        "Invalid number of arguments to function 'BOOLOR_AGG'. Was expecting 1 arguments",
        false);
    f.checkFails("^boolor_agg(true, true)^",
        "Invalid number of arguments to function 'BOOLOR_AGG'. Was expecting 1 arguments",
        false);

    final String[] values1 = {"true", "true", "null"};
    f.checkAgg("boolor_agg(x)", values1, isSingle(true));
    String[] values2 = {"true", "false", "null"};
    f.checkAgg("boolor_agg(x)", values2, isSingle(true));
    String[] values3 = {"false", "false", "false"};
    f.checkAgg("boolor_agg(x)", values3, isSingle(false));
    String[] values4 = {"null"};
    f.checkAgg("boolor_agg(x)", values4, isNullValue());
  }

  @Test void testLogicalAndFunc() {
    final SqlOperatorFixture f = fixture();
    // not in standard dialect
    final String[] values = {"true", "true", "null"};
    f.checkAggFails("^logical_and(x)^", values,
        "No match found for function signature LOGICAL_AND\\(<BOOLEAN>\\)", false);

    checkLogicalAndFunc(f.withLibrary(SqlLibrary.BIG_QUERY));
  }

  private static void checkLogicalAndFunc(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.LOGICAL_AND, VM_EXPAND);

    f.checkFails("logical_and(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("logical_and(true)", "BOOLEAN");
    f.checkFails("^logical_and(1)^",
        "Cannot apply 'LOGICAL_AND' to arguments of type 'LOGICAL_AND\\(<INTEGER>\\)'\\. "
            + "Supported form\\(s\\): 'LOGICAL_AND\\(<BOOLEAN>\\)'",
        false);
    f.checkFails("^logical_and()^",
        "Invalid number of arguments to function 'LOGICAL_AND'. Was expecting 1 arguments",
        false);
    f.checkFails("^logical_and(true, true)^",
        "Invalid number of arguments to function 'LOGICAL_AND'. Was expecting 1 arguments",
        false);

    final String[] values1 = {"true", "true", "null"};
    f.checkAgg("logical_and(x)", values1, isSingle(true));
    String[] values2 = {"true", "false", "null"};
    f.checkAgg("logical_and(x)", values2, isSingle(false));
    String[] values3 = {"true", "false", "false"};
    f.checkAgg("logical_and(x)", values3, isSingle(false));
    String[] values4 = {"null"};
    f.checkAgg("logical_and(x)", values4, isNullValue());
  }

  @Test void testLogicalOrFunc() {
    final SqlOperatorFixture f = fixture();
    // not in standard dialect
    final String[] values = {"true", "true", "null"};
    f.checkAggFails("^logical_or(x)^", values,
        "No match found for function signature LOGICAL_OR\\(<BOOLEAN>\\)", false);

    checkLogicalOrFunc(f.withLibrary(SqlLibrary.BIG_QUERY));
  }

  private static void checkLogicalOrFunc(SqlOperatorFixture f) {
    f.setFor(SqlLibraryOperators.LOGICAL_OR, VM_EXPAND);

    f.checkFails("logical_or(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("logical_or(true)", "BOOLEAN");
    f.checkFails("^logical_or(1)^",
        "Cannot apply 'LOGICAL_OR' to arguments of type 'LOGICAL_OR\\(<INTEGER>\\)'\\. "
            + "Supported form\\(s\\): 'LOGICAL_OR\\(<BOOLEAN>\\)'",
        false);
    f.checkFails("^logical_or()^",
        "Invalid number of arguments to function 'LOGICAL_OR'. Was expecting 1 arguments",
        false);
    f.checkFails("^logical_or(true, true)^",
        "Invalid number of arguments to function 'LOGICAL_OR'. Was expecting 1 arguments",
        false);

    final String[] values1 = {"true", "true", "null"};
    f.checkAgg("logical_or(x)", values1, isSingle(true));
    String[] values2 = {"true", "false", "null"};
    f.checkAgg("logical_or(x)", values2, isSingle(true));
    String[] values3 = {"false", "false", "false"};
    f.checkAgg("logical_or(x)", values3, isSingle(false));
    String[] values4 = {"null"};
    f.checkAgg("logical_or(x)", values4, isNullValue());
  }

  @Test void testBitXorOperatorParserFunc() throws SqlParseException {
    String sql = "SELECT 5 ^ 3 ";
    SqlNode sqlNode = SqlParser.create(sql,  SqlParser.Config.DEFAULT).parseStmt();

    assertInstanceOf(SqlSelect.class, sqlNode);
    SqlSelect select = (SqlSelect) sqlNode;

    SqlNode selectItem = select.getSelectList().get(0);
    assertInstanceOf(SqlBasicCall.class, selectItem);
    SqlBasicCall call = (SqlBasicCall) selectItem;
    assertEquals(SqlStdOperatorTable.BITXOR_OPERATOR, call.getOperator());
    assertEquals(2, call.getOperandList().size());
  }

  @Test void testBitXorOperatorScalarFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.BITXOR_OPERATOR, VM_EXPAND);

    // Basic XOR between signed integer types
    // Notice that ^ needs to be escaped (doubled) everywhere
    f.checkScalar("2 ^^ 3", "1", "INTEGER NOT NULL");
    f.checkScalar("CAST(2 AS INTEGER) ^^ CAST(3 AS BIGINT)", "1", "BIGINT NOT NULL");
    f.checkScalar("-5 ^^ 7", "-4", "INTEGER NOT NULL");
    f.checkScalar("-5 ^^ -7", "2", "INTEGER NOT NULL");
    f.checkScalar("CAST(-5 AS TINYINT) ^^ CAST(7 AS TINYINT)", "-4", "TINYINT NOT NULL");
    f.checkScalar("CAST(-5 AS TINYINT) ^^ CAST(-31 AS TINYINT)", "26", "TINYINT NOT NULL");

    // Type propagation check
    f.checkType("CAST(2 AS TINYINT) ^^ CAST(6 AS TINYINT)", "TINYINT NOT NULL");
    f.checkType("CAST(2 AS SMALLINT) ^^ CAST(6 AS SMALLINT)", "SMALLINT NOT NULL");
    f.checkType("CAST(2 AS BIGINT) ^^ CAST(6 AS BIGINT)", "BIGINT NOT NULL");

    // XOR on binary/varbinary types
    f.checkScalar("CAST(x'0201' AS BINARY(2)) ^^ CAST(x'07f9' AS BINARY(2))", "05f8",
        "BINARY(2) NOT NULL");
    f.checkScalar("CAST(x'0201' AS VARBINARY(2)) ^^ CAST(x'07f9' AS VARBINARY(2))", "05f8",
        "VARBINARY(2) NOT NULL");

    // Binary length mismatch
    f.checkFails("CAST(x'0201' AS VARBINARY) ^^ CAST(x'02' AS VARBINARY)",
        "Different length for bitwise operands: the first: 2, the second: 1", true);

    f.checkNull("CAST(NULL AS INTEGER) ^^ 1");
    f.checkNull("1 ^^ CAST(NULL AS INTEGER)");
    f.checkNull("CAST(NULL AS BIGINT) ^^ CAST(NULL AS BIGINT)");
    // Test with unsigned numbers
    f.checkScalar("CAST(255 AS TINYINT UNSIGNED) ^^ CAST(15 AS TINYINT UNSIGNED)",
        "240", "TINYINT UNSIGNED NOT NULL");

    f.checkScalar("CAST(65535 AS SMALLINT UNSIGNED) ^^ CAST(255 AS SMALLINT UNSIGNED)",
        "65280", "SMALLINT UNSIGNED NOT NULL");

    f.checkScalar("CAST(4294967295 AS INTEGER UNSIGNED) ^^ CAST(255 AS INTEGER UNSIGNED)",
        "4294967040", "INTEGER UNSIGNED NOT NULL");

    f.checkScalar("CAST(2147483648 AS INTEGER UNSIGNED) ^^ CAST(123456789 AS INTEGER UNSIGNED)",
        "2270940437", "INTEGER UNSIGNED NOT NULL");

    // NULL handling (unsigned)
    f.checkNull("CAST(NULL AS INTEGER UNSIGNED) ^^ CAST(255 AS INTEGER UNSIGNED)");

    f.checkNull("CAST(4294967295 AS INTEGER UNSIGNED) ^^ CAST(NULL AS INTEGER UNSIGNED)");

    f.checkNull("CAST(NULL AS INTEGER UNSIGNED) ^^ CAST(NULL AS INTEGER UNSIGNED)");
  }

  /**
   * Test cases for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7109">[CALCITE-7109]
   * Implement SHIFT_LEFT operator </a>.
   */
  @Test void testLeftShiftScalarFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.BIT_LEFT_SHIFT, VmName.EXPAND);

    // === Basic functionality ===
    f.checkScalar("2 << 2", "8", "INTEGER NOT NULL");
    f.checkScalar("1 << 10", "1024", "INTEGER NOT NULL");
    f.checkScalar("0 << 5", "0", "INTEGER NOT NULL");

    // === Type coercion and signed behavior ===
    f.checkScalar("CAST(2 AS INTEGER) << CAST(3 AS BIGINT)", "16", "INTEGER NOT NULL");
    f.checkScalar("-5 << 2", "-20", "INTEGER NOT NULL");
    f.checkScalar("-5 << 3", "-40", "INTEGER NOT NULL");
    f.checkScalar("CAST(-5 AS TINYINT) << CAST(2 AS TINYINT)", "-20", "TINYINT NOT NULL");

    // === Verify return type matches first argument type ===
    f.checkType("CAST(2 AS TINYINT) << CAST(3 AS TINYINT)", "TINYINT NOT NULL");
    f.checkType("CAST(2 AS SMALLINT) << CAST(3 AS SMALLINT)", "SMALLINT NOT NULL");
    f.checkType("CAST(2 AS INTEGER) << CAST(3 AS INTEGER)", "INTEGER NOT NULL");
    f.checkType("CAST(2 AS BIGINT) << CAST(3 AS BIGINT)", "BIGINT NOT NULL");

    // === BigInt shifts with explicit BIGINT inputs ===
    f.checkScalar("CAST(1 AS BIGINT) << 62", BigInteger.ONE.shiftLeft(62).toString(),
        "BIGINT NOT NULL"); // 2^62
    f.checkScalar("CAST(1 AS BIGINT) << 63",
        BigInteger.ONE.shiftLeft(63).multiply(BigInteger.valueOf(-1)).toString(),
        "BIGINT NOT NULL"); // overflow
    f.checkScalar("CAST(4611686018427387904 AS BIGINT) << 1",
        BigInteger.valueOf(4611686018427387904L).shiftLeft(1).
            multiply(BigInteger.valueOf(-1)).toString(),
        "BIGINT NOT NULL");
    f.checkScalar("CAST(2305843009213693952 AS BIGINT) << 2",
        BigInteger.valueOf(2305843009213693952L).shiftLeft(2).
            multiply(BigInteger.valueOf(-1)).toString(),
        "BIGINT NOT NULL");
    f.checkScalar("CAST(-4611686018427387904 AS BIGINT) << 1",
        BigInteger.valueOf(-4611686018427387904L).shiftLeft(1).toString(), "BIGINT NOT NULL");
    f.checkScalar("CAST(-1 AS BIGINT) << 63",
        BigInteger.ONE.shiftLeft(63).multiply(BigInteger.valueOf(-1)).toString(),
        "BIGINT NOT NULL");
    f.checkScalar("CAST(9223372036854775807 AS BIGINT) << 0",
        BigInteger.valueOf(Long.MAX_VALUE).shiftLeft(0).toString(), "BIGINT NOT NULL");
    f.checkScalar("CAST(1000000000 AS BIGINT) << 35",
        "-2533749779419103232", "BIGINT NOT NULL");
    f.checkScalar("CAST(9223372036854775807 AS BIGINT) << 1",
        "-2", "BIGINT NOT NULL");

    // === Java shift semantics: bits masked to 5/6 bits ===
    f.checkScalar("CAST(1 AS BIGINT) << 32",
        BigInteger.ONE.shiftLeft(32).toString(), "BIGINT NOT NULL");
    f.checkScalar("CAST(1 AS BIGINT) << 50",
        BigInteger.ONE.shiftLeft(50).toString(), "BIGINT NOT NULL");
    f.checkScalar("CAST(1 AS BIGINT) << 100",
        BigInteger.ONE.shiftLeft(100 & 63).toString(), "BIGINT NOT NULL");
    f.checkScalar("CAST(100 AS BIGINT) << 50",
        BigInteger.valueOf(100L).shiftLeft(50).toString(), "BIGINT NOT NULL");

    f.checkScalar("CAST(100 AS BIGINT) << 50",
        BigInteger.valueOf(100L).shiftLeft(50).toString(), "BIGINT NOT NULL");

    // === Unsigned types ===
    f.checkScalar("CAST(63 AS TINYINT UNSIGNED) << 2", "252", "TINYINT UNSIGNED NOT NULL");
    f.checkScalar("CAST(255 AS SMALLINT UNSIGNED) << 8", "65280", "SMALLINT UNSIGNED NOT NULL");
    f.checkScalar("CAST(65535 AS INTEGER UNSIGNED) << 16", "4294901760",
        "INTEGER UNSIGNED NOT NULL");
    f.checkScalar("CAST(1 AS INTEGER UNSIGNED) << 31", "2147483648", "INTEGER UNSIGNED NOT NULL");
    f.checkScalar("CAST(1 AS INTEGER UNSIGNED) << -1", "0", "INTEGER UNSIGNED NOT NULL");

    // === Negative shift counts ===
    f.checkScalar("8 << -1", "0", "INTEGER NOT NULL");
    f.checkScalar("16 << -2", "0", "INTEGER NOT NULL");

    // === Shift by zero and large shifts ===
    f.checkScalar("0 << 32", "0", "INTEGER NOT NULL");
    f.checkScalar("0 << 100", "0", "INTEGER NOT NULL");

    // === Non-zero values with large shifts ===
    f.checkScalar("1 << 32", "1", "INTEGER NOT NULL");
    f.checkScalar("1 << 40", "256", "INTEGER NOT NULL");
    f.checkScalar("2 << 50", "524288", "INTEGER NOT NULL");
    f.checkScalar("123 << 60", "-1342177280", "INTEGER NOT NULL");

    // === Binary type tests ===
    f.checkScalar("CAST(X'FF' AS BINARY(1)) << 1", "fe", "BINARY(1) NOT NULL");
    f.checkScalar("CAST(X'0F' AS BINARY(1)) << 4", "f0", "BINARY(1) NOT NULL");
    f.checkScalar("CAST(X'01' AS BINARY(1)) << 3", "08", "BINARY(1) NOT NULL");
    f.checkScalar("CAST(X'00' AS BINARY(1)) << 5", "00", "BINARY(1) NOT NULL");

    f.checkScalar("CAST(X'FFFF' AS BINARY(2)) << 1", "feff", "BINARY(2) NOT NULL");
    f.checkScalar("CAST(X'1234' AS BINARY(2)) << 4", "2041", "BINARY(2) NOT NULL");
    f.checkScalar("CAST(X'1234' AS BINARY(2)) << 8", "0012", "BINARY(2) NOT NULL");

    f.checkScalar("CAST(X'FF' AS BINARY(1)) << 8", "ff", "BINARY(1) NOT NULL");
    f.checkScalar("CAST(X'FFFF' AS BINARY(2)) << 16", "ffff", "BINARY(2) NOT NULL");

    f.checkScalar("CAST(X'ABCD' AS BINARY(2)) << 0", "abcd", "BINARY(2) NOT NULL");
    f.checkScalar("CAST(X'123456' AS BINARY(3)) << 4", "204163", "BINARY(3) NOT NULL");
    f.checkScalar("CAST(X'8000' AS BINARY(2)) << 1", "0001", "BINARY(2) NOT NULL");
    f.checkScalar("CAST(X'4000' AS BINARY(2)) << 1", "8000", "BINARY(2) NOT NULL");
    f.checkScalar("CAST(X'0F' AS BINARY(1)) << -4", "f0", "BINARY(1) NOT NULL");

    // === Invalid argument types ===
    f.checkFails("^1.2 << 2^",
        "Cannot apply '<<' to arguments of type '<DECIMAL\\(2, 1\\)> << <INTEGER>'\\. Supported "
            + "form\\(s\\): '<INTEGER> << <INTEGER>'\\n'<BINARY> << <INTEGER>'\\n'<UNSIGNED_NUMERIC> "
            + "<< <INTEGER>'",
        false);

    // === Null propagation ===
    f.checkNull("CAST(NULL AS INTEGER) << 5");
    f.checkNull("10 << CAST(NULL AS INTEGER)");
    f.checkNull("CAST(NULL AS INTEGER) << CAST(NULL AS INTEGER)");
    f.checkNull("CAST(NULL AS INTEGER UNSIGNED) << 2");
  }

  /**
   * Test cases for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7109">[CALCITE-7109]
   * Implement SHIFT_LEFT operator </a>.
   */
  @Test void testLeftShiftFunctionCall() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.BIT_LEFT_SHIFT, VmName.EXPAND);

    // === Basic functionality ===
    f.checkScalar("LEFTSHIFT(2, 2)", "8", "INTEGER NOT NULL");
    f.checkScalar("LEFTSHIFT(1, 10)", "1024", "INTEGER NOT NULL");
    f.checkScalar("LEFTSHIFT(0, 5)", "0", "INTEGER NOT NULL");

    // === Type coercion and signed behavior ===
    f.checkScalar("LEFTSHIFT(CAST(2 AS INTEGER), CAST(3 AS BIGINT))", "16", "INTEGER NOT NULL");
    f.checkScalar("LEFTSHIFT(-5, 2)", "-20", "INTEGER NOT NULL");
    f.checkScalar("LEFTSHIFT(-5, 3)", "-40", "INTEGER NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(-5 AS TINYINT), CAST(2 AS TINYINT))", "-20", "TINYINT NOT NULL");

    // === Verify return type matches first argument type ===
    f.checkType("LEFTSHIFT(CAST(2 AS TINYINT), CAST(3 AS TINYINT))", "TINYINT NOT NULL");
    f.checkType("LEFTSHIFT(CAST(2 AS SMALLINT), CAST(3 AS SMALLINT))", "SMALLINT NOT NULL");
    f.checkType("LEFTSHIFT(CAST(2 AS INTEGER), CAST(3 AS INTEGER))", "INTEGER NOT NULL");
    f.checkType("LEFTSHIFT(CAST(2 AS BIGINT), CAST(3 AS BIGINT))", "BIGINT NOT NULL");

    // === BigInt shifts with explicit BIGINT inputs ===
    f.checkScalar("LEFTSHIFT(CAST(1 AS BIGINT), 62)",
        "4611686018427387904", "BIGINT NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(1 AS BIGINT), 63)",
        BigInteger.ONE.shiftLeft(63).multiply(BigInteger.valueOf(-1)).toString(),
        "BIGINT NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(4611686018427387904 AS BIGINT), 1)",
        BigInteger.ONE.shiftLeft(63).multiply(BigInteger.valueOf(-1)).toString(),
        "BIGINT NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(2305843009213693952 AS BIGINT), 2)",
        BigInteger.ONE.shiftLeft(63).multiply(BigInteger.valueOf(-1)).toString(),
        "BIGINT NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(-4611686018427387904 AS BIGINT), 1)",
        BigInteger.ONE.shiftLeft(63).multiply(BigInteger.valueOf(-1)).toString(),
        "BIGINT NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(-1 AS BIGINT), 63)",
        BigInteger.ONE.shiftLeft(63).multiply(BigInteger.valueOf(-1)).toString(),
        "BIGINT NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(9223372036854775807 AS BIGINT), 0)",
        BigInteger.ONE.shiftLeft(63).add(BigInteger.valueOf(-1)).toString(), "BIGINT NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(1000000000 AS BIGINT), 35)",
        "-2533749779419103232", "BIGINT NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(9223372036854775807 AS BIGINT), 1)",
        "-2", "BIGINT NOT NULL");

    // === Java shift semantics: bits masked to 5/6 bits ===
    f.checkScalar("LEFTSHIFT(CAST(1 AS BIGINT), 32)", "4294967296", "BIGINT NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(1 AS BIGINT), 50)", "1125899906842624", "BIGINT NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(1 AS BIGINT), 100)", "68719476736", "BIGINT NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(100 AS BIGINT), 50)", "112589990684262400", "BIGINT NOT NULL");

    // === Unsigned types ===
    f.checkScalar("LEFTSHIFT(CAST(63 AS TINYINT UNSIGNED), 2)", "252", "TINYINT UNSIGNED NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(255 AS SMALLINT UNSIGNED), 8)", "65280",
        "SMALLINT UNSIGNED NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(65535 AS INTEGER UNSIGNED), 16)", "4294901760",
        "INTEGER UNSIGNED NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(1 AS INTEGER UNSIGNED), 31)", "2147483648",
        "INTEGER UNSIGNED NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(1 AS INTEGER UNSIGNED), -1)", "0",
        "INTEGER UNSIGNED NOT NULL");

    // === Negative shifts ===
    f.checkScalar("LEFTSHIFT(8, -1)", "0", "INTEGER NOT NULL");
    f.checkScalar("LEFTSHIFT(16, -2)", "0", "INTEGER NOT NULL");

    // === Large shifts ===
    f.checkScalar("LEFTSHIFT(0, 32)", "0", "INTEGER NOT NULL");
    f.checkScalar("LEFTSHIFT(0, 100)", "0", "INTEGER NOT NULL");

    // === Binary types ===
    f.checkScalar("LEFTSHIFT(CAST(X'FF' AS BINARY(1)), 1)", "fe", "BINARY(1) NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(X'0F' AS BINARY(1)), 4)", "f0", "BINARY(1) NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(X'01' AS BINARY(1)), 3)", "08", "BINARY(1) NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(X'00' AS BINARY(1)), 5)", "00", "BINARY(1) NOT NULL");

    f.checkScalar("LEFTSHIFT(CAST(X'FFFF' AS BINARY(2)), 1)", "feff", "BINARY(2) NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(X'1234' AS BINARY(2)), 4)", "2041", "BINARY(2) NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(X'1234' AS BINARY(2)), 8)", "0012", "BINARY(2) NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(X'FF' AS BINARY(1)), 8)", "ff", "BINARY(1) NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(X'FFFF' AS BINARY(2)), 16)", "ffff", "BINARY(2) NOT NULL");

    f.checkScalar("LEFTSHIFT(CAST(X'ABCD' AS BINARY(2)), 0)", "abcd", "BINARY(2) NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(X'123456' AS BINARY(3)), 4)", "204163", "BINARY(3) NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(X'8000' AS BINARY(2)), 1)", "0001", "BINARY(2) NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(X'4000' AS BINARY(2)), 1)", "8000", "BINARY(2) NOT NULL");
    f.checkScalar("LEFTSHIFT(CAST(X'0F' AS BINARY(1)), -4)", "f0", "BINARY(1) NOT NULL");
    // === Invalid types ===
    f.checkFails("^LEFTSHIFT(1.2, 2)^",
        "Cannot apply 'LEFTSHIFT' to arguments of type 'LEFTSHIFT\\(<DECIMAL\\(2, 1\\)>, <INTEGER>\\)'\\. Supported form\\(s\\): 'LEFTSHIFT\\(<INTEGER>, <INTEGER>\\)'\\n'LEFTSHIFT\\(<BINARY>, <INTEGER>\\)'\\n'LEFTSHIFT\\(<UNSIGNED_NUMERIC>, <INTEGER>\\)'",
        false);

    // === Nulls ===
    f.checkNull("LEFTSHIFT(CAST(NULL AS INTEGER), 5)");
    f.checkNull("LEFTSHIFT(10, CAST(NULL AS INTEGER))");
    f.checkNull("LEFTSHIFT(CAST(NULL AS INTEGER), CAST(NULL AS INTEGER))");
    f.checkNull("LEFTSHIFT(CAST(NULL AS INTEGER UNSIGNED), 2)");
  }

  /**
   * Test cases for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-7184">[CALCITE-7184]
   * Implement BIT_AND operator </a>.
   */
  @Test void testBitAndOperatorScalarFunc() {
    final SqlOperatorFixture f = fixture();
    // Set the test fixture for the BITAND_OPERATOR
    f.setFor(SqlStdOperatorTable.BITAND_OPERATOR, VmName.EXPAND);

    // Basic test cases
    f.checkScalar("2 & 3", "2", "INTEGER NOT NULL");
    f.checkScalar("5 & 3", "1", "INTEGER NOT NULL");
    f.checkScalar("-5 & -3", "-7", "INTEGER NOT NULL");
    f.checkScalar("8 & 7", "0", "INTEGER NOT NULL");
    f.checkScalar("-1 & 255", "255", "INTEGER NOT NULL");

    // Tests with different integer types and type coercion
    f.checkScalar("CAST(2 AS INTEGER) & CAST(3 AS BIGINT)", "2", "BIGINT NOT NULL");
    f.checkScalar("-5 & 7", "3", "INTEGER NOT NULL");
    f.checkScalar("-5 & -31", "-31", "INTEGER NOT NULL");
    f.checkScalar("CAST(-5 AS TINYINT) & CAST(7 AS TINYINT)", "3", "TINYINT NOT NULL");

    // Verify return types
    f.checkType("CAST(2 AS TINYINT) & CAST(3 AS TINYINT)", "TINYINT NOT NULL");
    f.checkType("CAST(2 AS SMALLINT) & CAST(6 AS SMALLINT)", "SMALLINT NOT NULL");
    f.checkType("CAST(2 AS BIGINT) & CAST(6 AS BIGINT)", "BIGINT NOT NULL");

    // Mixed cases: negative signed integer with unsigned integer.
    // Ensure consistency with MySQL semantics where negative values behave as all bits set.
    f.checkScalar("CAST(1 AS INTEGER UNSIGNED) & CAST(255 AS INTEGER UNSIGNED)",
        "1", "INTEGER UNSIGNED NOT NULL");
    f.checkScalar("CAST(5 AS INTEGER UNSIGNED) & CAST(3 AS INTEGER UNSIGNED)",
        "1", "INTEGER UNSIGNED NOT NULL");

    // Pure unsigned cases: bitwise AND across unsigned integer families.
    // Verify result type and range are preserved after the operation.
    f.checkScalar("CAST(255 AS INTEGER UNSIGNED) & CAST(65535 AS INTEGER UNSIGNED)",
        "255", "INTEGER UNSIGNED NOT NULL");
    f.checkScalar("CAST(4294967295 AS BIGINT UNSIGNED) & CAST(255 AS BIGINT UNSIGNED)",
        "255", "BIGINT UNSIGNED NOT NULL");
    f.checkScalar("CAST(15 AS INTEGER UNSIGNED) & CAST(7 AS INTEGER)",
        "7", "INTEGER UNSIGNED NOT NULL");
    f.checkScalar("CAST(255 AS INTEGER UNSIGNED) & CAST(-1 AS INTEGER)",
        "255", "INTEGER UNSIGNED NOT NULL");
    f.checkScalar("CAST(128 AS INTEGER UNSIGNED) & CAST(127 AS INTEGER)",
        "0", "INTEGER UNSIGNED NOT NULL");

    // Test cases for ULong & Integer -> ULong
    f.checkScalar("CAST(4294967295 AS BIGINT UNSIGNED) & CAST(255 AS INTEGER)",
        "255", "BIGINT UNSIGNED NOT NULL");
    f.checkScalar("CAST(1099511627775 AS BIGINT UNSIGNED) & CAST(-1 AS INTEGER)",
        "1099511627775", "BIGINT UNSIGNED NOT NULL");
    f.checkScalar("CAST(0 AS BIGINT UNSIGNED) & CAST(255 AS INTEGER)",
        "0", "BIGINT UNSIGNED NOT NULL");

    // Test cases for Integer & ULong -> ULong
    f.checkScalar("CAST(-1 AS INTEGER) & CAST(4294967295 AS BIGINT UNSIGNED)",
        "4294967295", "BIGINT UNSIGNED NOT NULL");
    f.checkScalar("CAST(127 AS INTEGER) & CAST(255 AS BIGINT UNSIGNED)",
        "127", "BIGINT UNSIGNED NOT NULL");
    f.checkScalar("CAST(0 AS INTEGER) & CAST(1099511627775 AS BIGINT UNSIGNED)",
        "0", "BIGINT UNSIGNED NOT NULL");

    // Test cases for UShort & Integer -> Integer
    f.checkScalar("CAST(255 AS SMALLINT UNSIGNED) & CAST(15 AS INTEGER)",
        "15", "INTEGER NOT NULL");
    f.checkScalar("CAST(65535 AS SMALLINT UNSIGNED) & CAST(-1 AS INTEGER)",
        "65535", "INTEGER NOT NULL");
    f.checkScalar("CAST(0 AS SMALLINT UNSIGNED) & CAST(255 AS INTEGER)",
        "0", "INTEGER NOT NULL");

    // Test cases for Integer & UShort -> Integer
    f.checkScalar("CAST(-1 AS INTEGER) & CAST(255 AS SMALLINT UNSIGNED)",
        "255", "INTEGER NOT NULL");
    f.checkScalar("CAST(127 AS INTEGER) & CAST(128 AS SMALLINT UNSIGNED)",
        "0", "INTEGER NOT NULL");
    f.checkScalar("CAST(65535 AS INTEGER) & CAST(255 AS SMALLINT UNSIGNED)",
        "255", "INTEGER NOT NULL");

    // Edge cases with powers of 2
    f.checkScalar("CAST(1024 AS INTEGER UNSIGNED) & CAST(512 AS INTEGER)",
        "0", "INTEGER UNSIGNED NOT NULL");
    f.checkScalar("CAST(1023 AS INTEGER UNSIGNED) & CAST(512 AS INTEGER)",
        "512", "INTEGER UNSIGNED NOT NULL");

    // Mixed operations with zero
    f.checkScalar("CAST(0 AS INTEGER UNSIGNED) & CAST(-1 AS INTEGER)",
        "0", "INTEGER UNSIGNED NOT NULL");
    f.checkScalar("CAST(4294967295 AS BIGINT UNSIGNED) & CAST(0 AS INTEGER)",
        "0", "BIGINT UNSIGNED NOT NULL");

    // UInteger & Long -> ULong
    f.checkScalar("CAST(255 AS INTEGER UNSIGNED) & CAST(127 AS BIGINT)",
        "127", "BIGINT NOT NULL");
    f.checkScalar("CAST(4294967295 AS INTEGER UNSIGNED) & CAST(-1 AS BIGINT)",
        "-1", "BIGINT NOT NULL");
    f.checkScalar("CAST(1024 AS INTEGER UNSIGNED) & CAST(512 AS BIGINT)",
        "0", "BIGINT NOT NULL");
    f.checkScalar("CAST(0 AS INTEGER UNSIGNED) & CAST(9223372036854775807 AS BIGINT)",
        "0", "BIGINT NOT NULL");

    // Long & UInteger -> ULong
    f.checkScalar("CAST(-1 AS BIGINT) & CAST(255 AS INTEGER UNSIGNED)",
        "255", "BIGINT NOT NULL");
    f.checkScalar("CAST(127 AS BIGINT) & CAST(128 AS INTEGER UNSIGNED)",
        "0", "BIGINT NOT NULL");
    f.checkScalar("CAST(9223372036854775807 AS BIGINT) & CAST(1 AS INTEGER UNSIGNED)",
        "1", "BIGINT NOT NULL");
    f.checkScalar("CAST(0 AS BIGINT) & CAST(4294967295 AS INTEGER UNSIGNED)",
        "0", "BIGINT NOT NULL");

    // Edge cases with negative Long values
    f.checkScalar("CAST(-128 AS BIGINT) & CAST(255 AS INTEGER UNSIGNED)",
        "128", "BIGINT NOT NULL");
    f.checkScalar("CAST(-256 AS BIGINT) & CAST(255 AS INTEGER UNSIGNED)",
        "0", "BIGINT NOT NULL");

    // Powers of 2 tests
    f.checkScalar("CAST(65536 AS INTEGER UNSIGNED) & CAST(32768 AS BIGINT)",
        "0", "BIGINT NOT NULL");
    f.checkScalar("CAST(65535 AS INTEGER UNSIGNED) & CAST(32768 AS BIGINT)",
        "32768", "BIGINT NOT NULL");

    // Large number tests
    f.checkScalar("CAST(4294967295 AS INTEGER UNSIGNED) & CAST(4294967296 AS BIGINT)",
        "4294967296", "BIGINT NOT NULL");
    f.checkScalar("CAST(4294967295 AS INTEGER UNSIGNED) & CAST(4294967295 AS BIGINT)",
        "4294967295", "BIGINT NOT NULL");

    // Mixed sign tests
    f.checkScalar("CAST(4294967295 AS INTEGER UNSIGNED) & CAST(-4294967296 AS BIGINT)",
        "-4294967296", "BIGINT NOT NULL");
    // Binary type tests
    f.checkScalar("CAST(x'0201' AS BINARY(2)) & CAST(x'07f9' AS BINARY(2))", "0201",
        "BINARY(2) NOT NULL");
    f.checkScalar("CAST(x'0201' AS VARBINARY(2)) & CAST(x'07f9' AS VARBINARY(2))", "0201",
        "VARBINARY(2) NOT NULL");

    // Test invalid argument types
    f.checkFails("^1.2 & 2^",
        "Cannot apply '&' to arguments of type '<DECIMAL\\(2, 1\\)> & <INTEGER>'\\. Supported form\\(s\\): '<INTEGER> & <INTEGER>'\\n'<BINARY> & <BINARY>'\\n'<UNSIGNED_NUMERIC> & <UNSIGNED_NUMERIC>'\\n'<UNSIGNED_NUMERIC> & <INTEGER>'\\n'<INTEGER> & <UNSIGNED_NUMERIC>'",
        false);

    // NULL value tests
    f.checkNull("CAST(NULL AS INTEGER) & 5");
    f.checkNull("10 & CAST(NULL AS INTEGER)");
    f.checkNull("CAST(NULL AS INTEGER) & CAST(NULL AS INTEGER)");

    // Test binary operands with different lengths
    f.checkFails("CAST(x'0201' AS VARBINARY) & CAST(x'02' AS VARBINARY)",
        "Different length for bitwise operands: the first: 2, the second: 1",
        true);

    // Boundary cases for bitwise operations
    f.checkScalar("0 & 0", "0", "INTEGER NOT NULL");
    f.checkScalar("-1 & -1", "-1", "INTEGER NOT NULL");
    f.checkScalar("2147483647 & 0", "0", "INTEGER NOT NULL");  // MAX_INT & 0
    f.checkScalar("-2147483648 & -1", "-2147483648", "INTEGER NOT NULL");  // MIN_INT & -1
  }

  @Test void testBitAndScalarFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.BITAND, VmName.EXPAND);
    f.checkFails("bitand(^*^)", "Unknown identifier '\\*'", false);
    f.checkScalar("bitand(2, 3)", "2", "INTEGER NOT NULL");
    f.checkScalar("bitand(CAST(2 AS INTEGER), CAST(3 AS BIGINT))", "2", "BIGINT NOT NULL");
    f.checkScalar("bitand(-5, 7)", "3", "INTEGER NOT NULL");
    f.checkScalar("bitand(-5, -31)", "-31", "INTEGER NOT NULL");
    f.checkScalar("bitand(CAST(-5 AS TINYINT), CAST(7 AS TINYINT))", "3", "TINYINT NOT NULL");
    f.checkScalar("bitand(CAST(-5 AS TINYINT), CAST(-31 AS TINYINT))", "-31", "TINYINT NOT NULL");
    f.checkType("bitand(CAST(2 AS TINYINT), CAST(6 AS TINYINT))", "TINYINT NOT NULL");
    f.checkType("bitand(CAST(2 AS SMALLINT), CAST(6 AS SMALLINT))", "SMALLINT NOT NULL");
    f.checkType("bitand(CAST(2 AS BIGINT), CAST(6 AS BIGINT))", "BIGINT NOT NULL");
    f.checkScalar("bitand(CAST(x'0201' AS BINARY(2)), CAST(x'07f9' AS BINARY(2)))", "0201",
        "BINARY(2) NOT NULL");
    f.checkScalar("bitand(CAST(x'0201' AS VARBINARY(2)), CAST(x'07f9' AS VARBINARY(2)))", "0201",
        "VARBINARY(2) NOT NULL");
    f.checkFails("^bitand(1.2, 1.3)^",
        "Cannot apply 'BITAND' to arguments of type '"
            + "BITAND\\(<DECIMAL\\(2, 1\\)>, <DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): '"
            + "BITAND\\(<INTEGER>, <INTEGER>\\)'\n"
            + "'BITAND\\(<BINARY>, <BINARY>\\)'",
        false);
    f.checkFails("^bitand()^",
        "Invalid number of arguments to function 'BITAND'. Was expecting 2 arguments",
        false);
    f.checkFails("^bitand(1)^",
        "Invalid number of arguments to function 'BITAND'. Was expecting 2 arguments",
        false);
    f.checkFails("^bitand(1, 2, 3)^",
        "Invalid number of arguments to function 'BITAND'. Was expecting 2 arguments",
        false);
    f.checkNull("bitand(NULL, 1)");
    f.checkNull("bitand(1, NULL)");
    f.checkFails("^bitand(NULL, NULL)^",
        "At least one argument to function 'BITAND' must not be NULL",
        false);
    f.checkFails("bitand(CAST(x'0201' AS VARBINARY), CAST(x'02' AS VARBINARY))",
        "Different length for bitwise operands: the first: 2, the second: 1",
        true);
  }

  @Test void testBitOrScalarFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.BITOR, VmName.EXPAND);
    f.checkFails("bitor(^*^)", "Unknown identifier '\\*'", false);
    f.checkScalar("bitor(2, 4)", "6", "INTEGER NOT NULL");
    f.checkScalar("bitor(CAST(2 AS INTEGER), CAST(4 AS BIGINT))", "6", "BIGINT NOT NULL");
    f.checkScalar("bitor(-5, 7)", "-1", "INTEGER NOT NULL");
    f.checkScalar("bitor(-5, -31)", "-5", "INTEGER NOT NULL");
    f.checkScalar("bitor(CAST(-5 AS TINYINT), CAST(7 AS TINYINT))", "-1", "TINYINT NOT NULL");
    f.checkScalar("bitor(CAST(-5 AS TINYINT), CAST(-31 AS TINYINT))", "-5", "TINYINT NOT NULL");
    f.checkType("bitor(CAST(2 AS TINYINT), CAST(6 AS TINYINT))", "TINYINT NOT NULL");
    f.checkType("bitor(CAST(2 AS SMALLINT), CAST(6 AS SMALLINT))", "SMALLINT NOT NULL");
    f.checkType("bitor(CAST(2 AS BIGINT), CAST(6 AS BIGINT))", "BIGINT NOT NULL");
    f.checkScalar("bitor(CAST(x'0201' AS BINARY(2)), CAST(x'07f9' AS BINARY(2)))", "07f9",
        "BINARY(2) NOT NULL");
    f.checkScalar("bitor(CAST(x'0201' AS VARBINARY(2)), CAST(x'07f9' AS VARBINARY(2)))", "07f9",
        "VARBINARY(2) NOT NULL");
    f.checkFails("^bitor(1.2, 1.3)^",
        "Cannot apply 'BITOR' to arguments of type '"
            + "BITOR\\(<DECIMAL\\(2, 1\\)>, <DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): '"
            + "BITOR\\(<INTEGER>, <INTEGER>\\)'\n"
            + "'BITOR\\(<BINARY>, <BINARY>\\)'",
        false);
    f.checkFails("^bitor()^",
        "Invalid number of arguments to function 'BITOR'. Was expecting 2 arguments",
        false);
    f.checkFails("^bitor(1)^",
        "Invalid number of arguments to function 'BITOR'. Was expecting 2 arguments",
        false);
    f.checkFails("^bitor(1, 2, 3)^",
        "Invalid number of arguments to function 'BITOR'. Was expecting 2 arguments",
        false);
    f.checkNull("bitor(NULL, 1)");
    f.checkNull("bitor(1, NULL)");
    f.checkFails("^bitor(NULL, NULL)^",
        "At least one argument to function 'BITOR' must not be NULL",
        false);
    f.checkFails("bitor(CAST(x'0201' AS VARBINARY), CAST(x'02' AS VARBINARY))",
        "Different length for bitwise operands: the first: 2, the second: 1",
        true);
  }

  @Test void testBitXorScalarFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.BITXOR, VmName.EXPAND);
    f.checkFails("bitxor(^*^)", "Unknown identifier '\\*'", false);
    f.checkScalar("bitxor(2, 3)", "1", "INTEGER NOT NULL");
    f.checkScalar("bitxor(CAST(2 AS INTEGER), CAST(3 AS BIGINT))", "1", "BIGINT NOT NULL");
    f.checkScalar("bitxor(-5, 7)", "-4", "INTEGER NOT NULL");
    f.checkScalar("bitxor(-5, -31)", "26", "INTEGER NOT NULL");
    f.checkScalar("bitxor(CAST(-5 AS TINYINT), CAST(7 AS TINYINT))", "-4", "TINYINT NOT NULL");
    f.checkScalar("bitxor(CAST(-5 AS TINYINT), CAST(-31 AS TINYINT))", "26", "TINYINT NOT NULL");
    f.checkType("bitxor(CAST(2 AS TINYINT), CAST(6 AS TINYINT))", "TINYINT NOT NULL");
    f.checkType("bitxor(CAST(2 AS SMALLINT), CAST(6 AS SMALLINT))", "SMALLINT NOT NULL");
    f.checkType("bitxor(CAST(2 AS BIGINT), CAST(6 AS BIGINT))", "BIGINT NOT NULL");
    f.checkScalar("bitxor(CAST(x'0201' AS BINARY(2)), CAST(x'07f9' AS BINARY(2)))", "05f8",
        "BINARY(2) NOT NULL");
    f.checkScalar("bitxor(CAST(x'0201' AS VARBINARY(2)), CAST(x'07f9' AS VARBINARY(2)))", "05f8",
        "VARBINARY(2) NOT NULL");
    f.checkFails("^bitxor(1.2, 1.3)^",
        "Cannot apply 'BITXOR' to arguments of type '"
            + "BITXOR\\(<DECIMAL\\(2, 1\\)>, <DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): '"
            + "BITXOR\\(<INTEGER>, <INTEGER>\\)'\n"
            + "'BITXOR\\(<BINARY>, <BINARY>\\)'",
        false);
    f.checkFails("^bitxor()^",
        "Invalid number of arguments to function 'BITXOR'. Was expecting 2 arguments",
        false);
    f.checkFails("^bitxor(1)^",
        "Invalid number of arguments to function 'BITXOR'. Was expecting 2 arguments",
        false);
    f.checkFails("^bitxor(1, 2, 3)^",
        "Invalid number of arguments to function 'BITXOR'. Was expecting 2 arguments",
        false);
    f.checkNull("bitxor(NULL, 1)");
    f.checkNull("bitxor(1, NULL)");
    f.checkFails("^bitxor(NULL, NULL)^",
        "At least one argument to function 'BITXOR' must not be NULL",
        false);
    f.checkFails("bitxor(CAST(x'0201' AS VARBINARY), CAST(x'02' AS VARBINARY))",
        "Different length for bitwise operands: the first: 2, the second: 1",
        true);
  }

  /**
   * Test cases for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-3592">[CALCITE-3592]
   * Implement BITNOT scalar function</a>. */
  @Test void testBitNotScalarFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.BITNOT, VmName.EXPAND);
    f.checkFails("bitnot(^*^)", "Unknown identifier '\\*'", false);
    f.checkScalar("bitnot(2)", Integer.toString(~2), "INTEGER NOT NULL");
    f.checkScalar("bitnot(-5)", Integer.toString(~-5), "INTEGER NOT NULL");
    f.checkScalar("bitnot(CAST(-5 AS TINYINT))", Byte.toString((byte) ~-5), "TINYINT NOT NULL");
    f.checkScalar("bitnot(CAST(2 AS SMALLINT))", Short.toString((short) ~2), "SMALLINT NOT NULL");
    f.checkScalar("bitnot(CAST(2 AS INTEGER))", Integer.toString(~2), "INTEGER NOT NULL");
    f.checkScalar("bitnot(CAST(2 AS BIGINT))", Long.toString(~2), "BIGINT NOT NULL");
    f.checkFails("^bitnot(2.3)^",
        "Cannot apply 'BITNOT' to arguments of type '"
            + "BITNOT\\(<DECIMAL\\(2, 1\\)>\\)'. Supported form\\(s\\): '"
            + "BITNOT\\(<INTEGER>\\)'\n"
            + "'BITNOT\\(<BINARY>\\)'",
        false);
    f.checkScalar("bitnot(CAST(x'0201' AS BINARY(2)))", "fdfe",
        "BINARY(2) NOT NULL");
    f.checkScalar("bitnot(CAST(x'0201' AS VARBINARY(2)))", "fdfe",
        "VARBINARY(2) NOT NULL");
    f.checkFails("^bitnot()^",
        "Invalid number of arguments to function 'BITNOT'. Was expecting 1 arguments",
        false);
    f.checkFails("^bitnot(1, 2)^",
        "Invalid number of arguments to function 'BITNOT'. Was expecting 1 arguments",
        false);
    f.checkNull("bitnot(NULL)");
  }

  @Test void testBitAndAggFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.BITAND_AGG, VmName.EXPAND);
    checkBitAnd(f, FunctionAlias.of(SqlLibraryOperators.BITAND_AGG));
  }

  @Test void testBitAndFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.BIT_AND, VmName.EXPAND);
    checkBitAnd(f, FunctionAlias.of(SqlStdOperatorTable.BIT_AND));
  }

  /** Tests the {@code BIT_AND} and {@code BITAND_AGG} operators. */
  void checkBitAnd(SqlOperatorFixture f0, FunctionAlias functionAlias) {
    final SqlFunction function = functionAlias.function;
    final String fn = function.getName();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkFails(fn + "(^*^)", "Unknown identifier '\\*'", false);
      f.checkType(fn + "(1)", "INTEGER");
      f.checkType(fn + "(CAST(2 AS TINYINT))", "TINYINT");
      f.checkType(fn + "(CAST(2 AS SMALLINT))", "SMALLINT");
      f.checkType(fn + "(distinct CAST(2 AS BIGINT))", "BIGINT");
      f.checkType(fn + "(CAST(x'02' AS BINARY(1)))", "BINARY(1)");
      f.checkFails("^" + fn + "(1.2)^",
          "Cannot apply '" + fn + "' to arguments of type '"
          + fn + "\\(<DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): '"
          + fn + "\\(<INTEGER>\\)'\n"
          + "'" + fn + "\\(<BINARY>\\)'",
          false);
      f.checkFails("^" + fn + "()^",
          "Invalid number of arguments to function '" + fn + "'. Was expecting 1 arguments",
          false);
      f.checkFails("^" + fn + "(1, 2)^",
          "Invalid number of arguments to function '" + fn + "'. Was expecting 1 arguments",
          false);
      final String[] values = {"3", "2", "2"};
      f.checkAgg(fn + "(x)", values, isSingle("2"));
      final String[] binaryValues = {
          "CAST(x'03' AS BINARY)",
          "cast(x'02' as BINARY)",
          "cast(x'02' AS BINARY)",
          "cast(null AS BINARY)"};
      f.checkAgg(fn + "(x)", binaryValues, isSingle("02"));
      f.checkAgg(fn + "(x)", new String[]{"CAST(x'02' AS BINARY)"}, isSingle("02"));
      f.checkAggFails(fn + "(x)",
          new String[]{"CAST(x'0201' AS VARBINARY)", "CAST(x'02' AS VARBINARY)"},
          "Error while executing SQL .*"
              + " Different length for bitwise operands: the first: 2, the second: 1",
          true);
    };
    f0.forEachLibrary(list(functionAlias.libraries), consumer);
  }

  @Test void testBitCountFunc() {
    checkBitCount(SqlStdOperatorTable.BITCOUNT, null, false);
  }

  @Test void testBitCountBigQueryFunc() {
    checkBitCount(SqlLibraryOperators.BIT_COUNT_BIG_QUERY,
        list(SqlLibrary.BIG_QUERY, SqlLibrary.SPARK), false);
  }

  @Test void testBitCountMySQLFunc() {
    checkBitCount(SqlLibraryOperators.BIT_COUNT_MYSQL, list(SqlLibrary.MYSQL), true);
    checkBitCountMySQL();
  }

  /**
   * Test cases for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6700">[CALCITE-6700]
   * MySQL BIT_COUNT function should return result when parameter
   * is Boolean, String, Date, Time and Timestamp types</a>. */
  private void checkBitCountMySQL() {
    final SqlOperatorFixture f = fixture()
        .setFor(SqlLibraryOperators.BIT_COUNT_MYSQL, VmName.EXPAND)
        .withLibrary(SqlLibrary.MYSQL);

    f.checkScalar("BIT_COUNT(8)", "1", "BIGINT NOT NULL");
    f.checkScalar("BIT_COUNT('8')", "1", "BIGINT NOT NULL");
    f.checkScalar("BIT_COUNT('10')", "2", "BIGINT NOT NULL");
    f.checkScalar("BIT_COUNT('a')", "0", "BIGINT NOT NULL");
    f.checkScalar("BIT_COUNT('')", "0", "BIGINT NOT NULL");
    f.checkScalar("BIT_COUNT(1 + 1)", "1", "BIGINT NOT NULL");
    f.checkScalar("BIT_COUNT(true)", "1", "BIGINT NOT NULL");
    f.checkScalar("BIT_COUNT(false)", "0", "BIGINT NOT NULL");
    f.checkScalar("BIT_COUNT(DATE '1996-08-03')", "12", "BIGINT NOT NULL");
    f.checkScalar("BIT_COUNT(TIME '16:22:34')", "11", "BIGINT NOT NULL");
    f.checkScalar("BIT_COUNT(TIMESTAMP '1996-08-03 16:22:34')", "24", "BIGINT NOT NULL");
  }

  void checkBitCount(SqlFunction function, @Nullable  List<SqlLibrary> libraries,
      boolean testDecimal) {
    final SqlOperatorFixture f0 = fixture();
    f0.setFor(function, VmName.EXPAND);
    final String functionName = function.getName();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkFails(functionName + "(^*^)", "Unknown identifier '\\*'", false);
      f.checkType(functionName + "(1)", "BIGINT NOT NULL");
      f.checkType(functionName + "(CAST(2 AS TINYINT))", "BIGINT NOT NULL");
      f.checkType(functionName + "(CAST(2 AS SMALLINT))", "BIGINT NOT NULL");
      f.checkFails(
          "^" + functionName + "()^",
          "Invalid number of arguments to function '" + functionName
              + "'. Was expecting 1 arguments",
          false);
      f.checkFails(
          "^" + functionName + "(1, 2)^",
          "Invalid number of arguments to function '" + functionName
              + "'. Was expecting 1 arguments",
          false);
      f.checkScalar(functionName + "(8)", "1", "BIGINT NOT NULL");
      f.checkScalar(functionName + "(CAST(x'ad' AS BINARY(1)))", "5", "BIGINT NOT NULL");
      f.checkScalar(functionName + "(CAST(x'ad' AS VARBINARY(1)))", "5", "BIGINT NOT NULL");
      f.checkScalar(functionName + "(-1)", "64", "BIGINT NOT NULL");
      f.checkNull(functionName + "(cast(NULL as TINYINT))");
      f.checkNull(functionName + "(cast(NULL as BINARY))");
      f.checkNull(functionName + "(NULL)");
      if (testDecimal) {
        f.checkType(functionName + "(CAST(2 AS DOUBLE))", "BIGINT NOT NULL");
        // Verify that only bits in the integer portion of a decimal value are counted
        f.checkScalar(functionName + "(5.23)", "2", "BIGINT NOT NULL");
        f.checkScalar(functionName + "(CAST('-9223372036854775808' AS DECIMAL(19, 0)))", "1",
            "BIGINT NOT NULL");
        f.checkScalar(functionName + "(CAST('-9223372036854775809' AS DECIMAL(19, 0)))", "1",
            "BIGINT NOT NULL");
      } else {
        f.checkType(functionName + "(CAST(x'ad' AS BINARY(1)))", "BIGINT NOT NULL");
        f.checkFails("^" + functionName + "(1.2)^",
            "Cannot apply '" + functionName + "' to arguments of type '" + functionName
                + "\\(<DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): '" + functionName
                + "\\(<INTEGER>\\)'\n"
                + "'" + functionName + "\\(<BINARY>\\)'",
            false);
      }
    };
    if (libraries == null) {
      consumer.accept(f0);
    } else {
      f0.forEachLibrary(libraries, consumer);
    }
  }

  @Test void testBitOrAggFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlLibraryOperators.BITOR_AGG, VmName.EXPAND);
    checkBitOr(f, FunctionAlias.of(SqlLibraryOperators.BITOR_AGG));
  }

  @Test void testBitOrFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.BIT_OR, VmName.EXPAND);
    checkBitOr(f, FunctionAlias.of(SqlStdOperatorTable.BIT_OR));
  }

  /** Tests the {@code BIT_OR} and {@code BITOR_AGG} operators. */
  void checkBitOr(SqlOperatorFixture f0, FunctionAlias functionAlias) {
    final SqlFunction function = functionAlias.function;
    final String fn = function.getName();
    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkFails(fn + "(^*^)", "Unknown identifier '\\*'", false);
      f.checkType(fn + "(1)", "INTEGER");
      f.checkType(fn + "(CAST(2 AS TINYINT))", "TINYINT");
      f.checkType(fn + "(CAST(2 AS SMALLINT))", "SMALLINT");
      f.checkType(fn + "(distinct CAST(2 AS BIGINT))", "BIGINT");
      f.checkType(fn + "(CAST(x'02' AS BINARY(1)))", "BINARY(1)");
      f.checkFails("^" + fn + "(1.2)^",
          "Cannot apply '" + fn + "' to arguments of type "
              + "'" + fn + "\\(<DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): "
              + "'" + fn + "\\(<INTEGER>\\)'\n"
              + "'" + fn + "\\(<BINARY>\\)'",
          false);
      f.checkFails("^" + fn + "()^",
          "Invalid number of arguments to function '" + fn + "'. Was expecting 1 arguments",
          false);
      f.checkFails("^" + fn + "(1, 2)^",
          "Invalid number of arguments to function '" + fn + "'. Was expecting 1 arguments",
          false);
      final String[] values = {"1", "2", "2"};
      f.checkAgg("bit_or(x)", values, isSingle(3));
      final String[] binaryValues = {
          "CAST(x'01' AS BINARY)",
          "cast(x'02' as BINARY)",
          "cast(x'02' AS BINARY)",
          "cast(null AS BINARY)"};
      f.checkAgg(fn + "(x)", binaryValues, isSingle("03"));
      f.checkAgg(fn + "(x)", new String[]{"CAST(x'02' AS BINARY)"},
          isSingle("02"));
    };
    f0.forEachLibrary(list(functionAlias.libraries), consumer);
  }

  @Test void testBitXorFunc() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.BIT_XOR, VM_JAVA);
    f.checkFails("bit_xor(^*^)", "Unknown identifier '\\*'", false);
    f.checkType("bit_xor(1)", "INTEGER");
    f.checkType("bit_xor(CAST(2 AS TINYINT))", "TINYINT");
    f.checkType("bit_xor(CAST(2 AS SMALLINT))", "SMALLINT");
    f.checkType("bit_xor(distinct CAST(2 AS BIGINT))", "BIGINT");
    f.checkType("bit_xor(CAST(x'02' AS BINARY(1)))", "BINARY(1)");
    f.checkFails("^bit_xor(1.2)^",
        "Cannot apply 'BIT_XOR' to arguments of type "
            + "'BIT_XOR\\(<DECIMAL\\(2, 1\\)>\\)'\\. Supported form\\(s\\): "
            + "'BIT_XOR\\(<INTEGER>\\)'\n"
            + "'BIT_XOR\\(<BINARY>\\)'",
        false);
    f.checkFails("^bit_xor()^",
        "Invalid number of arguments to function 'BIT_XOR'. Was expecting 1 arguments",
        false);
    f.checkFails("^bit_xor(1, 2)^",
        "Invalid number of arguments to function 'BIT_XOR'. Was expecting 1 arguments",
        false);
    final String[] values = {"1", "2", "1"};
    f.checkAgg("bit_xor(x)", values, isSingle(2));
    final String[] binaryValues = {
        "CAST(x'01' AS BINARY)",
        "cast(x'02' as BINARY)",
        "cast(x'01' AS BINARY)",
        "cast(null AS BINARY)"};
    f.checkAgg("bit_xor(x)", binaryValues, isSingle("02"));
    f.checkAgg("bit_xor(x)", new String[]{"CAST(x'02' AS BINARY)"},
        isSingle("02"));
    f.checkAgg("bit_xor(distinct(x))",
        new String[]{"CAST(x'02' AS BINARY)", "CAST(x'02' AS BINARY)"},
        isSingle("02"));
  }

  @Test void testArgMin() {
    final SqlOperatorFixture f0 = fixture();
    final String[] xValues = {"2", "3", "4", "4", "5", "7"};

    final Consumer<SqlOperatorFixture> consumer = f -> {
      f.checkAgg("arg_min(mod(x, 3), x)", xValues, isSingle("2"));
      f.checkAgg("arg_max(mod(x, 3), x)", xValues, isSingle("1"));
    };

    final Consumer<SqlOperatorFixture> consumer2 = f -> {
      f.checkAgg("min_by(mod(x, 3), x)", xValues, isSingle("2"));
      f.checkAgg("max_by(mod(x, 3), x)", xValues, isSingle("1"));
    };

    consumer.accept(f0);
    consumer2.accept(f0.withLibrary(SqlLibrary.SPARK));
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
  @Test void testLiteralAtLimit() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);
    final List<RelDataType> types =
        SqlTests.getTypes(f.getFactory().getTypeFactory());
    for (RelDataType type : types) {
      for (Object o : getValues((BasicSqlType) type, true)) {
        SqlLiteral literal =
            type.getSqlTypeName().createLiteral(o, SqlParserPos.ZERO);
        SqlString literalString =
            literal.toSqlString(AnsiSqlDialect.DEFAULT);
        final String expr = "CAST(" + literalString + " AS " + type + ")";
        try {
          f.checkType(expr, type.getFullTypeString());

          if (type.getSqlTypeName() == SqlTypeName.BINARY) {
            // Casting a string/binary values may change the value.
            // For example, CAST(X'AB' AS BINARY(2)) yields
            // X'AB00'.
          } else {
            f.checkScalar(expr + " = " + literalString,
                true, "BOOLEAN NOT NULL");
          }
        } catch (Error | RuntimeException e) {
          throw new RuntimeException("Failed for expr=[" + expr + "]", e);
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
  @Test void testLiteralBeyondLimit() {
    final SqlOperatorFixture f = fixture();
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);
    final List<RelDataType> types =
        SqlTests.getTypes(f.getFactory().getTypeFactory());
    for (RelDataType type : types) {
      SqlTypeName sqlTypeName = type.getSqlTypeName();
      for (Object o : getValues((BasicSqlType) type, false)) {
        SqlLiteral literal =
            sqlTypeName.createLiteral(o, SqlParserPos.ZERO);
        SqlString literalString =
            literal.toSqlString(AnsiSqlDialect.DEFAULT);

        if (sqlTypeName == SqlTypeName.DECIMAL) {
          // Casting to decimal does not fail
        } else if (sqlTypeName == SqlTypeName.BIGINT) {
          // This is in fact a cast of a decimal literal to BIGINT,
          // so the error is different.
          f.checkFails("CAST(" + literalString + " AS " + type + ")",
              "Overflow",
              true);
        } else if ((sqlTypeName == SqlTypeName.CHAR)
            || (sqlTypeName == SqlTypeName.VARCHAR)
            || (sqlTypeName == SqlTypeName.BINARY)
            || (sqlTypeName == SqlTypeName.VARBINARY)) {
          // Casting overlarge string/binary values do not fail -
          // they are truncated. See testCastTruncates().
        } else {
          // Value outside legal bound should fail at runtime (not
          // validate time).
          String expected;
          if (type.getSqlTypeName() == SqlTypeName.DECIMAL) {
            expected = "Value .* cannot be represented as .*";
          } else {
            expected = "Value .* out of range";
          }
          f.checkFails("CAST(" + literalString + " AS " + type + ")", expected, true);
        }
      }
    }
  }

  /**
   * Test cases for
   * <a href="https://issues.apache.org/jira/browse/CALCITE-6111">[CALCITE-6111]
   * Explicit cast from expression to numeric type doesn't check overflow</a>. */
  @Test public void testOverflow() {
    final SqlOperatorFixture f = fixture();
    f.checkFails(String.format(Locale.US, "SELECT cast(%d+30 as tinyint)", Byte.MAX_VALUE),
        OUT_OF_RANGE_MESSAGE, true);
    f.checkFails(String.format(Locale.US, "SELECT cast(%d+30 as smallint)", Short.MAX_VALUE),
        OUT_OF_RANGE_MESSAGE, true);
    // We use a long value because otherwise calcite interprets the literal as an integer.
    f.checkFails(String.format(Locale.US, "SELECT cast(%d as int)", Long.MAX_VALUE),
        OUT_OF_RANGE_MESSAGE, true);

    // Casting a floating point value larger than the maximum allowed value.
    // 1e60 is larger than the largest BIGINT value allowed.
    f.checkFails("SELECT cast(1e60+30 as tinyint)",
        OUT_OF_RANGE_MESSAGE, true);
    f.checkFails("SELECT cast(1e60+30 as smallint)",
        OUT_OF_RANGE_MESSAGE, true);
    f.checkFails("SELECT cast(1e60+30 as int)",
        OUT_OF_RANGE_MESSAGE, true);
    f.checkFails("SELECT cast(1e60+30 as bigint)",
        ".*Overflow", true);

    // Casting a decimal value larger than the maximum allowed value.
    // Concatenating .0 to a value makes it decimal.
    f.checkFails(String.format(Locale.US, "SELECT cast(%d.0 AS tinyint)", Short.MAX_VALUE),
        OUT_OF_RANGE_MESSAGE, true);
    f.checkFails(String.format(Locale.US, "SELECT cast(%d.0 AS smallint)", Integer.MAX_VALUE),
        OUT_OF_RANGE_MESSAGE, true);
    // Dividing Long.MAX_VALUE by 10 ensures that the resulting decimal does not exceed the
    // maximum allowed precision for decimals but is still too large for an integer.
    f.checkFails(String.format(Locale.US, "SELECT cast(%d.0 AS int)", Long.MAX_VALUE / 10),
        OUT_OF_RANGE_MESSAGE, true);
  }

  @ParameterizedTest
  @MethodSource("safeParameters")
  void testCastTruncates(CastType castType, SqlOperatorFixture f) {
    f.setFor(SqlStdOperatorTable.CAST, VmName.EXPAND);
    f.checkScalar("CAST('ABCD' AS CHAR(2))", "AB", "CHAR(2) NOT NULL");
    f.checkScalar("CAST('ABCD' AS VARCHAR(2))", "AB",
        "VARCHAR(2) NOT NULL");
    f.checkScalar("CAST('ABCD' AS VARCHAR)", "ABCD", "VARCHAR NOT NULL");
    f.checkScalar("CAST(CAST('ABCD' AS VARCHAR) AS VARCHAR(3))", "ABC",
        "VARCHAR(3) NOT NULL");

    f.checkScalar("CAST(x'ABCDEF12' AS BINARY(2))", "abcd",
        "BINARY(2) NOT NULL");
    f.checkScalar("CAST(x'ABCDEF12' AS VARBINARY(2))", "abcd",
        "VARBINARY(2) NOT NULL");
    f.checkScalar("CAST(x'ABCDEF12' AS VARBINARY)", "abcdef12",
        "VARBINARY NOT NULL");
    f.checkScalar("CAST(CAST(x'ABCDEF12' AS VARBINARY) AS VARBINARY(3))",
        "abcdef", "VARBINARY(3) NOT NULL");

    if (!f.brokenTestsEnabled()) {
      return;
    }
    f.checkBoolean("CAST(X'' AS BINARY(3)) = X'000000'", true);
    f.checkBoolean("CAST(X'' AS BINARY(3)) = X''", false);
  }

  /** Test that calls all operators with all possible argument types, and for
   * each type, with a set of tricky values.
   *
   * <p>This is not really a unit test since there are no assertions;
   * it either succeeds or fails in the preparation of the operator case
   * and not when actually testing (validating/executing) the call.
   *
   * <p>Nevertheless the log messages conceal many problems which potentially
   * need to be fixed especially cases where the query passes from the
   * validation stage and fails at runtime. */
  @Disabled("Too slow and not really a unit test")
  @Tag("slow")
  @Test void testArgumentBounds() {
    final SqlOperatorFixture f = fixture();
    final SqlValidatorImpl validator =
        (SqlValidatorImpl) f.getFactory().createValidator();
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

    Set<SqlOperator> operatorsToSkip = new HashSet<>();
    if (!Bug.CALCITE_3243_FIXED) {
      // TODO: Remove entirely the if block when the bug is fixed
      // REVIEW zabetak 12-August-2019: It may still make sense to avoid the
      // JSON functions since for most of the values above they are expected
      // to raise an error and due to the big number of operands they accept
      // they increase significantly the running time of the method.
      operatorsToSkip.add(SqlStdOperatorTable.JSON_VALUE);
      operatorsToSkip.add(SqlStdOperatorTable.JSON_QUERY);
    }
    // Skip since ClassCastException is raised in SqlOperator#unparse
    // since the operands of the call do not have the expected type.
    // Moreover, the values above do not make much sense for this operator.
    operatorsToSkip.add(SqlStdOperatorTable.WITHIN_GROUP);
    operatorsToSkip.add(SqlStdOperatorTable.TRIM); // can't handle the flag argument
    operatorsToSkip.add(SqlStdOperatorTable.EXISTS);
    for (SqlOperator op : SqlStdOperatorTable.instance().getOperatorList()) {
      if (operatorsToSkip.contains(op)) {
        continue;
      }
      if (op.getSyntax() == SqlSyntax.SPECIAL) {
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
          final SqlPrettyWriter writer = new SqlPrettyWriter();
          op.unparse(writer, call, 0, 0);
          final String s = writer.toSqlString().toString();
          if (s.startsWith("OVERLAY(")
              || s.contains(" / 0")
              || s.matches("MOD\\(.*, 0\\)")) {
            continue;
          }
          final Strong.Policy policy = Strong.policy(op);
          try {
            if (nullCount > 0 && policy == Strong.Policy.ANY) {
              f.checkNull(s);
            } else {
              final String query;
              if (op instanceof SqlAggFunction) {
                if (op.requiresOrder()) {
                  query = "SELECT " + s + " OVER () FROM (VALUES (1))";
                } else {
                  query = "SELECT " + s + " FROM (VALUES (1))";
                }
              } else {
                StringAndPos sap = StringAndPos.of(s);
                query = AbstractSqlTester.buildQuery(sap);
              }
              f.check(query, SqlTests.ANY_TYPE_CHECKER,
                  SqlTests.ANY_PARAMETER_CHECKER, (sql, result) -> { });
            }
          } catch (Throwable e) {
            // Logging the top-level throwable directly makes the message
            // difficult to read since it either contains too much information
            // or very few details.
            Throwable cause = findMostDescriptiveCause(e);
            LOGGER.info("Failed: " + s + ": " + cause);
          }
        }
      }
    }
  }

  private Throwable findMostDescriptiveCause(Throwable ex) {
    if (ex instanceof CalciteException
        || ex instanceof CalciteContextException
        || ex instanceof SqlParseException) {
      return ex;
    }
    Throwable cause = ex.getCause();
    if (cause != null) {
      return findMostDescriptiveCause(cause);
    }
    return ex;
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

    @Override public void checkResult(String sql, ResultSet result) {
      Throwable thrown = null;
      try {
        if (!result.next()) {
          // empty result is OK
          return;
        }
        final Object actual = result.getObject(1);
        assertThat("Query: " + sql, actual, is(expected));
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

  /**
   * Implementation of {@link org.apache.calcite.sql.test.SqlTester} based on a
   * JDBC connection.
   */
  protected static class TesterImpl extends SqlRuntimeTester {
    /** Assign a type system object to this thread-local, pass the name of the
     * field as the {@link CalciteConnectionProperty#TYPE_SYSTEM} property,
     * and any connection you make in the same thread will use your type
     * system. */
    public static final TryThreadLocal<RelDataTypeSystem> THREAD_TYPE_SYSTEM =
        TryThreadLocal.of(RelDataTypeSystem.DEFAULT);

    private static final Field FIELD =
        Types.lookupField(TesterImpl.class, "THREAD_TYPE_SYSTEM");

    private static String uri(Field field) {
      return field.getDeclaringClass().getName() + '#' + field.getName();
    }

    public TesterImpl() {
    }

    @Override public void check(SqlTestFactory factory, String queryWithCarets,
        SqlTester.TypeChecker typeChecker,
        SqlTester.ParameterChecker parameterChecker,
        SqlTester.ResultChecker resultChecker) {
      super.check(factory, queryWithCarets, typeChecker, parameterChecker, resultChecker);
      final RelDataTypeSystem typeSystem =
          factory.typeSystemTransform.apply(RelDataTypeSystem.DEFAULT);
      final ConnectionFactory connectionFactory =
          factory.connectionFactory
              .with(CalciteConnectionProperty.TYPE_SYSTEM, uri(FIELD));
      try (TryThreadLocal.Memo ignore = THREAD_TYPE_SYSTEM.push(typeSystem);
           Connection connection = connectionFactory.createConnection();
           Statement statement = connection.createStatement()) {
        final StringAndPos sap = StringAndPos.of(queryWithCarets);
        final ResultSet resultSet =
            statement.executeQuery(sap.sql);
        resultChecker.checkResult(queryWithCarets, resultSet);
      } catch (Exception e) {
        throw TestUtil.rethrow(e);
      }
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
        return SqlLiteral.createTimestamp(type.getSqlTypeName(), ts,
            type.getPrecision(), SqlParserPos.ZERO);
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
  static class OverlapChecker {
    final SqlOperatorFixture f;
    final String[] values;

    OverlapChecker(SqlOperatorFixture f, String... values) {
      this.f = f;
      this.values = values;
    }

    public void isTrue(String s) {
      f.checkBoolean(sub(s), true);
    }

    public void isFalse(String s) {
      f.checkBoolean(sub(s), false);
    }

    private String sub(String s) {
      return s.replace("$0", values[0])
          .replace("$1", values[1])
          .replace("$2", values[2])
          .replace("$3", values[3]);
    }
  }

  /** Contains alias data to re-use tests for aliased operators. */
  static class FunctionAlias {
    final SqlFunction function;
    final List<SqlLibrary> libraries;

    private FunctionAlias(SqlFunction function, List<SqlLibrary> libraries) {
      this.function = function;
      this.libraries = libraries;
    }

    static FunctionAlias of(SqlFunction function) {
      Field field =
          lookupField(function.getName(), SqlStdOperatorTable.class,
              SqlLibraryOperators.class);
      List<SqlLibrary> libraries = new ArrayList<SqlLibrary>();
      LibraryOperator libraryOperator = field.getAnnotation(LibraryOperator.class);
      if (libraryOperator == null) {
        libraries.add(SqlLibrary.STANDARD);
      } else {
        libraries.addAll(Arrays.asList(libraryOperator.libraries()));
      }
      return new FunctionAlias(function, libraries);
    }

    @SuppressWarnings("rawtypes")
    static Field lookupField(String name, Class... classes) {
      for (Class aClass : classes) {
        try {
          return aClass.getField(name);
        } catch (NoSuchFieldException e) {
          // ignore, and try the next class
        }
      }
      throw new AssertionError("field " + name
          + " not found in classes" + Arrays.toString(classes));
    }

    // Used for test naming while running tests in IDE
    @Override public String toString() {
      return "function=" + function
          + ", libraries=" + libraries;
    }
  }
}
