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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.test.SqlTester.ResultChecker;
import org.apache.calcite.sql.test.SqlTester.TypeChecker;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.ConnectionFactories;
import org.apache.calcite.test.ConnectionFactory;
import org.apache.calcite.test.Matchers;
import org.apache.calcite.util.Bug;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.apache.calcite.rel.type.RelDataTypeImpl.NON_NULLABLE_SUFFIX;
import static org.apache.calcite.sql.test.ResultCheckers.isSingle;

/**
 * A fixture for testing the SQL operators.
 *
 * <p>It provides a fluent API so that you can write tests by chaining method
 * calls.
 *
 * <p>It is immutable. If you have two test cases that require a similar set up
 * (for example, the same SQL expression and parser configuration), it is safe
 * to use the same fixture object as a starting point for both tests.
 *
 * <p>The idea is that when you define an operator (or another piece of SQL
 * functionality), you can define the logical behavior of that operator once, as
 * part of that operator. Later you can define one or more physical
 * implementations of that operator, and test them all using the same set of
 * tests.
 *
 * <p>Depending on the implementation of {@link SqlTester} used
 * (see {@link #withTester(UnaryOperator)}), the fixture may or may not
 * evaluate expressions and check their results.
 */
public interface SqlOperatorFixture extends AutoCloseable {
  //~ Enums ------------------------------------------------------------------

  // TODO: Change message
  String INVALID_CHAR_MESSAGE = "(?s).*";

  String OUT_OF_RANGE_MESSAGE = ".* out of range.*";

  String INTEGER_OVERFLOW = "integer overflow.*";

  String LONG_OVERFLOW = "long overflow.*";

  String DECIMAL_OVERFLOW = ".*cannot be represented as a DECIMAL.*";

  String WRONG_FORMAT_MESSAGE = "Number has wrong format.*";

  // TODO: Change message
  String DIVISION_BY_ZERO_MESSAGE = "(?s).*";

  // TODO: Change message
  String STRING_TRUNC_MESSAGE = "(?s).*";

  // TODO: Change message
  String BAD_DATETIME_MESSAGE = "(?s).*";

  String LITERAL_OUT_OF_RANGE_MESSAGE =
      "(?s).*Numeric literal.*out of range.*";

  String INVALID_ARGUMENTS_NUMBER =
      "Invalid number of arguments to function .* Was expecting .* arguments";

  String INVALID_ARGUMENTS_TYPE_VALIDATION_ERROR =
      "Cannot apply '.*' to arguments of type .*";

  //~ Enums ------------------------------------------------------------------

  /**
   * Name of a virtual machine that can potentially implement an operator.
   */
  enum VmName {
    JAVA, EXPAND
  }

  //~ Methods ----------------------------------------------------------------

  /** Returns the test factory. */
  SqlTestFactory getFactory();

  /** Creates a copy of this fixture with a new test factory. */
  SqlOperatorFixture withFactory(UnaryOperator<SqlTestFactory> transform);

  /** Returns the tester. */
  SqlTester getTester();

  /** Creates a copy of this fixture with a new tester. */
  SqlOperatorFixture withTester(UnaryOperator<SqlTester> transform);

  /** Creates a copy of this fixture with a new parser configuration. */
  default SqlOperatorFixture withParserConfig(
      UnaryOperator<SqlParser.Config> transform) {
    return withFactory(f -> f.withParserConfig(transform));
  }

  /** Returns a fixture that tests a given SQL quoting style. */
  default SqlOperatorFixture withQuoting(Quoting quoting) {
    return withParserConfig(c -> c.withQuoting(quoting));
  }

  /** Returns a fixture that applies a given casing policy to quoted
   * identifiers. */
  default SqlOperatorFixture withQuotedCasing(Casing casing) {
    return withParserConfig(c -> c.withQuotedCasing(casing));
  }

  /** Returns a fixture that applies a given casing policy to unquoted
   * identifiers. */
  default SqlOperatorFixture withUnquotedCasing(Casing casing) {
    return withParserConfig(c -> c.withUnquotedCasing(casing));
  }

  /** Returns a fixture that matches identifiers by case-sensitive or
   * case-insensitive. */
  default SqlOperatorFixture withCaseSensitive(boolean sensitive) {
    return withParserConfig(c -> c.withCaseSensitive(sensitive));
  }

  /** Returns a fixture that follows a given lexical policy. */
  default SqlOperatorFixture withLex(Lex lex) {
    return withParserConfig(c -> c.withLex(lex));
  }

  /** Returns a fixture that tests conformance to a particular SQL language
   * version. */
  default SqlOperatorFixture withConformance(SqlConformance conformance) {
    return withParserConfig(c -> c.withConformance(conformance))
        .withValidatorConfig(c -> c.withConformance(conformance))
        .withConnectionFactory(cf -> cf.with("conformance", conformance));
  }

  /** Returns the conformance. */
  default SqlConformance conformance() {
    return getFactory().parserConfig().conformance();
  }

  /** Returns a fixture with a given validator configuration. */
  default SqlOperatorFixture withValidatorConfig(
      UnaryOperator<SqlValidator.Config> transform) {
    return withFactory(f -> f.withValidatorConfig(transform));
  }

  /** Returns a fixture that tests with implicit type coercion on/off. */
  default SqlOperatorFixture enableTypeCoercion(boolean enabled) {
    return withValidatorConfig(c -> c.withTypeCoercionEnabled(enabled));
  }

  /** Returns a fixture that does not fail validation if it encounters an
   * unknown function. */
  default SqlOperatorFixture withLenientOperatorLookup(boolean lenient) {
    return withValidatorConfig(c -> c.withLenientOperatorLookup(lenient));
  }

  /** Returns a fixture that gets connections from a given factory. */
  default SqlOperatorFixture withConnectionFactory(
      UnaryOperator<ConnectionFactory> transform) {
    return withFactory(f -> f.withConnectionFactory(transform));
  }

  /** Returns a fixture that uses a given operator table. */
  default SqlOperatorFixture withOperatorTable(
      SqlOperatorTable operatorTable) {
    return withFactory(f -> f.withOperatorTable(o -> operatorTable));
  }

  /** Returns whether to run tests that are considered 'broken'.
   * Returns false by default, but it is useful to temporarily enable the
   * 'broken' tests to see whether they are still broken. */
  boolean brokenTestsEnabled();

  /** Sets {@link #brokenTestsEnabled()}. */
  SqlOperatorFixture withBrokenTestsEnabled(boolean enableBrokenTests);

  void checkScalar(String expression,
      TypeChecker typeChecker,
      ResultChecker resultChecker);

  /**
   * Tests that a scalar SQL expression returns the expected result and the
   * expected type. For example,
   *
   * <blockquote>
   * <pre>checkScalar("1.1 + 2.9", "4.0", "DECIMAL(2, 1) NOT NULL");</pre>
   * </blockquote>
   *
   * @param expression Scalar expression
   * @param result     Expected result
   * @param resultType Expected result type
   */
  default void checkScalar(
      String expression,
      Object result,
      String resultType) {
    checkType(expression, resultType);
    checkScalar(expression, SqlTests.ANY_TYPE_CHECKER,
        ResultCheckers.createChecker(result));
  }

  /**
   * Tests that a scalar SQL expression returns the expected exact numeric
   * result as an integer. For example,
   *
   * <blockquote>
   * <pre>checkScalarExact("1 + 2", 3);</pre>
   * </blockquote>
   *
   * @param expression Scalar expression
   * @param result     Expected result
   */
  default void checkScalarExact(String expression, int result) {
    checkScalar(expression, SqlTests.INTEGER_TYPE_CHECKER, isSingle(result));
  }

  /**
   * Tests that a scalar SQL expression returns the expected exact numeric
   * result. For example,
   *
   * <blockquote>
   * <pre>checkScalarExact("1 + 2", "3");</pre>
   * </blockquote>
   *
   * @param expression   Scalar expression
   * @param expectedType Type we expect the result to have, including
   *                     nullability, precision and scale, for example
   *                     <code>DECIMAL(2, 1) NOT NULL</code>.
   * @param result       Expected result
   */
  default void checkScalarExact(
      String expression,
      String expectedType,
      String result) {
    checkScalarExact(expression, expectedType, isSingle(result));
  }

  void checkScalarExact(
      String expression,
      String expectedType,
      ResultChecker resultChecker);

  /**
   * Tests that a scalar SQL expression returns expected approximate numeric
   * result. For example,
   *
   * <blockquote>
   * <pre>checkScalarApprox("1.0 + 2.1", "3.1");</pre>
   * </blockquote>
   *
   * @param expression     Scalar expression
   * @param expectedType   Type we expect the result to have, including
   *                       nullability, precision and scale, for example
   *                       <code>DECIMAL(2, 1) NOT NULL</code>.
   * @param result         Expected result, or a matcher
   *
   * @see Matchers#within(Number, double)
   */
  void checkScalarApprox(
      String expression,
      String expectedType,
      Object result);

  /**
   * Tests that a scalar SQL expression returns the expected boolean result.
   * For example,
   *
   * <blockquote>
   * <pre>checkScalarExact("TRUE AND FALSE", Boolean.TRUE);</pre>
   * </blockquote>
   *
   * <p>The expected result can be null:
   *
   * <blockquote>
   * <pre>checkScalarExact("NOT UNKNOWN", null);</pre>
   * </blockquote>
   *
   * @param expression Scalar expression
   * @param result     Expected result (null signifies NULL).
   */
  void checkBoolean(
      String expression,
      @Nullable Boolean result);

  /**
   * Tests that a scalar SQL expression returns the expected string result.
   * For example,
   *
   * <blockquote>
   * <pre>checkScalarExact("'ab' || 'c'", "abc");</pre>
   * </blockquote>
   *
   * @param expression Scalar expression
   * @param result     Expected result
   * @param resultType Expected result type
   */
  void checkString(
      String expression,
      String result,
      String resultType);

  /**
   * Tests that a SQL expression returns the SQL NULL value. For example,
   *
   * <blockquote>
   * <pre>checkNull("CHAR_LENGTH(CAST(NULL AS VARCHAR(3))");</pre>
   * </blockquote>
   *
   * @param expression Scalar expression
   */
  void checkNull(String expression);

  /**
   * Tests that a SQL expression has a given type. For example,
   *
   * <blockquote>
   * <code>checkType("SUBSTR('hello' FROM 1 FOR 3)",
   * "VARCHAR(3) NOT NULL");</code>
   * </blockquote>
   *
   * <p>This method checks length/precision, scale, and whether the type allows
   * NULL values, so is more precise than the type-checking done by methods
   * such as {@link #checkScalarExact}.
   *
   * @param expression Scalar expression
   * @param type       Type string
   */
  void checkType(
      String expression,
      String type);

  /** Very similar to {@link #checkType}, but generates inside a SELECT
   * with a non-empty GROUP BY. Aggregate functions may be nullable if executed
   * in a SELECT with an empty GROUP BY.
   *
   * <p>Viz: {@code SELECT sum(1) FROM emp} has type "INTEGER",
   * {@code SELECT sum(1) FROM emp GROUP BY deptno} has type "INTEGER NOT NULL",
   */
  default SqlOperatorFixture checkAggType(String expr, String type) {
    checkColumnType(AbstractSqlTester.buildQueryAgg(expr), type);
    return this;
  }

  /**
   * Checks that a query returns one column of an expected type. For example,
   * <code>checkType("VALUES (1 + 2)", "INTEGER NOT NULL")</code>.
   *
   * @param sql  Query expression
   * @param type Type string
   */
  void checkColumnType(
      String sql,
      String type);

  /**
   * Tests that a SQL query returns a single column with the given type. For
   * example,
   *
   * <blockquote>
   * <pre>check("VALUES (1 + 2)", "3", SqlTypeName.Integer);</pre>
   * </blockquote>
   *
   * <p>If <code>result</code> is null, the expression must yield the SQL NULL
   * value. If <code>result</code> is a {@link java.util.regex.Pattern}, the
   * result must match that pattern.
   *
   * @param query       SQL query
   * @param typeChecker Checks whether the result is the expected type; must
   *                    not be null
   * @param result      Expected result, or matcher
   */
  default void check(String query,
      TypeChecker typeChecker,
      Object result) {
    check(query, typeChecker, SqlTests.ANY_PARAMETER_CHECKER,
        ResultCheckers.createChecker(result));
  }

  default void check(String query, String expectedType, Object result) {
    check(query, new SqlTests.StringTypeChecker(expectedType), result);
  }

  /**
   * Tests that a SQL query returns a result of expected type and value.
   * Checking of type and value are abstracted using {@link TypeChecker}
   * and {@link ResultChecker} functors.
   *
   * @param query         SQL query
   * @param typeChecker   Checks whether the result is the expected type
   * @param parameterChecker Checks whether the parameters are of expected
   *                      types
   * @param resultChecker Checks whether the result has the expected value
   */
  default void check(String query,
      SqlTester.TypeChecker typeChecker,
      SqlTester.ParameterChecker parameterChecker,
      ResultChecker resultChecker) {
    getTester()
        .check(getFactory(), query, typeChecker, parameterChecker,
            resultChecker);
  }

  /**
   * Declares that this test is for a given operator. So we can check that all
   * operators are tested.
   *
   * @param operator             Operator
   * @param unimplementedVmNames Names of virtual machines for which this
   */
  SqlOperatorFixture setFor(
      SqlOperator operator,
      VmName... unimplementedVmNames);

  /**
   * Checks that an aggregate expression returns the expected result.
   *
   * <p>For example, <code>checkAgg("AVG(DISTINCT x)", new String[] {"2", "3",
   * null, "3" }, new Double(2.5), 0);</code>
   *
   * @param expr        Aggregate expression, e.g. <code>SUM(DISTINCT x)</code>
   * @param inputValues Array of input values, e.g. <code>["1", null,
   *                    "2"]</code>.
   * @param checker     Result checker
   */
  void checkAgg(
      String expr,
      String[] inputValues,
      ResultChecker checker);

  /**
   * Checks that an aggregate expression returns the expected result.
   *
   * <p>For example, <code>checkAgg("AVG(DISTINCT x)", new String[] {"2", "3",
   * null, "3" }, "INTEGER", isSingle([2, 3]));</code>
   *
   * @param expr        Aggregate expression, e.g. <code>SUM(DISTINCT x)</code>
   * @param inputValues Array of input values, e.g. <code>["1", null,
   *                    "2"]</code>.
   * @param type        Expected result type
   * @param checker     Result checker
   */
  void checkAgg(
      String expr,
      String[] inputValues,
      String type,
      ResultChecker checker);

  /**
   * Checks that an aggregate expression with multiple args returns the expected
   * result.
   *
   * @param expr        Aggregate expression, e.g. <code>AGG_FUNC(x, x2, x3)</code>
   * @param inputValues Nested array of input values, e.g. <code>[
   *                    ["1", null, "2"]
   *                    ["3", "4", null]
   *                    ]</code>
   * @param resultChecker Checks whether the result has the expected value
   */
  void checkAggWithMultipleArgs(
      String expr,
      String[][] inputValues,
      ResultChecker resultChecker);

  /**
   * Checks that a windowed aggregate expression returns the expected result.
   *
   * <p>For example, <code>checkWinAgg("FIRST_VALUE(x)", new String[] {"2",
   * "3", null, "3" }, "INTEGER NOT NULL", 2, 0d);</code>
   *
   * @param expr          Aggregate expression, e.g. {@code SUM(DISTINCT x)}
   * @param inputValues   Array of input values, e.g. {@code ["1", null, "2"]}
   * @param type          Expected result type
   * @param resultChecker Checks whether the result has the expected value
   */
  void checkWinAgg(
      String expr,
      String[] inputValues,
      String windowSpec,
      String type,
      ResultChecker resultChecker);

  /**
   * Tests that an aggregate expression fails at run time.
   *
   * @param expr An aggregate expression
   * @param inputValues Array of input values
   * @param expectedError Pattern for expected error
   * @param runtime       If true, must fail at runtime; if false, must fail at
   *                      validate time
   */
  void checkAggFails(
      String expr,
      String[] inputValues,
      String expectedError,
      boolean runtime);

  /**
   * Tests that a scalar SQL expression fails at run time.
   *
   * @param expression    SQL scalar expression
   * @param expectedError Pattern for expected error. If !runtime, must
   *                      include an error location.
   * @param runtime       If true, must fail at runtime; if false, must fail at
   *                      validate time
   */
  void checkFails(
      StringAndPos expression,
      String expectedError,
      boolean runtime);

  /** As {@link #checkFails(StringAndPos, String, boolean)}, but with a string
   * that contains carets. */
  default void checkFails(
      String expression,
      String expectedError,
      boolean runtime) {
    checkFails(StringAndPos.of(expression), expectedError, runtime);
  }

  /**
   * Tests that a SQL query fails at prepare time.
   *
   * @param sap           SQL query and error position
   * @param expectedError Pattern for expected error. Must
   *                      include an error location.
   */
  void checkQueryFails(StringAndPos sap, String expectedError);

  /**
   * Tests that a SQL query succeeds at prepare time.
   *
   * @param sql           SQL query
   */
  void checkQuery(String sql);

  default SqlOperatorFixture withLibrary(SqlLibrary library) {
    return withOperatorTable(
        SqlLibraryOperatorTableFactory.INSTANCE
            .getOperatorTable(SqlLibrary.STANDARD, library))
        .withConnectionFactory(cf ->
            cf.with(ConnectionFactories.add(CalciteAssert.SchemaSpec.HR))
                .with(CalciteConnectionProperty.FUN, library.fun));
  }

  default SqlOperatorFixture withLibraries(SqlLibrary... libraries) {
    List<String> names = new ArrayList<>();
    for (SqlLibrary lib : libraries) {
      names.add(lib.fun);
    }
    return withOperatorTable(
        SqlLibraryOperatorTableFactory.INSTANCE
            .getOperatorTable(libraries))
        .withConnectionFactory(cf ->
            cf.with(ConnectionFactories.add(CalciteAssert.SchemaSpec.HR))
                .with(CalciteConnectionProperty.FUN, String.join(",", names)));
  }

  /** Applies this fixture to some code for each of the given libraries. */
  default void forEachLibrary(Iterable<? extends SqlLibrary> libraries,
      Consumer<SqlOperatorFixture> consumer) {
    SqlLibrary.expand(libraries).forEach(library -> {
      try {
        consumer.accept(this.withLibrary(library));
      } catch (Exception e) {
        throw new RuntimeException("for library " + library, e);
      }
    });
  }

  /** Applies this fixture to some code for each of the given conformances. */
  default void forEachConformance(Iterable<? extends SqlConformanceEnum> conformances,
      Consumer<SqlOperatorFixture> consumer) {
    conformances.forEach(conformance -> {
      try {
        consumer.accept(this.withConformance(conformance));
      } catch (Exception e) {
        throw new RuntimeException("for conformance " + conformance, e);
      }
    });
  }

  default SqlOperatorFixture forOracle(SqlConformance conformance) {
    return withConformance(conformance)
        .withOperatorTable(
            SqlLibraryOperatorTableFactory.INSTANCE
                .getOperatorTable(SqlLibrary.STANDARD, SqlLibrary.ORACLE))
        .withConnectionFactory(cf ->
            cf.with(ConnectionFactories.add(CalciteAssert.SchemaSpec.HR))
                .with("fun", "oracle"));
  }

  /**
   * Types for cast.
   */
  enum CastType {
    CAST("cast"),
    SAFE_CAST("safe_cast"),
    TRY_CAST("try_cast");

    CastType(String name) {
      this.name = name;
    }

    final String name;
  }

  default String getCastString(
      String value,
      String targetType,
      boolean errorLoc,
      CastType castType) {
    if (errorLoc) {
      value = "^" + value + "^";
    }
    String function = castType.name;
    return function + "(" + value + " as " + targetType + ")";
  }

  default void checkCastToApproxOkay(String value, String targetType,
      Object expected, CastType castType) {
    checkScalarApprox(getCastString(value, targetType, false, castType),
        getTargetType(targetType, castType), expected);
  }

  default void checkCastToStringOkay(String value, String targetType,
      String expected, CastType castType) {
    final String castString = getCastString(value, targetType, false, castType);
    checkString(castString, expected, getTargetType(targetType, castType));
  }

  default void checkCastToScalarOkay(String value, String targetType,
      String expected, CastType castType) {
    final String castString = getCastString(value, targetType, false, castType);
    checkScalarExact(castString, getTargetType(targetType, castType), expected);
  }

  default String getTargetType(String targetType, CastType castType) {
    return castType == CastType.CAST ? targetType + NON_NULLABLE_SUFFIX : targetType;
  }

  default void checkCastToScalarOkay(String value, String targetType,
      CastType castType) {
    checkCastToScalarOkay(value, targetType, value, castType);
  }

  default void checkCastFails(String value, String targetType,
      String expectedError, boolean runtime, CastType castType) {
    // Safe casts should never fail
    boolean shouldFail = castType == CastType.CAST;
    final String castString = getCastString(value, targetType, shouldFail && !runtime, castType);
    if (shouldFail) {
      checkFails(castString, expectedError, runtime);
    } else {
      checkNull(castString);
    }
  }

  default void checkCastToString(String value, @Nullable String type,
      @Nullable String expected, CastType castType) {
    String spaces = "     ";
    if (expected == null) {
      expected = value.trim();
    }
    int len = expected.length();
    if (type != null) {
      value = getCastString(value, type, false, castType);
    }

    // currently no exception thrown for truncation
    if (Bug.DT239_FIXED) {
      checkCastFails(value,
          "VARCHAR(" + (len - 1) + ")", STRING_TRUNC_MESSAGE,
          true, castType);
    }

    checkCastToStringOkay(value, "VARCHAR(" + len + ")", expected, castType);
    checkCastToStringOkay(value, "VARCHAR(" + (len + 5) + ")", expected, castType);

    // currently no exception thrown for truncation
    if (Bug.DT239_FIXED) {
      checkCastFails(value,
          "CHAR(" + (len - 1) + ")", STRING_TRUNC_MESSAGE,
          true, castType);
    }

    checkCastToStringOkay(value, "CHAR(" + len + ")", expected, castType);
    checkCastToStringOkay(value, "CHAR(" + (len + 5) + ")",
        expected + spaces, castType);
  }
}
