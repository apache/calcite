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
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.SqlValidatorTestCase;

import java.sql.ResultSet;

/**
 * SqlTester defines a callback for testing SQL queries and expressions.
 *
 * <p>The idea is that when you define an operator (or another piece of SQL
 * functionality), you can define the logical behavior of that operator once, as
 * part of that operator. Later you can define one or more physical
 * implementations of that operator, and test them all using the same set of
 * tests.
 *
 * <p>Specific implementations of <code>SqlTester</code> might evaluate the
 * queries in different ways, for example, using a C++ versus Java calculator.
 * An implementation might even ignore certain calls altogether.
 */
public interface SqlTester extends AutoCloseable, SqlValidatorTestCase.Tester {
  //~ Enums ------------------------------------------------------------------

  /**
   * Name of a virtual machine that can potentially implement an operator.
   */
  enum VmName {
    FENNEL, JAVA, EXPAND
  }

  //~ Methods ----------------------------------------------------------------

  SqlTestFactory getFactory();

  /** Returns a tester that tests a given SQL quoting style. */
  SqlTester withQuoting(Quoting quoting);

  /** Returns a tester that applies a given casing policy to quoted
   * identifiers. */
  SqlTester withQuotedCasing(Casing casing);

  /** Returns a tester that applies a given casing policy to unquoted
   * identifiers. */
  SqlTester withUnquotedCasing(Casing casing);

  /** Returns a tester that matches identifiers by case-sensitive or
   * case-insensitive. */
  SqlTester withCaseSensitive(boolean sensitive);

  /** Returns a tester that follows a lex policy. */
  SqlTester withLex(Lex lex);

  /** Returns a tester that tests conformance to a particular SQL language
   * version. */
  SqlTester withConformance(SqlConformance conformance);

  /** Returns a tester that tests with implicit type coercion on/off. */
  SqlTester enableTypeCoercion(boolean enabled);

  /** Returns a tester that gets connections from a given factory. */
  SqlTester withConnectionFactory(
      CalciteAssert.ConnectionFactory connectionFactory);

  /** Returns a tester that uses a given operator table. */
  SqlTester withOperatorTable(SqlOperatorTable operatorTable);

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
  void checkScalar(
      String expression,
      Object result,
      String resultType);

  /**
   * Tests that a scalar SQL expression returns the expected exact numeric
   * result as an integer. For example,
   *
   * <blockquote>
   * <pre>checkScalarExact("1 + 2", "3");</pre>
   * </blockquote>
   *
   * @param expression Scalar expression
   * @param result     Expected result
   */
  void checkScalarExact(
      String expression,
      String result);

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
  void checkScalarExact(
      String expression,
      String expectedType,
      String result);

  /**
   * Tests that a scalar SQL expression returns expected appoximate numeric
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
   * @param expectedResult Expected result
   * @param delta          Allowed margin of error between expected and actual
   *                       result
   */
  void checkScalarApprox(
      String expression,
      String expectedType,
      double expectedResult,
      double delta);

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
      Boolean result);

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
   * @param result      Expected result
   * @param delta       The acceptable tolerance between the expected and actual
   */
  void check(
      String query,
      TypeChecker typeChecker,
      Object result,
      double delta);

  /**
   * Tests that a SQL query returns a result of expected type and value.
   * Checking of type and value are abstracted using {@link TypeChecker}
   * and {@link ResultChecker} functors.
   *
   * @param query         SQL query
   * @param typeChecker   Checks whether the result is the expected type; must
   *                      not be null
   * @param parameterChecker Checks whether the parameters are of expected
   *                      types
   * @param resultChecker Checks whether the result has the expected value;
   *                      must not be null
   */
  void check(
      String query,
      TypeChecker typeChecker,
      ParameterChecker parameterChecker,
      ResultChecker resultChecker);

  /**
   * Tests that the first column of a SQL query has a given monotonicity.
   *
   * @param expectedMonotonicity Expected monotonicity
   * @param query                SQL query
   */
  void checkMonotonic(String query, SqlMonotonicity expectedMonotonicity);

  /**
   * Declares that this test is for a given operator. So we can check that all
   * operators are tested.
   *
   * @param operator             Operator
   * @param unimplementedVmNames Names of virtual machines for which this
   */
  void setFor(
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
   * @param result      Expected result
   * @param delta       Allowable variance from expected result
   */
  void checkAgg(
      String expr,
      String[] inputValues,
      Object result,
      double delta);

  /**
   * Checks that an aggregate expression with multiple args returns the expected
   * result.
   *
   * @param expr        Aggregate expression, e.g. <code>AGG_FUNC(x, x2, x3)</code>
   * @param inputValues Nested array of input values, e.g. <code>[
   *                    ["1", null, "2"]
   *                    ["3", "4", null]
   *                    ]</code>.
   * @param result      Expected result
   * @param delta       Allowable variance from expected result
   */
  void checkAggWithMultipleArgs(
      String expr,
      String[][] inputValues,
      Object result,
      double delta);

  /**
   * Checks that a windowed aggregate expression returns the expected result.
   *
   * <p>For example, <code>checkWinAgg("FIRST_VALUE(x)", new String[] {"2",
   * "3", null, "3" }, "INTEGER NOT NULL", 2, 0d);</code>
   *
   * @param expr        Aggregate expression, e.g. <code>SUM(DISTINCT x)</code>
   * @param inputValues Array of input values, e.g. <code>["1", null,
   *                    "2"]</code>.
   * @param type        Expected result type
   * @param result      Expected result
   * @param delta       Allowable variance from expected result
   */
  void checkWinAgg(
      String expr,
      String[] inputValues,
      String windowSpec,
      String type,
      Object result,
      double delta);

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
      String expression,
      String expectedError,
      boolean runtime);

  /**
   * Tests that a SQL query fails at prepare time.
   *
   * @param sql           SQL query
   * @param expectedError Pattern for expected error. Must
   *                      include an error location.
   */
  void checkQueryFails(
      String sql,
      String expectedError);

  /**
   * Tests that a SQL query succeeds at prepare time.
   *
   * @param sql           SQL query
   */
  void checkQuery(String sql);

  //~ Inner Interfaces -------------------------------------------------------

  /** Type checker. */
  interface TypeChecker {
    void checkType(RelDataType type);
  }

  /** Parameter checker. */
  interface ParameterChecker {
    void checkParameters(RelDataType parameterRowType);
  }

  /** Result checker. */
  interface ResultChecker {
    void checkResult(ResultSet result) throws Exception;
  }
}

// End SqlTester.java
