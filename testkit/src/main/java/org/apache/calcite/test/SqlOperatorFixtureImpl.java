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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.test.ResultCheckers;
import org.apache.calcite.sql.test.SqlOperatorFixture;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlTests;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.JdbcType;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.function.UnaryOperator;

import static org.apache.calcite.sql.test.ResultCheckers.isNullValue;
import static org.apache.calcite.sql.test.ResultCheckers.isSingle;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static java.util.Objects.requireNonNull;

/**
 * Implementation of {@link SqlOperatorFixture}.
 */
class SqlOperatorFixtureImpl implements SqlOperatorFixture {
  public static final SqlOperatorFixtureImpl DEFAULT =
      new SqlOperatorFixtureImpl(SqlTestFactory.INSTANCE,
          SqlValidatorTester.DEFAULT, false);

  private final SqlTestFactory factory;
  private final SqlTester tester;
  private final boolean brokenTestsEnabled;

  SqlOperatorFixtureImpl(SqlTestFactory factory, SqlTester tester,
      boolean brokenTestsEnabled) {
    this.factory = requireNonNull(factory, "factory");
    this.tester = requireNonNull(tester, "tester");
    this.brokenTestsEnabled = brokenTestsEnabled;
  }

  @Override public void close() {
  }

  @Override public SqlTestFactory getFactory() {
    return factory;
  }

  @Override public SqlTester getTester() {
    return tester;
  }

  @Override public SqlOperatorFixture withFactory(
      UnaryOperator<SqlTestFactory> transform) {
    final SqlTestFactory factory = transform.apply(this.factory);
    if (factory == this.factory) {
      return this;
    }
    return new SqlOperatorFixtureImpl(factory, tester, brokenTestsEnabled);
  }

  @Override public SqlOperatorFixture withTester(
      UnaryOperator<SqlTester> transform) {
    final SqlTester tester = transform.apply(this.tester);
    if (tester == this.tester) {
      return this;
    }
    return new SqlOperatorFixtureImpl(factory, tester, brokenTestsEnabled);
  }

  @Override public boolean brokenTestsEnabled() {
    return brokenTestsEnabled;
  }

  @Override public SqlOperatorFixture withBrokenTestsEnabled(
      boolean brokenTestsEnabled) {
    if (brokenTestsEnabled == this.brokenTestsEnabled) {
      return this;
    }
    return new SqlOperatorFixtureImpl(factory, tester, brokenTestsEnabled);
  }

  @Override public SqlOperatorFixture setFor(SqlOperator operator,
      VmName... unimplementedVmNames) {
    return this;
  }

  SqlNode parseAndValidate(SqlValidator validator, String sql) {
    SqlNode sqlNode;
    try {
      sqlNode = tester.parseQuery(factory, sql);
    } catch (Throwable e) {
      throw new RuntimeException("Error while parsing query: " + sql, e);
    }
    return validator.validate(sqlNode);
  }

  @Override public void checkColumnType(String sql, String expected) {
    tester.validateAndThen(factory, StringAndPos.of(sql),
        checkColumnTypeAction(is(expected)));
  }

  @Override public void checkType(String expression, String type) {
    forEachQueryValidateAndThen(StringAndPos.of(expression),
        checkColumnTypeAction(is(type)));
  }

  private static SqlTester.ValidatedNodeConsumer checkColumnTypeAction(
      Matcher<String> matcher) {
    return (sql, validator, validatedNode) -> {
      final RelDataType rowType =
          validator.getValidatedNodeType(validatedNode);
      final List<RelDataTypeField> fields = rowType.getFieldList();
      assertThat("expected query to return 1 field", fields, hasSize(1));
      final RelDataType actualType = fields.get(0).getType();
      String actual = SqlTests.getTypeString(actualType);
      assertThat("Query: " + sql.sql, actual, matcher);
    };
  }

  @Override public void checkQuery(String sql) {
    tester.assertExceptionIsThrown(factory, StringAndPos.of(sql), null);
  }

  void forEachQueryValidateAndThen(StringAndPos expression,
      SqlTester.ValidatedNodeConsumer consumer) {
    tester.forEachQuery(factory, expression.addCarets(), query ->
        tester.validateAndThen(factory, StringAndPos.of(query), consumer));
  }

  @Override public void checkFails(StringAndPos sap, String expectedError,
      boolean runtime) {
    if (runtime) {
      // We need to test that the expression fails at runtime.
      // Ironically, that means that it must succeed at prepare time.
      final String sql = "values (" + sap.sql + ")";
      SqlValidator validator = factory.createValidator();
      SqlNode n = parseAndValidate(validator, sql);
      assertNotNull(n);
      tester.checkFails(factory, sap, expectedError, runtime);
    } else {
      final String sql = "values (" + sap.addCarets() + ")";
      checkQueryFails(StringAndPos.of(sql),
          expectedError);
    }
  }

  @Override public void checkQueryFails(StringAndPos sap,
      String expectedError) {
    tester.assertExceptionIsThrown(factory, sap, expectedError);
  }

  @Override public void checkAggFails(
      String expr,
      String[] inputValues,
      String expectedError,
      boolean runtime) {
    final String sql =
        SqlTests.generateAggQuery(expr, inputValues);
    if (runtime) {
      SqlValidator validator = factory.createValidator();
      SqlNode n = parseAndValidate(validator, sql);
      assertNotNull(n);
      tester.checkAggFails(factory, expr, inputValues, expectedError, runtime);
    } else {
      checkQueryFails(StringAndPos.of(sql), expectedError);
    }
  }

  @Override public void checkAgg(String expr, String[] inputValues,
      SqlTester.ResultChecker checker) {
    checkAgg(expr, inputValues, SqlTests.ANY_TYPE_CHECKER, checker);
  }

  @Override public void checkAgg(String expr, String[] inputValues,
      String type, SqlTester.ResultChecker checker) {
    final SqlTester.TypeChecker typeChecker =
        new SqlTests.StringTypeChecker(type);
    checkAgg(expr, inputValues, typeChecker, checker);
  }

  private void checkAgg(String expr, String[] inputValues,
      SqlTester.TypeChecker typeChecker, SqlTester.ResultChecker resultChecker) {
    String query =
        SqlTests.generateAggQuery(expr, inputValues);
    tester.check(factory, query, typeChecker, resultChecker);
  }

  @Override public void checkAggWithMultipleArgs(
      String expr,
      String[][] inputValues,
      SqlTester.ResultChecker resultChecker) {
    String query =
        SqlTests.generateAggQueryWithMultipleArgs(expr, inputValues);
    tester.check(factory, query, SqlTests.ANY_TYPE_CHECKER, resultChecker);
  }

  @Override public void checkWinAgg(
      String expr,
      String[] inputValues,
      String windowSpec,
      String type,
      SqlTester.ResultChecker resultChecker) {
    final SqlTester.TypeChecker typeChecker =
        new SqlTests.StringTypeChecker(type);
    String query =
        SqlTests.generateWinAggQuery(expr, windowSpec, inputValues);
    tester.check(factory, query, typeChecker, resultChecker);
  }

  @Override public void checkScalar(String expression,
      SqlTester.TypeChecker typeChecker,
      SqlTester.ResultChecker resultChecker) {
    tester.forEachQuery(factory, expression, sql ->
        tester.check(factory, sql, typeChecker, resultChecker));
  }

  @Override public void checkScalarExact(String expression,
      String expectedType, SqlTester.ResultChecker resultChecker) {
    final SqlTester.TypeChecker typeChecker =
        new SqlTests.StringTypeChecker(expectedType);
    tester.forEachQuery(factory, expression, sql ->
        tester.check(factory, sql, typeChecker, resultChecker));
  }

  @Override public void checkScalarApprox(
      String expression,
      String expectedType,
      Object result) {
    SqlTester.TypeChecker typeChecker =
        new SqlTests.StringTypeChecker(expectedType);
    final SqlTester.ResultChecker checker = ResultCheckers.createChecker(result);
    tester.forEachQuery(factory, expression, sql ->
        tester.check(factory, sql, typeChecker, checker));
  }

  @Override public void checkBoolean(
      String expression,
      @Nullable Boolean result) {
    if (null == result) {
      checkNull(expression);
    } else {
      SqlTester.ResultChecker resultChecker =
          ResultCheckers.createChecker(is(result), JdbcType.BOOLEAN);
      tester.forEachQuery(factory, expression, sql ->
          tester.check(factory, sql, SqlTests.BOOLEAN_TYPE_CHECKER,
              SqlTests.ANY_PARAMETER_CHECKER, resultChecker));
    }
  }

  @Override public void checkString(
      String expression,
      String result,
      String expectedType) {
    SqlTester.TypeChecker typeChecker =
        new SqlTests.StringTypeChecker(expectedType);
    SqlTester.ResultChecker resultChecker = isSingle(result);
    tester.forEachQuery(factory, expression, sql ->
        tester.check(factory, sql, typeChecker, resultChecker));
  }

  @Override public void checkNull(String expression) {
    tester.forEachQuery(factory, expression, sql ->
        tester.check(factory, sql, SqlTests.ANY_NULLABLE_TYPE_CHECKER, isNullValue()));
  }
}
