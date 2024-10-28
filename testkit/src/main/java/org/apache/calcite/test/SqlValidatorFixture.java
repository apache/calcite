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

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.test.AbstractSqlTester;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlTests;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.test.catalog.MockCatalogReaderExtended;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import org.hamcrest.Matcher;

import java.nio.charset.Charset;
import java.util.List;
import java.util.function.UnaryOperator;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.sql.SqlUtil.stripAs;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasToString;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static java.util.Objects.requireNonNull;

/**
 * A fixture for testing the SQL validator.
 *
 * <p>It provides a fluent API so that you can write tests by chaining method
 * calls.
 *
 * <p>It is immutable. If you have two test cases that require a similar set up
 * (for example, the same SQL expression and parser configuration), it is safe
 * to use the same fixture object as a starting point for both tests.
 */
public class SqlValidatorFixture {
  public final SqlTester tester;
  public final SqlTestFactory factory;
  public final StringAndPos sap;
  public final boolean expression;
  public final boolean whole;

  /**
   * Creates a SqlValidatorFixture.
   *
   * @param tester     Tester
   * @param sap        SQL query or expression
   * @param expression True if {@code sql} is an expression,
   *                   false if it is a query
   * @param whole      Whether the failure location is the whole query or
   *                   expression
   */
  protected SqlValidatorFixture(SqlTester tester, SqlTestFactory factory,
      StringAndPos sap, boolean expression, boolean whole) {
    this.tester = tester;
    this.factory = factory;
    this.expression = expression;
    this.sap = sap;
    this.whole = whole;
  }

  public SqlValidatorFixture withTester(UnaryOperator<SqlTester> transform) {
    final SqlTester tester = transform.apply(this.tester);
    return new SqlValidatorFixture(tester, factory, sap, expression, whole);
  }

  public SqlValidatorFixture withFactory(
      UnaryOperator<SqlTestFactory> transform) {
    final SqlTestFactory factory = transform.apply(this.factory);
    return new SqlValidatorFixture(tester, factory, sap, expression, whole);
  }

  public SqlValidatorFixture withParserConfig(
      UnaryOperator<SqlParser.Config> transform) {
    return withFactory(f -> f.withParserConfig(transform));
  }

  public SqlParser.Config parserConfig() {
    return factory.parserConfig();
  }

  public SqlValidatorFixture withSql(String sql) {
    StringAndPos sap = StringAndPos.of(sql);
    return new SqlValidatorFixture(tester, factory, sap, false, false);
  }

  public SqlValidatorFixture withExpr(String sql) {
    StringAndPos sap = StringAndPos.of(sql);
    return new SqlValidatorFixture(tester, factory, sap, true, false);
  }

  public StringAndPos toSql(boolean withCaret) {
    return expression
        ? StringAndPos.of(AbstractSqlTester.buildQuery(sap.addCarets()))
        : sap;
  }

  public SqlValidatorFixture withExtendedCatalog() {
    return withCatalogReader(MockCatalogReaderExtended::create);
  }

  public SqlValidatorFixture withCatalogReader(
      SqlTestFactory.CatalogReaderFactory catalogReaderFactory) {
    return withFactory(f -> f.withCatalogReader(catalogReaderFactory));
  }

  public SqlValidatorFixture withQuoting(Quoting quoting) {
    return withParserConfig(config -> config.withQuoting(quoting));
  }

  public SqlValidatorFixture withLex(Lex lex) {
    return withParserConfig(c -> c.withQuoting(lex.quoting)
        .withCaseSensitive(lex.caseSensitive)
        .withQuotedCasing(lex.quotedCasing)
        .withUnquotedCasing(lex.unquotedCasing));
  }

  public SqlValidatorFixture withConformance(SqlConformance conformance) {
    return withValidatorConfig(c -> c.withConformance(conformance))
        .withParserConfig(c -> c.withConformance(conformance))
        .withFactory(f -> conformance instanceof SqlConformanceEnum
            ? f.withConnectionFactory(cf ->
            cf.with(CalciteConnectionProperty.CONFORMANCE, conformance))
            : f);
  }

  public SqlConformance conformance() {
    return factory.parserConfig().conformance();
  }

  public SqlValidatorFixture withTypeCoercion(boolean typeCoercion) {
    return withValidatorConfig(c -> c.withTypeCoercionEnabled(typeCoercion));
  }

  /**
   * Returns a tester that does not fail validation if it encounters an
   * unknown function.
   */
  public SqlValidatorFixture withLenientOperatorLookup(boolean lenient) {
    return withValidatorConfig(c -> c.withLenientOperatorLookup(lenient));
  }

  public SqlValidatorFixture withWhole(boolean whole) {
    checkArgument(sap.cursor < 0);
    final StringAndPos sap = StringAndPos.of("^" + this.sap.sql + "^");
    return new SqlValidatorFixture(tester, factory, sap, expression, whole);
  }

  public SqlValidatorFixture ok() {
    tester.assertExceptionIsThrown(factory, toSql(false), null);
    return this;
  }

  /**
   * Checks that a SQL expression gives a particular error.
   */
  public SqlValidatorFixture fails(String expected) {
    requireNonNull(expected, "expected");
    tester.assertExceptionIsThrown(factory, toSql(true), expected);
    return this;
  }

  /**
   * Checks that a SQL expression fails, giving an {@code expected} error,
   * if {@code b} is true, otherwise succeeds.
   */
  public SqlValidatorFixture failsIf(boolean b, String expected) {
    if (b) {
      fails(expected);
    } else {
      ok();
    }
    return this;
  }

  /**
   * Checks that a query returns a row of the expected type. For example,
   *
   * <blockquote>
   * <code>sql("select empno, name from emp")<br>
   * .type("{EMPNO INTEGER NOT NULL, NAME VARCHAR(10) NOT NULL}");</code>
   * </blockquote>
   *
   * @param expectedType Expected row type
   */
  public SqlValidatorFixture type(String expectedType) {
    tester.validateAndThen(factory, sap, (sql1, validator, n) -> {
      RelDataType actualType = validator.getValidatedNodeType(n);
      String actual = SqlTests.getTypeString(actualType);
      assertThat(actual, is(expectedType));
    });
    return this;
  }

  /**
   * Checks that a query returns a single column, and that the column has the
   * expected type. For example,
   *
   * <blockquote>
   * <code>sql("SELECT empno FROM Emp").columnType("INTEGER NOT NULL");</code>
   * </blockquote>
   *
   * @param expectedType Expected type, including nullability
   */
  public SqlValidatorFixture columnType(String expectedType) {
    tester.checkColumnType(factory, toSql(false).sql, expectedType);
    return this;
  }

  /**
   * Tests that the first column of the query has a given monotonicity.
   *
   * @param matcher Expected monotonicity
   */
  public SqlValidatorFixture assertMonotonicity(
      Matcher<SqlMonotonicity> matcher) {
    tester.validateAndThen(factory, toSql(false),
        (sap, validator, n) -> {
          final RelDataType rowType = validator.getValidatedNodeType(n);
          final SqlValidatorNamespace selectNamespace =
              validator.getNamespace(n);
          final String field0 = rowType.getFieldList().get(0).getName();
          final SqlMonotonicity monotonicity =
              selectNamespace.getMonotonicity(field0);
          assertThat(monotonicity, matcher);
        });
    return this;
  }

  public SqlValidatorFixture assertBindType(Matcher<String> matcher) {
    tester.validateAndThen(factory, sap, (sap, validator, validatedNode) -> {
      final RelDataType parameterRowType =
          validator.getParameterRowType(validatedNode);
      assertThat(parameterRowType, hasToString(matcher));
    });
    return this;
  }

  public void assertCharset(Matcher<Charset> charsetMatcher) {
    tester.forEachQuery(factory, sap.addCarets(), query ->
        tester.validateAndThen(factory, StringAndPos.of(query),
            (sap, validator, n) -> {
              final RelDataType rowType = validator.getValidatedNodeType(n);
              final List<RelDataTypeField> fields = rowType.getFieldList();
              assertThat("expected query to return 1 field", fields.size(),
                  is(1));
              RelDataType actualType = fields.get(0).getType();
              Charset actualCharset = actualType.getCharset();
              assertThat(actualCharset, charsetMatcher);
            }));
  }

  public void assertCollation(Matcher<String> collationMatcher,
      Matcher<SqlCollation.Coercibility> coercibilityMatcher) {
    tester.forEachQuery(factory, sap.addCarets(), query ->
        tester.validateAndThen(factory, StringAndPos.of(query),
            (sap, validator, n) -> {
              RelDataType rowType = validator.getValidatedNodeType(n);
              final List<RelDataTypeField> fields = rowType.getFieldList();
              assertThat("expected query to return 1 field", fields.size(),
                  is(1));
              RelDataType actualType = fields.get(0).getType();
              SqlCollation collation = actualType.getCollation();
              assertThat(collation, notNullValue());
              assertThat(collation.getCollationName(), collationMatcher);
              assertThat(collation.getCoercibility(), coercibilityMatcher);
            }));
  }

  /**
   * Checks if the interval value conversion to milliseconds is valid. For
   * example,
   *
   * <blockquote>
   * <code>sql("VALUES (INTERVAL '1' Minute)").intervalConv("60000");</code>
   * </blockquote>
   */
  public void assertInterval(Matcher<Long> matcher) {
    tester.validateAndThen(factory, toSql(false),
        (sap, validator, validatedNode) -> {
          final SqlCall n = (SqlCall) validatedNode;
          SqlNode node = null;
          for (int i = 0; i < n.operandCount(); i++) {
            node = stripAs(n.operand(i));
            if (node instanceof SqlCall) {
              node = ((SqlCall) node).operand(0);
              break;
            }
          }

          assertNotNull(node);
          SqlIntervalLiteral intervalLiteral = (SqlIntervalLiteral) node;
          SqlIntervalLiteral.IntervalValue interval =
              intervalLiteral.getValueAs(
                  SqlIntervalLiteral.IntervalValue.class);
          long l =
              interval.getIntervalQualifier().isYearMonth()
                  ? SqlParserUtil.intervalToMonths(interval)
                  : SqlParserUtil.intervalToMillis(interval);
          assertThat(l, matcher);
        });
  }

  public SqlValidatorFixture withCaseSensitive(boolean caseSensitive) {
    return withParserConfig(c -> c.withCaseSensitive(caseSensitive));
  }

  public SqlValidatorFixture withOperatorTable(SqlOperatorTable operatorTable) {
    return withFactory(c -> c.withOperatorTable(o -> operatorTable));
  }

  public SqlValidatorFixture withQuotedCasing(Casing casing) {
    return withParserConfig(c -> c.withQuotedCasing(casing));
  }

  public SqlValidatorFixture withUnquotedCasing(Casing casing) {
    return withParserConfig(c -> c.withUnquotedCasing(casing));
  }

  public SqlValidatorFixture withValidatorConfig(
      UnaryOperator<SqlValidator.Config> transform) {
    return withFactory(f -> f.withValidatorConfig(transform));
  }

  public SqlValidatorFixture withValidatorIdentifierExpansion(
      boolean expansion) {
    return withValidatorConfig(c -> c.withIdentifierExpansion(expansion));
  }

  public SqlValidatorFixture withValidatorCallRewrite(boolean rewrite) {
    return withValidatorConfig(c -> c.withCallRewrite(rewrite));
  }

  public SqlValidatorFixture withValidatorColumnReferenceExpansion(
      boolean expansion) {
    return withValidatorConfig(c ->
        c.withColumnReferenceExpansion(expansion));
  }

  public SqlValidatorFixture rewritesTo(String expected) {
    tester.validateAndThen(factory, toSql(false),
        (sap, validator, validatedNode) -> {
          String actualRewrite =
              validatedNode.toSqlString(AnsiSqlDialect.DEFAULT, false)
                  .getSql();
          TestUtil.assertEqualsVerbose(expected, Util.toLinux(actualRewrite));
        });
    return this;
  }

  public SqlValidatorFixture isAggregate(Matcher<Boolean> matcher) {
    tester.validateAndThen(factory, toSql(false),
        (sap, validator, validatedNode) ->
            assertThat(validator.isAggregate((SqlSelect) validatedNode),
                matcher));
    return this;
  }

  /**
   * Tests that the list of the origins of each result field of
   * the current query match expected.
   *
   * <p>The field origin list looks like this:
   * <code>"{(CATALOG.SALES.EMP.EMPNO, null)}"</code>.
   */
  public SqlValidatorFixture assertFieldOrigin(Matcher<String> matcher) {
    tester.validateAndThen(factory, toSql(false), (sap, validator, n) -> {
      final List<List<String>> list = validator.getFieldOrigins(n);
      final StringBuilder buf = new StringBuilder("{");
      int i = 0;
      for (List<String> strings : list) {
        if (i++ > 0) {
          buf.append(", ");
        }
        if (strings == null) {
          buf.append("null");
        } else {
          int j = 0;
          for (String s : strings) {
            if (j++ > 0) {
              buf.append('.');
            }
            buf.append(s);
          }
        }
      }
      buf.append("}");
      assertThat(buf, hasToString(matcher));
    });
    return this;
  }

  public void setFor(SqlOperator operator) {
  }
}
