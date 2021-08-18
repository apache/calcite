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
import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.test.AbstractSqlTester;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.test.catalog.MockCatalogReaderExtended;
import org.apache.calcite.test.catalog.MockCatalogReaderSimpleNamedParam;
import org.apache.calcite.testlib.annotations.WithLex;

import com.google.common.base.Preconditions;

import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.function.UnaryOperator;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * An abstract base class for implementing tests against {@link SqlValidator}.
 *
 * <p>A derived class can refine this test in two ways. First, it can add <code>
 * testXxx()</code> methods, to test more functionality.
 *
 * <p>Second, it can override the {@link #getTester} method to return a
 * different implementation of the {@link Tester} object. This encapsulates the
 * differences between test environments, for example, which SQL parser or
 * validator to use.</p>
 */
public class SqlValidatorTestCase {
  private static final SqlTestFactory EXTENDED_TEST_FACTORY =
      SqlTestFactory.INSTANCE.withCatalogReader(MockCatalogReaderExtended::new);

  private static final SqlTestFactory NAMED_PARAM_TEST_FACTORY =
      SqlTestFactory.INSTANCE.with("namedParamTable", "BodoNamedParams").withCatalogReader(
          MockCatalogReaderSimpleNamedParam::new);

  private static final SqlTestFactory NAMED_PARAM_TEST_FACTORY_NO_SCHEMA =
      SqlTestFactory.INSTANCE.with("namedParamTable", "BodoNamedParams");

  static final SqlTester EXTENDED_CATALOG_TESTER =
      new SqlValidatorTester(EXTENDED_TEST_FACTORY);

  static final SqlTester EXTENDED_CATALOG_TESTER_2003 =
      new SqlValidatorTester(EXTENDED_TEST_FACTORY)
          .withConformance(SqlConformanceEnum.PRAGMATIC_2003);

  static final SqlTester EXTENDED_CATALOG_TESTER_LENIENT =
      new SqlValidatorTester(EXTENDED_TEST_FACTORY)
          .withConformance(SqlConformanceEnum.LENIENT);

  static final SqlTester NAMED_PARAMS_TESTER =
      new SqlValidatorTester(NAMED_PARAM_TEST_FACTORY);

  static final SqlTester NAMED_PARAMS_TESTER_NO_SCHEMA =
      new SqlValidatorTester(NAMED_PARAM_TEST_FACTORY_NO_SCHEMA);

  protected SqlTester tester;

  /**
   * Creates a test case.
   */
  public SqlValidatorTestCase() {
    this.tester = getTester();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns a tester. Derived classes should override this method to run the
   * same set of tests in a different testing environment.
   */
  public SqlTester getTester() {
    return new SqlValidatorTester(SqlTestFactory.INSTANCE);
  }

  /** Creates a test context with a SQL query. */
  public final Sql sql(String sql) {
    return new Sql(tester, StringAndPos.of(sql), true, false);
  }

  /** Creates a test context with a SQL expression. */
  public final Sql expr(String sql) {
    return new Sql(tester, StringAndPos.of(sql), false, false);
  }

  /** Creates a test context with a SQL expression.
   * If an error occurs, the error is expected to span the entire expression. */
  public final Sql wholeExpr(String sql) {
    return expr(sql).withWhole(true);
  }

  public final Sql winSql(String sql) {
    return sql(sql);
  }

  public final Sql win(String sql) {
    return sql("select * from emp " + sql);
  }

  public Sql winExp(String sql) {
    return winSql("select " + sql + " from emp window w as (order by deptno)");
  }

  public Sql winExp2(String sql) {
    return winSql("select " + sql + " from emp");
  }

  /**
   * Encapsulates differences between test environments, for example, which
   * SQL parser or validator to use.
   *
   * <p>It contains a mock schema with <code>EMP</code> and <code>DEPT</code>
   * tables, which can run without having to start up Farrago.
   */
  public interface Tester {
    SqlNode parseQuery(String sql) throws SqlParseException;

    SqlNode parseAndValidate(SqlValidator validator, String sql);

    SqlValidator getValidator();

    /**
     * Checks that a query is valid, or, if invalid, throws the right
     * message at the right location.
     *
     * <p>If <code>expectedMsgPattern</code> is null, the query must
     * succeed.
     *
     * <p>If <code>expectedMsgPattern</code> is not null, the query must
     * fail, and give an error location of (expectedLine, expectedColumn)
     * through (expectedEndLine, expectedEndColumn).
     *  @param sap                SQL statement
     * @param expectedMsgPattern If this parameter is null the query must be
     *                           valid for the test to pass; If this parameter
     *                           is not null the query must be malformed and the
     */
    void assertExceptionIsThrown(
        StringAndPos sap,
        String expectedMsgPattern);

    /**
     * Returns the data type of the sole column of a SQL query.
     *
     * <p>For example, <code>getResultType("VALUES (1")</code> returns
     * <code>INTEGER</code>.
     *
     * <p>Fails if query returns more than one column.
     *
     * @see #getResultType(String)
     */
    RelDataType getColumnType(String sql);

    /**
     * Returns the data type of the row returned by a SQL query.
     *
     * <p>For example, <code>getResultType("VALUES (1, 'foo')")</code>
     * returns <code>RecordType(INTEGER EXPR$0, CHAR(3) EXPR#1)</code>.
     */
    RelDataType getResultType(String sql);

    void checkCollation(
        String sql,
        String expectedCollationName,
        SqlCollation.Coercibility expectedCoercibility);

    void checkCharset(
        String sql,
        Charset expectedCharset);

    /**
     * Checks that a query returns one column of an expected type. For
     * example, <code>checkType("VALUES (1 + 2)", "INTEGER NOT
     * NULL")</code>.
     */
    void checkColumnType(
        String sql,
        String expected);

    /**
     * Given a SQL query, returns a list of the origins of each result
     * field.
     *
     * @param sql             SQL query
     * @param fieldOriginList Field origin list, e.g.
     *                        "{(CATALOG.SALES.EMP.EMPNO, null)}"
     */
    void checkFieldOrigin(String sql, String fieldOriginList);

    /**
     * Checks that a query gets rewritten to an expected form.
     *
     * @param query           query to test
     * @param expectedRewrite expected SQL text after rewrite and unparse
     */
    void checkRewrite(String query, String expectedRewrite);

    /**
     * Checks that a query returns one column of an expected type. For
     * example, <code>checkType("select empno, name from emp""{EMPNO INTEGER
     * NOT NULL, NAME VARCHAR(10) NOT NULL}")</code>.
     */
    void checkResultType(
        String sql,
        String expected);

    /**
     * Checks if the interval value conversion to milliseconds is valid. For
     * example, <code>checkIntervalConv(VALUES (INTERVAL '1' Minute),
     * "60000")</code>.
     */
    void checkIntervalConv(
        String sql,
        String expected);

    /**
     * Given a SQL query, returns the monotonicity of the first item in the
     * SELECT clause.
     *
     * @param sql SQL query
     * @return Monotonicity
     */
    SqlMonotonicity getMonotonicity(String sql);

    SqlConformance getConformance();
  }

  /** Fluent testing API. */
  static class Sql {
    private final SqlTester tester;
    private final StringAndPos sap;
    private final boolean query;
    private final boolean whole;

    /** Creates a Sql.
     *
     * @param tester Tester
     * @param sap SQL query or expression
     * @param query True if {@code sql} is a query, false if it is an expression
     * @param whole Whether the failure location is the whole query or
     *              expression
     */
    Sql(SqlTester tester, StringAndPos sap, boolean query,
        boolean whole) {
      this.tester = tester;
      this.query = query;
      this.sap = sap;
      this.whole = whole;
    }

    Sql withTester(UnaryOperator<SqlTester> transform) {
      return new Sql(transform.apply(tester), sap, query, whole);
    }

    public Sql sql(String sql) {
      return new Sql(tester, StringAndPos.of(sql), true, false);
    }

    public Sql expr(String sql) {
      return new Sql(tester, StringAndPos.of(sql), false, false);
    }

    public StringAndPos toSql(boolean withCaret) {
      final String sql2 = withCaret && sap.cursor >= 0
          ? sap.sql.substring(0, sap.cursor)
          + "^" + sap.sql.substring(sap.cursor)
          : sap.sql;
      return query ? sap
          : StringAndPos.of(AbstractSqlTester.buildQuery(sap.addCarets()));
    }

    Sql withExtendedCatalog() {
      return withTester(tester -> EXTENDED_CATALOG_TESTER);
    }

    public Sql withNamedParamters() {
      return withTester(tester -> NAMED_PARAMS_TESTER);
    }

    public Sql withNamedParametersNoSchema() {
      return withTester(tester -> NAMED_PARAMS_TESTER_NO_SCHEMA);
    }

    public Sql withQuoting(Quoting quoting) {
      return withTester(tester -> tester.withQuoting(quoting));
    }

    Sql withLex(Lex lex) {
      return withTester(tester -> tester.withLex(lex));
    }

    Sql withConformance(SqlConformance conformance) {
      return withTester(tester -> tester.withConformance(conformance));
    }

    Sql withTypeCoercion(boolean typeCoercion) {
      return withTester(tester -> tester.enableTypeCoercion(typeCoercion));
    }

    Sql withWhole(boolean whole) {
      Preconditions.checkArgument(sap.cursor < 0);
      return new Sql(tester, StringAndPos.of("^" + sap.sql + "^"),
          query, whole);
    }

    Sql ok() {
      tester.assertExceptionIsThrown(toSql(false), null);
      return this;
    }

    /**
     * Checks that a SQL expression gives a particular error.
     */
    Sql fails(String expected) {
      Objects.requireNonNull(expected, "expected");
      tester.assertExceptionIsThrown(toSql(true), expected);
      return this;
    }

    /**
     * Checks that a SQL expression fails, giving an {@code expected} error,
     * if {@code b} is true, otherwise succeeds.
     */
    Sql failsIf(boolean b, String expected) {
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
     *   <code>sql("select empno, name from emp")<br>
     *     .type("{EMPNO INTEGER NOT NULL, NAME VARCHAR(10) NOT NULL}");</code>
     * </blockquote>
     *
     * @param expectedType Expected row type
     */
    public Sql type(String expectedType) {
      tester.checkResultType(sap.sql, expectedType);
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
    public Sql columnType(String expectedType) {
      tester.checkColumnType(toSql(false).sql, expectedType);
      return this;
    }

    public Sql monotonic(SqlMonotonicity expectedMonotonicity) {
      tester.checkMonotonic(toSql(false).sql, expectedMonotonicity);
      return this;
    }

    public Sql bindType(final String bindType) {
      tester.check(sap.sql, null, parameterRowType ->
          assertThat(parameterRowType.toString(), is(bindType)),
          result -> { });
      return this;
    }

    public void charset(Charset expectedCharset) {
      tester.checkCharset(sap.sql, expectedCharset);
    }

    public void collation(String expectedCollationName,
        SqlCollation.Coercibility expectedCoercibility) {
      tester.checkCollation(sap.sql, expectedCollationName, expectedCoercibility);
    }

    /**
     * Checks if the interval value conversion to milliseconds is valid. For
     * example,
     *
     * <blockquote>
     *   <code>sql("VALUES (INTERVAL '1' Minute)").intervalConv("60000");</code>
     * </blockquote>
     */
    public void intervalConv(String expected) {
      tester.checkIntervalConv(toSql(false).sql, expected);
    }

    public Sql withCaseSensitive(boolean caseSensitive) {
      return withTester(tester -> tester.withCaseSensitive(caseSensitive));
    }

    public Sql withOperatorTable(SqlOperatorTable operatorTable) {
      return withTester(tester -> tester.withOperatorTable(operatorTable));
    }

    public Sql withUnquotedCasing(Casing casing) {
      return withTester(tester -> tester.withUnquotedCasing(casing));
    }

    private SqlTester addTransform(SqlTester tester, UnaryOperator<SqlValidator> after) {
      return this.tester.withValidatorTransform(transform ->
          validator -> after.apply(transform.apply(validator)));
    }

    public Sql withValidatorIdentifierExpansion(boolean expansion) {
      final UnaryOperator<SqlValidator> after = sqlValidator ->
          sqlValidator.transform(config -> config.withIdentifierExpansion(expansion));
      return withTester(tester -> addTransform(tester, after));
    }

    public Sql withValidatorCallRewrite(boolean rewrite) {
      final UnaryOperator<SqlValidator> after = sqlValidator ->
          sqlValidator.transform(config -> config.withCallRewrite(rewrite));
      return withTester(tester -> addTransform(tester, after));
    }

    public Sql withValidatorColumnReferenceExpansion(boolean expansion) {
      final UnaryOperator<SqlValidator> after = sqlValidator ->
          sqlValidator.transform(config -> config.withColumnReferenceExpansion(expansion));
      return withTester(tester -> addTransform(tester, after));
    }

    public Sql rewritesTo(String expected) {
      tester.checkRewrite(toSql(false).sql, expected);
      return this;
    }
  }

  /**
   * Enables to configure {@link #tester} behavior on a per-test basis.
   * {@code tester} object is created in the test object constructor, and
   * there's no trivial way to override its features.
   *
   * <p>This JUnit rule enables post-process test object on a per test method
   * basis.
   */
  public static class LexConfiguration implements BeforeEachCallback {
    @Override public void beforeEach(ExtensionContext context) {
      context.getElement()
          .flatMap(element -> AnnotationSupport.findAnnotation(element, WithLex.class))
          .ifPresent(lex -> {
            SqlValidatorTestCase tc = (SqlValidatorTestCase) context.getTestInstance().get();
            SqlTester tester = tc.tester;
            tester = tester.withLex(lex.value());
            tc.tester = tester;
          });
    }
  }
}
