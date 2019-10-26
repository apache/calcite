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
import org.apache.calcite.sql.test.AbstractSqlTester;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlValidatorTester;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.test.catalog.MockCatalogReaderExtended;

import com.google.common.base.Preconditions;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

import java.nio.charset.Charset;
import java.util.Objects;
import java.util.function.UnaryOperator;
import javax.annotation.Nonnull;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

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
  //~ Static fields/initializers ---------------------------------------------

  private static final SqlTestFactory EXTENDED_TEST_FACTORY =
      SqlTestFactory.INSTANCE.withCatalogReader(MockCatalogReaderExtended::new);

  static final SqlTester EXTENDED_CATALOG_TESTER =
      new SqlValidatorTester(EXTENDED_TEST_FACTORY);

  static final SqlTester EXTENDED_CATALOG_TESTER_2003 =
      new SqlValidatorTester(EXTENDED_TEST_FACTORY)
          .withConformance(SqlConformanceEnum.PRAGMATIC_2003);

  static final SqlTester EXTENDED_CATALOG_TESTER_LENIENT =
      new SqlValidatorTester(EXTENDED_TEST_FACTORY)
          .withConformance(SqlConformanceEnum.LENIENT);

  public static final MethodRule TESTER_CONFIGURATION_RULE = new TesterConfigurationRule();

  //~ Instance fields --------------------------------------------------------

  protected SqlTester tester;

  //~ Constructors -----------------------------------------------------------

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
    return new Sql(tester, sql, true, false);
  }

  /** Creates a test context with a SQL expression. */
  public final Sql expr(String sql) {
    return new Sql(tester, sql, false, false);
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

  @Deprecated // to be removed before 1.23
  public void check(String sql) {
    sql(sql).ok();
  }

  @Deprecated // to be removed before 1.23
  public void checkExp(String sql) {
    expr(sql).ok();
  }

  @Deprecated // to be removed before 1.23
  public final void checkFails(
      String sql,
      String expected) {
    sql(sql).fails(expected);
  }

  @Deprecated // to be removed before 1.23
  public final void checkExpFails(
      String sql,
      String expected) {
    expr(sql).fails(expected);
  }

  @Deprecated // to be removed before 1.23
  public final void checkWholeExpFails(
      String sql,
      String expected) {
    wholeExpr(sql).fails(expected);
  }

  @Deprecated // to be removed before 1.23
  public final void checkExpType(
      String sql,
      String expected) {
    expr(sql).columnType(expected);
  }

  @Deprecated // to be removed before 1.23
  public final void checkColumnType(
      String sql,
      String expected) {
    sql(sql).columnType(expected);
  }

  @Deprecated // to be removed before 1.23
  public final void checkResultType(
      String sql,
      String expected) {
    sql(sql).type(expected);
  }

  @Deprecated // to be removed before 1.23
  public final void checkIntervalConv(
      String sql,
      String expected) {
    expr(sql).intervalConv(expected);
  }

  @Deprecated // to be removed before 1.23
  protected final void assertExceptionIsThrown(
      String sql,
      String expectedMsgPattern) {
    sql(sql).fails(expectedMsgPattern);
  }

  @Deprecated // to be removed before 1.23
  public void checkCharset(
      String sql,
      Charset expectedCharset) {
    sql(sql).charset(expectedCharset);
  }

  @Deprecated // to be removed before 1.23
  public void checkCollation(
      String sql,
      String expectedCollationName,
      SqlCollation.Coercibility expectedCoercibility) {
    sql(sql).collation(expectedCollationName, expectedCoercibility);
  }

  //~ Inner Interfaces -------------------------------------------------------

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
     *
     * @param sql                SQL statement
     * @param expectedMsgPattern If this parameter is null the query must be
     *                           valid for the test to pass; If this parameter
     *                           is not null the query must be malformed and the
     *                           message given must match the pattern
     */
    void assertExceptionIsThrown(
        String sql,
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
    private final String sql;
    private final boolean query;
    private final boolean whole;

    /** Creates a Sql.
     *
     * @param tester Tester
     * @param sql SQL query or expression
     * @param query True if {@code sql} is a query, false if it is an expression
     * @param whole Whether the failure location is the whole query or
     *              expression
     */
    Sql(SqlTester tester, String sql, boolean query, boolean whole) {
      this.tester = tester;
      this.query = query;
      this.sql = sql;
      this.whole = whole;
    }

    @Deprecated // to be removed before 1.23
    Sql tester(SqlTester tester) {
      return withTester(t -> tester);
    }

    Sql withTester(UnaryOperator<SqlTester> transform) {
      return new Sql(transform.apply(tester), sql, query, whole);
    }

    public Sql sql(String sql) {
      return new Sql(tester, sql, true, false);
    }

    public Sql expr(String sql) {
      return new Sql(tester, sql, false, false);
    }

    public String toSql(boolean withCaret) {
      final String sql2 = withCaret
          ? (whole ? ("^" + sql + "^") : sql)
          : (whole ? sql : sql.replace("^", ""));
      return query ? sql2 : AbstractSqlTester.buildQuery(sql2);
    }

    Sql withExtendedCatalog() {
      return withTester(tester -> EXTENDED_CATALOG_TESTER);
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
      Preconditions.checkArgument(sql.indexOf('^') < 0);
      return new Sql(tester, sql, query, whole);
    }

    Sql ok() {
      tester.assertExceptionIsThrown(toSql(false), null);
      return this;
    }

    /**
     * Checks that a SQL expression gives a particular error.
     */
    Sql fails(@Nonnull String expected) {
      Objects.requireNonNull(expected);
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
      tester.checkResultType(sql, expectedType);
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
      tester.checkColumnType(toSql(false), expectedType);
      return this;
    }

    public Sql monotonic(SqlMonotonicity expectedMonotonicity) {
      tester.checkMonotonic(toSql(false), expectedMonotonicity);
      return this;
    }

    public Sql bindType(final String bindType) {
      tester.check(sql, null, parameterRowType ->
          assertThat(parameterRowType.toString(), is(bindType)),
          result -> { });
      return this;
    }

    /** Removes the carets from the SQL string. Useful if you want to run
     * a test once at a conformance level where it fails, then run it again
     * at a conformance level where it succeeds. */
    public Sql sansCarets() {
      return new Sql(tester, sql.replace("^", ""), true, false);
    }

    public void charset(Charset expectedCharset) {
      tester.checkCharset(sql, expectedCharset);
    }

    public void collation(String expectedCollationName,
        SqlCollation.Coercibility expectedCoercibility) {
      tester.checkCollation(sql, expectedCollationName, expectedCoercibility);
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
      tester.checkIntervalConv(toSql(false), expected);
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
      final UnaryOperator<SqlValidator> after = sqlValidator -> {
        sqlValidator.setIdentifierExpansion(expansion);
        return sqlValidator;
      };
      return withTester(tester -> addTransform(tester, after));
    }

    public Sql withValidatorCallRewrite(boolean rewrite) {
      final UnaryOperator<SqlValidator> after = sqlValidator -> {
        sqlValidator.setCallRewrite(rewrite);
        return sqlValidator;
      };
      return withTester(tester -> addTransform(tester, after));
    }

    public Sql withValidatorColumnReferenceExpansion(boolean expansion) {
      final UnaryOperator<SqlValidator> after = sqlValidator -> {
        sqlValidator.setColumnReferenceExpansion(expansion);
        return sqlValidator;
      };
      return withTester(tester -> addTransform(tester, after));
    }

    public Sql rewritesTo(String expected) {
      tester.checkRewrite(toSql(false), expected);
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
  private static class TesterConfigurationRule implements MethodRule {
    @Override public Statement apply(Statement statement, FrameworkMethod frameworkMethod,
        Object o) {
      return new Statement() {
        @Override public void evaluate() throws Throwable {
          SqlValidatorTestCase tc = (SqlValidatorTestCase) o;
          SqlTester tester = tc.tester;
          WithLex lex = frameworkMethod.getAnnotation(WithLex.class);
          if (lex != null) {
            tester = tester.withLex(lex.value());
          }
          tc.tester = tester;
          statement.evaluate();
        }
      };
    }
  }
}

// End SqlValidatorTestCase.java
