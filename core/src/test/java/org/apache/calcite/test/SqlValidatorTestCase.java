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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.test.DefaultSqlTestFactory;
import org.apache.calcite.sql.test.DelegatingSqlTestFactory;
import org.apache.calcite.sql.test.SqlTestFactory;
import org.apache.calcite.sql.test.SqlTester;
import org.apache.calcite.sql.test.SqlTesterImpl;
import org.apache.calcite.sql.test.SqlTests;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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

  private static final Pattern LINE_COL_PATTERN =
      Pattern.compile("At line ([0-9]+), column ([0-9]+)");

  private static final Pattern LINE_COL_TWICE_PATTERN =
      Pattern.compile(
          "(?s)From line ([0-9]+), column ([0-9]+) to line ([0-9]+), column ([0-9]+): (.*)");

  private static final SqlTestFactory EXTENDED_TEST_FACTORY =
      new DelegatingSqlTestFactory(DefaultSqlTestFactory.INSTANCE) {
        @Override public MockCatalogReader createCatalogReader(
            SqlTestFactory factory, JavaTypeFactory typeFactory) {
          return super.createCatalogReader(this, typeFactory).init2();
        }
      };

  static final SqlTesterImpl EXTENDED_CATALOG_TESTER =
      new SqlTesterImpl(EXTENDED_TEST_FACTORY);

  static final SqlTesterImpl EXTENDED_CATALOG_TESTER_2003 =
      new SqlTesterImpl(EXTENDED_TEST_FACTORY)
          .withConformance(SqlConformanceEnum.PRAGMATIC_2003);

  static final SqlTesterImpl EXTENDED_CATALOG_TESTER_LENIENT =
      new SqlTesterImpl(EXTENDED_TEST_FACTORY)
          .withConformance(SqlConformanceEnum.LENIENT);

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
    return new SqlTesterImpl(DefaultSqlTestFactory.INSTANCE);
  }

  public final Sql sql(String sql) {
    return new Sql(tester, sql);
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

  public void check(String sql) {
    sql(sql).ok();
  }

  public void checkExp(String sql) {
    tester.assertExceptionIsThrown(
        SqlTesterImpl.buildQuery(sql),
        null);
  }

  /**
   * Checks that a SQL query gives a particular error, or succeeds if {@code
   * expected} is null.
   */
  public final void checkFails(
      String sql,
      String expected) {
    sql(sql).fails(expected);
  }

  /**
   * Checks that a SQL expression gives a particular error.
   */
  public final void checkExpFails(
      String sql,
      String expected) {
    tester.assertExceptionIsThrown(
        SqlTesterImpl.buildQuery(sql),
        expected);
  }

  /**
   * Checks that a SQL expression gives a particular error, and that the
   * location of the error is the whole expression.
   */
  public final void checkWholeExpFails(
      String sql,
      String expected) {
    assert sql.indexOf('^') < 0;
    checkExpFails("^" + sql + "^", expected);
  }

  public final void checkExpType(
      String sql,
      String expected) {
    checkColumnType(
        SqlTesterImpl.buildQuery(sql),
        expected);
  }

  /**
   * Checks that a query returns a single column, and that the column has the
   * expected type. For example,
   *
   * <blockquote><code>checkColumnType("SELECT empno FROM Emp", "INTEGER NOT
   * NULL");</code></blockquote>
   *
   * @param sql      Query
   * @param expected Expected type, including nullability
   */
  public final void checkColumnType(
      String sql,
      String expected) {
    tester.checkColumnType(sql, expected);
  }

  /**
   * Checks that a query returns a row of the expected type. For example,
   *
   * <blockquote><code>checkResultType("select empno, name from emp","{EMPNO
   * INTEGER NOT NULL, NAME VARCHAR(10) NOT NULL}");</code></blockquote>
   *
   * @param sql      Query
   * @param expected Expected row type
   */
  public final void checkResultType(
      String sql,
      String expected) {
    tester.checkResultType(sql, expected);
  }

  /**
   * Checks that the first column returned by a query has the expected type.
   * For example,
   *
   * <blockquote><code>checkQueryType("SELECT empno FROM Emp", "INTEGER NOT
   * NULL");</code></blockquote>
   *
   * @param sql      Query
   * @param expected Expected type, including nullability
   */
  public final void checkIntervalConv(
      String sql,
      String expected) {
    tester.checkIntervalConv(
        SqlTesterImpl.buildQuery(sql),
        expected);
  }

  protected final void assertExceptionIsThrown(
      String sql,
      String expectedMsgPattern) {
    assert expectedMsgPattern != null;
    tester.assertExceptionIsThrown(sql, expectedMsgPattern);
  }

  public void checkCharset(
      String sql,
      Charset expectedCharset) {
    tester.checkCharset(sql, expectedCharset);
  }

  public void checkCollation(
      String sql,
      String expectedCollationName,
      SqlCollation.Coercibility expectedCoercibility) {
    tester.checkCollation(sql, expectedCollationName, expectedCoercibility);
  }

  /**
   * Checks whether an exception matches the expected pattern. If <code>
   * sap</code> contains an error location, checks this too.
   *
   * @param ex                 Exception thrown
   * @param expectedMsgPattern Expected pattern
   * @param sap                Query and (optional) position in query
   */
  public static void checkEx(
      Throwable ex,
      String expectedMsgPattern,
      SqlParserUtil.StringAndPos sap) {
    if (null == ex) {
      if (expectedMsgPattern == null) {
        // No error expected, and no error happened.
        return;
      } else {
        throw new AssertionError("Expected query to throw exception, "
            + "but it did not; query [" + sap.sql
            + "]; expected [" + expectedMsgPattern + "]");
      }
    }
    Throwable actualException = ex;
    String actualMessage = actualException.getMessage();
    int actualLine = -1;
    int actualColumn = -1;
    int actualEndLine = 100;
    int actualEndColumn = 99;

    // Search for an CalciteContextException somewhere in the stack.
    CalciteContextException ece = null;
    for (Throwable x = ex; x != null; x = x.getCause()) {
      if (x instanceof CalciteContextException) {
        ece = (CalciteContextException) x;
        break;
      }
      if (x.getCause() == x) {
        break;
      }
    }

    // Search for a SqlParseException -- with its position set -- somewhere
    // in the stack.
    SqlParseException spe = null;
    for (Throwable x = ex; x != null; x = x.getCause()) {
      if ((x instanceof SqlParseException)
          && (((SqlParseException) x).getPos() != null)) {
        spe = (SqlParseException) x;
        break;
      }
      if (x.getCause() == x) {
        break;
      }
    }

    if (ece != null) {
      actualLine = ece.getPosLine();
      actualColumn = ece.getPosColumn();
      actualEndLine = ece.getEndPosLine();
      actualEndColumn = ece.getEndPosColumn();
      if (ece.getCause() != null) {
        actualException = ece.getCause();
        actualMessage = actualException.getMessage();
      }
    } else if (spe != null) {
      actualLine = spe.getPos().getLineNum();
      actualColumn = spe.getPos().getColumnNum();
      actualEndLine = spe.getPos().getEndLineNum();
      actualEndColumn = spe.getPos().getEndColumnNum();
      if (spe.getCause() != null) {
        actualException = spe.getCause();
        actualMessage = actualException.getMessage();
      }
    } else {
      final String message = ex.getMessage();
      if (message != null) {
        Matcher matcher = LINE_COL_TWICE_PATTERN.matcher(message);
        if (matcher.matches()) {
          actualLine = Integer.parseInt(matcher.group(1));
          actualColumn = Integer.parseInt(matcher.group(2));
          actualEndLine = Integer.parseInt(matcher.group(3));
          actualEndColumn = Integer.parseInt(matcher.group(4));
          actualMessage = matcher.group(5);
        } else {
          matcher = LINE_COL_PATTERN.matcher(message);
          if (matcher.matches()) {
            actualLine = Integer.parseInt(matcher.group(1));
            actualColumn = Integer.parseInt(matcher.group(2));
          } else {
            if (actualMessage.toString().matches(expectedMsgPattern)) {
              return;
            }
          }
        }
      }
    }

    if (null == expectedMsgPattern) {
      actualException.printStackTrace();
      fail("Validator threw unexpected exception"
          + "; query [" + sap.sql
          + "]; exception [" + actualMessage
          + "]; class [" + actualException.getClass()
          + "]; pos [line " + actualLine
          + " col " + actualColumn
          + " thru line " + actualLine
          + " col " + actualColumn + "]");
    }

    String sqlWithCarets;
    if (actualColumn <= 0
        || actualLine <= 0
        || actualEndColumn <= 0
        || actualEndLine <= 0) {
      if (sap.pos != null) {
        AssertionError e =
            new AssertionError("Expected error to have position,"
                + " but actual error did not: "
                + " actual pos [line " + actualLine
                + " col " + actualColumn
                + " thru line " + actualEndLine + " col "
                + actualEndColumn + "]");
        e.initCause(actualException);
        throw e;
      }
      sqlWithCarets = sap.sql;
    } else {
      sqlWithCarets =
          SqlParserUtil.addCarets(
              sap.sql,
              actualLine,
              actualColumn,
              actualEndLine,
              actualEndColumn + 1);
      if (sap.pos == null) {
        throw new AssertionError("Actual error had a position, but expected "
            + "error did not. Add error position carets to sql:\n"
            + sqlWithCarets);
      }
    }

    if (actualMessage != null) {
      actualMessage = Util.toLinux(actualMessage);
    }

    if (actualMessage == null
        || !actualMessage.matches(expectedMsgPattern)) {
      actualException.printStackTrace();
      final String actualJavaRegexp =
          (actualMessage == null)
              ? "null"
              : TestUtil.quoteForJava(
                  TestUtil.quotePattern(actualMessage));
      fail("Validator threw different "
          + "exception than expected; query [" + sap.sql
          + "];\n"
          + " expected pattern [" + expectedMsgPattern
          + "];\n"
          + " actual [" + actualMessage
          + "];\n"
          + " actual as java regexp [" + actualJavaRegexp
          + "]; pos [" + actualLine
          + " col " + actualColumn
          + " thru line " + actualEndLine
          + " col " + actualEndColumn
          + "]; sql [" + sqlWithCarets + "]");
    } else if (sap.pos != null
        && (actualLine != sap.pos.getLineNum()
            || actualColumn != sap.pos.getColumnNum()
            || actualEndLine != sap.pos.getEndLineNum()
            || actualEndColumn != sap.pos.getEndColumnNum())) {
      fail("Validator threw expected "
          + "exception [" + actualMessage
          + "];\nbut at pos [line " + actualLine
          + " col " + actualColumn
          + " thru line " + actualEndLine
          + " col " + actualEndColumn
          + "];\nsql [" + sqlWithCarets + "]");
    }
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
     * @param validator       validator to use; null for default
     * @param query           query to test
     * @param expectedRewrite expected SQL text after rewrite and unparse
     */
    void checkRewrite(
        SqlValidator validator,
        String query,
        String expectedRewrite);

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

    Sql(SqlTester tester, String sql) {
      this.tester = tester;
      this.sql = sql;
    }

    Sql tester(SqlTester tester) {
      return new Sql(tester, sql);
    }

    public Sql sql(String sql) {
      return new Sql(tester, sql);
    }

    Sql withExtendedCatalog() {
      return tester(EXTENDED_CATALOG_TESTER);
    }

    Sql withExtendedCatalog2003() {
      return tester(EXTENDED_CATALOG_TESTER_2003);
    }

    Sql withExtendedCatalogLenient() {
      return tester(EXTENDED_CATALOG_TESTER_LENIENT);
    }

    Sql ok() {
      tester.assertExceptionIsThrown(sql, null);
      return this;
    }

    Sql fails(String expected) {
      tester.assertExceptionIsThrown(sql, expected);
      return this;
    }

    Sql failsIf(boolean b, String expected) {
      if (b) {
        fails(expected);
      } else {
        ok();
      }
      return this;
    }

    public Sql type(String expectedType) {
      tester.checkResultType(sql, expectedType);
      return this;
    }

    public Sql columnType(String expectedType) {
      tester.checkColumnType(sql, expectedType);
      return this;
    }

    public Sql monotonic(SqlMonotonicity expectedMonotonicity) {
      tester.checkMonotonic(sql, expectedMonotonicity);
      return this;
    }

    public Sql bindType(final String bindType) {
      tester.check(sql, null,
          new SqlTester.ParameterChecker() {
            public void checkParameters(RelDataType parameterRowType) {
              assertThat(parameterRowType.toString(), is(bindType));
            }
          },
          SqlTests.ANY_RESULT_CHECKER);
      return this;
    }

    /** Removes the carets from the SQL string. Useful if you want to run
     * a test once at a conformance level where it fails, then run it again
     * at a conformance level where it succeeds. */
    public Sql sansCarets() {
      return new Sql(tester, sql.replace("^", ""));
    }
  }
}

// End SqlValidatorTestCase.java
