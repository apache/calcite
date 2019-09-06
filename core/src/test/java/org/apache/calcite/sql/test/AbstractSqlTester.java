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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.test.SqlValidatorTestCase;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.calcite.sql.SqlUtil.stripAs;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Abstract implementation of
 * {@link org.apache.calcite.test.SqlValidatorTestCase.Tester}
 * that talks to a mock catalog.
 *
 * <p>This is to implement the default behavior: testing is only against the
 * {@link SqlValidator}.
 */
public abstract class AbstractSqlTester implements SqlTester, AutoCloseable {
  protected final SqlTestFactory factory;

  public AbstractSqlTester(SqlTestFactory factory) {
    this.factory = factory;
  }

  public final SqlTestFactory getFactory() {
    return factory;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This default implementation does nothing.
   */
  public void close() {
    // no resources to release
  }

  public final SqlConformance getConformance() {
    return (SqlConformance) factory.get("conformance");
  }

  public final SqlValidator getValidator() {
    return factory.getValidator();
  }

  public void assertExceptionIsThrown(String sql, String expectedMsgPattern) {
    final SqlValidator validator;
    final SqlNode sqlNode;
    final SqlParserUtil.StringAndPos sap = SqlParserUtil.findPos(sql);
    try {
      sqlNode = parseQuery(sap.sql);
      validator = getValidator();
    } catch (Throwable e) {
      checkParseEx(e, expectedMsgPattern, sap.sql);
      return;
    }

    Throwable thrown = null;
    try {
      validator.validate(sqlNode);
    } catch (Throwable ex) {
      thrown = ex;
    }

    SqlValidatorTestCase.checkEx(thrown, expectedMsgPattern, sap);
  }

  protected void checkParseEx(Throwable e, String expectedMsgPattern, String sql) {
    try {
      throw e;
    } catch (SqlParseException spe) {
      String errMessage = spe.getMessage();
      if (expectedMsgPattern == null) {
        throw new RuntimeException("Error while parsing query:" + sql, spe);
      } else if (errMessage == null
          || !errMessage.matches(expectedMsgPattern)) {
        throw new RuntimeException("Error did not match expected ["
            + expectedMsgPattern + "] while parsing query ["
            + sql + "]", spe);
      }
    } catch (Throwable t) {
      throw new RuntimeException("Error while parsing query: " + sql, t);
    }
  }

  public RelDataType getColumnType(String sql) {
    RelDataType rowType = getResultType(sql);
    final List<RelDataTypeField> fields = rowType.getFieldList();
    assertEquals("expected query to return 1 field", 1, fields.size());
    return fields.get(0).getType();
  }

  public RelDataType getResultType(String sql) {
    SqlValidator validator = getValidator();
    SqlNode n = parseAndValidate(validator, sql);

    return validator.getValidatedNodeType(n);
  }

  public SqlNode parseAndValidate(SqlValidator validator, String sql) {
    if (validator == null) {
      validator = getValidator();
    }
    SqlNode sqlNode;
    try {
      sqlNode = parseQuery(sql);
    } catch (Throwable e) {
      throw new RuntimeException("Error while parsing query: " + sql, e);
    }
    return validator.validate(sqlNode);
  }

  public SqlNode parseQuery(String sql) throws SqlParseException {
    SqlParser parser = factory.createParser(sql);
    return parser.parseQuery();
  }

  public void checkColumnType(String sql, String expected) {
    RelDataType actualType = getColumnType(sql);
    String actual = SqlTests.getTypeString(actualType);
    assertEquals(expected, actual);
  }

  public void checkFieldOrigin(String sql, String fieldOriginList) {
    SqlValidator validator = getValidator();
    SqlNode n = parseAndValidate(validator, sql);
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
    assertEquals(fieldOriginList, buf.toString());
  }

  public void checkResultType(String sql, String expected) {
    RelDataType actualType = getResultType(sql);
    String actual = SqlTests.getTypeString(actualType);
    assertEquals(expected, actual);
  }

  public void checkIntervalConv(String sql, String expected) {
    SqlValidator validator = getValidator();
    final SqlCall n = (SqlCall) parseAndValidate(validator, sql);

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
        (SqlIntervalLiteral.IntervalValue) intervalLiteral.getValue();
    long l =
        interval.getIntervalQualifier().isYearMonth()
            ? SqlParserUtil.intervalToMonths(interval)
            : SqlParserUtil.intervalToMillis(interval);
    String actual = l + "";
    assertEquals(expected, actual);
  }

  public void checkType(String expression, String type) {
    for (String sql : buildQueries(expression)) {
      checkColumnType(sql, type);
    }
  }

  public void checkCollation(
      String expression,
      String expectedCollationName,
      SqlCollation.Coercibility expectedCoercibility) {
    for (String sql : buildQueries(expression)) {
      RelDataType actualType = getColumnType(sql);
      SqlCollation collation = actualType.getCollation();

      assertEquals(
          expectedCollationName, collation.getCollationName());
      assertEquals(expectedCoercibility, collation.getCoercibility());
    }
  }

  public void checkCharset(
      String expression,
      Charset expectedCharset) {
    for (String sql : buildQueries(expression)) {
      RelDataType actualType = getColumnType(sql);
      Charset actualCharset = actualType.getCharset();

      if (!expectedCharset.equals(actualCharset)) {
        fail("\n"
            + "Expected=" + expectedCharset.name() + "\n"
            + "  actual=" + actualCharset.name());
      }
    }
  }

  public SqlTester withQuoting(Quoting quoting) {
    return with("quoting", quoting);
  }

  public SqlTester withQuotedCasing(Casing casing) {
    return with("quotedCasing", casing);
  }

  public SqlTester withUnquotedCasing(Casing casing) {
    return with("unquotedCasing", casing);
  }

  public SqlTester withCaseSensitive(boolean sensitive) {
    return with("caseSensitive", sensitive);
  }

  public SqlTester withLex(Lex lex) {
    return withQuoting(lex.quoting)
        .withCaseSensitive(lex.caseSensitive)
        .withQuotedCasing(lex.quotedCasing)
        .withUnquotedCasing(lex.unquotedCasing);
  }

  public SqlTester withConformance(SqlConformance conformance) {
    if (conformance == null) {
      conformance = SqlConformanceEnum.DEFAULT;
    }
    final SqlTester tester = with("conformance", conformance);
    if (conformance instanceof SqlConformanceEnum) {
      return tester
          .withConnectionFactory(
              CalciteAssert.EMPTY_CONNECTION_FACTORY
                  .with(CalciteConnectionProperty.CONFORMANCE, conformance));
    } else {
      return tester;
    }
  }

  public SqlTester enableTypeCoercion(boolean enabled) {
    return with("enableTypeCoercion", enabled);
  }

  public SqlTester withOperatorTable(SqlOperatorTable operatorTable) {
    return with("operatorTable", operatorTable);
  }

  public SqlTester withConnectionFactory(
      CalciteAssert.ConnectionFactory connectionFactory) {
    return with("connectionFactory", connectionFactory);
  }

  protected final SqlTester with(final String name, final Object value) {
    return with(factory.with(name, value));
  }

  protected abstract SqlTester with(SqlTestFactory factory);

  // SqlTester methods

  public void setFor(
      SqlOperator operator,
      VmName... unimplementedVmNames) {
    // do nothing
  }

  public void checkAgg(
      String expr,
      String[] inputValues,
      Object result,
      double delta) {
    String query =
        SqlTests.generateAggQuery(expr, inputValues);
    check(query, SqlTests.ANY_TYPE_CHECKER, result, delta);
  }

  public void checkAggWithMultipleArgs(
      String expr,
      String[][] inputValues,
      Object result,
      double delta) {
    String query =
        SqlTests.generateAggQueryWithMultipleArgs(expr, inputValues);
    check(query, SqlTests.ANY_TYPE_CHECKER, result, delta);
  }

  public void checkWinAgg(
      String expr,
      String[] inputValues,
      String windowSpec,
      String type,
      Object result,
      double delta) {
    String query =
        SqlTests.generateWinAggQuery(
            expr, windowSpec, inputValues);
    check(query, SqlTests.ANY_TYPE_CHECKER, result, delta);
  }

  public void checkScalar(
      String expression,
      Object result,
      String resultType) {
    checkType(expression, resultType);
    for (String sql : buildQueries(expression)) {
      check(sql, SqlTests.ANY_TYPE_CHECKER, result, 0);
    }
  }

  public void checkScalarExact(
      String expression,
      String result) {
    for (String sql : buildQueries(expression)) {
      check(sql, SqlTests.INTEGER_TYPE_CHECKER, result, 0);
    }
  }

  public void checkScalarExact(
      String expression,
      String expectedType,
      String result) {
    for (String sql : buildQueries(expression)) {
      TypeChecker typeChecker =
          new SqlTests.StringTypeChecker(expectedType);
      check(sql, typeChecker, result, 0);
    }
  }

  public void checkScalarApprox(
      String expression,
      String expectedType,
      double expectedResult,
      double delta) {
    for (String sql : buildQueries(expression)) {
      TypeChecker typeChecker =
          new SqlTests.StringTypeChecker(expectedType);
      check(sql, typeChecker, expectedResult, delta);
    }
  }

  public void checkBoolean(
      String expression,
      Boolean result) {
    for (String sql : buildQueries(expression)) {
      if (null == result) {
        checkNull(expression);
      } else {
        check(
            sql,
            SqlTests.BOOLEAN_TYPE_CHECKER,
            result.toString(),
            0);
      }
    }
  }

  public void checkString(
      String expression,
      String result,
      String expectedType) {
    for (String sql : buildQueries(expression)) {
      TypeChecker typeChecker =
          new SqlTests.StringTypeChecker(expectedType);
      check(sql, typeChecker, result, 0);
    }
  }

  public void checkNull(String expression) {
    for (String sql : buildQueries(expression)) {
      check(sql, SqlTests.ANY_TYPE_CHECKER, null, 0);
    }
  }

  public final void check(
      String query,
      TypeChecker typeChecker,
      Object result,
      double delta) {
    check(query, typeChecker, SqlTests.ANY_PARAMETER_CHECKER,
        SqlTests.createChecker(result, delta));
  }

  public void check(String query, TypeChecker typeChecker,
      ParameterChecker parameterChecker, ResultChecker resultChecker) {
    // This implementation does NOT check the result!
    // All it does is check the return type.

    if (typeChecker == null) {
      // Parse and validate. There should be no errors.
      Util.discard(getResultType(query));
    } else {
      // Parse and validate. There should be no errors.
      // There must be 1 column. Get its type.
      RelDataType actualType = getColumnType(query);

      // Check result type.
      typeChecker.checkType(actualType);
    }

    SqlValidator validator = getValidator();
    SqlNode n = parseAndValidate(validator, query);
    final RelDataType parameterRowType = validator.getParameterRowType(n);
    parameterChecker.checkParameters(parameterRowType);
  }

  public void checkMonotonic(String query,
      SqlMonotonicity expectedMonotonicity) {
    SqlValidator validator = getValidator();
    SqlNode n = parseAndValidate(validator, query);
    final RelDataType rowType = validator.getValidatedNodeType(n);
    final SqlValidatorNamespace selectNamespace = validator.getNamespace(n);
    final String field0 = rowType.getFieldList().get(0).getName();
    final SqlMonotonicity monotonicity =
        selectNamespace.getMonotonicity(field0);
    assertThat(monotonicity, equalTo(expectedMonotonicity));
  }

  public void checkRewrite(
      SqlValidator validator,
      String query,
      String expectedRewrite) {
    SqlNode rewrittenNode = parseAndValidate(validator, query);
    String actualRewrite =
        rewrittenNode.toSqlString(AnsiSqlDialect.DEFAULT, false).getSql();
    TestUtil.assertEqualsVerbose(expectedRewrite, Util.toLinux(actualRewrite));
  }

  public void checkFails(
      String expression,
      String expectedError,
      boolean runtime) {
    if (runtime) {
      // We need to test that the expression fails at runtime.
      // Ironically, that means that it must succeed at prepare time.
      SqlValidator validator = getValidator();
      final String sql = buildQuery(expression);
      SqlNode n = parseAndValidate(validator, sql);
      assertNotNull(n);
    } else {
      checkQueryFails(buildQuery(expression), expectedError);
    }
  }

  public void checkQueryFails(String sql, String expectedError) {
    assertExceptionIsThrown(sql, expectedError);
  }

  public void checkQuery(String sql) {
    assertExceptionIsThrown(sql, null);
  }

  public SqlMonotonicity getMonotonicity(String sql) {
    final SqlValidator validator = getValidator();
    final SqlNode node = parseAndValidate(validator, sql);
    final SqlSelect select = (SqlSelect) node;
    final SqlNode selectItem0 = select.getSelectList().get(0);
    final SqlValidatorScope scope = validator.getSelectScope(select);
    return selectItem0.getMonotonicity(scope);
  }

  public static String buildQuery(String expression) {
    return "values (" + expression + ")";
  }

  public static String buildQueryAgg(String expression) {
    return "select " + expression + " from (values (1)) as t(x) group by x";
  }

  /**
   * Builds a query that extracts all literals as columns in an underlying
   * select.
   *
   * <p>For example,</p>
   *
   * <blockquote>{@code 1 < 5}</blockquote>
   *
   * <p>becomes</p>
   *
   * <blockquote>{@code SELECT p0 < p1
   * FROM (VALUES (1, 5)) AS t(p0, p1)}</blockquote>
   *
   * <p>Null literals don't have enough type information to be extracted.
   * We push down {@code CAST(NULL AS type)} but raw nulls such as
   * {@code CASE 1 WHEN 2 THEN 'a' ELSE NULL END} are left as is.</p>
   *
   * @param expression Scalar expression
   * @return Query that evaluates a scalar expression
   */
  protected String buildQuery2(String expression) {
    // "values (1 < 5)"
    // becomes
    // "select p0 < p1 from (values (1, 5)) as t(p0, p1)"
    SqlNode x;
    final String sql = "values (" + expression + ")";
    try {
      x = parseQuery(sql);
    } catch (SqlParseException e) {
      throw TestUtil.rethrow(e);
    }
    final Collection<SqlNode> literalSet = new LinkedHashSet<>();
    x.accept(
        new SqlShuttle() {
          private final List<SqlOperator> ops =
              ImmutableList.of(
                  SqlStdOperatorTable.LITERAL_CHAIN,
                  SqlStdOperatorTable.LOCALTIME,
                  SqlStdOperatorTable.LOCALTIMESTAMP,
                  SqlStdOperatorTable.CURRENT_TIME,
                  SqlStdOperatorTable.CURRENT_TIMESTAMP);

          @Override public SqlNode visit(SqlLiteral literal) {
            if (!isNull(literal)
                && literal.getTypeName() != SqlTypeName.SYMBOL) {
              literalSet.add(literal);
            }
            return literal;
          }

          @Override public SqlNode visit(SqlCall call) {
            SqlOperator operator = call.getOperator();
            if (operator instanceof SqlUnresolvedFunction) {
              final SqlUnresolvedFunction unresolvedFunction = (SqlUnresolvedFunction) operator;
              final SqlOperator lookup = SqlValidatorUtil.lookupSqlFunctionByID(
                  SqlStdOperatorTable.instance(),
                  unresolvedFunction.getSqlIdentifier(),
                  unresolvedFunction.getFunctionType());
              if (lookup != null) {
                operator = lookup;
                final SqlNode[] operands = call.getOperandList().toArray(SqlNode.EMPTY_ARRAY);
                call = operator.createCall(
                    call.getFunctionQuantifier(),
                    call.getParserPosition(),
                    operands);
              }
            }
            if (operator == SqlStdOperatorTable.CAST
                && isNull(call.operand(0))) {
              literalSet.add(call);
              return call;
            } else if (ops.contains(operator)) {
              // "Argument to function 'LOCALTIME' must be a
              // literal"
              return call;
            } else {
              return super.visit(call);
            }
          }

          private boolean isNull(SqlNode sqlNode) {
            return sqlNode instanceof SqlLiteral
                && ((SqlLiteral) sqlNode).getTypeName()
                == SqlTypeName.NULL;
          }
        });
    final List<SqlNode> nodes = new ArrayList<>(literalSet);
    nodes.sort((o1, o2) -> {
      final SqlParserPos pos0 = o1.getParserPosition();
      final SqlParserPos pos1 = o2.getParserPosition();
      int c = -Utilities.compare(pos0.getLineNum(), pos1.getLineNum());
      if (c != 0) {
        return c;
      }
      return -Utilities.compare(pos0.getColumnNum(), pos1.getColumnNum());
    });
    String sql2 = sql;
    final List<Pair<String, String>> values = new ArrayList<>();
    int p = 0;
    for (SqlNode literal : nodes) {
      final SqlParserPos pos = literal.getParserPosition();
      final int start =
          SqlParserUtil.lineColToIndex(
              sql, pos.getLineNum(), pos.getColumnNum());
      final int end =
          SqlParserUtil.lineColToIndex(
              sql,
              pos.getEndLineNum(),
              pos.getEndColumnNum()) + 1;
      String param = "p" + (p++);
      values.add(Pair.of(sql2.substring(start, end), param));
      sql2 = sql2.substring(0, start)
          + param
          + sql2.substring(end);
    }
    if (values.isEmpty()) {
      values.add(Pair.of("1", "p0"));
    }
    return "select "
        + sql2.substring("values (".length(), sql2.length() - 1)
        + " from (values ("
        + Util.commaList(Pair.left(values))
        + ")) as t("
        + Util.commaList(Pair.right(values))
        + ")";
  }

  /**
   * Converts a scalar expression into a list of SQL queries that
   * evaluate it.
   *
   * @param expression Scalar expression
   * @return List of queries that evaluate an expression
   */
  private Iterable<String> buildQueries(final String expression) {
    // Why an explicit iterable rather than a list? If there is
    // a syntax error in the expression, the calling code discovers it
    // before we try to parse it to do substitutions on the parse tree.
    return () -> new Iterator<String>() {
      int i = 0;

      public void remove() {
        throw new UnsupportedOperationException();
      }

      public String next() {
        switch (i++) {
        case 0:
          return buildQuery(expression);
        case 1:
          return buildQuery2(expression);
        default:
          throw new NoSuchElementException();
        }
      }

      public boolean hasNext() {
        return i < 2;
      }
    };
  }

}

// End AbstractSqlTester.java
