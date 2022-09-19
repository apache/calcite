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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.parser.StringAndPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.test.DiffRepository;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.calcite.test.Matchers.relIsValid;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import static java.util.Objects.requireNonNull;

/**
 * Abstract implementation of {@link SqlTester}
 * that talks to a mock catalog.
 *
 * <p>This is to implement the default behavior: testing is only against the
 * {@link SqlValidator}.
 */
public abstract class AbstractSqlTester implements SqlTester, AutoCloseable {
  private  static final String NL = System.getProperty("line.separator");

  public AbstractSqlTester() {
  }

  /**
   * {@inheritDoc}
   *
   * <p>This default implementation does nothing.
   */
  @Override public void close() {
    // no resources to release
  }

  @Override public void assertExceptionIsThrown(SqlTestFactory factory,
      StringAndPos sap, @Nullable String expectedMsgPattern) {
    final SqlNode sqlNode;
    try {
      sqlNode = parseQuery(factory, sap.sql);
    } catch (Throwable e) {
      SqlTests.checkEx(e, expectedMsgPattern, sap, SqlTests.Stage.PARSE);
      return;
    }

    final SqlValidator validator = factory.createValidator();
    Throwable thrown = null;
    try {
      validator.validate(sqlNode);
    } catch (Throwable ex) {
      thrown = ex;
    }

    SqlTests.checkEx(thrown, expectedMsgPattern, sap, SqlTests.Stage.VALIDATE);
  }

  protected void checkParseEx(Throwable e, @Nullable String expectedMsgPattern,
      StringAndPos sap) {
    try {
      throw e;
    } catch (SqlParseException spe) {
      String errMessage = spe.getMessage();
      if (expectedMsgPattern == null) {
        throw new RuntimeException("Error while parsing query:" + sap, spe);
      } else if (errMessage == null
          || !Util.toLinux(errMessage).matches(expectedMsgPattern)) {
        throw new RuntimeException("Error did not match expected ["
            + expectedMsgPattern + "] while parsing query ["
            + sap + "]", spe);
      }
    } catch (Throwable t) {
      throw new RuntimeException("Error while parsing query: " + sap, t);
    }
  }

  @Override public RelDataType getColumnType(SqlTestFactory factory,
      String sql) {
    return validateAndApply(factory, StringAndPos.of(sql),
        (sql1, validator, n) -> {
          final RelDataType rowType =
              validator.getValidatedNodeType(n);
          final List<RelDataTypeField> fields = rowType.getFieldList();
          assertThat("expected query to return 1 field", fields.size(), is(1));
          return fields.get(0).getType();
        });
  }

  @Override public RelDataType getResultType(SqlTestFactory factory,
      String sql) {
    return validateAndApply(factory, StringAndPos.of(sql),
        (sql1, validator, n) ->
            validator.getValidatedNodeType(n));
  }

  Pair<SqlValidator, SqlNode> parseAndValidate(SqlTestFactory factory,
      String sql) {
    SqlNode sqlNode;
    try {
      sqlNode = parseQuery(factory, sql);
    } catch (Throwable e) {
      throw new RuntimeException("Error while parsing query: " + sql, e);
    }
    SqlValidator validator = factory.createValidator();
    return Pair.of(validator, validator.validate(sqlNode));
  }

  @Override public SqlNode parseQuery(SqlTestFactory factory, String sql)
      throws SqlParseException {
    SqlParser parser = factory.createParser(sql);
    return parser.parseQuery();
  }

  @Override public SqlNode parseExpression(SqlTestFactory factory,
      String expr) throws SqlParseException {
    SqlParser parser = factory.createParser(expr);
    return parser.parseExpression();
  }

  @Override public void checkColumnType(SqlTestFactory factory, String sql,
      String expected) {
    validateAndThen(factory, StringAndPos.of(sql),
        checkColumnTypeAction(is(expected)));
  }

  private static ValidatedNodeConsumer checkColumnTypeAction(
      Matcher<String> matcher) {
    return (sql1, validator, validatedNode) -> {
      final RelDataType rowType =
          validator.getValidatedNodeType(validatedNode);
      final List<RelDataTypeField> fields = rowType.getFieldList();
      assertEquals(1, fields.size(), "expected query to return 1 field");
      final RelDataType actualType = fields.get(0).getType();
      String actual = SqlTests.getTypeString(actualType);
      assertThat(actual, matcher);
    };
  }

  // SqlTester methods

  @Override public void setFor(
      SqlOperator operator,
      VmName... unimplementedVmNames) {
    // do nothing
  }

  @Override public void checkAgg(SqlTestFactory factory,
      String expr,
      String[] inputValues,
      ResultChecker resultChecker) {
    String query =
        SqlTests.generateAggQuery(expr, inputValues);
    check(factory, query, SqlTests.ANY_TYPE_CHECKER, resultChecker);
  }

  @Override public void checkWinAgg(SqlTestFactory factory,
      String expr,
      String[] inputValues,
      String windowSpec,
      String type,
      ResultChecker resultChecker) {
    String query =
        SqlTests.generateWinAggQuery(
            expr, windowSpec, inputValues);
    check(factory, query, SqlTests.ANY_TYPE_CHECKER, resultChecker);
  }

  @Override public void check(SqlTestFactory factory,
      String query, TypeChecker typeChecker,
      ParameterChecker parameterChecker, ResultChecker resultChecker) {
    // This implementation does NOT check the result!
    // All it does is check the return type.
    requireNonNull(typeChecker, "typeChecker");
    requireNonNull(parameterChecker, "parameterChecker");
    requireNonNull(resultChecker, "resultChecker");

    // Parse and validate. There should be no errors.
    // There must be 1 column. Get its type.
    RelDataType actualType = getColumnType(factory, query);

    // Check result type.
    typeChecker.checkType(actualType);

    Pair<SqlValidator, SqlNode> p = parseAndValidate(factory, query);
    SqlValidator validator = requireNonNull(p.left);
    SqlNode n = requireNonNull(p.right);
    final RelDataType parameterRowType = validator.getParameterRowType(n);
    parameterChecker.checkParameters(parameterRowType);
  }

  @Override public void validateAndThen(SqlTestFactory factory,
      StringAndPos sap, ValidatedNodeConsumer consumer) {
    Pair<SqlValidator, SqlNode> p = parseAndValidate(factory, sap.sql);
    SqlValidator validator = requireNonNull(p.left);
    SqlNode rewrittenNode = requireNonNull(p.right);
    consumer.accept(sap, validator, rewrittenNode);
  }

  @Override public <R> R validateAndApply(SqlTestFactory factory,
      StringAndPos sap, ValidatedNodeFunction<R> function) {
    Pair<SqlValidator, SqlNode> p = parseAndValidate(factory, sap.sql);
    SqlValidator validator = requireNonNull(p.left);
    SqlNode rewrittenNode = requireNonNull(p.right);
    return function.apply(sap, validator, rewrittenNode);
  }

  @Override public void checkFails(SqlTestFactory factory, StringAndPos sap,
      String expectedError, boolean runtime) {
    if (runtime) {
      // We need to test that the expression fails at runtime.
      // Ironically, that means that it must succeed at prepare time.
      final String sql = buildQuery(sap.addCarets());
      Pair<SqlValidator, SqlNode> p = parseAndValidate(factory, sql);
      SqlNode n = p.right;
      assertNotNull(n);
    } else {
      StringAndPos sap1 = StringAndPos.of(buildQuery(sap.addCarets()));
      checkQueryFails(factory, sap1, expectedError);
    }
  }

  @Override public void checkQueryFails(SqlTestFactory factory,
      StringAndPos sap, String expectedError) {
    assertExceptionIsThrown(factory, sap, expectedError);
  }

  @Override public void checkAggFails(SqlTestFactory factory,
      String expr,
      String[] inputValues,
      String expectedError,
      boolean runtime) {
    final String sql =
        SqlTests.generateAggQuery(expr, inputValues);
    if (runtime) {
      Pair<SqlValidator, SqlNode> p = parseAndValidate(factory, sql);
      SqlNode n = p.right;
      assertNotNull(n);
    } else {
      checkQueryFails(factory, StringAndPos.of(sql), expectedError);
    }
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
   * @param factory Test factory
   * @param expression Scalar expression
   * @return Query that evaluates a scalar expression
   */
  protected String buildQuery2(SqlTestFactory factory, String expression) {
    if (expression.matches("(?i).*percentile_(cont|disc).*")) {
      // PERCENTILE_CONT requires its argument to be a literal,
      // so converting its argument to a column will cause false errors.
      return buildQuery(expression);
    }
    // "values (1 < 5)"
    // becomes
    // "select p0 < p1 from (values (1, 5)) as t(p0, p1)"
    SqlNode x;
    final String sql = "values (" + expression + ")";
    try {
      x = parseQuery(factory, sql);
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
                call = operator.createCall(call.getFunctionQuantifier(),
                    call.getParserPosition(), call.getOperandList());
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
      String param = "p" + p++;
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

  @Override public void forEachQuery(SqlTestFactory factory,
      String expression, Consumer<String> consumer) {
    // Why not return a list? If there is a syntax error in the expression, the
    // consumer will discover it before we try to parse it to do substitutions
    // on the parse tree.
    consumer.accept("values (" + expression + ")");
    consumer.accept(buildQuery2(factory, expression));
  }

  @Override public void assertConvertsTo(SqlTestFactory factory,
      DiffRepository diffRepos,
      String sql,
      String plan,
      boolean trim,
      boolean expression,
      boolean decorrelate) {
    if (expression) {
      assertExprConvertsTo(factory, diffRepos, sql, plan);
    } else {
      assertSqlConvertsTo(factory, diffRepos, sql, plan, trim, decorrelate);
    }
  }

  private void assertExprConvertsTo(SqlTestFactory factory,
      DiffRepository diffRepos, String expr, String plan) {
    String expr2 = diffRepos.expand("sql", expr);
    RexNode rex = convertExprToRex(factory, expr2);
    assertNotNull(rex);
    // NOTE jvs 28-Mar-2006:  insert leading newline so
    // that plans come out nicely stacked instead of first
    // line immediately after CDATA start
    String actual = NL + rex + NL;
    diffRepos.assertEquals("plan", plan, actual);
  }

  private void assertSqlConvertsTo(SqlTestFactory factory,
      DiffRepository diffRepos, String sql, String plan,
      boolean trim,
      boolean decorrelate) {
    String sql2 = diffRepos.expand("sql", sql);
    final Pair<SqlValidator, RelRoot> pair =
        convertSqlToRel2(factory, sql2, decorrelate, trim);
    final RelRoot root = requireNonNull(pair.right);
    final SqlValidator validator = requireNonNull(pair.left);
    RelNode rel = root.project();

    assertNotNull(rel);
    assertThat(rel, relIsValid());

    if (trim) {
      final RelBuilder relBuilder =
          RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
      final RelFieldTrimmer trimmer =
          createFieldTrimmer(validator, relBuilder);
      rel = trimmer.trim(rel);
      assertNotNull(rel);
      assertThat(rel, relIsValid());
    }

    // NOTE jvs 28-Mar-2006:  insert leading newline so
    // that plans come out nicely stacked instead of first
    // line immediately after CDATA start
    String actual = NL + RelOptUtil.toString(rel);
    diffRepos.assertEquals("plan", plan, actual);
  }

  private RexNode convertExprToRex(SqlTestFactory factory, String expr) {
    requireNonNull(expr, "expr");
    final SqlNode sqlQuery;
    try {
      sqlQuery = parseExpression(factory, expr);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }

    final SqlToRelConverter converter = factory.createSqlToRelConverter();
    final SqlValidator validator = requireNonNull(converter.validator);
    final SqlNode validatedQuery = validator.validate(sqlQuery);
    return converter.convertExpression(validatedQuery);
  }

  @Override public Pair<SqlValidator, RelRoot> convertSqlToRel2(
      SqlTestFactory factory, String sql, boolean decorrelate,
      boolean trim) {
    requireNonNull(sql, "sql");
    final SqlNode sqlQuery;
    try {
      sqlQuery = parseQuery(factory, sql);
    } catch (RuntimeException | Error e) {
      throw e;
    } catch (Exception e) {
      throw TestUtil.rethrow(e);
    }
    final SqlToRelConverter converter = factory.createSqlToRelConverter();
    final SqlValidator validator = requireNonNull(converter.validator);

    final SqlNode validatedQuery = validator.validate(sqlQuery);
    RelRoot root =
        converter.convertQuery(validatedQuery, false, true);
    requireNonNull(root, "root");
    if (decorrelate || trim) {
      root = root.withRel(converter.flattenTypes(root.rel, true));
    }
    if (decorrelate) {
      root = root.withRel(converter.decorrelate(sqlQuery, root.rel));
    }
    if (trim) {
      root = root.withRel(converter.trimUnusedFields(true, root.rel));
    }
    return Pair.of(validator, root);
  }

  @Override public RelNode trimRelNode(SqlTestFactory factory,
      RelNode relNode) {
    final SqlToRelConverter converter = factory.createSqlToRelConverter();
    RelNode r2 = converter.flattenTypes(relNode, true);
    return converter.trimUnusedFields(true, r2);
  }

  /**
   * Creates a RelFieldTrimmer.
   *
   * @param validator Validator
   * @param relBuilder Builder
   * @return Field trimmer
   */
  public RelFieldTrimmer createFieldTrimmer(SqlValidator validator,
      RelBuilder relBuilder) {
    return new RelFieldTrimmer(validator, relBuilder);
  }
}
