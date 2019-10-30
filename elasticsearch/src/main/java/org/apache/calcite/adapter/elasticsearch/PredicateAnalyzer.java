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
package org.apache.calcite.adapter.elasticsearch;

import org.apache.calcite.adapter.elasticsearch.QueryBuilders.BoolQueryBuilder;
import org.apache.calcite.adapter.elasticsearch.QueryBuilders.QueryBuilder;
import org.apache.calcite.adapter.elasticsearch.QueryBuilders.RangeQueryBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.boolQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.existsQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.rangeQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.regexpQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.termQuery;

import static java.lang.String.format;

/**
 * Query predicate analyzer. Uses visitor pattern to traverse existing expression
 * and convert it to {@link QueryBuilder}.
 *
 * <p>Major part of this class have been copied from
 * <a href="https://www.dremio.com/">dremio</a> ES adapter
 * (thanks to their team for improving calcite-ES integration).
 */
class PredicateAnalyzer {

  /**
   * Internal exception
   */
  @SuppressWarnings("serial")
  private static final class PredicateAnalyzerException extends RuntimeException {

    PredicateAnalyzerException(String message) {
      super(message);
    }

    PredicateAnalyzerException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * Thrown when {@link org.apache.calcite.rel.RelNode} expression can't be processed
   * (or converted into ES query)
   */
  static class ExpressionNotAnalyzableException extends Exception {
    ExpressionNotAnalyzableException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private PredicateAnalyzer() {}

  /**
   * Walks the expression tree, attempting to convert the entire tree into
   * an equivalent Elasticsearch query filter. If an error occurs, or if it
   * is determined that the expression cannot be converted, an exception is
   * thrown and an error message logged.
   *
   * <p>Callers should catch ExpressionNotAnalyzableException
   * and fall back to not using push-down filters.
   *
   * @param expression expression to analyze
   * @return search query which can be used to query ES cluster
   * @throws ExpressionNotAnalyzableException when expression can't processed by this analyzer
   */
  static QueryBuilder analyze(RexNode expression) throws ExpressionNotAnalyzableException {
    Objects.requireNonNull(expression, "expression");
    try {
      // visits expression tree
      QueryExpression e = (QueryExpression) expression.accept(new Visitor());

      if (e != null && e.isPartial()) {
        throw new UnsupportedOperationException("Can't handle partial QueryExpression: " + e);
      }
      return e != null ? e.builder() : null;
    } catch (Throwable e) {
      Throwables.propagateIfPossible(e, UnsupportedOperationException.class);
      throw new ExpressionNotAnalyzableException("Can't convert " + expression, e);
    }
  }

  /**
   * Converts expressions of the form NOT(LIKE(...)) into NOT_LIKE(...)
   */
  private static class NotLikeConverter extends RexShuttle {
    final RexBuilder rexBuilder;

    NotLikeConverter(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override public RexNode visitCall(RexCall call) {
      if (call.getOperator().getKind() == SqlKind.NOT) {
        RexNode child = call.getOperands().get(0);
        if (child.getKind() == SqlKind.LIKE) {
          List<RexNode> operands = ((RexCall) child).getOperands()
              .stream()
              .map(rexNode -> rexNode.accept(NotLikeConverter.this))
              .collect(Collectors.toList());
          return rexBuilder.makeCall(SqlStdOperatorTable.NOT_LIKE, operands);
        }
      }
      return super.visitCall(call);
    }
  }

  /**
   * Traverses {@link RexNode} tree and builds ES query.
   */
  private static class Visitor extends RexVisitorImpl<Expression> {

    private Visitor() {
      super(true);
    }

    @Override public Expression visitInputRef(RexInputRef inputRef) {
      return new NamedFieldExpression(inputRef);
    }

    @Override public Expression visitLiteral(RexLiteral literal) {
      return new LiteralExpression(literal);
    }

    private boolean supportedRexCall(RexCall call) {
      final SqlSyntax syntax = call.getOperator().getSyntax();
      switch (syntax) {
      case BINARY:
        switch (call.getKind()) {
        case AND:
        case OR:
        case LIKE:
        case EQUALS:
        case NOT_EQUALS:
        case GREATER_THAN:
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
          return true;
        default:
          return false;
        }
      case SPECIAL:
        switch (call.getKind()) {
        case CAST:
        case LIKE:
        case ITEM:
        case OTHER_FUNCTION:
          return true;
        case CASE:
        case SIMILAR:
        default:
          return false;
        }
      case FUNCTION:
        return true;
      case POSTFIX:
        switch (call.getKind()) {
        case IS_NOT_NULL:
        case IS_NULL:
          return true;
        }
      case PREFIX: // NOT()
        switch (call.getKind()) {
        case NOT:
          return true;
        }
      // fall through
      case FUNCTION_ID:
      case FUNCTION_STAR:
      default:
        return false;
      }
    }

    @Override public Expression visitCall(RexCall call) {

      SqlSyntax syntax = call.getOperator().getSyntax();
      if (!supportedRexCall(call)) {
        String message = String.format(Locale.ROOT, "Unsupported call: [%s]", call);
        throw new PredicateAnalyzerException(message);
      }

      switch (syntax) {
      case BINARY:
        return binary(call);
      case POSTFIX:
        return postfix(call);
      case PREFIX:
        return prefix(call);
      case SPECIAL:
        switch (call.getKind()) {
        case CAST:
          return toCastExpression(call);
        case LIKE:
          return binary(call);
        default:
          // manually process ITEM($0, 'foo') which in our case will be named attribute
          if (call.getOperator().getName().equalsIgnoreCase("ITEM")) {
            return toNamedField((RexLiteral) call.getOperands().get(1));
          }
          String message = String.format(Locale.ROOT, "Unsupported call: [%s]", call);
          throw new PredicateAnalyzerException(message);
        }
      case FUNCTION:
        if (call.getOperator().getName().equalsIgnoreCase("CONTAINS")) {
          List<Expression> operands = new ArrayList<>();
          for (RexNode node : call.getOperands()) {
            final Expression nodeExpr = node.accept(this);
            operands.add(nodeExpr);
          }
          String query = convertQueryString(operands.subList(0, operands.size() - 1),
              operands.get(operands.size() - 1));
          return QueryExpression.create(new NamedFieldExpression()).queryString(query);
        }
        // fall through
      default:
        String message = format(Locale.ROOT, "Unsupported syntax [%s] for call: [%s]",
            syntax, call);
        throw new PredicateAnalyzerException(message);
      }
    }

    private static String convertQueryString(List<Expression> fields, Expression query) {
      int index = 0;
      Preconditions.checkArgument(query instanceof LiteralExpression,
          "Query string must be a string literal");
      String queryString = ((LiteralExpression) query).stringValue();
      Map<String, String> fieldMap = new LinkedHashMap<>();
      for (Expression expr : fields) {
        if (expr instanceof NamedFieldExpression) {
          NamedFieldExpression field = (NamedFieldExpression) expr;
          String fieldIndexString = String.format(Locale.ROOT, "$%d", index++);
          fieldMap.put(fieldIndexString, field.getReference());
        }
      }
      try {
        return queryString;
      } catch (Exception e) {
        throw new PredicateAnalyzerException(e);
      }
    }

    private QueryExpression prefix(RexCall call) {
      Preconditions.checkArgument(call.getKind() == SqlKind.NOT,
          "Expected %s got %s", SqlKind.NOT, call.getKind());

      if (call.getOperands().size() != 1) {
        String message = String.format(Locale.ROOT, "Unsupported NOT operator: [%s]", call);
        throw new PredicateAnalyzerException(message);
      }

      QueryExpression expr = (QueryExpression) call.getOperands().get(0).accept(this);
      return expr.not();
    }

    private QueryExpression postfix(RexCall call) {
      Preconditions.checkArgument(call.getKind() == SqlKind.IS_NULL
          || call.getKind() == SqlKind.IS_NOT_NULL);
      if (call.getOperands().size() != 1) {
        String message = String.format(Locale.ROOT, "Unsupported operator: [%s]", call);
        throw new PredicateAnalyzerException(message);
      }
      Expression a = call.getOperands().get(0).accept(this);
      // Elasticsearch does not want is null/is not null (exists query)
      // for _id and _index, although it supports for all other metadata column
      isColumn(a, call, ElasticsearchConstants.ID, true);
      isColumn(a, call, ElasticsearchConstants.INDEX, true);
      QueryExpression operand = QueryExpression.create((TerminalExpression) a);
      return call.getKind() == SqlKind.IS_NOT_NULL ? operand.exists() : operand.notExists();
    }

    /**
     * Process a call which is a binary operation, transforming into an equivalent
     * query expression. Note that the incoming call may be either a simple binary
     * expression, such as {@code foo > 5}, or it may be several simple expressions connected
     * by {@code AND} or {@code OR} operators, such as {@code foo > 5 AND bar = 'abc' AND 'rot' < 1}
     *
     * @param call existing call
     * @return evaluated expression
     */
    private QueryExpression binary(RexCall call) {

      // if AND/OR, do special handling
      if (call.getKind() == SqlKind.AND || call.getKind() == SqlKind.OR) {
        return andOr(call);
      }

      checkForIncompatibleDateTimeOperands(call);

      Preconditions.checkState(call.getOperands().size() == 2);
      final Expression a = call.getOperands().get(0).accept(this);
      final Expression b = call.getOperands().get(1).accept(this);

      final SwapResult pair = swap(a, b);
      final boolean swapped = pair.isSwapped();

      // For _id and _index columns, only equals/not_equals work!
      if (isColumn(pair.getKey(), call, ElasticsearchConstants.ID, false)
          || isColumn(pair.getKey(), call, ElasticsearchConstants.INDEX, false)
          || isColumn(pair.getKey(), call, ElasticsearchConstants.UID, false)) {
        switch (call.getKind()) {
        case EQUALS:
        case NOT_EQUALS:
          break;
        default:
          throw new PredicateAnalyzerException(
              "Cannot handle " + call.getKind() + " expression for _id field, " + call);
        }
      }

      switch (call.getKind()) {
      case LIKE:
        throw new UnsupportedOperationException("LIKE not yet supported");
      case EQUALS:
        return QueryExpression.create(pair.getKey()).equals(pair.getValue());
      case NOT_EQUALS:
        return QueryExpression.create(pair.getKey()).notEquals(pair.getValue());
      case GREATER_THAN:
        if (swapped) {
          return QueryExpression.create(pair.getKey()).lt(pair.getValue());
        }
        return QueryExpression.create(pair.getKey()).gt(pair.getValue());
      case GREATER_THAN_OR_EQUAL:
        if (swapped) {
          return QueryExpression.create(pair.getKey()).lte(pair.getValue());
        }
        return QueryExpression.create(pair.getKey()).gte(pair.getValue());
      case LESS_THAN:
        if (swapped) {
          return QueryExpression.create(pair.getKey()).gt(pair.getValue());
        }
        return QueryExpression.create(pair.getKey()).lt(pair.getValue());
      case LESS_THAN_OR_EQUAL:
        if (swapped) {
          return QueryExpression.create(pair.getKey()).gte(pair.getValue());
        }
        return QueryExpression.create(pair.getKey()).lte(pair.getValue());
      default:
        break;
      }
      String message = String.format(Locale.ROOT, "Unable to handle call: [%s]", call);
      throw new PredicateAnalyzerException(message);
    }

    private QueryExpression andOr(RexCall call) {
      QueryExpression[] expressions = new QueryExpression[call.getOperands().size()];
      PredicateAnalyzerException firstError = null;
      boolean partial = false;
      for (int i = 0; i < call.getOperands().size(); i++) {
        try {
          Expression expr = call.getOperands().get(i).accept(this);
          if (expr instanceof NamedFieldExpression) {
            // nop currently
          } else {
            expressions[i] = (QueryExpression) call.getOperands().get(i).accept(this);
          }
          partial |= expressions[i].isPartial();
        } catch (PredicateAnalyzerException e) {
          if (firstError == null) {
            firstError = e;
          }
          partial = true;
        }
      }

      switch (call.getKind()) {
      case OR:
        if (partial) {
          if (firstError != null) {
            throw firstError;
          } else {
            final String message = String.format(Locale.ROOT, "Unable to handle call: [%s]", call);
            throw new PredicateAnalyzerException(message);
          }
        }
        return CompoundQueryExpression.or(expressions);
      case AND:
        return CompoundQueryExpression.and(partial, expressions);
      default:
        String message = String.format(Locale.ROOT, "Unable to handle call: [%s]", call);
        throw new PredicateAnalyzerException(message);
      }
    }

    /**
     * Holder class for a pair of expressions. Used to convert {@code 1 = foo} into {@code foo = 1}
     */
    private static class SwapResult {
      final boolean swapped;
      final TerminalExpression terminal;
      final LiteralExpression literal;

      SwapResult(boolean swapped, TerminalExpression terminal, LiteralExpression literal) {
        super();
        this.swapped = swapped;
        this.terminal = terminal;
        this.literal = literal;
      }

      TerminalExpression getKey() {
        return terminal;
      }

      LiteralExpression getValue() {
        return literal;
      }

      boolean isSwapped() {
        return swapped;
      }
    }

    /**
     * Swap order of operands such that the literal expression is always on the right.
     *
     * <p>NOTE: Some combinations of operands are implicitly not supported and will
     * cause an exception to be thrown. For example, we currently do not support
     * comparing a literal to another literal as convention {@code 5 = 5}. Nor do we support
     * comparing named fields to other named fields as convention {@code $0 = $1}.
     * @param left left expression
     * @param right right expression
     */
    private static SwapResult swap(Expression left, Expression right) {

      TerminalExpression terminal;
      LiteralExpression literal = expressAsLiteral(left);
      boolean swapped = false;
      if (literal != null) {
        swapped = true;
        terminal = (TerminalExpression) right;
      } else {
        literal = expressAsLiteral(right);
        terminal = (TerminalExpression) left;
      }

      if (literal == null || terminal == null) {
        String message = String.format(Locale.ROOT,
            "Unexpected combination of expressions [left: %s] [right: %s]", left, right);
        throw new PredicateAnalyzerException(message);
      }

      if (CastExpression.isCastExpression(terminal)) {
        terminal = CastExpression.unpack(terminal);
      }

      return new SwapResult(swapped, terminal, literal);
    }

    private CastExpression toCastExpression(RexCall call) {
      TerminalExpression argument = (TerminalExpression) call.getOperands().get(0).accept(this);
      return new CastExpression(call.getType(), argument);
    }

    private static NamedFieldExpression toNamedField(RexLiteral literal) {
      return new NamedFieldExpression(literal);
    }

    /**
     * Try to convert a generic expression into a literal expression.
     */
    private static LiteralExpression expressAsLiteral(Expression exp) {

      if (exp instanceof LiteralExpression) {
        return (LiteralExpression) exp;
      }

      return null;
    }

    private static boolean isColumn(Expression exp, RexNode node,
        String columnName, boolean throwException) {
      if (!(exp instanceof NamedFieldExpression)) {
        return false;
      }

      final NamedFieldExpression termExp = (NamedFieldExpression) exp;
      if (columnName.equals(termExp.getRootName())) {
        if (throwException) {
          throw new PredicateAnalyzerException("Cannot handle _id field in " + node);
        }
        return true;
      }
      return false;
    }
  }

  /**
   * Empty interface; exists only to define type hierarchy
   */
  interface Expression {
  }

  /**
   * Main expression operators (like {@code equals}, {@code gt}, {@code exists} etc.)
   */
  abstract static class QueryExpression implements Expression {

    public abstract QueryBuilder builder();

    public boolean isPartial() {
      return false;
    }

    /**
     * Negate {@code this} QueryExpression (not the next one).
     */
    public abstract QueryExpression not();

    public abstract QueryExpression exists();

    public abstract QueryExpression notExists();

    public abstract QueryExpression like(LiteralExpression literal);

    public abstract QueryExpression notLike(LiteralExpression literal);

    public abstract QueryExpression equals(LiteralExpression literal);

    public abstract QueryExpression notEquals(LiteralExpression literal);

    public abstract QueryExpression gt(LiteralExpression literal);

    public abstract QueryExpression gte(LiteralExpression literal);

    public abstract QueryExpression lt(LiteralExpression literal);

    public abstract QueryExpression lte(LiteralExpression literal);

    public abstract QueryExpression queryString(String query);

    public abstract QueryExpression isTrue();

    public static QueryExpression create(TerminalExpression expression) {

      if (expression instanceof NamedFieldExpression) {
        return new SimpleQueryExpression((NamedFieldExpression) expression);
      } else {
        String message = String.format(Locale.ROOT, "Unsupported expression: [%s]", expression);
        throw new PredicateAnalyzerException(message);
      }
    }

  }

  /**
   * Builds conjunctions / disjunctions based on existing expressions
   */
  static class CompoundQueryExpression extends QueryExpression {

    private final boolean partial;
    private final BoolQueryBuilder builder;

    public static CompoundQueryExpression or(QueryExpression... expressions) {
      CompoundQueryExpression bqe = new CompoundQueryExpression(false);
      for (QueryExpression expression : expressions) {
        bqe.builder.should(expression.builder());
      }
      return bqe;
    }

    /**
     * if partial expression, we will need to complete it with a full filter
     * @param partial whether we partially converted a and for push down purposes.
     * @param expressions list of expressions to join with {@code and} boolean
     * @return new instance of expression
     */
    public static CompoundQueryExpression and(boolean partial, QueryExpression... expressions) {
      CompoundQueryExpression bqe = new CompoundQueryExpression(partial);
      for (QueryExpression expression : expressions) {
        if (expression != null) { // partial expressions have nulls for missing nodes
          bqe.builder.must(expression.builder());
        }
      }
      return bqe;
    }

    private CompoundQueryExpression(boolean partial) {
      this(partial, boolQuery());
    }

    private CompoundQueryExpression(boolean partial, BoolQueryBuilder builder) {
      this.partial = partial;
      this.builder = Objects.requireNonNull(builder, "builder");
    }

    @Override public boolean isPartial() {
      return partial;
    }


    @Override public QueryBuilder builder() {
      return builder;
    }

    @Override public QueryExpression not() {
      return new CompoundQueryExpression(partial, QueryBuilders.boolQuery().mustNot(builder()));
    }

    @Override public QueryExpression exists() {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['exists'] "
          + "cannot be applied to a compound expression");
    }

    @Override public QueryExpression notExists() {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['notExists'] "
          + "cannot be applied to a compound expression");
    }

    @Override public QueryExpression like(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['like'] "
          + "cannot be applied to a compound expression");
    }

    @Override public QueryExpression notLike(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['notLike'] "
          + "cannot be applied to a compound expression");
    }

    @Override public QueryExpression equals(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['='] "
          + "cannot be applied to a compound expression");
    }

    @Override public QueryExpression notEquals(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['not'] "
          + "cannot be applied to a compound expression");
    }

    @Override public QueryExpression gt(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['>'] "
          + "cannot be applied to a compound expression");
    }

    @Override public QueryExpression gte(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['>='] "
          + "cannot be applied to a compound expression");
    }

    @Override public QueryExpression lt(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['<'] "
          + "cannot be applied to a compound expression");
    }

    @Override public QueryExpression lte(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['<='] "
          + "cannot be applied to a compound expression");
    }

    @Override public QueryExpression queryString(String query) {
      throw new PredicateAnalyzerException("QueryString "
          + "cannot be applied to a compound expression");
    }

    @Override public QueryExpression isTrue() {
      throw new PredicateAnalyzerException("isTrue cannot be applied to a compound expression");
    }
  }

  /**
   * Usually basic expression of type {@code a = 'val'} or {@code b > 42}.
   */
  static class SimpleQueryExpression extends QueryExpression {

    private final NamedFieldExpression rel;
    private QueryBuilder builder;

    private String getFieldReference() {
      return rel.getReference();
    }

    private SimpleQueryExpression(NamedFieldExpression rel) {
      this.rel = rel;
    }

    @Override public QueryBuilder builder() {
      if (builder == null) {
        throw new IllegalStateException("Builder was not initialized");
      }
      return builder;
    }

    @Override public QueryExpression not() {
      builder = boolQuery().mustNot(builder());
      return this;
    }

    @Override public QueryExpression exists() {
      builder = existsQuery(getFieldReference());
      return this;
    }

    @Override public QueryExpression notExists() {
      // Even though Lucene doesn't allow a stand alone mustNot boolean query,
      // Elasticsearch handles this problem transparently on its end
      builder = boolQuery().mustNot(existsQuery(getFieldReference()));
      return this;
    }

    @Override public QueryExpression like(LiteralExpression literal) {
      builder = regexpQuery(getFieldReference(), literal.stringValue());
      return this;
    }

    @Override public QueryExpression notLike(LiteralExpression literal) {
      builder = boolQuery()
              // NOT LIKE should return false when field is NULL
              .must(existsQuery(getFieldReference()))
              .mustNot(regexpQuery(getFieldReference(), literal.stringValue()));
      return this;
    }

    @Override public QueryExpression equals(LiteralExpression literal) {
      Object value = literal.value();
      if (value instanceof GregorianCalendar) {
        builder = boolQuery()
                .must(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).gte(value)))
                .must(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lte(value)));
      } else {
        builder = termQuery(getFieldReference(), value);
      }
      return this;
    }

    @Override public QueryExpression notEquals(LiteralExpression literal) {
      Object value = literal.value();
      if (value instanceof GregorianCalendar) {
        builder = boolQuery()
                .should(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).gt(value)))
                .should(addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lt(value)));
      } else {
        builder = boolQuery()
                // NOT LIKE should return false when field is NULL
                .must(existsQuery(getFieldReference()))
                .mustNot(termQuery(getFieldReference(), value));
      }
      return this;
    }

    @Override public QueryExpression gt(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal,
          rangeQuery(getFieldReference()).gt(value));
      return this;
    }

    @Override public QueryExpression gte(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, rangeQuery(getFieldReference()).gte(value));
      return this;
    }

    @Override public QueryExpression lt(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lt(value));
      return this;
    }

    @Override public QueryExpression lte(LiteralExpression literal) {
      Object value = literal.value();
      builder = addFormatIfNecessary(literal, rangeQuery(getFieldReference()).lte(value));
      return this;
    }

    @Override public QueryExpression queryString(String query) {
      throw new UnsupportedOperationException("QueryExpression not yet supported: " + query);
    }

    @Override public QueryExpression isTrue() {
      builder = termQuery(getFieldReference(), true);
      return this;
    }
  }


  /**
   * By default, range queries on date/time need use the format of the source to parse the literal.
   * So we need to specify that the literal has "date_time" format
   * @param literal literal value
   * @param rangeQueryBuilder query builder to optionally add {@code format} expression
   * @return existing builder with possible {@code format} attribute
   */
  private static RangeQueryBuilder addFormatIfNecessary(LiteralExpression literal,
      RangeQueryBuilder rangeQueryBuilder) {
    if (literal.value() instanceof GregorianCalendar) {
      rangeQueryBuilder.format("date_time");
    }
    return rangeQueryBuilder;
  }

  /**
   * Empty interface; exists only to define type hierarchy
   */
  interface TerminalExpression extends Expression {
  }

  /**
   * SQL cast: {@code cast(col as INTEGER)}
   */
  static final class CastExpression implements TerminalExpression {
    private final RelDataType type;
    private final TerminalExpression argument;

    private CastExpression(RelDataType type, TerminalExpression argument) {
      this.type = type;
      this.argument = argument;
    }

    public boolean isCastFromLiteral() {
      return argument instanceof LiteralExpression;
    }

    static TerminalExpression unpack(TerminalExpression exp) {
      if (!(exp instanceof CastExpression)) {
        return exp;
      }
      return ((CastExpression) exp).argument;
    }

    static boolean isCastExpression(Expression exp) {
      return exp instanceof CastExpression;
    }

  }

  /**
   * Used for bind variables
   */
  static final class NamedFieldExpression implements TerminalExpression {

    private final String name;

    private NamedFieldExpression() {
      this.name = null;
    }

    private NamedFieldExpression(RexInputRef schemaField) {
      this.name = schemaField == null ? null : schemaField.getName();
    }

    private NamedFieldExpression(RexLiteral literal) {
      this.name = literal == null ? null : RexLiteral.stringValue(literal);
    }

    String getRootName() {
      return name;
    }

    boolean isMetaField() {
      return ElasticsearchConstants.META_COLUMNS.contains(getRootName());
    }

    String getReference() {
      return getRootName();
    }
  }

  /**
   * Literal like {@code 'foo' or 42 or true} etc.
   */
  static final class LiteralExpression implements TerminalExpression {

    final RexLiteral literal;

    LiteralExpression(RexLiteral literal) {
      this.literal = literal;
    }

    Object value() {

      if (isIntegral()) {
        return longValue();
      } else if (isFloatingPoint()) {
        return doubleValue();
      } else if (isBoolean()) {
        return booleanValue();
      } else if (isString()) {
        return RexLiteral.stringValue(literal);
      } else {
        return rawValue();
      }
    }

    boolean isIntegral() {
      return SqlTypeName.INT_TYPES.contains(literal.getType().getSqlTypeName());
    }

    boolean isFloatingPoint() {
      return SqlTypeName.APPROX_TYPES.contains(literal.getType().getSqlTypeName());
    }

    boolean isBoolean() {
      return SqlTypeName.BOOLEAN_TYPES.contains(literal.getType().getSqlTypeName());
    }

    public boolean isString() {
      return SqlTypeName.CHAR_TYPES.contains(literal.getType().getSqlTypeName());
    }

    long longValue() {
      return ((Number) literal.getValue()).longValue();
    }

    double doubleValue() {
      return ((Number) literal.getValue()).doubleValue();
    }

    boolean booleanValue() {
      return RexLiteral.booleanValue(literal);
    }

    String stringValue() {
      return RexLiteral.stringValue(literal);
    }

    Object rawValue() {
      return literal.getValue();
    }
  }

  /**
   * If one operand in a binary operator is a DateTime type, but the other isn't,
   * we should not push down the predicate
   * @param call current node being evaluated
   */
  private static void checkForIncompatibleDateTimeOperands(RexCall call) {
    RelDataType op1 = call.getOperands().get(0).getType();
    RelDataType op2 = call.getOperands().get(1).getType();
    if ((SqlTypeFamily.DATETIME.contains(op1) && !SqlTypeFamily.DATETIME.contains(op2))
           || (SqlTypeFamily.DATETIME.contains(op2) && !SqlTypeFamily.DATETIME.contains(op1))
           || (SqlTypeFamily.DATE.contains(op1) && !SqlTypeFamily.DATE.contains(op2))
           || (SqlTypeFamily.DATE.contains(op2) && !SqlTypeFamily.DATE.contains(op1))
           || (SqlTypeFamily.TIMESTAMP.contains(op1) && !SqlTypeFamily.TIMESTAMP.contains(op2))
           || (SqlTypeFamily.TIMESTAMP.contains(op2) && !SqlTypeFamily.TIMESTAMP.contains(op1))
           || (SqlTypeFamily.TIME.contains(op1) && !SqlTypeFamily.TIME.contains(op2))
           || (SqlTypeFamily.TIME.contains(op2) && !SqlTypeFamily.TIME.contains(op1))) {
      throw new PredicateAnalyzerException("Cannot handle " + call.getKind()
          + " expression for _id field, " + call);
    }
  }
}

// End PredicateAnalyzer.java
