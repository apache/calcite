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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Range;

import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.boolQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.existsQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.rangeQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.regexpQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.termQuery;
import static org.apache.calcite.adapter.elasticsearch.QueryBuilders.termsQuery;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

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
   * Internal exception.
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
   * Exception that is thrown when a {@link org.apache.calcite.rel.RelNode}
   * expression cannot be processed (or converted into an Elasticsearch query).
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

    private static boolean supportedRexCall(RexCall call) {
      final SqlSyntax syntax = call.getOperator().getSyntax();
      switch (syntax) {
      case BINARY:
        switch (call.getKind()) {
        case CONTAINS:
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
        default:
          return false;
        }
      case PREFIX: // NOT()
        switch (call.getKind()) {
        case NOT:
          return true;
        default:
          return false;
        }
      case INTERNAL:
        switch (call.getKind()) {
        case SEARCH:
          return canBeTranslatedToTermsQuery(call);
        default:
          return false;
        }
      case FUNCTION_ID:
      case FUNCTION_STAR:
      default:
        return false;
      }
    }

    /**
     * There are three types of the Sarg included in SEARCH RexCall:
     * 1) Sarg is points (In ('a', 'b', 'c' ...)).
     *    In this case the search call can be translated to terms Query
     * 2) Sarg is complementedPoints (Not in ('a', 'b')).
     *    In this case the search call can be translated to MustNot terms Query
     * 3) Sarg is real Range( > 1 and <= 10).
     *    In this case the search call should be translated to rang Query
     * Currently only the 1) and 2) cases are supported.
     * @param search SEARCH RexCall
     * @return true if it isSearchWithPoints or isSearchWithComplementedPoints, other false
     */
    static boolean canBeTranslatedToTermsQuery(RexCall search) {
      return isSearchWithPoints(search) || isSearchWithComplementedPoints(search);
    }

    @SuppressWarnings("BetaApi")
    static boolean isSearchWithPoints(RexCall search) {
      RexLiteral literal = (RexLiteral) search.getOperands().get(1);
      final Sarg<?> sarg = requireNonNull(literal.getValueAs(Sarg.class), "Sarg");
      return sarg.isPoints();
    }

    @SuppressWarnings("BetaApi")
    static boolean isSearchWithComplementedPoints(RexCall search) {
      RexLiteral literal = (RexLiteral) search.getOperands().get(1);
      final Sarg<?> sarg = requireNonNull(literal.getValueAs(Sarg.class), "Sarg");
      return sarg.isComplementedPoints();
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
      case INTERNAL:
        return binary(call);
      case SPECIAL:
        switch (call.getKind()) {
        case CAST:
          return toCastExpression(call);
        case LIKE:
          return binary(call);
        case CONTAINS:
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
          List<Expression> operands = visitList(call.getOperands());
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
      @SuppressWarnings("ModifiedButNotUsed")
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
      case CONTAINS:
        return QueryExpression.create(pair.getKey()).contains(pair.getValue());
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
      case SEARCH:
        if (isSearchWithComplementedPoints(call)) {
          return QueryExpression.create(pair.getKey()).notIn(pair.getValue());
        } else {
          return QueryExpression.create(pair.getKey()).in(pair.getValue());
        }
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
   * Empty interface; exists only to define the type hierarchy.
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

    public abstract QueryExpression contains(LiteralExpression literal);

    /**
     * Negate {@code this} QueryExpression (not the next one).
     */
    public abstract QueryExpression not();

    public abstract QueryExpression exists();

    public abstract QueryExpression notExists();

    public abstract QueryExpression like(LiteralExpression literal);

    public abstract QueryExpression notLike(LiteralExpression literal);

    public abstract QueryExpression equals(LiteralExpression literal);

    public abstract QueryExpression in(LiteralExpression literal);

    public abstract QueryExpression notIn(LiteralExpression literal);

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
   * Builds conjunctions / disjunctions based on existing expressions.
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
     * If partial expression, we will need to complete it with a full filter.
     *
     * @param partial whether we partially converted a and for push down purposes
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

    @Override public QueryExpression contains(LiteralExpression literal) {
      throw new PredicateAnalyzerException("SqlOperatorImpl ['contains'] "
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

    @Override public QueryExpression in(LiteralExpression literal) {
      throw new PredicateAnalyzerException("in cannot be applied to a compound expression");
    }

    @Override public QueryExpression notIn(LiteralExpression literal) {
      throw new PredicateAnalyzerException("notIn cannot be applied to a compound expression");
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

    @Override public QueryExpression contains(LiteralExpression literal) {
      builder = QueryBuilders.matchQuery(getFieldReference(), literal.value());
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

    @Override public QueryExpression in(LiteralExpression literal) {
      Iterable<?> iterable = (Iterable<?>) literal.value();
      builder = termsQuery(getFieldReference(), iterable);
      return this;
    }

    @Override public QueryExpression notIn(LiteralExpression literal) {
      Iterable<?> iterable = (Iterable<?>) literal.value();
      builder = boolQuery().mustNot(termsQuery(getFieldReference(), iterable));
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
   * Empty interface; exists only to define the type hierarchy.
   */
  interface TerminalExpression extends Expression {
  }

  /**
   * SQL cast. For example, {@code cast(col as INTEGER)}.
   */
  static final class CastExpression implements TerminalExpression {
    @SuppressWarnings("unused")
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
   * Used for bind variables.
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

      if (isSarg()) {
        return sargValue();
      } else if (isIntegral()) {
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

    public boolean isSarg() {
      return SqlTypeName.SARG.getName().equalsIgnoreCase(literal.getTypeName().getName());
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

    @SuppressWarnings("BetaApi")
    List<Object> sargValue() {
      final Sarg sarg = requireNonNull(literal.getValueAs(Sarg.class), "Sarg");
      final RelDataType type = literal.getType();
      List<Object> values = new ArrayList<>();
      final SqlTypeName sqlTypeName = type.getSqlTypeName();
      if (sarg.isPoints()) {
        Set<Range> ranges = sarg.rangeSet.asRanges();
        ranges.forEach(range ->
            values.add(sargPointValue(range.lowerEndpoint(), sqlTypeName)));
      } else if (sarg.isComplementedPoints()) {
        Set<Range> ranges = sarg.negate().rangeSet.asRanges();
        ranges.forEach(range ->
            values.add(sargPointValue(range.lowerEndpoint(), sqlTypeName)));
      }
      return values;
    }

    Object sargPointValue(Object point, SqlTypeName sqlTypeName) {
      switch (sqlTypeName) {
      case CHAR:
      case VARCHAR:
        return ((NlsString) point).getValue();
      default:
        return point;
      }
    }

    Object rawValue() {
      return literal.getValue();
    }
  }

  /**
   * If one operand in a binary operator is a DateTime type, but the other isn't,
   * we should not push down the predicate.
   *
   * @param call Current node being evaluated
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
