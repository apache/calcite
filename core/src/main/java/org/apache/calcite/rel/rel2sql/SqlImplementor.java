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
package org.apache.calcite.rel.rel2sql;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * State for generating a SQL statement.
 */
public abstract class SqlImplementor {

  public static final SqlParserPos POS = SqlParserPos.ZERO;

  public final SqlDialect dialect;
  protected final Set<String> aliasSet = new LinkedHashSet<>();
  protected final Map<String, SqlNode> ordinalMap = new HashMap<>();

  protected final Map<CorrelationId, Context> correlTableMap = new HashMap<>();

  protected SqlImplementor(SqlDialect dialect) {
    this.dialect = Preconditions.checkNotNull(dialect);
  }

  public abstract Result visitChild(int i, RelNode e);

  public void addSelect(List<SqlNode> selectList, SqlNode node,
      RelDataType rowType) {
    String name = rowType.getFieldNames().get(selectList.size());
    String alias = SqlValidatorUtil.getAlias(node, -1);
    if (alias == null || !alias.equals(name)) {
      node = SqlStdOperatorTable.AS.createCall(
          POS, node, new SqlIdentifier(name, POS));
    }
    selectList.add(node);
  }

  /** Returns whether a list of expressions projects all fields, in order,
   * from the input, with the same names. */
  public static boolean isStar(List<RexNode> exps, RelDataType inputRowType,
      RelDataType projectRowType) {
    assert exps.size() == projectRowType.getFieldCount();
    int i = 0;
    for (RexNode ref : exps) {
      if (!(ref instanceof RexInputRef)) {
        return false;
      } else if (((RexInputRef) ref).getIndex() != i++) {
        return false;
      }
    }
    return i == inputRowType.getFieldCount()
        && inputRowType.getFieldNames().equals(projectRowType.getFieldNames());
  }

  public static boolean isStar(RexProgram program) {
    int i = 0;
    for (RexLocalRef ref : program.getProjectList()) {
      if (ref.getIndex() != i++) {
        return false;
      }
    }
    return i == program.getInputRowType().getFieldCount();
  }

  public Result setOpToSql(SqlSetOperator operator, RelNode rel) {
    SqlNode node = null;
    for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
      final Result result = visitChild(input.i, input.e);
      if (node == null) {
        node = result.asSelect();
      } else {
        node = operator.createCall(POS, node, result.asSelect());
      }
    }
    final List<Clause> clauses =
        Expressions.list(Clause.SET_OP);
    return result(node, clauses, rel, null);
  }

  /**
   * Converts a {@link RexNode} condition into a {@link SqlNode}.
   *
   * @param node            Join condition
   * @param leftContext     Left context
   * @param rightContext    Right context
   * @param leftFieldCount  Number of fields on left result
   * @return SqlNode that represents the condition
   */
  public static SqlNode convertConditionToSqlNode(RexNode node,
      Context leftContext,
      Context rightContext, int leftFieldCount) {
    if (node.isAlwaysTrue()) {
      return SqlLiteral.createBoolean(true, POS);
    }
    if (node.isAlwaysFalse()) {
      return SqlLiteral.createBoolean(false, POS);
    }
    if (!(node instanceof RexCall)) {
      throw new AssertionError(node);
    }
    final List<RexNode> operands;
    final SqlOperator op;
    final Context joinContext;
    switch (node.getKind()) {
    case AND:
    case OR:
      operands = ((RexCall) node).getOperands();
      op = ((RexCall) node).getOperator();
      SqlNode sqlCondition = null;
      for (RexNode operand : operands) {
        SqlNode x = convertConditionToSqlNode(operand, leftContext,
            rightContext, leftFieldCount);
        if (sqlCondition == null) {
          sqlCondition = x;
        } else {
          sqlCondition = op.createCall(POS, sqlCondition, x);
        }
      }
      return sqlCondition;

    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      node = stripCastFromString(node);
      operands = ((RexCall) node).getOperands();
      op = ((RexCall) node).getOperator();
      if (operands.size() == 2
          && operands.get(0) instanceof RexInputRef
          && operands.get(1) instanceof RexInputRef) {
        final RexInputRef op0 = (RexInputRef) operands.get(0);
        final RexInputRef op1 = (RexInputRef) operands.get(1);

        if (op0.getIndex() < leftFieldCount
            && op1.getIndex() >= leftFieldCount) {
          // Arguments were of form 'op0 = op1'
          return op.createCall(POS,
              leftContext.field(op0.getIndex()),
              rightContext.field(op1.getIndex() - leftFieldCount));
        }
        if (op1.getIndex() < leftFieldCount
            && op0.getIndex() >= leftFieldCount) {
          // Arguments were of form 'op1 = op0'
          return reverseOperatorDirection(op).createCall(POS,
              leftContext.field(op1.getIndex()),
              rightContext.field(op0.getIndex() - leftFieldCount));
        }
      }
      joinContext =
          leftContext.implementor().joinContext(leftContext, rightContext);
      return joinContext.toSql(null, node);
    case IS_NULL:
    case IS_NOT_NULL:
      operands = ((RexCall) node).getOperands();
      if (operands.size() == 1
          && operands.get(0) instanceof RexInputRef) {
        op = ((RexCall) node).getOperator();
        final RexInputRef op0 = (RexInputRef) operands.get(0);
        if (op0.getIndex() < leftFieldCount) {
          return op.createCall(POS, leftContext.field(op0.getIndex()));
        } else {
          return op.createCall(POS,
              rightContext.field(op0.getIndex() - leftFieldCount));
        }
      }
      joinContext =
          leftContext.implementor().joinContext(leftContext, rightContext);
      return joinContext.toSql(null, node);
    default:
      throw new AssertionError(node);
    }
  }

  /** Removes cast from string.
   *
   * <p>For example, {@code x > CAST('2015-01-07' AS DATE)}
   * becomes {@code x > '2015-01-07'}.
   */
  private static RexNode stripCastFromString(RexNode node) {
    switch (node.getKind()) {
    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      final RexCall call = (RexCall) node;
      final RexNode o0 = call.operands.get(0);
      final RexNode o1 = call.operands.get(1);
      if (o0.getKind() == SqlKind.CAST
          && o1.getKind() != SqlKind.CAST) {
        final RexNode o0b = ((RexCall) o0).getOperands().get(0);
        switch (o0b.getType().getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          return call.clone(call.getType(), ImmutableList.of(o0b, o1));
        }
      }
      if (o1.getKind() == SqlKind.CAST
          && o0.getKind() != SqlKind.CAST) {
        final RexNode o1b = ((RexCall) o1).getOperands().get(0);
        switch (o1b.getType().getSqlTypeName()) {
        case CHAR:
        case VARCHAR:
          return call.clone(call.getType(), ImmutableList.of(o0, o1b));
        }
      }
    }
    return node;
  }

  private static SqlOperator reverseOperatorDirection(SqlOperator op) {
    switch (op.kind) {
    case GREATER_THAN:
      return SqlStdOperatorTable.LESS_THAN;
    case GREATER_THAN_OR_EQUAL:
      return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    case LESS_THAN:
      return SqlStdOperatorTable.GREATER_THAN;
    case LESS_THAN_OR_EQUAL:
      return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
    case EQUALS:
    case IS_NOT_DISTINCT_FROM:
    case NOT_EQUALS:
      return op;
    default:
      throw new AssertionError(op);
    }
  }

  public static JoinType joinType(JoinRelType joinType) {
    switch (joinType) {
    case LEFT:
      return JoinType.LEFT;
    case RIGHT:
      return JoinType.RIGHT;
    case INNER:
      return JoinType.INNER;
    case FULL:
      return JoinType.FULL;
    default:
      throw new AssertionError(joinType);
    }
  }

  /** Creates a result based on a single relational expression. */
  public Result result(SqlNode node, Collection<Clause> clauses,
      RelNode rel, Map<String, RelDataType> aliases) {
    assert aliases == null
        || aliases.size() < 2
        || aliases instanceof LinkedHashMap
        || aliases instanceof ImmutableMap
        : "must use a Map implementation that preserves order";
    final String alias2 = SqlValidatorUtil.getAlias(node, -1);
    final String alias3 = alias2 != null ? alias2 : "t";
    final String alias4 =
        SqlValidatorUtil.uniquify(
            alias3, aliasSet, SqlValidatorUtil.EXPR_SUGGESTER);
    if (aliases != null
        && !aliases.isEmpty()
        && (!dialect.hasImplicitTableAlias()
          || aliases.size() > 1)) {
      return new Result(node, clauses, alias4, rel.getRowType(), aliases);
    }
    final String alias5;
    if (alias2 == null
        || !alias2.equals(alias4)
        || !dialect.hasImplicitTableAlias()) {
      alias5 = alias4;
    } else {
      alias5 = null;
    }
    return new Result(node, clauses, alias5, rel.getRowType(),
        ImmutableMap.of(alias4, rel.getRowType()));
  }

  /** Creates a result based on a join. (Each join could contain one or more
   * relational expressions.) */
  public Result result(SqlNode join, Result leftResult, Result rightResult) {
    final ImmutableMap.Builder<String, RelDataType> builder =
        ImmutableMap.builder();
    collectAliases(builder, join,
        Iterables.concat(leftResult.aliases.values(),
            rightResult.aliases.values()).iterator());
    return new Result(join, Expressions.list(Clause.FROM), null, null,
        builder.build());
  }

  private void collectAliases(ImmutableMap.Builder<String, RelDataType> builder,
      SqlNode node, Iterator<RelDataType> aliases) {
    if (node instanceof SqlJoin) {
      final SqlJoin join = (SqlJoin) node;
      collectAliases(builder, join.getLeft(),  aliases);
      collectAliases(builder, join.getRight(), aliases);
    } else {
      final String alias = SqlValidatorUtil.getAlias(node, -1);
      assert alias != null;
      builder.put(alias, aliases.next());
    }
  }

  /** Wraps a node in a SELECT statement that has no clauses:
   *  "SELECT ... FROM (node)". */
  SqlSelect wrapSelect(SqlNode node) {
    assert node instanceof SqlJoin
        || node instanceof SqlIdentifier
        || node instanceof SqlMatchRecognize
        || node instanceof SqlCall
            && (((SqlCall) node).getOperator() instanceof SqlSetOperator
                || ((SqlCall) node).getOperator() == SqlStdOperatorTable.AS
                || ((SqlCall) node).getOperator() == SqlStdOperatorTable.VALUES)
        : node;
    return new SqlSelect(POS, SqlNodeList.EMPTY, null, node, null, null, null,
        SqlNodeList.EMPTY, null, null, null);
  }

  /** Context for translating a {@link RexNode} expression (within a
   * {@link RelNode}) into a {@link SqlNode} expression (within a SQL parse
   * tree). */
  public abstract class Context {
    final int fieldCount;
    private final boolean ignoreCast;

    protected Context(int fieldCount) {
      this(fieldCount, false);
    }

    protected Context(int fieldCount, boolean ignoreCast) {
      this.fieldCount = fieldCount;
      this.ignoreCast = ignoreCast;
    }

    public abstract SqlNode field(int ordinal);

    /** Converts an expression from {@link RexNode} to {@link SqlNode}
     * format.
     *
     * @param program Required only if {@code rex} contains {@link RexLocalRef}
     * @param rex Expression to convert
     */
    public SqlNode toSql(RexProgram program, RexNode rex) {
      final RexSubQuery subQuery;
      final SqlNode sqlSubQuery;
      switch (rex.getKind()) {
      case LOCAL_REF:
        final int index = ((RexLocalRef) rex).getIndex();
        return toSql(program, program.getExprList().get(index));

      case INPUT_REF:
        return field(((RexInputRef) rex).getIndex());

      case FIELD_ACCESS:
        RexFieldAccess access = (RexFieldAccess) rex;
        final RexCorrelVariable variable =
            (RexCorrelVariable) access.getReferenceExpr();
        final Context aliasContext = correlTableMap.get(variable.id);
        return aliasContext.field(access.getField().getIndex());

      case PATTERN_INPUT_REF:
        final RexPatternFieldRef ref = (RexPatternFieldRef) rex;
        String pv = ref.getAlpha();
        SqlNode refNode = field(ref.getIndex());
        final SqlIdentifier id = (SqlIdentifier) refNode;
        if (id.names.size() > 1) {
          return id.setName(0, pv);
        } else {
          return new SqlIdentifier(ImmutableList.of(pv, id.names.get(0)), POS);
        }

      case LITERAL:
        final RexLiteral literal = (RexLiteral) rex;
        if (literal.getTypeName() == SqlTypeName.SYMBOL) {
          final Enum symbol = (Enum) literal.getValue();
          return SqlLiteral.createSymbol(symbol, POS);
        }
        switch (literal.getTypeName().getFamily()) {
        case CHARACTER:
          return SqlLiteral.createCharString((String) literal.getValue2(), POS);
        case NUMERIC:
        case EXACT_NUMERIC:
          return SqlLiteral.createExactNumeric(
              literal.getValueAs(BigDecimal.class).toString(), POS);
        case APPROXIMATE_NUMERIC:
          return SqlLiteral.createApproxNumeric(
              literal.getValueAs(BigDecimal.class).toString(), POS);
        case BOOLEAN:
          return SqlLiteral.createBoolean(literal.getValueAs(Boolean.class),
              POS);
        case INTERVAL_YEAR_MONTH:
        case INTERVAL_DAY_TIME:
          final boolean negative = literal.getValueAs(Boolean.class);
          return SqlLiteral.createInterval(negative ? -1 : 1,
              literal.getValueAs(String.class),
              literal.getType().getIntervalQualifier(), POS);
        case DATE:
          return SqlLiteral.createDate(literal.getValueAs(DateString.class),
              POS);
        case TIME:
          return SqlLiteral.createTime(literal.getValueAs(TimeString.class),
              literal.getType().getPrecision(), POS);
        case TIMESTAMP:
          return SqlLiteral.createTimestamp(
              literal.getValueAs(TimestampString.class),
              literal.getType().getPrecision(), POS);
        case ANY:
        case NULL:
          switch (literal.getTypeName()) {
          case NULL:
            return SqlLiteral.createNull(POS);
          // fall through
          }
        default:
          throw new AssertionError(literal + ": " + literal.getTypeName());
        }

      case CASE:
        final RexCall caseCall = (RexCall) rex;
        final List<SqlNode> caseNodeList =
            toSql(program, caseCall.getOperands());
        final SqlNode valueNode;
        final List<SqlNode> whenList = Expressions.list();
        final List<SqlNode> thenList = Expressions.list();
        final SqlNode elseNode;
        if (caseNodeList.size() % 2 == 0) {
          // switched:
          //   "case x when v1 then t1 when v2 then t2 ... else e end"
          valueNode = caseNodeList.get(0);
          for (int i = 1; i < caseNodeList.size() - 1; i += 2) {
            whenList.add(caseNodeList.get(i));
            thenList.add(caseNodeList.get(i + 1));
          }
        } else {
          // other: "case when w1 then t1 when w2 then t2 ... else e end"
          valueNode = null;
          for (int i = 0; i < caseNodeList.size() - 1; i += 2) {
            whenList.add(caseNodeList.get(i));
            thenList.add(caseNodeList.get(i + 1));
          }
        }
        elseNode = caseNodeList.get(caseNodeList.size() - 1);
        return new SqlCase(POS, valueNode, new SqlNodeList(whenList, POS),
            new SqlNodeList(thenList, POS), elseNode);

      case DYNAMIC_PARAM:
        final RexDynamicParam caseParam = (RexDynamicParam) rex;
        return new SqlDynamicParam(caseParam.getIndex(), POS);

      case IN:
        subQuery = (RexSubQuery) rex;
        sqlSubQuery = visitChild(0, subQuery.rel).asQueryOrValues();
        List<RexNode> operands = subQuery.operands;
        SqlNode op0;
        if (operands.size() == 1) {
          op0 = toSql(program, operands.get(0));
        } else {
          final List<SqlNode> cols = toSql(program, operands);
          op0 = new SqlNodeList(cols, POS);
        }
        return subQuery.getOperator().createCall(POS, op0, sqlSubQuery);

      case EXISTS:
      case SCALAR_QUERY:
        subQuery = (RexSubQuery) rex;
        sqlSubQuery = visitChild(0, subQuery.rel).asQueryOrValues();
        return subQuery.getOperator().createCall(POS, sqlSubQuery);

      case NOT:
        RexNode operand = ((RexCall) rex).operands.get(0);
        final SqlNode node = toSql(program, operand);
        switch (operand.getKind()) {
        case IN:
          return SqlStdOperatorTable.NOT_IN
              .createCall(POS, ((SqlCall) node).getOperandList());
        case LIKE:
          return SqlStdOperatorTable.NOT_LIKE
              .createCall(POS, ((SqlCall) node).getOperandList());
        case SIMILAR:
          return SqlStdOperatorTable.NOT_SIMILAR_TO
              .createCall(POS, ((SqlCall) node).getOperandList());
        default:
          return SqlStdOperatorTable.NOT.createCall(POS, node);
        }

      default:
        if (rex instanceof RexOver) {
          return toSql(program, (RexOver) rex);
        }

        final RexCall call = (RexCall) stripCastFromString(rex);
        SqlOperator op = call.getOperator();
        switch (op.getKind()) {
        case SUM0:
          op = SqlStdOperatorTable.SUM;
        }
        final List<SqlNode> nodeList = toSql(program, call.getOperands());
        switch (call.getKind()) {
        case CAST:
          if (ignoreCast) {
            assert nodeList.size() == 1;
            return nodeList.get(0);
          } else {
            nodeList.add(dialect.getCastSpec(call.getType()));
          }
        }
        if (op instanceof SqlBinaryOperator && nodeList.size() > 2) {
          // In RexNode trees, OR and AND have any number of children;
          // SqlCall requires exactly 2. So, convert to a left-deep binary tree.
          return createLeftCall(op, nodeList);
        }
        return op.createCall(new SqlNodeList(nodeList, POS));
      }
    }

    private SqlCall toSql(RexProgram program, RexOver rexOver) {
      final RexWindow rexWindow = rexOver.getWindow();
      final SqlNodeList partitionList = new SqlNodeList(
          toSql(program, rexWindow.partitionKeys), POS);

      ImmutableList.Builder<SqlNode> orderNodes = ImmutableList.builder();
      if (rexWindow.orderKeys != null) {
        for (RexFieldCollation rfc : rexWindow.orderKeys) {
          orderNodes.add(toSql(program, rfc));
        }
      }
      final SqlNodeList orderList =
          new SqlNodeList(orderNodes.build(), POS);

      final SqlLiteral isRows =
          SqlLiteral.createBoolean(rexWindow.isRows(), POS);

      final SqlNode lowerBound =
          createSqlWindowBound(rexWindow.getLowerBound());
      final SqlNode upperBound =
          createSqlWindowBound(rexWindow.getUpperBound());

      // null defaults to true.
      // During parsing the allowPartial == false (e.g. disallow partial)
      // is expand into CASE expression and is handled as a such.
      // Not sure if we can collapse this CASE expression back into
      // "disallow partial" and set the allowPartial = false.
      final SqlLiteral allowPartial = null;

      final SqlWindow sqlWindow = SqlWindow.create(null, null, partitionList,
          orderList, isRows, lowerBound, upperBound, allowPartial, POS);

      final List<SqlNode> nodeList = toSql(program, rexOver.getOperands());
      final SqlCall aggFunctionCall =
          rexOver.getAggOperator().createCall(POS, nodeList);

      return SqlStdOperatorTable.OVER.createCall(POS, aggFunctionCall,
          sqlWindow);
    }

    private SqlNode toSql(RexProgram program, RexFieldCollation rfc) {
      SqlNode node = toSql(program, rfc.left);
      switch (rfc.getDirection()) {
      case DESCENDING:
      case STRICTLY_DESCENDING:
        node = SqlStdOperatorTable.DESC.createCall(POS, node);
      }
      if (rfc.getNullDirection()
              != dialect.defaultNullDirection(rfc.getDirection())) {
        switch (rfc.getNullDirection()) {
        case FIRST:
          node = SqlStdOperatorTable.NULLS_FIRST.createCall(POS, node);
          break;
        case LAST:
          node = SqlStdOperatorTable.NULLS_LAST.createCall(POS, node);
          break;
        }
      }
      return node;
    }

    private SqlNode createSqlWindowBound(RexWindowBound rexWindowBound) {
      if (rexWindowBound.isCurrentRow()) {
        return SqlWindow.createCurrentRow(POS);
      }
      if (rexWindowBound.isPreceding()) {
        if (rexWindowBound.isUnbounded()) {
          return SqlWindow.createUnboundedPreceding(POS);
        } else {
          SqlNode literal = toSql(null, rexWindowBound.getOffset());
          return SqlWindow.createPreceding(literal, POS);
        }
      }
      if (rexWindowBound.isFollowing()) {
        if (rexWindowBound.isUnbounded()) {
          return SqlWindow.createUnboundedFollowing(POS);
        } else {
          SqlNode literal = toSql(null, rexWindowBound.getOffset());
          return SqlWindow.createFollowing(literal, POS);
        }
      }

      throw new AssertionError("Unsupported Window bound: "
          + rexWindowBound);
    }

    private SqlNode createLeftCall(SqlOperator op, List<SqlNode> nodeList) {
      if (nodeList.size() == 2) {
        return op.createCall(new SqlNodeList(nodeList, POS));
      }
      final List<SqlNode> butLast = Util.skipLast(nodeList);
      final SqlNode last = nodeList.get(nodeList.size() - 1);
      final SqlNode call = createLeftCall(op, butLast);
      return op.createCall(new SqlNodeList(ImmutableList.of(call, last), POS));
    }

    private List<SqlNode> toSql(RexProgram program, List<RexNode> operandList) {
      final List<SqlNode> list = new ArrayList<>();
      for (RexNode rex : operandList) {
        list.add(toSql(program, rex));
      }
      return list;
    }

    public List<SqlNode> fieldList() {
      return new AbstractList<SqlNode>() {
        public SqlNode get(int index) {
          return field(index);
        }

        public int size() {
          return fieldCount;
        }
      };
    }

    /** Converts a call to an aggregate function to an expression. */
    public SqlNode toSql(AggregateCall aggCall) {
      SqlOperator op = aggCall.getAggregation();
      if (op instanceof SqlSumEmptyIsZeroAggFunction) {
        op = SqlStdOperatorTable.SUM;
      }
      final List<SqlNode> operands = Expressions.list();
      for (int arg : aggCall.getArgList()) {
        operands.add(field(arg));
      }
      return op.createCall(
          aggCall.isDistinct() ? SqlSelectKeyword.DISTINCT.symbol(POS) : null,
          POS, operands.toArray(new SqlNode[operands.size()]));
    }

    /** Converts a collation to an ORDER BY item. */
    public SqlNode toSql(RelFieldCollation collation) {
      SqlNode node = field(collation.getFieldIndex());
      switch (collation.getDirection()) {
      case DESCENDING:
      case STRICTLY_DESCENDING:
        node = SqlStdOperatorTable.DESC.createCall(POS, node);
      }
      if (collation.nullDirection != dialect.defaultNullDirection(collation.direction)) {
        switch (collation.nullDirection) {
        case FIRST:
          node = SqlStdOperatorTable.NULLS_FIRST.createCall(POS, node);
          break;
        case LAST:
          node = SqlStdOperatorTable.NULLS_LAST.createCall(POS, node);
          break;
        }
      }
      return node;
    }

    public SqlImplementor implementor() {
      return SqlImplementor.this;
    }
  }

  private static int computeFieldCount(
      Map<String, RelDataType> aliases) {
    int x = 0;
    for (RelDataType type : aliases.values()) {
      x += type.getFieldCount();
    }
    return x;
  }

  public Context aliasContext(Map<String, RelDataType> aliases,
      boolean qualified) {
    return new AliasContext(aliases, qualified);
  }

  public Context joinContext(Context leftContext, Context rightContext) {
    return new JoinContext(leftContext, rightContext);
  }

  public Context matchRecognizeContext(Context context) {
    return new MatchRecognizeContext(((AliasContext) context).aliases);
  }

  /**
   * Context for translating MATCH_RECOGNIZE clause
   */
  public class MatchRecognizeContext extends AliasContext {
    protected MatchRecognizeContext(Map<String, RelDataType> aliases) {
      super(aliases, false);
    }

    @Override public SqlNode toSql(RexProgram program, RexNode rex) {
      if (rex.getKind() == SqlKind.LITERAL) {
        final RexLiteral literal = (RexLiteral) rex;
        if (literal.getTypeName().getFamily() == SqlTypeFamily.CHARACTER) {
          return new SqlIdentifier(RexLiteral.stringValue(literal), POS);
        }
      }
      return super.toSql(program, rex);
    }
  }

  /** Implementation of Context that precedes field references with their
   * "table alias" based on the current sub-query's FROM clause. */
  public class AliasContext extends Context {
    private final boolean qualified;
    private final Map<String, RelDataType> aliases;

    /** Creates an AliasContext; use {@link #aliasContext(Map, boolean)}. */
    protected AliasContext(Map<String, RelDataType> aliases,
        boolean qualified) {
      super(computeFieldCount(aliases));
      this.aliases = aliases;
      this.qualified = qualified;
    }

    public SqlNode field(int ordinal) {
      for (Map.Entry<String, RelDataType> alias : aliases.entrySet()) {
        final List<RelDataTypeField> fields = alias.getValue().getFieldList();
        if (ordinal < fields.size()) {
          RelDataTypeField field = fields.get(ordinal);
          final SqlNode mappedSqlNode =
              ordinalMap.get(field.getName().toLowerCase(Locale.ROOT));
          if (mappedSqlNode != null) {
            return mappedSqlNode;
          }
          return new SqlIdentifier(!qualified
              ? ImmutableList.of(field.getName())
              : ImmutableList.of(alias.getKey(), field.getName()),
              POS);
        }
        ordinal -= fields.size();
      }
      throw new AssertionError(
          "field ordinal " + ordinal + " out of range " + aliases);
    }
  }

  /** Context for translating ON clause of a JOIN from {@link RexNode} to
   * {@link SqlNode}. */
  class JoinContext extends Context {
    private final SqlImplementor.Context leftContext;
    private final SqlImplementor.Context rightContext;

    /** Creates a JoinContext; use {@link #joinContext(Context, Context)}. */
    private JoinContext(Context leftContext, Context rightContext) {
      super(leftContext.fieldCount + rightContext.fieldCount);
      this.leftContext = leftContext;
      this.rightContext = rightContext;
    }

    public SqlNode field(int ordinal) {
      if (ordinal < leftContext.fieldCount) {
        return leftContext.field(ordinal);
      } else {
        return rightContext.field(ordinal - leftContext.fieldCount);
      }
    }
  }

  /** Result of implementing a node. */
  public class Result {
    final SqlNode node;
    private final String neededAlias;
    private final RelDataType neededType;
    private final Map<String, RelDataType> aliases;
    final Expressions.FluentList<Clause> clauses;

    public Result(SqlNode node, Collection<Clause> clauses, String neededAlias,
        RelDataType neededType, Map<String, RelDataType> aliases) {
      this.node = node;
      this.neededAlias = neededAlias;
      this.neededType = neededType;
      this.aliases = aliases;
      this.clauses = Expressions.list(clauses);
    }

    /** Once you have a Result of implementing a child relational expression,
     * call this method to create a Builder to implement the current relational
     * expression by adding additional clauses to the SQL query.
     *
     * <p>You need to declare which clauses you intend to add. If the clauses
     * are "later", you can add to the same query. For example, "GROUP BY" comes
     * after "WHERE". But if they are the same or earlier, this method will
     * start a new SELECT that wraps the previous result.
     *
     * <p>When you have called
     * {@link Builder#setSelect(SqlNodeList)},
     * {@link Builder#setWhere(SqlNode)} etc. call
     * {@link Builder#result(SqlNode, Collection, RelNode, Map)}
     * to fix the new query.
     *
     * @param rel Relational expression being implemented
     * @param clauses Clauses that will be generated to implement current
     *                relational expression
     * @return A builder
     */
    public Builder builder(RelNode rel, Clause... clauses) {
      final Clause maxClause = maxClause();
      boolean needNew = false;
      // If old and new clause are equal and belong to below set,
      // then new SELECT wrap is not required
      Set<Clause> nonWrapSet = ImmutableSet.of(Clause.SELECT);
      for (Clause clause : clauses) {
        if (maxClause.ordinal() > clause.ordinal()
            || (maxClause == clause && !nonWrapSet.contains(clause))) {
          needNew = true;
        }
      }
      if (rel instanceof LogicalAggregate
          && !dialect.supportsNestedAggregations()
          && hasNestedAggregations((LogicalAggregate) rel)) {
        needNew = true;
      }

      SqlSelect select;
      Expressions.FluentList<Clause> clauseList = Expressions.list();
      if (needNew) {
        select = subSelect();
      } else {
        select = asSelect();
        clauseList.addAll(this.clauses);
      }
      clauseList.appendAll(clauses);
      Context newContext;
      final SqlNodeList selectList = select.getSelectList();
      if (selectList != null) {
        newContext = new Context(selectList.size()) {
          public SqlNode field(int ordinal) {
            final SqlNode selectItem = selectList.get(ordinal);
            switch (selectItem.getKind()) {
            case AS:
              return ((SqlCall) selectItem).operand(0);
            }
            return selectItem;
          }
        };
      } else {
        boolean qualified =
            !dialect.hasImplicitTableAlias() || aliases.size() > 1;
        // basically, we did a subSelect() since needNew is set and neededAlias is not null
        // now, we need to make sure that we need to update the alias context.
        // if our aliases map has a single element:  <neededAlias, rowType>,
        // then we don't need to rewrite the alias but otherwise, it should be updated.
        if (needNew
                && neededAlias != null
                && (aliases.size() != 1 || !aliases.containsKey(neededAlias))) {
          final Map<String, RelDataType> newAliases =
              ImmutableMap.of(neededAlias, rel.getInput(0).getRowType());
          newContext = aliasContext(newAliases, qualified);
        } else {
          newContext = aliasContext(aliases, qualified);
        }
      }
      return new Builder(rel, clauseList, select, newContext,
          needNew ? null : aliases);
    }

    private boolean hasNestedAggregations(LogicalAggregate rel) {
      List<AggregateCall> aggCallList = rel.getAggCallList();
      HashSet<Integer> aggregatesArgs = new HashSet<>();
      for (AggregateCall aggregateCall: aggCallList) {
        aggregatesArgs.addAll(aggregateCall.getArgList());
      }
      for (Integer aggregatesArg : aggregatesArgs) {
        SqlNode selectNode = ((SqlSelect) node).getSelectList().get(aggregatesArg);
        if (!(selectNode instanceof SqlBasicCall)) {
          continue;
        }
        for (SqlNode operand : ((SqlBasicCall) selectNode).getOperands()) {
          if (operand instanceof SqlCall) {
            final SqlOperator operator = ((SqlCall) operand).getOperator();
            if (operator instanceof SqlAggFunction) {
              return true;
            }
          }
        }
      }
      return false;
    }

    // make private?
    public Clause maxClause() {
      Clause maxClause = null;
      for (Clause clause : clauses) {
        if (maxClause == null || clause.ordinal() > maxClause.ordinal()) {
          maxClause = clause;
        }
      }
      assert maxClause != null;
      return maxClause;
    }

    /** Returns a node that can be included in the FROM clause or a JOIN. It has
     * an alias that is unique within the query. The alias is implicit if it
     * can be derived using the usual rules (For example, "SELECT * FROM emp" is
     * equivalent to "SELECT * FROM emp AS emp".) */
    public SqlNode asFrom() {
      if (neededAlias != null) {
        return SqlStdOperatorTable.AS.createCall(POS, node,
            new SqlIdentifier(neededAlias, POS));
      }
      return node;
    }

    public SqlSelect subSelect() {
      return wrapSelect(asFrom());
    }

    /** Converts a non-query node into a SELECT node. Set operators (UNION,
     * INTERSECT, EXCEPT) remain as is. */
    public SqlSelect asSelect() {
      if (node instanceof SqlSelect) {
        return (SqlSelect) node;
      }
      if (!dialect.hasImplicitTableAlias()) {
        return wrapSelect(asFrom());
      }
      return wrapSelect(node);
    }

    /** Converts a non-query node into a SELECT node. Set operators (UNION,
     * INTERSECT, EXCEPT) and DML operators (INSERT, UPDATE, DELETE, MERGE)
     * remain as is. */
    public SqlNode asStatement() {
      switch (node.getKind()) {
      case UNION:
      case INTERSECT:
      case EXCEPT:
      case INSERT:
      case UPDATE:
      case DELETE:
      case MERGE:
        return node;
      default:
        return asSelect();
      }
    }

    /** Converts a non-query node into a SELECT node. Set operators (UNION,
     * INTERSECT, EXCEPT) and VALUES remain as is. */
    public SqlNode asQueryOrValues() {
      switch (node.getKind()) {
      case UNION:
      case INTERSECT:
      case EXCEPT:
      case VALUES:
        return node;
      default:
        return asSelect();
      }
    }

    /** Returns a context that always qualifies identifiers. Useful if the
     * Context deals with just one arm of a join, yet we wish to generate
     * a join condition that qualifies column names to disambiguate them. */
    public Context qualifiedContext() {
      return aliasContext(aliases, true);
    }

    /**
     * In join, when the left and right nodes have been generated,
     * update their alias with 'neededAlias' if not null.
     */
    public Result resetAlias() {
      if (neededAlias == null) {
        return this;
      } else {
        return new Result(node, clauses, neededAlias, neededType,
            ImmutableMap.<String, RelDataType>of(neededAlias, neededType));
      }
    }
  }

  /** Builder. */
  public class Builder {
    private final RelNode rel;
    final List<Clause> clauses;
    final SqlSelect select;
    public final Context context;
    private final Map<String, RelDataType> aliases;

    public Builder(RelNode rel, List<Clause> clauses, SqlSelect select,
        Context context, Map<String, RelDataType> aliases) {
      this.rel = rel;
      this.clauses = clauses;
      this.select = select;
      this.context = context;
      this.aliases = aliases;
    }

    public void setSelect(SqlNodeList nodeList) {
      select.setSelectList(nodeList);
    }

    public void setWhere(SqlNode node) {
      assert clauses.contains(Clause.WHERE);
      select.setWhere(node);
    }

    public void setGroupBy(SqlNodeList nodeList) {
      assert clauses.contains(Clause.GROUP_BY);
      select.setGroupBy(nodeList);
    }

    public void setHaving(SqlNode node) {
      assert clauses.contains(Clause.HAVING);
      select.setHaving(node);
    }

    public void setOrderBy(SqlNodeList nodeList) {
      assert clauses.contains(Clause.ORDER_BY);
      select.setOrderBy(nodeList);
    }

    public void setFetch(SqlNode fetch) {
      assert clauses.contains(Clause.FETCH);
      select.setFetch(fetch);
    }

    public void setOffset(SqlNode offset) {
      assert clauses.contains(Clause.OFFSET);
      select.setOffset(offset);
    }

    public void addOrderItem(List<SqlNode> orderByList,
        RelFieldCollation field) {
      if (field.nullDirection != RelFieldCollation.NullDirection.UNSPECIFIED) {
        boolean first = field.nullDirection == RelFieldCollation.NullDirection.FIRST;
        SqlNode nullDirectionNode =
            dialect.emulateNullDirection(context.field(field.getFieldIndex()),
                first, field.direction.isDescending());
        if (nullDirectionNode != null) {
          orderByList.add(nullDirectionNode);
          field = new RelFieldCollation(field.getFieldIndex(),
              field.getDirection(),
              RelFieldCollation.NullDirection.UNSPECIFIED);
        }
      }
      orderByList.add(context.toSql(field));
    }

    public Result result() {
      return SqlImplementor.this.result(select, clauses, rel, aliases);
    }
  }

  /** Clauses in a SQL query. Ordered by evaluation order.
   * SELECT is set only when there is a NON-TRIVIAL SELECT clause. */
  public enum Clause {
    FROM, WHERE, GROUP_BY, HAVING, SELECT, SET_OP, ORDER_BY, FETCH, OFFSET
  }
}

// End SqlImplementor.java
