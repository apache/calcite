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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Limit;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.fun.SqlSumEmptyIsZeroAggFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Utility to convert RelNodes to SqlNode
 */
public class RelToSqlConverter {

  private static final Logger LOGGER = Logger.getLogger(RelToSqlConverter.class.getName());

  public static final SqlParserPos POS = SqlParserPos.ZERO;

  final SqlDialect dialect;
  private final Set<String> aliasSet = new LinkedHashSet<String>();

  private final Map<String, SqlNode> ordinalMap = new HashMap<>();

  public RelToSqlConverter(SqlDialect dialect) {
    this.dialect = dialect;
  }

  /**
   * Creates a result based on a single relational expression.
   */
  public Result result(SqlNode node, Collection<Clause> clauses, RelNode rel) {
    final String alias2 = SqlValidatorUtil.getAlias(node, -1);
    final String alias3 = alias2 != null ? alias2 : "t";
    final String alias4 =
        SqlValidatorUtil.uniquify(
            alias3, aliasSet, SqlValidatorUtil.EXPR_SUGGESTER);
    final String alias5 = alias2 == null || !alias2.equals(alias4) ? alias4
        : null;
    return new Result(node, clauses, alias5,
        Collections.singletonList(Pair.of(alias4, rel.getRowType())));
  }

  /**
   * Creates a result based on a join. (Each join could contain one or more
   * relational expressions.)
   */
  public Result result(SqlNode join, Result leftResult, Result rightResult) {
    final List<Pair<String, RelDataType>> list =
        new ArrayList<Pair<String, RelDataType>>();
    list.addAll(leftResult.aliases);
    list.addAll(rightResult.aliases);
    return new Result(join, Expressions.list(Clause.FROM), null, list);
  }

  /**
   * Wraps a node in a SELECT statement that has no clauses:
   * "SELECT ... FROM (node)".
   */
  SqlSelect wrapSelect(SqlNode node) {
    assert node instanceof SqlJoin
        || node instanceof SqlIdentifier
        || node instanceof SqlCall
        && (((SqlCall) node).getOperator() instanceof SqlSetOperator
        || ((SqlCall) node).getOperator() == SqlStdOperatorTable.AS)
        : node;
    return new SqlSelect(POS, SqlNodeList.EMPTY, null, node, null, null, null,
        SqlNodeList.EMPTY, null, null, null);
  }

  public Result visitChild(int i, RelNode e) {
    if (e instanceof Union) {
      return visitUnion((Union) e);
    } else if (e instanceof Join) {
      return visitJoin((Join) e);
    } else if (e instanceof Filter) {
      return visitFilter((Filter) e);
    } else if (e instanceof Project) {
      return visitProject((Project) e);
    } else if (e instanceof Aggregate) {
      return visitAggregate((Aggregate) e);
    } else if (e instanceof TableScan) {
      return visitTableScan((TableScan) e);
    } else if (e instanceof Intersect) {
      return visitIntersect((Intersect) e);
    } else if (e instanceof Minus) {
      return visitMinus((Minus) e);
    } else if (e instanceof Calc) {
      return visitCalc((Calc) e);
    } else if (e instanceof Sort) {
      return visitSort((Sort) e);
    } else if (e instanceof TableModify) {
      return visitTableModify((TableModify) e);
    } else if (e instanceof Limit) {
      return visitLimit((Limit) e);
    } else {
      throw new AssertionError("Need to Implement for " + e.getClass().getName()); // TODO:
    }
  }

  public Result visitLimit(Limit e) {
    final Result x = visitChild(0, e.getInput());
    final Builder builder = x.builder(e, Clause.FETCH, Clause.OFFSET);
    final SqlNode fetch = builder.context.toSql(null, e.getFetch());
    builder.setfetch(fetch);
    if (e.getOffset() != null) {
      final SqlNode offset = builder.context.toSql(null, e.getOffset());
      builder.setOffset(offset);
    }
    return builder.result();
  }

  public Result visitUnion(Union e) {
    final SqlSetOperator operator = e.all
        ? SqlStdOperatorTable.UNION_ALL
        : SqlStdOperatorTable.UNION;
    return setOpToSql(operator, e);
  }

  public Result visitJoin(Join e) {
    final Result leftResult = visitChild(0, e.getLeft());
    final Result rightResult = visitChild(1, e.getRight());
    final Context leftContext = leftResult.qualifiedContext();
    final Context rightContext =
        rightResult.qualifiedContext();
    SqlNode sqlCondition = convertConditionToSqlNode(e.getCondition(),
        leftContext,
        rightContext,
        e.getLeft().getRowType().getFieldCount());
    SqlNode join =
        new SqlJoin(POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            joinType(e.getJoinType()).symbol(POS),
            rightResult.asFrom(),
            JoinConditionType.ON.symbol(POS),
            sqlCondition);
    return result(join, leftResult, rightResult);
  }

  public Result visitFilter(Filter e) {
    Result x = visitChild(0, e.getInput());
    final Builder builder =
        x.builder(e, Clause.WHERE);
    builder.setWhere(builder.context.toSql(null, e.getCondition()));
    return builder.result();
  }

  public Result visitProject(Project e) {
    Result x = visitChild(0, e.getInput());
    if (isStar(e.getChildExps(), e.getInput().getRowType())) {
      return x;
    }
    final Builder builder =
        x.builder(e, Clause.SELECT);
    final List<SqlNode> selectList = new ArrayList<>();
    for (RexNode ref : e.getChildExps()) {
      SqlNode sqlExpr = builder.context.toSql(null, ref);
      addSelect(selectList, sqlExpr, e.getRowType());
    }

    builder.setSelect(new SqlNodeList(selectList, POS));
    return builder.result();
  }


  public Result visitAggregate(Aggregate e) {
    // "select a, b, sum(x) from ( ... ) group by a, b"
    final Result x = visitChild(0, e.getInput());
    final Builder builder;
    if (e.getInput() instanceof Project) {
      builder = x.builder(e);
      builder.clauses.add(Clause.GROUP_BY);
    } else {
      builder = x.builder(e, Clause.GROUP_BY);
    }
    List<SqlNode> groupByList = Expressions.list();
    final List<SqlNode> selectList = new ArrayList<>();
    for (int group : e.getGroupSet()) {
      final SqlNode field = builder.context.field(group);
      addSelect(selectList, field, e.getRowType());
      groupByList.add(field);
    }
    for (AggregateCall aggCall : e.getAggCallList()) {
      SqlNode aggCallSqlNode = builder.context.toSql(aggCall);
      if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
        aggCallSqlNode =
            rewriteSingleValueExpr(aggCallSqlNode, dialect);
      }
      addSelect(selectList, aggCallSqlNode, e.getRowType());
    }
    builder.setSelect(new SqlNodeList(selectList, POS));
    if (!groupByList.isEmpty() || e.getAggCallList().isEmpty()) {
      // Some databases don't support "GROUP BY ()". We can omit it as long
      // as there is at least one aggregate function.
      builder.setGroupBy(new SqlNodeList(groupByList, POS));
    }
    return builder.result();
  }

  public Result visitTableScan(TableScan e) {
    return result(new SqlIdentifier(e.getTable().getQualifiedName(), SqlParserPos.ZERO),
        Collections.singletonList(Clause.FROM), e);
  }

  public Result visitIntersect(Intersect e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.INTERSECT_ALL
        : SqlStdOperatorTable.INTERSECT, e);
  }

  public Result visitMinus(Minus e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.EXCEPT_ALL
        : SqlStdOperatorTable.EXCEPT, e);
  }

  public Result visitCalc(Calc e) {
    Result x = visitChild(0, e.getInput());
    final RexProgram program = e.getProgram();
    Builder builder =
        program.getCondition() != null
            ? x.builder(e, Clause.WHERE)
            : x.builder(e);
    if (!isStar(program)) {
      final List<SqlNode> selectList = new ArrayList<>();
      for (RexLocalRef ref : program.getProjectList()) {
        SqlNode sqlExpr = builder.context.toSql(program, ref);
        addSelect(selectList, sqlExpr, e.getRowType());
      }
      builder.setSelect(new SqlNodeList(selectList, POS));
    }

    if (program.getCondition() != null) {
      builder.setWhere(
          builder.context.toSql(program, program.getCondition()));
    }
    return builder.result();
  }

  public Result visitValues(Values e) {
    final List<String> fields = e.getRowType().getFieldNames();
    final List<Clause> clauses = Collections.singletonList(Clause.SELECT);
    final Context context =
        new AliasContext(Collections.<Pair<String, RelDataType>>emptyList(), false);
    final List<SqlSelect> selects = new ArrayList<>();
    for (List<RexLiteral> tuple : e.getTuples()) {
      final List<SqlNode> selectList = new ArrayList<>();
      for (Pair<RexLiteral, String> literal : Pair.zip(tuple, fields)) {
        selectList.add(
            SqlStdOperatorTable.AS.createCall(
                POS,
                context.toSql(null, literal.left),
                new SqlIdentifier(literal.right, POS)));
      }
      selects.add(
          new SqlSelect(POS, SqlNodeList.EMPTY,
              new SqlNodeList(selectList, POS), null, null, null,
              null, null, null, null, null));
    }
    SqlNode query = null;
    for (SqlSelect select : selects) {
      if (query == null) {
        query = select;
      } else {
        query = SqlStdOperatorTable.UNION_ALL.createCall(POS, query,
            select);
      }
    }
    return result(query, clauses, e);
  }

  public Result visitSort(Sort e) {
    final Result x = visitChild(0, e.getInput());
    final Builder builder = x.builder(e, Clause.ORDER_BY);
    List<SqlNode> orderByList = Expressions.list();
    for (RelFieldCollation fieldCollation : e.getCollation().getFieldCollations()) {
      if (fieldCollation.nullDirection != RelFieldCollation.NullDirection.UNSPECIFIED
          && dialect.getDatabaseProduct() == SqlDialect.DatabaseProduct.MYSQL) {
        orderByList.add(
            ISNULL_FUNCTION.createCall(POS,
                builder.context.field(fieldCollation.getFieldIndex())));
        fieldCollation = new RelFieldCollation(fieldCollation.getFieldIndex(),
            fieldCollation.getDirection());
      }
      orderByList.add(builder.context.toSql(fieldCollation));
    }
    builder.setOrderBy(new SqlNodeList(orderByList, POS));
    return builder.result();
  }

  public Result visitTableModify(TableModify e) {
    throw new AssertionError(); // TODO:
  }

  /**
   * MySQL specific function.
   */
  private static final SqlFunction ISNULL_FUNCTION =
      new SqlFunction("ISNULL", SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN, InferTypes.FIRST_KNOWN,
          OperandTypes.ANY, SqlFunctionCategory.SYSTEM);

  /**
   * Rewrite SINGLE_VALUE into expression based on database variants
   * E.g. HSQLDB, MYSQL, ORACLE, etc
   */
  private SqlNode rewriteSingleValueExpr(SqlNode aggCall,
                                         SqlDialect sqlDialect) {
    final SqlNode operand = ((SqlBasicCall) aggCall).operand(0);
    final SqlNode caseOperand;
    final SqlNode elseExpr;
    final SqlNode countCall =
        SqlStdOperatorTable.COUNT.createCall(POS, operand);

    final SqlLiteral nullLiteral = SqlLiteral.createNull(POS);
    final SqlNode wrappedOperand;
    switch (sqlDialect.getDatabaseProduct()) {
    case MYSQL:
    case HSQLDB:
      caseOperand = countCall;
      final SqlNodeList selectList = new SqlNodeList(POS);
      selectList.add(nullLiteral);
      final SqlNode unionOperand;
      switch (sqlDialect.getDatabaseProduct()) {
      case MYSQL:
        wrappedOperand = operand;
        unionOperand = new SqlSelect(POS, SqlNodeList.EMPTY, selectList,
            null, null, null, null, SqlNodeList.EMPTY, null, null, null);
        break;
      default:
        wrappedOperand = SqlStdOperatorTable.MIN.createCall(POS, operand);
        unionOperand = SqlStdOperatorTable.VALUES.createCall(POS,
            SqlLiteral.createApproxNumeric("0", POS));
      }

      SqlCall unionAll = SqlStdOperatorTable.UNION_ALL
          .createCall(POS, unionOperand, unionOperand);

      final SqlNodeList subQuery = new SqlNodeList(POS);
      subQuery.add(unionAll);

      final SqlNodeList selectList2 = new SqlNodeList(POS);
      selectList2.add(nullLiteral);
      elseExpr = SqlStdOperatorTable.SCALAR_QUERY.createCall(POS, subQuery);
      break;

    default:
      LOGGER.info("SINGLE_VALUE rewrite not supported for "
          + sqlDialect.getDatabaseProduct());
      return aggCall;
    }

    final SqlNodeList whenList = new SqlNodeList(POS);
    whenList.add(SqlLiteral.createExactNumeric("0", POS));
    whenList.add(SqlLiteral.createExactNumeric("1", POS));

    final SqlNodeList thenList = new SqlNodeList(POS);
    thenList.add(nullLiteral);
    thenList.add(wrappedOperand);

    SqlNode caseExpr =
        new SqlCase(POS, caseOperand, whenList, thenList, elseExpr);

    LOGGER.info("SINGLE_VALUE rewritten into [" + caseExpr + "]");

    return caseExpr;
  }

  private void addSelect(
      List<SqlNode> selectList, SqlNode node, RelDataType rowType) {
    String name = rowType.getFieldNames().get(selectList.size());
    String alias = SqlValidatorUtil.getAlias(node, -1);
    if (name.toLowerCase().startsWith("expr$")) {
      //Put it in ordinalMap
      ordinalMap.put(name.toLowerCase(), node);
    } else if (alias == null || !alias.equals(name)) {
      node = SqlStdOperatorTable.AS.createCall(
          POS, node, new SqlIdentifier(name, POS));
    }
    selectList.add(node);
  }

  private static boolean isStar(List<RexNode> exps, RelDataType inputRowType) {
    int i = 0;
    for (RexNode ref : exps) {
      if (!(ref instanceof RexInputRef)) {
        return false;
      } else if (((RexInputRef) ref).getIndex() != i++) {
        return false;
      }
    }
    return i == inputRowType.getFieldCount();
  }

  private static boolean isStar(RexProgram program) {
    int i = 0;

    for (RexLocalRef ref : program.getProjectList()) {
      if (ref.getIndex() != i++) {
        return false;
      }
    }
    return i == program.getInputRowType().getFieldCount();
  }

  private Result setOpToSql(SqlSetOperator operator, RelNode rel) {
    List<SqlNode> list = Expressions.list();
    for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
      final Result result = this.visitChild(input.i, input.e);
      list.add(result.asSelect());
    }
    final SqlCall node = operator.createCall(new SqlNodeList(list, POS));
    final List<Clause> clauses =
        Expressions.list(Clause.SET_OP);
    return result(node, clauses, rel);
  }

  /**
   * Convert {@link RexNode} condition into {@link SqlNode}
   *
   * @param node           condition Node
   * @param leftContext    LeftContext
   * @param rightContext   RightContext
   * @param leftFieldCount Number of field on left result
   * @return SqlJoin which represent the condition
   */
  private SqlNode convertConditionToSqlNode(RexNode node,
                                            Context leftContext,
                                            Context rightContext,
                                            int leftFieldCount) {
    if (!(node instanceof RexCall)) {
      throw new AssertionError(node);
    }
    final List<RexNode> operands;
    final SqlOperator op;
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
      operands = ((RexCall) node).getOperands();
      op = ((RexCall) node).getOperator();
      if (operands.get(0) instanceof RexInputRef
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
    }
    throw new AssertionError(node);
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

  private static JoinType joinType(JoinRelType joinType) {
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

  /**
   * Context for translating a {@link RexNode} expression (within a
   * {@link RelNode}) into a {@link SqlNode} expression (within a SQL parse
   * tree).
   */
  public abstract class Context {
    private final int fieldCount;
    private final boolean ignoreCast;

    protected Context(int fieldCount) {
      this(fieldCount, true);
    }

    protected Context(int fieldCount, boolean ignoreCast) {
      this.fieldCount = fieldCount;
      this.ignoreCast = ignoreCast;
    }

    public abstract SqlNode field(int ordinal);

    /**
     * Converts an expression from {@link RexNode} to {@link SqlNode}
     * format.
     */
    SqlNode toSql(RexProgram program, RexNode rex) {
      switch (rex.getKind()) {
      case LOCAL_REF:
        final int index = ((RexLocalRef) rex).getIndex();
        return toSql(program, program.getExprList().get(index));

      case INPUT_REF:
        return field(((RexInputRef) rex).getIndex());

      case LITERAL:
        final RexLiteral literal = (RexLiteral) rex;
        if (literal.getTypeName() == SqlTypeName.SYMBOL) {
          final SqlLiteral.SqlSymbol symbol =
              (SqlLiteral.SqlSymbol) literal.getValue();
          return SqlLiteral.createSymbol(symbol, POS);
        }
        switch (literal.getTypeName().getFamily()) {
        case CHARACTER:
          return SqlLiteral.createCharString((String) literal.getValue2(), POS);
        case NUMERIC:
        case EXACT_NUMERIC:
          return SqlLiteral.createExactNumeric(literal.getValue().toString(),
              POS);
        case APPROXIMATE_NUMERIC:
          return SqlLiteral.createApproxNumeric(
              literal.getValue().toString(), POS);
        case BOOLEAN:
          return SqlLiteral.createBoolean((Boolean) literal.getValue(), POS);
        case DATE:
          return SqlLiteral.createDate((Calendar) literal.getValue(), POS);
        case TIME:
          return SqlLiteral.createTime((Calendar) literal.getValue(),
              literal.getType().getPrecision(), POS);
        case TIMESTAMP:
          return SqlLiteral.createTimestamp((Calendar) literal.getValue(),
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

      default:
        final RexCall call = (RexCall) rex;
        final SqlOperator op = call.getOperator();
        final List<SqlNode> nodeList = toSql(program, call.getOperands());
        switch (rex.getKind()) {
        case CAST:
          if (!ignoreCast) {
            nodeList.add(toSql(call.getType()));
          } else {
            assert nodeList.size() == 1;
            return nodeList.get(0);
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

    private SqlNode createLeftCall(SqlOperator op, List<SqlNode> nodeList) {
      if (nodeList.size() == 2) {
        return op.createCall(new SqlNodeList(nodeList, POS));
      }
      final List<SqlNode> butLast = Util.skipLast(nodeList);
      final SqlNode last = nodeList.get(nodeList.size() - 1);
      final SqlNode call = createLeftCall(op, butLast);
      return op.createCall(new SqlNodeList(ImmutableList.of(call, last), POS));
    }

    private SqlNode toSql(RelDataType type) {
      switch (dialect.getDatabaseProduct()) {
      case MYSQL:
        switch (type.getSqlTypeName()) {
        case VARCHAR:
          // MySQL doesn't have a VARCHAR type, only CHAR.
          return new SqlDataTypeSpec(new SqlIdentifier("CHAR", POS),
              type.getPrecision(), -1, null, null, POS);
        case INTEGER:
          return new SqlDataTypeSpec(new SqlIdentifier("_UNSIGNED", POS),
              type.getPrecision(), -1, null, null, POS);
        }
        break;
      }
      if (type instanceof BasicSqlType) {
        return new SqlDataTypeSpec(
            new SqlIdentifier(type.getSqlTypeName().name(), POS),
            type.getPrecision(),
            type.getScale(),
            type.getCharset() != null
                && dialect.supportsCharSet()
                ? type.getCharset().name()
                : null,
            null,
            POS);
      }

      return SqlTypeUtil.convertTypeToSpec(type);
      //throw new AssertionError(type); // TODO: implement
    }

    private List<SqlNode> toSql(RexProgram program, List<RexNode> operandList) {
      final List<SqlNode> list = new ArrayList<SqlNode>();
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

    /**
     * Converts a call to an aggregate function to an expression.
     */
    public SqlNode toSql(AggregateCall aggCall) {
      SqlOperator op = (SqlAggFunction) aggCall.getAggregation();
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

    /**
     * Converts a collation to an ORDER BY item.
     */
    public SqlNode toSql(RelFieldCollation collation) {
      SqlNode node = field(collation.getFieldIndex());
      switch (collation.getDirection()) {
      case DESCENDING:
      case STRICTLY_DESCENDING:
        node = SqlStdOperatorTable.DESC.createCall(POS, node);
      }
      switch (collation.nullDirection) {
      case FIRST:
        node = SqlStdOperatorTable.NULLS_FIRST.createCall(POS, node);
        break;
      case LAST:
        node = SqlStdOperatorTable.NULLS_LAST.createCall(POS, node);
        break;
      }
      return node;
    }
  }

  private static int computeFieldCount(
      List<Pair<String, RelDataType>> aliases) {
    int x = 0;
    for (Pair<String, RelDataType> alias : aliases) {
      x += alias.right.getFieldCount();
    }
    return x;
  }

  /**
   * Implementation of Context that precedes field references with their
   * "table alias" based on the current sub-query's FROM clause.
   */
  public class AliasContext extends Context {
    private final boolean qualified;
    private final List<Pair<String, RelDataType>> aliases;

    public AliasContext(List<Pair<String, RelDataType>> aliases,
                        boolean qualified) {
      super(computeFieldCount(aliases));
      this.aliases = aliases;
      this.qualified = qualified;
    }

    public SqlNode field(int ordinal) {
      for (Pair<String, RelDataType> alias : aliases) {
        final List<RelDataTypeField> fields = alias.right.getFieldList();
        if (ordinal < fields.size()) {
          RelDataTypeField field = fields.get(ordinal);
          final SqlNode mappedSqlNode =
              ordinalMap.get(field.getName().toLowerCase());
          if (mappedSqlNode != null) {
            return mappedSqlNode;
          } else {
            return new SqlIdentifier(!qualified
                ? ImmutableList.of(field.getName())
                : ImmutableList.of(alias.left, field.getName()),
                POS);
          }
        }
        ordinal -= fields.size();
      }
      throw new AssertionError(
          "field ordinal " + ordinal + " out of range " + aliases);
    }
  }

  /**
   * Result of implementing a node.
   */
  public class Result {
    final SqlNode node;
    private final String neededAlias;
    private final List<Pair<String, RelDataType>> aliases;
    final Expressions.FluentList<Clause> clauses;

    private Result(SqlNode node, Collection<Clause> clauses, String neededAlias,
                   List<Pair<String, RelDataType>> aliases) {
      this.node = node;
      this.neededAlias = neededAlias;
      this.aliases = aliases;
      this.clauses = Expressions.list(clauses);
    }

    /**
     * Once you have a Result of implementing a child relational expression,
     * call this method to create a Builder to implement the current relational
     * expression by adding additional clauses to the SQL query.
     * <p/>
     * <p>You need to declare which clauses you intend to add. If the clauses
     * are "later", you can add to the same query. For example, "GROUP BY" comes
     * after "WHERE". But if they are the same or earlier, this method will
     * start a new SELECT that wraps the previous result.</p>
     * <p/>
     * <p>When you have called
     * {@link Builder#setSelect(org.apache.calcite.sql.SqlNodeList)},
     * {@link Builder#setWhere(org.apache.calcite.sql.SqlNode)} etc. call
     * {@link Builder#result(org.apache.calcite.sql.SqlNode, java.util.Collection, org.apache.calcite.rel.RelNode)}
     * to fix the new query.</p>
     *
     * @param rel     Relational expression being implemented
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
            || (maxClause.equals(clause) && !nonWrapSet.contains(clause))) {
          needNew = true;
        }
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
          @Override
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
        newContext = new AliasContext(aliases, aliases.size() > 1);
      }
      return new Builder(rel, clauseList, select, newContext);
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

    /**
     * Returns a node that can be included in the FROM clause or a JOIN. It has
     * an alias that is unique within the query. The alias is implicit if it
     * can be derived using the usual rules (For example, "SELECT * FROM emp" is
     * equivalent to "SELECT * FROM emp AS emp".)
     */
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

    /**
     * Converts a non-query node into a SELECT node. Set operators (UNION,
     * INTERSECT, EXCEPT) remain as is.
     */
    SqlSelect asSelect() {
      if (node instanceof SqlSelect) {
        return (SqlSelect) node;
      }
      return wrapSelect(node);
    }

    /**
     * Converts a non-query node into a SELECT node. Set operators (UNION,
     * INTERSECT, EXCEPT) remain as is.
     */
    public SqlNode asQuery() {
      if (node instanceof SqlCall
          && ((SqlCall) node).getOperator() instanceof SqlSetOperator) {
        return node;
      }
      return asSelect();
    }

    /**
     * Returns a context that always qualifies identifiers. Useful if the
     * Context deals with just one arm of a join, yet we wish to generate
     * a join condition that qualifies column names to disambiguate them.
     */
    public Context qualifiedContext() {
      return new AliasContext(aliases, true);
    }
  }

  /**
   * Builder.
   */
  public class Builder {
    private final RelNode rel;
    private final List<Clause> clauses;
    private final SqlSelect select;
    public final Context context;

    public Builder(RelNode rel, List<Clause> clauses, SqlSelect select,
                   Context context) {
      this.rel = rel;
      this.clauses = clauses;
      this.select = select;
      this.context = context;
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

    public void setOrderBy(SqlNodeList nodeList) {
      assert clauses.contains(Clause.ORDER_BY);
      select.setOrderBy(nodeList);
    }

    public void setfetch(SqlNode fetch) {
      assert clauses.contains(Clause.FETCH);
      select.setFetch(fetch);
    }

    public void setOffset(SqlNode offset) {
      assert clauses.contains(Clause.OFFSET);
      select.setOffset(offset);
    }

    public Result result() {
      return RelToSqlConverter.this.result(select, clauses, rel);
    }
  }

  /**
   * Clauses in a SQL query. Ordered by evaluation order.
   * SELECT is set only when there is a NON-TRIVIAL SELECT clause.
   */
  enum Clause {
    FROM, WHERE, GROUP_BY, HAVING, SELECT, SET_OP, ORDER_BY, FETCH, OFFSET
  }
}
