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

import org.apache.calcite.adapter.jdbc.JdbcTable;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptSamplingParameters;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.AsofJoin;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsofJoin;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.fun.SqlInternalOperators;
import org.apache.calcite.sql.fun.SqlMinMaxAggFunction;
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.rex.RexLiteral.stringValue;
import static org.apache.calcite.util.Util.last;

import static java.util.Objects.requireNonNull;

/**
 * Utility to convert relational expressions to SQL abstract syntax tree.
 */
public class RelToSqlConverter extends SqlImplementor
    implements ReflectiveVisitor {
  private final ReflectUtil.MethodDispatcher<Result> dispatcher;

  private final Deque<Frame> stack = new ArrayDeque<>();

  /** Creates a RelToSqlConverter. */
  @SuppressWarnings("argument.type.incompatible")
  public RelToSqlConverter(SqlDialect dialect) {
    super(dialect);
    dispatcher =
        ReflectUtil.createMethodDispatcher(Result.class, this, "visit",
            RelNode.class);
  }

  /** Dispatches a call to the {@code visit(Xxx e)} method where {@code Xxx}
   * most closely matches the runtime type of the argument. */
  protected Result dispatch(RelNode e) {
    return dispatcher.invoke(e);
  }

  @Override public Result visitInput(RelNode parent, int i, boolean anon,
      boolean ignoreClauses, Set<Clause> expectedClauses) {
    try {
      final RelNode e = parent.getInput(i);
      stack.push(new Frame(parent, i, e, anon, ignoreClauses, expectedClauses));
      return dispatch(e);
    } finally {
      stack.pop();
    }
  }

  @Override protected boolean isAnon() {
    Frame peek = stack.peek();
    return peek == null || peek.anon;
  }

  @Override protected Result result(SqlNode node, Collection<Clause> clauses,
      @Nullable String neededAlias, @Nullable RelDataType neededType,
      Map<String, RelDataType> aliases) {
    final Frame frame = requireNonNull(stack.peek());
    return super.result(node, clauses, neededAlias, neededType, aliases)
        .withAnon(isAnon())
        .withExpectedClauses(frame.ignoreClauses, frame.expectedClauses,
            frame.parent);
  }

  /** Visits a RelNode; called by {@link #dispatch} via reflection. */
  public Result visit(RelNode e) {
    throw new AssertionError("Need to implement " + e.getClass().getName());
  }

  /**
   * A SqlShuttle to replace references to a column of a table alias with the expression
   * from the select item that is the source of that column.
   * ANTI- and SEMI-joins generate an alias for right hand side relation which
   * is used in the ON condition. But that alias is never created, so we have to inline references.
   */
  private static class AliasReplacementShuttle extends SqlShuttle {
    private final String tableAlias;
    private final RelDataType tableType;
    private final SqlNodeList replaceSource;

    AliasReplacementShuttle(String tableAlias, RelDataType tableType,
        SqlNodeList replaceSource) {
      this.tableAlias = tableAlias;
      this.tableType = tableType;
      this.replaceSource = replaceSource;
    }

    @Override public SqlNode visit(SqlIdentifier id) {
      SqlNodeList source = requireNonNull(replaceSource, "replaceSource");
      SqlNode firstItem = source.get(0);
      if (firstItem instanceof SqlIdentifier && ((SqlIdentifier) firstItem).isStar()) {
        return id;
      }
      if (tableAlias.equals(id.names.get(0))) {
        int index =
            requireNonNull(tableType.getField(id.names.get(1), false, false),
                () -> "field " + id.names.get(1) + " is not found in "
                    + tableType)
            .getIndex();
        SqlNode selectItem = source.get(index);
        if (selectItem.getKind() == SqlKind.AS) {
          selectItem = ((SqlCall) selectItem).operand(0);
        }
        return selectItem.clone(id.getParserPosition());
      }
      return id;
    }
  }

  /** Visits a Join; called by {@link #dispatch} via reflection. */
  public Result visit(Join e) {
    switch (e.getJoinType()) {
    case ANTI:
    case SEMI:
      return visitAntiOrSemiJoin(e);
    default:
      break;
    }
    final Result leftResult = visitInput(e, 0).resetAlias();
    final Result rightResult = visitInput(e, 1).resetAlias();
    final Context leftContext = leftResult.qualifiedContext();
    final Context rightContext = rightResult.qualifiedContext();
    final SqlNode sqlCondition;
    final JoinConditionType condType;
    JoinType joinType = joinType(e.getJoinType());
    if (joinType == JoinType.INNER
        && e.getCondition().isAlwaysTrue()) {
      if (isCommaJoin(e)) {
        joinType = JoinType.COMMA;
      } else {
        joinType = JoinType.CROSS;
      }
      sqlCondition = null;
      condType = JoinConditionType.NONE;
    } else {
      sqlCondition =
          convertConditionToSqlNode(e.getCondition(), leftContext,
              rightContext);
      condType = JoinConditionType.ON;
    }
    if (joinType == JoinType.ASOF || joinType == JoinType.LEFT_ASOF) {
      final RexNode match = ((AsofJoin) e).getMatchCondition();
      final SqlNode matchCondition = convertConditionToSqlNode(match, leftContext, rightContext);
      SqlNode join =
          new SqlAsofJoin(POS,
              leftResult.asFrom(),
              SqlLiteral.createBoolean(false, POS),
              joinType.symbol(POS),
              rightResult.asFrom(),
              condType.symbol(POS),
              sqlCondition,
              matchCondition);
      return result(join, leftResult, rightResult);
    }

    SqlNode join =
        new SqlJoin(POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            joinType.symbol(POS),
            rightResult.asFrom(),
            condType.symbol(POS),
            sqlCondition);
    return result(join, leftResult, rightResult);
  }

  protected Result visitAntiOrSemiJoin(Join e) {
    final Result leftResult = visitInput(e, 0).resetAlias();
    final Result rightResult = visitInput(e, 1).resetAlias();
    final Context leftContext = leftResult.qualifiedContext();
    final Context rightContext = rightResult.qualifiedContext();

    final SqlSelect sqlSelect = leftResult.asSelect();
    SqlNode sqlCondition =
        convertConditionToSqlNode(e.getCondition(), leftContext, rightContext);
    if (leftResult.neededAlias != null) {
      SqlShuttle visitor =
          new AliasReplacementShuttle(leftResult.neededAlias,
              e.getLeft().getRowType(), sqlSelect.getSelectList());
      sqlCondition = sqlCondition.accept(visitor);
    }
    SqlNode fromPart = rightResult.asFrom();
    SqlSelect existsSqlSelect;
    if (fromPart.getKind() == SqlKind.SELECT) {
      existsSqlSelect = (SqlSelect) fromPart;
      existsSqlSelect.setSelectList(
          new SqlNodeList(ImmutableList.of(ONE), POS));
      if (existsSqlSelect.getWhere() != null) {
        sqlCondition =
            SqlStdOperatorTable.AND.createCall(POS, existsSqlSelect.getWhere(),
                sqlCondition);
      }
      existsSqlSelect.setWhere(sqlCondition);
    } else {
      existsSqlSelect =
          new SqlSelect(POS, null,
              new SqlNodeList(
                  ImmutableList.of(ONE), POS),
              fromPart, sqlCondition, null,
              null, null, null, null, null, null, null);
    }
    sqlCondition = SqlStdOperatorTable.EXISTS.createCall(POS, existsSqlSelect);
    if (e.getJoinType() == JoinRelType.ANTI) {
      sqlCondition = SqlStdOperatorTable.NOT.createCall(POS, sqlCondition);
    }
    if (sqlSelect.getWhere() != null) {
      sqlCondition =
          SqlStdOperatorTable.AND.createCall(POS, sqlSelect.getWhere(),
              sqlCondition);
    }
    sqlSelect.setWhere(sqlCondition);

    if (leftResult.neededAlias != null
        && sqlSelect.getFrom() != null
        && sqlSelect.getFrom().getKind() != SqlKind.JOIN) {
      sqlSelect.setFrom(as(sqlSelect.getFrom(), leftResult.neededAlias));
    }
    return result(sqlSelect, ImmutableList.of(Clause.FROM), e, null);
  }

  /** Returns whether this join should be unparsed as a {@link JoinType#COMMA}.
   *
   * <p>Comma-join is one possible syntax for {@code CROSS JOIN ... ON TRUE},
   * supported on most but not all databases
   * (see {@link SqlDialect#emulateJoinTypeForCrossJoin()}).
   *
   * <p>For example, the following queries are equivalent:
   *
   * <blockquote><pre>{@code
   * // Comma join
   * SELECT *
   * FROM Emp, Dept
   *
   * // Cross join
   * SELECT *
   * FROM Emp CROSS JOIN Dept
   *
   * // Inner join
   * SELECT *
   * FROM Emp INNER JOIN Dept ON TRUE
   * }</pre></blockquote>
   *
   * <p>Examples:
   * <ul>
   *   <li>{@code FROM (x CROSS JOIN y ON TRUE) CROSS JOIN z ON TRUE}
   *   is a comma join, because all joins are comma joins;
   *   <li>{@code FROM (x CROSS JOIN y ON TRUE) CROSS JOIN z ON TRUE}
   *   would not be a comma join when run on Apache Spark, because Spark does
   *   not support comma join;
   *   <li>{@code FROM (x CROSS JOIN y ON TRUE) LEFT JOIN z ON TRUE}
   *   is not comma join because one of the joins is not INNER;
   *   <li>{@code FROM (x CROSS JOIN y ON c) CROSS JOIN z ON TRUE}
   *   is not a comma join because one of the joins is ON TRUE.
   * </ul>
   */
  private boolean isCommaJoin(Join join) {
    if (!join.getCondition().isAlwaysTrue()) {
      return false;
    }
    if (dialect.emulateJoinTypeForCrossJoin() != JoinType.COMMA) {
      return false;
    }

    // Find the topmost enclosing Join. For example, if the tree is
    //   Join((Join(a, b), Join(c, Join(d, e)))
    // we get the same topJoin for a, b, c, d and e. Join. Stack is never empty,
    // and frame.r on the first iteration is always "join".
    assert !stack.isEmpty();
    assert stack.element().r == join;
    Join j = null;
    for (Frame frame : stack) {
      j = (Join) frame.r;
      if (!(frame.parent instanceof Join)) {
        break;
      }
    }
    final Join topJoin = requireNonNull(j, "top join");

    // Flatten the join tree, using a breadth-first algorithm.
    // After flattening, the list contains all of the joins that will make up
    // the FROM clause.
    final List<Join> flatJoins = new ArrayList<>();
    flatJoins.add(topJoin);
    for (int i = 0; i < flatJoins.size();) {
      final Join j2 = flatJoins.get(i++);
      if (j2.getLeft() instanceof Join) {
        flatJoins.add((Join) j2.getLeft());
      }
      if (j2.getRight() instanceof Join) {
        flatJoins.add((Join) j2.getRight());
      }
    }

    // If all joins are cross-joins (INNER JOIN ON TRUE), we can use
    // we can use comma syntax "FROM a, b, c, d, e".
    for (Join j2 : flatJoins) {
      if (j2.getJoinType() != JoinRelType.INNER
          || !j2.getCondition().isAlwaysTrue()) {
        return false;
      }
    }
    return true;
  }

  /** Visits a Correlate; called by {@link #dispatch} via reflection. */
  public Result visit(Correlate e) {
    final Result leftResult =
        visitInput(e, 0)
            .resetAlias(e.getCorrelVariable(), e.getInput(0).getRowType());
    parseCorrelTable(e, leftResult);
    final Result rightResult = visitInput(e, 1);
    final SqlNode rightResultNode = rightResult.node;
    final SqlIdentifier id =
        new SqlIdentifier(
            requireNonNull(rightResult.neededAlias,
                () -> "rightResult.neededAlias is null, node is " + rightResultNode), POS);
    SqlNode rightLateral =
        SqlStdOperatorTable.LATERAL.createCall(POS, rightResultNode);
    SqlNode rightLateralAs;
    if (rightResultNode.getKind() == SqlKind.AS) {
      // If node already is an AS node, we need to replace the alias
      // For example:
      // Before: AS "t1" ("xs") AS "t10"
      // Now：AS "t10" ("xs")
      SqlCall sqlRightCall = (SqlCall) rightResultNode;
      List<SqlNode> operands = new ArrayList<>(sqlRightCall.getOperandList());
      rightLateral =
          SqlStdOperatorTable.LATERAL.createCall(POS, operands.get(0));
      operands.set(0, rightLateral);
      operands.set(1, id);
      rightLateralAs =  SqlStdOperatorTable.AS.createCall(POS, operands);
    } else {
      rightLateralAs =  SqlStdOperatorTable.AS.createCall(POS, rightLateral, id);
    }

    final SqlNode join =
        new SqlJoin(POS,
            leftResult.asFrom(),
            SqlLiteral.createBoolean(false, POS),
            JoinType.COMMA.symbol(POS),
            rightLateralAs,
            JoinConditionType.NONE.symbol(POS),
            null);
    return result(join, leftResult, rightResult);
  }

  /** Visits a Filter; called by {@link #dispatch} via reflection. */
  public Result visit(Filter e) {
    final RelNode input = e.getInput();
    if (input instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) input;
      final boolean ignoreClauses = aggregate.getInput() instanceof Project;
      final Result x =
          visitInput(e, 0, isAnon(), ignoreClauses,
              ImmutableSet.of(Clause.HAVING));
      parseCorrelTable(e, x);
      final Builder builder = x.builder(e);
      x.asSelect().setHaving(
          SqlUtil.andExpressions(x.asSelect().getHaving(),
              builder.context.toSql(null, e.getCondition())));
      return builder.result();
    } else {
      Result x = visitInput(e, 0, Clause.WHERE);
      if (!e.getVariablesSet().isEmpty()) {
        x = x.resetAlias();
      }
      parseCorrelTable(e, x);
      final Builder builder = x.builder(e);
      if (input instanceof Join) {
        final Context context = x.qualifiedContext();
        if (selectListRequired(context)) {
          final ImmutableList.Builder<SqlNode> selectList = ImmutableList.builder();
          // Fieldnames are unique since they are created by SqlValidatorUtil.deriveJoinRowType()
          final List<String> uniqueFieldNames = input.getRowType().getFieldNames();
          for (int i = 0; i < context.fieldCount; i++) {
            final SqlNode field = context.field(i);
            final String fieldName = uniqueFieldNames.get(i);
            selectList.add(
                SqlStdOperatorTable.AS.createCall(POS, field,
                new SqlIdentifier(fieldName, POS)));
          }
          builder.setSelect(new SqlNodeList(selectList.build(), POS));
        }
      }
      builder.setWhere(builder.context.toSql(null, e.getCondition()));
      return builder.result();
    }
  }

  private static boolean selectListRequired(Context context) {
    Set<String> uniqueFieldNames = new HashSet<>();
    for (SqlNode node : context.fieldList()) {
      if (node instanceof SqlIdentifier) {
        SqlIdentifier field = (SqlIdentifier) node;
        if (uniqueFieldNames.contains(last(field.names))) {
          return true;
        }
        uniqueFieldNames.add(last(field.names));
      }
    }
    return false;
  }

  /** Visits a Project; called by {@link #dispatch} via reflection. */
  public Result visit(Project e) {
    // If the input is a Sort, wrap SELECT is not required.
    final Result x;
    if (e.getInput() instanceof Sort) {
      x = visitInput(e, 0);
    } else {
      x = visitInput(e, 0, Clause.SELECT);
    }
    parseCorrelTable(e, x);
    final Builder builder = x.builder(e);
    if (!isStar(e.getProjects(), e.getInput().getRowType(), e.getRowType())
        || !dialect.supportGenerateSelectStar(e.getInput())) {
      final List<SqlNode> selectList = new ArrayList<>();
      for (RexNode ref : e.getProjects()) {
        SqlNode sqlExpr = builder.context.toSql(null, ref);
        if (SqlUtil.isNullLiteral(sqlExpr, false)) {
          final RelDataTypeField field =
              e.getRowType().getFieldList().get(selectList.size());
          sqlExpr = castNullType(sqlExpr, field.getType());
        }
        addSelect(selectList, sqlExpr, e.getRowType());
      }
      // We generate "SELECT 1 FROM EMP" replace "SELECT FROM EMP"
      if (selectList.isEmpty()) {
        selectList.add(SqlLiteral.createExactNumeric("1", POS));
      }
      final SqlNodeList selectNodeList = new SqlNodeList(selectList, POS);
      if (builder.select.getGroup() == null
          && builder.select.getHaving() == null
          && SqlUtil.containsAgg(builder.select.getSelectList())
          && !SqlUtil.containsAgg(selectNodeList)) {
        // We are just about to remove the last aggregate function from the
        // SELECT clause. The "GROUP BY ()" was implicit, but we now need to
        // make it explicit.
        builder.setGroupBy(SqlNodeList.EMPTY);
      }
      builder.setSelect(selectNodeList);
    }
    return builder.result();
  }

  /** Wraps a NULL literal in a CAST operator to a target type.
   *
   * @param nullLiteral NULL literal
   * @param type Target type
   *
   * @return null literal wrapped in CAST call
   */
  private SqlNode castNullType(SqlNode nullLiteral, RelDataType type) {
    final SqlNode typeNode = dialect.getCastSpec(type);
    if (typeNode == null) {
      return nullLiteral;
    }
    return SqlStdOperatorTable.CAST.createCall(POS, nullLiteral, typeNode);
  }

  /** Visits a Window; called by {@link #dispatch} via reflection. */
  public Result visit(Window e) {
    final Result x = visitInput(e, 0);
    final Builder builder = x.builder(e);
    final RelNode input = e.getInput();
    final int inputFieldCount = input.getRowType().getFieldCount();
    final List<SqlNode> rexOvers = new ArrayList<>();
    for (Window.Group group : e.groups) {
      rexOvers.addAll(builder.context.toSql(group, e.constants, inputFieldCount));
    }
    final List<SqlNode> selectList = new ArrayList<>();

    for (RelDataTypeField field : input.getRowType().getFieldList()) {
      addSelect(selectList, builder.context.field(field.getIndex()), e.getRowType());
    }

    for (SqlNode rexOver : rexOvers) {
      addSelect(selectList, rexOver, e.getRowType());
    }

    builder.setSelect(new SqlNodeList(selectList, POS));
    return builder.result();
  }

  /** Visits an Aggregate; called by {@link #dispatch} via reflection. */
  public Result visit(Aggregate e) {
    final Builder builder =
        visitAggregate(e, e.getGroupSet().toList(), Clause.GROUP_BY);
    return builder.result();
  }

  private Builder visitAggregate(Aggregate e, List<Integer> groupKeyList,
      Clause... clauses) {
    // groupSet contains at least one column that is not in any groupSet.
    // Set of clauses that we expect the builder need to add extra Clause.HAVING
    // then can add Having filter condition in buildAggregate.
    final Set<Clause> clauseSet = new TreeSet<>(Arrays.asList(clauses));
    if (!e.getGroupSet().equals(ImmutableBitSet.union(e.getGroupSets()))) {
      clauseSet.add(Clause.HAVING);
    }
    // "select a, b, sum(x) from ( ... ) group by a, b"
    final boolean ignoreClauses = e.getInput() instanceof Project;
    final Result x = visitInput(e, 0, isAnon(), ignoreClauses, clauseSet);
    final Builder builder = x.builder(e);
    final List<SqlNode> selectList = new ArrayList<>();
    final List<SqlNode> groupByList =
        generateGroupList(builder, selectList, e, groupKeyList);
    return buildAggregate(e, builder, selectList, groupByList);
  }

  /**
   * Builds the group list for an Aggregate node.
   *
   * @param e The Aggregate node
   * @param builder The SQL builder
   * @param groupByList output group list
   * @param selectList output select list
   */
  protected void buildAggGroupList(Aggregate e, Builder builder,
      List<SqlNode> groupByList, List<SqlNode> selectList) {
    for (int group : e.getGroupSet()) {
      final SqlNode field = builder.context.field(group);
      addSelect(selectList, field, e.getRowType());
      groupByList.add(field);
    }
  }

  /**
   * Builds an aggregate query.
   *
   * @param e The Aggregate node
   * @param builder The SQL builder
   * @param selectList The precomputed group list
   * @param groupByList The precomputed select list
   * @return The aggregate query result
   */
  protected Builder buildAggregate(Aggregate e, Builder builder,
      List<SqlNode> selectList, List<SqlNode> groupByList) {
    for (AggregateCall aggCall : e.getAggCallList()) {
      SqlNode aggCallSqlNode = builder.context.toSql(aggCall);
      RelDataType aggCallRelDataType = aggCall.getType();
      if (aggCall.getAggregation() instanceof SqlSingleValueAggFunction) {
        aggCallSqlNode = dialect.rewriteSingleValueExpr(aggCallSqlNode, aggCallRelDataType);
      } else if (aggCall.getAggregation() instanceof SqlMinMaxAggFunction) {
        aggCallSqlNode = dialect.rewriteMaxMinExpr(aggCallSqlNode, aggCallRelDataType);
      }
      addSelect(selectList, aggCallSqlNode, e.getRowType());
    }
    builder.setSelect(new SqlNodeList(selectList, POS));
    if (!groupByList.isEmpty() || e.getAggCallList().isEmpty()) {
      // Some databases don't support "GROUP BY ()". We can omit it as long
      // as there is at least one aggregate function. (We have to take care
      // if we later prune away that last aggregate function.)
      builder.setGroupBy(new SqlNodeList(groupByList, POS));
    }

    if (builder.clauses.contains(Clause.HAVING) && !e.getGroupSet()
        .equals(ImmutableBitSet.union(e.getGroupSets()))) {
      // groupSet contains at least one column that is not in any groupSets.
      // To make such columns must appear in the output (their value will
      // always be NULL), we generate an extra grouping set, then filter
      // it out using a "HAVING GROUPING(groupSets) <> 0".
      // We want to generate the
      final SqlNodeList groupingList = new SqlNodeList(POS);
      e.getGroupSet().forEachInt(g ->
          groupingList.add(builder.context.field(g)));
      builder.setHaving(
          SqlStdOperatorTable.NOT_EQUALS.createCall(POS,
              SqlStdOperatorTable.GROUPING.createCall(groupingList), ZERO));
    }
    return builder;
  }

  /** Generates the GROUP BY items, for example {@code GROUP BY x, y},
   * {@code GROUP BY CUBE (x, y)} or {@code GROUP BY ROLLUP (x, y)}.
   *
   * <p>Also populates the SELECT clause. If the GROUP BY list is simple, the
   * SELECT will be identical; if the GROUP BY list contains GROUPING SETS,
   * CUBE or ROLLUP, the SELECT clause will contain the distinct leaf
   * expressions. */
  private List<SqlNode> generateGroupList(Builder builder,
      List<SqlNode> selectList, Aggregate aggregate, List<Integer> groupList) {
    final List<Integer> sortedGroupList =
        Ordering.natural().sortedCopy(groupList);
    assert aggregate.getGroupSet().asList().equals(sortedGroupList)
        : "groupList " + groupList + " must be equal to groupSet "
        + aggregate.getGroupSet() + ", just possibly a different order";

    final List<SqlNode> groupKeys = new ArrayList<>();
    for (int key : groupList) {
      final SqlNode field = builder.context.field(key);
      groupKeys.add(field);
    }
    for (int key : sortedGroupList) {
      final SqlNode field = builder.context.field(key);
      addSelect(selectList, field, aggregate.getRowType());
    }
    switch (aggregate.getGroupType()) {
    case SIMPLE:
      return ImmutableList.copyOf(groupKeys);
    case CUBE:
      if (aggregate.getGroupSet().cardinality() > 1) {
        return ImmutableList.of(
            SqlStdOperatorTable.CUBE.createCall(SqlParserPos.ZERO, groupKeys));
      }
      // a singleton CUBE and ROLLUP are the same but we prefer ROLLUP;
      // fall through
    case ROLLUP:
      final List<Integer> rollupBits = Aggregate.Group.getRollup(aggregate.groupSets);
      final List<SqlNode> rollupKeys = rollupBits
          .stream()
          .map(bit -> builder.context.field(bit))
          .collect(Collectors.toList());
      return ImmutableList.of(
          SqlStdOperatorTable.ROLLUP.createCall(SqlParserPos.ZERO, rollupKeys));
    default:
    case OTHER:
      // Make sure that the group sets contains all bits.
      final List<ImmutableBitSet> groupSets;
      if (aggregate.getGroupSet()
          .equals(ImmutableBitSet.union(aggregate.groupSets))) {
        groupSets = aggregate.getGroupSets();
      } else {
        groupSets = new ArrayList<>(aggregate.getGroupSets().size() + 1);
        groupSets.add(aggregate.getGroupSet());
        groupSets.addAll(aggregate.getGroupSets());
      }
      return ImmutableList.of(
          SqlStdOperatorTable.GROUPING_SETS.createCall(SqlParserPos.ZERO,
              groupSets.stream()
                  .map(groupSet ->
                      groupItem(groupKeys, groupSet, aggregate.getGroupSet()))
                  .collect(Collectors.toList())));
    }
  }

  private static SqlNode groupItem(List<SqlNode> groupKeys,
      ImmutableBitSet groupSet, ImmutableBitSet wholeGroupSet) {
    final List<SqlNode> nodes = groupSet.asList().stream()
        .map(key -> groupKeys.get(wholeGroupSet.indexOf(key)))
        .collect(Collectors.toList());
    switch (nodes.size()) {
    case 1:
      return nodes.get(0);
    default:
      return SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, nodes);
    }
  }

  /** Visits a TableScan; called by {@link #dispatch} via reflection. */
  public Result visit(TableScan e) {
    final SqlIdentifier identifier = getSqlTargetTable(e);
    final SqlNode node;
    final ImmutableList<RelHint> hints = e.getHints();
    if (!hints.isEmpty()) {
      SqlParserPos pos = identifier.getParserPosition();
      node =
          new SqlTableRef(pos, identifier,
              SqlNodeList.of(pos,
                  hints.stream()
                      .map(h -> RelToSqlConverter.toSqlHint(h, pos))
                      .collect(Collectors.toList())));
    } else {
      node = identifier;
    }
    return result(node, ImmutableList.of(Clause.FROM), e, null);
  }

  private static SqlHint toSqlHint(RelHint hint, SqlParserPos pos) {
    if (hint.kvOptions != null) {
      return new SqlHint(pos, new SqlIdentifier(hint.hintName, pos),
          SqlNodeList.of(pos, hint.kvOptions.entrySet().stream()
              .flatMap(
                  e -> Stream.of(new SqlIdentifier(e.getKey(), pos),
                  SqlLiteral.createCharString(e.getValue(), pos)))
              .collect(Collectors.toList())),
          SqlHint.HintOptionFormat.KV_LIST);
    } else if (hint.listOptions != null) {
      return new SqlHint(pos, new SqlIdentifier(hint.hintName, pos),
          SqlNodeList.of(pos, hint.listOptions.stream()
              .map(e -> SqlLiteral.createCharString(e, pos))
              .collect(Collectors.toList())),
          SqlHint.HintOptionFormat.LITERAL_LIST);
    }
    return new SqlHint(pos, new SqlIdentifier(hint.hintName, pos),
        SqlNodeList.EMPTY, SqlHint.HintOptionFormat.EMPTY);
  }

  /** Visits a Union; called by {@link #dispatch} via reflection. */
  public Result visit(Union e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.UNION_ALL
        : SqlStdOperatorTable.UNION, e);
  }

  /** Visits an Intersect; called by {@link #dispatch} via reflection. */
  public Result visit(Intersect e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.INTERSECT_ALL
        : SqlStdOperatorTable.INTERSECT, e);
  }

  /** Visits a Minus; called by {@link #dispatch} via reflection. */
  public Result visit(Minus e) {
    return setOpToSql(e.all
        ? SqlStdOperatorTable.EXCEPT_ALL
        : SqlStdOperatorTable.EXCEPT, e);
  }

  /** Visits a Calc; called by {@link #dispatch} via reflection. */
  public Result visit(Calc e) {
    final RexProgram program = e.getProgram();
    final ImmutableSet<Clause> expectedClauses =
        program.getCondition() != null
            ? ImmutableSet.of(Clause.WHERE)
            : ImmutableSet.of();
    final Result x = visitInput(e, 0, expectedClauses);
    parseCorrelTable(e, x);
    final Builder builder = x.builder(e);
    if (!isStar(program)) {
      final List<SqlNode> selectList = new ArrayList<>(program.getProjectList().size());
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

  /** Visits a Values; called by {@link #dispatch} via reflection. */
  public Result visit(Values e) {
    final List<Clause> clauses = ImmutableList.of(Clause.SELECT);
    final Map<String, RelDataType> pairs = ImmutableMap.of();
    final Context context = aliasContext(pairs, false);
    SqlNode query;
    final boolean rename = stack.size() <= 1
        || !(Iterables.get(stack, 1).r instanceof TableModify);
    final List<String> fieldNames = e.getRowType().getFieldNames();
    if (!dialect.supportsAliasedValues() && rename) {
      // Some dialects (such as Oracle and BigQuery) don't support
      // "AS t (c1, c2)". So instead of
      //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
      // we generate
      //   SELECT v0 AS c0, v1 AS c1 FROM DUAL
      //   UNION ALL
      //   SELECT v2 AS c0, v3 AS c1 FROM DUAL
      // for Oracle and
      //   SELECT v0 AS c0, v1 AS c1
      //   UNION ALL
      //   SELECT v2 AS c0, v3 AS c1
      // for dialects that support SELECT-without-FROM.
      List<SqlSelect> list = new ArrayList<>();
      for (List<RexLiteral> tuple : e.getTuples()) {
        final List<SqlNode> values2 = new ArrayList<>();
        final SqlNodeList exprList = exprList(context, tuple);
        for (Pair<SqlNode, String> value : Pair.zip(exprList, fieldNames)) {
          values2.add(as(value.left, value.right));
        }
        list.add(
            new SqlSelect(POS, null,
                new SqlNodeList(values2, POS),
                getDual(), null, null,
                null, null, null, null, null, null, null));
      }
      if (list.isEmpty()) {
        // In this case we need to construct the following query:
        // SELECT NULL as C0, NULL as C1, NULL as C2 ... FROM DUAL WHERE FALSE
        // This would return an empty result set with the same number of columns as the field names.
        final List<SqlNode> nullColumnNames = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
          SqlCall nullColumnName = as(SqlLiteral.createNull(POS), fieldName);
          nullColumnNames.add(nullColumnName);
        }
        final SqlIdentifier dual = getDual();
        if (dual == null) {
          query =
              new SqlSelect(POS, null,
                  new SqlNodeList(nullColumnNames, POS), null, null, null, null,
                  null, null, null, null, null, null);

          // Wrap "SELECT 1 AS x"
          // as "SELECT * FROM (SELECT 1 AS x) AS t WHERE false"
          query =
              new SqlSelect(POS, null, SqlNodeList.SINGLETON_STAR,
                  as(query, "t"), createAlwaysFalseCondition(), null, null,
                  null, null, null, null, null, null);
        } else {
          query =
              new SqlSelect(POS, null,
                  new SqlNodeList(nullColumnNames, POS),
                  dual, createAlwaysFalseCondition(), null,
                  null, null, null, null, null, null, null);
        }
      } else if (list.size() == 1) {
        query = list.get(0);
      } else {
        query = list.stream()
            .map(select -> (SqlNode) select)
            .reduce((l, r) -> SqlStdOperatorTable.UNION_ALL.createCall(POS, l, r))
            .get();
      }
    } else {
      // Generate ANSI syntax
      //   (VALUES (v0, v1), (v2, v3))
      // or, if rename is required
      //   (VALUES (v0, v1), (v2, v3)) AS t (c0, c1)
      final SqlNodeList selects = new SqlNodeList(POS);
      final boolean isEmpty = Values.isEmpty(e);
      if (isEmpty) {
        // In case of empty values, we need to build:
        //   SELECT *
        //   FROM (VALUES (NULL, NULL ...)) AS T (C1, C2 ...)
        //   WHERE 1 = 0
        selects.add(
            SqlInternalOperators.ANONYMOUS_ROW.createCall(POS,
                Collections.nCopies(fieldNames.size(),
                    SqlLiteral.createNull(POS))));
      } else {
        for (List<RexLiteral> tuple : e.getTuples()) {
          selects.add(
              SqlInternalOperators.ANONYMOUS_ROW.createCall(
                  exprList(context, tuple)));
        }
      }
      query = SqlStdOperatorTable.VALUES.createCall(selects);
      if (rename) {
        query = as(query, "t", fieldNames.toArray(new String[0]));
      }
      if (isEmpty) {
        if (!rename) {
          query = as(query, "t");
        }
        query =
            new SqlSelect(POS, null, SqlNodeList.SINGLETON_STAR, query,
                createAlwaysFalseCondition(), null, null, null,
                null, null, null, null, null);
      }
    }
    return result(query, clauses, e, null);
  }

  /** Visits a Sample; called by {@link #dispatch} via reflection. */
  public Result visit(Sample e) {
    Result x = visitInput(e, 0);
    RelOptSamplingParameters parameters = e.getSamplingParameters();
    boolean isRepeatable = parameters.isRepeatable();
    boolean isBernoulli = parameters.isBernoulli();

    SqlSampleSpec tableSampleSpec =
        isRepeatable
            ? SqlSampleSpec.createTableSample(
            isBernoulli, parameters.sampleRate, parameters.getRepeatableSeed())
            : SqlSampleSpec.createTableSample(isBernoulli, parameters.sampleRate);

    SqlLiteral tableSampleLiteral =
        SqlLiteral.createSample(tableSampleSpec, POS);

    SqlNode tableRef =
        SqlStdOperatorTable.TABLESAMPLE.createCall(POS, x.node, tableSampleLiteral);

    return result(tableRef, ImmutableList.of(Clause.FROM), e, null);
  }

  private @Nullable SqlIdentifier getDual() {
    final List<String> names = dialect.getSingleRowTableName();
    if (names == null) {
      return null;
    }
    return new SqlIdentifier(names, POS);
  }

  private static SqlNode createAlwaysFalseCondition() {
    // Building the select query in the form:
    // select * from VALUES(NULL,NULL ...) where 1=0
    // Use condition 1=0 since "where false" does not seem to be supported
    // on some DB vendors.
    return SqlStdOperatorTable.EQUALS.createCall(POS,
        ImmutableList.of(ONE, ZERO));
  }

  /** Visits a Sort; called by {@link #dispatch} via reflection. */
  public Result visit(Sort e) {
    if (e.getInput() instanceof Aggregate) {
      final Aggregate aggregate = (Aggregate) e.getInput();
      if (hasTrickyRollup(e, aggregate)) {
        // MySQL 5 does not support standard "GROUP BY ROLLUP(x, y)", only
        // the non-standard "GROUP BY x, y WITH ROLLUP".
        List<Integer> rollupList =
            Aggregate.Group.getRollup(aggregate.getGroupSets());
        List<Integer> sortList = e.getCollation()
            .getFieldCollations()
            .stream()
            .map(f -> aggregate.getGroupSet().nth(f.getFieldIndex()))
            .collect(Collectors.toList());
        // "GROUP BY x, y WITH ROLLUP" implicitly sorts by x, y,
        // so skip the ORDER BY.
        final boolean isImplicitlySort = Util.startsWith(rollupList, sortList);
        final Builder builder =
            visitAggregate(aggregate, rollupList,
                Clause.GROUP_BY, Clause.OFFSET, Clause.FETCH);
        Result result = builder.result();
        if (sortList.isEmpty()
            || isImplicitlySort) {
          offsetFetch(e, builder);
          return result;
        }
        // MySQL does not allow "WITH ROLLUP" in combination with "ORDER BY",
        // so generate the grouped result apply ORDER BY to it.
        SqlSelect sqlSelect = result.subSelect();
        SqlNodeList sortExps = exprList(builder.context, e.getSortExps());
        sqlSelect.setOrderBy(sortExps);
        if (e.offset != null) {
          SqlNode offset = builder.context.toSql(null, e.offset);
          sqlSelect.setOffset(offset);
        }
        if (e.fetch != null) {
          SqlNode fetch = builder.context.toSql(null, e.fetch);
          sqlSelect.setFetch(fetch);
        }
        return result(sqlSelect, ImmutableList.of(Clause.ORDER_BY), e, null);
      }
    }
    if (e.getInput() instanceof Project) {
      // Deal with the case Sort(Project(Aggregate ...))
      // by converting it to Project(Sort(Aggregate ...)).
      final Project project = (Project) e.getInput();
      final Permutation permutation = project.getPermutation();
      if (permutation != null
          && project.getInput() instanceof Aggregate) {
        final Aggregate aggregate = (Aggregate) project.getInput();
        if (hasTrickyRollup(e, aggregate)) {
          final RelCollation collation =
              RelCollations.permute(e.collation, permutation);
          final Sort sort2 =
              LogicalSort.create(aggregate, collation, e.offset, e.fetch);
          final Project project2 =
              LogicalProject.create(
                  sort2,
                  ImmutableList.of(),
                  project.getProjects(),
                  project.getRowType(),
                  project.getVariablesSet());
          return visit(project2);
        }
      }
    }
    final Result x =
        visitInput(e, 0, Clause.ORDER_BY, Clause.OFFSET, Clause.FETCH);
    final Builder builder = x.builder(e);
    if (stack.size() != 1
        && builder.select.getSelectList().equals(SqlNodeList.SINGLETON_STAR)) {
      // Generates explicit column names instead of start(*) for
      // non-root order by to avoid ambiguity.
      final List<SqlNode> selectList = Expressions.list();
      for (RelDataTypeField field : e.getRowType().getFieldList()) {
        addSelect(selectList, builder.context.field(field.getIndex()), e.getRowType());
      }
      builder.select.setSelectList(new SqlNodeList(selectList, POS));
    }
    List<SqlNode> orderByList = Expressions.list();
    for (RelFieldCollation field : e.getCollation().getFieldCollations()) {
      builder.addOrderItem(orderByList, field);
    }
    if (!orderByList.isEmpty()) {
      builder.setOrderBy(new SqlNodeList(orderByList, POS));
    }
    offsetFetch(e, builder);
    return builder.result();
  }

  /** Adds OFFSET and FETCH to a builder, if applicable.
   * The builder must have been created with OFFSET and FETCH clauses. */
  void offsetFetch(Sort e, Builder builder) {
    if (e.fetch != null) {
      builder.setFetch(builder.context.toSql(null, e.fetch));
    }
    if (e.offset != null) {
      builder.setOffset(builder.context.toSql(null, e.offset));
    }
  }

  public boolean hasTrickyRollup(Sort e, Aggregate aggregate) {
    return !dialect.supportsAggregateFunction(SqlKind.ROLLUP)
        && dialect.supportsGroupByWithRollup()
        && (aggregate.getGroupType() == Aggregate.Group.ROLLUP
            || aggregate.getGroupType() == Aggregate.Group.CUBE
                && aggregate.getGroupSet().cardinality() == 1)
        && e.collation.getFieldCollations().stream().allMatch(fc ->
            fc.getFieldIndex() < aggregate.getGroupSet().cardinality());
  }

  private static SqlIdentifier getSqlTargetTable(RelNode e) {
    // Use the foreign catalog, schema and table names, if they exist,
    // rather than the qualified name of the shadow table in Calcite.
    final RelOptTable table = requireNonNull(e.getTable());
    return table.maybeUnwrap(JdbcTable.class)
        .map(JdbcTable::tableName)
        .orElseGet(() ->
            new SqlIdentifier(table.getQualifiedName(), SqlParserPos.ZERO));
  }

  /** Visits a TableModify; called by {@link #dispatch} via reflection. */
  public Result visit(TableModify modify) {
    // Target Table Name
    final SqlIdentifier sqlTargetTable = getSqlTargetTable(modify);

    switch (modify.getOperation()) {
    case INSERT: {
      // Convert the input to a SELECT query or keep as VALUES. Not all
      // dialects support naked VALUES, but all support VALUES inside INSERT.
      final SqlNode sqlSource =
          visitInput(modify, 0).asQueryOrValues();

      final SqlInsert sqlInsert =
          new SqlInsert(POS, SqlNodeList.EMPTY, sqlTargetTable, sqlSource,
              identifierList(modify.getTable().getRowType().getFieldNames()));

      return result(sqlInsert, ImmutableList.of(), modify, null);
    }
    case UPDATE: {
      final Result input = visitInput(modify, 0);
      // SELECT statement has two parts: columns from the target table (old values) and
      // expressions for the UPDATE. See the TableModify documentation for details.
      final SqlSelect select = input.asSelect();
      final Context context = selectListContext(select.getSelectList(), false);

      final SqlUpdate sqlUpdate =
          new SqlUpdate(POS, sqlTargetTable,
              identifierList(
                  requireNonNull(modify.getUpdateColumnList(),
                      () -> "modify.getUpdateColumnList() is null for " + modify)),
              exprList(context,
                  requireNonNull(modify.getSourceExpressionList(),
                      () -> "modify.getSourceExpressionList() is null for " + modify)),
              select.getWhere(), select, null);

      return result(sqlUpdate, input.clauses, modify, null);
    }
    case DELETE: {
      final Result input = visitInput(modify, 0);

      final SqlDelete sqlDelete =
          new SqlDelete(POS, sqlTargetTable,
              input.asSelect().getWhere(), input.asSelect(), null);

      return result(sqlDelete, input.clauses, modify, null);
    }
    case MERGE: {
      final Result input = visitInput(modify, 0);
      final SqlSelect select = input.asSelect();
      // When querying with both the `WHEN MATCHED THEN UPDATE` and
      // `WHEN NOT MATCHED THEN INSERT` clauses, the selectList consists of three parts:
      // the insert expression, the target table reference, and the update expression.
      // When querying with the `WHEN MATCHED THEN UPDATE` clause, the selectList will not
      // include the insert expression.
      // However, when querying with the `WHEN NOT MATCHED THEN INSERT` clause,
      // the expression list will only contain the insert expression.
      final SqlNodeList selectList = SqlUtil.stripListAs(select.getSelectList());
      final SqlJoin join =  requireNonNull((SqlJoin) select.getFrom());
      final SqlNode condition = requireNonNull(join.getCondition());
      final SqlNode source = join.getLeft();

      SqlUpdate update = null;
      final List<String> updateColumnList =
          requireNonNull(modify.getUpdateColumnList(),
              () -> "modify.getUpdateColumnList() is null for " + modify);
      final int nUpdateFiled = updateColumnList.size();
      if (nUpdateFiled != 0) {
        final SqlNodeList expressionList =
            last(selectList, nUpdateFiled).stream()
                .collect(SqlNodeList.toList());
        update =
            new SqlUpdate(POS, sqlTargetTable,
                identifierList(updateColumnList),
                expressionList,
                condition, null, null);
      }

      final RelDataType targetRowType = modify.getTable().getRowType();
      final int nTargetFiled = targetRowType.getFieldCount();
      final int nInsertFiled = nUpdateFiled == 0
          ? selectList.size() : selectList.size() - nTargetFiled - nUpdateFiled;
      SqlInsert insert = null;
      if (nInsertFiled != 0) {
        final SqlNodeList expressionList =
            Util.first(selectList, nInsertFiled).stream()
                .collect(SqlNodeList.toList());
        final SqlNode valuesCall =
            SqlStdOperatorTable.VALUES.createCall(expressionList);
        final SqlNodeList columnList = targetRowType.getFieldNames().stream()
            .map(f -> new SqlIdentifier(f, POS))
            .collect(SqlNodeList.toList());
        insert = new SqlInsert(POS, SqlNodeList.EMPTY, sqlTargetTable, valuesCall, columnList);
      }

      final SqlNode target = join.getRight();
      final SqlNode targetTableAlias = target.getKind() == SqlKind.AS
          ? ((SqlCall) target).operand(1) : null;
      final SqlMerge merge =
          new SqlMerge(POS, sqlTargetTable, condition, source, update, insert, null,
              (SqlIdentifier) targetTableAlias);
      return result(merge, input.clauses, modify, null);
    }
    default:
      throw new AssertionError("not implemented: " + modify);
    }
  }

  /** Converts a list of {@link RexNode} expressions to {@link SqlNode}
   * expressions. */
  private static SqlNodeList exprList(final Context context,
      List<? extends RexNode> exprs) {
    return new SqlNodeList(
        Util.transform(exprs, e -> context.toSql(null, e)), POS);
  }

  /** Converts a list of names expressions to a list of single-part
   * {@link SqlIdentifier}s. */
  private static SqlNodeList identifierList(List<String> names) {
    return new SqlNodeList(
        Util.transform(names, name -> new SqlIdentifier(name, POS)), POS);
  }

  /** Visits a Match; called by {@link #dispatch} via reflection. */
  public Result visit(Match e) {
    final RelNode input = e.getInput();
    final Result x = visitInput(e, 0);
    final Context context = matchRecognizeContext(x.qualifiedContext());

    SqlNode tableRef = x.asQueryOrValues();

    final RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    final List<SqlNode> partitionSqlList = new ArrayList<>();
    for (int key : e.getPartitionKeys()) {
      final RexInputRef ref = rexBuilder.makeInputRef(input, key);
      SqlNode sqlNode = context.toSql(null, ref);
      partitionSqlList.add(sqlNode);
    }
    final SqlNodeList partitionList = new SqlNodeList(partitionSqlList, POS);

    final List<SqlNode> orderBySqlList = new ArrayList<>();
    if (e.getOrderKeys() != null) {
      for (RelFieldCollation fc : e.getOrderKeys().getFieldCollations()) {
        if (fc.nullDirection != RelFieldCollation.NullDirection.UNSPECIFIED) {
          boolean first =
              fc.nullDirection == RelFieldCollation.NullDirection.FIRST;
          SqlNode nullDirectionNode =
              dialect.emulateNullDirection(context.field(fc.getFieldIndex()),
                  first, fc.direction.isDescending());
          if (nullDirectionNode != null) {
            orderBySqlList.add(nullDirectionNode);
            fc =
                new RelFieldCollation(fc.getFieldIndex(), fc.getDirection(),
                    RelFieldCollation.NullDirection.UNSPECIFIED);
          }
        }
        orderBySqlList.add(context.toSql(fc));
      }
    }
    final SqlNodeList orderByList =
        new SqlNodeList(orderBySqlList, SqlParserPos.ZERO);

    final SqlLiteral rowsPerMatch = e.isAllRows()
        ? SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS.symbol(POS)
        : SqlMatchRecognize.RowsPerMatchOption.ONE_ROW.symbol(POS);

    final SqlNode after;
    if (e.getAfter() instanceof RexLiteral) {
      SqlMatchRecognize.AfterOption value = (SqlMatchRecognize.AfterOption)
          ((RexLiteral) e.getAfter()).getValue2();
      after = SqlLiteral.createSymbol(value, POS);
    } else {
      RexCall call = (RexCall) e.getAfter();
      String operand =
          requireNonNull(stringValue(call.getOperands().get(0)),
              () -> "non-null string expected for 0th operand of AFTER call "
                  + call);
      after = call.getOperator().createCall(POS, new SqlIdentifier(operand, POS));
    }

    RexNode rexPattern = e.getPattern();
    final SqlNode pattern = context.toSql(null, rexPattern);
    final SqlLiteral strictStart = SqlLiteral.createBoolean(e.isStrictStart(), POS);
    final SqlLiteral strictEnd = SqlLiteral.createBoolean(e.isStrictEnd(), POS);

    RexLiteral rexInterval = (RexLiteral) e.getInterval();
    SqlIntervalLiteral interval = null;
    if (rexInterval != null) {
      interval = (SqlIntervalLiteral) context.toSql(null, rexInterval);
    }

    final SqlNodeList subsetList = new SqlNodeList(POS);
    for (Map.Entry<String, SortedSet<String>> entry : e.getSubsets().entrySet()) {
      SqlNode left = new SqlIdentifier(entry.getKey(), POS);
      List<SqlNode> rhl = new ArrayList<>();
      for (String right : entry.getValue()) {
        rhl.add(new SqlIdentifier(right, POS));
      }
      subsetList.add(
          SqlStdOperatorTable.EQUALS.createCall(POS, left,
              new SqlNodeList(rhl, POS)));
    }

    final SqlNodeList measureList = new SqlNodeList(POS);
    for (Map.Entry<String, RexNode> entry : e.getMeasures().entrySet()) {
      final String alias = entry.getKey();
      final SqlNode sqlNode = context.toSql(null, entry.getValue());
      measureList.add(as(sqlNode, alias));
    }

    final SqlNodeList patternDefList = new SqlNodeList(POS);
    for (Map.Entry<String, RexNode> entry : e.getPatternDefinitions().entrySet()) {
      final String alias = entry.getKey();
      final SqlNode sqlNode = context.toSql(null, entry.getValue());
      patternDefList.add(as(sqlNode, alias));
    }

    final SqlNode matchRecognize =
        new SqlMatchRecognize(POS, tableRef,
            pattern, strictStart, strictEnd, patternDefList, measureList, after,
            subsetList, rowsPerMatch, partitionList, orderByList, interval);
    return result(matchRecognize, Expressions.list(Clause.FROM), e, null);
  }

  private static SqlCall as(SqlNode e, String alias) {
    return SqlStdOperatorTable.AS.createCall(POS, e,
        new SqlIdentifier(alias, POS));
  }

  public Result visit(Uncollect e) {
    final Result x = visitInput(e, 0);
    final SqlOperator operator =
        e.withOrdinality ? SqlStdOperatorTable.UNNEST_WITH_ORDINALITY : SqlStdOperatorTable.UNNEST;
    final SqlNode unnestNode = operator.createCall(POS, x.asStatement());
    final List<SqlNode> operands =
        createAsFullOperands(e.getRowType(), unnestNode,
            requireNonNull(x.neededAlias,
                () -> "x.neededAlias is null, node is " + x.node));
    final SqlNode asNode = SqlStdOperatorTable.AS.createCall(POS, operands);
    return result(asNode, ImmutableList.of(Clause.FROM), e, null);
  }

  public Result visit(TableFunctionScan e) {
    final List<SqlNode> inputSqlNodes = new ArrayList<>();
    final List<SqlNode> fieldNodes = new ArrayList<>();

    final int fieldCount = e.getRowType().getFieldCount();
    final int inputSize = e.getInputs().size();

    for (int i = 0; i < fieldCount; i++) {
      fieldNodes.add(new SqlIdentifier(e.getRowType().getFieldNames().get(i), POS));
    }

    for (int i = 0; i < inputSize; i++) {
      final Result x = visitInput(e, i);
      inputSqlNodes.add(x.asStatement());
    }

    SqlNode callNode = null;
    if (((RexCall) e.getCall()).getOperator() instanceof SqlWindowTableFunction) {
      checkArgument(inputSqlNodes.size() == 1,
          "Number of input sql nodes for SqlWindowTableFunction must be 1.");
      final Context context = windowTableFunctionScanContext(inputSqlNodes.get(0), fieldNodes);
      callNode = context.toSql(null, e.getCall());
    } else {
      final Context context = tableFunctionScanContext(inputSqlNodes);
      callNode = context.toSql(null, e.getCall());
    }

    // Convert to table function call, "TABLE($function_name(xxx))"
    SqlNode tableCall =
        new SqlBasicCall(SqlStdOperatorTable.COLLECTION_TABLE,
            ImmutableList.of(callNode), SqlParserPos.ZERO);
    SqlNode select =
        new SqlSelect(SqlParserPos.ZERO, null, SqlNodeList.SINGLETON_STAR,
            tableCall, null, null, null, null, null, null, null, null,
            SqlNodeList.EMPTY);
    return result(select, ImmutableList.of(Clause.SELECT), e, null);
  }

  /**
   * Creates operands for a full AS operator. Format SqlNode AS alias(col_1, col_2,... ,col_n).
   *
   * @param rowType Row type of the SqlNode
   * @param leftOperand SqlNode
   * @param alias alias
   */
  public List<SqlNode> createAsFullOperands(RelDataType rowType, SqlNode leftOperand,
      String alias) {
    final List<SqlNode> result = new ArrayList<>();
    result.add(leftOperand);
    result.add(new SqlIdentifier(alias, POS));
    Ord.forEach(rowType.getFieldNames(), (fieldName, i) -> {
      if (SqlUtil.isGeneratedAlias(fieldName)) {
        fieldName = "col_" + i;
      }
      result.add(new SqlIdentifier(fieldName, POS));
    });
    return result;
  }

  @Override public void addSelect(List<SqlNode> selectList, SqlNode node,
      RelDataType rowType) {
    String name = rowType.getFieldNames().get(selectList.size());
    @Nullable String alias = SqlValidatorUtil.alias(node);
    if (alias == null || !alias.equals(name)) {
      node = as(node, name);
    }
    selectList.add(node);
  }

  private void parseCorrelTable(RelNode relNode, Result x) {
    for (CorrelationId id : relNode.getVariablesSet()) {
      correlTableMap.put(id, x.qualifiedContext());
    }
  }

  /** Stack frame. */
  private static class Frame {
    private final RelNode parent;
    private final RelNode r;
    private final boolean anon;
    private final boolean ignoreClauses;
    private final ImmutableSet<? extends Clause> expectedClauses;

    Frame(RelNode parent, int unusedOrdinalInParent, RelNode r, boolean anon,
        boolean ignoreClauses, Iterable<? extends Clause> expectedClauses) {
      this.parent = requireNonNull(parent, "parent");
      this.r = requireNonNull(r, "r");
      this.anon = anon;
      this.ignoreClauses = ignoreClauses;
      this.expectedClauses = ImmutableSet.copyOf(expectedClauses);
    }
  }
}
