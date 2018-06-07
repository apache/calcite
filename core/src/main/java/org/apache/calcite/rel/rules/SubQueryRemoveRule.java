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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Transform that converts IN, EXISTS and scalar sub-queries into joins.
 *
 * <p>Sub-queries are represented by {@link RexSubQuery} expressions.
 *
 * <p>A sub-query may or may not be correlated. If a sub-query is correlated,
 * the wrapped {@link RelNode} will contain a {@link RexCorrelVariable} before
 * the rewrite, and the product of the rewrite will be a {@link Correlate}.
 * The Correlate can be removed using {@link RelDecorrelator}.
 */
public abstract class SubQueryRemoveRule extends RelOptRule {
  public static final SubQueryRemoveRule PROJECT =
      new SubQueryProjectRemoveRule(RelFactories.LOGICAL_BUILDER);

  public static final SubQueryRemoveRule FILTER =
      new SubQueryFilterRemoveRule(RelFactories.LOGICAL_BUILDER);

  public static final SubQueryRemoveRule JOIN =
      new SubQueryJoinRemoveRule(RelFactories.LOGICAL_BUILDER);

  /**
   * Creates a SubQueryRemoveRule.
   *
   * @param operand     root operand, must not be null
   * @param description Description, or null to guess description
   * @param relBuilderFactory Builder for relational expressions
   */
  public SubQueryRemoveRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory,
      String description) {
    super(operand, relBuilderFactory, description);
  }

  protected RexNode apply(RexSubQuery e, Set<CorrelationId> variablesSet,
      RelOptUtil.Logic logic,
      RelBuilder builder, int inputCount, int offset) {
    switch (e.getKind()) {
    case SCALAR_QUERY:
      return rewriteScalarQuery(e, variablesSet, builder, inputCount, offset);
    case SOME:
      return rewriteSome(e, builder);
    case IN:
      return rewriteIn(e, variablesSet, logic, builder, offset);
    case EXISTS:
      return rewriteExists(e, variablesSet, logic, builder);
    default:
      throw new AssertionError(e.getKind());
    }
  }

  /**
   * Rewrites scalar sub-query into aggregate rel nodes.
   *
   * @param e            IN sub-query to rewrite
   * @param variablesSet a set of variables used by a relational
   *                     expression of the specified RexSubQuery
   * @param builder      RelBuilder instance
   * @param offset       offset to shift {@link RexInputRef}
   * @return RexNode instance which may be used to replace specified RexSubQuery.
   */
  private RexNode rewriteScalarQuery(RexSubQuery e, Set<CorrelationId> variablesSet,
      RelBuilder builder, int inputCount, int offset) {
    builder.push(e.rel);
    final RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
    final Boolean unique = mq.areColumnsUnique(builder.peek(),
        ImmutableBitSet.of());
    if (unique == null || !unique) {
      builder.aggregate(builder.groupKey(),
          builder.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE, false,
              false, null, null, builder.field(0)));
    }
    builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);
    return field(builder, inputCount, offset);
  }

  /**
   * Rewrites SOME sub-query into join rel nodes.
   *
   * @param e            SOME sub-query to rewrite
   * @param builder      RelBuilder instance
   * @return RexNode instance which may be used to replace specified RexSubQuery.
   */
  private RexNode rewriteSome(RexSubQuery e, RelBuilder builder) {
    // Most general case, where the left and right keys might have nulls, and
    // caller requires 3-valued logic return.
    //
    // select e.deptno, e.deptno < some (select deptno from emp) as v
    // from emp as e
    //
    // becomes
    //
    // select e.deptno,
    //   case
    //   when q.c = 0 then false // sub-query is empty
    //   when (e.deptno < q.m) is true then true
    //   when q.c > q.d then unknown // sub-query has at least one null
    //   else e.deptno < q.m
    //   end as v
    // from emp as e
    // cross join (
    //   select max(deptno) as m, count(*) as c, count(deptno) as d
    //   from emp) as q
    //
    final SqlQuantifyOperator op = (SqlQuantifyOperator) e.op;
    builder.push(e.rel)
        .aggregate(builder.groupKey(),
            op.comparisonKind == SqlKind.GREATER_THAN
              || op.comparisonKind == SqlKind.GREATER_THAN_OR_EQUAL
              ? builder.min("m", builder.field(0))
              : builder.max("m", builder.field(0)),
            builder.count(false, "c"),
            builder.count(false, "d", builder.field(0)))
        .as("q")
        .join(JoinRelType.INNER);
    return builder.call(SqlStdOperatorTable.CASE,
      builder.call(SqlStdOperatorTable.EQUALS,
          builder.field("q", "c"), builder.literal(0)),
      builder.literal(false),
      builder.call(SqlStdOperatorTable.IS_TRUE,
          builder.call(RelOptUtil.op(op.comparisonKind, null),
              e.operands.get(0), builder.field("q", "m"))),
      builder.literal(true),
      builder.call(SqlStdOperatorTable.GREATER_THAN,
          builder.field("q", "c"), builder.field("q", "d")),
      builder.literal(null),
      builder.call(RelOptUtil.op(op.comparisonKind, null),
          e.operands.get(0), builder.field("q", "m")));
  }

  /**
   * Rewrites EXISTS RexSubQuery into join rel nodes.
   *
   * @param e            EXISTS sub-query to rewrite
   * @param variablesSet a set of variables used by a relational
   *                     expression of the specified RexSubQuery
   * @param logic        a suitable logic for evaluating
   * @param builder      RelBuilder instance
   * @return RexNode instance which may be used to replace specified RexSubQuery.
   */
  private RexNode rewriteExists(RexSubQuery e, Set<CorrelationId> variablesSet,
      RelOptUtil.Logic logic, RelBuilder builder) {
    builder.push(e.rel);

    builder.project(builder.alias(builder.literal(true), "i"));
    switch (logic) {
    case TRUE:
      // Handles queries with single EXISTS in filter condition:
      // select e.deptno from emp as e
      // where exists (select deptno from emp)
      builder.aggregate(builder.groupKey(0));
      builder.as("dt");
      builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);
      return builder.literal(true);
    default:
      builder.distinct();
    }

    builder.as("dt");

    builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);

    return builder.isNotNull(Util.last(builder.fields()));
  }

  /**
   * Rewrites IN RexSubQuery into join rel nodes.
   *
   * @param e            IN sub-query to rewrite
   * @param variablesSet a set of variables used by a relational
   *                     expression of the specified RexSubQuery
   * @param logic        a suitable logic for evaluating
   * @param builder      RelBuilder instance
   * @param offset       offset to shift {@link RexInputRef}
   * @return RexNode instance which may be used to replace specified RexSubQuery.
   */
  private RexNode rewriteIn(RexSubQuery e, Set<CorrelationId> variablesSet,
      RelOptUtil.Logic logic, RelBuilder builder, int offset) {
    // Most general case, where the left and right keys might have nulls, and
    // caller requires 3-valued logic return.
    //
    // select e.deptno, e.deptno in (select deptno from emp)
    // from emp as e
    //
    // becomes
    //
    // select e.deptno,
    //   case
    //   when ct.c = 0 then false
    //   when dt.i is not null then true
    //   when e.deptno is null then null
    //   when ct.ck < ct.c then null
    //   else false
    //   end
    // from emp as e
    // left join (
    //   (select count(*) as c, count(deptno) as ck from emp) as ct
    //   cross join (select distinct deptno, true as i from emp)) as dt
    //   on e.deptno = dt.deptno
    //
    // If keys are not null we can remove "ct" and simplify to
    //
    // select e.deptno,
    //   case
    //   when dt.i is not null then true
    //   else false
    //   end
    // from emp as e
    // left join (select distinct deptno, true as i from emp) as dt
    //   on e.deptno = dt.deptno
    //
    // We could further simplify to
    //
    // select e.deptno,
    //   dt.i is not null
    // from emp as e
    // left join (select distinct deptno, true as i from emp) as dt
    //   on e.deptno = dt.deptno
    //
    // but have not yet.
    //
    // If the logic is TRUE we can just kill the record if the condition
    // evaluates to FALSE or UNKNOWN. Thus the query simplifies to an inner
    // join:
    //
    // select e.deptno,
    //   true
    // from emp as e
    // inner join (select distinct deptno from emp) as dt
    //   on e.deptno = dt.deptno
    //

    builder.push(e.rel);
    final List<RexNode> fields = new ArrayList<>(builder.fields());

    // for the case when IN has only literal operands, it may be handled
    // in the simpler way:
    //
    // select e.deptno, 123456 in (select deptno from emp)
    // from emp as e
    //
    // becomes
    //
    // SELECT e.deptno,
    //        CASE
    //            WHEN dt.cs IS FALSE THEN NULL
    //            WHEN dt.cs IS NOT NULL THEN TRUE
    //            WHEN e.deptno IS NULL THEN NULL
    //            ELSE FALSE
    //        END
    // FROM emp AS e
    // CROSS JOIN
    //   (SELECT DISTINCT CASE
    //                        WHEN deptno IS NULL THEN FALSE
    //                        ELSE TRUE
    //                    END cs
    //    FROM emp
    //    WHERE deptno=123
    //      OR deptno IS NULL) AS dt
    //

    boolean allLiterals = RexUtil.allLiterals(e.getOperands());
    final List<RexNode> expressionOperands = new ArrayList<>(e.getOperands());
    if (allLiterals) {
      final List<RexNode> conditions = new ArrayList<>();
      for (Pair<RexNode, RexNode> pair
          : Pair.zip(expressionOperands, fields)) {
        conditions.add(
            builder.equals(pair.left, pair.right));
      }
      switch (logic) {
      case TRUE:
      case TRUE_FALSE:
        builder.filter(conditions);
        builder.project(builder.alias(builder.literal(true), "cs"));
        builder.distinct();
        break;
      default:
        List<RexNode> isNullConditions = new ArrayList<>();
        for (RexNode field : fields) {
          isNullConditions.add(builder.isNull(field));
        }
        builder.filter(
            builder.or(
                builder.and(conditions),
                builder.or(isNullConditions)));
        final ImmutableList.Builder<RexNode> operands = ImmutableList.builder();
        operands.add(builder.or(isNullConditions), builder.literal(false));
        operands.add(builder.literal(true));
        RexNode project = builder.call(SqlStdOperatorTable.CASE, operands.build());
        builder.project(builder.alias(project, "cs"));

        builder.distinct();
      }
      // clears expressionOperands and fields lists since
      // all expressions were used in the filter
      expressionOperands.clear();
      fields.clear();
    } else {
      switch (logic) {
      case TRUE:
        builder.aggregate(builder.groupKey(fields));
        break;
      case TRUE_FALSE_UNKNOWN:
      case UNKNOWN_AS_TRUE:
        // Builds the cross join
        builder.aggregate(builder.groupKey(),
            builder.count(false, "c"),
            builder.aggregateCall(SqlStdOperatorTable.COUNT, false, false, null,
                "ck", builder.fields()));
        builder.as("ct");
        if (!variablesSet.isEmpty()) {
          builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);
        } else {
          builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);
        }
        offset += 2;
        builder.push(e.rel);
        // fall through
      default:
        fields.add(builder.alias(builder.literal(true), "i"));
        builder.project(fields);
        builder.distinct();
      }
    }

    builder.as("dt");
    final List<RexNode> conditions = new ArrayList<>();
    for (Pair<RexNode, RexNode> pair
        : Pair.zip(expressionOperands, builder.fields())) {
      conditions.add(
          builder.equals(pair.left, RexUtil.shift(pair.right, offset)));
    }
    switch (logic) {
    case TRUE:
      builder.join(JoinRelType.INNER, builder.and(conditions), variablesSet);
      return builder.literal(true);
    }
    // Now the left join
    builder.join(JoinRelType.LEFT, builder.and(conditions), variablesSet);

    final ImmutableList.Builder<RexNode> operands = ImmutableList.builder();

    Boolean b = true;
    switch (logic) {
    case TRUE_FALSE_UNKNOWN:
      b = null;
      // fall through
    case UNKNOWN_AS_TRUE:
      if (allLiterals) {
        operands.add(
            builder.equals(builder.field("cs"), builder.literal(false)),
            builder.literal(b));
        break;
      }
      operands.add(
          builder.equals(builder.field("ct", "c"), builder.literal(0)),
          builder.literal(false));
      break;
    }

    if (allLiterals) {
      operands.add(builder.isNotNull(builder.field("cs")),
          builder.literal(true));
    } else {
      operands.add(builder.isNotNull(Util.last(builder.fields())),
          builder.literal(true));
    }

    final List<RexNode> keyIsNulls = new ArrayList<>();
    for (RexNode operand : e.getOperands()) {
      if (operand.getType().isNullable()) {
        keyIsNulls.add(builder.isNull(operand));
      }
    }

    if (!keyIsNulls.isEmpty()) {
      operands.add(builder.or(keyIsNulls), builder.literal(null));
    }

    if (!allLiterals) {
      switch (logic) {
      case TRUE_FALSE_UNKNOWN:
      case UNKNOWN_AS_TRUE:
        operands.add(
            builder.call(SqlStdOperatorTable.LESS_THAN,
                builder.field("ct", "ck"), builder.field("ct", "c")),
            builder.literal(b));
      }
    }
    operands.add(builder.literal(false));
    return builder.call(SqlStdOperatorTable.CASE, operands.build());
  }

  /** Returns a reference to a particular field, by offset, across several
   * inputs on a {@link RelBuilder}'s stack. */
  private RexInputRef field(RelBuilder builder, int inputCount, int offset) {
    for (int inputOrdinal = 0;;) {
      final RelNode r = builder.peek(inputCount, inputOrdinal);
      if (offset < r.getRowType().getFieldCount()) {
        return builder.field(inputCount, inputOrdinal, offset);
      }
      ++inputOrdinal;
      offset -= r.getRowType().getFieldCount();
    }
  }

  /** Returns a list of expressions that project the first {@code fieldCount}
   * fields of the top input on a {@link RelBuilder}'s stack. */
  private static List<RexNode> fields(RelBuilder builder, int fieldCount) {
    final List<RexNode> projects = new ArrayList<>();
    for (int i = 0; i < fieldCount; i++) {
      projects.add(builder.field(i));
    }
    return projects;
  }

  /**
   * Rule for converting sub-queries from project expressions into the {@link Correlate}.
   */
  public static class SubQueryProjectRemoveRule extends SubQueryRemoveRule {

    public SubQueryProjectRemoveRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Project.class, null, RexUtil.SubQueryFinder.PROJECT_PREDICATE,
              any()), relBuilderFactory, "SubQueryRemoveRule:Project");
    }

    public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final RelBuilder builder = call.builder();
      final RexSubQuery e =
          RexUtil.SubQueryFinder.find(project.getProjects());
      assert e != null;
      final RelOptUtil.Logic logic =
          LogicVisitor.find(RelOptUtil.Logic.TRUE_FALSE_UNKNOWN,
              project.getProjects(), e);
      builder.push(project.getInput());
      final int fieldCount = builder.peek().getRowType().getFieldCount();
      final RexNode target = apply(e, ImmutableSet.of(),
          logic, builder, 1, fieldCount);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
      builder.project(shuttle.apply(project.getProjects()),
          project.getRowType().getFieldNames());
      call.transformTo(builder.build());
    }
  }

  /**
   * Rule for converting sub-queries from filter expressions into the {@link Correlate}.
   */
  public static class SubQueryFilterRemoveRule extends SubQueryRemoveRule {

    public SubQueryFilterRemoveRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Filter.class, null, RexUtil.SubQueryFinder.FILTER_PREDICATE,
              any()), relBuilderFactory, "SubQueryRemoveRule:Filter");
    }

    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final RelBuilder builder = call.builder();
      builder.push(filter.getInput());
      int count = 0;
      RexNode c = filter.getCondition();
      while (true) {
        final RexSubQuery e = RexUtil.SubQueryFinder.find(c);
        if (e == null) {
          assert count > 0;
          break;
        }
        ++count;
        final RelOptUtil.Logic logic =
            LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(c), e);
        final Set<CorrelationId>  variablesSet =
            RelOptUtil.getVariablesUsed(e.rel);
        final RexNode target = apply(e, variablesSet, logic,
            builder, 1, builder.peek().getRowType().getFieldCount());
        final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
        c = c.accept(shuttle);
      }
      builder.filter(c);
      builder.project(fields(builder, filter.getRowType().getFieldCount()));
      call.transformTo(builder.build());
    }
  }

  /**
   * Rule for converting sub-queries from join expressions into the {@link Correlate}.
   */
  public static class SubQueryJoinRemoveRule extends SubQueryRemoveRule {
    public SubQueryJoinRemoveRule(RelBuilderFactory relBuilderFactory) {
      super(
          operand(Join.class, null, RexUtil.SubQueryFinder.JOIN_PREDICATE,
              any()), relBuilderFactory, "SubQueryRemoveRule:Join");
    }

    public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      final RelBuilder builder = call.builder();
      final RexSubQuery e =
          RexUtil.SubQueryFinder.find(join.getCondition());
      assert e != null;
      final RelOptUtil.Logic logic =
          LogicVisitor.find(RelOptUtil.Logic.TRUE,
              ImmutableList.of(join.getCondition()), e);
      builder.push(join.getLeft());
      builder.push(join.getRight());
      final int fieldCount = join.getRowType().getFieldCount();
      final RexNode target = apply(e, ImmutableSet.of(),
          logic, builder, 2, fieldCount);
      final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
      builder.join(join.getJoinType(), shuttle.apply(join.getCondition()));
      builder.project(fields(builder, join.getRowType().getFieldCount()));
      call.transformTo(builder.build());
    }
  }

  /** Shuttle that replaces occurrences of a given
   * {@link org.apache.calcite.rex.RexSubQuery} with a replacement
   * expression. */
  private static class ReplaceSubQueryShuttle extends RexShuttle {
    private final RexSubQuery subQuery;
    private final RexNode replacement;

    ReplaceSubQueryShuttle(RexSubQuery subQuery, RexNode replacement) {
      this.subQuery = subQuery;
      this.replacement = replacement;
    }

    @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
      return RexUtil.eq(subQuery, this.subQuery) ? replacement : subQuery;
    }
  }
}

// End SubQueryRemoveRule.java
