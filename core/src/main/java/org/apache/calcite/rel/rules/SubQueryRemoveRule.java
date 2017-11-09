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
      new SubQueryRemoveRule(
          operand(Project.class, null, RexUtil.SubQueryFinder.PROJECT_PREDICATE,
              any()),
          RelFactories.LOGICAL_BUILDER, "SubQueryRemoveRule:Project") {
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
          final RexNode target = apply(e, ImmutableSet.<CorrelationId>of(),
              logic, builder, 1, fieldCount);
          final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
          builder.project(shuttle.apply(project.getProjects()),
              project.getRowType().getFieldNames());
          call.transformTo(builder.build());
        }
      };

  public static final SubQueryRemoveRule FILTER =
      new SubQueryRemoveRule(
          operand(Filter.class, null, RexUtil.SubQueryFinder.FILTER_PREDICATE,
              any()),
          RelFactories.LOGICAL_BUILDER, "SubQueryRemoveRule:Filter") {
        public void onMatch(RelOptRuleCall call) {
          final Filter filter = call.rel(0);
          final RelBuilder builder = call.builder();
          builder.push(filter.getInput());
          int count = 0;
          RexNode c = filter.getCondition();
          for (;;) {
            final RexSubQuery e = RexUtil.SubQueryFinder.find(c);
            if (e == null) {
              assert count > 0;
              break;
            }
            ++count;
            final RelOptUtil.Logic logic =
                LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(c),
                    e);
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
      };

  public static final SubQueryRemoveRule JOIN =
      new SubQueryRemoveRule(
          operand(Join.class, null, RexUtil.SubQueryFinder.JOIN_PREDICATE,
              any()), RelFactories.LOGICAL_BUILDER, "SubQueryRemoveRule:Join") {
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
          final RexNode target = apply(e, ImmutableSet.<CorrelationId>of(),
              logic, builder, 2, fieldCount);
          final RexShuttle shuttle = new ReplaceSubQueryShuttle(e, target);
          builder.join(join.getJoinType(), shuttle.apply(join.getCondition()));
          builder.project(fields(builder, join.getRowType().getFieldCount()));
          call.transformTo(builder.build());
        }
      };

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
      builder.push(e.rel);
      final RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
      final Boolean unique = mq.areColumnsUnique(builder.peek(),
          ImmutableBitSet.of());
      if (unique == null || !unique) {
        builder.aggregate(builder.groupKey(),
            builder.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE, false, null,
                null, builder.field(0)));
      }
      builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);
      return field(builder, inputCount, offset);

    case SOME:
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

    case IN:
    case EXISTS:
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
      final List<RexNode> fields = new ArrayList<>();
      switch (e.getKind()) {
      case IN:
        fields.addAll(builder.fields());
      }

      // First, the cross join
      switch (logic) {
      case TRUE_FALSE_UNKNOWN:
      case UNKNOWN_AS_TRUE:
        // Since EXISTS/NOT EXISTS are not affected by presence of
        // null keys we do not need to generate count(*), count(c)
        if (e.getKind() == SqlKind.EXISTS) {
          logic = RelOptUtil.Logic.TRUE_FALSE;
          break;
        }
        builder.aggregate(builder.groupKey(),
            builder.count(false, "c"),
            builder.aggregateCall(SqlStdOperatorTable.COUNT, false, null, "ck",
                builder.fields()));
        builder.as("ct");
        if (!variablesSet.isEmpty()) {
          builder.join(JoinRelType.LEFT, builder.literal(true), variablesSet);
        } else {
          builder.join(JoinRelType.INNER, builder.literal(true), variablesSet);
        }
        offset += 2;
        builder.push(e.rel);
        break;
      }

      // Now the left join
      switch (logic) {
      case TRUE:
        if (fields.isEmpty()) {
          builder.project(builder.alias(builder.literal(true), "i"));
          builder.aggregate(builder.groupKey(0));
        } else {
          builder.aggregate(builder.groupKey(fields));
        }
        break;
      default:
        fields.add(builder.alias(builder.literal(true), "i"));
        builder.project(fields);
        builder.distinct();
      }
      builder.as("dt");
      final List<RexNode> conditions = new ArrayList<>();
      for (Pair<RexNode, RexNode> pair
          : Pair.zip(e.getOperands(), builder.fields())) {
        conditions.add(
            builder.equals(pair.left, RexUtil.shift(pair.right, offset)));
      }
      switch (logic) {
      case TRUE:
        builder.join(JoinRelType.INNER, builder.and(conditions), variablesSet);
        return builder.literal(true);
      }
      builder.join(JoinRelType.LEFT, builder.and(conditions), variablesSet);

      final List<RexNode> keyIsNulls = new ArrayList<>();
      for (RexNode operand : e.getOperands()) {
        if (operand.getType().isNullable()) {
          keyIsNulls.add(builder.isNull(operand));
        }
      }
      final ImmutableList.Builder<RexNode> operands = ImmutableList.builder();
      switch (logic) {
      case TRUE_FALSE_UNKNOWN:
      case UNKNOWN_AS_TRUE:
        operands.add(
            builder.equals(builder.field("ct", "c"), builder.literal(0)),
            builder.literal(false));
        break;
      }
      operands.add(builder.isNotNull(Util.last(builder.fields())),
          builder.literal(true));
      if (!keyIsNulls.isEmpty()) {
        operands.add(builder.or(keyIsNulls), builder.literal(null));
      }
      Boolean b = true;
      switch (logic) {
      case TRUE_FALSE_UNKNOWN:
        b = null;
        // fall through
      case UNKNOWN_AS_TRUE:
        operands.add(
            builder.call(SqlStdOperatorTable.LESS_THAN,
                builder.field("ct", "ck"), builder.field("ct", "c")),
            builder.literal(b));
        break;
      }
      operands.add(builder.literal(false));
      return builder.call(SqlStdOperatorTable.CASE, operands.build());

    default:
      throw new AssertionError(e.getKind());
    }
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
