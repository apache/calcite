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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Planner rule that expands distinct aggregates
 * (such as {@code COUNT(DISTINCT x)}) from a
 * {@link org.apache.calcite.rel.logical.LogicalAggregate}.
 *
 * <p>How this is done depends upon the arguments to the function. If all
 * functions have the same argument
 * (e.g. {@code COUNT(DISTINCT x), SUM(DISTINCT x)} both have the argument
 * {@code x}) then one extra {@link org.apache.calcite.rel.core.Aggregate} is
 * sufficient.
 *
 * <p>If there are multiple arguments
 * (e.g. {@code COUNT(DISTINCT x), COUNT(DISTINCT y)})
 * the rule creates separate {@code Aggregate}s and combines using a
 * {@link org.apache.calcite.rel.core.Join}.
 */
public final class AggregateExpandDistinctAggregatesRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The default instance of the rule; operates only on logical expressions. */
  public static final AggregateExpandDistinctAggregatesRule INSTANCE =
      new AggregateExpandDistinctAggregatesRule(LogicalAggregate.class,
          RelFactories.DEFAULT_JOIN_FACTORY);

  private final RelFactories.JoinFactory joinFactory;

  //~ Constructors -----------------------------------------------------------

  public AggregateExpandDistinctAggregatesRule(
      Class<? extends LogicalAggregate> clazz,
      RelFactories.JoinFactory joinFactory) {
    super(operand(clazz, any()));
    this.joinFactory = joinFactory;
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    if (!aggregate.containsDistinctCall()) {
      return;
    }

    // Find all of the agg expressions. We use a LinkedHashSet to ensure
    // determinism.
    int nonDistinctCount = 0;
    Set<List<Integer>> argListSets = new LinkedHashSet<List<Integer>>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (!aggCall.isDistinct()) {
        ++nonDistinctCount;
        continue;
      }
      ArrayList<Integer> argList = new ArrayList<Integer>();
      for (Integer arg : aggCall.getArgList()) {
        argList.add(arg);
      }
      argListSets.add(argList);
    }
    Util.permAssert(argListSets.size() > 0, "containsDistinctCall lied");

    // If all of the agg expressions are distinct and have the same
    // arguments then we can use a more efficient form.
    if ((nonDistinctCount == 0) && (argListSets.size() == 1)) {
      RelNode converted =
          convertMonopole(
              aggregate,
              argListSets.iterator().next());
      call.transformTo(converted);
      return;
    }

    // Create a list of the expressions which will yield the final result.
    // Initially, the expressions point to the input field.
    final List<RelDataTypeField> aggFields =
        aggregate.getRowType().getFieldList();
    final List<RexInputRef> refs = new ArrayList<RexInputRef>();
    final List<String> fieldNames = aggregate.getRowType().getFieldNames();
    final ImmutableBitSet groupSet = aggregate.getGroupSet();
    final int groupAndIndicatorCount =
        aggregate.getGroupCount() + aggregate.getIndicatorCount();
    for (int i : Util.range(groupAndIndicatorCount)) {
      refs.add(RexInputRef.of(i, aggFields));
    }

    // Aggregate the original relation, including any non-distinct aggregates.
    List<AggregateCall> newAggCallList = new ArrayList<AggregateCall>();
    int i = -1;
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      ++i;
      if (aggCall.isDistinct()) {
        refs.add(null);
        continue;
      }
      refs.add(
          new RexInputRef(
              groupAndIndicatorCount + newAggCallList.size(),
              aggFields.get(groupAndIndicatorCount + i).getType()));
      newAggCallList.add(aggCall);
    }

    // In the case where there are no non-distinct aggregates (regardless of
    // whether there are group bys), there's no need to generate the
    // extra aggregate and join.
    RelNode rel;
    if (newAggCallList.isEmpty()) {
      rel = null;
    } else {
      rel =
          LogicalAggregate.create(aggregate.getInput(), aggregate.indicator,
              groupSet, aggregate.getGroupSets(), newAggCallList);
    }

    // For each set of operands, find and rewrite all calls which have that
    // set of operands.
    for (List<Integer> argList : argListSets) {
      rel = doRewrite(aggregate, rel, argList, refs);
    }

    rel = RelOptUtil.createProject(rel, refs, fieldNames);

    call.transformTo(rel);
  }

  /**
   * Converts an aggregate relational expression that contains just one
   * distinct aggregate function (or perhaps several over the same arguments)
   * and no non-distinct aggregate functions.
   */
  private RelNode convertMonopole(
      Aggregate aggregate,
      List<Integer> argList) {
    // For example,
    //    SELECT deptno, COUNT(DISTINCT sal), SUM(DISTINCT sal)
    //    FROM emp
    //    GROUP BY deptno
    //
    // becomes
    //
    //    SELECT deptno, COUNT(distinct_sal), SUM(distinct_sal)
    //    FROM (
    //      SELECT DISTINCT deptno, sal AS distinct_sal
    //      FROM EMP GROUP BY deptno)
    //    GROUP BY deptno

    // Project the columns of the GROUP BY plus the arguments
    // to the agg function.
    Map<Integer, Integer> sourceOf = new HashMap<Integer, Integer>();
    final Aggregate distinct =
        createSelectDistinct(aggregate, argList, sourceOf);

    // Create an aggregate on top, with the new aggregate list.
    final List<AggregateCall> newAggCalls =
        Lists.newArrayList(aggregate.getAggCallList());
    rewriteAggCalls(newAggCalls, argList, sourceOf);
    final int cardinality = aggregate.getGroupSet().cardinality();
    return aggregate.copy(aggregate.getTraitSet(), distinct,
        aggregate.indicator, ImmutableBitSet.range(cardinality), null,
        newAggCalls);
  }

  /**
   * Converts all distinct aggregate calls to a given set of arguments.
   *
   * <p>This method is called several times, one for each set of arguments.
   * Each time it is called, it generates a JOIN to a new SELECT DISTINCT
   * relational expression, and modifies the set of top-level calls.
   *
   * @param aggregate Original aggregate
   * @param left      Child relational expression (either the original
   *                  aggregate, the output from the previous call to this
   *                  method, or null in the case where we're converting the
   *                  first distinct aggregate in a query with no non-distinct
   *                  aggregates)
   * @param argList   Arguments to the distinct aggregate function
   * @param refs      Array of expressions which will be the projected by the
   *                  result of this rule. Those relating to this arg list will
   *                  be modified
   * @return Relational expression
   */
  private RelNode doRewrite(
      Aggregate aggregate,
      RelNode left,
      List<Integer> argList,
      List<RexInputRef> refs) {
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final List<RelDataTypeField> leftFields;
    if (left == null) {
      leftFields = null;
    } else {
      leftFields = left.getRowType().getFieldList();
    }

    // LogicalAggregate(
    //     child,
    //     {COUNT(DISTINCT 1), SUM(DISTINCT 1), SUM(2)})
    //
    // becomes
    //
    // LogicalAggregate(
    //     LogicalJoin(
    //         child,
    //         LogicalAggregate(child, < all columns > {}),
    //         INNER,
    //         <f2 = f5>))
    //
    // E.g.
    //   SELECT deptno, SUM(DISTINCT sal), COUNT(DISTINCT gender), MAX(age)
    //   FROM Emps
    //   GROUP BY deptno
    //
    // becomes
    //
    //   SELECT e.deptno, adsal.sum_sal, adgender.count_gender, e.max_age
    //   FROM (
    //     SELECT deptno, MAX(age) as max_age
    //     FROM Emps GROUP BY deptno) AS e
    //   JOIN (
    //     SELECT deptno, COUNT(gender) AS count_gender FROM (
    //       SELECT DISTINCT deptno, gender FROM Emps) AS dgender
    //     GROUP BY deptno) AS adgender
    //     ON e.deptno = adgender.deptno
    //   JOIN (
    //     SELECT deptno, SUM(sal) AS sum_sal FROM (
    //       SELECT DISTINCT deptno, sal FROM Emps) AS dsal
    //     GROUP BY deptno) AS adsal
    //   ON e.deptno = adsal.deptno
    //   GROUP BY e.deptno
    //
    // Note that if a query contains no non-distinct aggregates, then the
    // very first join/group by is omitted.  In the example above, if
    // MAX(age) is removed, then the sub-select of "e" is not needed, and
    // instead the two other group by's are joined to one another.

    // Project the columns of the GROUP BY plus the arguments
    // to the agg function.
    Map<Integer, Integer> sourceOf = new HashMap<Integer, Integer>();
    final Aggregate distinct =
        createSelectDistinct(aggregate, argList, sourceOf);

    // Now compute the aggregate functions on top of the distinct dataset.
    // Each distinct agg becomes a non-distinct call to the corresponding
    // field from the right; for example,
    //   "COUNT(DISTINCT e.sal)"
    // becomes
    //   "COUNT(distinct_e.sal)".
    List<AggregateCall> aggCallList = new ArrayList<AggregateCall>();
    final List<AggregateCall> aggCalls = aggregate.getAggCallList();

    final int groupAndIndicatorCount =
        aggregate.getGroupCount() + aggregate.getIndicatorCount();
    int i = groupAndIndicatorCount - 1;
    for (AggregateCall aggCall : aggCalls) {
      ++i;

      // Ignore agg calls which are not distinct or have the wrong set
      // arguments. If we're rewriting aggs whose args are {sal}, we will
      // rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
      // COUNT(DISTINCT gender) or SUM(sal).
      if (!aggCall.isDistinct()) {
        continue;
      }
      if (!aggCall.getArgList().equals(argList)) {
        continue;
      }

      // Re-map arguments.
      final int argCount = aggCall.getArgList().size();
      final List<Integer> newArgs = new ArrayList<Integer>(argCount);
      for (int j = 0; j < argCount; j++) {
        final Integer arg = aggCall.getArgList().get(j);
        newArgs.add(sourceOf.get(arg));
      }
      final AggregateCall newAggCall =
          new AggregateCall(
              aggCall.getAggregation(),
              false,
              newArgs,
              aggCall.getType(),
              aggCall.getName());
      assert refs.get(i) == null;
      if (left == null) {
        refs.set(i,
            new RexInputRef(groupAndIndicatorCount + aggCallList.size(),
                newAggCall.getType()));
      } else {
        refs.set(i,
            new RexInputRef(leftFields.size() + groupAndIndicatorCount
                + aggCallList.size(), newAggCall.getType()));
      }
      aggCallList.add(newAggCall);
    }

    Aggregate distinctAgg =
        aggregate.copy(aggregate.getTraitSet(), distinct, aggregate.indicator,
            ImmutableBitSet.range(aggregate.getGroupSet().cardinality()),
            aggregate.getGroupSets(), aggCallList);

    // If there's no left child yet, no need to create the join
    if (left == null) {
      return distinctAgg;
    }

    // Create the join condition. It is of the form
    //  'left.f0 = right.f0 and left.f1 = right.f1 and ...'
    // where {f0, f1, ...} are the GROUP BY fields.
    final List<RelDataTypeField> distinctFields =
        distinctAgg.getRowType().getFieldList();
    final List<RexNode> conditions = Lists.newArrayList();
    for (i = 0; i < groupAndIndicatorCount; ++i) {
      // null values form its own group
      // use "is not distinct from" so that the join condition
      // allows null values to match.
      conditions.add(
          rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
              RexInputRef.of(i, leftFields),
              new RexInputRef(leftFields.size() + i,
                  distinctFields.get(i).getType())));
    }

    // Join in the new 'select distinct' relation.
    return joinFactory.createJoin(left,
        distinctAgg,
        RexUtil.composeConjunction(rexBuilder, conditions, false),
        JoinRelType.INNER,
        ImmutableSet.<String>of(),
        false);
  }

  private static void rewriteAggCalls(
      List<AggregateCall> newAggCalls,
      List<Integer> argList,
      Map<Integer, Integer> sourceOf) {
    // Rewrite the agg calls. Each distinct agg becomes a non-distinct call
    // to the corresponding field from the right; for example,
    // "COUNT(DISTINCT e.sal)" becomes   "COUNT(distinct_e.sal)".
    for (int i = 0; i < newAggCalls.size(); i++) {
      final AggregateCall aggCall = newAggCalls.get(i);

      // Ignore agg calls which are not distinct or have the wrong set
      // arguments. If we're rewriting aggs whose args are {sal}, we will
      // rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
      // COUNT(DISTINCT gender) or SUM(sal).
      if (!aggCall.isDistinct()) {
        continue;
      }
      if (!aggCall.getArgList().equals(argList)) {
        continue;
      }

      // Re-map arguments.
      final int argCount = aggCall.getArgList().size();
      final List<Integer> newArgs = new ArrayList<Integer>(argCount);
      for (int j = 0; j < argCount; j++) {
        final Integer arg = aggCall.getArgList().get(j);
        newArgs.add(sourceOf.get(arg));
      }
      final AggregateCall newAggCall =
          new AggregateCall(
              aggCall.getAggregation(),
              false,
              newArgs,
              aggCall.getType(),
              aggCall.getName());
      newAggCalls.set(i, newAggCall);
    }
  }

  /**
   * Given an {@link org.apache.calcite.rel.logical.LogicalAggregate}
   * and the ordinals of the arguments to a
   * particular call to an aggregate function, creates a 'select distinct'
   * relational expression which projects the group columns and those
   * arguments but nothing else.
   *
   * <p>For example, given
   *
   * <blockquote>
   * <pre>select f0, count(distinct f1), count(distinct f2)
   * from t group by f0</pre>
   * </blockquote>
   *
   * and the arglist
   *
   * <blockquote>{2}</blockquote>
   *
   * returns
   *
   * <blockquote>
   * <pre>select distinct f0, f2 from t</pre>
   * </blockquote>
   *
   * '
   *
   * <p>The <code>sourceOf</code> map is populated with the source of each
   * column; in this case sourceOf.get(0) = 0, and sourceOf.get(1) = 2.</p>
   *
   * @param aggregate Aggregate relational expression
   * @param argList   Ordinals of columns to make distinct
   * @param sourceOf  Out parameter, is populated with a map of where each
   *                  output field came from
   * @return Aggregate relational expression which projects the required
   * columns
   */
  private static Aggregate createSelectDistinct(
      Aggregate aggregate,
      List<Integer> argList,
      Map<Integer, Integer> sourceOf) {
    final List<Pair<RexNode, String>> projects =
        new ArrayList<Pair<RexNode, String>>();
    final RelNode child = aggregate.getInput();
    final List<RelDataTypeField> childFields =
        child.getRowType().getFieldList();
    for (int i : aggregate.getGroupSet()) {
      sourceOf.put(i, projects.size());
      projects.add(RexInputRef.of2(i, childFields));
    }
    for (Integer arg : argList) {
      if (sourceOf.get(arg) != null) {
        continue;
      }
      sourceOf.put(arg, projects.size());
      projects.add(RexInputRef.of2(arg, childFields));
    }
    final RelNode project =
        RelOptUtil.createProject(child, projects, false);

    // Get the distinct values of the GROUP BY fields and the arguments
    // to the agg functions.
    return aggregate.copy(aggregate.getTraitSet(), project, false,
        ImmutableBitSet.range(projects.size()),
        null, ImmutableList.<AggregateCall>of());
  }
}

// End AggregateExpandDistinctAggregatesRule.java
