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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * PushFilterPastJoinRule implements the rule for pushing filters above and
 * within a join node into the join node and/or its children nodes.
 */
public abstract class PushFilterPastJoinRule extends RelOptRule {
  public static final PushFilterPastJoinRule FILTER_ON_JOIN =
      new PushFilterIntoJoinRule(true);

  /** Dumber version of {@link #FILTER_ON_JOIN}. Not intended for production
   * use, but keeps some tests working for which {@code FILTER_ON_JOIN} is too
   * smart. */
  public static final PushFilterPastJoinRule DUMB_FILTER_ON_JOIN =
      new PushFilterIntoJoinRule(false);

  public static final PushFilterPastJoinRule JOIN =
      new PushDownJoinConditionRule(RelFactories.DEFAULT_FILTER_FACTORY,
          RelFactories.DEFAULT_PROJECT_FACTORY);

  /** Whether to try to strengthen join-type. */
  private final boolean smart;

  private final RelFactories.FilterFactory filterFactory;

  private final RelFactories.ProjectFactory projectFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushFilterPastJoinRule with an explicit root operand and
   * factories.
   */
  protected PushFilterPastJoinRule(RelOptRuleOperand operand, String id,
      boolean smart, RelFactories.FilterFactory filterFactory,
      RelFactories.ProjectFactory projectFactory) {
    super(operand, "PushFilterRule: " + id);
    this.smart = smart;
    this.filterFactory = filterFactory;
    this.projectFactory = projectFactory;
  }

  //~ Methods ----------------------------------------------------------------

  protected void perform(RelOptRuleCall call, FilterRelBase filter,
      JoinRelBase join) {
    final List<RexNode> joinFilters =
        RelOptUtil.conjunctions(join.getCondition());
    final List<RexNode> origJoinFilters = ImmutableList.copyOf(joinFilters);

    // If there is only the joinRel,
    // make sure it does not match a cartesian product joinRel
    // (with "true" condition), otherwise this rule will be applied
    // again on the new cartesian product joinRel.
    if (filter == null && joinFilters.isEmpty()) {
      return;
    }

    final List<RexNode> aboveFilters =
        filter != null
            ? RelOptUtil.conjunctions(filter.getCondition())
            : Lists.<RexNode>newArrayList();
    final ImmutableList<RexNode> origAboveFilters =
        ImmutableList.copyOf(aboveFilters);

    // Simplify Outer Joins
    JoinRelType joinType = join.getJoinType();
    if (smart
        && !origAboveFilters.isEmpty()
        && join.getJoinType() != JoinRelType.INNER) {
      joinType = RelOptUtil.simplifyJoin(join, origAboveFilters, joinType);
    }

    List<RexNode> leftFilters = new ArrayList<RexNode>();
    List<RexNode> rightFilters = new ArrayList<RexNode>();

    // TODO - add logic to derive additional filters.  E.g., from
    // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
    // derive table filters:
    // (t1.a = 1 OR t1.b = 3)
    // (t2.a = 2 OR t2.b = 4)

    // Try to push down above filters. These are typically where clause
    // filters. They can be pushed down if they are not on the NULL
    // generating side.
    boolean filterPushed = false;
    if (RelOptUtil.classifyFilters(
        join,
        aboveFilters,
        joinType,
        !(join instanceof EquiJoinRel),
        !joinType.generatesNullsOnLeft(),
        !joinType.generatesNullsOnRight(),
        joinFilters,
        leftFilters,
        rightFilters)) {
      filterPushed = true;
    }

    // Move join filters up if needed
    validateJoinFilters(aboveFilters, joinFilters, join, joinType);

    // If no filter got pushed after validate, reset filterPushed flag
    if (leftFilters.isEmpty()
        && rightFilters.isEmpty()
        && joinFilters.size() == origJoinFilters.size()) {
      if (Sets.newHashSet(joinFilters)
          .equals(Sets.newHashSet(origJoinFilters))) {
        filterPushed = false;
      }
    }

    // Try to push down filters in ON clause. A ON clause filter can only be
    // pushed down if it does not affect the non-matching set, i.e. it is
    // not on the side which is preserved.
    if (RelOptUtil.classifyFilters(
        join,
        joinFilters,
        joinType,
        false,
        !joinType.generatesNullsOnRight(),
        !joinType.generatesNullsOnLeft(),
        joinFilters,
        leftFilters,
        rightFilters)) {
      filterPushed = true;
    }

    // if nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if ((!filterPushed
            && joinType == join.getJoinType())
        || (joinFilters.isEmpty()
            && leftFilters.isEmpty()
            && rightFilters.isEmpty())) {
      return;
    }

    // create FilterRels on top of the children if any filters were
    // pushed to them
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    RelNode leftRel =
        RelOptUtil.createFilter(join.getLeft(), leftFilters, filterFactory);
    RelNode rightRel =
        RelOptUtil.createFilter(join.getRight(), rightFilters, filterFactory);

    // create the new join node referencing the new children and
    // containing its new join filters (if there are any)
    final RexNode joinFilter =
        RexUtil.composeConjunction(rexBuilder, joinFilters, false);

    // If nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if (joinFilter.isAlwaysTrue()
        && leftFilters.isEmpty()
        && rightFilters.isEmpty()
        && joinType == join.getJoinType()) {
      return;
    }

    RelNode newJoinRel =
        join.copy(
            join.getCluster().traitSetOf(Convention.NONE),
            joinFilter,
            leftRel,
            rightRel,
            joinType,
            join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newJoinRel);
    if (!leftFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, leftRel);
    }
    if (!rightFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, rightRel);
    }

    // Create a project on top of the join if some of the columns have become
    // NOT NULL due to the join-type getting stricter.
    newJoinRel = RelOptUtil.createCastRel(newJoinRel, join.getRowType(),
        false, projectFactory);

    // create a FilterRel on top of the join if needed
    RelNode newRel =
        RelOptUtil.createFilter(newJoinRel, aboveFilters, filterFactory);

    call.transformTo(newRel);
  }

  /**
   * Validates that target execution framework can satisfy join filters.
   *
   * <p>If the join filter cannot be satisfied (for example, if it is
   * {@code l.c1 > r.c2} and the join only supports equi-join), removes the
   * filter from {@code joinFilters} and adds it to {@code aboveFilters}.
   *
   * <p>The default implementation does nothing; i.e. the join can handle all
   * conditions.
   *
   * @param aboveFilters Filter above Join
   * @param joinFilters Filters in join condition
   * @param join Join
   *
   * @deprecated Use {@link #validateJoinFilters(java.util.List, java.util.List, org.eigenbase.rel.JoinRelBase, org.eigenbase.rel.JoinRelType)};
   * very short-term; will be removed before
   * {@link org.eigenbase.util.Bug#upgrade(String) calcite-0.9.2}.
   */
  protected void validateJoinFilters(List<RexNode> aboveFilters,
      List<RexNode> joinFilters, JoinRelBase join) {
    validateJoinFilters(aboveFilters, joinFilters, join, join.getJoinType());
  }

  /**
   * Validates that target execution framework can satisfy join filters.
   *
   * <p>If the join filter cannot be satisfied (for example, if it is
   * {@code l.c1 > r.c2} and the join only supports equi-join), removes the
   * filter from {@code joinFilters} and adds it to {@code aboveFilters}.
   *
   * <p>The default implementation does nothing; i.e. the join can handle all
   * conditions.
   *
   * @param aboveFilters Filter above Join
   * @param joinFilters Filters in join condition
   * @param join Join
   * @param joinType JoinRelType could be different from type in Join due to
   * outer join simplification.
   */
  protected void validateJoinFilters(List<RexNode> aboveFilters,
      List<RexNode> joinFilters, JoinRelBase join, JoinRelType joinType) {
    return;
  }

  /** Rule that pushes parts of the join condition to its inputs. */
  public static class PushDownJoinConditionRule
      extends PushFilterPastJoinRule {
    public PushDownJoinConditionRule(RelFactories.FilterFactory filterFactory,
        RelFactories.ProjectFactory projectFactory) {
      super(RelOptRule.operand(JoinRelBase.class, RelOptRule.any()),
          "PushFilterPastJoinRule:no-filter",
          true, filterFactory, projectFactory);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      JoinRelBase join = call.rel(0);
      perform(call, null, join);
    }
  }

  /** Rule that tries to push filter expressions into a join
   * condition and into the inputs of the join. */
  public static class PushFilterIntoJoinRule extends PushFilterPastJoinRule {
    public PushFilterIntoJoinRule(boolean smart) {
      this(smart, RelFactories.DEFAULT_FILTER_FACTORY,
          RelFactories.DEFAULT_PROJECT_FACTORY);
    }

    public PushFilterIntoJoinRule(boolean smart,
        RelFactories.FilterFactory filterFactory,
        RelFactories.ProjectFactory projectFactory) {
      super(
          RelOptRule.operand(FilterRelBase.class,
              RelOptRule.operand(JoinRelBase.class, RelOptRule.any())),
          "PushFilterPastJoinRule:filter",
          smart, filterFactory, projectFactory);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      FilterRelBase filter = call.rel(0);
      JoinRelBase join = call.rel(1);
      perform(call, filter, join);
    }
  }
}

// End PushFilterPastJoinRule.java
