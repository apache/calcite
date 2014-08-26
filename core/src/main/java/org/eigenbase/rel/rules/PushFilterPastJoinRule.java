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
import org.eigenbase.util.Holder;

import com.google.common.collect.ImmutableList;

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
    if (join instanceof SemiJoinRel) {
      return;
    }
    final List<RexNode> joinFilters =
        RelOptUtil.conjunctions(join.getCondition());

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
            : ImmutableList.<RexNode>of();

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
    final Holder<JoinRelType> joinTypeHolder = Holder.of(join.getJoinType());
    if (RelOptUtil.classifyFilters(
        join,
        aboveFilters,
        join.getJoinType(),
        !(join instanceof EquiJoinRel),
        !join.getJoinType().generatesNullsOnLeft(),
        !join.getJoinType().generatesNullsOnRight(),
        joinFilters,
        leftFilters,
        rightFilters,
        joinTypeHolder,
        smart)) {
      filterPushed = true;
    }

    // Try to push down filters in ON clause. A ON clause filter can only be
    // pushed down if it does not affect the non-matching set, i.e. it is
    // not on the side which is preserved.
    if (RelOptUtil.classifyFilters(
        join,
        joinFilters,
        join.getJoinType(),
        false,
        !join.getJoinType().generatesNullsOnRight(),
        !join.getJoinType().generatesNullsOnLeft(),
        joinFilters,
        leftFilters,
        rightFilters,
        joinTypeHolder,
        false)) {
      filterPushed = true;
    }

    if (!filterPushed) {
      return;
    }

    // if nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if (joinFilters.isEmpty()
        && leftFilters.isEmpty()
        && rightFilters.isEmpty()) {
      return;
    }

    // create FilterRels on top of the children if any filters were
    // pushed to them
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    RelNode leftRel =
        createFilterOnRel(
            rexBuilder,
            join.getLeft(),
            leftFilters);
    RelNode rightRel =
        createFilterOnRel(
            rexBuilder,
            join.getRight(),
            rightFilters);

    // create the new join node referencing the new children and
    // containing its new join filters (if there are any)
    final RexNode joinFilter =
        RexUtil.composeConjunction(rexBuilder, joinFilters, false);

    // If nothing actually got pushed and there is nothing leftover,
    // then this rule is a no-op
    if (joinFilter.isAlwaysTrue()
        && leftFilters.isEmpty()
        && rightFilters.isEmpty()
        && joinTypeHolder.get() == join.getJoinType()) {
      return;
    }

    RelNode newJoinRel =
        join.copy(
            join.getCluster().traitSetOf(Convention.NONE),
            joinFilter,
            leftRel,
            rightRel,
            joinTypeHolder.get(),
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
        createFilterOnRel(rexBuilder, newJoinRel, aboveFilters);

    call.transformTo(newRel);
  }

  /**
   * If the filter list passed in is non-empty, creates a FilterRel on top of
   * the existing RelNode; otherwise, just returns the RelNode
   *
   * @param rexBuilder rex builder
   * @param rel        the RelNode that the filter will be put on top of
   * @param filters    list of filters
   * @return new RelNode or existing one if no filters
   */
  private RelNode createFilterOnRel(
      RexBuilder rexBuilder,
      RelNode rel,
      List<RexNode> filters) {
    RexNode andFilters =
        RexUtil.composeConjunction(rexBuilder, filters, false);
    if (andFilters.isAlwaysTrue()) {
      return rel;
    }
    return filterFactory.createFilter(rel, andFilters);
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
