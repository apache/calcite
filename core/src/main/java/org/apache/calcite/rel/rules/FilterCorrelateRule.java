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
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes filters above Correlate node into the children of the Correlate.
 */
public class FilterCorrelateRule extends RelOptRule {

  public static final FilterCorrelateRule INSTANCE =
      new FilterCorrelateRule(RelFactories.DEFAULT_FILTER_FACTORY,
          RelFactories.DEFAULT_PROJECT_FACTORY);

  private final RelFactories.FilterFactory filterFactory;

  private final RelFactories.ProjectFactory projectFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FilterCorrelateRule with an explicit root operand and
   * factories.
   */
  public FilterCorrelateRule(RelFactories.FilterFactory filterFactory,
      RelFactories.ProjectFactory projectFactory) {
    super(
        operand(Filter.class,
            operand(Correlate.class, RelOptRule.any())),
        "FilterCorrelateRule");
    this.filterFactory = filterFactory;
    this.projectFactory = projectFactory;
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    Filter filter = call.rel(0);
    Correlate corr = call.rel(1);

    if (filter == null) {
      return;
    }

    final List<RexNode> aboveFilters =
        filter != null
            ? RelOptUtil.conjunctions(filter.getCondition())
            : Lists.<RexNode>newArrayList();

    List<RexNode> leftFilters = new ArrayList<RexNode>();
    List<RexNode> rightFilters = new ArrayList<RexNode>();

    // Try to push down above filters. These are typically where clause
    // filters. They can be pushed down if they are not on the NULL
    // generating side.
    RelOptUtil.classifyFilters(
        corr,
        aboveFilters,
        JoinRelType.INNER,
        false,
        !corr.getJoinType().toJoinType().generatesNullsOnLeft(),
        !corr.getJoinType().toJoinType().generatesNullsOnRight(),
        aboveFilters,
        leftFilters,
        rightFilters);

    if (leftFilters.isEmpty()
        && rightFilters.isEmpty()) {
      // no filters got pushed
      return;
    }

    // create FilterRels on top of the children if any filters were
    // pushed to them
    final RexBuilder rexBuilder = corr.getCluster().getRexBuilder();
    RelNode leftRel =
        RelOptUtil.createFilter(corr.getLeft(), leftFilters, filterFactory);
    RelNode rightRel =
        RelOptUtil.createFilter(corr.getRight(), rightFilters, filterFactory);

    // create the new LogicalCorrelate rel
    List<RelNode> corrInputs = Lists.newArrayList();
    corrInputs.add(leftRel);
    corrInputs.add(rightRel);

    RelNode newCorrRel = corr.copy(corr.getTraitSet(), corrInputs);

    call.getPlanner().onCopy(corr, newCorrRel);

    if (!leftFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, leftRel);
    }
    if (!rightFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, rightRel);
    }

    // create a LogicalFilter on top of the join if needed
    RelNode newRel =
        RelOptUtil.createFilter(newCorrRel,
            RexUtil.fixUp(rexBuilder, aboveFilters, newCorrRel.getRowType()),
            filterFactory);

    call.transformTo(newRel);
  }

}

// End FilterCorrelateRule.java
