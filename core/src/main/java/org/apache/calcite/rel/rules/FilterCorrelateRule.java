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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

/**
 * Planner rule that pushes a {@link Filter} above a {@link Correlate} into the
 * inputs of the Correlate.
 *
 * @see CoreRules#FILTER_CORRELATE
 */
@Value.Enclosing
public class FilterCorrelateRule
    extends RelRule<FilterCorrelateRule.Config>
    implements TransformationRule {

  /** Creates a FilterCorrelateRule. */
  protected FilterCorrelateRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public FilterCorrelateRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  @Deprecated // to be removed before 2.0
  public FilterCorrelateRule(RelFactories.FilterFactory filterFactory,
      RelFactories.ProjectFactory projectFactory) {
    this(RelBuilder.proto(filterFactory, projectFactory));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Correlate corr = call.rel(1);

    final List<RexNode> aboveFilters =
        RelOptUtil.conjunctions(filter.getCondition());

    final List<RexNode> leftFilters = new ArrayList<>();
    final List<RexNode> rightFilters = new ArrayList<>();

    // Try to push down above filters. These are typically where clause
    // filters. They can be pushed down if they are not on the NULL
    // generating side.
    RelOptUtil.classifyFilters(
        corr,
        aboveFilters,
        false,
        true,
        corr.getJoinType().canPushRightFromAbove(),
        aboveFilters,
        leftFilters,
        rightFilters);

    if (leftFilters.isEmpty()
        && rightFilters.isEmpty()) {
      // no filters got pushed
      return;
    }

    // Create Filters on top of the children if any filters were
    // pushed to them.
    final RexBuilder rexBuilder = corr.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();
    final RelNode leftRel =
        relBuilder.push(corr.getLeft()).filter(leftFilters).build();
    final RelNode rightRel =
        relBuilder.push(corr.getRight()).filter(rightFilters).build();

    // Create the new Correlate
    RelNode newCorrRel =
        corr.copy(corr.getTraitSet(), ImmutableList.of(leftRel, rightRel));

    call.getPlanner().onCopy(corr, newCorrRel);

    if (!leftFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, leftRel);
    }
    if (!rightFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, rightRel);
    }

    // Create a Filter on top of the join if needed
    relBuilder.push(newCorrRel);
    relBuilder.filter(
        RexUtil.fixUp(rexBuilder, aboveFilters,
            RelOptUtil.getFieldTypeList(relBuilder.peek().getRowType())));

    if (! (corr.getRight() instanceof RelSubset
        || corr.getLeft() instanceof RelSubset)) {
      HepRelVertex rightHepRelVertex = (HepRelVertex) corr.getRight();
      HepRelVertex leftHepRelVertex = (HepRelVertex) corr.getLeft();
      if (!(rightHepRelVertex.getCurrentRel() instanceof LogicalCorrelate
          || leftHepRelVertex.getCurrentRel() instanceof LogicalCorrelate)
          && (rightHepRelVertex.getCurrentRel() instanceof Uncollect
          || leftHepRelVertex.getCurrentRel() instanceof Uncollect)) {
        Stack<Triple<RelNode, Integer, JoinRelType>> stackForTableScanWithEndColumnIndex =
            new Stack<>();
        List<RexNode> filterToModify = RelOptUtil.conjunctions(filter.getCondition());
        populateStackWithEndIndexesForTables(corr,
            stackForTableScanWithEndColumnIndex, filterToModify);
        RelNode uncollectRelWithWhere = moveConditionsFromWhereClauseToJoinOnClause(filterToModify,
            stackForTableScanWithEndColumnIndex, relBuilder, corr);
        relBuilder.push(uncollectRelWithWhere);
      }
    }
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableFilterCorrelateRule.Config.of()
        .withOperandFor(Filter.class, Correlate.class);

    @Override default FilterCorrelateRule toRule() {
      return new FilterCorrelateRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass,
        Class<? extends Correlate> correlateClass) {
      return withOperandSupplier(b0 ->
          b0.operand(filterClass).oneInput(b1 ->
              b1.operand(correlateClass).anyInputs()))
          .as(Config.class);
    }
  }


  private RelNode moveConditionsFromWhereClauseToJoinOnClause(List<RexNode> allConditions,
      Stack<Triple<RelNode, Integer, JoinRelType>> stack, RelBuilder builder, Correlate correlate) {
    Triple<RelNode, Integer, JoinRelType> leftEntry = stack.pop();
    Triple<RelNode, Integer, JoinRelType> rightEntry;
    RelNode left = leftEntry.getLeft();
    Set<CorrelationId> data = new LinkedHashSet<>();
    data.add(correlate.getCorrelationId());

    while (!stack.isEmpty()) {
      rightEntry = stack.pop();
      left = LogicalJoin.create(left, rightEntry.getLeft(), ImmutableList.of(),
          allConditions.get(0), data, rightEntry.getRight());
      return builder.push(left).build();
    }
    return builder.push(left)
        .filter(builder.and(allConditions))
        .build();
  }

  private void populateStackWithEndIndexesForTables(
      Correlate join,
      Stack<Triple<RelNode, Integer,
      JoinRelType>> stack,
      List<RexNode> joinConditions) {
    RelNode left = ((HepRelVertex) join.getLeft()).getCurrentRel();
    RelNode right = ((HepRelVertex) join.getRight()).getCurrentRel();
    int leftTableColumnSize = join.getLeft().getRowType().getFieldCount();
    int rightTableColumnSize = join.getRight().getRowType().getFieldCount();
    stack.push(
        new ImmutableTriple<>(right, leftTableColumnSize + rightTableColumnSize - 1,
            join.getJoinType()));
    if (left instanceof Correlate) {
      populateStackWithEndIndexesForTables((Correlate) left, stack, joinConditions);
    } else {
      stack.push(new ImmutableTriple<>(left, leftTableColumnSize - 1, join.getJoinType()));
    }
  }

}
