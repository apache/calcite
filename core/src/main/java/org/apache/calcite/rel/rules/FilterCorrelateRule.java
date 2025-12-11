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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

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

    List<RexNode> aboveFilters =
        RelOptUtil.conjunctions(filter.getCondition());

    final List<RexNode> leftFilters = new ArrayList<>();
    final List<RexNode> rightFilters = new ArrayList<>();

    // Do not consider moving predicates that contain correlation variables
    final List<RexNode> ineligible = new ArrayList<>();
    final List<RexNode> eligible = new ArrayList<>();
    for (RexNode f : aboveFilters) {
      if (RexUtil.containsCorrelation(f)) {
        ineligible.add(f);
      } else {
        eligible.add(f);
      }
    }
    aboveFilters = eligible;

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

    // Add back the ineligible filters
    aboveFilters.addAll(ineligible);

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
}
