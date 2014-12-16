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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Filter}
 * past a {@link org.apache.calcite.rel.core.Aggregate}.
 *
 * @see org.apache.calcite.rel.rules.AggregateFilterTransposeRule
 */
public class FilterAggregateTransposeRule extends RelOptRule {

  /** The default instance of
   * {@link FilterAggregateTransposeRule}.
   *
   * <p>It matches any kind of agg. or filter */
  public static final FilterAggregateTransposeRule INSTANCE =
      new FilterAggregateTransposeRule(Filter.class,
          RelFactories.DEFAULT_FILTER_FACTORY,
          Aggregate.class);

  private final RelFactories.FilterFactory filterFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushFilterPastAggRule.
   *
   * <p>If {@code filterFactory} is null, creates the same kind of filter as
   * matched in the rule. Similarly {@code aggregateFactory}.</p>
   */
  public FilterAggregateTransposeRule(
      Class<? extends Filter> filterClass,
      RelFactories.FilterFactory filterFactory,
      Class<? extends Aggregate> aggregateClass) {
    super(
        operand(filterClass,
            operand(aggregateClass, any())));
    this.filterFactory = filterFactory;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final Filter filterRel = call.rel(0);
    final Aggregate aggRel = call.rel(1);

    final List<RexNode> conditions =
        RelOptUtil.conjunctions(filterRel.getCondition());
    final ImmutableBitSet groupKeys = aggRel.getGroupSet();
    final RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
    final List<RelDataTypeField> origFields =
        aggRel.getRowType().getFieldList();
    final int[] adjustments = new int[origFields.size()];
    final List<RexNode> pushedConditions = Lists.newArrayList();
    final List<RexNode> remainingConditions = Lists.newArrayList();

    for (RexNode condition : conditions) {
      ImmutableBitSet rCols = RelOptUtil.InputFinder.bits(condition);
      boolean push = groupKeys.contains(rCols);
      if (push && aggRel.indicator) {
        // If grouping sets are used, the filter can be pushed if
        // the columns referenced in the predicate are present in
        // all the grouping sets.
        for (ImmutableBitSet groupingSet: aggRel.getGroupSets()) {
          if (!groupingSet.contains(rCols)) {
            push = false;
            break;
          }
        }
      }
      if (push) {
        pushedConditions.add(
            condition.accept(
                new RelOptUtil.RexInputConverter(rexBuilder, origFields,
                    aggRel.getInput(0).getRowType().getFieldList(),
                    adjustments)));
      } else {
        remainingConditions.add(condition);
      }
    }

    RelNode rel = RelOptUtil.createFilter(aggRel.getInput(0), pushedConditions,
        filterFactory);
    if (rel == aggRel.getInput(0)) {
      return;
    }
    rel = aggRel.copy(aggRel.getTraitSet(), ImmutableList.of(rel));
    rel = RelOptUtil.createFilter(rel, remainingConditions, filterFactory);
    call.transformTo(rel);
  }
}

// End FilterAggregateTransposeRule.java
