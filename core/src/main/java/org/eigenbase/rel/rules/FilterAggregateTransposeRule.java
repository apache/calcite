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

import java.util.BitSet;
import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;

import net.hydromatic.optiq.util.BitSets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Planner rule that pushes a {@link FilterRelBase}
 * past a {@link AggregateRelBase}.
 *
 * @see org.eigenbase.rel.rules.AggregateFilterTransposeRule
 */
public class FilterAggregateTransposeRule extends RelOptRule {

  /** The default instance of
   * {@link FilterAggregateTransposeRule}.
   *
   * <p>It matches any kind of agg. or filter */
  public static final FilterAggregateTransposeRule INSTANCE =
      new FilterAggregateTransposeRule(FilterRelBase.class,
          RelFactories.DEFAULT_FILTER_FACTORY,
          AggregateRelBase.class);

  private final RelFactories.FilterFactory filterFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushFilterPastAggRule.
   *
   * <p>If {@code filterFactory} is null, creates the same kind of filter as
   * matched in the rule. Similarly {@code aggregateFactory}.</p>
   */
  public FilterAggregateTransposeRule(
      Class<? extends FilterRelBase> filterClass,
      RelFactories.FilterFactory filterFactory,
      Class<? extends AggregateRelBase> aggregateClass) {
    super(
        operand(filterClass,
            operand(aggregateClass, any())));
    this.filterFactory = filterFactory;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final FilterRelBase filterRel = call.rel(0);
    final AggregateRelBase aggRel = call.rel(1);

    final List<RexNode> conditions =
        RelOptUtil.conjunctions(filterRel.getCondition());
    final BitSet groupKeys = aggRel.getGroupSet();
    final RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
    final List<RelDataTypeField> origFields =
        aggRel.getRowType().getFieldList();
    final int[] adjustments = new int[origFields.size()];
    final List<RexNode> pushedConditions = Lists.newArrayList();

    for (RexNode condition : conditions) {
      BitSet rCols = RelOptUtil.InputFinder.bits(condition);
      if (BitSets.contains(groupKeys, rCols)) {
        pushedConditions.add(
            condition.accept(
                new RelOptUtil.RexInputConverter(rexBuilder, origFields,
                    aggRel.getInput(0).getRowType().getFieldList(),
                    adjustments)));
      }
    }

    final RexNode pushedCondition = RexUtil.composeConjunction(rexBuilder,
        pushedConditions, true);

    if (pushedCondition != null) {
      RelNode newFilterRel = filterFactory.createFilter(aggRel.getInput(0),
          pushedCondition);
      RelNode newAggRel = aggRel.copy(aggRel.getTraitSet(),
          ImmutableList.of(newFilterRel));
      call.transformTo(newAggRel);
    }
  }
}

// End FilterAggregateTransposeRule.java
