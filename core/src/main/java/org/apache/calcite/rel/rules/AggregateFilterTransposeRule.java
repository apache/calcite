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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that matches an {@link org.apache.calcite.rel.core.Aggregate}
 * on a {@link org.apache.calcite.rel.core.Filter} and transposes them,
 * pushing the aggregate below the filter.
 *
 * <p>In some cases, it is necessary to split the aggregate.
 *
 * <p>This rule does not directly improve performance. The aggregate will
 * have to process more rows, to produce aggregated rows that will be thrown
 * away. The rule might be beneficial if the predicate is very expensive to
 * evaluate. The main use of the rule is to match a query that has a filter
 * under an aggregate to an existing aggregate table.
 *
 * @see org.apache.calcite.rel.rules.FilterAggregateTransposeRule
 * @see CoreRules#AGGREGATE_FILTER_TRANSPOSE
 */
public class AggregateFilterTransposeRule
    extends RelRule<AggregateFilterTransposeRule.Config>
    implements TransformationRule {

  /** Creates an AggregateFilterTransposeRule. */
  protected AggregateFilterTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public AggregateFilterTransposeRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier(b -> b.exactly(operand))
        .as(Config.class));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Filter filter = call.rel(1);

    // Do the columns used by the filter appear in the output of the aggregate?
    final ImmutableBitSet filterColumns =
        RelOptUtil.InputFinder.bits(filter.getCondition());
    final ImmutableBitSet newGroupSet =
        aggregate.getGroupSet().union(filterColumns);
    final RelNode input = filter.getInput();
    final RelMetadataQuery mq = call.getMetadataQuery();
    final Boolean unique = mq.areColumnsUnique(input, newGroupSet);
    if (unique != null && unique) {
      // The input is already unique on the grouping columns, so there's little
      // advantage of aggregating again. More important, without this check,
      // the rule fires forever: A-F => A-F-A => A-A-F-A => A-A-A-F-A => ...
      return;
    }
    final boolean allColumnsInAggregate =
        aggregate.getGroupSet().contains(filterColumns);
    final Aggregate newAggregate =
        aggregate.copy(aggregate.getTraitSet(), input,
            newGroupSet, null, aggregate.getAggCallList());
    final Mappings.TargetMapping mapping = Mappings.target(
        newGroupSet::indexOf,
        input.getRowType().getFieldCount(),
        newGroupSet.cardinality());
    final RexNode newCondition =
        RexUtil.apply(mapping, filter.getCondition());
    final Filter newFilter = filter.copy(filter.getTraitSet(),
        newAggregate, newCondition);
    if (allColumnsInAggregate && aggregate.getGroupType() == Group.SIMPLE) {
      // Everything needed by the filter is returned by the aggregate.
      assert newGroupSet.equals(aggregate.getGroupSet());
      call.transformTo(newFilter);
    } else {
      // If aggregate uses grouping sets, we always need to split it.
      // Otherwise, it means that grouping sets are not used, but the
      // filter needs at least one extra column, and now aggregate it away.
      final ImmutableBitSet.Builder topGroupSet = ImmutableBitSet.builder();
      for (int c : aggregate.getGroupSet()) {
        topGroupSet.set(newGroupSet.indexOf(c));
      }
      ImmutableList<ImmutableBitSet> newGroupingSets = null;
      if (aggregate.getGroupType() != Group.SIMPLE) {
        ImmutableList.Builder<ImmutableBitSet> newGroupingSetsBuilder =
                ImmutableList.builder();
        for (ImmutableBitSet groupingSet : aggregate.getGroupSets()) {
          final ImmutableBitSet.Builder newGroupingSet =
                  ImmutableBitSet.builder();
          for (int c : groupingSet) {
            newGroupingSet.set(newGroupSet.indexOf(c));
          }
          newGroupingSetsBuilder.add(newGroupingSet.build());
        }
        newGroupingSets = newGroupingSetsBuilder.build();
      }
      final List<AggregateCall> topAggCallList = new ArrayList<>();
      int i = newGroupSet.cardinality();
      for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
        final SqlAggFunction rollup = aggregateCall.getAggregation().getRollup();
        if (rollup == null) {
          // This aggregate cannot be rolled up.
          return;
        }
        if (aggregateCall.isDistinct()) {
          // Cannot roll up distinct.
          return;
        }
        topAggCallList.add(
            AggregateCall.create(rollup, aggregateCall.isDistinct(),
                aggregateCall.isApproximate(), aggregateCall.ignoreNulls(),
                ImmutableList.of(i++), -1,
                aggregateCall.distinctKeys, aggregateCall.collation,
                aggregateCall.type, aggregateCall.name));
      }
      final Aggregate topAggregate =
          aggregate.copy(aggregate.getTraitSet(), newFilter,
              topGroupSet.build(), newGroupingSets, topAggCallList);
      call.transformTo(topAggregate);
    }
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(Aggregate.class, Filter.class);

    @Override default AggregateFilterTransposeRule toRule() {
      return new AggregateFilterTransposeRule(this);
    }

    /** Defines an operand tree for the given 2 classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<? extends Filter> filterClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass).oneInput(b1 ->
              b1.operand(filterClass).anyInputs()))
          .as(Config.class);
    }

    /** Defines an operand tree for the given 3 classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<? extends Filter> filterClass,
        Class<? extends RelNode> relClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass).oneInput(b1 ->
              b1.operand(filterClass).oneInput(b2 ->
                  b2.operand(relClass).anyInputs())))
          .as(Config.class);
    }
  }
}
