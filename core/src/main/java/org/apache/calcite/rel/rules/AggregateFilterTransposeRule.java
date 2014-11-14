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
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.BitSet;
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
 */
public class AggregateFilterTransposeRule extends RelOptRule {
  public static final AggregateFilterTransposeRule INSTANCE =
      new AggregateFilterTransposeRule();

  private AggregateFilterTransposeRule() {
    super(
        operand(Aggregate.class,
            operand(Filter.class, any())));
  }

  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Filter filter = call.rel(1);

    // Do the columns used by the filter appear in the output of the aggregate?
    final BitSet filterColumns =
        RelOptUtil.InputFinder.bits(filter.getCondition());
    final BitSet newGroupSet =
        BitSets.union(aggregate.getGroupSet(), filterColumns);
    final RelNode input = filter.getInput();
    final Boolean unique =
        RelMetadataQuery.areColumnsUnique(input, newGroupSet);
    if (unique != null && unique) {
      // The input is already unique on the grouping columns, so there's little
      // advantage of aggregating again. More important, without this check,
      // the rule fires forever: A-F => A-F-A => A-A-F-A => A-A-A-F-A => ...
      return;
    }
    final Aggregate newAggregate =
        aggregate.copy(aggregate.getTraitSet(), input, newGroupSet,
            aggregate.getAggCallList());
    final Mappings.TargetMapping mapping = Mappings.target(
        new Function<Integer, Integer>() {
          public Integer apply(Integer a0) {
            return BitSets.toList(newGroupSet).indexOf(a0);
          }
        },
        input.getRowType().getFieldCount(),
        newGroupSet.cardinality());
    final RexNode newCondition =
        RexUtil.apply(mapping, filter.getCondition());
    final Filter newFilter = filter.copy(filter.getTraitSet(),
        newAggregate, newCondition);
    if (BitSets.contains(aggregate.getGroupSet(), filterColumns)) {
      // Everything needed by the filter is returned by the aggregate.
      assert newGroupSet.equals(aggregate.getGroupSet());
      call.transformTo(newFilter);
    } else {
      // The filter needs at least one extra column.
      // Now aggregate it away.
      final BitSet topGroupSet = new BitSet();
      for (int c : BitSets.toIter(aggregate.getGroupSet())) {
        topGroupSet.set(BitSets.toList(newGroupSet).indexOf(c));
      }
      final List<AggregateCall> topAggCallList = Lists.newArrayList();
      int i = newGroupSet.cardinality();
      for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
        final SqlAggFunction rollup =
            SubstitutionVisitor.getRollup(aggregateCall.getAggregation());
        if (rollup == null) {
          // This aggregate cannot be rolled up.
          return;
        }
        if (aggregateCall.isDistinct()) {
          // Cannot roll up distinct.
          return;
        }
        topAggCallList.add(
            new AggregateCall(rollup, aggregateCall.isDistinct(),
                ImmutableList.of(i++), aggregateCall.type, aggregateCall.name));
      }
      final Aggregate topAggregate =
          aggregate.copy(aggregate.getTraitSet(), newFilter, topGroupSet,
              topAggCallList);
      call.transformTo(topAggregate);
    }
  }
}

// End AggregateFilterTransposeRule.java
