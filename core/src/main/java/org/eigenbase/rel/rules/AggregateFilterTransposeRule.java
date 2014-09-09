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

import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.Aggregation;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.relopt.SubstitutionVisitor;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.util.mapping.Mappings;

import net.hydromatic.optiq.util.BitSets;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Planner rule that matches an {@link org.eigenbase.rel.AggregateRelBase}
 * on a {@link org.eigenbase.rel.FilterRelBase} and transposes them,
 * pushing the aggregate below the filter.
 *
 * <p>In some cases, it is necessary to split the aggregate.
 *
 * <p>This rule does not directly improve performance. The aggregate will
 * have to process more rows, to produce aggregated rows that will be thrown
 * away. The rule might be beneficial if the predicate is very expensive to
 * evaluate. The main use of the rule is to match a query that has a filter
 * under an aggregate to an existing aggregate table.
 */
public class AggregateFilterTransposeRule extends RelOptRule {
  public static final AggregateFilterTransposeRule INSTANCE =
      new AggregateFilterTransposeRule();

  private AggregateFilterTransposeRule() {
    super(
        operand(AggregateRelBase.class,
            operand(FilterRelBase.class, any())));
  }

  public void onMatch(RelOptRuleCall call) {
    final AggregateRelBase aggregate = call.rel(0);
    final FilterRelBase filter = call.rel(1);

    // Do the columns used by the filter appear in the output of the aggregate?
    final BitSet filterColumns =
        RelOptUtil.InputFinder.bits(filter.getCondition());
    final BitSet newGroupSet =
        BitSets.union(aggregate.getGroupSet(), filterColumns);
    final RelNode input = filter.getChild();
    final Boolean unique =
        RelMetadataQuery.areColumnsUnique(input, newGroupSet);
    if (unique != null && unique) {
      // The input is already unique on the grouping columns, so there's little
      // advantage of aggregating again. More important, without this check,
      // the rule fires forever: A-F => A-F-A => A-A-F-A => A-A-A-F-A => ...
      return;
    }
    final AggregateRelBase newAggregate =
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
    final FilterRelBase newFilter = filter.copy(filter.getTraitSet(),
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
      final int offset = newGroupSet.cardinality()
          - aggregate.getGroupSet().cardinality();
      assert offset > 0;
      for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
        final List<Integer> args = Lists.newArrayList();
        for (int arg : aggregateCall.getArgList()) {
          args.add(arg + offset);
        }
        final Aggregation rollup =
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
            new AggregateCall(rollup, aggregateCall.isDistinct(), args,
                aggregateCall.type, aggregateCall.name));
      }
      final AggregateRelBase topAggregate =
          aggregate.copy(aggregate.getTraitSet(), newFilter, topGroupSet,
              topAggCallList);
      call.transformTo(topAggregate);
    }
  }
}

// End AggregateFilterTransposeRule.java
