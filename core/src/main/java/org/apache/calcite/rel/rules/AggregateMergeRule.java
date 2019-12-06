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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Planner rule that matches an {@link Aggregate} on a {@link Aggregate}
 * and the top aggregate's group key is a subset of the lower aggregate's
 * group key, and the aggregates are expansions of rollups, then it would
 * convert into a single aggregate.
 *
 * <p>For example, SUM of SUM becomes SUM; SUM of COUNT becomes COUNT;
 * MAX of MAX becomes MAX; MIN of MIN becomes MIN. AVG of AVG would not
 * match, nor would COUNT of COUNT.
 */
public class AggregateMergeRule extends RelOptRule {
  public static final AggregateMergeRule INSTANCE =
      new AggregateMergeRule();

  private AggregateMergeRule() {
    this(
        operand(Aggregate.class,
            operandJ(Aggregate.class, null,
                agg -> Aggregate.isSimple(agg), any())),
        RelFactories.LOGICAL_BUILDER);
  }

  /** Creates an AggregateMergeRule. */
  public AggregateMergeRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, null);
  }

  private boolean isAggregateSupported(AggregateCall aggCall) {
    if (aggCall.isDistinct()
        || aggCall.hasFilter()
        || aggCall.isApproximate()
        || aggCall.getArgList().size() > 1) {
      return false;
    }
    SqlSplittableAggFunction splitter = aggCall.getAggregation()
        .unwrap(SqlSplittableAggFunction.class);
    return splitter != null;
  }

  public void onMatch(RelOptRuleCall call) {
    final Aggregate topAgg = call.rel(0);
    final Aggregate bottomAgg = call.rel(1);
    if (topAgg.getGroupCount() > bottomAgg.getGroupCount()) {
      return;
    }

    final ImmutableBitSet bottomGroupSet = bottomAgg.getGroupSet();
    final Map<Integer, Integer> map = new HashMap<>();
    bottomGroupSet.forEach(v -> map.put(map.size(), v));
    for (int k : topAgg.getGroupSet()) {
      if (!map.containsKey(k)) {
        return;
      }
    }

    // top aggregate keys must be subset of lower aggregate keys
    final ImmutableBitSet topGroupSet = topAgg.getGroupSet().permute(map);
    if (!bottomGroupSet.contains(topGroupSet)) {
      return;
    }

    boolean hasEmptyGroup = topAgg.getGroupSets()
        .stream().anyMatch(n -> n.isEmpty());

    final List<AggregateCall> finalCalls = new ArrayList<>();
    for (AggregateCall topCall : topAgg.getAggCallList()) {
      if (!isAggregateSupported(topCall)
          || topCall.getArgList().size() == 0) {
        return;
      }
      // Make sure top aggregate argument refers to one of the aggregate
      int bottomIndex = topCall.getArgList().get(0) - bottomGroupSet.cardinality();
      if (bottomIndex >= bottomAgg.getAggCallList().size()
          || bottomIndex < 0) {
        return;
      }
      AggregateCall bottomCall = bottomAgg.getAggCallList().get(bottomIndex);
      // Should not merge if top agg with empty group keys and the lower agg
      // function is COUNT, because in case of empty input for lower agg,
      // the result is empty, if we merge them, we end up with 1 result with
      // 0, which is wrong.
      if (!isAggregateSupported(bottomCall)
          || (bottomCall.getAggregation() == SqlStdOperatorTable.COUNT
               && hasEmptyGroup)) {
        return;
      }
      SqlSplittableAggFunction splitter = Objects.requireNonNull(
          bottomCall.getAggregation().unwrap(SqlSplittableAggFunction.class));
      AggregateCall finalCall = splitter.merge(topCall, bottomCall);
      // fail to merge the aggregate call, bail out
      if (finalCall == null) {
        return;
      }
      finalCalls.add(finalCall);
    }

    // re-map grouping sets
    ImmutableList<ImmutableBitSet> newGroupingSets = null;
    if (topAgg.getGroupType() != Group.SIMPLE) {
      newGroupingSets =
          ImmutableBitSet.ORDERING.immutableSortedCopy(
              ImmutableBitSet.permute(topAgg.getGroupSets(), map));
    }

    final Aggregate finalAgg =
        topAgg.copy(topAgg.getTraitSet(), bottomAgg.getInput(), topGroupSet,
            newGroupingSets, finalCalls);
    call.transformTo(finalAgg);
  }
}

// End AggregateMergeRule.java
