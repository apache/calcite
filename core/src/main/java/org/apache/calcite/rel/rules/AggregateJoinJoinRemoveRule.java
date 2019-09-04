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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Planner rule that matches an {@link org.apache.calcite.rel.core.Aggregate}
 * on a {@link org.apache.calcite.rel.core.Join} and removes the left input
 * of the join provided that the left input is also a left join if possible.
 *
 * <p>For instance,</p>
 *
 * <blockquote>
 * <pre>select distinct s.product_id, pc.product_id from
 * sales as s
 * left join product as p
 * on s.product_id = p.product_id
 * left join product_class pc
 * on s.product_id = pc.product_id</pre></blockquote>
 *
 * <p>becomes
 *
 * <blockquote>
 * <pre>select distinct s.product_id, pc.product_id from
 * sales as s
 * left join product_class pc
 * on s.product_id = pc.product_id</pre></blockquote>
 *
 */
public class AggregateJoinJoinRemoveRule extends RelOptRule {
  public static final AggregateJoinJoinRemoveRule INSTANCE =
      new AggregateJoinJoinRemoveRule(LogicalAggregate.class,
          LogicalJoin.class, RelFactories.LOGICAL_BUILDER);

  /** Creates an AggregateJoinJoinRemoveRule. */
  public AggregateJoinJoinRemoveRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
    super(
        operand(aggregateClass,
            operandJ(joinClass, null,
                join -> join.getJoinType() == JoinRelType.LEFT,
                operandJ(joinClass, null,
                    join -> join.getJoinType() == JoinRelType.LEFT, any()))),
        relBuilderFactory, null);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Join topJoin = call.rel(1);
    final Join bottomJoin = call.rel(2);
    int leftBottomChildSize = bottomJoin.getLeft().getRowType()
        .getFieldCount();

    // Check whether the aggregate uses columns in the right input of
    // bottom join.
    final Set<Integer> allFields = RelOptUtil.getAllFields(aggregate);
    if (allFields.stream().anyMatch(i -> i >= leftBottomChildSize
        && i < bottomJoin.getRowType().getFieldCount())) {
      return;
    }

    if (aggregate.getAggCallList().stream().anyMatch(aggregateCall ->
        !aggregateCall.isDistinct())) {
      return;
    }

    // Check whether the top join uses columns in the right input of bottom join.
    final List<Integer> leftKeys = new ArrayList<>();
    RelOptUtil.splitJoinCondition(topJoin.getLeft(), topJoin.getRight(),
        topJoin.getCondition(), leftKeys, new ArrayList<>(),
        new ArrayList<>());
    if (leftKeys.stream().anyMatch(s -> s >= leftBottomChildSize)) {
      return;
    }

    // Check whether left join keys in top join and bottom join are equal.
    final List<Integer> leftChildKeys = new ArrayList<>();
    RelOptUtil.splitJoinCondition(bottomJoin.getLeft(), bottomJoin.getRight(),
        bottomJoin.getCondition(), leftChildKeys, new ArrayList<>(),
        new ArrayList<>());
    if (!leftKeys.equals(leftChildKeys)) {
      return;
    }

    int offset = bottomJoin.getRight().getRowType().getFieldCount();
    final RelBuilder relBuilder = call.builder();
    RexNode condition = RexUtil.shift(topJoin.getCondition(),
        leftBottomChildSize, -offset);
    RelNode join = relBuilder.push(bottomJoin.getLeft())
        .push(topJoin.getRight())
        .join(topJoin.getJoinType(), condition)
        .build();

    final Map<Integer, Integer> map = new HashMap<>();
    allFields.forEach(
        index ->
            map.put(index,
                index < leftBottomChildSize ? index : index - offset));
    final ImmutableBitSet groupSet = aggregate.getGroupSet().permute(map);

    final ImmutableList.Builder<AggregateCall> aggCalls =
        ImmutableList.builder();
    final int sourceCount = aggregate.getInput().getRowType().getFieldCount();
    final Mappings.TargetMapping targetMapping =
        Mappings.target(map, sourceCount, sourceCount);
    aggregate.getAggCallList().forEach(
        aggregateCall ->
            aggCalls.add(aggregateCall.transform(targetMapping)));

    RelNode newAggregate = relBuilder.push(join)
        .aggregate(relBuilder.groupKey(groupSet), aggCalls.build())
        .build();

    call.transformTo(newAggregate);
  }
}

// End AggregateJoinJoinRemoveRule.java
