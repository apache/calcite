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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Planner rule that pushes an
 * {@link org.apache.calcite.rel.core.Aggregate}
 * past a {@link org.apache.calcite.rel.core.Join}.
 */
public class AggregateJoinTransposeRule extends RelOptRule {
  public static final AggregateJoinTransposeRule INSTANCE =
      new AggregateJoinTransposeRule(LogicalAggregate.class,
          RelFactories.DEFAULT_AGGREGATE_FACTORY,
          LogicalJoin.class,
          RelFactories.DEFAULT_JOIN_FACTORY);

  private final RelFactories.AggregateFactory aggregateFactory;

  private final RelFactories.JoinFactory joinFactory;

  /** Creates an AggregateJoinTransposeRule. */
  public AggregateJoinTransposeRule(Class<? extends Aggregate> aggregateClass,
      RelFactories.AggregateFactory aggregateFactory,
      Class<? extends Join> joinClass,
      RelFactories.JoinFactory joinFactory) {
    super(
        operand(aggregateClass, null, Aggregate.IS_SIMPLE,
            operand(joinClass, any())));
    this.aggregateFactory = aggregateFactory;
    this.joinFactory = joinFactory;
  }

  public void onMatch(RelOptRuleCall call) {
    Aggregate aggregate = call.rel(0);
    Join join = call.rel(1);

    // If aggregate functions are present, we bail out
    if (!aggregate.getAggCallList().isEmpty()) {
      return;
    }

    // If it is not an inner join, we do not push the
    // aggregate operator
    if (join.getJoinType() != JoinRelType.INNER) {
      return;
    }

    // Do the columns used by the join appear in the output of the aggregate?
    final ImmutableBitSet aggregateColumns = aggregate.getGroupSet();
    final ImmutableBitSet joinColumns =
        RelOptUtil.InputFinder.bits(join.getCondition());
    boolean allColumnsInAggregate = aggregateColumns.contains(joinColumns);
    if (!allColumnsInAggregate) {
      return;
    }

    // Split join condition
    final List<Integer> leftKeys = Lists.newArrayList();
    final List<Integer> rightKeys = Lists.newArrayList();
    RexNode nonEquiConj =
        RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(),
            join.getCondition(), leftKeys, rightKeys);
    // If it contains non-equi join conditions, we bail out
    if (!nonEquiConj.isAlwaysTrue()) {
      return;
    }

    // Create new aggregate operators below join
    final ImmutableBitSet leftKeysBitSet = ImmutableBitSet.of(leftKeys);
    RelNode newLeftInput = aggregateFactory.createAggregate(join.getLeft(),
        false, leftKeysBitSet, null, aggregate.getAggCallList());
    final ImmutableBitSet rightKeysBitSet = ImmutableBitSet.of(rightKeys);
    RelNode newRightInput = aggregateFactory.createAggregate(join.getRight(),
        false, rightKeysBitSet, null, aggregate.getAggCallList());

    // Update condition
    final Mappings.TargetMapping mapping = Mappings.target(
        new Function<Integer, Integer>() {
          public Integer apply(Integer a0) {
            return aggregateColumns.indexOf(a0);
          }
        },
        join.getRowType().getFieldCount(),
        aggregateColumns.cardinality());
    final RexNode newCondition =
        RexUtil.apply(mapping, join.getCondition());

    // Create new join
    RelNode newJoin = joinFactory.createJoin(newLeftInput, newRightInput,
        newCondition, join.getJoinType(),
        join.getVariablesStopped(), join.isSemiJoinDone());

    call.transformTo(newJoin);
  }
}

// End AggregateJoinTransposeRule.java
