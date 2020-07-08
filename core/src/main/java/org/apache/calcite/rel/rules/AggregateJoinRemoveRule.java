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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Planner rule that matches an {@link org.apache.calcite.rel.core.Aggregate}
 * on a {@link org.apache.calcite.rel.core.Join} and removes the join
 * provided that the join is a left join or right join and it computes no
 * aggregate functions or all the aggregate calls have distinct.
 *
 * <p>For instance,</p>
 *
 * <blockquote>
 * <pre>select distinct s.product_id from
 * sales as s
 * left join product as p
 * on s.product_id = p.product_id</pre></blockquote>
 *
 * <p>becomes
 *
 * <blockquote>
 * <pre>select distinct s.product_id from sales as s</pre></blockquote>
 *
 * @see CoreRules#AGGREGATE_JOIN_REMOVE
 */
public class AggregateJoinRemoveRule
    extends RelRule<AggregateJoinRemoveRule.Config>
    implements TransformationRule {

  /** Creates an AggregateJoinRemoveRule. */
  protected AggregateJoinRemoveRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public AggregateJoinRemoveRule(
      Class<? extends Aggregate> aggregateClass,
      Class<? extends Join> joinClass, RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(aggregateClass, joinClass));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final Join join = call.rel(1);
    boolean isLeftJoin = join.getJoinType() == JoinRelType.LEFT;
    int lower = isLeftJoin
        ? join.getLeft().getRowType().getFieldCount() - 1 : 0;
    int upper = isLeftJoin ? join.getRowType().getFieldCount()
        : join.getLeft().getRowType().getFieldCount();

    // Check whether the aggregate uses columns whose index is between
    // lower(included) and upper(excluded).
    final Set<Integer> allFields = RelOptUtil.getAllFields(aggregate);
    if (allFields.stream().anyMatch(i -> i >= lower && i < upper)) {
      return;
    }

    if (aggregate.getAggCallList().stream().anyMatch(
        aggregateCall -> !aggregateCall.isDistinct())) {
      return;
    }

    RelNode node;
    if (isLeftJoin) {
      node = aggregate.copy(aggregate.getTraitSet(), join.getLeft(),
          aggregate.getGroupSet(), aggregate.getGroupSets(),
          aggregate.getAggCallList());
    } else {
      final Map<Integer, Integer> map = new HashMap<>();
      allFields.forEach(index -> map.put(index, index - upper));
      final ImmutableBitSet groupSet = aggregate.getGroupSet().permute(map);

      final ImmutableList.Builder<AggregateCall> aggCalls =
          ImmutableList.builder();
      final int sourceCount = aggregate.getInput().getRowType().getFieldCount();
      final Mappings.TargetMapping targetMapping =
          Mappings.target(map, sourceCount, sourceCount);
      aggregate.getAggCallList().forEach(aggregateCall ->
          aggCalls.add(aggregateCall.transform(targetMapping)));

      final RelBuilder relBuilder = call.builder();
      node = relBuilder.push(join.getRight())
          .aggregate(relBuilder.groupKey(groupSet), aggCalls.build())
          .build();
    }
    call.transformTo(node);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(LogicalAggregate.class, LogicalJoin.class);

    @Override default AggregateJoinRemoveRule toRule() {
      return new AggregateJoinRemoveRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Aggregate> aggregateClass,
        Class<? extends Join> joinClass) {
      return withOperandSupplier(b0 ->
          b0.operand(aggregateClass).oneInput(b1 ->
              b1.operand(joinClass)
                  .predicate(join ->
                      join.getJoinType() == JoinRelType.LEFT
                          || join.getJoinType() == JoinRelType.RIGHT)
                  .anyInputs()))
          .as(Config.class);
    }
  }
}
