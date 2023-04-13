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
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that derives IS NOT NULL predicates from a inner
 * {@link org.apache.calcite.rel.core.Join} and creates
 * {@link org.apache.calcite.rel.core.Filter}s with those predicates as new inputs of the join.
 *
 * Since the Null value can never match in the inner join and it can lead to skewness due to
 * too many Null values, a not-null filter can be created and pushed down into the input of join.
 *
 * Similar to {@link CoreRules#FILTER_INTO_JOIN}, it would try to create filters and push them into
 * the inputs of the join to filter data as much as possible before join.
 *
 */
@Value.Enclosing
public class JoinDeriveIsNotNullFilterRule
    extends RelRule<JoinDeriveIsNotNullFilterRule.Config> implements TransformationRule {

  public JoinDeriveIsNotNullFilterRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final RelBuilder relBuilder = call.builder();
    final RelMetadataQuery mq = call.getMetadataQuery();

    final ImmutableBitSet.Builder notNullableKeys = ImmutableBitSet.builder();
    RelOptUtil.conjunctions(join.getCondition()).forEach(node -> {
      if (Strong.isStrong(node)) {
        notNullableKeys.addAll(RelOptUtil.InputFinder.bits(node));
      }
    });
    final List<Integer> leftKeys = new ArrayList<>();
    final List<Integer> rightKeys = new ArrayList<>();

    final int offset = join.getLeft().getRowType().getFieldCount();
    notNullableKeys.build().asList().forEach(i -> {
      if (i < offset) {
        leftKeys.add(i);
      } else {
        rightKeys.add(i - offset);
      }
    });

    relBuilder.push(join.getLeft())
        .withPredicates(mq, r ->
            r.filter(leftKeys.stream().map(r::field).map(r::isNotNull)
                .collect(Collectors.toList())));
    final RelNode newLeft = relBuilder.build();

    relBuilder.push(join.getRight())
        .withPredicates(mq, r ->
            r.filter(rightKeys.stream().map(r::field).map(r::isNotNull)
                .collect(Collectors.toList())));
    final RelNode newRight = relBuilder.build();

    if (newLeft != join.getLeft() || newRight != join.getRight()) {
      final RelNode newJoin =
          join.copy(join.getTraitSet(), ImmutableList.of(newLeft, newRight));
      call.transformTo(newJoin);
    }
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable public interface Config extends RelRule.Config {
    ImmutableJoinDeriveIsNotNullFilterRule.Config DEFAULT =
        ImmutableJoinDeriveIsNotNullFilterRule.Config
        .of().withOperandSupplier(
            b -> b.operand(LogicalJoin.class).predicate(
                join -> join.getJoinType() == JoinRelType.INNER
                    && !join.getCondition().isAlwaysTrue())
            .anyInputs());

    @Override default JoinDeriveIsNotNullFilterRule toRule() {
      return new JoinDeriveIsNotNullFilterRule(this);
    }
  }
}
