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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Bug;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Planner rule that converts a
 * {@link LogicalJoin} relational expression
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 * You may provide a custom config to convert other nodes that extend {@link Join}.
 *
 * @see EnumerableRules#ENUMERABLE_JOIN_RULE */
class EnumerableJoinRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalJoin.class, Convention.NONE,
          EnumerableConvention.INSTANCE, "EnumerableJoinRule")
      .withRuleFactory(EnumerableJoinRule::new);

  /** Called from the Config. */
  protected EnumerableJoinRule(Config config) {
    super(config);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    Join join = (Join) rel;
    if (!Bug.TODO_FIXED && join.getJoinType() == JoinRelType.LEFT_MARK) {
      // TODO implement LEFT MARK join
      return null;
    }
    List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input : join.getInputs()) {
      if (!(input.getConvention() instanceof EnumerableConvention)) {
        input =
            convert(
                input,
                input.getTraitSet()
                    .replace(EnumerableConvention.INSTANCE));
      }
      newInputs.add(input);
    }
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RelNode left = newInputs.get(0);
    final RelNode right = newInputs.get(1);
    final JoinInfo info = join.analyzeCondition();

    // If the join has equiKeys (i.e. complete or partial equi-join),
    // create an EnumerableHashJoin, which supports all types of joins,
    // even if the join condition contains partial non-equi sub-conditions;
    // otherwise (complete non-equi-join), create an EnumerableNestedLoopJoin,
    // since a hash join strategy in this case would not be beneficial.
    final boolean hasEquiKeys = !info.leftKeys.isEmpty()
        && !info.rightKeys.isEmpty();
    if (hasEquiKeys) {
      // Re-arrange condition: first the equi-join elements, then the non-equi-join ones (if any);
      // this is not strictly necessary but it will be useful to avoid spurious errors in the
      // unit tests when verifying the plan.
      final RexNode equi = info.getEquiCondition(left, right, rexBuilder);
      final RexNode condition;
      if (info.isEqui()) {
        condition = equi;
      } else {
        final RexNode nonEqui = RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions);
        condition = RexUtil.composeConjunction(rexBuilder, Arrays.asList(equi, nonEqui));
      }
      return EnumerableHashJoin.create(
          left,
          right,
          condition,
          join.getVariablesSet(),
          join.getJoinType());
    }
    return EnumerableNestedLoopJoin.create(
        left,
        right,
        join.getCondition(),
        join.getVariablesSet(),
        join.getJoinType());
  }
}
