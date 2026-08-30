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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static java.util.Objects.requireNonNull;

/** Planner rule that converts an inner {@link LogicalJoin} whose condition
 * consists of at least two cross-input field inequalities to an
 * {@link EnumerableIEJoin}.
 *
 * <p>Based on Khayyat et al.,
 * <a href="https://doi.org/10.14778/2831360.2831362">"Lightning Fast and Space
 * Efficient Inequality Joins," PVLDB 8(13), 2015</a>. The first two
 * inequalities drive the join and additional inequalities are evaluated by an
 * {@link EnumerableCalc}.
 *
 * @see EnumerableRules#ENUMERABLE_IE_JOIN_RULE
 */
class EnumerableIEJoinRule extends ConverterRule {
  /** Default configuration. */
  static final Config DEFAULT_CONFIG = Config.INSTANCE
      .withConversion(LogicalJoin.class, Convention.NONE,
          EnumerableConvention.INSTANCE, "EnumerableIEJoinRule")
      .withRuleFactory(EnumerableIEJoinRule::new);

  /** Called from the Config. */
  protected EnumerableIEJoinRule(Config config) {
    super(config);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    final Join join = (Join) rel;
    if (join.getJoinType() != JoinRelType.INNER
        || !join.getVariablesSet().isEmpty()
        || !join.getSystemFieldList().isEmpty()) {
      return null;
    }

    final int leftFieldCount = join.getLeft().getRowType().getFieldCount();
    final List<RexNode> conjunctions =
        RelOptUtil.conjunctions(join.getCondition());
    if (conjunctions.size() < 2) {
      return null;
    }
    for (int i = 0; i < conjunctions.size(); i++) {
      final EnumerableIEJoin.Condition condition =
          EnumerableIEJoin.analyzeConjunction(conjunctions.get(i), leftFieldCount);
      if (condition == null) {
        return null;
      }
      if (i < 2
          && !EnumerableIEJoin.supportsKeyTypes(
              join.getLeft(), join.getRight(), condition)) {
        return null;
      }
    }

    final RelNode left = convert(join.getLeft(), join.getLeft().getTraitSet()
        .replace(EnumerableConvention.INSTANCE));
    final RelNode right = convert(join.getRight(), join.getRight().getTraitSet()
        .replace(EnumerableConvention.INSTANCE));
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RexNode ieCondition =
        requireNonNull(RexUtil.composeConjunction(rexBuilder, conjunctions.subList(0, 2)));
    final EnumerableIEJoin ieJoin =
        EnumerableIEJoin.create(left, right, ieCondition);
    if (conjunctions.size() == 2) {
      return ieJoin;
    }

    final RexNode residual =
        requireNonNull(
            RexUtil.composeConjunction(rexBuilder,
            conjunctions.subList(2, conjunctions.size())));
    final RexProgram program =
        RexProgram.create(ieJoin.getRowType(),
            rexBuilder.identityProjects(ieJoin.getRowType()), residual,
            ieJoin.getRowType(), rexBuilder);
    return EnumerableCalc.create(ieJoin, program);
  }
}
