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
package org.apache.calcite.plan.cascades.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.CascadesRuleCall;
import org.apache.calcite.plan.cascades.ImplementationRule;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;

import static org.apache.calcite.plan.cascades.CascadesTestUtils.CASCADES_TEST_CONVENTION;
import static org.apache.calcite.plan.cascades.CascadesTestUtils.deriveInputsDistribution;

/**
 *
 */
public class CascadesTestHashJoinRule extends ImplementationRule<LogicalJoin> {
  public static final CascadesTestHashJoinRule CASCADES_HASH_JOIN_RULE =
      new CascadesTestHashJoinRule();
  public CascadesTestHashJoinRule() {
    super(LogicalJoin.class,
        join -> true,
        Convention.NONE,
        CASCADES_TEST_CONVENTION,
        RelFactories.LOGICAL_BUILDER,
        "CascadesHashJoinRule");
  }

  @Override public void implement(LogicalJoin join, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    // Let's assume that hash join kills collation.
    requestedTraits = requestedTraits.replace(RelCollationTraitDef.INSTANCE.getDefault());

    Pair<RelDistribution, RelDistribution> childrenDistribution =
        deriveInputsDistribution(join);

    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RelNode left = convert(join.getInputs().get(0),
        requestedTraits.plus(childrenDistribution.left));
    final RelNode right = convert(join.getInputs().get(1),
        requestedTraits.plus(childrenDistribution.right));
    final JoinInfo info = join.analyzeCondition();

    // If the join has equiKeys (i.e. complete or partial equi-join),
    // create an EnumerableHashJoin, which supports all types of joins,
    // even if the join condition contains partial non-equi sub-conditions;
    // otherwise (complete non-equi-join), create an EnumerableNestedLoopJoin,
    // since a hash join strategy in this case would not be beneficial.
    final boolean hasEquiKeys = !info.leftKeys.isEmpty()
        && !info.rightKeys.isEmpty();
    if (!hasEquiKeys) {
      return;
    }

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
    CascadesTestHashJoin hashJoin = new CascadesTestHashJoin(join.getCluster(),
        requestedTraits,
        ImmutableList.of(),
        left,
        right,
        condition,
        join.getVariablesSet(),
        join.getJoinType());

    call.transformTo(hashJoin);
  }
}
