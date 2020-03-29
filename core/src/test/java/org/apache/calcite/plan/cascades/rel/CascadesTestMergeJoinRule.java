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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.CascadesRuleCall;
import org.apache.calcite.plan.cascades.ImplementationRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.plan.cascades.CascadesTestUtils.CASCADES_TEST_CONVENTION;
import static org.apache.calcite.plan.cascades.CascadesTestUtils.deriveInputsDistribution;

/**
 *
 */
public class CascadesTestMergeJoinRule extends ImplementationRule<LogicalJoin> {
  public static final CascadesTestMergeJoinRule CASCADES_MERGE_JOIN_RULE =
      new CascadesTestMergeJoinRule();

  CascadesTestMergeJoinRule() {
    super(LogicalJoin.class,
        join -> true,
        Convention.NONE,
        CASCADES_TEST_CONVENTION,
        RelFactories.LOGICAL_BUILDER,
        "CascadesMergeJoinRule");
  }

  @Override public void implement(LogicalJoin join, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    final JoinInfo info = join.analyzeCondition();
    if (join.getJoinType() != JoinRelType.INNER) {
      // EnumerableMergeJoin only supports inner join.
      return;
    }
    if (info.pairs().size() == 0) {
      // MergeJoin CAN support cartesian join, but disable it for now.
      return;
    }
    // TODO reconcile parent traits request and children distribution.
    Pair<RelDistribution, RelDistribution> childrenDistribution =
        deriveInputsDistribution(join);

    final List<RelNode> newInputs = new ArrayList<>();
    final List<RelCollation> collations = new ArrayList<>();
    int offset = 0;
    for (Ord<RelNode> ord : Ord.zip(join.getInputs())) {
      RelTraitSet traits = requestedTraits;
      if (!info.pairs().isEmpty()) {
        final List<RelFieldCollation> fieldCollations = new ArrayList<>();
        for (int key : info.keys().get(ord.i)) {
          fieldCollations.add(
              new RelFieldCollation(key, RelFieldCollation.Direction.ASCENDING,
                  RelFieldCollation.NullDirection.LAST));
        }
        final RelCollation collation = RelCollations.of(fieldCollations);
        collations.add(RelCollations.shift(collation, offset));
        traits = traits.replace(collation);
      }
      RelDistribution desiredDistr = ord.i == 0
          ? childrenDistribution.left
          : childrenDistribution.right;
      newInputs.add(convert(ord.e, traits.plus(desiredDistr)));
      offset += ord.e.getRowType().getFieldCount();
    }
    final RelNode left = newInputs.get(0);
    final RelNode right = newInputs.get(1);
    final RelOptCluster cluster = join.getCluster();
    RelNode newRel;

    RelTraitSet traitSet = join.getTraitSet()
        .replace(CASCADES_TEST_CONVENTION);
    if (!collations.isEmpty()) {
      traitSet = traitSet.replace(collations);
    }
    // Re-arrange condition: first the equi-join elements, then the non-equi-join ones (if any);
    // this is not strictly necessary but it will be useful to avoid spurious errors in the
    // unit tests when verifying the plan.
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RexNode equi = info.getEquiCondition(left, right, rexBuilder);
    final RexNode condition;
    if (info.isEqui()) {
      condition = equi;
    } else {
      final RexNode nonEqui = RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions);
      condition = RexUtil.composeConjunction(rexBuilder, Arrays.asList(equi, nonEqui));
    }
    newRel = new CascadesTestMergeJoin(cluster,
        traitSet,
        ImmutableList.of(),
        left,
        right,
        condition,
        join.getVariablesSet(),
        join.getJoinType());

    call.transformTo(newRel);
  }
}
