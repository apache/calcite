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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import java.util.ArrayList;
import java.util.List;

/** Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalJoin} relational expression
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
class EnumerableJoinRule extends ConverterRule {
  EnumerableJoinRule() {
    super(
        LogicalJoin.class,
        Convention.NONE,
        EnumerableConvention.INSTANCE,
        "EnumerableJoinRule");
  }

  @Override public RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;
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
    final RelOptCluster cluster = join.getCluster();
    final RelNode left = newInputs.get(0);
    final RelNode right = newInputs.get(1);
    final JoinInfo info = join.analyzeCondition();
    if (!info.isEqui() && join.getJoinType() != JoinRelType.INNER) {
      RelNode newRel;
      /**
       *  if it has equiKeys ,we can create an EnumerableHashJoin with
       *  nonEquiConditions, EnumerableHashJoin now supports INNER/ANTI/SEMI
       *  EnumerableHashJoin with equiKeys and nonEquiConditions
       */
      boolean hasEquiKeys = !info.leftKeys.isEmpty()
          && !info.rightKeys.isEmpty();

      if (hasEquiKeys) {
        newRel = EnumerableHashJoin.create(
            left,
            right,
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType());
      } else {
        newRel = EnumerableNestedLoopJoin.create(
            left,
            right,
            join.getCondition(),
            join.getVariablesSet(),
            join.getJoinType());
      }
      return newRel;

    } else {
      // TODO:EnumerableHashJoin + Filter should be supported with just a
      // (new version of) EnumerableHashJoin, which can take the full condition.
      RelNode newRel;
      newRel = EnumerableHashJoin.create(
          left,
          right,
          info.getEquiCondition(left, right, cluster.getRexBuilder()),
          join.getVariablesSet(),
          join.getJoinType());
      if (!info.isEqui()) {
        RexNode nonEqui = RexUtil.composeConjunction(cluster.getRexBuilder(),
            info.nonEquiConditions);
        newRel = new EnumerableFilter(cluster, newRel.getTraitSet(),
            newRel, nonEqui);
      }
      return newRel;
    }
  }
}

// End EnumerableJoinRule.java
