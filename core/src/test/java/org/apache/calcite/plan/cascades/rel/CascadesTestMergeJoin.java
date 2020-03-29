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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class CascadesTestMergeJoin extends Join implements PhysicalNode {

  protected CascadesTestMergeJoin(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelNode left, RelNode right,
      RexNode condition, Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
  }

  @Override public CascadesTestMergeJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right,
      JoinRelType joinType, boolean semiJoinDone) {
    return new CascadesTestMergeJoin(getCluster(), traitSet, ImmutableList.of(), left, right,
        conditionExpr, Collections.emptySet(), joinType);
  }

  @Override public PhysicalNode withNewInputs(List<RelNode> newInputs) {
    assert newInputs.size() == 2;
    RelNode leftInput = newInputs.get(0);
    RelNode rightInput = newInputs.get(1);

    List<RelCollation> leftCollations = leftInput
        .getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);

    RelTraitSet newTraits = traitSet;
    newTraits = newTraits.replace(leftCollations.get(0)); // TODO support composite traits

    List<RelDistribution> distributions = new ArrayList<>(2);
    distributions.addAll(leftInput.getTraitSet().getTraits(RelDistributionTraitDef.INSTANCE));
    distributions.addAll(rightInput.getTraitSet().getTraits(RelDistributionTraitDef.INSTANCE));
    distributions.sort(Ordering.natural());

    newTraits = newTraits.replace(distributions.get(0)); // TODO support composite traits

    return copy(newTraits, getCondition(), leftInput, rightInput, getJoinType(), isSemiJoinDone());
  }
}
