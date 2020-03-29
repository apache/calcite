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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 *
 */
public class CascadesTestHashJoin extends Join implements PhysicalNode {

  protected CascadesTestHashJoin(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelNode left, RelNode right,
      RexNode condition, Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
  }

  @Override public CascadesTestHashJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    return new CascadesTestHashJoin(getCluster(), traitSet, ImmutableList.of(), left, right,
        conditionExpr, Collections.emptySet(), joinType);
  }

  @Override public PhysicalNode withNewInputs(List<RelNode> newInputs) {
    assert newInputs.size() == 2;
    // Let's assume that hash join kills collation.
    RelTraitSet newTraits = newInputs.get(0).getTraitSet().replace(RelCollations.EMPTY);
    return copy(newTraits, getCondition(), newInputs.get(0), newInputs.get(1), getJoinType(),
        isSemiJoinDone());
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(this);
    double leftRowCount = mq.getRowCount(getLeft());
    double rightRowCount = mq.getRowCount(getRight());
    return planner.getCostFactory().makeCost(rowCount, leftRowCount + rightRowCount, 0);
  }
}
