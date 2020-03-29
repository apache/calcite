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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *
 */
public class CascadesTestHashAggregate extends Aggregate implements PhysicalNode {

  protected CascadesTestHashAggregate(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelNode input, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
  }

  @Override public CascadesTestHashAggregate copy(RelTraitSet traitSet, RelNode input,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return new CascadesTestHashAggregate(getCluster(), traitSet, ImmutableList.of(), input,
        groupSet, groupSets, aggCalls);
  }

  @Override public PhysicalNode withNewInputs(List<RelNode> newInputs) {
    // Hash aggregate destroys child's collation.
    RelTraitSet newTraits = newInputs.get(0).getTraitSet().replace(RelCollations.EMPTY);
    return copy(newTraits, newInputs.get(0), groupSet, groupSets, aggCalls);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(this);
    // Aggregates with more aggregate functions cost a bit more
    float multiplier = 1f + (float) aggCalls.size() * 0.125f;
    for (AggregateCall aggCall : aggCalls) {
      if (aggCall.getAggregation().getName().equals("SUM")) {
        // Pretend that SUM costs a little bit more than $SUM0,
        // to make things deterministic.
        multiplier += 0.0125f;
      }
    }
    return planner.getCostFactory().makeCost(rowCount, 2 * multiplier * rowCount, 0);
  }
}
