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
import org.apache.calcite.plan.cascades.CascadesTestUtils;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

/**
 *
 */
public class CascadesTestExchange extends Exchange implements PhysicalNode {
  public CascadesTestExchange(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input,
      RelDistribution distribution) {
    super(cluster, traitSet, input, distribution);
  }

  @Override public CascadesTestExchange copy(RelTraitSet traitSet, RelNode newInput,
      RelDistribution newDistribution) {
    return new CascadesTestExchange(getCluster(), traitSet, newInput, newDistribution);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(this);
    double bytesPerRow = getRowType().getFieldCount() * 4;
    return planner.getCostFactory().makeCost(
        rowCount, 0, bytesPerRow * rowCount);
  }

  public static CascadesTestExchange create(RelNode child, RelDistribution distribution) {
    final RelOptCluster cluster = child.getCluster();
    final RelTraitSet traitSet =
        child.getTraitSet()
            .plus(CascadesTestUtils.CASCADES_TEST_CONVENTION)
            .plus(distribution);
    return new CascadesTestExchange(cluster, traitSet, child, distribution);
  }

  @Override public PhysicalNode withNewInputs(List<RelNode> newInputs) {
    assert newInputs.size() == 1;
    // Exchange preserves all child traits except distribution.
    RelNode input = newInputs.get(0);
    RelDistribution newDistr = getDistribution();
    RelTraitSet traits = input.getTraitSet().plus(newDistr);
    return copy(traits, input, getDistribution());
  }
}
