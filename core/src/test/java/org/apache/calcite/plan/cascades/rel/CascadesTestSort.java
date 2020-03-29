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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 *
 */
public class CascadesTestSort extends Sort implements PhysicalNode {
  CascadesTestSort(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, input, collation);
  }

  @Override public CascadesTestSort copy(RelTraitSet traitSet, RelNode newInput,
      RelCollation newCollation, RexNode offset, RexNode fetch) {
    return new CascadesTestSort(newInput.getCluster(), traitSet,
        newInput, newCollation, offset, fetch);
  }

  public static CascadesTestSort create(RelNode child, RelCollation collation,
      RexNode offset, RexNode fetch) {
    final RelOptCluster cluster = child.getCluster();

    final RelTraitSet traitSet =
        child.getTraitSet()
            .plus(CascadesTestUtils.CASCADES_TEST_CONVENTION)
            .replace(collation);

    collation = traitSet.canonize(collation);
    return new CascadesTestSort(cluster, traitSet, child, collation, offset,
        fetch);
  }

  @Override public PhysicalNode withNewInputs(List<RelNode> newInputs) {
    assert newInputs.size() == 1;
    RelTraitSet traits = newInputs.get(0).getTraitSet().plus(getCollation());
    return copy(traits, newInputs.get(0), getCollation(), offset, fetch);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Make distributed sort cheaper.
    int nodesNum = getTraitSet().contains(RelDistributions.SINGLETON) ? 1 : 10;
    final double rowCount = mq.getRowCount(this) / nodesNum;
    final double bytesPerRow = getRowType().getFieldCount() * 4;
    final double cpu = nodesNum * Util.nLogN(rowCount) * bytesPerRow;
    return planner.getCostFactory().makeCost(nodesNum * rowCount, cpu, 0);
  }
}
