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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.CascadesTestUtils;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *
 */
public class CascadesTestTableScan extends TableScan implements PhysicalNode {
  public CascadesTestTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table) {
    super(cluster, traitSet, ImmutableList.of(), table);
    assert getConvention() == CascadesTestUtils.CASCADES_TEST_CONVENTION;
  }

  public static CascadesTestTableScan create(RelOptCluster cluster,
      RelOptTable relOptTable) {
    final Table table = relOptTable.unwrap(Table.class);
    final RelTraitSet traitSet =
        cluster.traitSetOf(CascadesTestUtils.CASCADES_TEST_CONVENTION)
            .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
              if (table != null) {
                return table.getStatistic().getCollations();
              }
              return ImmutableList.of();
            })
            .replaceIf(RelDistributionTraitDef.INSTANCE, () -> {
              if (table != null) {
                return table.getStatistic().getDistribution();
              }
              return RelDistributions.ANY;
            });
    return new CascadesTestTableScan(cluster, traitSet, relOptTable);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("sort", traitSet.getTrait(RelCollationTraitDef.INSTANCE));
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double dRows = table.getRowCount();
    double dCpu = dRows + 1; // ensure non-zero cost
    double dIo = 0;
    // Make sorted (indexed) tables slightly more expensive.
    if (!traitSet.contains(RelCollations.EMPTY)) {
      dCpu *= 1.1;
    }
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  @Override public PhysicalNode withNewInputs(List<RelNode> newInputs) {
    return this;
  }
}
