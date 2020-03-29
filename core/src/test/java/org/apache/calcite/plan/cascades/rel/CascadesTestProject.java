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
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *
 */
public class CascadesTestProject extends Project implements PhysicalNode {
  CascadesTestProject(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      List<? extends RexNode> projects,
      RelDataType rowType) {
    super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
    assert getConvention() == CascadesTestUtils.CASCADES_TEST_CONVENTION;
  }

  public CascadesTestProject copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> projects, RelDataType rowType) {
    return new CascadesTestProject(getCluster(), traitSet, input,
        projects, rowType);
  }

  @Override public PhysicalNode withNewInputs(List<RelNode> newInputs) {
    assert newInputs.size() == 1;
    RelNode input = newInputs.get(0);
    RelTraitSet traits = input.getTraitSet();
    Mappings.TargetMapping mapping = getMapping();


    if (mapping == null) {
      // Clear traits if no mapping possible.
      traits = emptyTraits(traits);
    } else {
      RelTraitSet newTraits = traits.apply(mapping);
      traits = newTraits == null ? emptyTraits(traits) : newTraits;
    }

    return copy(traits, input, getProjects(), getRowType());
  }

  public RelTraitSet emptyTraits(RelTraitSet traits) {
    return traits.plus(RelCollationTraitDef.INSTANCE.getDefault())
        .plus(RelDistributionTraitDef.INSTANCE.getDefault());
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double singletonPenalty = getTraitSet().contains(RelDistributions.SINGLETON) ? 1.01 : 1.0;
    double dRows = mq.getRowCount(getInput());
    double dCpu = dRows * exps.size() * singletonPenalty;
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }
}
