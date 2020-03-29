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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.CascadesRuleCall;
import org.apache.calcite.plan.cascades.CascadesTestUtils;
import org.apache.calcite.plan.cascades.ImplementationRule;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.util.mapping.Mappings;

/**
 *
 */
public class CascadesTestProjectRule extends ImplementationRule<LogicalProject> {
  public static final CascadesTestProjectRule CASCADES_PROJECT_RULE =
      new CascadesTestProjectRule();

  public CascadesTestProjectRule() {
    super(LogicalProject.class,
        RelOptUtil::notContainsWindowedAgg,
        Convention.NONE, CascadesTestUtils.CASCADES_TEST_CONVENTION,
        RelFactories.LOGICAL_BUILDER, "CascadesProjectRule");
  }

  @Override public void implement(LogicalProject rel, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    RelNode input = rel.getInput();
    requestedTraits = convertTraits(rel, requestedTraits);
    input = convert(input, requestedTraits);
    CascadesTestProject project = new CascadesTestProject(rel.getCluster(), requestedTraits, input,
        rel.getProjects(), rel.getRowType());
    call.transformTo(project);
  }

  public static RelTraitSet convertTraits(Project project, RelTraitSet traits) {
    Mappings.TargetMapping mapping = project.getMapping();

    // TODO fix mappings for traits.
    if (mapping == null) {
      // Clear traits if no mapping possible.
      traits = traits.plus(RelCollationTraitDef.INSTANCE.getDefault())
          .plus(RelDistributionTraitDef.INSTANCE.getDefault());
    } else {
      traits = traits.apply(mapping.inverse())
          .plus(RelDistributionTraitDef.INSTANCE.getDefault());
    }

    return traits;
  }
}
