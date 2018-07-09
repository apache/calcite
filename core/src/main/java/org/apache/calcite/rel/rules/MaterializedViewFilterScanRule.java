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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.MaterializedViewSubstitutionVisitor;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

/**
 * Planner rule that converts
 * a {@link org.apache.calcite.rel.core.Filter}
 * on a {@link org.apache.calcite.rel.core.TableScan}
 * to a {@link org.apache.calcite.rel.core.Filter} on Materialized View
 */
public class MaterializedViewFilterScanRule extends RelOptRule {
  public static final MaterializedViewFilterScanRule INSTANCE =
      new MaterializedViewFilterScanRule(RelFactories.LOGICAL_BUILDER);

  private final HepProgram program = new HepProgramBuilder()
      .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
      .addRuleInstance(ProjectMergeRule.INSTANCE)
      .build();

  //~ Constructors -----------------------------------------------------------

  /** Creates a MaterializedViewFilterScanRule. */
  public MaterializedViewFilterScanRule(RelBuilderFactory relBuilderFactory) {
    super(operand(Filter.class, operand(TableScan.class, null, none())),
        relBuilderFactory, "MaterializedViewFilterScanRule");
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final TableScan scan = call.rel(1);
    apply(call, filter, scan);
  }

  protected void apply(RelOptRuleCall call, Filter filter, TableScan scan) {
    RelOptPlanner planner = call.getPlanner();
    List<RelOptMaterialization> materializations =
        (planner instanceof VolcanoPlanner)
            ? ((VolcanoPlanner) planner).getMaterializations()
            : ImmutableList.<RelOptMaterialization>of();
    if (!materializations.isEmpty()) {
      RelNode root = filter.copy(filter.getTraitSet(),
          Collections.singletonList((RelNode) scan));
      List<RelOptMaterialization> applicableMaterializations =
          RelOptMaterializations.getApplicableMaterializations(root, materializations);
      for (RelOptMaterialization materialization : applicableMaterializations) {
        if (RelOptUtil.areRowTypesEqual(scan.getRowType(),
            materialization.queryRel.getRowType(), false)) {
          RelNode target = materialization.queryRel;
          final HepPlanner hepPlanner =
              new HepPlanner(program, planner.getContext());
          hepPlanner.setRoot(target);
          target = hepPlanner.findBestExp();
          List<RelNode> subs = new MaterializedViewSubstitutionVisitor(target, root)
              .go(materialization.tableRel);
          for (RelNode s : subs) {
            call.transformTo(s);
          }
        }
      }
    }
  }
}

// End MaterializedViewFilterScanRule.java
