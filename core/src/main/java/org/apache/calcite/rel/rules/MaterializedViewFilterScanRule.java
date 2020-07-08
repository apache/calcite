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

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptMaterializations;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.SubstitutionVisitor;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.materialize.MaterializedViewRules;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.base.Suppliers;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * Planner rule that converts
 * a {@link org.apache.calcite.rel.core.Filter}
 * on a {@link org.apache.calcite.rel.core.TableScan}
 * to a {@link org.apache.calcite.rel.core.Filter} on a Materialized View.
 *
 * @see org.apache.calcite.rel.rules.materialize.MaterializedViewRules#FILTER_SCAN
 */
public class MaterializedViewFilterScanRule
    extends RelRule<MaterializedViewFilterScanRule.Config>
    implements TransformationRule {
  /** @deprecated Use {@link MaterializedViewRules#FILTER_SCAN}. */
  @Deprecated // to be removed before 1.25
  public static final MaterializedViewFilterScanRule INSTANCE =
      Config.DEFAULT.toRule();

  private static final Supplier<HepProgram> PROGRAM = Suppliers.memoize(() ->
      new HepProgramBuilder()
          .addRuleInstance(CoreRules.FILTER_PROJECT_TRANSPOSE)
          .addRuleInstance(CoreRules.PROJECT_MERGE)
          .build())::get;

  //~ Constructors -----------------------------------------------------------

  /** Creates a MaterializedViewFilterScanRule. */
  protected MaterializedViewFilterScanRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public MaterializedViewFilterScanRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final TableScan scan = call.rel(1);
    apply(call, filter, scan);
  }

  protected void apply(RelOptRuleCall call, Filter filter, TableScan scan) {
    final RelOptPlanner planner = call.getPlanner();
    final List<RelOptMaterialization> materializations =
        planner.getMaterializations();
    if (!materializations.isEmpty()) {
      RelNode root = filter.copy(filter.getTraitSet(),
          Collections.singletonList(scan));
      List<RelOptMaterialization> applicableMaterializations =
          RelOptMaterializations.getApplicableMaterializations(root, materializations);
      for (RelOptMaterialization materialization : applicableMaterializations) {
        if (RelOptUtil.areRowTypesEqual(scan.getRowType(),
            materialization.queryRel.getRowType(), false)) {
          RelNode target = materialization.queryRel;
          final HepPlanner hepPlanner =
              new HepPlanner(PROGRAM.get(), planner.getContext());
          hepPlanner.setRoot(target);
          target = hepPlanner.findBestExp();
          List<RelNode> subs = new SubstitutionVisitor(target, root)
              .go(materialization.tableRel);
          for (RelNode s : subs) {
            call.transformTo(s);
          }
        }
      }
    }
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(Filter.class, TableScan.class);

    @Override default MaterializedViewFilterScanRule toRule() {
      return new MaterializedViewFilterScanRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Filter> filterClass,
        Class<? extends TableScan> scanClass) {
      return withOperandSupplier(b0 ->
          b0.operand(filterClass).oneInput(b1 ->
              b1.operand(scanClass).noInputs()))
          .as(Config.class);
    }
  }
}
