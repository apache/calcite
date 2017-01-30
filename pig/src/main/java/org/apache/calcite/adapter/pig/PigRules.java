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
package org.apache.calcite.adapter.pig;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

/**
 *
 */
public class PigRules {

  public static final RelOptRule[] ALL_PIG_OPT_RULES = new RelOptRule[] { PigFilterRule.INSTANCE,
    PigTableScanRule.INSTANCE, PigProjectRule.INSTANCE };

  /**
   * Prevent instantiation.
   */
  private PigRules() {
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalFilter} to a
   * {@link PigFilter}.
   */
  private static class PigFilterRule extends ConverterRule {
    private static final PigFilterRule INSTANCE = new PigFilterRule();

    private PigFilterRule() {
      super(LogicalFilter.class, Convention.NONE, PigRel.CONVENTION, "PigFilterRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalFilter filter = (LogicalFilter) rel;
      final RelTraitSet traitSet = filter.getTraitSet().replace(PigRel.CONVENTION);
      return new PigFilter(rel.getCluster(), traitSet,
          convert(filter.getInput(), PigRel.CONVENTION), filter.getCondition());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalTableScan}
   * to a {@link PigTableScan}.
   */
  private static class PigTableScanRule extends ConverterRule {
    private static final PigTableScanRule INSTANCE = new PigTableScanRule();

    private PigTableScanRule() {
      super(LogicalTableScan.class, Convention.NONE, PigRel.CONVENTION, "PigTableScanRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalTableScan scan = (LogicalTableScan) rel;
      final RelTraitSet traitSet = scan.getTraitSet().replace(PigRel.CONVENTION);
      return new PigTableScan(rel.getCluster(), traitSet, scan.getTable());
    }
  }

  /**
   * Rule to convert a {@link org.apache.calcite.rel.logical.LogicalProject} to
   * a {@link PigProject}.
   */
  private static class PigProjectRule extends ConverterRule {
    private static final PigProjectRule INSTANCE = new PigProjectRule();

    private PigProjectRule() {
      super(LogicalProject.class, Convention.NONE, PigRel.CONVENTION, "PigProjectRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalProject project = (LogicalProject) rel;
      final RelTraitSet traitSet = project.getTraitSet().replace(PigRel.CONVENTION);
      return new PigProject(project.getCluster(), traitSet, project.getInput(),
          project.getProjects(), project.getRowType());
    }
  }
}
// End PigRules.java
