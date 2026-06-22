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
package org.apache.calcite.adapter.file;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import org.immutables.value.Value;

import java.util.List;

/**
 * Planner rule that projects from a {@link CsvTableScan} scan just the columns
 * needed to satisfy a projection. If the projection's expressions are trivial,
 * the projection is removed.
 *
 * @see FileRules#PROJECT_SCAN
 */
@Value.Enclosing
public class CsvProjectTableScanRule
    extends RelRule<CsvProjectTableScanRule.Config> {

  /** Creates a CsvProjectTableScanRule. */
  protected CsvProjectTableScanRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    final CsvTableScan scan = call.rel(1);
    int[] projectFieldIndices = getProjectFields(project.getProjects());
    if (projectFieldIndices == null) {
      // Project contains expressions more complex than just field references.
      return;
    }
    // The project field indices are into the scan's *current* row type (which
    // may already be a subset of the full table due to a prior projection).
    // Map through scan.fields to get the original full-table column indices.
    final int[] newFields = new int[projectFieldIndices.length];
    for (int i = 0; i < projectFieldIndices.length; i++) {
      newFields[i] = scan.fields[projectFieldIndices[i]];
    }
    call.transformTo(
        new CsvTableScan(
            scan.getCluster(),
            scan.getTable(),
            scan.csvTable,
            newFields,
            scan.filterValues));
  }

  private static int[] getProjectFields(List<RexNode> exps) {
    final int[] fields = new int[exps.size()];
    for (int i = 0; i < exps.size(); i++) {
      final RexNode exp = exps.get(i);
      if (exp instanceof RexInputRef) {
        fields[i] = ((RexInputRef) exp).getIndex();
      } else {
        return null; // not a simple projection
      }
    }
    return fields;
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCsvProjectTableScanRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(b1 ->
                b1.operand(CsvTableScan.class).noInputs()))
        .build();

    @Override default CsvProjectTableScanRule toRule() {
      return new CsvProjectTableScanRule(this);
    }
  }
}
