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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import org.immutables.value.Value;

import java.util.List;

/**
 * Planner rule that matches a {@link LogicalProject} on a {@link LogicalFilter}
 * on a {@link CsvTableScan}, and pushes filter predicates into the scan.
 *
 * @see FileRules#PROJECT_FILTER_SCAN
 */
@Value.Enclosing
public class CsvProjectFilterTableScanRule
    extends RelRule<CsvProjectFilterTableScanRule.Config> {

  /** Creates a CsvProjectFilterTableScanRule. */
  protected CsvProjectFilterTableScanRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    final LogicalFilter filter = call.rel(1);
    final CsvTableScan scan = call.rel(2);

    // Find all input fields referenced by the project expressions
    final java.util.Set<Integer> projectInputFields = new java.util.HashSet<>();
    for (RexNode proj : project.getProjects()) {
      proj.accept(new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
        @Override public Void visitInputRef(RexInputRef inputRef) {
          projectInputFields.add(inputRef.getIndex());
          return null;
        }
      });
    }

    // Find all input fields referenced by the filter condition
    final java.util.Set<Integer> filterInputFields = new java.util.HashSet<>();
    filter.getCondition().accept(new org.apache.calcite.rex.RexVisitorImpl<Void>(true) {
      @Override public Void visitInputRef(RexInputRef inputRef) {
        filterInputFields.add(inputRef.getIndex());
        return null;
      }
    });

    // Union the projected/referenced indices
    final java.util.Set<Integer> neededProjectedIndices = new java.util.TreeSet<>();
    neededProjectedIndices.addAll(projectInputFields);
    neededProjectedIndices.addAll(filterInputFields);

    // Map needed scan projected indices to full-table indices
    final int[] newFields = new int[neededProjectedIndices.size()];
    int k = 0;
    for (int idx : neededProjectedIndices) {
      newFields[k++] = scan.fields[idx];
    }

    // Build index map from old projected index to new index in newFields
    final java.util.Map<Integer, Integer> indexMap = new java.util.HashMap<>();
    int newIdx = 0;
    for (int idx : neededProjectedIndices) {
      indexMap.put(idx, newIdx++);
    }

    // Create shuttle to map RexInputRef indices
    final org.apache.calcite.rex.RexShuttle shuttle = new org.apache.calcite.rex.RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        final Integer mapped = indexMap.get(inputRef.getIndex());
        if (mapped == null) {
          return inputRef;
        }
        return scan.getCluster().getRexBuilder().makeInputRef(inputRef.getType(), mapped);
      }
    };

    final RexNode mappedCondition = filter.getCondition().accept(shuttle);
    final List<RexNode> mappedProjects = new java.util.ArrayList<>();
    for (RexNode proj : project.getProjects()) {
      mappedProjects.add(proj.accept(shuttle));
    }

    final RexNode finalCondition;
    if (scan.condition == null) {
      finalCondition = mappedCondition;
    } else {
      finalCondition =
          RexUtil.composeConjunction(scan.getCluster().getRexBuilder(),
          java.util.Arrays.asList(scan.condition.accept(shuttle), mappedCondition));
    }

    final CsvTableScan newScan =
        new CsvTableScan(scan.getCluster(), scan.getTable(), scan.csvTable,
        newFields, finalCondition);

    final RelNode result =
        project.copy(project.getTraitSet(), newScan, mappedProjects, project.getRowType());

    call.transformTo(result);
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCsvProjectFilterTableScanRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(b1 ->
                b1.operand(LogicalFilter.class).oneInput(b2 ->
                    b2.operand(CsvTableScan.class).noInputs())))
        .build();

    @Override default CsvProjectFilterTableScanRule toRule() {
      return new CsvProjectFilterTableScanRule(this);
    }
  }
}
