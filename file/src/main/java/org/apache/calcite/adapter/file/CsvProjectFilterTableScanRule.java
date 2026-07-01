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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that matches a {@link LogicalProject} on a {@link LogicalFilter}
 * on a {@link CsvTableScan}, and pushes simple equality filter predicates
 * into the scan.
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

    final int fullFieldCount = scan.getTable().getRowType().getFieldCount();
    final @Nullable String[] filterValues = new String[fullFieldCount];

    // Partition the filter condition into predicates we can push down
    // (simple column = literal equality) and predicates we cannot.
    final List<RexNode> residualFilters = new ArrayList<>();
    decomposeFilter(filter.getCondition(), scan.fields, filterValues, residualFilters);

    // Only transform if at least one predicate could be pushed into the scan.
    boolean anyPushed = false;
    for (String v : filterValues) {
      if (v != null) {
        anyPushed = true;
        break;
      }
    }
    if (!anyPushed) {
      return;
    }

    // Build a new scan that carries the pushed-down filter values.
    final CsvTableScan newScan =
        new CsvTableScan(scan.getCluster(), scan.getTable(), scan.csvTable,
        scan.fields, filterValues);

    // If there are residual predicates that could not be pushed down,
    // keep a LogicalFilter node above the new scan to evaluate them.
    RelNode rel = newScan;
    if (!residualFilters.isEmpty()) {
      final RexNode residual =
          RexUtil.composeConjunction(scan.getCluster().getRexBuilder(), residualFilters);
      rel = filter.copy(filter.getTraitSet(), newScan, residual);
    }

    // Keep the LogicalProject on top of the scan/filter.
    final RelNode result =
        project.copy(project.getTraitSet(), rel, project.getProjects(), project.getRowType());

    call.transformTo(result);
  }

  private static void decomposeFilter(RexNode condition, int[] projectedFields,
      @Nullable String[] filterValues, List<RexNode> residualFilters) {
    if (condition.isA(SqlKind.AND)) {
      for (RexNode operand : ((RexCall) condition).getOperands()) {
        decomposeFilter(operand, projectedFields, filterValues, residualFilters);
      }
    } else if (condition.isA(SqlKind.EQUALS)) {
      if (!tryPushEquality((RexCall) condition, projectedFields, filterValues)) {
        residualFilters.add(condition);
      }
    } else {
      residualFilters.add(condition);
    }
  }

  private static boolean tryPushEquality(RexCall call, int[] projectedFields,
      @Nullable String[] filterValues) {
    RexNode left = call.getOperands().get(0);
    final RexNode right = call.getOperands().get(1);

    if (left.isA(SqlKind.CAST)) {
      left = ((RexCall) left).getOperands().get(0);
    }

    if (!(left instanceof RexInputRef) || !(right instanceof RexLiteral)) {
      return false;
    }

    final int projectedIndex = ((RexInputRef) left).getIndex();
    if (projectedIndex >= projectedFields.length) {
      return false;
    }
    final int fullTableIndex = projectedFields[projectedIndex];

    if (filterValues[fullTableIndex] != null) {
      return false;
    }

    final RexLiteral literal = (RexLiteral) right;
    final Object value;
    switch (literal.getTypeName()) {
    case CHAR:
    case VARCHAR:
      value = literal.getValueAs(String.class);
      break;
    default:
      value = literal.getValueAs(Comparable.class);
    }
    if (value == null) {
      return false;
    }

    filterValues[fullTableIndex] = value.toString();
    return true;
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
