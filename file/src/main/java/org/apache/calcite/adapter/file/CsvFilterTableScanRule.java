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
 * Planner rule that pushes simple equality filter predicates into a
 * {@link CsvTableScan}.
 *
 * <p>Only equality conditions of the form {@code column = literal} can be
 * pushed down, because {@link CsvEnumerator} only supports per-column
 * equality filtering via its {@code filterValues} array.
 * Any predicates that cannot be pushed down (e.g., range comparisons,
 * {@code OR} expressions, or comparisons involving expressions) are left
 * as a residual {@link LogicalFilter} above the scan.
 *
 * <p>This rule fires after {@link CsvProjectTableScanRule} so that the
 * scan's field list may already be a subset of the full table fields.
 * Filter column references are mapped back through the projected field list
 * to the original column indices expected by {@link CsvEnumerator}.
 *
 * @see FileRules#FILTER_SCAN
 */
@Value.Enclosing
public class CsvFilterTableScanRule
    extends RelRule<CsvFilterTableScanRule.Config> {

  /** Creates a CsvFilterTableScanRule. */
  protected CsvFilterTableScanRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(0);
    final CsvTableScan scan = call.rel(1);

    // filterValues is indexed by the *full* table column index so that
    // CsvEnumerator can match against the raw CSV row directly.
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
    final RelNode result;
    if (residualFilters.isEmpty()) {
      result = newScan;
    } else {
      final RexNode residual =
          RexUtil.composeConjunction(scan.getCluster().getRexBuilder(), residualFilters);
      result = filter.copy(filter.getTraitSet(), newScan, residual);
    }
    call.transformTo(result);
  }

  /**
   * Decomposes a filter condition into pushable equality predicates and
   * non-pushable residual predicates.
   *
   * <p>AND conjunctions are recursively decomposed so that pushable
   * sub-predicates can be extracted even when some siblings are not pushable.
   *
   * <p>A predicate {@code col = literal} is pushable when:
   * <ul>
   *   <li>The left operand is a {@link RexInputRef} (optionally wrapped in
   *       a {@code CAST}), referring to one of the projected columns in
   *       {@code projectedFields}.</li>
   *   <li>The right operand is a {@link RexLiteral}.</li>
   *   <li>No earlier predicate has already set a value for the same column
   *       (first match wins).</li>
   * </ul>
   *
   * @param condition        The filter condition to decompose
   * @param projectedFields  The field projection array from the current
   *                         {@link CsvTableScan} ({@code scan.fields}), used
   *                         to map projected column indices back to full-table
   *                         column indices for {@code filterValues}
   * @param filterValues     Output: per-full-table-column equality values;
   *                         populated in place for pushable predicates
   * @param residualFilters  Output: predicates that could not be pushed down
   */
  private static void decomposeFilter(RexNode condition, int[] projectedFields,
      @Nullable String[] filterValues, List<RexNode> residualFilters) {
    if (condition.isA(SqlKind.AND)) {
      // Decompose AND: process each conjunct independently so that pushable
      // sub-predicates can be separated from non-pushable ones.
      for (RexNode operand : ((RexCall) condition).getOperands()) {
        decomposeFilter(operand, projectedFields, filterValues, residualFilters);
      }
    } else if (condition.isA(SqlKind.EQUALS)) {
      if (!tryPushEquality((RexCall) condition, projectedFields, filterValues)) {
        residualFilters.add(condition);
      }
    } else {
      // Any other predicate kind (OR, comparison, function call, etc.)
      // cannot be pushed into the CsvEnumerator's string-equality filter.
      residualFilters.add(condition);
    }
  }

  /**
   * Attempts to push a single equality predicate into {@code filterValues}.
   *
   * @return {@code true} if the predicate was pushed; {@code false} if it
   *         must remain as a residual filter
   */
  private static boolean tryPushEquality(RexCall call, int[] projectedFields,
      @Nullable String[] filterValues) {
    RexNode left = call.getOperands().get(0);
    final RexNode right = call.getOperands().get(1);

    // Unwrap a CAST on the left side (e.g., CAST(col AS VARCHAR) = 'val').
    if (left.isA(SqlKind.CAST)) {
      left = ((RexCall) left).getOperands().get(0);
    }

    if (!(left instanceof RexInputRef) || !(right instanceof RexLiteral)) {
      return false;
    }

    // The RexInputRef index is relative to the *projected* row type of the
    // current scan, not to the full table row type. Map it back.
    final int projectedIndex = ((RexInputRef) left).getIndex();
    if (projectedIndex >= projectedFields.length) {
      return false;
    }
    final int fullTableIndex = projectedFields[projectedIndex];

    // First equality for this column wins; ignore duplicates.
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
      // NULL literals cannot be represented by CsvEnumerator's filter values.
      return false;
    }

    filterValues[fullTableIndex] = value.toString();
    return true;
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCsvFilterTableScanRule.Config.builder()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalFilter.class).oneInput(b1 ->
                b1.operand(CsvTableScan.class).noInputs()))
        .build();

    @Override default CsvFilterTableScanRule toRule() {
      return new CsvFilterTableScanRule(this);
    }
  }
}
