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
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;

import org.immutables.value.Value;

/**
 * Planner rule that pushes filter predicates into a
 * {@link CsvTableScan}.
 *
 * <p>Any predicate expressible as a {@link org.apache.calcite.rex.RexNode}
 * (including AND, OR, NOT, IS NULL, comparisons, LIKE, etc.) can be pushed
 * down. The condition is compiled at plan time via
 * {@link org.apache.calcite.adapter.enumerable.RexToLixTranslator} into a
 * Java {@link org.apache.calcite.linq4j.function.Predicate1} and applied
 * directly on the enumerable produced by the scan, so no rows that fail the
 * predicate are ever materialised.
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

    // Compose a conjunction of the existing condition and the new one.
    final RexNode newCondition;
    if (scan.condition == null) {
      newCondition = filter.getCondition();
    } else {
      newCondition =
          RexUtil.composeConjunction(scan.getCluster().getRexBuilder(),
          java.util.Arrays.asList(scan.condition, filter.getCondition()));
    }

    // Build a new scan that carries the pushed-down filter condition.
    final CsvTableScan newScan =
        new CsvTableScan(scan.getCluster(), scan.getTable(), scan.csvTable,
        scan.fields, newCondition);

    call.transformTo(newScan);
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
