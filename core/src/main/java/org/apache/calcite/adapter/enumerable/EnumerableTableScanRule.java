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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.Table;

import org.checkerframework.checker.nullness.qual.Nullable;

/** Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalTableScan} to
 * {@link EnumerableConvention enumerable calling convention}.
 *
 * @see EnumerableRules#ENUMERABLE_TABLE_SCAN_RULE */
public class EnumerableTableScanRule extends ConverterRule {
  /** Default configuration. */
  public static final Config DEFAULT_CONFIG = Config.EMPTY
      .as(Config.class)
      .withConversion(LogicalTableScan.class,
          r -> EnumerableTableScan.canHandle(r.getTable()),
          Convention.NONE, EnumerableConvention.INSTANCE,
          "EnumerableTableScanRule")
      .withRuleFactory(EnumerableTableScanRule::new);

  protected EnumerableTableScanRule(Config config) {
    super(config);
  }

  @Override public @Nullable RelNode convert(RelNode rel) {
    LogicalTableScan scan = (LogicalTableScan) rel;
    final RelOptTable relOptTable = scan.getTable();
    final Table table = relOptTable.unwrap(Table.class);
    // The QueryableTable can only be implemented as ENUMERABLE convention,
    // but some test QueryableTables do not really implement the expressions,
    // just skips the QueryableTable#getExpression invocation and returns early.
    if (table instanceof QueryableTable || relOptTable.getExpression(Object.class) != null) {
      return EnumerableTableScan.create(scan.getCluster(), relOptTable);
    }

    return null;
  }
}
