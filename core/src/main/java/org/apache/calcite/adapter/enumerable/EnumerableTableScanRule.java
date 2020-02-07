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

import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/** Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalTableFunctionScan}
 * relational expression
 * {@link EnumerableConvention enumerable calling convention}. */
public class EnumerableTableScanRule extends ConverterRule {

  @Deprecated // to be removed before 2.0
  public EnumerableTableScanRule() {
    this(RelFactories.LOGICAL_BUILDER);
  }

  /**
   * Creates an EnumerableTableScanRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public EnumerableTableScanRule(RelBuilderFactory relBuilderFactory) {
    super(LogicalTableScan.class,
        (Predicate<LogicalTableScan>) r -> EnumerableTableScan.canHandle(r.getTable()),
        Convention.NONE, EnumerableConvention.INSTANCE, relBuilderFactory,
        "EnumerableTableScanRule");
  }

  @Override public RelNode convert(RelNode rel) {
    LogicalTableScan scan = (LogicalTableScan) rel;
    final RelOptTable relOptTable = scan.getTable();
    final Table table = relOptTable.unwrap(Table.class);
    final Expression expression = relOptTable.getExpression(Object.class);

    if (table instanceof QueryableTable
        && (expression != null
            || EnumerableTableScan.canHandle(relOptTable))) {
      return EnumerableTableScan.create(scan.getCluster(), relOptTable);
    }

    if (table instanceof ScannableTable
        || table instanceof FilterableTable
        || table instanceof ProjectableFilterableTable) {
      // Transform to EnumerableInterpreter + BindableTableScan
      // because:
      // 1. ScannableTable can bind data directly;
      // 2. Only BindableTable supports project push down now.
      final Bindables.BindableTableScan bindableScan =
          Bindables.BindableTableScan.create(rel.getCluster(), relOptTable);
      return EnumerableInterpreter.create(bindableScan, 0.5d);
    }

    if (expression != null) {
      return EnumerableTableScan.create(scan.getCluster(), relOptTable);
    }

    return null;
  }
}
