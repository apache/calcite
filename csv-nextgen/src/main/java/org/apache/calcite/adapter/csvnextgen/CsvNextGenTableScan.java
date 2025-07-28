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
package org.apache.calcite.adapter.csvnextgen;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Relational expression representing a scan of a CSV NextGen table.
 *
 * <p>This table scan can be optimized by the Calcite planner and supports
 * different execution engines based on the table configuration.
 */
public class CsvNextGenTableScan extends TableScan implements EnumerableRel {
  final CsvNextGenTable csvTable;

  protected CsvNextGenTableScan(RelOptCluster cluster, RelOptTable table,
      CsvNextGenTable csvTable) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
    this.csvTable = csvTable;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new CsvNextGenTableScan(getCluster(), table, csvTable);
  }

  @Override public RelDataType deriveRowType() {
    return csvTable.getRowType(getCluster().getTypeFactory());
  }

  @Override public void register(RelOptPlanner planner) {
    // Register CSV NextGen specific rules here if needed
    // For now, we rely on the default enumerable conventions
  }

  /**
   * Gets the underlying CSV table.
   */
  public CsvNextGenTable getCsvTable() {
    return csvTable;
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(),
        getRowType(),
        pref.preferArray());

    return implementor.result(
        physType,
        Blocks.toBlock(
            Expressions.call(
                Expressions.new_(
                    csvTable.getClass(),
                    Expressions.constant(csvTable.getSource()),
                    Expressions.constant(csvTable.hasHeader()),
                    Expressions.constant(csvTable.getExecutionEngine()),
                    Expressions.constant(csvTable.getBatchSize())),
                "scan",
                implementor.getRootExpression())));
  }
}
