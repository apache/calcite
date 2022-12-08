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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Table;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Exact copy of logical table scan, except for the class name. This node is used exclusively
 * for identifying the target table of a merge into.
 */
public class LogicalTargetTableScan extends TableScan {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalTargetTableScan.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public LogicalTargetTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelOptTable table) {
    super(cluster, traitSet, hints, table);
  }

  @Deprecated // to be removed before 2.0
  public LogicalTargetTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table) {
    this(cluster, traitSet, ImmutableList.of(), table);
  }

  @Deprecated // to be removed before 2.0
  public LogicalTargetTableScan(RelOptCluster cluster, RelOptTable table) {
    this(cluster, cluster.traitSetOf(Convention.NONE), ImmutableList.of(), table);
  }

  /**
   * Creates a LogicalTargetTableScan by parsing serialized output.
   */
  public LogicalTargetTableScan(RelInput input) {
    super(input);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    assert inputs.isEmpty();
    return this;
  }

  /** Creates a LogicalTargetTableScan.
   *
   * @param cluster     Cluster
   * @param relOptTable Table
   * @param hints       The hints
   */
  public static LogicalTargetTableScan create(RelOptCluster cluster,
      final RelOptTable relOptTable, List<RelHint> hints) {
    final Table table = relOptTable.unwrap(Table.class);
    final RelTraitSet traitSet =
        cluster.traitSetOf(Convention.NONE)
            .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
              if (table != null) {
                return table.getStatistic().getCollations();
              }
              return ImmutableList.of();
            });
    return new LogicalTargetTableScan(cluster, traitSet, hints, relOptTable);
  }

  @Override public RelNode withHints(List<RelHint> hintList) {
    return new LogicalTargetTableScan(getCluster(), traitSet, hintList, table);
  }
}
