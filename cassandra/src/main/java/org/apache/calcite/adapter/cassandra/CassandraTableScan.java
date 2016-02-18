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
package org.apache.calcite.adapter.cassandra;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

/**
 * Relational expression representing a scan of a Cassandra collection.
 */
public class CassandraTableScan extends TableScan implements CassandraRel {
  final CassandraTable cassandraTable;
  final RelDataType projectRowType;

  /**
   * Creates a CassandraTableScan.
   *
   * @param cluster        Cluster
   * @param traitSet       Traits
   * @param table          Table
   * @param cassandraTable Cassandra table
   * @param projectRowType Fields and types to project; null to project raw row
   */
  protected CassandraTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, CassandraTable cassandraTable, RelDataType projectRowType) {
    super(cluster, traitSet, table);
    this.cassandraTable = cassandraTable;
    this.projectRowType = projectRowType;

    assert cassandraTable != null;
    assert getConvention() == CassandraRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(CassandraToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : CassandraRules.RULES) {
      planner.addRule(rule);
    }
  }

  public void implement(Implementor implementor) {
    implementor.cassandraTable = cassandraTable;
    implementor.table = table;
  }
}

// End CassandraTableScan.java
