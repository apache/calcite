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
package org.apache.calcite.adapter.openapi;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Relational expression representing a scan of an OpenAPI endpoint.
 *
 * <p>Additional operations might be applied using the "find" method.
 */
public class OpenAPITableScan extends TableScan implements OpenAPIRel {

  private final OpenAPITable openAPITable;
  private final RelDataType projectRowType;

  /**
   * Creates an OpenAPITableScan.
   *
   * @param cluster Cluster
   * @param traitSet Trait set
   * @param table Table
   * @param openAPITable OpenAPI table
   * @param projectRowType Fields and types to project; null to project raw row
   */
  OpenAPITableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, OpenAPITable openAPITable,
      RelDataType projectRowType) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.openAPITable = Objects.requireNonNull(openAPITable, "openAPITable");
    this.projectRowType = projectRowType;

    assert getConvention() == OpenAPIRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new OpenAPITableScan(getCluster(), traitSet, table, openAPITable, projectRowType);
  }

  @Override public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Estimate cost based on projected fields
    final float projectionFactor = projectRowType == null ? 1f :
        (float) projectRowType.getFieldCount() / 100f;
    return super.computeSelfCost(planner, mq).multiplyBy(0.1 * projectionFactor);
  }

  @Override public void register(RelOptPlanner planner) {
    // Register converter rule to convert back to enumerable
    planner.addRule(OpenAPIToEnumerableConverterRule.INSTANCE);

    // Register pushdown rules
    for (RelOptRule rule : OpenAPIRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override public void implement(Implementor implementor) {
    implementor.openAPITable = openAPITable;
    implementor.table = table;
  }
}
