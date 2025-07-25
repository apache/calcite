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
package org.apache.calcite.adapter.salesforce;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Relational expression representing a scan of a Salesforce table.
 */
public class SalesforceTableScan extends TableScan implements SalesforceRel {

  private final SalesforceTable salesforceTable;
  private final String sObjectType;
  private final ImmutableList<String> projectedFields;

  public SalesforceTableScan(RelOptCluster cluster, RelOptTable table,
      SalesforceTable salesforceTable, String sObjectType) {
    this(cluster, cluster.traitSetOf(SalesforceRel.CONVENTION), table,
        salesforceTable, sObjectType, null);
  }

  public SalesforceTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, SalesforceTable salesforceTable, String sObjectType,
      List<String> projectedFields) {
    super(cluster, traitSet, ImmutableList.of(), table);
    this.salesforceTable = salesforceTable;
    this.sObjectType = sObjectType;
    this.projectedFields = projectedFields == null ? null : ImmutableList.copyOf(projectedFields);

    assert getConvention() == SalesforceRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new SalesforceTableScan(getCluster(), traitSet, table,
        salesforceTable, sObjectType, projectedFields);
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(SalesforceRules.TO_ENUMERABLE);
    for (RelOptRule rule : SalesforceRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override public void implement(Implementor implementor) {
    implementor.salesforceTable = salesforceTable;
    implementor.table = table;
    implementor.sObjectType = sObjectType;
  }

  /**
   * Get the projected fields, or null if all fields should be selected.
   */
  public List<String> getProjectedFields() {
    return projectedFields;
  }

  /**
   * Get the sObject type.
   */
  public String getSObjectType() {
    return sObjectType;
  }
}
