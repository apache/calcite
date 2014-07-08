/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.apache.optiq.impl.mongodb;

import org.apache.optiq.rel.*;
import org.apache.optiq.relopt.*;
import org.apache.optiq.reltype.RelDataType;

import java.util.*;

/**
 * Relational expression representing a scan of a MongoDB collection.
 *
 * <p> Additional operations might be applied,
 * using the "find" or "aggregate" methods.</p>
 */
public class MongoTableScan extends TableAccessRelBase implements MongoRel {
  final MongoTable mongoTable;
  final RelDataType projectRowType;

  /**
   * Creates a MongoTableScan.
   *
   * @param cluster        Cluster
   * @param traitSet       Traits
   * @param table          Table
   * @param mongoTable     MongoDB table
   * @param projectRowType Fields and types to project; null to project raw row
   */
  protected MongoTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, MongoTable mongoTable, RelDataType projectRowType) {
    super(cluster, traitSet, table);
    this.mongoTable = mongoTable;
    this.projectRowType = projectRowType;

    assert mongoTable != null;
    assert getConvention() == MongoRel.CONVENTION;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return this;
  }

  @Override
  public RelDataType deriveRowType() {
    return projectRowType != null ? projectRowType : super.deriveRowType();
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // scans with a small project list are cheaper
    final float f = projectRowType == null ? 1f
        : (float) projectRowType.getFieldCount() / 100f;
    return super.computeSelfCost(planner).multiplyBy(.1 * f);
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(MongoToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : MongoRules.RULES) {
      planner.addRule(rule);
    }
  }

  public void implement(Implementor implementor) {
    implementor.mongoTable = mongoTable;
    implementor.table = table;
  }
}

// End MongoTableScan.java
