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
package org.apache.calcite.plan.cascades.rel;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.cascades.CascadesRuleCall;
import org.apache.calcite.plan.cascades.CascadesTestUtils;
import org.apache.calcite.plan.cascades.ImplementationRule;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.Table;

/**
 *
 */
public class CascadesTestTableScanRule extends ImplementationRule<LogicalTableScan> {
  public static final CascadesTestTableScanRule CASCADES_TABLE_SCAN_RULE =
      new CascadesTestTableScanRule();

  public CascadesTestTableScanRule() {
    super(LogicalTableScan.class,
        r -> true,
        Convention.NONE, CascadesTestUtils.CASCADES_TEST_CONVENTION, RelFactories.LOGICAL_BUILDER,
        "CascadesTableScanRule");
  }

  @Override public void implement(LogicalTableScan rel, RelTraitSet requestedTraits,
      CascadesRuleCall call) {
    RelOptTable relOptTable = rel.getTable();
    final Table table = relOptTable.unwrap(Table.class);

    RelTraitSet traitSet = rel.getTraitSet()
        .plus(CascadesTestUtils.CASCADES_TEST_CONVENTION)
        .plus(table.getStatistic().getDistribution())
        .plus(RelCollationTraitDef.INSTANCE.getDefault());

    CascadesTestTableScan tableScan =
        new CascadesTestTableScan(rel.getCluster(), traitSet, relOptTable);
    call.transformTo(tableScan);

    for (RelCollation collation : table.getStatistic().getCollations()) {
      traitSet = traitSet.plus(collation);
      tableScan =
          new CascadesTestTableScan(rel.getCluster(), traitSet, relOptTable);
      call.transformTo(tableScan);
    }
  }
}
