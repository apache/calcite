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
package org.apache.calcite.adapter.arrow;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Relational expression representing a scan of an Arrow collection.
 */
class ArrowTableScan extends TableScan implements ArrowRel {
  private final ArrowTable arrowTable;
  private final ImmutableIntList fields;

  ArrowTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable relOptTable, ArrowTable arrowTable, ImmutableIntList fields) {
    super(cluster, traitSet, ImmutableList.of(), relOptTable);
    this.arrowTable = arrowTable;
    this.fields = fields;

    assert getConvention() == ArrowRel.CONVENTION;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    checkArgument(inputs.isEmpty());
    return this;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("fields", fields);
  }

  @Override public RelDataType deriveRowType() {
    final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();
    final RelDataTypeFactory.Builder builder =
        getCluster().getTypeFactory().builder();
    for (int field : fields) {
      builder.add(fieldList.get(field));
    }
    return builder.build();
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(ArrowRules.TO_ENUMERABLE);
    for (RelOptRule rule : ArrowRules.RULES) {
      planner.addRule(rule);
    }
  }

  @Override public void implement(ArrowRel.Implementor implementor) {
    implementor.arrowTable = arrowTable;
    implementor.table = table;
  }
}
