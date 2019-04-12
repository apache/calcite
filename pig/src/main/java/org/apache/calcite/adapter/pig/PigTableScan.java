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
package org.apache.calcite.adapter.pig;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.type.RelDataTypeField;

import org.apache.pig.data.DataType;

import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.TableScan} in
 * {@link PigRel#CONVENTION Pig calling convention}. */
public class PigTableScan extends TableScan implements PigRel {

  /** Creates a PigTableScan. */
  public PigTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
    super(cluster, traitSet, table);
    assert getConvention() == PigRel.CONVENTION;
  }

  @Override public void implement(Implementor implementor) {
    final PigTable pigTable = getPigTable(implementor.getTableName(this));
    final String alias = implementor.getPigRelationAlias(this);
    final String schema = '(' + getSchemaForPigStatement(implementor)
        + ')';
    final String statement = alias + " = LOAD '" + pigTable.getFilePath()
        + "' USING PigStorage() AS " + schema + ';';
    implementor.addStatement(statement);
  }

  private PigTable getPigTable(String name) {
    final CalciteSchema schema = getTable().unwrap(org.apache.calcite.jdbc.CalciteSchema.class);
    return (PigTable) schema.getTable(name, false).getTable();
  }

  private String getSchemaForPigStatement(Implementor implementor) {
    final List<String> fieldNamesAndTypes = new ArrayList<>(
        getTable().getRowType().getFieldList().size());
    for (RelDataTypeField f : getTable().getRowType().getFieldList()) {
      fieldNamesAndTypes.add(getConcatenatedFieldNameAndTypeForPigSchema(implementor, f));
    }
    return String.join(", ", fieldNamesAndTypes);
  }

  private String getConcatenatedFieldNameAndTypeForPigSchema(Implementor implementor,
      RelDataTypeField field) {
    final PigDataType pigDataType = PigDataType.valueOf(field.getType().getSqlTypeName());
    final String fieldName = implementor.getFieldName(this, field.getIndex());
    return fieldName + ':' + DataType.findTypeName(pigDataType.getPigType());
  }

  @Override public void register(RelOptPlanner planner) {
    planner.addRule(PigToEnumerableConverterRule.INSTANCE);
    for (RelOptRule rule : PigRules.ALL_PIG_OPT_RULES) {
      planner.addRule(rule);
    }
    // Don't move Aggregates around, otherwise PigAggregate.implement() won't
    // know how to correctly procuce Pig Latin
    planner.removeRule(AggregateExpandDistinctAggregatesRule.INSTANCE);
    // Make sure planner picks PigJoin over EnumerableHashJoin. Should there be
    // a rule for this instead for removing ENUMERABLE_JOIN_RULE here?
    planner.removeRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
  }
}

// End PigTableScan.java
