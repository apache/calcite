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
package org.apache.calcite.adapter.file;

import org.apache.calcite.adapter.enumerable.EnumerableCalc;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Relational expression representing a scan of a CSV file.
 *
 * <p>Like any table scan, it serves as a leaf node of a query tree.
 */
public class CsvTableScan extends TableScan implements EnumerableRel {
  final CsvTranslatableTable csvTable;
  final int[] fields;
  final @Nullable RexNode condition;

  protected CsvTableScan(RelOptCluster cluster, RelOptTable table,
      CsvTranslatableTable csvTable, int[] fields) {
    this(cluster, table, csvTable, fields, null);
  }

  protected CsvTableScan(RelOptCluster cluster, RelOptTable table,
      CsvTranslatableTable csvTable, int[] fields,
      @Nullable RexNode condition) {
    super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), ImmutableList.of(), table);
    this.csvTable = requireNonNull(csvTable, "csvTable");
    this.fields = fields;
    this.condition = condition;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return new CsvTableScan(getCluster(), table, csvTable, fields, condition);
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("fields", Primitive.asList(fields))
        .itemIf("condition", condition, condition != null);
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
    planner.addRule(FileRules.PROJECT_SCAN);
    planner.addRule(FileRules.FILTER_SCAN);
    planner.addRule(FileRules.PROJECT_FILTER_SCAN);
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // Multiply the cost by a factor that makes a scan more attractive if it
    // has significantly fewer fields than the original scan.
    //
    // The "+ 2D" on top and bottom keeps the function fairly smooth.
    //
    // For example, if the table has 3 fields and the scan has 1 field,
    // then factor = (1 + 2) / (3 + 2) = 0.6.
    final RelOptCost cost =
        requireNonNull(super.computeSelfCost(planner, mq));
    final double factor =
        (fields.length + 2D)
            / (table.getRowType().getFieldCount() + 2D);
    return cost.multiplyBy(factor);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.preferArray());

    final Expression expression =
        requireNonNull(table.getExpression(CsvTranslatableTable.class));

    // Call CsvTranslatableTable.project(root, fields) to get the base enumerable.
    Expression enumerable =
        Expressions.call(expression,
            "project", implementor.getRootExpression(),
            Expressions.constant(fields));

    if (condition != null) {
      final List<RexNode> projects = new ArrayList<>();
      for (int i = 0; i < getRowType().getFieldCount(); i++) {
        projects.add(
            getCluster().getRexBuilder().makeInputRef(
                getRowType().getFieldList().get(i).getType(), i));
      }
      final RexProgram program =
          RexProgram.create(getRowType(), projects, condition,
              getRowType(), getCluster().getRexBuilder());

      // Create a scan node without the condition so EnumerableCalc sees a plain
      // enumerable input, then wrap it with EnumerableCalc to apply the filter.
      final CsvTableScan plainScan =
          new CsvTableScan(getCluster(), table, csvTable, fields);
      final EnumerableCalc calc = EnumerableCalc.create(plainScan, program);
      return calc.implement(implementor, pref);
    }

    return implementor.result(physType, Blocks.toBlock(enumerable));
  }
}
