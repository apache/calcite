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
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.TableModify}
 * not targeted at any particular engine or calling convention.
 */
public final class LogicalTableModify extends TableModify {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalTableModify.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public LogicalTableModify(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, Prepare.CatalogReader schema, RelNode input,
      Operation operation, @Nullable List<String> updateColumnList,
      @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
    super(cluster, traitSet, table, schema, input, operation, updateColumnList,
        sourceExpressionList, flattened);
  }

  /**
   * Creates a LogicalTableModify by parsing serialized output.
   */
  public LogicalTableModify(RelInput input) {
    super(input);
  }

  @Deprecated // to be removed before 2.0
  public LogicalTableModify(RelOptCluster cluster, RelOptTable table,
      Prepare.CatalogReader schema, RelNode input, Operation operation,
      List<String> updateColumnList, boolean flattened) {
    this(cluster,
        cluster.traitSetOf(Convention.NONE),
        table,
        schema,
        input,
        operation,
        updateColumnList,
        null,
        flattened);
  }

  /** Creates a LogicalTableModify. */
  public static LogicalTableModify create(RelOptTable table,
      Prepare.CatalogReader schema, RelNode input,
      Operation operation, @Nullable List<String> updateColumnList,
      @Nullable List<RexNode> sourceExpressionList, boolean flattened) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalTableModify(cluster, traitSet, table, schema, input,
        operation, updateColumnList, sourceExpressionList, flattened);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalTableModify copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalTableModify(getCluster(), traitSet, table, catalogReader,
        sole(inputs), getOperation(), getUpdateColumnList(),
        getSourceExpressionList(), isFlattened());
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}
