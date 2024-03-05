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
package org.apache.calcite.rel.dm;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;

import org.codehaus.commons.nullanalysis.Nullable;

import java.util.List;

/**
 * RelNode for creating a Databricks Merge Statement.
 */
public class DatabricksTableMergeModify extends TableModify {

  /**
   * Enumeration of supported modification operations.
   */
  enum DatabricksTableMergeModifyOperation {
    UPDATE, DELETE
  }

  public final List<RexNode> updateExpressionList;
  public final DatabricksTableMergeModifyOperation matchedAction;

  DatabricksTableMergeModify(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table,
       Prepare.CatalogReader catalogReader, RelNode input, @Nullable List<String> updateColumns,
       @Nullable List<RexNode> updateExpressionList, boolean flattened,
       DatabricksTableMergeModifyOperation matchedAction) {
    super(cluster, traitSet, table, catalogReader, input, Operation.MERGE, updateColumns,
            null, flattened);
    this.updateExpressionList = updateExpressionList;
    this.matchedAction = matchedAction;
  }

    /** Creates a DatabricksTableMergeModify. */
  static DatabricksTableMergeModify create(RelOptTable table, Prepare.CatalogReader schema,
       RelNode input, @Nullable List<String> updateColumnList,
       @Nullable List<RexNode> updateExpressionList,
       boolean flattened, DatabricksTableMergeModifyOperation matchedAction) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new DatabricksTableMergeModify(cluster, traitSet, table, schema, input,
        updateColumnList, updateExpressionList, flattened, matchedAction);
  }

  @Override public DatabricksTableMergeModify copy(
        RelTraitSet traitSet,
        List<RelNode> inputs) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new DatabricksTableMergeModify(getCluster(), traitSet, table, catalogReader,
            sole(inputs), getUpdateColumnList(), updateExpressionList, isFlattened(),
            matchedAction);
  }

}
