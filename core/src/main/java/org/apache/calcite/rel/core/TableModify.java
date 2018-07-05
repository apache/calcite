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
package org.apache.calcite.rel.core;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * Relational expression that modifies a table.
 *
 * <p>It is similar to {@link org.apache.calcite.rel.core.TableScan},
 * but represents a request to modify a table rather than read from it.
 * It takes one child which produces the modified rows. Those rows are:
 *
 * <ul>
 * <li>For {@code INSERT}, those rows are the new values;
 * <li>for {@code DELETE}, the old values;
 * <li>for {@code UPDATE}, all old values plus updated new values.
 * </ul>
 */
public abstract class TableModify extends SingleRel {
  //~ Enums ------------------------------------------------------------------

  /**
   * Enumeration of supported modification operations.
   */
  public enum Operation {
    INSERT, UPDATE, DELETE, MERGE
  }

  //~ Instance fields --------------------------------------------------------

  /**
   * The connection to the optimizing session.
   */
  protected Prepare.CatalogReader catalogReader;

  /**
   * The table definition.
   */
  protected final RelOptTable table;
  private final Operation operation;
  private final List<String> updateColumnList;
  private final List<RexNode> sourceExpressionList;
  private RelDataType inputRowType;
  private final boolean flattened;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a {@code TableModify}.
   *
   * <p>The UPDATE operation has format like this:
   * <blockquote>
   *   <pre>UPDATE table SET iden1 = exp1, ident2 = exp2  WHERE condition</pre>
   * </blockquote>
   *
   * @param cluster    Cluster this relational expression belongs to
   * @param traitSet   Traits of this relational expression
   * @param table      Target table to modify
   * @param catalogReader accessor to the table metadata.
   * @param input      Sub-query or filter condition
   * @param operation  Modify operation (INSERT, UPDATE, DELETE)
   * @param updateColumnList List of column identifiers to be updated
   *           (e.g. ident1, ident2); null if not UPDATE
   * @param sourceExpressionList List of value expressions to be set
   *           (e.g. exp1, exp2); null if not UPDATE
   * @param flattened Whether set flattens the input row type
   */
  protected TableModify(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      Prepare.CatalogReader catalogReader,
      RelNode input,
      Operation operation,
      List<String> updateColumnList,
      List<RexNode> sourceExpressionList,
      boolean flattened) {
    super(cluster, traitSet, input);
    this.table = table;
    this.catalogReader = catalogReader;
    this.operation = operation;
    this.updateColumnList = updateColumnList;
    this.sourceExpressionList = sourceExpressionList;
    if (operation == Operation.UPDATE) {
      Objects.requireNonNull(updateColumnList);
      Objects.requireNonNull(sourceExpressionList);
      Preconditions.checkArgument(sourceExpressionList.size()
          == updateColumnList.size());
    } else {
      Preconditions.checkArgument(updateColumnList == null);
      Preconditions.checkArgument(sourceExpressionList == null);
    }
    if (table.getRelOptSchema() != null) {
      cluster.getPlanner().registerSchema(table.getRelOptSchema());
    }
    this.flattened = flattened;
  }

  //~ Methods ----------------------------------------------------------------

  public Prepare.CatalogReader getCatalogReader() {
    return catalogReader;
  }

  public RelOptTable getTable() {
    return table;
  }

  public List<String> getUpdateColumnList() {
    return updateColumnList;
  }

  public List<RexNode> getSourceExpressionList() {
    return sourceExpressionList;
  }

  public boolean isFlattened() {
    return flattened;
  }

  public Operation getOperation() {
    return operation;
  }

  public boolean isInsert() {
    return operation == Operation.INSERT;
  }

  public boolean isUpdate() {
    return operation == Operation.UPDATE;
  }

  public boolean isDelete() {
    return operation == Operation.DELETE;
  }

  public boolean isMerge() {
    return operation == Operation.MERGE;
  }

  @Override public RelDataType deriveRowType() {
    return RelOptUtil.createDmlRowType(
        SqlKind.INSERT, getCluster().getTypeFactory());
  }

  @Override public RelDataType getExpectedInputRowType(int ordinalInParent) {
    assert ordinalInParent == 0;

    if (inputRowType != null) {
      return inputRowType;
    }

    final RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    final RelDataType rowType = table.getRowType();
    switch (operation) {
    case UPDATE:
      inputRowType =
          typeFactory.createJoinType(rowType,
              getCatalogReader().createTypeFromProjection(rowType,
                  updateColumnList));
      break;
    case MERGE:
      inputRowType =
          typeFactory.createJoinType(
              typeFactory.createJoinType(rowType, rowType),
              getCatalogReader().createTypeFromProjection(rowType,
                  updateColumnList));
      break;
    default:
      inputRowType = rowType;
      break;
    }

    if (flattened) {
      inputRowType =
          SqlTypeUtil.flattenRecordType(
              typeFactory,
              inputRowType,
              null);
    }

    return inputRowType;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("table", table.getQualifiedName())
        .item("operation", getOperation())
        .itemIf("updateColumnList", updateColumnList, updateColumnList != null)
        .itemIf("sourceExpressionList", sourceExpressionList,
            sourceExpressionList != null)
        .item("flattened", flattened);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // REVIEW jvs 21-Apr-2006:  Just for now...
    double rowCount = mq.getRowCount(this);
    return planner.getCostFactory().makeCost(rowCount, 0, 0);
  }
}

// End TableModify.java
