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
package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.SqlKind;
import org.eigenbase.sql.type.*;

import net.hydromatic.optiq.prepare.Prepare;

/**
 * <code>TableModificationRelBase</code> is an abstract base class for
 * implementations of {@link TableModificationRel}.
 */
public abstract class TableModificationRelBase extends SingleRel {
  //~ Enums ------------------------------------------------------------------

  /**
   * Enumeration of supported modification operations.
   */
  public enum Operation {
    INSERT, UPDATE, DELETE, MERGE;
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
  private RelDataType inputRowType;
  private final boolean flattened;

  //~ Constructors -----------------------------------------------------------

  protected TableModificationRelBase(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelOptTable table,
      Prepare.CatalogReader catalogReader,
      RelNode child,
      Operation operation,
      List<String> updateColumnList,
      boolean flattened) {
    super(cluster, traits, child);
    this.table = table;
    this.catalogReader = catalogReader;
    this.operation = operation;
    this.updateColumnList = updateColumnList;
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

  // implement RelNode
  public RelDataType deriveRowType() {
    return RelOptUtil.createDmlRowType(
        SqlKind.INSERT, getCluster().getTypeFactory());
  }

  // override RelNode
  public RelDataType getExpectedInputRowType(int ordinalInParent) {
    assert ordinalInParent == 0;

    if (inputRowType != null) {
      return inputRowType;
    }

    if (isUpdate()) {
      inputRowType =
          getCluster().getTypeFactory().createJoinType(
              table.getRowType(),
              getCatalogReader().createTypeFromProjection(
                  table.getRowType(),
                  updateColumnList));
    } else if (isMerge()) {
      inputRowType =
          getCluster().getTypeFactory().createJoinType(
              getCluster().getTypeFactory().createJoinType(
                  table.getRowType(),
                  table.getRowType()),
              getCatalogReader().createTypeFromProjection(
                  table.getRowType(),
                  updateColumnList));
    } else {
      inputRowType = table.getRowType();
    }

    if (flattened) {
      inputRowType =
          SqlTypeUtil.flattenRecordType(
              getCluster().getTypeFactory(),
              inputRowType,
              null);
    }

    return inputRowType;
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("table", table.getQualifiedName())
        .item("operation", getOperation())
        .item(
            "updateColumnList",
            (updateColumnList == null)
                ? Collections.EMPTY_LIST
                : updateColumnList)
        .item("flattened", flattened);
  }

  // implement RelNode
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // REVIEW jvs 21-Apr-2006:  Just for now...
    double rowCount = RelMetadataQuery.getRowCount(this);
    return planner.getCostFactory().makeCost(rowCount, 0, 0);
  }
}

// End TableModificationRelBase.java
