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

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;

import net.hydromatic.optiq.util.BitSets;

/**
 * <code>TableAccessRelBase</code> is an abstract base class for implementations
 * of {@link TableAccessRel}.
 */
public abstract class TableAccessRelBase extends AbstractRelNode {
  //~ Instance fields --------------------------------------------------------

  /**
   * The table definition.
   */
  protected final RelOptTable table;

  //~ Constructors -----------------------------------------------------------

  protected TableAccessRelBase(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelOptTable table) {
    super(cluster, traits);
    this.table = table;
    if (table.getRelOptSchema() != null) {
      cluster.getPlanner().registerSchema(table.getRelOptSchema());
    }
  }

  /**
   * Creates a TableAccessRelBase by parsing serialized output.
   */
  protected TableAccessRelBase(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getTable("table"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public double getRows() {
    return table.getRowCount();
  }

  @Override public RelOptTable getTable() {
    return table;
  }

  @Override public List<RelCollation> getCollationList() {
    return table.getCollationList();
  }

  @Override public boolean isKey(BitSet columns) {
    return table.isKey(columns);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
    double dRows = table.getRowCount();
    double dCpu = dRows + 1; // ensure non-zero cost
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  @Override public RelDataType deriveRowType() {
    return table.getRowType();
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("table", table.getQualifiedName());
  }

  /**
   * Projects a subset of the fields of the table, and also asks for "extra"
   * fields that were not included in the table's official type.
   *
   * <p>The default implementation assumes that tables cannot do either of
   * these operations, therefore it adds a {@link ProjectRel}, projecting
   * {@code NULL} values for the extra fields.</p>
   *
   * <p>Sub-classes, representing table types that have these capabilities,
   * should override.</p>
   *
   * @param fieldsUsed  Bitmap of the fields desired by the consumer
   * @param extraFields Extra fields, not advertised in the table's row-type,
   *                    wanted by the consumer
   * @return Relational expression that projects the desired fields
   */
  public RelNode project(BitSet fieldsUsed, Set<RelDataTypeField> extraFields,
    RelFactories.ProjectFactory projectFactory) {
    final int fieldCount = getRowType().getFieldCount();
    if (fieldsUsed.equals(BitSets.range(fieldCount))
        && extraFields.isEmpty()) {
      return this;
    }
    List<RexNode> exprList = new ArrayList<RexNode>();
    List<String> nameList = new ArrayList<String>();
    RexBuilder rexBuilder = getCluster().getRexBuilder();
    final List<RelDataTypeField> fields = getRowType().getFieldList();

    // Project the subset of fields.
    for (int i : BitSets.toIter(fieldsUsed)) {
      RelDataTypeField field = fields.get(i);
      exprList.add(rexBuilder.makeInputRef(this, i));
      nameList.add(field.getName());
    }

    // Project nulls for the extra fields. (Maybe a sub-class table has
    // extra fields, but we don't.)
    for (RelDataTypeField extraField : extraFields) {
      exprList.add(
          rexBuilder.ensureType(
              extraField.getType(),
              rexBuilder.constantNull(),
              true));
      nameList.add(extraField.getName());
    }

    return projectFactory.createProject(this, exprList, nameList);
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End TableAccessRelBase.java
