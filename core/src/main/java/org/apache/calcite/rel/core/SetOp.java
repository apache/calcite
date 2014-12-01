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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

/**
 * <code>SetOp</code> is an abstract base for relational set operators such
 * as UNION, MINUS (aka EXCEPT), and INTERSECT.
 */
public abstract class SetOp extends AbstractRelNode {
  //~ Instance fields --------------------------------------------------------

  protected ImmutableList<RelNode> inputs;
  public final SqlKind kind;
  public final boolean all;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SetOp.
   */
  protected SetOp(RelOptCluster cluster, RelTraitSet traits,
      List<RelNode> inputs, SqlKind kind, boolean all) {
    super(cluster, traits);
    Preconditions.checkArgument(kind == SqlKind.UNION
        || kind == SqlKind.INTERSECT
        || kind == SqlKind.EXCEPT);
    this.kind = kind;
    this.inputs = ImmutableList.copyOf(inputs);
    this.all = all;
  }

  /**
   * Creates a SetOp by parsing serialized output.
   */
  protected SetOp(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInputs(),
        SqlKind.UNION, input.getBoolean("all", false));
  }

  //~ Methods ----------------------------------------------------------------

  public abstract SetOp copy(
      RelTraitSet traitSet,
      List<RelNode> inputs,
      boolean all);

  @Override public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, inputs, all);
  }

  @Override public void replaceInput(int ordinalInParent, RelNode p) {
    final List<RelNode> newInputs = new ArrayList<RelNode>(inputs);
    newInputs.set(ordinalInParent, p);
    inputs = ImmutableList.copyOf(newInputs);
    recomputeDigest();
  }

  @Override public boolean isKey(ImmutableBitSet columns) {
    // If not ALL then the rows are distinct.
    // Therefore the set of all columns is a key.
    return !all && columns.nextClearBit(0) >= getRowType().getFieldCount();
  }

  @Override public List<RelNode> getInputs() {
    return inputs;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      pw.input("input#" + ord.i, ord.e);
    }
    return pw.item("all", all);
  }

  @Override protected RelDataType deriveRowType() {
    return getCluster().getTypeFactory().leastRestrictive(
        new AbstractList<RelDataType>() {
          @Override public RelDataType get(int index) {
            return inputs.get(index).getRowType();
          }

          @Override public int size() {
            return inputs.size();
          }
        });
  }

  /**
   * Returns whether all the inputs of this set operator have the same row
   * type as its output row.
   *
   * @param compareNames Whether column names are important in the
   *                     homogeneity comparison
   */
  public boolean isHomogeneous(boolean compareNames) {
    RelDataType unionType = getRowType();
    for (RelNode input : getInputs()) {
      if (!RelOptUtil.areRowTypesEqual(
          input.getRowType(), unionType, compareNames)) {
        return false;
      }
    }
    return true;
  }
}

// End SetOp.java
