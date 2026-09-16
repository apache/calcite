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
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.EnsuresNonNullIf;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.sql.SqlKind.SET_QUERY;

/**
 * <code>SetOp</code> is an abstract base for relational set operators such
 * as UNION, MINUS (aka EXCEPT), and INTERSECT.
 */
public abstract class SetOp extends AbstractRelNode implements Hintable {
  //~ Instance fields --------------------------------------------------------

  protected ImmutableList<RelNode> inputs;
  public final SqlKind kind;
  public final boolean all;
  protected final ImmutableList<RelHint> hints;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SetOp.
   */
  protected SetOp(RelOptCluster cluster, RelTraitSet traits, List<RelHint> hints,
      List<RelNode> inputs, SqlKind kind, boolean all) {
    super(cluster, traits);
    checkArgument(SET_QUERY.contains(kind));
    this.kind = kind;
    this.inputs = ImmutableList.copyOf(inputs);
    this.all = all;
    this.hints = ImmutableList.copyOf(hints);
  }

  /**
   * Creates a SetOp.
   */
  protected SetOp(RelOptCluster cluster, RelTraitSet traits,
      List<RelNode> inputs, SqlKind kind, boolean all) {
    this(cluster, traits, Collections.emptyList(), inputs, kind, all);
  }

  /**
   * Creates a SetOp by parsing serialized output.
   */
  protected SetOp(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), Collections.emptyList(),
        input.getInputs(), SqlKind.UNION, input.getBoolean("all", false));
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
    final List<RelNode> newInputs = new ArrayList<>(inputs);
    newInputs.set(ordinalInParent, p);
    inputs = ImmutableList.copyOf(newInputs);
    recomputeDigest();
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

  @API(since = "1.43", status = API.Status.INTERNAL)
  @EnsuresNonNullIf(expression = "#1", result = true)
  protected boolean deepEquals0(@Nullable Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SetOp o = (SetOp) obj;
    if (all != o.all
        || !traitSet.equals(o.traitSet)
        || !hints.equals(o.hints)
        || inputs.size() != o.inputs.size()) {
      return false;
    }
    for (int i = 0; i < inputs.size(); i++) {
      if (!inputs.get(i).deepEquals(o.inputs.get(i))) {
        return false;
      }
    }
    // A SetOp's row type is derived solely from its inputs' row types, so equal inputs
    // imply equal row types; there is no need to compare rowType here.
    return true;
  }

  @API(since = "1.43", status = API.Status.INTERNAL)
  protected int deepHashCode0() {
    int result = 31 + traitSet.hashCode();
    for (int i = 0; i < inputs.size(); i++) {
      result = result * 31 + inputs.get(i).deepHashCode();
    }
    result = result * 31 + Boolean.hashCode(all);
    return result * 31 + hints.hashCode();
  }

  @Override protected RelDataType deriveRowType() {
    return deriveLeastRestrictiveRowType();
  }

  protected RelDataType deriveLeastRestrictiveRowType() {
    final List<RelDataType> inputRowTypes =
        Util.transform(inputs, RelNode::getRowType);
    final RelDataType rowType =
        getCluster().getTypeFactory().leastRestrictive(inputRowTypes);
    if (rowType == null) {
      throw new IllegalArgumentException("Cannot compute compatible row type "
          + "for arguments to set op: "
          + Util.sepList(inputRowTypes, ", "));
    }
    return rowType;
  }

  @Override public ImmutableList<RelHint> getHints() {
    return hints;
  }

  /**
   * Returns whether all the inputs of this set operator have the same row
   * type as its output row.
   *
   * @param compareNames Whether column names are important in the
   *                     homogeneity comparison
   * @return Whether all the inputs of this set operator have the same row
   *   type as its output row
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
