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
import org.eigenbase.rex.*;

import com.google.common.collect.ImmutableList;

/**
 * <code>CalcRelBase</code> is an abstract base class for implementations of
 * {@link CalcRel}.
 */
public abstract class CalcRelBase extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final RexProgram program;
  private final ImmutableList<RelCollation> collationList;

  //~ Constructors -----------------------------------------------------------

  protected CalcRelBase(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RelDataType rowType,
      RexProgram program,
      List<RelCollation> collationList) {
    super(cluster, traits, child);
    this.rowType = rowType;
    this.program = program;
    this.collationList = ImmutableList.copyOf(collationList);
    assert isValid(true);
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public final CalcRelBase copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), program, collationList);
  }

  /**
   * Creates a copy of this {@code CalcRelBase}.
   */
  public abstract CalcRelBase copy(
      RelTraitSet traitSet,
      RelNode child,
      RexProgram program,
      List<RelCollation> collationList);

  public boolean isValid(boolean fail) {
    if (!RelOptUtil.equal(
        "program's input type",
        program.getInputRowType(),
        "child's output type",
        getChild().getRowType(),
        fail)) {
      return false;
    }
    if (!RelOptUtil.equal(
        "rowtype of program",
        program.getOutputRowType(),
        "declared rowtype of rel",
        rowType,
        fail)) {
      return false;
    }
    if (!program.isValid(fail)) {
      return false;
    }
    if (!program.isNormalized(fail, getCluster().getRexBuilder())) {
      return false;
    }
    if (!RelCollationImpl.isValid(
        getRowType(),
        collationList,
        fail)) {
      return false;
    }
    return true;
  }

  public RexProgram getProgram() {
    return program;
  }

  public double getRows() {
    return FilterRel.estimateFilteredRows(
        getChild(),
        program);
  }

  public List<RelCollation> getCollationList() {
    return collationList;
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    double dRows = RelMetadataQuery.getRowCount(this);
    double dCpu =
        RelMetadataQuery.getRowCount(getChild())
            * program.getExprCount();
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  public RelWriter explainTerms(RelWriter pw) {
    return program.explainCalc(super.explainTerms(pw));
  }
}

// End CalcRelBase.java
