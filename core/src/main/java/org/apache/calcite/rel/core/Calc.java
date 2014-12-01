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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgram;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * <code>Calc</code> is an abstract base class for implementations of
 * {@link org.apache.calcite.rel.logical.LogicalCalc}.
 */
public abstract class Calc extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final RexProgram program;
  private final ImmutableList<RelCollation> collationList;

  //~ Constructors -----------------------------------------------------------

  protected Calc(
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

  @Override public final Calc copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), program, collationList);
  }

  /** Creates a copy of this {@code Calc}. */
  public abstract Calc copy(
      RelTraitSet traitSet,
      RelNode child,
      RexProgram program,
      List<RelCollation> collationList);

  public boolean isValid(boolean fail) {
    if (!RelOptUtil.equal(
        "program's input type",
        program.getInputRowType(),
        "child's output type",
        getInput().getRowType(),
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
    return LogicalFilter.estimateFilteredRows(
        getInput(),
        program);
  }

  public List<RelCollation> getCollationList() {
    return collationList;
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    double dRows = RelMetadataQuery.getRowCount(this);
    double dCpu =
        RelMetadataQuery.getRowCount(getInput())
            * program.getExprCount();
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  public RelWriter explainTerms(RelWriter pw) {
    return program.explainCalc(super.explainTerms(pw));
  }
}

// End Calc.java
