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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import java.util.List;

/**
 * <code>Calc</code> is an abstract base class for implementations of
 * {@link org.apache.calcite.rel.logical.LogicalCalc}.
 */
public abstract class Calc extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final RexProgram program;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Calc.
   *
   * @param cluster Cluster
   * @param traits Traits
   * @param child Input relation
   * @param program Calc program
   */
  protected Calc(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexProgram program) {
    super(cluster, traits, child);
    this.rowType = program.getOutputRowType();
    this.program = program;
    assert isValid(Litmus.THROW, null);
  }

  @Deprecated // to be removed before 2.0
  protected Calc(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexProgram program,
      List<RelCollation> collationList) {
    this(cluster, traits, child, program);
    Util.discard(collationList);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final Calc copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), program);
  }

  /**
   * Creates a copy of this {@code Calc}.
   *
   * @param traitSet Traits
   * @param child Input relation
   * @param program Calc program
   * @return New {@code Calc} if any parameter differs from the value of this
   *   {@code Calc}, or just {@code this} if all the parameters are the same
   *
   * @see #copy(org.apache.calcite.plan.RelTraitSet, java.util.List)
   */
  public abstract Calc copy(
      RelTraitSet traitSet,
      RelNode child,
      RexProgram program);

  @Deprecated // to be removed before 2.0
  public Calc copy(
      RelTraitSet traitSet,
      RelNode child,
      RexProgram program,
      List<RelCollation> collationList) {
    Util.discard(collationList);
    return copy(traitSet, child, program);
  }

  public boolean isValid(Litmus litmus, Context context) {
    if (!RelOptUtil.equal(
        "program's input type",
        program.getInputRowType(),
        "child's output type",
        getInput().getRowType(), litmus)) {
      return litmus.fail(null);
    }
    if (!program.isValid(litmus, context)) {
      return litmus.fail(null);
    }
    if (!program.isNormalized(litmus, getCluster().getRexBuilder())) {
      return litmus.fail(null);
    }
    return litmus.succeed();
  }

  public RexProgram getProgram() {
    return program;
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    return RelMdUtil.estimateFilteredRows(getInput(), program, mq);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double dRows = mq.getRowCount(this);
    double dCpu = mq.getRowCount(getInput())
        * program.getExprCount();
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  public RelWriter explainTerms(RelWriter pw) {
    return program.explainCalc(super.explainTerms(pw));
  }

  public RelNode accept(RexShuttle shuttle) {
    List<RexNode> oldExprs = program.getExprList();
    List<RexNode> exprs = shuttle.apply(oldExprs);
    List<RexLocalRef> oldProjects = program.getProjectList();
    List<RexLocalRef> projects = shuttle.apply(oldProjects);
    RexLocalRef oldCondition = program.getCondition();
    RexNode condition;
    if (oldCondition != null) {
      condition = shuttle.apply(oldCondition);
      assert condition instanceof RexLocalRef
          : "Invalid condition after rewrite. Expected RexLocalRef, got "
          + condition;
    } else {
      condition = null;
    }
    if (exprs == oldExprs
        && projects == oldProjects
        && condition == oldCondition) {
      return this;
    }
    return copy(traitSet, getInput(),
        new RexProgram(program.getInputRowType(),
            exprs,
            projects,
            (RexLocalRef) condition,
            program.getOutputRowType()));
  }
}

// End Calc.java
