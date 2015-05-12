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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalcitePrepareImpl;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexChecker;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Relational expression that iterates over its input
 * and returns elements for which <code>condition</code> evaluates to
 * <code>true</code>.
 *
 * <p>If the condition allows nulls, then a null value is treated the same as
 * false.</p>
 *
 * @see org.apache.calcite.rel.logical.LogicalFilter
 */
public abstract class Filter extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final RexNode condition;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a filter.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param traits    the traits of this rel
   * @param child     input relational expression
   * @param condition boolean expression which determines whether a row is
   *                  allowed to pass
   */
  protected Filter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexNode condition) {
    super(cluster, traits, child);
    assert condition != null;
    assert RexUtil.isFlat(condition) : condition;
    this.condition = condition;
    // Too expensive for everyday use:
    assert !CalcitePrepareImpl.DEBUG || isValid(true);
  }

  /**
   * Creates a Filter by parsing serialized output.
   */
  protected Filter(RelInput input) {
    this(input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getExpression("condition"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public final RelNode copy(RelTraitSet traitSet,
      List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), getCondition());
  }

  public abstract Filter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition);

  @Override public List<RexNode> getChildExps() {
    return ImmutableList.of(condition);
  }

  public RelNode accept(RexShuttle shuttle) {
    RexNode condition = shuttle.apply(this.condition);
    if (this.condition == condition) {
      return this;
    }
    return copy(traitSet, getInput(), condition);
  }

  public RexNode getCondition() {
    return condition;
  }

  /**
   * Check if any of the operands of the filter contains a
   * correlation variable
   */
  public boolean hasCorrelation() {
    if (condition instanceof RexCall) {
      try {
        RexVisitor<Void> visitor =
            new RexVisitorImpl<Void>(true) {
              public Void visitCorrelVariable(RexCorrelVariable var) {
                throw new Util.FoundOne(var);
              }
            };
        condition.accept(visitor);
        return false;
      } catch (Util.FoundOne e) {
        Util.swallow(e,  null);
        return true;
      }
    }
    return false;
  }

  @Override public boolean isValid(boolean fail) {
    if (RexUtil.isNullabilityCast(getCluster().getTypeFactory(), condition)) {
      assert !fail : "Cast for just nullability not allowed";
      return false;
    }
    final RexChecker checker = new RexChecker(getInput().getRowType(), fail);
    condition.accept(checker);
    if (checker.getFailureCount() > 0) {
      assert !fail;
      return false;
    }
    return true;
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    double dRows = RelMetadataQuery.getRowCount(this);
    double dCpu = RelMetadataQuery.getRowCount(getInput());
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  // override RelNode
  public double getRows() {
    return estimateFilteredRows(
        getInput(),
        condition);
  }

  public static double estimateFilteredRows(RelNode child, RexProgram program) {
    // convert the program's RexLocalRef condition to an expanded RexNode
    RexLocalRef programCondition = program.getCondition();
    RexNode condition;
    if (programCondition == null) {
      condition = null;
    } else {
      condition = program.expandLocalRef(programCondition);
    }
    return estimateFilteredRows(
        child,
        condition);
  }

  public static double estimateFilteredRows(RelNode child, RexNode condition) {
    return RelMetadataQuery.getRowCount(child)
        * RelMetadataQuery.getSelectivity(child, condition);
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("condition", condition);
  }
}

// End Filter.java
