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

import java.util.List;

import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

import com.google.common.collect.ImmutableList;

/**
 * <code>FilterRelBase</code> is an abstract base class for implementations of
 * {@link FilterRel}.
 */
public abstract class FilterRelBase extends SingleRel {
  //~ Instance fields --------------------------------------------------------

  protected final RexNode condition;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a filter.
   *
   * @param cluster   {@link RelOptCluster}  this relational expression belongs
   *                  to
   * @param traits    the traits of this rel
   * @param child     input relational expression
   * @param condition boolean expression which determines whether a row is
   *                  allowed to pass
   */
  protected FilterRelBase(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexNode condition) {
    super(cluster, traits, child);
    assert condition != null;
    assert RexUtil.isFlat(condition) : condition;
    this.condition = condition;
  }

  /**
   * Creates a FilterRelBase by parsing serialized output.
   */
  protected FilterRelBase(RelInput input) {
    this(
        input.getCluster(), input.getTraitSet(), input.getInput(),
        input.getExpression("condition"));
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return copy(traitSet, sole(inputs), getCondition());
  }

  public abstract FilterRelBase copy(RelTraitSet traitSet, RelNode input,
      RexNode condition);

  @Override
  public List<RexNode> getChildExps() {
    return ImmutableList.of(condition);
  }

  public RexNode getCondition() {
    return condition;
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    double dRows = RelMetadataQuery.getRowCount(this);
    double dCpu = RelMetadataQuery.getRowCount(getChild());
    double dIo = 0;
    return planner.getCostFactory().makeCost(dRows, dCpu, dIo);
  }

  // override RelNode
  public double getRows() {
    return estimateFilteredRows(
        getChild(),
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

// End FilterRelBase.java
