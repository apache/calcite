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

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;

/**
 * <code>EmptyRel</code> represents a relational expression with zero rows.
 *
 * <p>EmptyRel can not be implemented, but serves as a token for rules to match
 * so that empty sections of queries can be eliminated.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>Created by {@code net.sf.farrago.query.FarragoReduceValuesRule}</li>
 * <li>Triggers {@link org.eigenbase.rel.rules.RemoveEmptyRules}</li>
 * </ul>
 *
 * @see org.eigenbase.rel.ValuesRel
 */
public class EmptyRel extends AbstractRelNode {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new EmptyRel.
   *
   * @param cluster Cluster
   * @param rowType row type for tuples which would be produced by this rel if
   *                it actually produced any, but it doesn't (see, philosophy is
   *                good for something after all!)
   */
  public EmptyRel(
      RelOptCluster cluster,
      RelDataType rowType) {
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE));
    this.rowType = rowType;
  }

  //~ Methods ----------------------------------------------------------------

  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.comprises(Convention.NONE);
    assert inputs.isEmpty();
    // immutable with no children
    return this;
  }

  // implement RelNode
  protected RelDataType deriveRowType() {
    return rowType;
  }

  // implement RelNode
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return planner.getCostFactory().makeZeroCost();
  }

  // implement RelNode
  public double getRows() {
    return 0.0;
  }

  // implement RelNode
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        // For rel digest, include the row type to discriminate
        // this from other empties with different row types.
        // For normal EXPLAIN PLAN, omit the type.
        .itemIf(
            "type",
            rowType,
            pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES);
  }
}

// End EmptyRel.java
