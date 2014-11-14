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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.List;

/**
 * Relational expression with zero rows.
 *
 * <p>Empty can not be implemented, but serves as a token for rules to match
 * so that empty sections of queries can be eliminated.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>Created by {@code net.sf.farrago.query.FarragoReduceValuesRule}</li>
 * <li>Triggers {@link org.apache.calcite.rel.rules.EmptyPruneRules}</li>
 * </ul>
 *
 * @see org.apache.calcite.rel.logical.LogicalValues
 * @see OneRow
 */
public class Empty extends AbstractRelNode {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new Empty.
   *
   * @param cluster Cluster
   * @param rowType row type for tuples which would be produced by this rel if
   *                it actually produced any, but it doesn't (see, philosophy is
   *                good for something after all!)
   */
  public Empty(RelOptCluster cluster, RelDataType rowType) {
    super(cluster, cluster.traitSetOf(Convention.NONE));
    this.rowType = rowType;
  }

  //~ Methods ----------------------------------------------------------------

  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert traitSet.comprises(Convention.NONE);
    assert inputs.isEmpty();
    // immutable with no children
    return this;
  }

  @Override protected RelDataType deriveRowType() {
    return rowType;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return planner.getCostFactory().makeZeroCost();
  }

  @Override public double getRows() {
    return 0.0;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
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

// End Empty.java
