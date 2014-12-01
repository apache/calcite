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
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * A relational operator that performs nested-loop joins.
 * <p/>
 * <p>It behaves like a kind of {@link org.apache.calcite.rel.core.Join},
 * but works by setting variables in its environment and restarting its
 * right-hand input.
 * <p/>
 * <p>Correlate is not a join since: typical rules should not match Correlate.
 * <p/>
 * <p>A Correlate is used to represent a correlated query. One
 * implementation strategy is to de-correlate the expression.
 *
 * NestedLoops     -> Correlate(A, B, regular)
 * NestedLoopsOuter-> Correlate(A, B, outer)
 * NestedLoopsSemi -> Correlate(A, B, semi)
 * NestedLoopsAnti -> Correlate(A, B, anti)
 * HashJoin        -> EquiJoin(A, B)
 * HashJoinOuter   -> EquiJoin(A, B)
 * HashJoinSemi    -> SemiJoin(A, B, semi)
 * HashJoinAnti    -> SemiJoin(A, B, anti)
 *
 * @see CorrelationId
 */
public abstract class Correlate extends BiRel {
  //~ Instance fields --------------------------------------------------------

//  protected final ImmutableList<CorrelationId> correlationIds;
  protected final CorrelationId correlationId;
  protected final ImmutableBitSet requiredColumns;
  protected final SemiJoinType joinType;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Correlate.
   * @param cluster      cluster this relational expression belongs to
   * @param left         left input relational expression
   * @param right        right input relational expression
   * @param correlationId variable name for the row of left input
   * @param requiredColumns
   * @param joinType join type
   */
  protected Correlate(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns, SemiJoinType joinType) {
    super(cluster, traits, left, right);
    this.joinType = joinType;
    this.correlationId = correlationId;
    this.requiredColumns = requiredColumns;
  }

  /**
   * Creates a Correlate by parsing serialized output.
   */
  public Correlate(RelInput input) {
    this(
        input.getCluster(), input.getTraitSet(), input.getInputs().get(0),
        input.getInputs().get(1),
        new CorrelationId((Integer) input.get("correlationId")),
        input.getBitSet("requiredColumns"),
        input.getEnum("joinType", SemiJoinType.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public Correlate copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 2;
    return copy(traitSet,
        inputs.get(0),
        inputs.get(1),
        correlationId,
        requiredColumns,
        joinType);
  }

  public abstract Correlate copy(RelTraitSet traitSet,
      RelNode left, RelNode right, CorrelationId correlationId,
      ImmutableBitSet requiredColumns, SemiJoinType joinType);

  public SemiJoinType getJoinType() {
    return joinType;
  }

  @Override
  protected RelDataType deriveRowType() {
    switch (joinType) {
    case LEFT:
    case INNER:
      // LogicalJoin is used to share the code of column names deduplication
      return new LogicalJoin(getCluster(), left, right,
          getCluster().getRexBuilder().makeLiteral(true),
          joinType.toJoinType(), ImmutableSet.<String>of())
          .deriveRowType();
    case ANTI:
    case SEMI:
      return left.getRowType();
    default:
      throw new IllegalStateException("Unknown join type " + joinType);
    }
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("correlation", correlationId)
        .item("joinType", joinType)
        .item("requiredColumns", requiredColumns.toString());
  }

  /**
   * Returns the correlating expressions.
   *
   * @return correlating expressions
   */
  public CorrelationId getCorrelationId() {
    return correlationId;
  }

  @Override
  public String getCorrelVariable() {
    return correlationId.getName();
  }

  /**
   * Returns the required columns in left relation required for the correlation
   * in the right.
   *
   * @return columns in left relation required for the correlation in the right
   */
  public ImmutableBitSet getRequiredColumns() {
    return requiredColumns;
  }

  @Override
  public Set<String> getVariablesStopped() {
    return ImmutableSet.of(correlationId.getName());
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    double rowCount = RelMetadataQuery.getRowCount(this);

    final double rightRowCount = right.getRows();
    final double leftRowCount = left.getRows();
    if (Double.isInfinite(leftRowCount) || Double.isInfinite(rightRowCount)) {
      return planner.getCostFactory().makeInfiniteCost();
    }

    Double restartCount = RelMetadataQuery.getRowCount(getLeft());
    // RelMetadataQuery.getCumulativeCost(getRight()); does not work for
    // RelSubset, so we ask planner to cost-estimate right relation
    RelOptCost rightCost = planner.getCost(getRight());
    RelOptCost rescanCost =
        rightCost.multiplyBy(Math.max(1.0, restartCount - 1));

    return planner.getCostFactory().makeCost(
        rowCount /* generate results */ + leftRowCount /* scan left results */,
        0, 0).plus(rescanCost);
  }
}

// End Correlate.java
