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
package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.ConditionalCorrelate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Sub-class of {@link ConditionalCorrelate} not targeted at any particular engine or calling convention.
 */
public final class LogicalConditionalCorrelate extends ConditionalCorrelate {
  //~ Instance fields --------------------------------------------------------

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalConditionalCorrelate.
   *
   * @param cluster         Cluster this relational expression belongs to
   * @param left            Left input relational expression
   * @param right           Right input relational expression
   * @param correlationId   Variable name for the row of left input
   * @param requiredColumns Required columns
   * @param joinType        Join type
   * @param condition       Join condition
   */
  public LogicalConditionalCorrelate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns,
      JoinRelType joinType,
      RexNode condition) {
    super(cluster, traitSet, hints, left, right, correlationId,
        requiredColumns, joinType, condition);
  }

  /** Creates a LogicalConditionalCorrelate. */
  public static LogicalConditionalCorrelate create(RelNode left, RelNode right, List<RelHint> hints,
      CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType,
      RexNode condition) {
    final RelOptCluster cluster = left.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalConditionalCorrelate(cluster, traitSet, hints, left, right, correlationId,
        requiredColumns, joinType, condition);
  }

  @Override public ConditionalCorrelate copy(RelTraitSet traitSet, RelNode left, RelNode right,
      CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType,
      RexNode condition) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalConditionalCorrelate(getCluster(), traitSet, hints, left, right,
        correlationId, requiredColumns, joinType, condition);
  }

  @Override public Correlate copy(RelTraitSet traitSet,
      RelNode left, RelNode right, CorrelationId correlationId,
      ImmutableBitSet requiredColumns, JoinRelType joinType) {
    // This method does not provide the condition as an argument, so it should never be called
    throw new RuntimeException("This method should not be called");
  }
}
