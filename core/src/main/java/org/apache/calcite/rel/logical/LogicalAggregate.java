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
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * <code>LogicalAggregate</code> is a relational operator which eliminates
 * duplicates and computes totals.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule}
 * <li>{@link org.apache.calcite.rel.rules.AggregateReduceFunctionsRule}.
 * </ul>
 */
public final class LogicalAggregate extends Aggregate {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a LogicalAggregate.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster    Cluster that this relational expression belongs to
   * @param traitSet   Traits
   * @param input      Input relational expression
   * @param groupSet Bit set of grouping fields
   * @param groupSets Grouping sets, or null to use just {@code groupSet}
   * @param aggCalls Array of aggregates to compute, not null
   */
  public LogicalAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
  }

  @Deprecated // to be removed before 2.0
  public LogicalAggregate(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, boolean indicator, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
    checkIndicator(indicator);
  }

  @Deprecated // to be removed before 2.0
  public LogicalAggregate(RelOptCluster cluster,
      RelNode input, boolean indicator, ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, cluster.traitSetOf(Convention.NONE), input, groupSet,
        groupSets, aggCalls);
    checkIndicator(indicator);
  }

  /**
   * Creates a LogicalAggregate by parsing serialized output.
   */
  public LogicalAggregate(RelInput input) {
    super(input);
  }

  /** Creates a LogicalAggregate. */
  public static LogicalAggregate create(final RelNode input,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return create_(input, groupSet, groupSets, aggCalls);
  }

  @Deprecated // to be removed before 2.0
  public static LogicalAggregate create(final RelNode input,
      boolean indicator,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    checkIndicator(indicator);
    return create_(input, groupSet, groupSets, aggCalls);
  }

  private static LogicalAggregate create_(final RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new LogicalAggregate(cluster, traitSet, input, groupSet,
        groupSets, aggCalls);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public LogicalAggregate copy(RelTraitSet traitSet, RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new LogicalAggregate(getCluster(), traitSet, input,
        groupSet, groupSets, aggCalls);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End LogicalAggregate.java
