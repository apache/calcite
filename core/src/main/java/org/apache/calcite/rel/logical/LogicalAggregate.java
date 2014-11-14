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

import org.eigenbase.relopt.*;

/**
 * <code>AggregateRel</code> is a relational operator which eliminates
 * duplicates and computes totals.
 *
 * <p>Rules:
 *
 * <ul>
 * <li>{@link org.eigenbase.rel.rules.PullConstantsThroughAggregatesRule}
 * <li>{@link org.eigenbase.rel.rules.RemoveDistinctAggregateRule}
 * <li>{@link org.eigenbase.rel.rules.ReduceAggregatesRule}.
 * </ul>
 */
public final class AggregateRel extends AggregateRelBase {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an AggregateRel.
   *
   * @param cluster  Cluster that this relational expression belongs to
   * @param child    input relational expression
   * @param groupSet Bit set of grouping fields
   * @param aggCalls Array of aggregates to compute, not null
   */
  public AggregateRel(
      RelOptCluster cluster,
      RelNode child,
      BitSet groupSet,
      List<AggregateCall> aggCalls) {
    super(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        child,
        groupSet,
        aggCalls);
  }

  /**
   * Creates an AggregateRel by parsing serialized output.
   */
  public AggregateRel(RelInput input) {
    super(input);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public AggregateRel copy(RelTraitSet traitSet, RelNode input,
      BitSet groupSet, List<AggregateCall> aggCalls) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new AggregateRel(getCluster(), input, groupSet, aggCalls);
  }

  @Override public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}

// End AggregateRel.java
