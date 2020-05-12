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
package org.apache.calcite.plan.volcano;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

/***
 * An implementation of RelSubset for CascadePlanner.
 * Holding the optimizing state.
 *
 * In cascade, a subset is a group with required physical properties (TraitSet)
 */
public class CascadeRelSubset extends RelSubset {

  /**
   * An enum representing optimizing state of current RelSubset
   */
  enum OptimizeState {
    /**
     * the subset is not yet optimized
     */
    NEW,

    /**
     * the subset is optimizing
     */
    OPTIMIZING,

    /**
     * the subset is optimized. It may have a winner (best).
     * Or it may fail to optimize,
     * in which case, best == null or !bestCost.isLt(upperBound)
     */
    OPTIMIZED
  }

  OptimizeState state = OptimizeState.NEW;

  /**
   * the upper bound of the last OptimizeGroup call
   */
  RelOptCost upperBound;

  /**
   * RelNode ids that is invoked passThrough method before
   */
  Set<Integer> passThroughCache;

  CascadeRelSubset(RelOptCluster cluster, RelSet set, RelTraitSet traits) {
    super(cluster, set, traits);
    upperBound = bestCost;
  }

  public RelOptCost getWinnerCost() {
    if (bestCost == upperBound && state == OptimizeState.OPTIMIZED) {
      return bestCost;
    }
    // if bestCost != upperBound, it means optimize failed
    return null;
  }

  public void startOptimize(RelOptCost ub) {
    if (ub.isLt(bestCost)) {
      upperBound = ub;
    }
    state = OptimizeState.OPTIMIZING;
  }

  @Override public CascadeRelSet getSet() {
    return (CascadeRelSet) set;
  }

  public void optimized() {
    state = OptimizeState.OPTIMIZED;
  }

  public boolean resetOptimizing() {
    boolean optimized = state != OptimizeState.NEW;
    state = OptimizeState.NEW;
    upperBound = bestCost;
    return optimized;
  }

  @Override void propagateCostImprovements0(
      VolcanoPlanner planner, RelMetadataQuery mq,
      RelNode rel, Set<RelSubset> activeSet,
      Queue<Pair<RelSubset, RelNode>> propagationQueue) {
    super.propagateCostImprovements0(planner, mq, rel, activeSet, propagationQueue);
    if (bestCost != upperBound && bestCost.isLe(upperBound)) {
      upperBound = bestCost;
    }
  }

  public RelNode passThrough(RelNode rel) {
    if (!(rel instanceof PhysicalNode)) {
      return null;
    }
    if (passThroughCache == null) {
      passThroughCache = new HashSet<>();
      passThroughCache.add(rel.getId());
    } else if (!passThroughCache.add(rel.getId())) {
      return null;
    }
    return ((PhysicalNode) rel).passThrough(this.getTraitSet());
  }
}
