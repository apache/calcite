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

  CascadeRelSubset(RelOptCluster cluster, RelSet set, RelTraitSet traits) {
    super(cluster, set, traits);
  }

  public RelOptCost getWinnerCost() {
    if (bestCost == upperBound) {
      return bestCost;
    }
    return null;
  }

  public void startOptimize(RelOptCost ub) {
    state = OptimizeState.OPTIMIZING;
    this.upperBound = ub;
  }

  @Override public CascadeRelSet getSet() {
    return (CascadeRelSet) set;
  }

  public void optimized() {
    state = OptimizeState.OPTIMIZED;
    if (upperBound == null || bestCost.isLt(upperBound)) {
      upperBound = bestCost;
    }
  }

  public boolean resetOptimizing() {
    boolean optimized = state != OptimizeState.NEW;
    state = OptimizeState.NEW;
    upperBound = null;
    return optimized;
  }
}
