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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.CorrelationId;

import java.util.Set;

/***
 * An implementation of RelSet for CascadePlanner. Holding the exploring state.
 */
public class CascadeRelSet extends RelSet {

  /**
   * An enum representing exploring state of current RelSet
   */
  public enum ExploreState {
    /**
     * The group is not yet explored
     */
    NEW,

    /**
     * The group is exploring.
     * It means all possible rule matches are scheduled, but not fully applied.
     * This group will refuse to explore again, but cannot provide a valid LB.
     */
    EXPLORING,

    /**
     * The group is fully explored and is able to provide a valid LB.
     */
    EXPLORED
  }

  ExploreState state = ExploreState.NEW;

  CascadeRelSet(int id, Set<CorrelationId> variablesPropagated,
      Set<CorrelationId> variablesUsed) {
    super(id, variablesPropagated, variablesUsed);
  }

  @Override protected RelSubset newRelSubset(RelOptCluster cluster,
      RelSet relSet, RelTraitSet traits) {
    return new CascadeRelSubset(cluster, relSet, traits);
  }

  public ExploreState getState() {
    return state;
  }

  @Override RelSubset getOrCreateSubset(
      RelOptCluster cluster, RelTraitSet traits, boolean require) {
    // always add converters eagerly
    return getOrCreateSubset(cluster, traits, require, true);
  }

  @Override void mergeWith(VolcanoPlanner planner, RelSet otherSet) {
    super.mergeWith(planner, otherSet);
    for (RelSubset subset : otherSet.subsets) {
      CascadeRelSubset cascadeRelSubset = (CascadeRelSubset) subset;
      CascadeRelSubset newSubset = (CascadeRelSubset) getSubset(subset.getTraitSet());
      if (newSubset.passThroughCache == null) {
        newSubset.passThroughCache = cascadeRelSubset.passThroughCache;
      } else if (cascadeRelSubset.passThroughCache != null) {
        newSubset.passThroughCache.addAll(cascadeRelSubset.passThroughCache);
      }
    }
  }
}
