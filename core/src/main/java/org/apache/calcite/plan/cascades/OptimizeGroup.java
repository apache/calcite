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
package org.apache.calcite.plan.cascades;

import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/**
 * Task which starts group optimization for a given traits.
 */
class OptimizeGroup extends CascadesTask {
  private final RelGroup group;
  private final double upperBound;

  OptimizeGroup(CascadesTask parentTask, RelGroup group, RelTraitSet traits,
      double upperBound) {
    super(parentTask, upperBound, traits);
    this.group = group;
    this.upperBound = upperBound;
  }

  @Override public void perform() {
    RelSubGroup subGroup = group.getOrCreateSubGroup(group.originalRel().getCluster(),
        requestedTraits);
    if (subGroup != null && subGroup.winnerRel() != null) {
      return; // Group is already optimized for these traits: nothing to do else.
    }

    subGroup.markOptimizationStarted();

    planner.submitTask(new CompleteGroupOptimization(this, group, upperBound, requestedTraits));

    for (RelNode relNode : group.logicalRels()) {
      planner.submitTask(new OptimizeRel(this, requestedTraits, relNode, false));
    }
  }

  @Override protected void description(StringBuilder stringBuilder) {
    stringBuilder
        .append("OptimizeGroup{group=")
        .append(group)
        .append(", traits=")
        .append(requestedTraits)
        .append('}');
  }
}
