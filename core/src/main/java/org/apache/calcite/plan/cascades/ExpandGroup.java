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

import org.apache.calcite.rel.RelNode;

/**
 * Carries out the logical exploration of the group.
 */
public class ExpandGroup extends CascadesTask {
  private final RelGroup group;

  public ExpandGroup(CascadesTask parentTask, RelGroup group) {
    super(parentTask);
    this.group = group;
  }

  @Override public void perform() {
    if (group.isExpanded()) {
      return;
    }

    for (RelNode relNode : group.logicalRels()) {
      OptimizeRel task =
          new OptimizeRel(this, planner.emptyTraitSet(), relNode, true);
      planner.submitTask(task);
    }

    group.markExpanded();
  }

  @Override protected void description(StringBuilder stringBuilder) {
    stringBuilder
        .append("ExpandGroup{group=")
        .append(group)
        .append('}');
  }
}
