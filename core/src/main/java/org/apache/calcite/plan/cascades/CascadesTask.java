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

import java.util.Collections;

/**
 * Basic class for all {@link CascadesPlanner} optimization tasks.
 */
abstract class CascadesTask {
  protected final CascadesPlanner planner;
  protected final CascadesTask parentTask;
  protected final RelTraitSet requestedTraits;
  protected final double upperBound;
  protected final int depth;

  private CascadesTask(CascadesPlanner planner) {
    this.planner = planner;
    this.parentTask = null;
    this.upperBound = Double.POSITIVE_INFINITY;
    this.depth = -1;
    requestedTraits = null;
  }

  CascadesTask(CascadesTask parentTask) {
    this.planner = parentTask.planner;
    this.parentTask = parentTask;
    this.upperBound = parentTask.upperBound;
    this.depth = parentTask.depth + 1;
    this.requestedTraits = parentTask.requestedTraits;
  }

  CascadesTask(CascadesTask parentTask, double newUpperBound, RelTraitSet requestedTraits) {
    this.planner = parentTask.planner;
    this.parentTask = parentTask;
    this.upperBound = newUpperBound;
    this.depth = parentTask.depth + 1;
    this.requestedTraits = requestedTraits;
  }

  abstract void perform();

  protected abstract void description(StringBuilder stringBuilder);

  private void fullDescription(StringBuilder stringBuilder) {
    if (parentTask != null) {
      parentTask.fullDescription(stringBuilder);
    }
    stringBuilder.append("\n")
        .append(String.join("", Collections.nCopies(depth + 1, ".")));
    description(stringBuilder);
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    description(sb);
    return sb.toString();
  }

  public String fullToString() {
    StringBuilder sb = new StringBuilder();
    fullDescription(sb);
    return sb.toString();
  }


  static class InitialTask extends CascadesTask {
    private final RelGroup group;
    private final RelTraitSet traits;

    InitialTask(CascadesPlanner planner, RelGroup group, RelTraitSet traits) {
      super(planner);
      this.group = group;
      this.traits = traits;
    }

    @Override void perform() {
      OptimizeGroup task = new OptimizeGroup(this, group, traits, Double.POSITIVE_INFINITY);
      planner.submitTask(task);
    }

    @Override protected void description(StringBuilder stringBuilder) {
      stringBuilder
          .append("InitialTask{group=")
          .append(group)
          .append(", traits=")
          .append(traits)
          .append('}');
    }
  }
}
