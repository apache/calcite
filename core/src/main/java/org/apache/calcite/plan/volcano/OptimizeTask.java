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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.PhysicalNode;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.CalciteTrace;

import org.apiguardian.api.API;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <code>OptimizeTask</code> represents the optimization task
 * of VolcanoPlanner.
 */
@API(since = "1.23", status = API.Status.INTERNAL)
abstract class OptimizeTask {

  static final Logger LOGGER = CalciteTrace.getPlannerTaskTracer();

  static OptimizeTask create(RelNode node) {
    if (node instanceof RelSubset) {
      return new OptSubsetTask((RelSubset) node);
    }
    return new OptRelNodeTask(node);
  }

  final VolcanoPlanner planner;
  final int id;

  OptimizeTask(RelNode node) {
    planner = (VolcanoPlanner) node.getCluster().getPlanner();
    id = planner.nextTaskId++;
    LOGGER.debug("Scheduled task(id={}) for {}", id, node);
  }

  abstract boolean hasSubTask();

  abstract OptimizeTask nextSubTask();

  abstract void execute();

  /**
   * Task State
   */
  public enum State {
    SCHEDULED,
    EXECUTING,
    COMPLETED
  }

  /**
   * Task for optimizing RelNode.
   */
  static class OptRelNodeTask extends OptimizeTask {
    final RelNode node;
    int nextId = 0; // next child index

    OptRelNodeTask(RelNode node) {
      super(node);
      this.node = node;
    }

    @Override boolean hasSubTask() {
      int size = node.getInputs().size();
      while (nextId < size) {
        RelSubset subset = (RelSubset) node.getInput(nextId);
        if (subset.taskState == null) {
          // not yet scheduled
          return true;
        } else {
          // maybe a cycle if it is not completed
          nextId++;
        }
      }

      return false;
    }

    @Override OptimizeTask nextSubTask() {
      RelNode child = node.getInput(nextId++);
      return new OptSubsetTask((RelSubset) child);
    }

    @Override void execute() {
      if (!(node instanceof PhysicalNode)
          || ((PhysicalNode) node).getDeriveMode() == DeriveMode.PROHIBITED
          || !planner.isSeedNode(node)) {
        LOGGER.debug("Completed task(id={}) for {}", id, node);
        return;
      }

      PhysicalNode rel = (PhysicalNode) node;
      DeriveMode mode = rel.getDeriveMode();
      int arity = node.getInputs().size();
      // for OMAKASE
      List<List<RelTraitSet>> inputTraits = new ArrayList<>(arity);

      for (int i = 0; i < arity; i++) {
        int childId = i;
        if (mode == DeriveMode.RIGHT_FIRST) {
          childId = arity - i - 1;
        }

        RelSubset input = (RelSubset) node.getInput(childId);
        RelTraitSet required = input.getTraitSet();
        List<RelTraitSet> traits = new ArrayList<>();
        inputTraits.add(traits);

        final int numSubset = input.set.subsets.size();
        for (int j = 0; j < numSubset; j++) {
          RelSubset subset = input.set.subsets.get(j);
          if (!subset.isDelivered()
              // TODO: should use matching type to determine
              || required.equals(subset.getTraitSet())) {
            continue;
          }

          if (mode == DeriveMode.OMAKASE) {
            traits.add(subset.getTraitSet());
          } else {
            RelNode newRel = rel.derive(subset.getTraitSet(), childId);
            if (newRel != null) {
              RelSubset relSubset = planner.register(newRel, node);
              assert relSubset.set == planner.getSubset(node).set;
            }
          }
        }

        if (mode == DeriveMode.LEFT_FIRST
            || mode == DeriveMode.RIGHT_FIRST) {
          break;
        }
      }

      if (mode == DeriveMode.OMAKASE) {
        List<RelNode> relList = rel.derive(inputTraits);
        for (RelNode relNode : relList) {
          planner.register(relNode, node);
        }
      }

      LOGGER.debug("Completed task(id={}) for {}", id, node);
    }

    @Override public String toString() {
      return "#" + id + ":OptRelNodeTask{ " + node + " }";
    }
  }

  /**
   * Task for optimizing RelSubset.
   */
  static class OptSubsetTask extends OptimizeTask {
    final RelSubset subset;
    final Map<RelTraitSet, RelSubset> derivedSubsets = new HashMap<>();

    OptSubsetTask(RelSubset subset) {
      super(subset);
      this.subset = subset;
      subset.taskState = State.SCHEDULED;
      propagateTraits();
    }

    private void propagateTraits() {
      int size = subset.set.getSeedSize();

      for (int i = 0; i < size; i++) {
        RelNode rel = subset.set.rels.get(i);
        if (!(rel instanceof PhysicalNode)
            || rel.getConvention() == Convention.NONE
            || rel.getTraitSet().satisfies(subset.getTraitSet())) {
          continue;
        }

        RelNode node = ((PhysicalNode) rel).passThrough(
            subset.getTraitSet());
        if (node != null) {
          RelSubset newSubset = planner.register(node, subset);
          derivedSubsets.put(newSubset.getTraitSet(), newSubset);
        } else {
          // TODO: should we consider stop trying propagation on node
          // with the same traitset as phyNode?
          assert true;
        }
      }
    }

    @Override boolean hasSubTask() {
      return subset.set.hasNextPhysicalNode();
    }

    @Override OptimizeTask nextSubTask() {
      RelNode rel = subset.set.nextPhysicalNode();
      return new OptRelNodeTask(rel);
    }

    @Override void execute() {
      subset.taskState = State.EXECUTING;

      subset.set.addConverters(subset, true, false);

      for (RelSubset derived : derivedSubsets.values()) {
        subset.set.addConverters(derived, false, false);
      }

      subset.taskState = State.COMPLETED;
      LOGGER.debug("Completed task(id={}) for {}", id, subset);
    }

    @Override public String toString() {
      return "#" + id + ":OptSubsetTask{ " + subset + " }";
    }
  }
}
