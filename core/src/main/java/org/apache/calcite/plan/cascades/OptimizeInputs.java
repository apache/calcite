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
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.calcite.plan.cascades.CascadesUtils.isLogical;

/**
 * Performs inputs optimization for a physical node in order to find the cheapest inputs
 * with desired traits.
 */
class OptimizeInputs extends CascadesTask {
  private final PhysicalNode rel;
  private final double upperBound;
  private final double[] inputsCost;
  private final List<RelNode> inputs;
  private final double rootCost;
  private final RelGroup group;
  private boolean firstExecution = true;
  private int lastHandledInput = 0;
  private int suspendedInput = -1;

  OptimizeInputs(CascadesTask parentTask, RelTraitSet traits, RelNode rel,
      RelGroup group, double upperBound) {
    super(parentTask, upperBound, traits);
    assert !isLogical(rel);
    assert upperBound >= 0;
    this.upperBound  = upperBound;
    this.rel = (PhysicalNode) rel;
    this.group = group;
    inputs = new ArrayList<>(rel.getInputs().size());
    for (int i = 0; i < rel.getInputs().size(); i++) {
      inputs.add(null);
    }
    inputsCost = new double[rel.getInputs().size()];
    Arrays.fill(inputsCost, Double.POSITIVE_INFINITY);
    CascadesCost rootCascadesCost =
        (CascadesCost) rel.getCluster().getMetadataQuery().getNonCumulativeCost(rel);
    rootCost = rootCascadesCost.scalarCost();
  }

  @Override public void perform() {
    if (firstExecution) {
      for (int i = 0; i < rel.getInputs().size(); i++) {
        RelSubGroup child = (RelSubGroup) rel.getInputs().get(i);
        RelNode winner = child.winnerRel();
        if (winner != null) {
          inputsCost[i] = child.winnerCost();
          inputs.set(i, winner);
        }
      }
      firstExecution = false;
    }
    // We have exceeded the upper bound, further optimization does not make sense.
    if (getCostSoFar() > upperBound) {
      return;
    }
    for (int i = lastHandledInput; i < rel.getInputs().size(); i++) {
      if (inputOptimized(i)) {
        continue;
      }
      lastHandledInput = i;
      RelSubGroup input = (RelSubGroup) rel.getInput(i);
      RelNode inputWinner = input.winnerRel();
      if (inputWinner != null) {
        inputsCost[i] = input.winnerCost();
        inputs.set(i, inputWinner);
        if (getCostSoFar() > upperBound) {
          return;
        }
      } else if (input.isOptimizationStarted()) {
        RelNode bestSoFar = input.cheapestSoFar();
        if (bestSoFar == null) {
          return; // TODO enforce properties instead of giving up optimization.
        }
        inputsCost[i] = RelSubGroup.getScalarCost(bestSoFar);
        inputs.set(i, input.cheapestSoFar());
      } else if (i != suspendedInput) { // We optimize this input first time.
        suspendedInput = i;
        // We need to continue calculation for remaining inputs.
        planner.submitTask(this);
        // Optimize input with remaining cost budget.
        double childUpBound = upperBound - getCostSoFar();
        OptimizeGroup task =
            new OptimizeGroup(this, input.getGroup(), input.getTraitSet(), childUpBound);
        planner.submitTask(task);
        return;
      } else {
        // We tried to optimize this input but didn't succeed - terminate the task
        // TODO: why we couldn't succeed?
        return;
      }
    }
    // All inputs should be optimized here.
    assert allInputsOptimized()
        : "inputsCost=" + Arrays.toString(inputsCost) + ", rels=" + rel.getInputs();
    // TODO convert inputs to SubGroups
    List<RelNode> newInputs = new ArrayList<>(inputs.size());
    for (RelNode input : inputs) {
      RelSubGroup subGroup = planner.ensureRegistered(input, null);
      newInputs.add(subGroup);
    }
    PhysicalNode newPhysNode = rel.withNewInputs(newInputs);
    // TODO pruning
    planner.ensureRegistered(newPhysNode, group.originalRel());
    newPhysNode.getCluster().invalidateMetadataQuery();
  }

  private double getCostSoFar() {
    double totalCost = rootCost;
    for (int i = 0; i < inputsCost.length; i++) {
      if (inputOptimized(i)) {
        totalCost += inputsCost[i];
      }
    }
    return totalCost;
  }

  private boolean inputOptimized(int inputNumber) {
    assert inputsCost[inputNumber] >= 0;
    return inputsCost[inputNumber] != Double.POSITIVE_INFINITY;
  }

  private boolean allInputsOptimized() {
    for (int i = 0; i < inputsCost.length; i++) {
      double cost = inputsCost[i];
      if (cost < 0 || cost == Double.POSITIVE_INFINITY) {
        return false;
      }
    }
    return true;
  }

  @Override protected void description(StringBuilder stringBuilder) {
    stringBuilder
        .append("OptimizeInputs{node=")
        .append(rel)
        .append(", inputs=")
        .append(rel.getInputs())
        .append('}');
  }
}
