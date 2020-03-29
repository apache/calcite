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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.calcite.plan.cascades.CascadesUtils.isLogical;

/**
 * Group of logically equivalent nodes with a similar traits.
 */
public class RelSubGroup extends AbstractRelNode {

  private final RelGroup group;

  private PhysicalNode winner;

  private double winnerCost = Double.POSITIVE_INFINITY;

  private PhysicalNode cheapestSoFar;

  private CascadesCost cheapestCostSoFar;

  private boolean optimizationStarted;

  public RelSubGroup(RelOptCluster cluster, RelTraitSet traitSet, RelGroup group) {
    super(cluster, traitSet);
    this.group = group;
    recomputeDigest();
  }

  public RelGroup getGroup() {
    return group;
  }

  public double winnerCost() {
    return winnerCost;
  }

  public PhysicalNode winnerRel() {
    return winner;
  }

  public void onRelAdded(RelNode rel) {
    if (isLogical(rel)) {
      return;
    }

    CascadesCost curCost = getCost(rel);
    if (cheapestCostSoFar == null || curCost.scalarCost() < cheapestCostSoFar.scalarCost()) {
      // TODO winner cost assertions
      cheapestCostSoFar = curCost;
      cheapestSoFar = (PhysicalNode) rel;
    }
  }

  public PhysicalNode cheapestSoFar() {
    return cheapestSoFar;
  }

  public CascadesCost cheapestCostSoFar() {
    return cheapestCostSoFar;
  }

  public void updateWinnerIfNeeded(PhysicalNode rel) {
    double cost = getScalarCost(rel);
    if (rel.getTraitSet().satisfies(traitSet) && winnerCost > cost) {
      winner = rel;
      winnerCost = cost;
    }
  }

  public static double getScalarCost(RelNode rel) {
    return getCost(rel).scalarCost();
  }

  public static CascadesCost getCost(RelNode rel) {
    RelOptCluster cluster = rel.getCluster();
    CascadesCost cascadesCost =
        (CascadesCost) cluster.getPlanner().getCost(rel, cluster.getMetadataQuery());
    return cascadesCost;
  }

  public Set<RelNode> getRels() {
    Set<RelNode> all = isLogical(this) ? group.logicalRels() : group.physicalRels();
    Set<RelNode> rels = new HashSet<>(all.size());
    for (RelNode relNode : all) {
      if (relNode.getTraitSet().satisfies(traitSet)) {
        rels.add(relNode);
      }
    }
    return rels;
  }

  @Override protected String computeDigest() {
    StringBuilder digest = new StringBuilder("SubGroup#");
    digest.append(group.getId());
    for (RelTrait trait : traitSet) {
      digest.append('.').append(trait);
    }
    return digest.toString();
  }

  @Override public void explain(RelWriter pw) {
    pw.item("subGroup", toString());
    final AbstractRelNode input =
        (AbstractRelNode) (getOriginal());
    if (input == null) {
      return;
    }
    input.explainTerms(pw);
    pw.done(input);
  }

  @Override protected RelDataType deriveRowType() {
    return group.rowType();
  }

  public RelNode getOriginal() {
    return group.originalRel();
  }

  public boolean isOptimizationStarted() {
    return optimizationStarted;
  }

  public void markOptimizationStarted() {
    this.optimizationStarted = true;
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    return planner.getCostFactory().makeZeroCost();
  }

  /**
   * Recursively builds a tree consisting of the cheapest plan at each node.
   */
  RelNode buildCheapestPlan(CascadesPlanner planner) {
    CheapestPlanReplacer replacer = new CheapestPlanReplacer(planner);
    final RelNode cheapest = replacer.visit(this, -1, null);

//    if (planner.listener != null) { TODO listeners?
//      RelOptListener.RelChosenEvent event =
//          new RelOptListener.RelChosenEvent(
//              planner,
//              null);
//      planner.listener.relChosen(event);
//    }

    return cheapest;
  }


  /**
   * Identifies the leaf-most non-implementable nodes.
   */
  static class DeadEndFinder {
    final Set<RelSubGroup> deadEnds = new HashSet<>();
    // To save time
    private final Set<RelNode> visitedNodes = new HashSet<>();
    // For cycle detection
    private final Set<RelNode> activeNodes = new HashSet<>();

    private boolean visit(RelNode p) {
      if (p instanceof RelSubGroup) {
        visitSubset((RelSubGroup) p);
        return false;
      }
      return visitRel(p);
    }

    private void visitSubset(RelSubGroup subset) {
      RelNode cheapest = subset.cheapestSoFar();
      if (cheapest != null) {
        // Subset is implementable, and we are looking for bad ones, so stop here
        return;
      }

      boolean isEmpty = true;
      for (RelNode rel : subset.getRels()) {
        if (rel instanceof AbstractConverter) {
          // Converters are not implementable
          continue;
        }
        if (!activeNodes.add(rel)) {
          continue;
        }
        boolean res = visit(rel);
        isEmpty &= res;
        activeNodes.remove(rel);
      }
      if (isEmpty) {
        deadEnds.add(subset);
      }
    }

    /**
     * Returns true when input {@code RelNode} is cyclic.
     */
    private boolean visitRel(RelNode p) {
      // If one of the inputs is in "active" set, that means the rel forms a cycle,
      // then we just ignore it. Cyclic rels are not implementable.
      for (RelNode oldInput : p.getInputs()) {
        if (activeNodes.contains(oldInput)) {
          return true;
        }
      }
      // The same subset can be used multiple times (e.g. union all with the same inputs),
      // so it is important to perform "contains" and "add" in different loops
      activeNodes.addAll(p.getInputs());
      for (RelNode oldInput : p.getInputs()) {
        if (!visitedNodes.add(oldInput)) {
          // We don't want to explore the same subset twice
          continue;
        }
        visit(oldInput);
      }
      activeNodes.removeAll(p.getInputs());
      return false;
    }
  }

  /**
   * Visitor which walks over a tree of {@link RelSubGroup}s, replacing each node
   * with the cheapest implementation of the expression.
   */
  static class CheapestPlanReplacer {
    CascadesPlanner planner;

    CheapestPlanReplacer(CascadesPlanner planner) {
      super();
      this.planner = planner;
    }

    private static String traitDiff(RelTraitSet original, RelTraitSet desired) {
      return Pair.zip(original, desired)
          .stream()
          .filter(p -> !p.left.satisfies(p.right))
          .map(p -> p.left.getTraitDef().getSimpleName() + ": " + p.left + " -> " + p.right)
          .collect(Collectors.joining(", ", "[", "]"));
    }

    public RelNode visit(
        RelNode p,
        int ordinal,
        RelNode parent) {
      if (p instanceof RelSubGroup) {
        RelSubGroup subGroup = (RelSubGroup) p;
        RelNode cheapest = subGroup.cheapestSoFar();
        if (cheapest == null) {
          // Dump the planner's expression pool so we can figure
          // out why we reached impasse.
          StringWriter sw = new StringWriter();
          final PrintWriter pw = new PrintWriter(sw);

          pw.print("There are not enough rules to produce a node with desired properties");
          RelTraitSet desiredTraits = subGroup.getTraitSet();
          String sep = ": ";
          for (RelTrait trait : desiredTraits) {
            pw.print(sep);
            pw.print(trait.getTraitDef().getSimpleName());
            pw.print("=");
            pw.print(trait);
            sep = ", ";
          }
          pw.print(".");
          DeadEndFinder finder = new DeadEndFinder();
          finder.visit(subGroup);
          if (finder.deadEnds.isEmpty()) {
            pw.print(" All the inputs have relevant nodes, however the cost is still infinite.");
          } else {
            Map<String, Long> problemCounts =
                finder.deadEnds.stream()
                    .filter(deadSubset -> deadSubset.getOriginal() != null)
                    .map(x -> x.getOriginal().getClass().getSimpleName()
                        + traitDiff(x.getOriginal().getTraitSet(), x.getTraitSet()))
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            // Sort problems from most often to less often ones
            String problems = problemCounts.entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry<String, Long>::getValue).reversed())
                .map(e -> e.getKey() + (e.getValue() > 1 ? " (" + e.getValue() + " cases)" : ""))
                .collect(Collectors.joining(", "));
            pw.println();
            pw.print("Missing conversion");
            pw.print(finder.deadEnds.size() == 1 ? " is " : "s are ");
            pw.print(problems);
            pw.println();
            if (finder.deadEnds.size() == 1) {
              pw.print("There is 1 empty subGroup: ");
            }
            if (finder.deadEnds.size() > 1) {
              pw.println("There are " + finder.deadEnds.size() + " empty subsets:");
            }
            int i = 0;
            int rest = finder.deadEnds.size();
            for (RelSubGroup deadEnd : finder.deadEnds) {
              if (finder.deadEnds.size() > 1) {
                pw.print("Empty subGroup ");
                pw.print(i);
                pw.print(": ");
              }
              pw.print(deadEnd);
              pw.println(", the relevant part of the original plan is as follows");
              RelNode original = deadEnd.getOriginal();
              original.explain(
                  new RelWriterImpl(pw, SqlExplainLevel.EXPPLAN_ATTRIBUTES, true));
              i++;
              rest--;
              if (rest > 0) {
                pw.println();
              }
              if (i >= 10 && rest > 1) {
                pw.print("The rest ");
                pw.print(rest);
                pw.println(" leafs are omitted.");
                break;
              }
            }
          }
          pw.println();

          planner.dump(pw);
          pw.flush();
          final String dump = sw.toString();
          RuntimeException e =
              new RelOptPlanner.CannotPlanException(dump);
          // TODO logging
          //LOGGER.trace("Caught exception in class={}, method=visit", getClass().getName(), e);
          throw e;
        }
        p = cheapest;
      }

//      if (ordinal != -1) { TODO
//        if (planner.listener != null) {
//          RelOptListener.RelChosenEvent event =
//              new RelOptListener.RelChosenEvent(
//                  planner,
//                  p);
//          planner.listener.relChosen(event);
//        }
//      }

      List<RelNode> oldInputs = p.getInputs();
      List<RelNode> inputs = new ArrayList<>();
      for (int i = 0; i < oldInputs.size(); i++) {
        RelNode oldInput = oldInputs.get(i);
        RelNode input = visit(oldInput, i, p);
        inputs.add(input);
      }
      if (!inputs.equals(oldInputs)) {
        final RelNode pOld = p;
        p = p.copy(p.getTraitSet(), inputs);
//        planner.provenanceMap.put( TODO
//            p, new VolcanoPlanner.DirectProvenance(pOld));
      }
      return p;
    }
  }
}
