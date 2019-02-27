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

import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Subset of an equivalence class where all relational expressions have the
 * same physical properties.
 *
 * <p>Physical properties are instances of the {@link RelTraitSet}, and consist
 * of traits such as calling convention and collation (sort-order).
 *
 * <p>For some traits, a relational expression can have more than one instance.
 * For example, R can be sorted on both [X] and [Y, Z]. In which case, R would
 * belong to the sub-sets for [X] and [Y, Z]; and also the leading edges [Y] and
 * [].
 *
 * @see RelNode
 * @see RelSet
 * @see RelTrait
 */
public class RelSubset extends AbstractRelNode {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  //~ Instance fields --------------------------------------------------------

  /**
   * cost of best known plan (it may have improved since)
   */
  RelOptCost bestCost;

  /**
   * The set this subset belongs to.
   */
  final RelSet set;

  /**
   * best known plan
   */
  RelNode best;

  /**
   * Timestamp for metadata validity
   */
  long timestamp;

  /**
   * Flag indicating whether this RelSubset's importance was artificially
   * boosted.
   */
  boolean boosted;

  //~ Constructors -----------------------------------------------------------

  RelSubset(
      RelOptCluster cluster,
      RelSet set,
      RelTraitSet traits) {
    super(cluster, traits);
    this.set = set;
    this.boosted = false;
    assert traits.allSimple();
    computeBestCost(cluster.getPlanner());
    recomputeDigest();
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Computes the best {@link RelNode} in this subset.
   *
   * <p>Only necessary when a subset is created in a set that has subsets that
   * subsume it. Rationale:</p>
   *
   * <ol>
   * <li>If the are no subsuming subsets, the subset is initially empty.</li>
   * <li>After creation, {@code best} and {@code bestCost} are maintained
   *    incrementally by {@link #propagateCostImprovements0} and
   *    {@link RelSet#mergeWith(VolcanoPlanner, RelSet)}.</li>
   * </ol>
   */
  private void computeBestCost(RelOptPlanner planner) {
    bestCost = planner.getCostFactory().makeInfiniteCost();
    final RelMetadataQuery mq = getCluster().getMetadataQuery();
    for (RelNode rel : getRels()) {
      final RelOptCost cost = planner.getCost(rel, mq);
      if (cost.isLt(bestCost)) {
        bestCost = cost;
        best = rel;
      }
    }
  }

  public RelNode getBest() {
    return best;
  }

  public RelNode getOriginal() {
    return set.rel;
  }

  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    if (inputs.isEmpty()) {
      final RelTraitSet traitSet1 = traitSet.simplify();
      if (traitSet1.equals(this.traitSet)) {
        return this;
      }
      return set.getOrCreateSubset(getCluster(), traitSet1);
    }
    throw new UnsupportedOperationException();
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeZeroCost();
  }

  public double estimateRowCount(RelMetadataQuery mq) {
    if (best != null) {
      return mq.getRowCount(best);
    } else {
      return mq.getRowCount(set.rel);
    }
  }

  @Override public void explain(RelWriter pw) {
    // Not a typical implementation of "explain". We don't gather terms &
    // values to be printed later. We actually do the work.
    String s = getDescription();
    pw.item("subset", s);
    final AbstractRelNode input =
        (AbstractRelNode) Util.first(getBest(), getOriginal());
    if (input == null) {
      return;
    }
    input.explainTerms(pw);
    pw.done(input);
  }

  @Override protected String computeDigest() {
    StringBuilder digest = new StringBuilder("Subset#");
    digest.append(set.id);
    for (RelTrait trait : traitSet) {
      digest.append('.').append(trait);
    }
    return digest.toString();
  }

  @Override protected RelDataType deriveRowType() {
    return set.rel.getRowType();
  }

  /**
   * Returns the collection of RelNodes one of whose inputs is in this
   * subset.
   */
  Set<RelNode> getParents() {
    final Set<RelNode> list = new LinkedHashSet<>();
    for (RelNode parent : set.getParentRels()) {
      for (RelSubset rel : inputSubsets(parent)) {
        if (rel.set == set && traitSet.satisfies(rel.getTraitSet())) {
          list.add(parent);
        }
      }
    }
    return list;
  }

  /**
   * Returns the collection of distinct subsets that contain a RelNode one
   * of whose inputs is in this subset.
   */
  Set<RelSubset> getParentSubsets(VolcanoPlanner planner) {
    final Set<RelSubset> list = new LinkedHashSet<>();
    for (RelNode parent : set.getParentRels()) {
      for (RelSubset rel : inputSubsets(parent)) {
        if (rel.set == set && rel.getTraitSet().equals(traitSet)) {
          list.add(planner.getSubset(parent));
        }
      }
    }
    return list;
  }

  private static List<RelSubset> inputSubsets(RelNode parent) {
    //noinspection unchecked
    return (List<RelSubset>) (List) parent.getInputs();
  }

  /**
   * Returns a list of relational expressions one of whose children is this
   * subset. The elements of the list are distinct.
   */
  public Collection<RelNode> getParentRels() {
    final Set<RelNode> list = new LinkedHashSet<>();
  parentLoop:
    for (RelNode parent : set.getParentRels()) {
      for (RelSubset rel : inputSubsets(parent)) {
        if (rel.set == set && traitSet.satisfies(rel.getTraitSet())) {
          list.add(parent);
          continue parentLoop;
        }
      }
    }
    return list;
  }

  RelSet getSet() {
    return set;
  }

  /**
   * Adds expression <code>rel</code> to this subset.
   */
  void add(RelNode rel) {
    if (set.rels.contains(rel)) {
      return;
    }

    VolcanoPlanner planner = (VolcanoPlanner) rel.getCluster().getPlanner();
    if (planner.listener != null) {
      RelOptListener.RelEquivalenceEvent event =
          new RelOptListener.RelEquivalenceEvent(
              planner,
              rel,
              this,
              true);
      planner.listener.relEquivalenceFound(event);
    }

    // If this isn't the first rel in the set, it must have compatible
    // row type.
    if (set.rel != null) {
      RelOptUtil.equal("rowtype of new rel", rel.getRowType(),
          "rowtype of set", getRowType(), Litmus.THROW);
    }
    set.addInternal(rel);
    if (false) {
      Set<CorrelationId> variablesSet = RelOptUtil.getVariablesSet(rel);
      Set<CorrelationId> variablesStopped = rel.getVariablesSet();
      Set<CorrelationId> variablesPropagated =
          Util.minus(variablesSet, variablesStopped);
      assert set.variablesPropagated.containsAll(variablesPropagated);
      Set<CorrelationId> variablesUsed = RelOptUtil.getVariablesUsed(rel);
      assert set.variablesUsed.containsAll(variablesUsed);
    }
  }

  /**
   * Recursively builds a tree consisting of the cheapest plan at each node.
   */
  RelNode buildCheapestPlan(VolcanoPlanner planner) {
    CheapestPlanReplacer replacer = new CheapestPlanReplacer(planner);
    final RelNode cheapest = replacer.visit(this, -1, null);

    if (planner.listener != null) {
      RelOptListener.RelChosenEvent event =
          new RelOptListener.RelChosenEvent(
              planner,
              null);
      planner.listener.relChosen(event);
    }

    return cheapest;
  }

  /**
   * Checks whether a relexp has made its subset cheaper, and if it so,
   * recursively checks whether that subset's parents have gotten cheaper.
   *
   * @param planner   Planner
   * @param mq        Metadata query
   * @param rel       Relational expression whose cost has improved
   * @param activeSet Set of active subsets, for cycle detection
   */
  void propagateCostImprovements(VolcanoPlanner planner, RelMetadataQuery mq,
      RelNode rel, Set<RelSubset> activeSet) {
    for (RelSubset subset : set.subsets) {
      if (rel.getTraitSet().satisfies(subset.traitSet)) {
        subset.propagateCostImprovements0(planner, mq, rel, activeSet);
      }
    }
  }

  void propagateCostImprovements0(VolcanoPlanner planner, RelMetadataQuery mq,
      RelNode rel, Set<RelSubset> activeSet) {
    ++timestamp;

    if (!activeSet.add(this)) {
      // This subset is already in the chain being propagated to. This
      // means that the graph is cyclic, and therefore the cost of this
      // relational expression - not this subset - must be infinite.
      LOGGER.trace("cyclic: {}", this);
      return;
    }
    try {
      final RelOptCost cost = planner.getCost(rel, mq);
      if (cost.isLt(bestCost)) {
        LOGGER.trace("Subset cost improved: subset [{}] cost was {} now {}", this, bestCost, cost);

        bestCost = cost;
        best = rel;

        // Lower cost means lower importance. Other nodes will change
        // too, but we'll get to them later.
        planner.ruleQueue.recompute(this);
        for (RelNode parent : getParents()) {
          final RelSubset parentSubset = planner.getSubset(parent);
          parentSubset.propagateCostImprovements(planner, mq, parent,
              activeSet);
        }
        planner.checkForSatisfiedConverters(set, rel);
      }
    } finally {
      activeSet.remove(this);
    }
  }

  public void propagateBoostRemoval(VolcanoPlanner planner) {
    planner.ruleQueue.recompute(this);

    if (boosted) {
      boosted = false;

      for (RelSubset parentSubset : getParentSubsets(planner)) {
        parentSubset.propagateBoostRemoval(planner);
      }
    }
  }

  @Override public void collectVariablesUsed(Set<CorrelationId> variableSet) {
    variableSet.addAll(set.variablesUsed);
  }

  @Override public void collectVariablesSet(Set<CorrelationId> variableSet) {
    variableSet.addAll(set.variablesPropagated);
  }

  /**
   * Returns the rel nodes in this rel subset.  All rels must have the same
   * traits and are logically equivalent.
   *
   * @return all the rels in the subset
   */
  public Iterable<RelNode> getRels() {
    return () -> Linq4j.asEnumerable(set.rels)
        .where(v1 -> v1.getTraitSet().satisfies(traitSet))
        .iterator();
  }

  /**
   * As {@link #getRels()} but returns a list.
   */
  public List<RelNode> getRelList() {
    final List<RelNode> list = new ArrayList<>();
    for (RelNode rel : set.rels) {
      if (rel.getTraitSet().satisfies(traitSet)) {
        list.add(rel);
      }
    }
    return list;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Identifies the leaf-most non-implementable nodes.
   */
  static class DeadEndFinder {
    final Set<RelSubset> deadEnds = new HashSet<>();
    // To save time
    private final Set<RelNode> visitedNodes = new HashSet<>();
    // For cycle detection
    private final Set<RelNode> activeNodes = new HashSet<>();

    private boolean visit(RelNode p) {
      if (p instanceof RelSubset) {
        visitSubset((RelSubset) p);
        return false;
      }
      return visitRel(p);
    }

    private void visitSubset(RelSubset subset) {
      RelNode cheapest = subset.getBest();
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
   * Visitor which walks over a tree of {@link RelSet}s, replacing each node
   * with the cheapest implementation of the expression.
   */
  static class CheapestPlanReplacer {
    VolcanoPlanner planner;

    CheapestPlanReplacer(VolcanoPlanner planner) {
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
      if (p instanceof RelSubset) {
        RelSubset subset = (RelSubset) p;
        RelNode cheapest = subset.best;
        if (cheapest == null) {
          // Dump the planner's expression pool so we can figure
          // out why we reached impasse.
          StringWriter sw = new StringWriter();
          final PrintWriter pw = new PrintWriter(sw);

          pw.print("There are not enough rules to produce a node with desired properties");
          RelTraitSet desiredTraits = subset.getTraitSet();
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
          finder.visit(subset);
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
              pw.print("There is 1 empty subset: ");
            }
            if (finder.deadEnds.size() > 1) {
              pw.println("There are " + finder.deadEnds.size() + " empty subsets:");
            }
            int i = 0;
            int rest = finder.deadEnds.size();
            for (RelSubset deadEnd : finder.deadEnds) {
              if (finder.deadEnds.size() > 1) {
                pw.print("Empty subset ");
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
          LOGGER.trace("Caught exception in class={}, method=visit", getClass().getName(), e);
          throw e;
        }
        p = cheapest;
      }

      if (ordinal != -1) {
        if (planner.listener != null) {
          RelOptListener.RelChosenEvent event =
              new RelOptListener.RelChosenEvent(
                  planner,
                  p);
          planner.listener.relChosen(event);
        }
      }

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
        planner.provenanceMap.put(
            p, new VolcanoPlanner.DirectProvenance(pOld));
      }
      return p;
    }
  }
}

// End RelSubset.java
