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
package org.apache.calcite.plan.hep;

import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.CommonRelSubExprRule;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.Converter;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.convert.TraitMatchingRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.graph.BreadthFirstIterator;
import org.apache.calcite.util.graph.CycleDetector;
import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DepthFirstIterator;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.Graphs;
import org.apache.calcite.util.graph.TopologicalOrderIterator;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * HepPlanner is a heuristic implementation of the {@link RelOptPlanner}
 * interface.
 */
public class HepPlanner extends AbstractRelOptPlanner {
  //~ Instance fields --------------------------------------------------------

  private HepProgram mainProgram;

  private HepProgram currentProgram;

  private HepRelVertex root;

  private RelTraitSet requestedRootTraits;

  private Map<String, HepRelVertex> mapDigestToVertex;

  private final Set<RelOptRule> allRules;

  private int nTransformations;

  private int graphSizeLastGC;

  private int nTransformationsLastGC;

  private boolean noDAG;

  /**
   * Query graph, with edges directed from parent to child. This is a
   * single-rooted DAG, possibly with additional roots corresponding to
   * discarded plan fragments which remain to be garbage-collected.
   */
  private DirectedGraph<HepRelVertex, DefaultEdge> graph;

  private final Function2<RelNode, RelNode, Void> onCopyHook;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a new HepPlanner that allows DAG.
   *
   * @param program program controlling rule application
   */
  public HepPlanner(HepProgram program) {
    this(program, null, false, null, RelOptCostImpl.FACTORY);
  }

  /**
   * Creates a new HepPlanner that allows DAG.
   *
   * @param program program controlling rule application
   * @param context to carry while planning
   */
  public HepPlanner(HepProgram program, Context context) {
    this(program, context, false, null, RelOptCostImpl.FACTORY);
  }

  /**
   * Creates a new HepPlanner with the option to keep the graph a
   * tree(noDAG=true) or allow DAG(noDAG=false).
   *
   * @param program    program controlling rule application
   * @param onCopyHook Function to call when a node is copied
   */
  public HepPlanner(
      HepProgram program,
      Context context,
      boolean noDAG,
      Function2<RelNode, RelNode, Void> onCopyHook,
      RelOptCostFactory costFactory) {
    super(costFactory, context);
    this.mainProgram = program;
    this.onCopyHook =
        Util.first(onCopyHook, Functions.<RelNode, RelNode, Void>ignore2());
    mapDigestToVertex = new HashMap<>();
    graph = DefaultDirectedGraph.create();

    // NOTE jvs 24-Apr-2006:  We use LinkedHashSet here and below
    // in order to provide deterministic behavior.
    allRules = new LinkedHashSet<>();
    this.noDAG = noDAG;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptPlanner
  public void setRoot(RelNode rel) {
    root = addRelToGraph(rel);
    dumpGraph();
  }

  // implement RelOptPlanner
  public RelNode getRoot() {
    return root;
  }

  public List<RelOptRule> getRules() {
    return ImmutableList.copyOf(allRules);
  }

  // implement RelOptPlanner
  public boolean addRule(RelOptRule rule) {
    boolean added = allRules.add(rule);
    if (added) {
      mapRuleDescription(rule);
    }
    return added;
  }

  @Override public void clear() {
    super.clear();
    for (RelOptRule rule : ImmutableList.copyOf(allRules)) {
      removeRule(rule);
    }
  }

  public boolean removeRule(RelOptRule rule) {
    unmapRuleDescription(rule);
    return allRules.remove(rule);
  }

  // implement RelOptPlanner
  public RelNode changeTraits(RelNode rel, RelTraitSet toTraits) {
    // Ignore traits, except for the root, where we remember
    // what the final conversion should be.
    if ((rel == root) || (rel == root.getCurrentRel())) {
      requestedRootTraits = toTraits;
    }
    return rel;
  }

  // implement RelOptPlanner
  public RelNode findBestExp() {
    assert root != null;

    executeProgram(mainProgram);

    // Get rid of everything except what's in the final plan.
    collectGarbage();

    return buildFinalPlan(root);
  }

  private void executeProgram(HepProgram program) {
    HepProgram savedProgram = currentProgram;
    currentProgram = program;
    currentProgram.initialize(program == mainProgram);
    for (HepInstruction instruction : currentProgram.instructions) {
      instruction.execute(this);
      int delta = nTransformations - nTransformationsLastGC;
      if (delta > graphSizeLastGC) {
        // The number of transformations performed since the last
        // garbage collection is greater than the number of vertices in
        // the graph at that time.  That means there should be a
        // reasonable amount of garbage to collect now.  We do it this
        // way to amortize garbage collection cost over multiple
        // instructions, while keeping the highwater memory usage
        // proportional to the graph size.
        collectGarbage();
      }
    }
    currentProgram = savedProgram;
  }

  void executeInstruction(
      HepInstruction.MatchLimit instruction) {
    LOGGER.trace("Setting match limit to {}", instruction.limit);
    currentProgram.matchLimit = instruction.limit;
  }

  void executeInstruction(
      HepInstruction.MatchOrder instruction) {
    LOGGER.trace("Setting match limit to {}", instruction.order);
    currentProgram.matchOrder = instruction.order;
  }

  void executeInstruction(
      HepInstruction.RuleInstance instruction) {
    if (skippingGroup()) {
      return;
    }
    if (instruction.rule == null) {
      assert instruction.ruleDescription != null;
      instruction.rule =
          getRuleByDescription(instruction.ruleDescription);
      LOGGER.trace("Looking up rule with description {}, found {}",
          instruction.ruleDescription, instruction.rule);
    }
    if (instruction.rule != null) {
      applyRules(
          Collections.singleton(instruction.rule),
          true);
    }
  }

  void executeInstruction(
      HepInstruction.RuleClass<?> instruction) {
    if (skippingGroup()) {
      return;
    }
    LOGGER.trace("Applying rule class {}", instruction.ruleClass);
    if (instruction.ruleSet == null) {
      instruction.ruleSet = new LinkedHashSet<>();
      for (RelOptRule rule : allRules) {
        if (instruction.ruleClass.isInstance(rule)) {
          instruction.ruleSet.add(rule);
        }
      }
    }
    applyRules(instruction.ruleSet, true);
  }

  void executeInstruction(
      HepInstruction.RuleCollection instruction) {
    if (skippingGroup()) {
      return;
    }
    applyRules(instruction.rules, true);
  }

  private boolean skippingGroup() {
    if (currentProgram.group != null) {
      // Skip if we've already collected the ruleset.
      return !currentProgram.group.collecting;
    } else {
      // Not grouping.
      return false;
    }
  }

  void executeInstruction(
      HepInstruction.ConverterRules instruction) {
    assert currentProgram.group == null;
    if (instruction.ruleSet == null) {
      instruction.ruleSet = new LinkedHashSet<>();
      for (RelOptRule rule : allRules) {
        if (!(rule instanceof ConverterRule)) {
          continue;
        }
        ConverterRule converter = (ConverterRule) rule;
        if (converter.isGuaranteed() != instruction.guaranteed) {
          continue;
        }

        // Add the rule itself to work top-down
        instruction.ruleSet.add(converter);
        if (!instruction.guaranteed) {
          // Add a TraitMatchingRule to work bottom-up
          instruction.ruleSet.add(
              new TraitMatchingRule(converter, RelFactories.LOGICAL_BUILDER));
        }
      }
    }
    applyRules(instruction.ruleSet, instruction.guaranteed);
  }

  void executeInstruction(HepInstruction.CommonRelSubExprRules instruction) {
    assert currentProgram.group == null;
    if (instruction.ruleSet == null) {
      instruction.ruleSet = new LinkedHashSet<>();
      for (RelOptRule rule : allRules) {
        if (!(rule instanceof CommonRelSubExprRule)) {
          continue;
        }
        instruction.ruleSet.add(rule);
      }
    }
    applyRules(instruction.ruleSet, true);
  }

  void executeInstruction(
      HepInstruction.Subprogram instruction) {
    LOGGER.trace("Entering subprogram");
    for (;;) {
      int nTransformationsBefore = nTransformations;
      executeProgram(instruction.subprogram);
      if (nTransformations == nTransformationsBefore) {
        // Nothing happened this time around.
        break;
      }
    }
    LOGGER.trace("Leaving subprogram");
  }

  void executeInstruction(
      HepInstruction.BeginGroup instruction) {
    assert currentProgram.group == null;
    currentProgram.group = instruction.endGroup;
    LOGGER.trace("Entering group");
  }

  void executeInstruction(
      HepInstruction.EndGroup instruction) {
    assert currentProgram.group == instruction;
    currentProgram.group = null;
    instruction.collecting = false;
    applyRules(instruction.ruleSet, true);
    LOGGER.trace("Leaving group");
  }

  private void applyRules(
      Collection<RelOptRule> rules,
      boolean forceConversions) {
    if (currentProgram.group != null) {
      assert currentProgram.group.collecting;
      currentProgram.group.ruleSet.addAll(rules);
      return;
    }

    LOGGER.trace("Applying rule set {}", rules);

    boolean fullRestartAfterTransformation =
        currentProgram.matchOrder != HepMatchOrder.ARBITRARY;

    int nMatches = 0;

    boolean fixpoint;
    do {
      Iterator<HepRelVertex> iter = getGraphIterator(root);
      fixpoint = true;
      while (iter.hasNext()) {
        HepRelVertex vertex = iter.next();
        for (RelOptRule rule : rules) {
          HepRelVertex newVertex =
              applyRule(rule, vertex, forceConversions);
          if (newVertex != null) {
            ++nMatches;
            if (nMatches >= currentProgram.matchLimit) {
              return;
            }
            if (fullRestartAfterTransformation) {
              iter = getGraphIterator(root);
            } else {
              // To the extent possible, pick up where we left
              // off; have to create a new iterator because old
              // one was invalidated by transformation.
              iter = getGraphIterator(newVertex);

              // Remember to go around again since we're
              // skipping some stuff.
              fixpoint = false;
            }
            break;
          }
        }
      }
    } while (!fixpoint);
  }

  private Iterator<HepRelVertex> getGraphIterator(HepRelVertex start) {
    // Make sure there's no garbage, because topological sort
    // doesn't start from a specific root, and rules can't
    // deal with firing on garbage.

    // FIXME jvs 25-Sept-2006:  I had to move this earlier because
    // of FRG-215, which is still under investigation.  Once we
    // figure that one out, move down to location below for
    // better optimizer performance.
    collectGarbage();

    if (currentProgram.matchOrder == HepMatchOrder.ARBITRARY) {
      return DepthFirstIterator.of(graph, start).iterator();
    }

    assert start == root;

    // see above
/*
        collectGarbage();
*/

    Iterable<HepRelVertex> iter =
        TopologicalOrderIterator.of(graph);

    if (currentProgram.matchOrder == HepMatchOrder.TOP_DOWN) {
      return iter.iterator();
    }

    // TODO jvs 4-Apr-2006:  enhance TopologicalOrderIterator
    // to support reverse walk.
    assert currentProgram.matchOrder == HepMatchOrder.BOTTOM_UP;
    final List<HepRelVertex> list = new ArrayList<>();
    for (HepRelVertex vertex : iter) {
      list.add(vertex);
    }
    Collections.reverse(list);
    return list.iterator();
  }

  private HepRelVertex applyRule(
      RelOptRule rule,
      HepRelVertex vertex,
      boolean forceConversions) {
    RelTrait parentTrait = null;
    List<RelNode> parents = null;
    if (rule instanceof ConverterRule) {
      // Guaranteed converter rules require special casing to make sure
      // they only fire where actually needed, otherwise they tend to
      // fire to infinity and beyond.
      ConverterRule converterRule = (ConverterRule) rule;
      if (converterRule.isGuaranteed() || !forceConversions) {
        if (!doesConverterApply(converterRule, vertex)) {
          return null;
        }
        parentTrait = converterRule.getOutTrait();
      }
    } else if (rule instanceof CommonRelSubExprRule) {
      // Only fire CommonRelSubExprRules if the vertex is a common
      // subexpression.
      List<HepRelVertex> parentVertices = getVertexParents(vertex);
      if (parentVertices.size() < 2) {
        return null;
      }
      parents = new ArrayList<>();
      for (HepRelVertex pVertex : parentVertices) {
        parents.add(pVertex.getCurrentRel());
      }
    }

    final List<RelNode> bindings = new ArrayList<>();
    final Map<RelNode, List<RelNode>> nodeChildren = new HashMap<>();
    boolean match =
        matchOperands(
            rule.getOperand(),
            vertex.getCurrentRel(),
            bindings,
            nodeChildren);

    if (!match) {
      return null;
    }

    HepRuleCall call =
        new HepRuleCall(
            this,
            rule.getOperand(),
            bindings.toArray(new RelNode[bindings.size()]),
            nodeChildren,
            parents);

    // Allow the rule to apply its own side-conditions.
    if (!rule.matches(call)) {
      return null;
    }

    fireRule(call);

    if (!call.getResults().isEmpty()) {
      return applyTransformationResults(
          vertex,
          call,
          parentTrait);
    }

    return null;
  }

  private boolean doesConverterApply(
      ConverterRule converterRule,
      HepRelVertex vertex) {
    RelTrait outTrait = converterRule.getOutTrait();
    List<HepRelVertex> parents = Graphs.predecessorListOf(graph, vertex);
    for (HepRelVertex parent : parents) {
      RelNode parentRel = parent.getCurrentRel();
      if (parentRel instanceof Converter) {
        // We don't support converter chains.
        continue;
      }
      if (parentRel.getTraitSet().contains(outTrait)) {
        // This parent wants the traits produced by the converter.
        return true;
      }
    }
    return (vertex == root)
        && (requestedRootTraits != null)
        && requestedRootTraits.contains(outTrait);
  }

  /**
   * Retrieves the parent vertices of a vertex.  If a vertex appears multiple
   * times as an input into a parent, then that counts as multiple parents,
   * one per input reference.
   *
   * @param vertex the vertex
   * @return the list of parents for the vertex
   */
  private List<HepRelVertex> getVertexParents(HepRelVertex vertex) {
    final List<HepRelVertex> parents = new ArrayList<>();
    final List<HepRelVertex> parentVertices =
        Graphs.predecessorListOf(graph, vertex);

    for (HepRelVertex pVertex : parentVertices) {
      RelNode parent = pVertex.getCurrentRel();
      for (int i = 0; i < parent.getInputs().size(); i++) {
        HepRelVertex child = (HepRelVertex) parent.getInputs().get(i);
        if (child == vertex) {
          parents.add(pVertex);
        }
      }
    }
    return parents;
  }

  private boolean matchOperands(
      RelOptRuleOperand operand,
      RelNode rel,
      List<RelNode> bindings,
      Map<RelNode, List<RelNode>> nodeChildren) {
    if (!operand.matches(rel)) {
      return false;
    }
    bindings.add(rel);
    @SuppressWarnings("unchecked")
    List<HepRelVertex> childRels = (List) rel.getInputs();
    switch (operand.childPolicy) {
    case ANY:
      return true;
    case UNORDERED:
      // For each operand, at least one child must match. If
      // matchAnyChildren, usually there's just one operand.
      for (RelOptRuleOperand childOperand : operand.getChildOperands()) {
        boolean match = false;
        for (HepRelVertex childRel : childRels) {
          match =
              matchOperands(
                  childOperand,
                  childRel.getCurrentRel(),
                  bindings,
                  nodeChildren);
          if (match) {
            break;
          }
        }
        if (!match) {
          return false;
        }
      }
      final List<RelNode> children = new ArrayList<>(childRels.size());
      for (HepRelVertex childRel : childRels) {
        children.add(childRel.getCurrentRel());
      }
      nodeChildren.put(rel, children);
      return true;
    default:
      int n = operand.getChildOperands().size();
      if (childRels.size() < n) {
        return false;
      }
      for (Pair<HepRelVertex, RelOptRuleOperand> pair
          : Pair.zip(childRels, operand.getChildOperands())) {
        boolean match =
            matchOperands(
                pair.right,
                pair.left.getCurrentRel(),
                bindings,
                nodeChildren);
        if (!match) {
          return false;
        }
      }
      return true;
    }
  }

  private HepRelVertex applyTransformationResults(
      HepRelVertex vertex,
      HepRuleCall call,
      RelTrait parentTrait) {
    // TODO jvs 5-Apr-2006:  Take the one that gives the best
    // global cost rather than the best local cost.  That requires
    // "tentative" graph edits.

    assert !call.getResults().isEmpty();

    RelNode bestRel = null;

    if (call.getResults().size() == 1) {
      // No costing required; skip it to minimize the chance of hitting
      // rels without cost information.
      bestRel = call.getResults().get(0);
    } else {
      RelOptCost bestCost = null;
      final RelMetadataQuery mq = call.getMetadataQuery();
      for (RelNode rel : call.getResults()) {
        RelOptCost thisCost = getCost(rel, mq);
        if (LOGGER.isTraceEnabled()) {
          // Keep in the isTraceEnabled for the getRowCount method call
          LOGGER.trace("considering {} with cumulative cost={} and rowcount={}",
              rel, thisCost, mq.getRowCount(rel));
        }
        if ((bestRel == null) || thisCost.isLt(bestCost)) {
          bestRel = rel;
          bestCost = thisCost;
        }
      }
    }

    ++nTransformations;
    notifyTransformation(
        call,
        bestRel,
        true);

    // Before we add the result, make a copy of the list of vertex's
    // parents.  We'll need this later during contraction so that
    // we only update the existing parents, not the new parents
    // (otherwise loops can result).  Also take care of filtering
    // out parents by traits in case we're dealing with a converter rule.
    final List<HepRelVertex> allParents =
        Graphs.predecessorListOf(graph, vertex);
    final List<HepRelVertex> parents = new ArrayList<>();
    for (HepRelVertex parent : allParents) {
      if (parentTrait != null) {
        RelNode parentRel = parent.getCurrentRel();
        if (parentRel instanceof Converter) {
          // We don't support automatically chaining conversions.
          // Treating a converter as a candidate parent here
          // can cause the "iParentMatch" check below to
          // throw away a new converter needed in
          // the multi-parent DAG case.
          continue;
        }
        if (!parentRel.getTraitSet().contains(parentTrait)) {
          // This parent does not want the converted result.
          continue;
        }
      }
      parents.add(parent);
    }

    HepRelVertex newVertex = addRelToGraph(bestRel);

    // There's a chance that newVertex is the same as one
    // of the parents due to common subexpression recognition
    // (e.g. the LogicalProject added by JoinCommuteRule).  In that
    // case, treat the transformation as a nop to avoid
    // creating a loop.
    int iParentMatch = parents.indexOf(newVertex);
    if (iParentMatch != -1) {
      newVertex = parents.get(iParentMatch);
    } else {
      contractVertices(newVertex, vertex, parents);
    }

    if (getListener() != null) {
      // Assume listener doesn't want to see garbage.
      collectGarbage();
    }

    notifyTransformation(
        call,
        bestRel,
        false);

    dumpGraph();

    return newVertex;
  }

  // implement RelOptPlanner
  public RelNode register(
      RelNode rel,
      RelNode equivRel) {
    // Ignore; this call is mostly to tell Volcano how to avoid
    // infinite loops.
    return rel;
  }

  @Override public void onCopy(RelNode rel, RelNode newRel) {
    onCopyHook.apply(rel, newRel);
  }

  // implement RelOptPlanner
  public RelNode ensureRegistered(RelNode rel, RelNode equivRel) {
    return rel;
  }

  // implement RelOptPlanner
  public boolean isRegistered(RelNode rel) {
    return true;
  }

  private HepRelVertex addRelToGraph(
      RelNode rel) {
    // Check if a transformation already produced a reference
    // to an existing vertex.
    if (rel instanceof HepRelVertex) {
      return (HepRelVertex) rel;
    }

    // Recursively add children, replacing this rel's inputs
    // with corresponding child vertices.
    final List<RelNode> inputs = rel.getInputs();
    final List<RelNode> newInputs = new ArrayList<>();
    for (RelNode input1 : inputs) {
      HepRelVertex childVertex = addRelToGraph(input1);
      newInputs.add(childVertex);
    }

    if (!Util.equalShallow(inputs, newInputs)) {
      RelNode oldRel = rel;
      rel = rel.copy(rel.getTraitSet(), newInputs);
      onCopy(oldRel, rel);
    }

    // try to find equivalent rel only if DAG is allowed
    if (!noDAG) {
      // Now, check if an equivalent vertex already exists in graph.
      String digest = rel.getDigest();
      HepRelVertex equivVertex = mapDigestToVertex.get(digest);
      if (equivVertex != null) {
        // Use existing vertex.
        return equivVertex;
      }
    }

    // No equivalence:  create a new vertex to represent this rel.
    HepRelVertex newVertex = new HepRelVertex(rel);
    graph.addVertex(newVertex);
    updateVertex(newVertex, rel);

    for (RelNode input : rel.getInputs()) {
      graph.addEdge(newVertex, (HepRelVertex) input);
    }

    nTransformations++;
    return newVertex;
  }

  private void contractVertices(
      HepRelVertex preservedVertex,
      HepRelVertex discardedVertex,
      List<HepRelVertex> parents) {
    if (preservedVertex == discardedVertex) {
      // Nop.
      return;
    }

    RelNode rel = preservedVertex.getCurrentRel();
    updateVertex(preservedVertex, rel);

    // Update specified parents of discardedVertex.
    for (HepRelVertex parent : parents) {
      RelNode parentRel = parent.getCurrentRel();
      List<RelNode> inputs = parentRel.getInputs();
      for (int i = 0; i < inputs.size(); ++i) {
        RelNode child = inputs.get(i);
        if (child != discardedVertex) {
          continue;
        }
        parentRel.replaceInput(i, preservedVertex);
      }
      graph.removeEdge(parent, discardedVertex);
      graph.addEdge(parent, preservedVertex);
      updateVertex(parent, parentRel);
    }

    // NOTE:  we don't actually do graph.removeVertex(discardedVertex),
    // because it might still be reachable from preservedVertex.
    // Leave that job for garbage collection.

    if (discardedVertex == root) {
      root = preservedVertex;
    }
  }

  private void updateVertex(HepRelVertex vertex, RelNode rel) {
    if (rel != vertex.getCurrentRel()) {
      // REVIEW jvs 5-Apr-2006:  We'll do this again later
      // during garbage collection.  Or we could get rid
      // of mark/sweep garbage collection and do it precisely
      // at this point by walking down to all rels which are only
      // reachable from here.
      notifyDiscard(vertex.getCurrentRel());
    }
    String oldDigest = vertex.getCurrentRel().toString();
    if (mapDigestToVertex.get(oldDigest) == vertex) {
      mapDigestToVertex.remove(oldDigest);
    }
    String newDigest = rel.recomputeDigest();
    if (mapDigestToVertex.get(newDigest) == null) {
      mapDigestToVertex.put(newDigest, vertex);
    } else {
      // REVIEW jvs 5-Apr-2006:  Could this lead us to
      // miss common subexpressions?  When called from
      // addRelToGraph, we'll check after this method returns,
      // but what about the other callers?
    }
    if (rel != vertex.getCurrentRel()) {
      vertex.replaceRel(rel);
    }
    notifyEquivalence(
        rel,
        vertex,
        false);
  }

  private RelNode buildFinalPlan(HepRelVertex vertex) {
    RelNode rel = vertex.getCurrentRel();

    notifyChosen(rel);

    // Recursively process children, replacing this rel's inputs
    // with corresponding child rels.
    List<RelNode> inputs = rel.getInputs();
    for (int i = 0; i < inputs.size(); ++i) {
      RelNode child = inputs.get(i);
      if (!(child instanceof HepRelVertex)) {
        // Already replaced.
        continue;
      }
      child = buildFinalPlan((HepRelVertex) child);
      rel.replaceInput(i, child);
      rel.recomputeDigest();
    }

    return rel;
  }

  private void collectGarbage() {
    if (nTransformations == nTransformationsLastGC) {
      // No modifications have taken place since the last gc,
      // so there can't be any garbage.
      return;
    }
    nTransformationsLastGC = nTransformations;

    LOGGER.trace("collecting garbage");

    // Yer basic mark-and-sweep.
    final Set<HepRelVertex> rootSet = new HashSet<>();
    if (graph.vertexSet().contains(root)) {
      BreadthFirstIterator.reachable(rootSet, graph, root);
    }

    if (rootSet.size() == graph.vertexSet().size()) {
      // Everything is reachable:  no garbage to collect.
      return;
    }
    final Set<HepRelVertex> sweepSet = new HashSet<>();
    for (HepRelVertex vertex : graph.vertexSet()) {
      if (!rootSet.contains(vertex)) {
        sweepSet.add(vertex);
        RelNode rel = vertex.getCurrentRel();
        notifyDiscard(rel);
      }
    }
    assert !sweepSet.isEmpty();
    graph.removeAllVertices(sweepSet);
    graphSizeLastGC = graph.vertexSet().size();

    // Clean up digest map too.
    Iterator<Map.Entry<String, HepRelVertex>> digestIter =
        mapDigestToVertex.entrySet().iterator();
    while (digestIter.hasNext()) {
      HepRelVertex vertex = digestIter.next().getValue();
      if (sweepSet.contains(vertex)) {
        digestIter.remove();
      }
    }
  }

  private void assertNoCycles() {
    // Verify that the graph is acyclic.
    final CycleDetector<HepRelVertex, DefaultEdge> cycleDetector =
        new CycleDetector<>(graph);
    Set<HepRelVertex> cyclicVertices = cycleDetector.findCycles();
    if (cyclicVertices.isEmpty()) {
      return;
    }

    throw new AssertionError("Query graph cycle detected in HepPlanner: "
        + cyclicVertices);
  }

  private void dumpGraph() {
    if (!LOGGER.isTraceEnabled()) {
      return;
    }

    assertNoCycles();

    final RelMetadataQuery mq = root.getCluster().getMetadataQuery();
    final StringBuilder sb = new StringBuilder();
    sb.append("\nBreadth-first from root:  {\n");
    for (HepRelVertex vertex : BreadthFirstIterator.of(graph, root)) {
      sb.append("    ")
          .append(vertex)
          .append(" = ");
      RelNode rel = vertex.getCurrentRel();
      sb.append(rel)
          .append(", rowcount=")
          .append(mq.getRowCount(rel))
          .append(", cumulative cost=")
          .append(getCost(rel, mq))
          .append('\n');
    }
    sb.append("}");
    LOGGER.trace(sb.toString());
  }

  // implement RelOptPlanner
  public void registerMetadataProviders(List<RelMetadataProvider> list) {
    list.add(0, new HepRelMetadataProvider());
  }

  // implement RelOptPlanner
  public long getRelMetadataTimestamp(RelNode rel) {
    // TODO jvs 20-Apr-2006: This is overly conservative.  Better would be
    // to keep a timestamp per HepRelVertex, and update only affected
    // vertices and all ancestors on each transformation.
    return nTransformations;
  }
}

// End HepPlanner.java
