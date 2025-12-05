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
import org.apache.calcite.plan.RelDigest;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelOptMaterialization;
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
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

import static org.apache.calcite.linq4j.Nullness.castNonNull;

import static java.util.Objects.requireNonNull;

/**
 * HepPlanner is a heuristic implementation of the {@link RelOptPlanner}
 * interface.
 */
public class HepPlanner extends AbstractRelOptPlanner {
  //~ Instance fields --------------------------------------------------------

  private final HepProgram mainProgram;

  private @Nullable HepRelVertex root;

  private @Nullable RelTraitSet requestedRootTraits;

  /**
   * {@link RelDataType} is represented with its field types as {@code List<RelDataType>}.
   * This enables to treat as equal projects that differ in expression names only.
   */
  private final Map<RelDigest, HepRelVertex> mapDigestToVertex =
      new HashMap<>();

  private int nTransformations;

  private int graphSizeLastGC;

  private int nTransformationsLastGC;

  private final boolean noDag;

  /**
   * Query graph, with edges directed from parent to child. This is a
   * single-rooted DAG, possibly with additional roots corresponding to
   * discarded plan fragments which remain to be garbage-collected.
   */
  private final DirectedGraph<HepRelVertex, DefaultEdge> graph =
      DefaultDirectedGraph.create();

  private final Function2<RelNode, RelNode, Void> onCopyHook;

  private final List<RelOptMaterialization> materializations =
      new ArrayList<>();

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
  public HepPlanner(HepProgram program, @Nullable Context context) {
    this(program, context, false, null, RelOptCostImpl.FACTORY);
  }

  /**
   * Creates a new HepPlanner with the option to keep the graph a
   * tree (noDag = true) or allow DAG (noDag = false).
   *
   * @param noDag      If false, create shared nodes if expressions are
   *                   identical
   * @param program    Program controlling rule application
   * @param onCopyHook Function to call when a node is copied
   */
  public HepPlanner(
      HepProgram program,
      @Nullable Context context,
      boolean noDag,
      @Nullable Function2<RelNode, RelNode, Void> onCopyHook,
      RelOptCostFactory costFactory) {
    super(costFactory, context);
    this.mainProgram = requireNonNull(program, "program");
    this.onCopyHook = Util.first(onCopyHook, Functions.ignore2());
    this.noDag = noDag;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void setRoot(RelNode rel) {
    root = addRelToGraph(rel);
    dumpGraph();
  }

  @Override public @Nullable RelNode getRoot() {
    return root;
  }

  @Override public void clear() {
    super.clear();
    for (RelOptRule rule : getRules()) {
      removeRule(rule);
    }
    this.materializations.clear();
  }

  @Override public RelNode changeTraits(RelNode rel, RelTraitSet toTraits) {
    // Ignore traits, except for the root, where we remember
    // what the final conversion should be.
    if ((rel == root) || (rel == requireNonNull(root, "root").getCurrentRel())) {
      requestedRootTraits = toTraits;
    }
    return rel;
  }

  @Override public RelNode findBestExp() {
    requireNonNull(root, "'root' must not be null");

    executeProgram(mainProgram);

    // Get rid of everything except what's in the final plan.
    collectGarbage();
    dumpRuleAttemptsInfo();
    return buildFinalPlan(requireNonNull(root, "'root' must not be null"));
  }

  /** Top-level entry point for a program. Initializes state and then invokes
   * the program. */
  private void executeProgram(HepProgram program) {
    final HepInstruction.PrepareContext px =
        HepInstruction.PrepareContext.create(this);
    final HepState state = program.prepare(px);
    state.execute();
  }

  void executeProgram(HepProgram instruction, HepProgram.State state) {
    state.init();
    state.instructionStates.forEach(instructionState -> {
      instructionState.execute();
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
    });
  }

  void executeMatchLimit(HepInstruction.MatchLimit instruction,
      HepInstruction.MatchLimit.State state) {
    LOGGER.trace("Setting match limit to {}", instruction.limit);
    state.programState.matchLimit = instruction.limit;
  }

  void executeMatchOrder(HepInstruction.MatchOrder instruction,
      HepInstruction.MatchOrder.State state) {
    LOGGER.trace("Setting match order to {}", instruction.order);
    state.programState.matchOrder = instruction.order;
  }

  void executeRuleInstance(HepInstruction.RuleInstance instruction,
      HepInstruction.RuleInstance.State state) {
    if (state.programState.skippingGroup()) {
      return;
    }
    applyRules(state.programState, ImmutableList.of(instruction.rule), true);
  }

  void executeRuleLookup(HepInstruction.RuleLookup instruction,
      HepInstruction.RuleLookup.State state) {
    if (state.programState.skippingGroup()) {
      return;
    }
    RelOptRule rule = state.rule;
    if (rule == null) {
      state.rule = rule = getRuleByDescription(instruction.ruleDescription);
      LOGGER.trace("Looking up rule with description {}, found {}",
          instruction.ruleDescription, rule);
    }
    if (rule != null) {
      applyRules(state.programState, ImmutableList.of(rule), true);
    }
  }

  void executeRuleClass(HepInstruction.RuleClass instruction,
      HepInstruction.RuleClass.State state) {
    if (state.programState.skippingGroup()) {
      return;
    }
    LOGGER.trace("Applying rule class {}", instruction.ruleClass);
    Set<RelOptRule> ruleSet = state.ruleSet;
    if (ruleSet == null) {
      state.ruleSet = ruleSet = new LinkedHashSet<>();
      Class<?> ruleClass = instruction.ruleClass;
      for (RelOptRule rule : mapDescToRule.values()) {
        if (ruleClass.isInstance(rule)) {
          ruleSet.add(rule);
        }
      }
    }
    applyRules(state.programState, ruleSet, true);
  }

  void executeRuleCollection(HepInstruction.RuleCollection instruction,
      HepInstruction.RuleCollection.State state) {
    if (state.programState.skippingGroup()) {
      return;
    }
    applyRules(state.programState, instruction.rules, true);
  }

  void executeConverterRules(HepInstruction.ConverterRules instruction,
      HepInstruction.ConverterRules.State state) {
    checkArgument(state.programState.group == null);
    Set<RelOptRule> ruleSet = state.ruleSet;
    if (ruleSet == null) {
      state.ruleSet = ruleSet = new LinkedHashSet<>();
      for (RelOptRule rule : mapDescToRule.values()) {
        if (!(rule instanceof ConverterRule)) {
          continue;
        }
        ConverterRule converter = (ConverterRule) rule;
        if (converter.isGuaranteed() != instruction.guaranteed) {
          continue;
        }

        // Add the rule itself to work top-down
        ruleSet.add(converter);
        if (!instruction.guaranteed) {
          // Add a TraitMatchingRule to work bottom-up
          ruleSet.add(
              TraitMatchingRule.config(converter, RelFactories.LOGICAL_BUILDER)
                  .toRule());
        }
      }
    }
    applyRules(state.programState, ruleSet, instruction.guaranteed);
  }

  void executeCommonRelSubExprRules(
      HepInstruction.CommonRelSubExprRules instruction,
      HepInstruction.CommonRelSubExprRules.State state) {
    checkArgument(state.programState.group == null);
    Set<RelOptRule> ruleSet = state.ruleSet;
    if (ruleSet == null) {
      state.ruleSet = ruleSet = new LinkedHashSet<>();
      for (RelOptRule rule : mapDescToRule.values()) {
        if (!(rule instanceof CommonRelSubExprRule)) {
          continue;
        }
        ruleSet.add(rule);
      }
    }
    applyRules(state.programState, ruleSet, true);
  }

  void executeSubProgram(HepInstruction.SubProgram instruction,
      HepInstruction.SubProgram.State state) {
    LOGGER.trace("Entering subprogram");
    for (;;) {
      int nTransformationsBefore = nTransformations;
      state.programState.execute();
      if (nTransformations == nTransformationsBefore) {
        // Nothing happened this time around.
        break;
      }
    }
    LOGGER.trace("Leaving subprogram");
  }

  void executeBeginGroup(HepInstruction.BeginGroup instruction,
      HepInstruction.BeginGroup.State state) {
    checkArgument(state.programState.group == null);
    state.programState.group = state.endGroup;
    LOGGER.trace("Entering group");
  }

  void executeEndGroup(HepInstruction.EndGroup instruction,
      HepInstruction.EndGroup.State state) {
    checkArgument(state.programState.group == state);
    state.programState.group = null;
    state.collecting = false;
    applyRules(state.programState, state.ruleSet, true);
    LOGGER.trace("Leaving group");
  }

  private int depthFirstApply(HepProgram.State programState,
      Iterator<HepRelVertex> iter, Collection<RelOptRule> rules,
      boolean forceConversions, int nMatches) {
    while (iter.hasNext()) {
      HepRelVertex vertex = iter.next();
      for (RelOptRule rule : rules) {
        HepRelVertex newVertex =
            applyRule(rule, vertex, forceConversions);
        if (newVertex == null || newVertex == vertex) {
          continue;
        }
        ++nMatches;
        if (nMatches >= programState.matchLimit) {
          return nMatches;
        }
        // To the extent possible, pick up where we left
        // off; have to create a new iterator because old
        // one was invalidated by transformation.
        Iterator<HepRelVertex> depthIter =
            getGraphIterator(programState, newVertex);
        nMatches =
            depthFirstApply(programState, depthIter, rules, forceConversions,
                nMatches);
        break;
      }
    }
    return nMatches;
  }

  private void applyRules(HepProgram.State programState,
      Collection<RelOptRule> rules, boolean forceConversions) {
    final HepInstruction.EndGroup.State group = programState.group;
    if (group != null) {
      checkArgument(group.collecting);
      Set<RelOptRule> ruleSet = requireNonNull(group.ruleSet, "group.ruleSet");
      ruleSet.addAll(rules);
      return;
    }

    LOGGER.trace("Applying rule set {}", rules);

    final boolean fullRestartAfterTransformation =
        programState.matchOrder != HepMatchOrder.ARBITRARY
            && programState.matchOrder != HepMatchOrder.DEPTH_FIRST;

    int nMatches = 0;

    boolean fixedPoint;
    do {
      Iterator<HepRelVertex> iter =
          getGraphIterator(programState, requireNonNull(root, "root"));
      fixedPoint = true;
      while (iter.hasNext()) {
        HepRelVertex vertex = iter.next();
        for (RelOptRule rule : rules) {
          HepRelVertex newVertex =
              applyRule(rule, vertex, forceConversions);
          if (newVertex == null || newVertex == vertex) {
            continue;
          }
          ++nMatches;
          if (nMatches >= programState.matchLimit) {
            return;
          }
          if (fullRestartAfterTransformation) {
            iter = getGraphIterator(programState, requireNonNull(root, "root"));
          } else {
            // To the extent possible, pick up where we left
            // off; have to create a new iterator because old
            // one was invalidated by transformation.
            iter = getGraphIterator(programState, newVertex);
            if (programState.matchOrder == HepMatchOrder.DEPTH_FIRST) {
              nMatches =
                  depthFirstApply(programState, iter, rules, forceConversions, nMatches);
              if (nMatches >= programState.matchLimit) {
                return;
              }
            }
            // Remember to go around again since we're
            // skipping some stuff.
            fixedPoint = false;
          }
          break;
        }
      }
    } while (!fixedPoint);
  }

  private Iterator<HepRelVertex> getGraphIterator(
      HepProgram.State programState, HepRelVertex start) {
    switch (requireNonNull(programState.matchOrder, "programState.matchOrder")) {
    case ARBITRARY:
    case DEPTH_FIRST:
      return DepthFirstIterator.of(graph, start).iterator();
    case TOP_DOWN:
    case BOTTOM_UP:
      assert start == root;
      collectGarbage();
      return TopologicalOrderIterator.of(graph, programState.matchOrder).iterator();
    default:
      throw new
          UnsupportedOperationException("Unsupported match order: " + programState.matchOrder);
    }
  }

  private @Nullable HepRelVertex applyRule(
      RelOptRule rule,
      HepRelVertex vertex,
      boolean forceConversions) {
    if (!graph.vertexSet().contains(vertex)) {
      return null;
    }
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
            bindings.toArray(new RelNode[0]),
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
  protected List<HepRelVertex> getVertexParents(HepRelVertex vertex) {
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

  private static boolean matchOperands(
      RelOptRuleOperand operand,
      RelNode rel,
      List<RelNode> bindings,
      Map<RelNode, List<RelNode>> nodeChildren) {
    if (!operand.matches(rel)) {
      return false;
    }
    for (RelNode input : rel.getInputs()) {
      if (!(input instanceof HepRelVertex)) {
        // The graph could be partially optimized for materialized view. In that
        // case, the input would be a RelNode and shouldn't be matched again here.
        return false;
      }
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
      @Nullable RelTrait parentTrait) {
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
        if (thisCost == null) {
          continue;
        }
        if (bestRel == null || thisCost.isLt(castNonNull(bestCost))) {
          bestRel = rel;
          bestCost = thisCost;
        }
      }
    }

    ++nTransformations;
    notifyTransformation(
        call,
        requireNonNull(bestRel, "bestRel"),
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

  @Override public RelNode register(
      RelNode rel,
      @Nullable RelNode equivRel) {
    // Ignore; this call is mostly to tell Volcano how to avoid
    // infinite loops.
    return rel;
  }

  @Override public void onCopy(RelNode rel, RelNode newRel) {
    onCopyHook.apply(rel, newRel);
  }

  @Override public RelNode ensureRegistered(RelNode rel, @Nullable RelNode equivRel) {
    return rel;
  }

  @Override public boolean isRegistered(RelNode rel) {
    return true;
  }

  private HepRelVertex addRelToGraph(
      RelNode rel) {
    // Check if a transformation already produced a reference
    // to an existing vertex.
    if (graph.vertexSet().contains(rel)) {
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
    // Compute digest first time we add to DAG,
    // otherwise can't get equivVertex for common sub-expression
    rel.recomputeDigest();

    // try to find equivalent rel only if DAG is allowed
    if (!noDag) {
      // Now, check if an equivalent vertex already exists in graph.
      HepRelVertex equivVertex = mapDigestToVertex.get(rel.getRelDigest());
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
      clearCache(parent);
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

  /**
   * Clears metadata cache for the RelNode and its ancestors.
   *
   * @param vertex relnode
   */
  private void clearCache(HepRelVertex vertex) {
    RelMdUtil.clearCache(vertex.getCurrentRel());
    if (!RelMdUtil.clearCache(vertex)) {
      return;
    }
    Queue<DefaultEdge> queue =
        new ArrayDeque<>(graph.getInwardEdges(vertex));
    while (!queue.isEmpty()) {
      DefaultEdge edge = queue.remove();
      HepRelVertex source = (HepRelVertex) edge.source;
      RelMdUtil.clearCache(source.getCurrentRel());
      if (RelMdUtil.clearCache(source)) {
        queue.addAll(graph.getInwardEdges(source));
      }
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
    RelDigest oldKey = vertex.getCurrentRel().getRelDigest();
    if (mapDigestToVertex.get(oldKey) == vertex) {
      mapDigestToVertex.remove(oldKey);
    }
    // When a transformation happened in one rule apply, support
    // vertex2 replace vertex1, but the current relNode of
    // vertex1 and vertex2 is same,
    // then the digest is also same. but we can't remove vertex2,
    // otherwise the digest will be removed wrongly in the mapDigestToVertex
    //  when collectGC
    // so it must update the digest that map to vertex
    mapDigestToVertex.put(rel.getRelDigest(), vertex);
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
    boolean changed = false;
    for (int i = 0; i < inputs.size(); ++i) {
      RelNode child = inputs.get(i);
      if (!(child instanceof HepRelVertex)) {
        // Already replaced.
        continue;
      }
      child = buildFinalPlan((HepRelVertex) child);
      rel.replaceInput(i, child);
      changed = true;
    }
    if (changed) {
      RelMdUtil.clearCache(rel);
      rel.recomputeDigest();
    }

    if (rel instanceof HepRelVertex) {
      throw new AssertionError("post-condition failed: " + rel);
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
    HepRelVertex root = requireNonNull(this.root, "this.root");
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
    Iterator<Map.Entry<RelDigest, HepRelVertex>> digestIter =
        mapDigestToVertex.entrySet().iterator();
    while (digestIter.hasNext()) {
      HepRelVertex vertex = digestIter.next().getValue();
      if (sweepSet.contains(vertex)) {
        digestIter.remove();
      }
    }

    // Clean up metadata cache too.
    sweepSet.forEach(this::clearCache);
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

    HepRelVertex root = this.root;
    if (root == null) {
      LOGGER.trace("dumpGraph: root is null");
      return;
    }
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

  @Deprecated // to be removed before 2.0
  @Override public void registerMetadataProviders(List<RelMetadataProvider> list) {
    list.add(0, new HepRelMetadataProvider());
  }

  @Deprecated // to be removed before 2.0
  @Override public long getRelMetadataTimestamp(RelNode rel) {
    // TODO jvs 20-Apr-2006: This is overly conservative.  Better would be
    // to keep a timestamp per HepRelVertex, and update only affected
    // vertices and all ancestors on each transformation.
    return nTransformations;
  }

  @Override public ImmutableList<RelOptMaterialization> getMaterializations() {
    return ImmutableList.copyOf(materializations);
  }

  @Override public void addMaterialization(RelOptMaterialization materialization) {
    materializations.add(materialization);
  }
}
