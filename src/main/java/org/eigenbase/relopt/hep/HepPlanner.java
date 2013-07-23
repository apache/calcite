/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.relopt.hep;

import java.util.*;
import java.util.logging.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;

import org.jgrapht.*;
import org.jgrapht.alg.*;
import org.jgrapht.graph.*;
import org.jgrapht.traverse.*;

/**
 * HepPlanner is a heuristic implementation of the {@link RelOptPlanner}
 * interface.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class HepPlanner
    extends AbstractRelOptPlanner
{
    //~ Instance fields --------------------------------------------------------

    private HepProgram mainProgram;

    private HepProgram currentProgram;

    private HepRelVertex root;

    private RelTraitSet requestedRootTraits;

    private Map<String, HepRelVertex> mapDigestToVertex;

    private Set<RelOptRule> allRules;

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

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new HepPlanner that allows DAG.
     *
     * @param program program controlling rule application
     */
    public HepPlanner(HepProgram program)
    {
        this(program, false);
    }

    /**
     * Creates a new HepPlanner with the option to keep the graph a
     * tree(noDAG=true) or allow DAG(noDAG=false).
     *
     * @param program program controlling rule application
     */
    public HepPlanner(HepProgram program, boolean noDAG)
    {
        this.mainProgram = program;

        mapDigestToVertex = new HashMap<String, HepRelVertex>();
        graph =
            new DefaultDirectedGraph<HepRelVertex, DefaultEdge>(
                DefaultEdge.class);

        // NOTE jvs 24-Apr-2006:  We use LinkedHashSet here and below
        // in order to provide deterministic behavior.
        allRules = new LinkedHashSet<RelOptRule>();
        this.noDAG = noDAG;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptPlanner
    public void setRoot(RelNode rel)
    {
        root = addRelToGraph(rel);
        dumpGraph();
    }

    // implement RelOptPlanner
    public RelNode getRoot()
    {
        return root;
    }

    public void addMaterialization(RelNode tableRel, RelNode queryRel) {
        // ignore - this planner does not support materializations
    }

    // implement RelOptPlanner
    public boolean addRule(RelOptRule rule)
    {
        boolean added = allRules.add(rule);
        if (added) {
            mapRuleDescription(rule);
        }
        return added;
    }

    // implement RelOptPlanner
    public boolean removeRule(RelOptRule rule)
    {
        unmapRuleDescription(rule);
        return allRules.remove(rule);
    }

    // implement RelOptPlanner
    public RelNode changeTraits(RelNode rel, RelTraitSet toTraits)
    {
        // Ignore traits, except for the root, where we remember
        // what the final conversion should be.
        if ((rel == root) || (rel == root.getCurrentRel())) {
            requestedRootTraits = toTraits;
        }
        return rel;
    }

    // implement RelOptPlanner
    public RelNode findBestExp()
    {
        assert (root != null);

        executeProgram(mainProgram);

        // Get rid of everything except what's in the final plan.
        collectGarbage();

        return buildFinalPlan(root);
    }

    private void executeProgram(HepProgram program)
    {
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
        HepInstruction.MatchLimit instruction)
    {
        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest("Setting match limit to " + instruction.limit);
        }
        currentProgram.matchLimit = instruction.limit;
    }

    void executeInstruction(
        HepInstruction.MatchOrder instruction)
    {
        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest("Setting match limit to " + instruction.order);
        }
        currentProgram.matchOrder = instruction.order;
    }

    void executeInstruction(
        HepInstruction.RuleInstance instruction)
    {
        if (skippingGroup()) {
            return;
        }
        if (instruction.rule == null) {
            assert (instruction.ruleDescription != null);
            instruction.rule =
                getRuleByDescription(instruction.ruleDescription);
            if (tracer.isLoggable(Level.FINEST)) {
                tracer.finest(
                    "Looking up rule with description "
                    + instruction.ruleDescription
                    + ", found " + instruction.rule);
            }
        }
        if (instruction.rule != null) {
            applyRules(
                Collections.singleton(instruction.rule),
                true);
        }
    }

    void executeInstruction(
        HepInstruction.RuleClass instruction)
    {
        if (skippingGroup()) {
            return;
        }
        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest("Applying rule class " + instruction.ruleClass);
        }
        if (instruction.ruleSet == null) {
            instruction.ruleSet = new LinkedHashSet<RelOptRule>();
            for (RelOptRule rule : allRules) {
                if (instruction.ruleClass.isInstance(rule)) {
                    instruction.ruleSet.add(rule);
                }
            }
        }
        applyRules(instruction.ruleSet, true);
    }

    void executeInstruction(
        HepInstruction.RuleCollection instruction)
    {
        if (skippingGroup()) {
            return;
        }
        applyRules(instruction.rules, true);
    }

    private boolean skippingGroup()
    {
        if (currentProgram.group != null) {
            // Skip if we've already collected the ruleset.
            return !currentProgram.group.collecting;
        } else {
            // Not grouping.
            return false;
        }
    }

    void executeInstruction(
        HepInstruction.ConverterRules instruction)
    {
        assert (currentProgram.group == null);
        if (instruction.ruleSet == null) {
            instruction.ruleSet = new LinkedHashSet<RelOptRule>();
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
                    instruction.ruleSet.add(new TraitMatchingRule(converter));
                }
            }
        }
        applyRules(instruction.ruleSet, instruction.guaranteed);
    }

    void executeInstruction(HepInstruction.CommonRelSubExprRules instruction)
    {
        assert (currentProgram.group == null);
        if (instruction.ruleSet == null) {
            instruction.ruleSet = new LinkedHashSet<RelOptRule>();
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
        HepInstruction.Subprogram instruction)
    {
        tracer.finest("Entering subprogram");
        for (;;) {
            int nTransformationsBefore = nTransformations;
            executeProgram(instruction.subprogram);
            if (nTransformations == nTransformationsBefore) {
                // Nothing happened this time around.
                break;
            }
        }
        tracer.finest("Leaving subprogram");
    }

    void executeInstruction(
        HepInstruction.BeginGroup instruction)
    {
        assert (currentProgram.group == null);
        currentProgram.group = instruction.endGroup;
        tracer.finest("Entering group");
    }

    void executeInstruction(
        HepInstruction.EndGroup instruction)
    {
        assert (currentProgram.group == instruction);
        currentProgram.group = null;
        instruction.collecting = false;
        applyRules(instruction.ruleSet, true);
        tracer.finest("Leaving group");
    }

    private void applyRules(
        Collection<RelOptRule> rules,
        boolean forceConversions)
    {
        if (currentProgram.group != null) {
            assert (currentProgram.group.collecting);
            currentProgram.group.ruleSet.addAll(rules);
            return;
        }

        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest("Applying rule set " + rules);
        }

        boolean fullRestartAfterTransformation =
            (currentProgram.matchOrder != HepMatchOrder.ARBITRARY);

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

    private Iterator<HepRelVertex> getGraphIterator(HepRelVertex start)
    {
        // Make sure there's no garbage, because topological sort
        // doesn't start from a specific root, and rules can't
        // deal with firing on garbage.

        // FIXME jvs 25-Sept-2006:  I had to move this earlier because
        // of FRG-215, which is still under investigation.  Once we
        // figure that one out, move down to location below for
        // better optimizer performance.
        collectGarbage();

        if (currentProgram.matchOrder == HepMatchOrder.ARBITRARY) {
            return new DepthFirstIterator<HepRelVertex, DefaultEdge>(
                graph,
                start);
        }

        assert (start == root);

        // see above
/*
        collectGarbage();
*/

        Iterator<HepRelVertex> iter =
            new TopologicalOrderIterator<HepRelVertex, DefaultEdge>(graph);

        if (currentProgram.matchOrder == HepMatchOrder.TOP_DOWN) {
            return iter;
        }

        // TODO jvs 4-Apr-2006:  enhance TopologicalOrderIterator
        // to support reverse walk.
        assert (currentProgram.matchOrder == HepMatchOrder.BOTTOM_UP);
        ArrayList<HepRelVertex> list = new ArrayList<HepRelVertex>();
        while (iter.hasNext()) {
            list.add(iter.next());
        }
        Collections.reverse(list);
        return list.iterator();
    }

    private HepRelVertex applyRule(
        RelOptRule rule,
        HepRelVertex vertex,
        boolean forceConversions)
    {
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
            parents = new ArrayList<RelNode>();
            for (HepRelVertex pVertex : parentVertices) {
                parents.add(pVertex.getCurrentRel());
            }
        }

        List<RelNode> bindings = new ArrayList<RelNode>();
        Map<RelNode, List<RelNode>> nodeChildren =
            new HashMap<RelNode, List<RelNode>>();
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
        HepRelVertex vertex)
    {
        RelTrait outTrait = converterRule.getOutTrait();
        List<HepRelVertex> parents = Graphs.predecessorListOf(graph, vertex);
        for (HepRelVertex parent : parents) {
            RelNode parentRel = parent.getCurrentRel();
            if (parentRel instanceof ConverterRel) {
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
     *
     * @return the list of parents for the vertex
     */
    private List<HepRelVertex> getVertexParents(HepRelVertex vertex)
    {
        List<HepRelVertex> parents = new ArrayList<HepRelVertex>();
        List<HepRelVertex> parentVertices =
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
        Map<RelNode, List<RelNode>> nodeChildren)
    {
        if (!operand.matches(rel)) {
            return false;
        }
        bindings.add(rel);
        RelOptRuleOperand [] childOperands = operand.getChildOperands();
        if (childOperands == null) {
            return true;
        }
        int n = childOperands.length;
        List<RelNode> childRels = rel.getInputs();
        if (operand.matchAnyChildren) {
            // For each operand, at least one child must match. If
            // matchAnyChildren, usually there's just one operand.
            for (RelOptRuleOperand childOperand : childOperands) {
                boolean match = false;
                for (int i = 0; i < childRels.size(); ++i) {
                    final HepRelVertex childRel =
                        (HepRelVertex) childRels.get(i);
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
            List<RelNode> children = new ArrayList<RelNode>(childRels.size());
            for (RelNode childRel : childRels) {
                children.add(((HepRelVertex) childRel).getCurrentRel());
            }
            nodeChildren.put(rel, children);
            return true;
        } else {
            if (childRels.size() < n) {
                return false;
            }
            for (int i = 0; i < n; ++i) {
                boolean match =
                    matchOperands(
                        childOperands[i],
                        ((HepRelVertex) childRels.get(i)).getCurrentRel(),
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
        RelTrait parentTrait)
    {
        // TODO jvs 5-Apr-2006:  Take the one that gives the best
        // global cost rather than the best local cost.  That requires
        // "tentative" graph edits.

        assert (!call.getResults().isEmpty());

        RelNode bestRel = null;

        if (call.getResults().size() == 1) {
            // No costing required; skip it to minimize the chance of hitting
            // rels without cost information.
            bestRel = call.getResults().get(0);
        } else {
            RelOptCost bestCost = null;
            for (RelNode rel : call.getResults()) {
                RelOptCost thisCost = getCost(rel);
                if (tracer.isLoggable(Level.FINER)) {
                    tracer.finer(
                        "considering " + rel + " with cumulative cost="
                        + thisCost + " and rowcount="
                        + RelMetadataQuery.getRowCount(rel));
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
        List<HepRelVertex> allParents = Graphs.predecessorListOf(graph, vertex);
        List<HepRelVertex> parents = new ArrayList<HepRelVertex>();
        for (HepRelVertex parent : allParents) {
            if (parentTrait != null) {
                RelNode parentRel = parent.getCurrentRel();
                if (parentRel instanceof ConverterRel) {
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
        // (e.g. the ProjectRel added by SwapJoinRule).  In that
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
        RelNode equivRel)
    {
        // Ignore; this call is mostly to tell Volcano how to avoid
        // infinite loops.
        return rel;
    }

    // implement RelOptPlanner
    public RelNode ensureRegistered(RelNode rel, RelNode equivRel)
    {
        return rel;
    }

    // implement RelOptPlanner
    public boolean isRegistered(RelNode rel)
    {
        return true;
    }

    private HepRelVertex addRelToGraph(
        RelNode rel)
    {
        // Check if a transformation already produced a reference
        // to an existing vertex.
        if (rel instanceof HepRelVertex) {
            return (HepRelVertex) rel;
        }

        // Recursively add children, replacing this rel's inputs
        // with corresponding child vertices.
        List<? extends RelNode> inputs = rel.getInputs();
        List<HepRelVertex> newInputs = new ArrayList<HepRelVertex>();
        for (int i = 0; i < inputs.size(); ++i) {
            HepRelVertex childVertex = addRelToGraph(inputs.get(i));
            newInputs.add(childVertex);
        }

        if (!Util.equalShallow(inputs, newInputs)) {
            rel = rel.copy(rel.getTraitSet(), (List) newInputs);
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

        return newVertex;
    }

    private void contractVertices(
        HepRelVertex preservedVertex,
        HepRelVertex discardedVertex,
        List<HepRelVertex> parents)
    {
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

    private void updateVertex(HepRelVertex vertex, RelNode rel)
    {
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

    private RelNode buildFinalPlan(HepRelVertex vertex)
    {
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
        }

        return rel;
    }

    private void collectGarbage()
    {
        if (nTransformations == nTransformationsLastGC) {
            // No modifications have taken place since the last gc,
            // so there can't be any garbage.
            return;
        }
        nTransformationsLastGC = nTransformations;

        tracer.finest("collecting garbage");

        // Yer basic mark-and-sweep.
        Set<HepRelVertex> rootSet = new HashSet<HepRelVertex>();
        Iterator<HepRelVertex> iter =
            new DepthFirstIterator<HepRelVertex, DefaultEdge>(
                graph,
                root);
        while (iter.hasNext()) {
            rootSet.add(iter.next());
        }

        if (rootSet.size() == graph.vertexSet().size()) {
            // Everything is reachable:  no garbage to collect.
            return;
        }
        Set<HepRelVertex> sweepSet = new HashSet<HepRelVertex>();
        for (HepRelVertex vertex : graph.vertexSet()) {
            if (!rootSet.contains(vertex)) {
                sweepSet.add(vertex);
                RelNode rel = vertex.getCurrentRel();
                notifyDiscard(rel);
            }
        }
        assert (!sweepSet.isEmpty());
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

    private void assertNoCycles()
    {
        // Verify that the graph is acyclic.
        CycleDetector<HepRelVertex, DefaultEdge> cycleDetector =
            new CycleDetector<HepRelVertex, DefaultEdge>(graph);
        Set<HepRelVertex> cyclicVertices = cycleDetector.findCycles();
        if (cyclicVertices.isEmpty()) {
            return;
        }

        throw Util.newInternal(
            "Query graph cycle detected in HepPlanner:  "
            + cyclicVertices);
    }

    private void dumpGraph()
    {
        if (!tracer.isLoggable(Level.FINER)) {
            return;
        }

        assertNoCycles();

        Iterator<HepRelVertex> bfsIter =
            new BreadthFirstIterator<HepRelVertex, DefaultEdge>(
                graph,
                root);

        StringBuilder sb = new StringBuilder();
        sb.append(Util.lineSeparator);
        sb.append("Breadth-first from root:  {");
        sb.append(Util.lineSeparator);
        while (bfsIter.hasNext()) {
            HepRelVertex vertex = bfsIter.next();
            sb.append("    ");
            sb.append(vertex);
            sb.append(" = ");
            RelNode rel = vertex.getCurrentRel();
            sb.append(rel);
            sb.append(", rowcount=" + RelMetadataQuery.getRowCount(rel));
            sb.append(", cumulative cost=" + getCost(rel));
            sb.append(Util.lineSeparator);
        }
        sb.append("}");
        tracer.finer(sb.toString());
    }

    // implement RelOptPlanner
    public void registerMetadataProviders(ChainedRelMetadataProvider chain)
    {
        chain.addProvider(new HepRelMetadataProvider());
    }

    // implement RelOptPlanner
    public long getRelMetadataTimestamp(RelNode rel)
    {
        // TODO jvs 20-Apr-2006: This is overly conservative.  Better would be
        // to keep a timestamp per HepRelVertex, and update only affected
        // vertices and all ancestors on each transformation.
        return nTransformations;
    }
}

// End HepPlanner.java
