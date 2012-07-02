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
package org.eigenbase.relopt.volcano;

import java.io.*;

import java.util.*;
import java.util.logging.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;


/**
 * A <code>RelSubset</code> is set of expressions in a set which have the same
 * calling convention. An expression may be in more than one sub-set of a set;
 * the same expression is used.
 *
 * @author jhyde
 * @version $Id$
 * @since 16 December, 2001
 */
public class RelSubset
    extends AbstractRelNode
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Instance fields --------------------------------------------------------

    /**
     * List of the relational expressions for which this subset is an input.
     */
    final List<RelNode> parents;

    /**
     * The relational expressions in this subset.
     */
    final List<RelNode> rels;

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
     * whether findBestPlan is being called
     */
    boolean active;

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
        RelTraitSet traits)
    {
        super(cluster, traits);
        this.set = set;
        this.rels = new ArrayList<RelNode>();
        this.parents = new ArrayList<RelNode>();
        this.bestCost = VolcanoCost.INFINITY;
        this.boosted = false;
        recomputeDigest();
    }

    //~ Methods ----------------------------------------------------------------

    // REVIEW jvs 15-Mar-2005: I disabled this exception because there is
    // actually code which walks over trees containing RelSubsets (in
    // RelOptUtil), and that code was special casing traversal of RelSubset,
    // which isn't right.
    /**
     * There are no children, as such. We throw an exception because you
     * probably don't want to be walking over trees which contain <code>
     * RelSubset</code>s.
     */
    /*
    public RelNode [] getInputs() { throw new UnsupportedOperationException(); }
     */

    public Set<String> getVariablesSet()
    {
        return set.variablesPropagated;
    }

    public Set<String> getVariablesUsed()
    {
        return set.variablesUsed;
    }

    public RelNode getBest()
    {
        return best;
    }

    /**
     * A <code>RelSubSet</code> is its own clone.
     */
    public RelSubset clone()
    {
        return this;
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        throw new UnsupportedOperationException();
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeZeroCost();
    }

    public double getRows()
    {
        if (best == null) {
            return VolcanoCost.INFINITY.getRows();
        } else {
            return RelMetadataQuery.getRowCount(best);
        }
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        StringBuilder s = new StringBuilder();
        s.append(id).append(": RelSubset(");
        for (int i = 0; i < traitSet.size(); i++) {
            if (i > 0) {
                s.append(", ");
            }
            s.append(traitSet.getTrait(i));
        }
        s.append(')');

        pw.explainSubset(
            s.toString(),
            rels.get(0));
    }

    protected String computeDigest()
    {
        StringBuilder digest = new StringBuilder("Subset#");
        digest.append(set.id);
        for (int i = 0; i < traitSet.size(); i++) {
            digest.append('.').append(traitSet.getTrait(i));
        }
        return digest.toString();
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        return set.rel.getRowType();
    }

    // implement RelNode
    public boolean isDistinct()
    {
        return set.rel.isDistinct();
    }

    Collection<RelSubset> getParentSubsets()
    {
        List<RelSubset> list = new ArrayList<RelSubset>(parents.size());

        for (RelNode rel : parents) {
            final RelSubset subset =
                ((VolcanoPlanner) getCluster().getPlanner()).getSubset(rel);
            assert (subset != null) : rel.toString() + " has no subset";
            list.add(subset);
        }
        return list;
    }

    RelSet getSet()
    {
        return set;
    }

    /**
     * Adds expression <code>rel</code> to this subset.
     */
    void add(RelNode rel)
    {
        if (rels.contains(rel)) {
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
        assert (set.rel == null)
            || RelOptUtil.equal(
                "rowtype of new rel",
                rel.getRowType(),
                "rowtype of set",
                getRowType(),
                true);
        rels.add(rel);
        set.addInternal(rel);
        Set<String> variablesSet = RelOptUtil.getVariablesSet(rel);
        Set<String> variablesStopped = rel.getVariablesStopped();
        if (false) {
            Set<String> variablesPropagated =
                Util.minus(variablesSet, variablesStopped);
            assert set.variablesPropagated.containsAll(variablesPropagated);
            Set<String> variablesUsed = RelOptUtil.getVariablesUsed(rel);
            assert set.variablesUsed.containsAll(variablesUsed);
        }
    }

    /**
     * Recursively builds a tree consisting of the cheapest plan at each node.
     */
    RelNode buildCheapestPlan(VolcanoPlanner planner)
    {
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
     * @param planner Planner
     * @param rel Relational expression whose cost has improved
     * @param activeSet Set of active subsets, for cycle detection
     */
    void propagateCostImprovements(
        VolcanoPlanner planner,
        RelNode rel,
        Set<RelSubset> activeSet)
    {
        ++timestamp;

        if (!activeSet.add(this)) {
            // This subset is already in the chain being propagated to. This
            // means that the graph is cyclic, and therefore the cost of this
            // relational expression - not this subset - must be infinite.
            tracer.finer("cyclic: " + this);
            return;
        }
        try {
            final RelOptCost cost = planner.getCost(rel);
            if (cost.isLt(bestCost)) {
                if (tracer.isLoggable(Level.FINER)) {
                    tracer.finer(
                        "Subset cost improved: subset [" + this
                        + "] cost was " + bestCost + " now " + cost);
                }

                bestCost = cost;
                best = rel;

                // Lower cost means lower importance. Other nodes will change
                // too, but we'll get to them later.
                planner.ruleQueue.recompute(this);
                for (RelNode parent : parents) {
                    final RelSubset parentSubset = planner.getSubset(parent);
                    parentSubset.propagateCostImprovements(
                        planner, parent, activeSet);
                }
                planner.checkForSatisfiedConverters(set, rel);
            }
        } finally {
            activeSet.remove(this);
        }
    }

    public void propagateBoostRemoval(VolcanoPlanner planner)
    {
        planner.ruleQueue.recompute(this);

        if (boosted) {
            boosted = false;

            for (RelNode parent : parents) {
                final RelSubset parentSubset = planner.getSubset(parent);
                parentSubset.propagateBoostRemoval(planner);
            }
        }
    }

    public void collectVariablesUsed(Set<String> variableSet)
    {
        variableSet.addAll(getVariablesUsed());
    }

    public void collectVariablesSet(Set<String> variableSet)
    {
        variableSet.addAll(getVariablesSet());
    }

    /**
     * Returns the rel nodes in this rel subset.  All rels must have the same
     * traits and are logically equivalent.
     * @return all the rels in the subset
     */
    public List<RelNode> getRels() {
        return rels;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Visitor which walks over a tree of {@link RelSet}s, replacing each node
     * with the cheapest implementation of the expression.
     */
    class CheapestPlanReplacer
    {
        VolcanoPlanner planner;

        CheapestPlanReplacer(VolcanoPlanner planner)
        {
            super();
            this.planner = planner;
        }

        public RelNode visit(
            RelNode p,
            int ordinal,
            RelNode parent)
        {
            if (p instanceof RelSubset) {
                RelSubset subset = (RelSubset) p;
                RelNode cheapest = subset.best;
                if (cheapest == null) {
                    if (tracer.isLoggable(Level.WARNING)) {
                        // Dump the planner's expression pool so we can figure
                        // out why we reached impasse.
                        StringWriter sw = new StringWriter();
                        final PrintWriter pw = new PrintWriter(sw);
                        pw.println(
                            "Node [" + subset.getDescription()
                            + "] could not be implemented; planner state:");
                        planner.dump(pw);
                        pw.flush();
                        tracer.warning(sw.toString());
                    }
                    Error e =
                        Util.newInternal(
                            "node could not be implemented: "
                            + subset.getDigest());
                    tracer.throwing(
                        getClass().getName(),
                        "visit",
                        e);
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
            List<RelNode> inputs = new ArrayList<RelNode>();
            for (int i = 0; i < oldInputs.size(); i++) {
                RelNode oldInput = oldInputs.get(i);
                RelNode input = visit(oldInput, i, p);
                inputs.add(input);
            }
            if (!inputs.equals(oldInputs)) {
                p = p.copy(p.getTraitSet(), inputs);
            }
            return p;
        }
    }
}

// End RelSubset.java
