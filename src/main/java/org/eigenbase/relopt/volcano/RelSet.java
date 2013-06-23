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

import java.util.*;
import java.util.logging.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.trace.*;


/**
 * A <code>RelSet</code> is an equivalence-set of expressions; that is, a set of
 * expressions which have identical semantics. We are generally interested in
 * using the expression which has the lowest cost.
 *
 * <p>All of the expressions in an <code>RelSet</code> have the same calling
 * convention.</p>
 *
 * @author jhyde
 * @version $Id$
 * @since 16 December, 2001
 */
class RelSet
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = EigenbaseTrace.getPlannerTracer();

    //~ Instance fields --------------------------------------------------------

    final List<RelNode> rels = new ArrayList<RelNode>();
    final List<RelNode> parents = new ArrayList<RelNode>();
    final List<RelSubset> subsets = new ArrayList<RelSubset>();

    /**
     * List of {@link AbstractConverter} objects which have not yet been
     * satisfied.
     */
    final List<AbstractConverter> abstractConverters =
        new ArrayList<AbstractConverter>();

    /**
     * Set to the superseding set when this is found to be equivalent to another
     * set.
     */
    RelSet equivalentSet;
    RelNode rel;

    /**
     * Names of variables which are set by relational expressions in this set
     * and available for use by parent and child expressions.
     */
    final Set<String> variablesPropagated;

    /**
     * Names of variables which are used by relational expressions in this set.
     */
    final Set<String> variablesUsed;
    final int id;

    /**
     * Reentrancy flag.
     */
    boolean inMetadataQuery;

    //~ Constructors -----------------------------------------------------------

    RelSet(
        int id,
        Set<String> variablesPropagated,
        Set<String> variablesUsed)
    {
        this.id = id;
        this.variablesPropagated = variablesPropagated;
        this.variablesUsed = variablesUsed;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns all of the {@link RelNode}s which reference {@link RelNode}s in
     * this set.
     */
    public List<RelNode> getParentRels()
    {
        return parents;
    }

    /**
     * @return all of the {@link RelNode}s contained by any subset of this set
     * (does not include the subset objects themselves)
     */
    public List<RelNode> getRelsFromAllSubsets()
    {
        return rels;
    }

    public RelSubset getSubset(RelTraitSet traits)
    {
        for (RelSubset subset : subsets) {
            if (subset.getTraitSet().equals(traits)) {
                return subset;
            }
        }
        return null;
    }

    /** Removes all references to a specific {@link RelNode} in both the subsets
     * and their parent relationships. */
    void obliterateRelNode(RelNode rel) {
        parents.remove(rel);
    }

    /**
     * Adds a relational expression to a set, with its results available under a
     * particular calling convention. An expression may be in the set several
     * times with different calling conventions (and hence different costs).
     */
    public RelSubset add(RelNode rel)
    {
        assert equivalentSet == null : "adding to a dead set";
        RelSubset subset =
            getOrCreateSubset(
                rel.getCluster(),
                rel.getTraitSet());
        subset.add(rel);
        return subset;
    }

    RelSubset getOrCreateSubset(
        RelOptCluster cluster,
        RelTraitSet traits)
    {
        RelSubset subset = getSubset(traits);
        if (subset == null) {
            subset = new RelSubset(cluster, this, traits);

            final VolcanoPlanner planner =
                (VolcanoPlanner) cluster.getPlanner();

            // Add converters to convert the new subset to each existing subset.
            for (RelSubset subset1 : subsets) {
                if (subset1.getConvention() == Convention.NONE) {
                    continue;
                }
                final AbstractConverter converter =
                    new AbstractConverter(
                        cluster, subset, ConventionTraitDef.instance,
                        subset1.getTraitSet());
                planner.register(converter, subset1);
            }

            subsets.add(subset);

            // Add converters to convert each existing subset to this subset.
            for (RelSubset subset1 : subsets) {
                if (subset1 == subset) {
                    continue;
                }
                final AbstractConverter converter =
                    new AbstractConverter(
                        cluster, subset1, ConventionTraitDef.instance,
                        traits);
                planner.register(converter, subset);
            }

            if (planner.listener != null) {
                postEquivalenceEvent(planner, subset);
            }
        }
        return subset;
    }

    private void postEquivalenceEvent(VolcanoPlanner planner, RelNode rel)
    {
        RelOptListener.RelEquivalenceEvent event =
            new RelOptListener.RelEquivalenceEvent(
                planner,
                rel,
                "equivalence class " + id,
                false);
        planner.listener.relEquivalenceFound(event);
    }

    /**
     * Adds an expression <code>rel</code> to this set, without creating a
     * {@link org.eigenbase.relopt.volcano.RelSubset}. (Called only from
     * {@link org.eigenbase.relopt.volcano.RelSubset#add}.
     *
     * @param rel Relational expression
     */
    void addInternal(RelNode rel)
    {
        if (!rels.contains(rel)) {
            rels.add(rel);

            VolcanoPlanner planner =
                (VolcanoPlanner) rel.getCluster().getPlanner();
            if (planner.listener != null) {
                postEquivalenceEvent(planner, rel);
            }
        }
        if (this.rel == null) {
            this.rel = rel;
        } else {
            assert (rel.getCorrelVariable() == null);
            String correl = this.rel.getCorrelVariable();
            if (correl != null) {
                rel.setCorrelVariable(correl);
            }

            // Row types must be the same, except for field names.
            RelOptUtil.verifyTypeEquivalence(
                this.rel,
                rel,
                this);
        }
    }

    /**
     * Merges <code>otherSet</code> into this RelSet.
     *
     * <p>One generally calls this method after discovering that two relational
     * expressions are equivalent, and hence the <code>RelSet</code>s they
     * belong to are equivalent also.
     *
     * <p>After this method completes, <code>otherSet</code> is obsolete, its
     * {@link #equivalentSet} member points to this RelSet, and this RelSet is
     * still alive.
     *
     * @param planner Planner
     * @param otherSet RelSet which is equivalent to this one
     */
    void mergeWith(
        VolcanoPlanner planner,
        RelSet otherSet)
    {
        assert (this != otherSet);
        assert (this.equivalentSet == null);
        assert (otherSet.equivalentSet == null);
        tracer.finer("Merge set#" + otherSet.id + " into set#" + id);
        otherSet.equivalentSet = this;

        // remove from table
        boolean existed = planner.allSets.remove(otherSet);
        assert (existed) : "merging with a dead otherSet";

        // merge subsets
        for (RelSubset otherSubset : otherSet.subsets) {
            planner.ruleQueue.subsetImportances.remove(otherSubset);
            RelSubset subset =
                getOrCreateSubset(
                    otherSubset.getCluster(),
                    otherSubset.getTraitSet());
            if (otherSubset.bestCost.isLt(subset.bestCost)) {
                subset.bestCost = otherSubset.bestCost;
                subset.best = otherSubset.best;
            }
            for (RelNode otherRel : otherSubset.getRels()) {
                planner.reregister(this, otherRel);
            }
        }

        // Has another set merged with this?
        assert equivalentSet == null;

        // Update all rels which have a child in the other set, to reflect the
        // fact that the child has been renamed.
        //
        // Copy array to prevent ConcurrentModificationException.
        final List<RelNode> previousParents =
            new ArrayList<RelNode>(otherSet.getParentRels());
        for (RelNode parentRel : previousParents) {
            planner.rename(parentRel);
        }

        // Renaming may have caused this set to merge with another. If so,
        // this set is now obsolete. There's no need to update the children
        // of this set - indeed, it could be dangerous.
        if (equivalentSet != null) {
            return;
        }

        // Make sure the cost changes as a result of merging are propagated.
        Set<RelSubset> activeSet = new HashSet<RelSubset>();
        for (RelNode parentRel : getParentRels()) {
            final RelSubset parentSubset = planner.getSubset(parentRel);
            parentSubset.propagateCostImprovements(
                planner,
                parentRel,
                activeSet);
        }
        assert activeSet.isEmpty();
        assert equivalentSet == null;

        // Each of the relations in the old set now has new parents, so
        // potentially new rules can fire. Check for rule matches, just as if
        // it were newly registered.  (This may cause rules which have fired
        // once to fire again.)
        for (RelNode rel : rels) {
            assert planner.getSet(rel) == this;
            planner.fireRules(rel, true);
        }
    }
}

// End RelSet.java
