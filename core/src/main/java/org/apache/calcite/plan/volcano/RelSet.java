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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A <code>RelSet</code> is an equivalence-set of expressions; that is, a set of
 * expressions which have identical semantics. We are generally interested in
 * using the expression which has the lowest cost.
 *
 * <p>All of the expressions in an <code>RelSet</code> have the same calling
 * convention.</p>
 */
class RelSet {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  //~ Instance fields --------------------------------------------------------

  final List<RelNode> rels = new ArrayList<>();
  /**
   * Relational expressions that have a subset in this set as a child. This
   * is a multi-set. If multiple relational expressions in this set have the
   * same parent, there will be multiple entries.
   */
  final List<RelNode> parents = new ArrayList<>();
  final List<RelSubset> subsets = new ArrayList<>();

  /**
   * List of {@link AbstractConverter} objects which have not yet been
   * satisfied.
   */
  final List<AbstractConverter> abstractConverters = new ArrayList<>();

  /**
   * Set to the superseding set when this is found to be equivalent to another
   * set.
   */
  RelSet equivalentSet;
  RelNode rel;

  /**
   * Variables that are set by relational expressions in this set
   * and available for use by parent and child expressions.
   */
  final Set<CorrelationId> variablesPropagated;

  /**
   * Variables that are used by relational expressions in this set.
   */
  final Set<CorrelationId> variablesUsed;
  final int id;

  /**
   * Reentrancy flag.
   */
  boolean inMetadataQuery;

  //~ Constructors -----------------------------------------------------------

  RelSet(
      int id,
      Set<CorrelationId> variablesPropagated,
      Set<CorrelationId> variablesUsed) {
    this.id = id;
    this.variablesPropagated = variablesPropagated;
    this.variablesUsed = variablesUsed;
  }

  //~ Methods ----------------------------------------------------------------

  /**
   * Returns all of the {@link RelNode}s which reference {@link RelNode}s in
   * this set.
   */
  public List<RelNode> getParentRels() {
    return parents;
  }

  /**
   * @return all of the {@link RelNode}s contained by any subset of this set
   * (does not include the subset objects themselves)
   */
  public List<RelNode> getRelsFromAllSubsets() {
    return rels;
  }

  public RelSubset getSubset(RelTraitSet traits) {
    for (RelSubset subset : subsets) {
      if (subset.getTraitSet().equals(traits)) {
        return subset;
      }
    }
    return null;
  }

  /**
   * Removes all references to a specific {@link RelNode} in both the subsets
   * and their parent relationships.
   */
  void obliterateRelNode(RelNode rel) {
    parents.remove(rel);
  }

  /**
   * Adds a relational expression to a set, with its results available under a
   * particular calling convention. An expression may be in the set several
   * times with different calling conventions (and hence different costs).
   */
  public RelSubset add(RelNode rel) {
    assert equivalentSet == null : "adding to a dead set";
    final RelTraitSet traitSet = rel.getTraitSet().simplify();
    final RelSubset subset = getOrCreateSubset(rel.getCluster(), traitSet);
    subset.add(rel);
    return subset;
  }

  private void addAbstractConverters(
      VolcanoPlanner planner, RelOptCluster cluster, RelSubset subset, boolean subsetToOthers) {
    // Converters from newly introduced subset to all the remaining one (vice versa), only if
    // we can convert.  No point adding converters if it is not possible.
    for (RelSubset other : subsets) {

      assert other.getTraitSet().size() == subset.getTraitSet().size();

      if ((other == subset)
          || (subsetToOthers
              && !subset.getConvention().useAbstractConvertersForConversion(
                  subset.getTraitSet(), other.getTraitSet()))
          || (!subsetToOthers
              && !other.getConvention().useAbstractConvertersForConversion(
                  other.getTraitSet(), subset.getTraitSet()))) {
        continue;
      }

      final ImmutableList<RelTrait> difference =
          subset.getTraitSet().difference(other.getTraitSet());

      boolean addAbstractConverter = true;
      int numTraitNeedConvert = 0;

      for (RelTrait curOtherTrait : difference) {
        RelTraitDef traitDef = curOtherTrait.getTraitDef();
        RelTrait curRelTrait = subset.getTraitSet().getTrait(traitDef);

        assert curRelTrait.getTraitDef() == traitDef;

        if (curRelTrait == null) {
          addAbstractConverter = false;
          break;
        }

        boolean canConvert = false;
        boolean needConvert = false;
        if (subsetToOthers) {
          // We can convert from subset to other.  So, add converter with subset as child and
          // traitset as the other's traitset.
          canConvert = traitDef.canConvert(
              cluster.getPlanner(), curRelTrait, curOtherTrait, subset);
          needConvert = !curRelTrait.satisfies(curOtherTrait);
        } else {
          // We can convert from others to subset.
          canConvert = traitDef.canConvert(
              cluster.getPlanner(), curOtherTrait, curRelTrait, other);
          needConvert = !curOtherTrait.satisfies(curRelTrait);
        }

        if (!canConvert) {
          addAbstractConverter = false;
          break;
        }

        if (needConvert) {
          numTraitNeedConvert++;
        }
      }

      if (addAbstractConverter && numTraitNeedConvert > 0) {
        if (subsetToOthers) {
          final AbstractConverter converter =
              new AbstractConverter(cluster, subset, null, other.getTraitSet());
          planner.register(converter, other);
        } else {
          final AbstractConverter converter =
              new AbstractConverter(cluster, other, null, subset.getTraitSet());
          planner.register(converter, subset);
        }
      }
    }
  }

  RelSubset getOrCreateSubset(
      RelOptCluster cluster,
      RelTraitSet traits) {
    RelSubset subset = getSubset(traits);
    if (subset == null) {
      subset = new RelSubset(cluster, this, traits);

      final VolcanoPlanner planner =
          (VolcanoPlanner) cluster.getPlanner();

      addAbstractConverters(planner, cluster, subset, true);

      // Need to first add to subset before adding the abstract converters (for others->subset)
      // since otherwise during register() the planner will try to add this subset again.
      subsets.add(subset);

      addAbstractConverters(planner, cluster, subset, false);

      if (planner.listener != null) {
        postEquivalenceEvent(planner, subset);
      }
    }
    return subset;
  }

  private void postEquivalenceEvent(VolcanoPlanner planner, RelNode rel) {
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
   * {@link org.apache.calcite.plan.volcano.RelSubset}. (Called only from
   * {@link org.apache.calcite.plan.volcano.RelSubset#add}.
   *
   * @param rel Relational expression
   */
  void addInternal(RelNode rel) {
    if (!rels.contains(rel)) {
      rels.add(rel);
      for (RelTrait trait : rel.getTraitSet()) {
        assert trait == trait.getTraitDef().canonize(trait);
      }

      VolcanoPlanner planner =
          (VolcanoPlanner) rel.getCluster().getPlanner();
      if (planner.listener != null) {
        postEquivalenceEvent(planner, rel);
      }
    }
    if (this.rel == null) {
      this.rel = rel;
    } else {
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
   * @param planner  Planner
   * @param otherSet RelSet which is equivalent to this one
   */
  void mergeWith(
      VolcanoPlanner planner,
      RelSet otherSet) {
    assert this != otherSet;
    assert this.equivalentSet == null;
    assert otherSet.equivalentSet == null;
    LOGGER.trace("Merge set#{} into set#{}", otherSet.id, id);
    otherSet.equivalentSet = this;
    final RelMetadataQuery mq = rel.getCluster().getMetadataQuery();

    // remove from table
    boolean existed = planner.allSets.remove(otherSet);
    assert existed : "merging with a dead otherSet";

    final Map<RelSubset, RelNode> changedSubsets = new IdentityHashMap<>();

    // merge subsets
    for (RelSubset otherSubset : otherSet.subsets) {
      planner.ruleQueue.subsetImportances.remove(otherSubset);
      RelSubset subset =
          getOrCreateSubset(
              otherSubset.getCluster(),
              otherSubset.getTraitSet());
      // collect RelSubset instances, whose best should be changed
      if (otherSubset.bestCost.isLt(subset.bestCost)) {
        changedSubsets.put(subset, otherSubset.best);
      }
      for (RelNode otherRel : otherSubset.getRels()) {
        planner.reregister(this, otherRel);
      }
    }

    // Has another set merged with this?
    assert equivalentSet == null;

    // calls propagateCostImprovements() for RelSubset instances,
    // whose best should be changed to checks whether that
    // subset's parents have gotten cheaper.
    final Set<RelSubset> activeSet = new HashSet<>();
    for (Map.Entry<RelSubset, RelNode> subsetBestPair : changedSubsets.entrySet()) {
      RelSubset relSubset = subsetBestPair.getKey();
      relSubset.propagateCostImprovements(
          planner, mq, subsetBestPair.getValue(),
          activeSet);
    }
    assert activeSet.isEmpty();

    // Update all rels which have a child in the other set, to reflect the
    // fact that the child has been renamed.
    //
    // Copy array to prevent ConcurrentModificationException.
    final List<RelNode> previousParents =
        ImmutableList.copyOf(otherSet.getParentRels());
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
    for (RelNode parentRel : getParentRels()) {
      final RelSubset parentSubset = planner.getSubset(parentRel);
      parentSubset.propagateCostImprovements(
          planner, mq, parentRel,
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
