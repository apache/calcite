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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelNodes;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ChunkList;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;

import org.slf4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Priority queue of relexps whose rules have not been called, and rule-matches
 * which have not yet been acted upon.
 */
class RuleQueue {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  private static final Set<String> ALL_RULES = ImmutableSet.of("<ALL RULES>");

  /**
   * Largest value which is less than one.
   */
  private static final double ONE_MINUS_EPSILON = computeOneMinusEpsilon();

  //~ Instance fields --------------------------------------------------------

  /**
   * The importance of each subset.
   */
  final Map<RelSubset, Double> subsetImportances = new HashMap<>();

  /**
   * The set of RelSubsets whose importance is currently in an artificially
   * raised state. Typically this only includes RelSubsets which have only
   * logical RelNodes.
   */
  final Set<RelSubset> boostedSubsets = new HashSet<>();

  /**
   * Map of {@link VolcanoPlannerPhase} to a list of rule-matches. Initially,
   * there is an empty {@link PhaseMatchList} for each planner phase. As the
   * planner invokes {@link #addMatch(VolcanoRuleMatch)} the rule-match is
   * added to the appropriate PhaseMatchList(s). As the planner completes
   * phases, the matching entry is removed from this list to avoid unused
   * work.
   */
  final Map<VolcanoPlannerPhase, PhaseMatchList> matchListMap =
      new EnumMap<>(VolcanoPlannerPhase.class);

  /**
   * Sorts rule-matches into decreasing order of importance.
   */
  private static final Comparator<VolcanoRuleMatch> MATCH_COMPARATOR =
      new RuleMatchImportanceComparator();

  private final VolcanoPlanner planner;

  /**
   * Compares relexps according to their cached 'importance'.
   */
  private final Ordering<RelSubset> relImportanceOrdering =
      Ordering.from(new RelImportanceComparator());

  /**
   * Maps a {@link VolcanoPlannerPhase} to a set of rule descriptions. Named rules
   * may be invoked in their corresponding phase.
   *
   * <p>See {@link VolcanoPlannerPhaseRuleMappingInitializer} for more
   * information regarding the contents of this Map and how it is initialized.
   */
  private final Map<VolcanoPlannerPhase, Set<String>> phaseRuleMapping;

  //~ Constructors -----------------------------------------------------------

  RuleQueue(VolcanoPlanner planner) {
    this.planner = planner;

    phaseRuleMapping = new EnumMap<>(VolcanoPlannerPhase.class);

    // init empty sets for all phases
    for (VolcanoPlannerPhase phase : VolcanoPlannerPhase.values()) {
      phaseRuleMapping.put(phase, new HashSet<>());
    }

    // configure phases
    planner.getPhaseRuleMappingInitializer().initialize(phaseRuleMapping);

    for (VolcanoPlannerPhase phase : VolcanoPlannerPhase.values()) {
      // empty phases get converted to "all rules"
      if (phaseRuleMapping.get(phase).isEmpty()) {
        phaseRuleMapping.put(phase, ALL_RULES);
      }

      // create a match list data structure for each phase
      PhaseMatchList matchList = new PhaseMatchList(phase);

      matchListMap.put(phase, matchList);
    }
  }

  //~ Methods ----------------------------------------------------------------
  /**
   * Clear internal data structure for this rule queue.
   */
  public void clear() {
    this.subsetImportances.clear();
    this.boostedSubsets.clear();
    for (PhaseMatchList matchList : matchListMap.values()) {
      matchList.clear();
    }
  }

  /**
   * Removes the {@link PhaseMatchList rule-match list} for the given planner
   * phase.
   */
  public void phaseCompleted(VolcanoPlannerPhase phase) {
    matchListMap.get(phase).clear();
  }

  /**
   * Computes the importance of a set (which is that of its most important
   * subset).
   */
  public double getImportance(RelSet set) {
    double importance = 0;
    for (RelSubset subset : set.subsets) {
      importance =
          Math.max(
              importance,
              getImportance(subset));
    }
    return importance;
  }

  /**
   * Recomputes the importance of the given RelSubset.
   *
   * @param subset RelSubset whose importance is to be recomputed
   * @param force  if true, forces an importance update even if the subset has
   *               not been registered
   */
  public void recompute(RelSubset subset, boolean force) {
    Double previousImportance = subsetImportances.get(subset);
    if (previousImportance == null) {
      if (!force) {
        // Subset has not been registered yet. Don't worry about it.
        return;
      }

      previousImportance = Double.NEGATIVE_INFINITY;
    }

    double importance = computeImportance(subset);
    if (previousImportance == importance) {
      return;
    }

    updateImportance(subset, importance);
  }

  /**
   * Equivalent to
   * {@link #recompute(RelSubset, boolean) recompute(subset, false)}.
   */
  public void recompute(RelSubset subset) {
    recompute(subset, false);
  }

  /**
   * Artificially boosts the importance of the given {@link RelSubset}s by a
   * given factor.
   *
   * <p>Iterates over the currently boosted RelSubsets and removes their
   * importance boost, forcing a recalculation of the RelSubsets' importances
   * (see {@link #recompute(RelSubset)}).
   *
   * <p>Once RelSubsets have been restored to their normal importance, the
   * given RelSubsets have their importances boosted. A RelSubset's boosted
   * importance is always less than 1.0 (and never equal to 1.0).
   *
   * @param subsets RelSubsets to boost importance (priority)
   * @param factor  the amount to boost their importances (e.g., 1.25 increases
   *                importance by 25%)
   */
  public void boostImportance(Collection<RelSubset> subsets, double factor) {
    LOGGER.trace("boostImportance({}, {})", factor, subsets);
    final List<RelSubset> boostRemovals = new ArrayList<>();
    final Iterator<RelSubset> iter = boostedSubsets.iterator();
    while (iter.hasNext()) {
      RelSubset subset = iter.next();

      if (!subsets.contains(subset)) {
        iter.remove();
        boostRemovals.add(subset);
      }
    }

    boostRemovals.sort(new Comparator<RelSubset>() {
      public int compare(RelSubset o1, RelSubset o2) {
        int o1children = countChildren(o1);
        int o2children = countChildren(o2);
        int c = Integer.compare(o1children, o2children);
        if (c == 0) {
          // for determinism
          c = Integer.compare(o1.getId(), o2.getId());
        }
        return c;
      }

      private int countChildren(RelSubset subset) {
        int count = 0;
        for (RelNode rel : subset.getRels()) {
          count += rel.getInputs().size();
        }
        return count;
      }
    });

    for (RelSubset subset : boostRemovals) {
      subset.propagateBoostRemoval(planner);
    }

    for (RelSubset subset : subsets) {
      double importance = subsetImportances.get(subset);

      updateImportance(
          subset,
          Math.min(ONE_MINUS_EPSILON, importance * factor));

      subset.boosted = true;
      boostedSubsets.add(subset);
    }
  }

  void updateImportance(RelSubset subset, Double importance) {
    subsetImportances.put(subset, importance);

    for (PhaseMatchList matchList : matchListMap.values()) {
      Multimap<RelSubset, VolcanoRuleMatch> relMatchMap =
          matchList.matchMap;
      if (relMatchMap.containsKey(subset)) {
        for (VolcanoRuleMatch match : relMatchMap.get(subset)) {
          match.clearCachedImportance();
        }
      }
    }
  }

  /**
   * Returns the importance of an equivalence class of relational expressions.
   * Subset importances are held in a lookup table, and importance changes
   * gradually propagate through that table.
   *
   * <p>If a subset in the same set but with a different calling convention is
   * deemed to be important, then this subset has at least half of its
   * importance. (This rule is designed to encourage conversions to take
   * place.)</p>
   */
  double getImportance(RelSubset rel) {
    assert rel != null;

    double importance = 0;
    final RelSet set = planner.getSet(rel);
    assert set != null;
    for (RelSubset subset2 : set.subsets) {
      final Double d = subsetImportances.get(subset2);
      if (d == null) {
        continue;
      }
      double subsetImportance = d;
      if (subset2 != rel) {
        subsetImportance /= 2;
      }
      if (subsetImportance > importance) {
        importance = subsetImportance;
      }
    }
    return importance;
  }

  /**
   * Adds a rule match. The rule-matches are automatically added to all
   * existing {@link PhaseMatchList per-phase rule-match lists} which allow
   * the rule referenced by the match.
   */
  void addMatch(VolcanoRuleMatch match) {
    final String matchName = match.toString();
    for (PhaseMatchList matchList : matchListMap.values()) {
      if (!matchList.names.add(matchName)) {
        // Identical match has already been added.
        continue;
      }

      Set<String> phaseRuleSet = phaseRuleMapping.get(matchList.phase);
      if (phaseRuleSet != ALL_RULES) {
        String ruleDescription = match.getRule().toString();
        if (!phaseRuleSet.contains(ruleDescription)) {
          continue;
        }
      }

      LOGGER.trace("{} Rule-match queued: {}", matchList.phase.toString(), matchName);

      matchList.list.add(match);

      matchList.matchMap.put(
          planner.getSubset(match.rels[0]), match);
    }
  }

  /**
   * Computes the <dfn>importance</dfn> of a node. Importance is defined as
   * follows:
   *
   * <ul>
   * <li>the root {@link RelSubset} has an importance of 1</li>
   * <li>the importance of any other subset is the max of its importance to
   * its parents</li>
   * <li>The importance of children is pro-rated according to the cost of the
   * children. Consider a node which has a cost of 3, and children with costs
   * of 2 and 5. The total cost is 10. If the node has an importance of .5,
   * then the children will have importance of .1 and .25. The retains .15
   * importance points, to reflect the fact that work needs to be done on the
   * node's algorithm.</li>
   * </ul>
   *
   * <p>The formula for the importance <i>I</i> of node n is:
   *
   * <blockquote>I<sub>n</sub> = Max<sub>parents p of n</sub>{I<sub>p</sub> .
   * W <sub>n, p</sub>}</blockquote>
   *
   * <p>where W<sub>n, p</sub>, the weight of n within its parent p, is
   *
   * <blockquote>W<sub>n, p</sub> = Cost<sub>n</sub> / (SelfCost<sub>p</sub> +
   * Cost<sub>n0</sub> + ... + Cost<sub>nk</sub>)
   * </blockquote>
   */
  double computeImportance(RelSubset subset) {
    double importance;
    if (subset == planner.root) {
      // The root always has importance = 1
      importance = 1.0;
    } else {
      final RelMetadataQuery mq = subset.getCluster().getMetadataQuery();

      // The importance of a subset is the max of its importance to its
      // parents
      importance = 0.0;
      for (RelSubset parent : subset.getParentSubsets(planner)) {
        final double childImportance =
            computeImportanceOfChild(mq, subset, parent);
        importance = Math.max(importance, childImportance);
      }
    }
    LOGGER.trace("Importance of [{}] is {}", subset, importance);
    return importance;
  }

  private void dump() {
    if (LOGGER.isTraceEnabled()) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      dump(pw);
      pw.flush();
      LOGGER.trace(sw.toString());
      planner.getRoot().getCluster().invalidateMetadataQuery();
    }
  }

  private void dump(PrintWriter pw) {
    planner.dump(pw);
    pw.print("Importances: {");
    for (RelSubset subset
        : relImportanceOrdering.sortedCopy(subsetImportances.keySet())) {
      pw.print(" " + subset.toString() + "=" + subsetImportances.get(subset));
    }
    pw.println("}");
  }

  /**
   * Removes the rule match with the highest importance, and returns it.
   *
   * <p>Returns {@code null} if there are no more matches.</p>
   *
   * <p>Note that the VolcanoPlanner may still decide to reject rule matches
   * which have become invalid, say if one of their operands belongs to an
   * obsolete set or has importance=0.
   *
   * @throws java.lang.AssertionError if this method is called with a phase
   *                              previously marked as completed via
   *                              {@link #phaseCompleted(VolcanoPlannerPhase)}.
   */
  VolcanoRuleMatch popMatch(VolcanoPlannerPhase phase) {
    dump();

    PhaseMatchList phaseMatchList = matchListMap.get(phase);
    if (phaseMatchList == null) {
      throw new AssertionError("Used match list for phase " + phase
          + " after phase complete");
    }

    final List<VolcanoRuleMatch> matchList = phaseMatchList.list;
    VolcanoRuleMatch match;
    for (;;) {
      if (matchList.isEmpty()) {
        return null;
      }
      if (LOGGER.isTraceEnabled()) {
        matchList.sort(MATCH_COMPARATOR);
        match = matchList.remove(0);

        StringBuilder b = new StringBuilder();
        b.append("Sorted rule queue:");
        for (VolcanoRuleMatch match2 : matchList) {
          final double importance = match2.computeImportance();
          b.append("\n");
          b.append(match2);
          b.append(" importance ");
          b.append(importance);
        }

        LOGGER.trace(b.toString());
      } else {
        // If we're not tracing, it's not worth the effort of sorting the
        // list to find the minimum.
        match = null;
        int bestPos = -1;
        int i = -1;
        for (VolcanoRuleMatch match2 : matchList) {
          ++i;
          if (match == null
              || MATCH_COMPARATOR.compare(match2, match) < 0) {
            bestPos = i;
            match = match2;
          }
        }
        match = matchList.remove(bestPos);
      }

      if (skipMatch(match)) {
        LOGGER.debug("Skip match: {}", match);
      } else {
        break;
      }
    }

    // A rule match's digest is composed of the operand RelNodes' digests,
    // which may have changed if sets have merged since the rule match was
    // enqueued.
    match.recomputeDigest();

    phaseMatchList.matchMap.remove(
        planner.getSubset(match.rels[0]), match);

    LOGGER.debug("Pop match: {}", match);
    return match;
  }

  /** Returns whether to skip a match. This happens if any of the
   * {@link RelNode}s have importance zero. */
  private boolean skipMatch(VolcanoRuleMatch match) {
    for (RelNode rel : match.rels) {
      Double importance = planner.relImportances.get(rel);
      if (importance != null && importance == 0d) {
        return true;
      }
    }

    // If the same subset appears more than once along any path from root
    // operand to a leaf operand, we have matched a cycle. A relational
    // expression that consumes its own output can never be implemented, and
    // furthermore, if we fire rules on it we may generate lots of garbage.
    // For example, if
    //   Project(A, X = X + 0)
    // is in the same subset as A, then we would generate
    //   Project(A, X = X + 0 + 0)
    //   Project(A, X = X + 0 + 0 + 0)
    // also in the same subset. They are valid but useless.
    final Deque<RelSubset> subsets = new ArrayDeque<>();
    try {
      checkDuplicateSubsets(subsets, match.rule.getOperand(), match.rels);
    } catch (Util.FoundOne e) {
      return true;
    }
    return false;
  }

  /** Recursively checks whether there are any duplicate subsets along any path
   * from root of the operand tree to one of the leaves.
   *
   * <p>It is OK for a match to have duplicate subsets if they are not on the
   * same path. For example,
   *
   * <blockquote><pre>
   *   Join
   *  /   \
   * X     X
   * </pre></blockquote>
   *
   * <p>is a valid match.
   *
   * @throws org.apache.calcite.util.Util.FoundOne on match
   */
  private void checkDuplicateSubsets(Deque<RelSubset> subsets,
      RelOptRuleOperand operand, RelNode[] rels) {
    final RelSubset subset = planner.getSubset(rels[operand.ordinalInRule]);
    if (subsets.contains(subset)) {
      throw Util.FoundOne.NULL;
    }
    if (!operand.getChildOperands().isEmpty()) {
      subsets.push(subset);
      for (RelOptRuleOperand childOperand : operand.getChildOperands()) {
        checkDuplicateSubsets(subsets, childOperand, rels);
      }
      final RelSubset x = subsets.pop();
      assert x == subset;
    }
  }

  /**
   * Returns the importance of a child to a parent. This is defined by the
   * importance of the parent, pro-rated by the cost of the child. For
   * example, if the parent has importance = 0.8 and cost 100, then a child
   * with cost 50 will have importance 0.4, and a child with cost 25 will have
   * importance 0.2.
   */
  private double computeImportanceOfChild(RelMetadataQuery mq, RelSubset child,
      RelSubset parent) {
    final double parentImportance = getImportance(parent);
    final double childCost = toDouble(planner.getCost(child, mq));
    final double parentCost = toDouble(planner.getCost(parent, mq));
    double alpha = childCost / parentCost;
    if (alpha >= 1.0) {
      // child is always less important than parent
      alpha = 0.99;
    }
    final double importance = parentImportance * alpha;
    LOGGER.trace("Importance of [{}] to its parent [{}] is {} (parent importance={}, child cost={},"
        + " parent cost={})", child, parent, importance, parentImportance, childCost, parentCost);
    return importance;
  }

  /**
   * Converts a cost to a scalar quantity.
   */
  private double toDouble(RelOptCost cost) {
    if (cost.isInfinite()) {
      return 1e+30;
    } else {
      return cost.getCpu() + cost.getRows() + cost.getIo();
    }
  }

  private static double computeOneMinusEpsilon() {
    for (double d = 0d;;) {
      double d0 = d;
      d = (d + 1d) / 2d;
      if (d == 1.0) {
        return d0;
      }
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Compares {@link RelNode} objects according to their cached 'importance'.
   */
  private class RelImportanceComparator implements Comparator<RelSubset> {
    public int compare(
        RelSubset rel1,
        RelSubset rel2) {
      double imp1 = getImportance(rel1);
      double imp2 = getImportance(rel2);
      int c = Double.compare(imp2, imp1);
      if (c == 0) {
        c = rel1.getId() - rel2.getId();
      }
      return c;
    }
  }

  /**
   * Compares {@link VolcanoRuleMatch} objects according to their importance.
   * Matches which are more important collate earlier. Ties are adjudicated by
   * comparing the {@link RelNode#getId id}s of the relational expressions
   * matched.
   */
  private static class RuleMatchImportanceComparator
      implements Comparator<VolcanoRuleMatch> {
    public int compare(VolcanoRuleMatch match1,
        VolcanoRuleMatch match2) {
      double imp1 = match1.getImportance();
      double imp2 = match2.getImportance();
      int c = Double.compare(imp1, imp2);
      if (c != 0) {
        return -c;
      }
      c = match1.rule.getClass().getName()
          .compareTo(match2.rule.getClass().getName());
      if (c != 0) {
        return -c;
      }
      return -RelNodes.compareRels(match1.rels, match2.rels);
    }
  }

  /**
   * PhaseMatchList represents a set of {@link VolcanoRuleMatch rule-matches}
   * for a particular
   * {@link VolcanoPlannerPhase phase of the planner's execution}.
   */
  private static class PhaseMatchList {
    /**
     * The VolcanoPlannerPhase that this PhaseMatchList is used in.
     */
    final VolcanoPlannerPhase phase;

    /**
     * Current list of VolcanoRuleMatches for this phase. New rule-matches
     * are appended to the end of this list. When removing a rule-match, the
     * list is sorted and the highest importance rule-match removed. It is
     * important for performance that this list remain mostly sorted.
     *
     * <p>Use a hunkList because {@link java.util.ArrayList} does not implement
     * remove(0) efficiently.</p>
     */
    final List<VolcanoRuleMatch> list = new ChunkList<>();

    /**
     * A set of rule-match names contained in {@link #list}. Allows fast
     * detection of duplicate rule-matches.
     */
    final Set<String> names = new HashSet<>();

    /**
     * Multi-map of RelSubset to VolcanoRuleMatches. Used to
     * {@link VolcanoRuleMatch#clearCachedImportance() clear} the rule-match's
     * cached importance when the importance of a related RelSubset is modified
     * (e.g., due to invocation of
     * {@link RuleQueue#boostImportance(Collection, double)}).
     */
    final Multimap<RelSubset, VolcanoRuleMatch> matchMap =
        HashMultimap.create();

    PhaseMatchList(VolcanoPlannerPhase phase) {
      this.phase = phase;
    }

    void clear() {
      list.clear();
      names.clear();
      matchMap.clear();
    }
  }
}

// End RuleQueue.java
