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

import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.slf4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * Priority queue of relexps whose rules have not been called, and rule-matches
 * which have not yet been acted upon.
 */
class RuleQueue {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  private static final Set<String> ALL_RULES = ImmutableSet.of("<ALL RULES>");

  //~ Instance fields --------------------------------------------------------

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

  private final VolcanoPlanner planner;

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
   * Adds a rule match. The rule-matches are automatically added to all
   * existing {@link PhaseMatchList per-phase rule-match lists} which allow
   * the rule referenced by the match.
   */
  void addMatch(VolcanoRuleMatch match) {
    final String matchName = match.toString();
    for (PhaseMatchList matchList : matchListMap.values()) {
      Set<String> phaseRuleSet = phaseRuleMapping.get(matchList.phase);
      if (phaseRuleSet != ALL_RULES) {
        String ruleDescription = match.getRule().toString();
        if (!phaseRuleSet.contains(ruleDescription)) {
          continue;
        }
      }

      if (!matchList.names.add(matchName)) {
        // Identical match has already been added.
        continue;
      }

      LOGGER.trace("{} Rule-match queued: {}", matchList.phase.toString(), matchName);

      matchList.offer(match);

      matchList.matchMap.put(
          planner.getSubset(match.rels[0]), match);
    }
  }

  /**
   * Removes the rule match from the head of match list, and returns it.
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
    dumpPlannerState();

    PhaseMatchList phaseMatchList = matchListMap.get(phase);
    if (phaseMatchList == null) {
      throw new AssertionError("Used match list for phase " + phase
          + " after phase complete");
    }

    VolcanoRuleMatch match;
    for (;;) {
      if (phaseMatchList.size() == 0) {
        return null;
      }

      dumpRuleQueue(phaseMatchList);

      match = phaseMatchList.poll();

      if (skipMatch(match)) {
        LOGGER.debug("Skip match: {}", match);
      } else {
        break;
      }
    }

    // If sets have merged since the rule match was enqueued, the match
    // may not be removed from the matchMap because the subset may have
    // changed, it is OK to leave it since the matchMap will be cleared
    // at the end.
    phaseMatchList.matchMap.remove(
        planner.getSubset(match.rels[0]), match);

    LOGGER.debug("Pop match: {}", match);
    return match;
  }

  /**
   * Dumps rules queue to the logger when debug level is set to {@code TRACE}.
   */
  private void dumpRuleQueue(PhaseMatchList phaseMatchList) {
    if (LOGGER.isTraceEnabled()) {
      StringBuilder b = new StringBuilder();
      b.append("Rule queue:");
      for (VolcanoRuleMatch rule : phaseMatchList.preQueue) {
        b.append("\n");
        b.append(rule);
      }
      for (VolcanoRuleMatch rule : phaseMatchList.queue) {
        b.append("\n");
        b.append(rule);
      }
      LOGGER.trace(b.toString());
    }
  }

  /**
   * Dumps planner's state to the logger when debug level is set to {@code TRACE}.
   */
  private void dumpPlannerState() {
    if (LOGGER.isTraceEnabled()) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      planner.dump(pw);
      pw.flush();
      LOGGER.trace(sw.toString());
      planner.getRoot().getCluster().invalidateMetadataQuery();
    }
  }

  /** Returns whether to skip a match. This happens if any of the
   * {@link RelNode}s have importance zero. */
  private boolean skipMatch(VolcanoRuleMatch match) {
    for (RelNode rel : match.rels) {
      if (planner.prunedNodes.contains(rel)) {
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

  //~ Inner Classes ----------------------------------------------------------

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
     * Rule match queue for SubstitutionRule
     */
    private final Queue<VolcanoRuleMatch> preQueue = new LinkedList<>();

    /**
     * Current list of VolcanoRuleMatches for this phase. New rule-matches
     * are appended to the end of this queue.
     * The rules are not sorted in any way.
     */
    private final Queue<VolcanoRuleMatch> queue = new LinkedList<>();

    /**
     * A set of rule-match names contained in {@link #queue}. Allows fast
     * detection of duplicate rule-matches.
     */
    final Set<String> names = new HashSet<>();

    /**
     * Multi-map of RelSubset to VolcanoRuleMatches.
     */
    final Multimap<RelSubset, VolcanoRuleMatch> matchMap =
        HashMultimap.create();

    PhaseMatchList(VolcanoPlannerPhase phase) {
      this.phase = phase;
    }

    int size() {
      return preQueue.size() + queue.size();
    }

    VolcanoRuleMatch poll() {
      VolcanoRuleMatch match = preQueue.poll();
      if (match == null) {
        match = queue.poll();
      }
      return match;
    }

    void offer(VolcanoRuleMatch match) {
      if (match.getRule() instanceof SubstitutionRule) {
        preQueue.offer(match);
      } else {
        queue.offer(match);
      }
    }

    void clear() {
      preQueue.clear();
      queue.clear();
      names.clear();
      matchMap.clear();
    }
  }
}
