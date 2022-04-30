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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.util.trace.CalciteTrace;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Priority queue of relexps whose rules have not been called, and rule-matches
 * which have not yet been acted upon.
 */
class IterativeRuleQueue extends RuleQueue {
  //~ Static fields/initializers ---------------------------------------------

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  //~ Instance fields --------------------------------------------------------

  /**
   * The list of rule-matches. Initially, there is an empty {@link MatchList}.
   * As the planner invokes {@link #addMatch(VolcanoRuleMatch)} the rule-match
   * is added to the appropriate MatchList(s). As the planner completes the
   * match, the matching entry is removed from this list to avoid unused work.
   */
  final MatchList matchList = new MatchList();

  //~ Constructors -----------------------------------------------------------

  IterativeRuleQueue(VolcanoPlanner planner) {
    super(planner);
  }

  //~ Methods ----------------------------------------------------------------
  /**
   * Clear internal data structure for this rule queue.
   */
  @Override public boolean clear() {
    boolean empty = true;
    if (!matchList.queue.isEmpty() || !matchList.preQueue.isEmpty()) {
      empty = false;
    }
    matchList.clear();
    return !empty;
  }

  /**
   * Add a rule match.
   */
  @Override public void addMatch(VolcanoRuleMatch match) {
    final String matchName = match.toString();

    if (!matchList.names.add(matchName)) {
      // Identical match has already been added.
      return;
    }

    LOGGER.trace("Rule-match queued: {}", matchName);

    matchList.offer(match);

    matchList.matchMap.put(
        requireNonNull(planner.getSubset(match.rels[0])), match);
  }

  /**
   * Removes the rule match from the head of match list, and returns it.
   *
   * <p>Returns {@code null} if there are no more matches.</p>
   *
   * <p>Note that the VolcanoPlanner may still decide to reject rule matches
   * which have become invalid, say if one of their operands belongs to an
   * obsolete set or has been pruned.
   *
   */
  public @Nullable VolcanoRuleMatch popMatch() {
    dumpPlannerState();

    VolcanoRuleMatch match;
    for (;;) {
      if (matchList.size() == 0) {
        return null;
      }

      dumpRuleQueue(matchList);

      match = matchList.poll();
      if (match == null) {
        return null;
      }

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
    matchList.matchMap.remove(
        planner.getSubset(match.rels[0]), match);

    LOGGER.debug("Pop match: {}", match);
    return match;
  }

  /**
   * Dumps rules queue to the logger when debug level is set to {@code TRACE}.
   */
  private static void dumpRuleQueue(MatchList matchList) {
    if (LOGGER.isTraceEnabled()) {
      StringBuilder b = new StringBuilder();
      b.append("Rule queue:");
      for (VolcanoRuleMatch rule : matchList.preQueue) {
        b.append("\n");
        b.append(rule);
      }
      for (VolcanoRuleMatch rule : matchList.queue) {
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
      RelNode root = planner.getRoot();
      if (root != null) {
        root.getCluster().invalidateMetadataQuery();
      }
    }
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * MatchList represents a set of {@link VolcanoRuleMatch rule-matches}.
   */
  private static class MatchList {

    /**
     * Rule match queue for SubstitutionRule.
     */
    private final Queue<VolcanoRuleMatch> preQueue = new ArrayDeque<>();

    /**
     * Current list of VolcanoRuleMatches for this phase. New rule-matches
     * are appended to the end of this queue.
     * The rules are not sorted in any way.
     */
    private final Queue<VolcanoRuleMatch> queue = new ArrayDeque<>();

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

    int size() {
      return preQueue.size() + queue.size();
    }

    @Nullable VolcanoRuleMatch poll() {
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
