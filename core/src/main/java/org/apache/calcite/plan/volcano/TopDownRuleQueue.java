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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A rule queue that manages rule matches for cascades planner.
 */
class TopDownRuleQueue extends RuleQueue {

  /**
   * A simplified way to simulate the promise mechanism.
   */
  enum Promise {
    PRUNED,
    EXPLORATION,
    IMPLEMENTATION,
    SUBSTITUTION
  }

  private final Map<RelSet, TopDownRuleMatchList> logicalMatches = new HashMap<>();

  private final Map<RelNode, List<VolcanoRuleMatch>> physicalMatches = new HashMap<>();

  private final Set<String> names = new HashSet<>();

  TopDownRuleQueue(VolcanoPlanner planner) {
    super(planner);
  }

  public void addMatch(VolcanoRuleMatch match) {
    if (!names.add(match.toString())) {
      return;
    }

    RelNode rel = match.rel(0);
    if (planner.isLogical(rel)) {
      logicalMatches.computeIfAbsent(
          planner.getSet(rel), r -> new TopDownRuleMatchList()).add(match);
    } else {
      physicalMatches.computeIfAbsent(rel, r -> new ArrayList<>()).add(match);
    }
  }

  @Override public @Nullable VolcanoRuleMatch popMatch() {
    for (TopDownRuleMatchList value : logicalMatches.values()) {
      if (value != null) {
        for (List<VolcanoRuleMatch> matches : value.ruleMatchLists) {
          if (matches != null && !matches.isEmpty()) {
            return matches.remove(matches.size() - 1);
          }
        }
      }
    }
    for (List<VolcanoRuleMatch> matches : physicalMatches.values()) {
      if (matches != null && matches.size() > 0) {
        return matches.remove(matches.size() - 1);
      }
    }
    return null;
  }

  /**
   * Gets the TopDownRuleMatchList associated with the corresponding RelSet.
   * This call is somewhat coupled with the rule driver in order to reduce
   * some overhead as it is frequently called.
   */
  TopDownRuleMatchList getMatchList(RelSet set) {
    return logicalMatches.get(set);
  }

  @Nullable List<VolcanoRuleMatch> getMatchForPhysicalNode(RelNode node) {
    List<VolcanoRuleMatch> matches = physicalMatches.get(node);
    if (matches != null && matches.isEmpty()) {
      physicalMatches.remove(node);
      return null;
    }
    return matches;
  }

  /**
   * Prune a rule match according to cost bounds.
   * Pruned matches are not dumped so that it could be recovered
   * in some corner cases.
   */
  public void prune(RelSet set, VolcanoRuleMatch match) {
    TopDownRuleMatchList matches = logicalMatches.get(set);
    if (matches != null) {
      matches.pruned(match);
    }
  }

  /**
   * Recover pruned matches for a RelSet.
   */
  public void recover(RelSet set) {
    TopDownRuleMatchList matches = logicalMatches.get(set);
    if (matches != null) {
      matches.restorePruned();
    }
  }

  @Override public void clear() {
    logicalMatches.clear();
    physicalMatches.clear();
    names.clear();
  }

  /**
   * Merge their rule queues when two RelSet are merged.
   */
  public void onSetMerged(RelSet dest, RelSet src) {
    TopDownRuleMatchList other = logicalMatches.get(src);
    if (other != null) {
      TopDownRuleMatchList current = logicalMatches.get(dest);
      if (current == null) {
        logicalMatches.put(dest, other);
      } else {
        current.mergeWith(other);
      }
    }
  }

  @Nullable Promise promise(VolcanoRuleMatch match) {
    if (skipMatch(match)) {
      return null;
    }
    if (planner.isSubstituteRule(match)) {
      return Promise.SUBSTITUTION;
    }
    if (planner.isTransformationRule(match)) {
      return Promise.EXPLORATION;
    }
    return Promise.IMPLEMENTATION;
  }

  /**
   * A simplified priority queue for rule matches, with priorities
   * Substitution rule > Implementation rule > Exploration rule.
   */
  class TopDownRuleMatchList {
    private final List<List<VolcanoRuleMatch>> ruleMatchLists;

    TopDownRuleMatchList() {
      ruleMatchLists = new ArrayList<>();
      for (Promise ignored : Promise.values()) {
        ruleMatchLists.add(null);
      }
    }

    @Nullable List<VolcanoRuleMatch> get(Promise promise) {
      return ruleMatchLists.get(promise.ordinal());
    }

    private void add(VolcanoRuleMatch match) {
      Promise p = promise(match);
      if (p != null) {
        add(p, match);
      }
    }

    private void add(Promise p, VolcanoRuleMatch match) {
      List<VolcanoRuleMatch> matches = ruleMatchLists.get(p.ordinal());
      if (matches == null) {
        matches = new ArrayList<>();
        ruleMatchLists.set(p.ordinal(), matches);
      }
      matches.add(match);
    }

    private void mergeWith(TopDownRuleMatchList other) {
      for (int i = 0; i < ruleMatchLists.size(); i++) {
        List<VolcanoRuleMatch> otherList = other.ruleMatchLists.get(i);
        if (otherList != null) {
          List<VolcanoRuleMatch> thisList = ruleMatchLists.get(i);
          if (thisList == null) {
            ruleMatchLists.set(i, otherList);
          } else {
            thisList.addAll(otherList);
          }
        }
      }
    }

    private void pruned(VolcanoRuleMatch match) {
      add(Promise.PRUNED, match);
    }

    private void restorePruned() {
      List<VolcanoRuleMatch> pruned = ruleMatchLists.get(0);
      if (pruned != null) {
        ruleMatchLists.set(0, null);
        pruned.forEach(this::add);
      }
    }
  }
}
