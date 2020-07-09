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
import org.apache.calcite.util.Util;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A data structure that manages rule matches for RuleDriver.
 * Different RuleDriver requires different ways to pop matches,
 * thus different ways to store rule matches that are not called.
 */
public abstract class RuleQueue {

  protected final VolcanoPlanner planner;

  protected RuleQueue(VolcanoPlanner planner) {
    this.planner = planner;
  }

  /**
   * Add a RuleMatch into the queue
   * @param match rule match to add
   */
  public abstract void addMatch(VolcanoRuleMatch match);

  /**
   * clear this rule queue.
   * The return value indicates whether the rule queue was empty before clear
   * @return true if the rule queue was not empty
   */
  public abstract boolean clear();


  /** Returns whether to skip a match. This happens if any of the
   * {@link RelNode}s have importance zero. */
  protected boolean skipMatch(VolcanoRuleMatch match) {
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
}
