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
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

import static java.util.Objects.requireNonNull;

/***
 * The algorithm executes repeatedly. The exact rules
 * that may be fired varies.
 *
 * <p>The planner iterates over the rule matches presented
 * by the rule queue until the rule queue becomes empty.
 */
class IterativeRuleDriver implements RuleDriver {

  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  private final VolcanoPlanner planner;
  private final IterativeRuleQueue ruleQueue;

  IterativeRuleDriver(VolcanoPlanner planner) {
    this.planner = planner;
    ruleQueue = new IterativeRuleQueue(planner);
  }

  @Override public IterativeRuleQueue getRuleQueue() {
    return ruleQueue;
  }

  @Override public void drive() {
    while (true) {
      requireNonNull(planner.root, "RelSubset must not be null at this point");
      LOGGER.debug("Best cost before rule match: {}", planner.root.bestCost);

      VolcanoRuleMatch match = ruleQueue.popMatch();
      if (match == null) {
        break;
      }

      assert match.getRule().matches(match);
      try {
        match.onMatch();
      } catch (VolcanoTimeoutException e) {
        LOGGER.warn("Volcano planning times out, cancels the subsequent optimization.");
        planner.canonize();
        break;
      }

      // The root may have been merged with another
      // subset. Find the new root subset.
      planner.canonize();
    }

  }

  @Override public void onProduce(RelNode rel, RelSubset subset) {
  }

  @Override public void onSetMerged(RelSet set) {
  }

  @Override public void clear() {
    ruleQueue.clear();
  }
}
