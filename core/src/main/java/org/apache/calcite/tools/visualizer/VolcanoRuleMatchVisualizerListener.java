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
package org.apache.calcite.tools.visualizer;

import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.plan.volcano.VolcanoRuleMatchVisualizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Visualizer Listener listens to events of Volcano Planner,
 * whenever a rule is successfully applied, it adds the information to the visualizer.
 */
public class VolcanoRuleMatchVisualizerListener implements RelOptListener {

  private VolcanoRuleMatchVisualizer visualizer;
  private String latestRuleID = "";
  private int latestRuleTransformCount = 1;

  public VolcanoRuleMatchVisualizerListener(VolcanoPlanner volcanoPlanner) {
    visualizer = new VolcanoRuleMatchVisualizer(volcanoPlanner);
  }

  /**
   * After a rule is matched, record the rule and the state after matching
   */
  @Override public void ruleAttempted(RuleAttemptedEvent event) {
  }

  @Override public void relChosen(RelChosenEvent event) {
  }

  @Override public void ruleProductionSucceeded(RuleProductionEvent event) {
    RelOptPlanner planner = event.getRuleCall().getPlanner();
    if (!(planner instanceof VolcanoPlanner)) {
      return;
    }

    // ruleAttempted is called once before ruleMatch, and once after ruleMatch
    if (event.isBefore()) {
      // add the initialState
      if (latestRuleID.isEmpty()) {
        visualizer.addRuleMatch("INITIAL", new ArrayList<>(), false);
        this.latestRuleID = "INITIAL";
      }
      return;
    }

    // we add the state after the rule is applied
    RelOptRuleCall ruleCall = event.getRuleCall();
    String ruleID = Integer.toString(ruleCall.id);

    String displayRuleName = ruleCall.id + "-" + ruleCall.getRule().toString();

    // a rule might call transform to multiple times, handle it by modifying the rule name
    if (ruleID.equals(this.latestRuleID)) {
      latestRuleTransformCount++;
      displayRuleName += "-" + latestRuleTransformCount;
    } else {
      latestRuleTransformCount = 1;
    }
    this.latestRuleID = ruleID;

    visualizer.addRuleMatch(displayRuleName, Arrays.stream(ruleCall.rels)
        .collect(Collectors.toList()), true);
  }

  @Override public void relDiscarded(RelDiscardedEvent event) {
  }

  @Override public void relEquivalenceFound(RelEquivalenceEvent event) {
  }

  public VolcanoRuleMatchVisualizer getVisualizer() {
    return visualizer;
  }
}
