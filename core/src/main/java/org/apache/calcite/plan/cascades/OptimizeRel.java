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
package org.apache.calcite.plan.cascades;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.calcite.plan.RelOptRuleOperandChildPolicy.SOME;
import static org.apache.calcite.plan.RelOptRuleOperandChildPolicy.UNORDERED;
import static org.apache.calcite.plan.cascades.CascadesUtils.isLogical;

/**
 * Task that performs logical and physical optimization for a given {@link RelNode}.
 * It schedules rules invocation and logical exploration of inputs (this is needed for
 * the rule operands matching).
 */
class OptimizeRel extends CascadesTask {
  private final RelNode rel;
  private final RelTraitSet traits;
  private final boolean explore;

  OptimizeRel(CascadesTask parentTask, RelTraitSet traits, RelNode rel,
      boolean explore) {
    super(parentTask);
    assert isLogical(rel)  : "rel=" + rel;
    this.rel = rel;
    this.traits = traits;
    this.explore = explore;
  }

  @Override public void perform() {
    List<RelOptRule> rules = new ArrayList<>();

    rules.addAll(findMatchedRules(planner.logicalRules()));

    if (!explore) {
      List<RelOptRule> physRules = findMatchedRules(planner.physicalRules());
      if (physRules.isEmpty()) {
        throw new RelOptPlanner.CannotPlanException("Converter is absent for rel=" + rel);
      }
      rules.addAll(physRules);
    }

    for (RelOptRule rule : rules) {
      planner.submitTask(new ApplyRule(this, rel, rule));
      // Expand input groups.
      Set<RelGroup> groupsToExpand = new HashSet<>();
      RelOptRuleOperand topOperand = rule.getOperand();
      RelNode topRel = rel;
      gatherGroupsToExpand(topOperand, topRel, groupsToExpand);
      for (RelGroup group : groupsToExpand) {
        if (!group.isExpanded()) {
          planner.submitTask(new ExpandGroup(this, group));
        }
      }
    }
  }

  /**
   * @param ruleSet Set of rules.
   * @return List of rules which top operand matched with the given Rel.
   */
  private List<RelOptRule> findMatchedRules(Set<RelOptRule> ruleSet) {
    ArrayList<RelOptRule> rules = new ArrayList<>();
    for (RelOptRule rule : ruleSet) {
      if (!planner.isFired(rule, rel)
          && rule.getOperand().matches(rel)) {
        rules.add(rule);
      }
    }
    return rules;
  }

  /**
   * Collects set of groups that should be expanded before rule matching.
   * TODO we can do it more efficiently.
   * @param topOperand Top rule operand.
   * @param topRel Top rel.
   * @param groupsToExpand Collector of the groups for expansion.
   */
  private void gatherGroupsToExpand(RelOptRuleOperand topOperand, RelNode topRel,
      Set<RelGroup> groupsToExpand) {
    if (topOperand.childPolicy == SOME || topOperand.childPolicy == UNORDERED) {
      for (RelOptRuleOperand childOp : topOperand.getChildOperands()) {
        for (RelNode input : topRel.getInputs()) {
          RelGroup group = planner.getGroup(input);
          if (!group.isExpanded()) {
            groupsToExpand.add(group);
          }
          gatherGroupsToExpand(childOp, input, groupsToExpand);
        }
      }
    }
  }

  @Override protected void description(StringBuilder stringBuilder) {
    stringBuilder
        .append("OptimizeRel{rel=")
        .append(rel)
        .append(", traits=")
        .append(traits)
        .append(", explore=")
        .append(explore)
        .append('}');
  }
}
