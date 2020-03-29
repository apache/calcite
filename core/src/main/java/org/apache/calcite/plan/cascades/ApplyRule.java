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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.PhysicalNode;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.apache.calcite.plan.cascades.CascadesUtils.isLogical;

import static java.util.Collections.singletonList;

/**
 * Planner task that performs {@link RelOptRule} call binding and invocation.
 * If the result of the rule invocation is a logical {@link RelNode}, the task
 * {@link OptimizeRel} will be spawned for the further exploration and optimization.
 * If the result of the rule invocation is a {@link PhysicalNode}, the {@link OptimizeInputs}
 * will be spawned.
 */
public class ApplyRule extends CascadesTask {
  private final RelNode topRel;
  private final RelOptRule rule;

  public ApplyRule(CascadesTask parentTask, RelNode rel, RelOptRule rule) {
    super(parentTask);
    assert rule != null;
    assert rel != null;
    assert isLogical(rel);
    this.topRel = rel;
    this.rule = rule;
  }

  @Override public void perform() {
    if (planner.isFired(rule, topRel)) {
      return;
    }

    Bindery bindery = new Bindery();
    List<CascadesRuleCall> bindings = bindery.bindRuleOperands();

    for (CascadesRuleCall call : bindings) {
      call.setRequestedTraits(requestedTraits);

      call.match();

      List<RelNode> convertedRels = call.getConvertedRels();

      for (RelNode convertedRel : convertedRels) {
        // SubGroup usually returned from logical rules where the rel replaced with its child.
        if (convertedRel instanceof RelSubGroup) {
          continue;
        }

        if (isLogical(convertedRel)) {
          // Rel was not registered in planner because the same rel is already registered.
          if (planner.getGroup(convertedRel) == null) {
            continue;
          }

          OptimizeRel task =
              new OptimizeRel(this, convertedRel.getTraitSet(), convertedRel, false);
          planner.submitTask(task);
        } else {
          // TODO upper bound
          OptimizeInputs task = new OptimizeInputs(this, convertedRel.getTraitSet(), convertedRel,
              planner.getGroup(topRel), Double.POSITIVE_INFINITY);
          planner.submitTask(task);
        }
      }
    }
    // Do not mark implementation rules as fired because we will fire it again
    // with another search context (another required traits)
    if (!(rule instanceof ImplementationRule)) {
      planner.addFired(rule, topRel);
    }
  }

  @Override protected void description(StringBuilder stringBuilder) {
    stringBuilder
        .append("ApplyRule{rule=")
        .append(rule)
        .append(", rel=")
        .append(topRel)
        .append('}');
  }

  /**
   * Helper class for rule operands matching.
   */
  private class Bindery {
    private RelNode[] matchedRels;
    private List<CascadesRuleCall> bindings;

    List<CascadesRuleCall> bindRuleOperands() {
      assert rule.getOperand().matches(topRel) : "rel=" + topRel + ", rule=" + rule;
      matchedRels = new RelNode[rule.operands.size()];
      bindings = new ArrayList<>();
      matchRecurse(rule.getOperand(), topRel);
      return bindings;
    }

    private void matchRecurse(RelOptRuleOperand op, RelNode rel) {
      boolean isRoot = rel == topRel;

      if (!isRoot && matchedRels[op.ordinalInRule - 1] == null) {
        return; // Exit if matching of the previous operand failed.
      }

      // The root rel is matched only with root operand, so we do not explore entire group.
      Collection<RelNode> candidates = isRoot
          ? singletonList(topRel)
          : planner.getGroup(rel).logicalRels();

      for (RelNode candidate : candidates) {
        if (!op.matches(candidate)) {
          continue;
        }

        matchedRels[op.ordinalInRule] = candidate;

        if (op.ordinalInRule == rule.getOperands().size() - 1) { // All operands matched.
          RelNode[] relsCopy = Arrays.copyOf(matchedRels, matchedRels.length);
          CascadesRuleCall binding = new CascadesRuleCall(planner, rule.getOperand(), relsCopy);
          bindings.add(binding);
          return;
        }

        List<RelOptRuleOperand> childOps = op.getChildOperands();
        List<RelNode> inputs = candidate.getInputs();
        switch (op.childPolicy) {
        case ANY:
        case LEAF:
          break; // Continue matching.

        case SOME:
          for (int i = 0; i < childOps.size(); i++) {
            RelOptRuleOperand childOp = childOps.get(i);
            RelNode input = inputs.get(i);
            matchRecurse(childOp, input);
          }
          break;

        case UNORDERED:
          RelOptRuleOperand childOp = childOps.get(0);
          for (int i = 0; i < inputs.size(); i++) {
            RelNode input = inputs.get(i);
            matchRecurse(childOp, input);
          }
          break;

        default:
          throw new UnsupportedOperationException("Unknown child policy:" + op.childPolicy);
        }
      }
    }
  }
}
