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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.sql2rel.RelDecorrelator;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * This rule invokes the standard decorrelator {@link RelDecorrelator} on subplans of a query
 * plan that are rooted at a {@link Correlate} node.  By invoking this rule repeatedly
 * one essentially obtains a bottom-up application of the decorrelator, where
 * {@link Correlate} nodes closer to leaves are processed first.  This strategy may
 * enable decorrelating plans where applying the decorrelator at the root may fail.
 * (The standard decorrelator is all-or-nothing, if it fails, it returns the original plan
 * unchanged.  This strategy may still simplify some plans by decorrelating parts of them).
 */
@Value.Enclosing
public class BottomUpDecorrelateRule
    extends RelRule<BottomUpDecorrelateRule.Config>
    implements TransformationRule {

  protected BottomUpDecorrelateRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    Correlate cor = call.rel(0);
    for (RelNode input : cor.stripped().getInputs()) {
      if (containsCorrelate(input)) {
        // Give up
        return;
      }
    }

    RelNode stripped = stripRecursively(cor);
    RelNode rel = RelDecorrelator.decorrelateQuery(stripped, call.builder());
    if (rel != stripped) {
      call.transformTo(rel);
    }
  }

  /** Strip all information added by the planner from the subtree rooted at {@code node}. */
  static RelNode stripRecursively(RelNode node) {
    RelNode stripped = node.stripped();
    List<RelNode> strippedInputs = new ArrayList<>();
    for (RelNode input : stripped.getInputs()) {
      strippedInputs.add(stripRecursively(input));
    }
    return stripped.copy(node.getTraitSet(), strippedInputs);
  }

  /**
   * Check if a Query tree contains a {@link Correlate} node.
   *
   * @return True if there is any instance of {@link Correlate} in the specified tree.
   * @param node Root of tree that is inspected for {@link Correlate} nodes. */
  static boolean containsCorrelate(RelNode node) {
    if (node instanceof Correlate) {
      return true;
    }
    for (RelNode input : node.getInputs()) {
      if (containsCorrelate(input.stripped())) {
        return true;
      }
    }
    return false;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    BottomUpDecorrelateRule.Config DEFAULT = ImmutableBottomUpDecorrelateRule.Config.of()
        .withOperandSupplier(b0 -> b0.operand(Correlate.class)
            .anyInputs())
        .as(Config.class);

    @Override default BottomUpDecorrelateRule toRule() {
      return new BottomUpDecorrelateRule(this);
    }
  }
}
