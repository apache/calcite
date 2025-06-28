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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

/**
 * Rule that replaces {@link Minus} operator with {@link Filter}
 * when both inputs are from the same source with only filter conditions differing.
 * Only inspect a single {@link Filter} layer in the inputs of {@link Minus}.
 * For inputs with nested {@link Filter}s, apply {@link CoreRules#FILTER_MERGE}
 * as a preprocessing step.
 *
 * <p>Example transformation:
 * <blockquote><pre>
 * SELECT mgr, comm FROM emp WHERE mgr = 12
 * EXCEPT
 * SELECT mgr, comm FROM emp WHERE comm = 5
 *
 * to
 *
 * SELECT DISTINCT mgr, comm FROM emp
 * WHERE mgr = 12 AND NOT(comm = 5)
 * </pre></blockquote>
 */
@Value.Enclosing
public class MinusToFilterRule
    extends RelRule<MinusToFilterRule.Config>
    implements TransformationRule {

  /**
   * This rule is replaced by {@link SetOpToFilterRule}.
   * Please see {@link CoreRules#MINUS_FILTER_TO_FILTER} */
  @Deprecated
  protected MinusToFilterRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Minus minus = call.rel(0);
    if (minus.all || minus.getInputs().size() != 2) {
      return;
    }
    final RelBuilder builder = call.builder();
    final RelNode leftInput = call.rel(1);
    final Filter rightInput = call.rel(2);

    if (!RexUtil.isDeterministic(rightInput.getCondition())) {
      return;
    }

    RelNode leftBase;
    RexNode leftCond = null;
    if (leftInput instanceof Filter) {
      Filter leftFilter = (Filter) leftInput;
      leftBase = leftFilter.getInput().stripped();
      leftCond = leftFilter.getCondition();
    } else {
      leftBase = leftInput.stripped();
    }

    final RelNode rightBase = rightInput.getInput().stripped();
    if (!leftBase.equals(rightBase)) {
      return;
    }

    // Right input is Filter, right cond should be not null
    final RexNode finalCond = leftCond != null
        ? builder.and(leftCond, builder.not(rightInput.getCondition()))
        : builder.not(rightInput.getCondition());

    builder.push(leftBase)
        .filter(finalCond)
        .distinct();

    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableMinusToFilterRule.Config.of()
        .withOperandFor(Minus.class, RelNode.class, Filter.class);

    @Override default MinusToFilterRule toRule() {
      return new MinusToFilterRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Minus> minusClass,
        Class<? extends RelNode> relNodeClass, Class<? extends Filter> filterClass) {
      return withOperandSupplier(
          b0 -> b0.operand(minusClass).inputs(
              b1 -> b1.operand(relNodeClass).anyInputs(),
              b2 -> b2.operand(filterClass).anyInputs()))
          .as(Config.class);
    }
  }
}
