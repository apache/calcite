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
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

/**
 * Rule that replaces {@link SetOp} operator with {@link Filter}
 * when both inputs are from the same source with only filter conditions differing.
 * For nested filters, the rule {@link CoreRules#FILTER_MERGE}
 * should be used prior to invoking this one.
 *
 * <p>Example:
 *
 * <p>UNION
 * <blockquote><pre>
 * SELECT mgr, comm FROM emp WHERE mgr = 12
 * UNION
 * SELECT mgr, comm FROM emp WHERE comm = 5
 *
 * is rewritten to
 *
 * SELECT DISTINCT mgr, comm FROM emp
 * WHERE mgr = 12 OR comm = 5
 * </pre></blockquote>
 *
 * <p>INTERSECT
 * <blockquote><pre>
 * SELECT mgr, comm FROM emp WHERE mgr = 12
 * INTERSECT
 * SELECT mgr, comm FROM emp WHERE comm = 5
 *
 * is rewritten to
 *
 * SELECT DISTINCT mgr, comm FROM emp
 * WHERE mgr = 12 AND comm = 5
 * </pre></blockquote>
 *
 * <p>EXCEPT
 * <blockquote><pre>
 * SELECT mgr, comm FROM emp WHERE mgr = 12
 * EXCEPT
 * SELECT mgr, comm FROM emp WHERE comm = 5
 *
 * is rewritten to
 *
 * SELECT DISTINCT mgr, comm FROM emp
 * WHERE mgr = 12 AND NOT(comm = 5)
 * </pre></blockquote>
 */
@Value.Enclosing
public class SetOpToFilterRule
    extends RelRule<SetOpToFilterRule.Config>
    implements TransformationRule {

  /** Creates an SetOpToFilterRule. */
  protected SetOpToFilterRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    config.matchHandler().accept(this, call);
  }

  private void match(RelOptRuleCall call) {
    final SetOp setOp = call.rel(0);
    if (setOp.all || setOp.getInputs().size() != 2) {
      return;
    }
    final RelBuilder builder = call.builder();
    final RelNode leftInput = call.rel(1);
    final Filter rightFilter = call.rel(2);

    if (!RexUtil.isDeterministic(rightFilter.getCondition())
        || RexUtil.SubQueryFinder.containsSubQuery(rightFilter)) {
      return;
    }

    RelNode leftBase;
    RexNode leftCond = null;
    if (leftInput instanceof Filter) {
      Filter leftFilter = (Filter) leftInput;
      if (RexUtil.SubQueryFinder.containsSubQuery(leftFilter)) {
        return;
      }
      leftBase = leftFilter.getInput().stripped();
      leftCond = leftFilter.getCondition();
    } else {
      leftBase = leftInput.stripped();
    }

    final RelNode rightBase = rightFilter.getInput().stripped();
    if (!leftBase.equals(rightBase)) {
      return;
    }

    RexNode finalCond = null;
    // Right input is Filter, right cond should be not null
    if (setOp instanceof Union) {
      finalCond = leftCond != null
          ? builder.or(leftCond, rightFilter.getCondition())
          : builder.literal(true);
    } else if (setOp instanceof Intersect) {
      finalCond = leftCond != null
          ? builder.and(leftCond, rightFilter.getCondition())
          : rightFilter.getCondition();
    } else if (setOp instanceof Minus) {
      finalCond = leftCond != null
          ? builder.and(leftCond, builder.not(rightFilter.getCondition()))
          : builder.not(rightFilter.getCondition());
    } else {
      // unreachable
      throw new IllegalStateException("unreachable code");
    }

    builder.push(leftBase)
        .filter(finalCond)
        .distinct();

    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config UNION = ImmutableSetOpToFilterRule.Config.builder()
        .withMatchHandler(SetOpToFilterRule::match)
        .build()
        .withOperandSupplier(
            b0 -> b0.operand(Union.class).inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(Filter.class).anyInputs()))
        .as(Config.class);

    Config INTERSECT = ImmutableSetOpToFilterRule.Config.builder()
        .withMatchHandler(SetOpToFilterRule::match)
        .build()
        .withOperandSupplier(
            b0 -> b0.operand(Intersect.class).inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(Filter.class).anyInputs()))
        .as(Config.class);

    Config MINUS = ImmutableSetOpToFilterRule.Config.builder()
        .withMatchHandler(SetOpToFilterRule::match)
        .build()
        .withOperandSupplier(
            b0 -> b0.operand(Minus.class).inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(Filter.class).anyInputs()))
        .as(Config.class);

    @Override default SetOpToFilterRule toRule() {
      return new SetOpToFilterRule(this);
    }

    /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
    MatchHandler<SetOpToFilterRule> matchHandler();

    /** Sets {@link #matchHandler()}. */
    Config withMatchHandler(MatchHandler<SetOpToFilterRule> matchHandler);
  }
}
