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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import org.immutables.value.Value;

import java.util.Map;

/**
 * Rule to expand disjunction for join inputs in condition of a {@link Filter} or {@link Join}.
 * For example:
 *
 * <blockquote><pre>{@code
 * select t1.id from t1, t2, t3
 * where t1.id = t2.id
 * and t1.id = t3.id
 * and (
 *     (t1.age < 50 and t3.age > 20)
 *     or
 *     (t2.weight > 70 and t3.height < 180)
 * )
 * }</pre></blockquote>
 *
 * <p>we can expand to obtain the condition
 *
 * <blockquote><pre>{@code
 * select t1.id from t1, t2, t3
 * where t1.id = t2.id
 * and t1.id = t3.id
 * and (
 *     (t1.age < 50 and t3.age > 20)
 *     or
 *     (t2.weight > 70 and t3.height < 180)
 * )
 * and (t1.age < 50 or t2.weight > 70)
 * and (t3.age > 20 or t3.height < 180)
 * }</pre></blockquote>
 *
 * <p>new generated predicates are redundant, but they could be pushed down to
 * join inputs([t1, t2], [t3]) and reduce the cardinality.
 *
 * <p>Note: This rule is similar to the {@link ExpandDisjunctionForTableRule},
 * but this rule divides the predicates by Join inputs.
 *
 * @see CoreRules#EXPAND_FILTER_DISJUNCTION_LOCAL
 * @see CoreRules#EXPAND_JOIN_DISJUNCTION_LOCAL
 */
@Value.Enclosing
public class ExpandDisjunctionForJoinInputsRule
    extends RelRule<ExpandDisjunctionForJoinInputsRule.Config>
    implements TransformationRule {

  /**
   * Creates a ExpandDisjunctionForJoinInputsRule.
   */
  protected ExpandDisjunctionForJoinInputsRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    config.matchHandler().accept(this, call);
  }

  private static void matchFilter(ExpandDisjunctionForJoinInputsRule rule, RelOptRuleCall call) {
    Filter filter = call.rel(0);
    Join join = call.rel(1);
    RelBuilder builder = call.builder();

    ExpandDisjunctionForJoinInputsHelper helper =
        new ExpandDisjunctionForJoinInputsHelper(
            join.getLeft().getRowType().getFieldCount(),
            join.getRight().getRowType().getFieldCount(),
            join.getJoinType().canPushLeftFromAbove(),
            join.getJoinType().canPushRightFromAbove(),
            builder,
            rule.config.bloat());
    Map<String, RexNode> expandResult = helper.expand(filter.getCondition());

    RexNode newCondition =
        builder.and(
            filter.getCondition(),
            expandResult.getOrDefault(helper.leftKey, builder.literal(true)),
            expandResult.getOrDefault(helper.rightKey, builder.literal(true)));
    if (newCondition.equals(filter.getCondition())) {
      return;
    }
    Filter newFilter = filter.copy(filter.getTraitSet(), join, newCondition);
    call.transformTo(newFilter);
  }

  private static void matchJoin(ExpandDisjunctionForJoinInputsRule rule, RelOptRuleCall call) {
    Join join = call.rel(0);
    RelBuilder builder = call.builder();

    ExpandDisjunctionForJoinInputsHelper helper =
        new ExpandDisjunctionForJoinInputsHelper(
            join.getLeft().getRowType().getFieldCount(),
            join.getRight().getRowType().getFieldCount(),
            join.getJoinType().canPushLeftFromWithin(),
            join.getJoinType().canPushRightFromWithin(),
            builder,
            rule.config.bloat());
    Map<String, RexNode> expandResult = helper.expand(join.getCondition());

    RexNode newCondition =
        call.builder().and(
            join.getCondition(),
            expandResult.getOrDefault(helper.leftKey, builder.literal(true)),
            expandResult.getOrDefault(helper.rightKey, builder.literal(true)));
    if (newCondition.equals(join.getCondition())) {
      return;
    }
    Join newJoin =
        join.copy(
            join.getTraitSet(),
            newCondition,
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());
    call.transformTo(newJoin);
  }

  /**
   * Helper class to expand predicates.
   */
  private static class ExpandDisjunctionForJoinInputsHelper
      extends RexUtil.ExpandDisjunctionHelper<String> {

    private final ImmutableBitSet leftBitmap;

    private final ImmutableBitSet rightBitmap;

    private final boolean canPushLeft;

    private final boolean canPushRight;

    private final String leftKey = "left";

    private final String rightKey = "right";

    private ExpandDisjunctionForJoinInputsHelper(
        int leftFieldCount,
        int rightFieldCount,
        boolean canPushLeft,
        boolean canPushRight,
        RelBuilder relBuilder,
        int maxNodeCount) {
      super(relBuilder, maxNodeCount);
      this.leftBitmap = ImmutableBitSet.range(0, leftFieldCount);
      this.rightBitmap = ImmutableBitSet.range(leftFieldCount, leftFieldCount + rightFieldCount);
      this.canPushLeft = canPushLeft;
      this.canPushRight = canPushRight;
    }

    @Override protected boolean canReturnEarly(
        RexNode condition,
        Map<String, RexNode> additionalConditions) {
      boolean earlyReturn = false;

      ImmutableBitSet inputRefs = RelOptUtil.InputFinder.bits(condition);
      if (inputRefs.isEmpty()) {
        earlyReturn = true;
      }
      if (!inputRefs.isEmpty() && leftBitmap.contains(inputRefs)) {
        earlyReturn = true;
        if (canPushLeft) {
          // The condition already belongs to the left input and it can be pushed down.
          checkExpandCount(condition.nodeCount());
          additionalConditions.put(leftKey, condition);
        }
      }
      if (!inputRefs.isEmpty() && rightBitmap.contains(inputRefs)) {
        earlyReturn = true;
        if (canPushRight) {
          // The condition already belongs to the right input and it can be pushed down.
          checkExpandCount(condition.nodeCount());
          additionalConditions.put(rightKey, condition);
        }
      }
      return earlyReturn;
    }
  }

  /** Rule configuration. */
  @Value.Immutable(singleton = false)
  public interface Config extends RelRule.Config {
    Config FILTER = ImmutableExpandDisjunctionForJoinInputsRule.Config.builder()
        .withDescription("ExpandDisjunctionForJoinInputsRule(Filter)")
        .withMatchHandler(ExpandDisjunctionForJoinInputsRule::matchFilter)
        .build()
        .withOperandSupplier(b0 ->
            b0.operand(Filter.class).oneInput(b1 ->
                b1.operand(Join.class).anyInputs()));

    Config JOIN = ImmutableExpandDisjunctionForJoinInputsRule.Config.builder()
        .withDescription("ExpandDisjunctionForJoinInputsRule(Join)")
        .withMatchHandler(ExpandDisjunctionForJoinInputsRule::matchJoin)
        .build()
        .withOperandSupplier(b -> b.operand(Join.class).anyInputs());

    @Override default ExpandDisjunctionForJoinInputsRule toRule() {
      return new ExpandDisjunctionForJoinInputsRule(this);
    }

    /**
     * Limit how much complexity can increase during expanding.
     * Default is {@link RelOptUtil#DEFAULT_BLOAT} (100).
     */
    default int bloat() {
      return RelOptUtil.DEFAULT_BLOAT;
    }

    /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
    MatchHandler<ExpandDisjunctionForJoinInputsRule> matchHandler();

    /** Sets {@link #matchHandler()}. */
    Config withMatchHandler(MatchHandler<ExpandDisjunctionForJoinInputsRule> matchHandler);
  }
}
