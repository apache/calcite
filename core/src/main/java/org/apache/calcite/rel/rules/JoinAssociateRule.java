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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that changes a join based on the associativity rule.
 *
 * <p>((a JOIN b) JOIN c) &rarr; (a JOIN (b JOIN c))
 *
 * <p>We do not need a rule to convert (a JOIN (b JOIN c)) &rarr;
 * ((a JOIN b) JOIN c) because we have
 * {@link JoinCommuteRule}.
 *
 * @see JoinCommuteRule
 * @see CoreRules#JOIN_ASSOCIATE
 */
@Value.Enclosing
public class JoinAssociateRule
    extends RelRule<JoinAssociateRule.Config>
    implements TransformationRule {

  /** Creates a JoinAssociateRule. */
  protected JoinAssociateRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public JoinAssociateRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    final Join topJoin = call.rel(0);
    final Join bottomJoin = call.rel(1);
    final RelNode relA = bottomJoin.getLeft();
    final RelNode relB = bottomJoin.getRight();
    final RelNode relC = call.rel(2);
    final RelOptCluster cluster = topJoin.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();

    if (relC.getConvention() != relA.getConvention()) {
      // relC could have any trait-set. But if we're matching say
      // EnumerableConvention, we're only interested in enumerable subsets.
      return;
    }

    //        topJoin
    //        /     \
    //   bottomJoin  C
    //    /    \
    //   A      B

    final int aCount = relA.getRowType().getFieldCount();
    final int bCount = relB.getRowType().getFieldCount();
    final int cCount = relC.getRowType().getFieldCount();
    final ImmutableBitSet aBitSet = ImmutableBitSet.range(0, aCount);
    @SuppressWarnings("unused")
    final ImmutableBitSet bBitSet =
        ImmutableBitSet.range(aCount, aCount + bCount);

    if (!topJoin.getSystemFieldList().isEmpty()) {
      // FIXME Enable this rule for joins with system fields
      return;
    }

    // If either join is not inner, we cannot proceed.
    // (Is this too strict?)
    if (topJoin.getJoinType() != JoinRelType.INNER
        || bottomJoin.getJoinType() != JoinRelType.INNER) {
      return;
    }

    // Goal is to transform to
    //
    //       newTopJoin
    //        /     \
    //       A   newBottomJoin
    //               /    \
    //              B      C

    // Split the condition of topJoin and bottomJoin into a conjunctions. A
    // condition can be pushed down if it does not use columns from A.
    final List<RexNode> top = new ArrayList<>();
    final List<RexNode> bottom = new ArrayList<>();
    JoinPushThroughJoinRule.split(topJoin.getCondition(), aBitSet, top, bottom);
    JoinPushThroughJoinRule.split(bottomJoin.getCondition(), aBitSet, top,
        bottom);

    final boolean allowAlwaysTrueCondition = config.isAllowAlwaysTrueCondition();
    if (!allowAlwaysTrueCondition && (top.isEmpty() || bottom.isEmpty())) {
      return;
    }

    // Mapping for moving conditions from topJoin or bottomJoin to
    // newBottomJoin.
    // target: | B | C      |
    // source: | A       | B | C      |
    final Mappings.TargetMapping bottomMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            0, aCount, bCount,
            bCount, aCount + bCount, cCount);
    final List<RexNode> newBottomList =
        new RexPermuteInputsShuttle(bottomMapping, relB, relC)
            .visitList(bottom);

    RexNode newBottomCondition = RexUtil.composeConjunction(rexBuilder, newBottomList);
    if (!allowAlwaysTrueCondition && newBottomCondition.isAlwaysTrue()) {
      return;
    }

    // Condition for newTopJoin consists of pieces from bottomJoin and topJoin.
    // Field ordinals do not need to be changed.
    RexNode newTopCondition = RexUtil.composeConjunction(rexBuilder, top);
    if (!allowAlwaysTrueCondition && newTopCondition.isAlwaysTrue()) {
      return;
    }

    final Join newBottomJoin =
        bottomJoin.copy(bottomJoin.getTraitSet(), newBottomCondition, relB,
            relC, JoinRelType.INNER, false);

    @SuppressWarnings("SuspiciousNameCombination")
    final Join newTopJoin =
        topJoin.copy(topJoin.getTraitSet(), newTopCondition, relA,
            newBottomJoin, JoinRelType.INNER, false);

    call.transformTo(newTopJoin);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableJoinAssociateRule.Config.of()
        .withOperandFor(Join.class);

    @Override default JoinAssociateRule toRule() {
      return new JoinAssociateRule(this);
    }

    /**
     * Whether to emit the new join tree if the new top or bottom join has a condition which
     * is always {@code TRUE}.
     */
    @Value.Default default boolean isAllowAlwaysTrueCondition() {
      return true;
    }

    /** Sets {@link #isAllowAlwaysTrueCondition()}. */
    Config withAllowAlwaysTrueCondition(boolean allowAlwaysTrueCondition);

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass) {
      return withOperandSupplier(b0 ->
          b0.operand(joinClass).inputs(
              b1 -> b1.operand(joinClass).anyInputs(),
              b2 -> b2.operand(RelNode.class).anyInputs()))
          .as(Config.class);
    }
  }
}
