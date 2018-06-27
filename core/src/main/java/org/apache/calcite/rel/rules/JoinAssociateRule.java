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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Planner rule that changes a join based on the associativity rule.
 *
 * <p>((a JOIN b) JOIN c) &rarr; (a JOIN (b JOIN c))</p>
 *
 * <p>We do not need a rule to convert (a JOIN (b JOIN c)) &rarr;
 * ((a JOIN b) JOIN c) because we have
 * {@link JoinCommuteRule}.
 *
 * @see JoinCommuteRule
 */
public class JoinAssociateRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final JoinAssociateRule INSTANCE =
      new JoinAssociateRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a JoinAssociateRule.
   */
  public JoinAssociateRule(RelBuilderFactory relBuilderFactory) {
    super(
        operand(Join.class,
            operand(Join.class, any()),
            operand(RelSubset.class, any())),
        relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(final RelOptRuleCall call) {
    final Join topJoin = call.rel(0);
    final Join bottomJoin = call.rel(1);
    final RelNode relA = bottomJoin.getLeft();
    final RelNode relB = bottomJoin.getRight();
    final RelSubset relC = call.rel(2);
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
    final List<RexNode> top = Lists.newArrayList();
    final List<RexNode> bottom = Lists.newArrayList();
    JoinPushThroughJoinRule.split(topJoin.getCondition(), aBitSet, top, bottom);
    JoinPushThroughJoinRule.split(bottomJoin.getCondition(), aBitSet, top,
        bottom);

    // Mapping for moving conditions from topJoin or bottomJoin to
    // newBottomJoin.
    // target: | B | C      |
    // source: | A       | B | C      |
    final Mappings.TargetMapping bottomMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            0, aCount, bCount,
            bCount, aCount + bCount, cCount);
    final List<RexNode> newBottomList = Lists.newArrayList();
    new RexPermuteInputsShuttle(bottomMapping, relB, relC)
        .visitList(bottom, newBottomList);
    RexNode newBottomCondition =
        RexUtil.composeConjunction(rexBuilder, newBottomList, false);

    final Join newBottomJoin =
        bottomJoin.copy(bottomJoin.getTraitSet(), newBottomCondition, relB,
            relC, JoinRelType.INNER, false);

    // Condition for newTopJoin consists of pieces from bottomJoin and topJoin.
    // Field ordinals do not need to be changed.
    RexNode newTopCondition =
        RexUtil.composeConjunction(rexBuilder, top, false);
    final Join newTopJoin =
        topJoin.copy(topJoin.getTraitSet(), newTopCondition, relA,
            newBottomJoin, JoinRelType.INNER, false);

    call.transformTo(newTopJoin);
  }
}

// End JoinAssociateRule.java
