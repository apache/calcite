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
package org.eigenbase.rel.rules;

import java.util.BitSet;
import java.util.List;

import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.volcano.RelSubset;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexPermuteInputsShuttle;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.util.mapping.Mappings;

import net.hydromatic.optiq.util.BitSets;

import com.google.common.collect.Lists;

/**
 * Planner rule that changes a join based on the commutativity rule.
 *
 * <p>((a JOIN b) JOIN c) &rarr; (a JOIN (b JOIN c))</p>
 *
 * <p>We do not need a rule to convert (a JOIN (b JOIN c)) &rarr;
 * ((a JOIN b) JOIN c) because we have
 * {@link org.eigenbase.rel.rules.SwapJoinRule}.
 */
public class CommutativeJoinRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final CommutativeJoinRule INSTANCE = new CommutativeJoinRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an CommutativeJoinRule.
   */
  private CommutativeJoinRule() {
    super(
        operand(JoinRelBase.class,
            operand(JoinRelBase.class, any()),
            operand(RelSubset.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(final RelOptRuleCall call) {
    final JoinRelBase topJoin = call.rel(0);
    final JoinRelBase bottomJoin = call.rel(1);
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
    final BitSet aBitSet = BitSets.range(0, aCount);
    final BitSet bBitSet = BitSets.range(aCount, aCount + bCount);

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
    PushJoinThroughJoinRule.split(topJoin.getCondition(), aBitSet, top, bottom);
    PushJoinThroughJoinRule.split(bottomJoin.getCondition(), aBitSet, top,
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

    final JoinRelBase newBottomJoin =
        bottomJoin.copy(bottomJoin.getTraitSet(), newBottomCondition, relB,
            relC, JoinRelType.INNER, false);

    // Condition for newTopJoin consists of pieces from bottomJoin and topJoin.
    // Field ordinals do not need to be changed.
    RexNode newTopCondition =
        RexUtil.composeConjunction(rexBuilder, top, false);
    final JoinRelBase newTopJoin =
        topJoin.copy(topJoin.getTraitSet(), newTopCondition, relA,
            newBottomJoin, JoinRelType.INNER, false);

    call.transformTo(newTopJoin);
  }
}

// End CommutativeJoinRule.java
