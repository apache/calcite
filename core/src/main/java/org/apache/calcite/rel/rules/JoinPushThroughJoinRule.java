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

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.RelFactories.ProjectFactory;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.mapping.Mappings;

import net.hydromatic.optiq.util.BitSets;

/**
 * Rule that pushes the right input of a join into through the left input of
 * the join, provided that the left input is also a join.
 *
 * <p>Thus, {@code (A join B) join C} becomes {@code (A join C) join B}. The
 * advantage of applying this rule is that it may be possible to apply
 * conditions earlier. For instance,</p>
 *
 * <pre>{@code
 * (sales as s join product_class as pc on true)
 * join product as p
 * on s.product_id = p.product_id
 * and p.product_class_id = pc.product_class_id}</pre>
 *
 * becomes
 *
 * <pre>{@code (sales as s join product as p on s.product_id = p.product_id)
 * join product_class as pc
 * on p.product_class_id = pc.product_class_id}</pre>
 *
 * <p>Before the rule, one join has two conditions and the other has none
 * ({@code ON TRUE}). After the rule, each join has one condition.</p>
 */
public class PushJoinThroughJoinRule extends RelOptRule {
  /** Instance of the rule that works on logical joins only, and pushes to the
   * right. */
  public static final RelOptRule RIGHT =
      new PushJoinThroughJoinRule(
          "PushJoinThroughJoinRule:right", true, JoinRel.class,
          RelFactories.DEFAULT_PROJECT_FACTORY);

  /** Instance of the rule that works on logical joins only, and pushes to the
   * left. */
  public static final RelOptRule LEFT =
      new PushJoinThroughJoinRule(
          "PushJoinThroughJoinRule:left", false, JoinRel.class,
          RelFactories.DEFAULT_PROJECT_FACTORY);

  private final boolean right;

  private final ProjectFactory projectFactory;

  /**
   * Creates a PushJoinThroughJoinRule.
   */
  public PushJoinThroughJoinRule(String description, boolean right,
      Class<? extends JoinRelBase> clazz, ProjectFactory projectFactory) {
    super(
        operand(
            clazz,
            operand(clazz, any()),
            operand(RelNode.class, any())),
        description);
    this.right = right;
    this.projectFactory = projectFactory;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (right) {
      onMatchRight(call);
    } else {
      onMatchLeft(call);
    }
  }

  private void onMatchRight(RelOptRuleCall call) {
    final JoinRelBase topJoin = call.rel(0);
    final JoinRelBase bottomJoin = call.rel(1);
    final RelNode relC = call.rel(2);
    final RelNode relA = bottomJoin.getLeft();
    final RelNode relB = bottomJoin.getRight();
    final RelOptCluster cluster = topJoin.getCluster();

    //        topJoin
    //        /     \
    //   bottomJoin  C
    //    /    \
    //   A      B

    final int aCount = relA.getRowType().getFieldCount();
    final int bCount = relB.getRowType().getFieldCount();
    final int cCount = relC.getRowType().getFieldCount();
    final BitSet bBitSet = BitSets.range(aCount, aCount + bCount);

    // becomes
    //
    //        newTopJoin
    //        /        \
    //   newBottomJoin  B
    //    /    \
    //   A      C

    // If either join is not inner, we cannot proceed.
    // (Is this too strict?)
    if (topJoin.getJoinType() != JoinRelType.INNER
        || bottomJoin.getJoinType() != JoinRelType.INNER) {
      return;
    }

    // Split the condition of topJoin into a conjunction. Each of the
    // parts that does not use columns from B can be pushed down.
    final List<RexNode> intersecting = new ArrayList<RexNode>();
    final List<RexNode> nonIntersecting = new ArrayList<RexNode>();
    split(topJoin.getCondition(), bBitSet, intersecting, nonIntersecting);

    // If there's nothing to push down, it's not worth proceeding.
    if (nonIntersecting.isEmpty()) {
      return;
    }

    // Split the condition of bottomJoin into a conjunction. Each of the
    // parts that use columns from B will need to be pulled up.
    final List<RexNode> bottomIntersecting = new ArrayList<RexNode>();
    final List<RexNode> bottomNonIntersecting = new ArrayList<RexNode>();
    split(
        bottomJoin.getCondition(), bBitSet, bottomIntersecting,
        bottomNonIntersecting);

    // target: | A       | C      |
    // source: | A       | B | C      |
    final Mappings.TargetMapping bottomMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            0, 0, aCount,
            aCount, aCount + bCount, cCount);
    List<RexNode> newBottomList = new ArrayList<RexNode>();
    new RexPermuteInputsShuttle(bottomMapping, relA, relC)
        .visitList(nonIntersecting, newBottomList);
    final Mappings.TargetMapping bottomBottomMapping =
        Mappings.createShiftMapping(
            aCount + bCount,
            0, 0, aCount);
    new RexPermuteInputsShuttle(bottomBottomMapping, relA, relC)
        .visitList(bottomNonIntersecting, newBottomList);
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode newBottomCondition =
        RexUtil.composeConjunction(rexBuilder, newBottomList, false);
    final JoinRelBase newBottomJoin =
        bottomJoin.copy(bottomJoin.getTraitSet(), newBottomCondition, relA,
            relC, bottomJoin.getJoinType(), bottomJoin.isSemiJoinDone());

    // target: | A       | C      | B |
    // source: | A       | B | C      |
    final Mappings.TargetMapping topMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            0, 0, aCount,
            aCount + cCount, aCount, bCount,
            aCount, aCount + bCount, cCount);
    List<RexNode> newTopList = new ArrayList<RexNode>();
    new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB)
        .visitList(intersecting, newTopList);
    new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB)
        .visitList(bottomIntersecting, newTopList);
    RexNode newTopCondition =
        RexUtil.composeConjunction(rexBuilder, newTopList, false);
    @SuppressWarnings("SuspiciousNameCombination")
    final JoinRelBase newTopJoin =
        topJoin.copy(topJoin.getTraitSet(), newTopCondition, newBottomJoin,
            relB, topJoin.getJoinType(), topJoin.isSemiJoinDone());

    assert !Mappings.isIdentity(topMapping);
    final RelNode newProject = RelOptUtil.createProject(projectFactory,
        newTopJoin, Mappings.asList(topMapping));

    call.transformTo(newProject);
  }

  /**
   * Similar to {@link #onMatch}, but swaps the upper sibling with the left
   * of the two lower siblings, rather than the right.
   */
  private void onMatchLeft(RelOptRuleCall call) {
    final JoinRelBase topJoin = call.rel(0);
    final JoinRelBase bottomJoin = call.rel(1);
    final RelNode relC = call.rel(2);
    final RelNode relA = bottomJoin.getLeft();
    final RelNode relB = bottomJoin.getRight();
    final RelOptCluster cluster = topJoin.getCluster();

    //        topJoin
    //        /     \
    //   bottomJoin  C
    //    /    \
    //   A      B

    final int aCount = relA.getRowType().getFieldCount();
    final int bCount = relB.getRowType().getFieldCount();
    final int cCount = relC.getRowType().getFieldCount();
    final BitSet aBitSet = BitSets.range(aCount);

    // becomes
    //
    //        newTopJoin
    //        /        \
    //   newBottomJoin  A
    //    /    \
    //   C      B

    // If either join is not inner, we cannot proceed.
    // (Is this too strict?)
    if (topJoin.getJoinType() != JoinRelType.INNER
        || bottomJoin.getJoinType() != JoinRelType.INNER) {
      return;
    }

    // Split the condition of topJoin into a conjunction. Each of the
    // parts that does not use columns from A can be pushed down.
    final List<RexNode> intersecting = new ArrayList<RexNode>();
    final List<RexNode> nonIntersecting = new ArrayList<RexNode>();
    split(topJoin.getCondition(), aBitSet, intersecting, nonIntersecting);

    // If there's nothing to push down, it's not worth proceeding.
    if (nonIntersecting.isEmpty()) {
      return;
    }

    // Split the condition of bottomJoin into a conjunction. Each of the
    // parts that use columns from B will need to be pulled up.
    final List<RexNode> bottomIntersecting = new ArrayList<RexNode>();
    final List<RexNode> bottomNonIntersecting = new ArrayList<RexNode>();
    split(
        bottomJoin.getCondition(), aBitSet, bottomIntersecting,
        bottomNonIntersecting);

    // target: | C      | B |
    // source: | A       | B | C      |
    final Mappings.TargetMapping bottomMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            cCount, aCount, bCount,
            0, aCount + bCount, cCount);
    List<RexNode> newBottomList = new ArrayList<RexNode>();
    new RexPermuteInputsShuttle(bottomMapping, relC, relB)
        .visitList(nonIntersecting, newBottomList);
    final Mappings.TargetMapping bottomBottomMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            0, aCount + bCount, cCount,
            cCount, aCount, bCount);
    new RexPermuteInputsShuttle(bottomBottomMapping, relC, relB)
        .visitList(bottomNonIntersecting, newBottomList);
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    RexNode newBottomCondition =
        RexUtil.composeConjunction(rexBuilder, newBottomList, false);
    final JoinRelBase newBottomJoin =
        bottomJoin.copy(bottomJoin.getTraitSet(), newBottomCondition, relC,
            relB, bottomJoin.getJoinType(), bottomJoin.isSemiJoinDone());

    // target: | C      | B | A       |
    // source: | A       | B | C      |
    final Mappings.TargetMapping topMapping =
        Mappings.createShiftMapping(
            aCount + bCount + cCount,
            cCount + bCount, 0, aCount,
            cCount, aCount, bCount,
            0, aCount + bCount, cCount);
    List<RexNode> newTopList = new ArrayList<RexNode>();
    new RexPermuteInputsShuttle(topMapping, newBottomJoin, relA)
        .visitList(intersecting, newTopList);
    new RexPermuteInputsShuttle(topMapping, newBottomJoin, relA)
        .visitList(bottomIntersecting, newTopList);
    RexNode newTopCondition =
        RexUtil.composeConjunction(rexBuilder, newTopList, false);
    @SuppressWarnings("SuspiciousNameCombination")
    final JoinRelBase newTopJoin =
        topJoin.copy(topJoin.getTraitSet(), newTopCondition, newBottomJoin,
            relA, topJoin.getJoinType(), topJoin.isSemiJoinDone());

    final RelNode newProject = RelOptUtil.createProject(projectFactory,
        newTopJoin, Mappings.asList(topMapping));

    call.transformTo(newProject);
  }

  /**
   * Splits a condition into conjunctions that do or do not intersect with
   * a given bit set.
   */
  static void split(
      RexNode condition,
      BitSet bitSet,
      List<RexNode> intersecting,
      List<RexNode> nonIntersecting) {
    for (RexNode node : RelOptUtil.conjunctions(condition)) {
      BitSet inputBitSet = RelOptUtil.InputFinder.bits(node);
      if (bitSet.intersects(inputBitSet)) {
        intersecting.add(node);
      } else {
        nonIntersecting.add(node);
      }
    }
  }
}

// End PushJoinThroughJoinRule.java
