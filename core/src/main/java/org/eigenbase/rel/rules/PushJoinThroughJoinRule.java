/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.JoinRelType;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexPermuteInputsShuttle;
import org.eigenbase.util.Util;
import org.eigenbase.util.mapping.MappingType;
import org.eigenbase.util.mapping.Mappings;

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
    public static final RelOptRule RIGHT =
        new PushJoinThroughJoinRule("PushJoinThroughJoinRule:right", true);
    public static final RelOptRule LEFT =
        new PushJoinThroughJoinRule("PushJoinThroughJoinRule:left", false);

    private final boolean right;

    private PushJoinThroughJoinRule(String description, boolean right) {
        super(
            some(
                JoinRel.class, any(JoinRel.class), any(RelNode.class)),
            description);
        this.right = right;
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
        final JoinRel topJoin = call.rel(0);
        final JoinRel bottomJoin = call.rel(1);
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
        final BitSet bBitSet = Util.bitSetBetween(aCount, aCount + bCount);

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
            || bottomJoin.getJoinType() != JoinRelType.INNER)
        {
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
            createShiftMapping(
                aCount + bCount + cCount,
                0, 0, aCount,
                aCount, aCount + bCount, cCount);
        List<RexNode> newBottomList = new ArrayList<RexNode>();
        new RexPermuteInputsShuttle(bottomMapping, relA, relC)
            .visitList(nonIntersecting, newBottomList);
        final Mappings.TargetMapping bottomBottomMapping =
            createShiftMapping(
                aCount + bCount,
                0, 0, aCount);
        new RexPermuteInputsShuttle(bottomBottomMapping, relA, relC)
            .visitList(bottomNonIntersecting, newBottomList);
        RexNode newBottomCondition =
            RelOptUtil.composeConjunction(
                cluster.getRexBuilder(), newBottomList);
        final JoinRel newBottomJoin =
            new JoinRel(
                cluster, relA, relC, newBottomCondition,
                JoinRelType.INNER, Collections.<String>emptySet());

        // target: | A       | C      | B |
        // source: | A       | B | C      |
        final Mappings.TargetMapping topMapping =
            createShiftMapping(
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
            RelOptUtil.composeConjunction(
                cluster.getRexBuilder(),
                newTopList);
        @SuppressWarnings("SuspiciousNameCombination")
        final JoinRel newTopJoin =
            new JoinRel(
                cluster, newBottomJoin, relB, newTopCondition,
                JoinRelType.INNER, Collections.<String>emptySet());

      assert !Mappings.isIdentity(topMapping);
        final RelNode newProject =
            CalcRel.createProject(newTopJoin, Mappings.asList(topMapping));

        call.transformTo(newProject);
    }

    /** Similar to {@link #onMatch}, but swaps the upper sibling with the left
     * of the two lower siblings, rather than the right. */
    private void onMatchLeft(RelOptRuleCall call) {
        final JoinRel topJoin = call.rel(0);
        final JoinRel bottomJoin = call.rel(1);
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
        final BitSet aBitSet = Util.bitSetBetween(0, aCount);

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
            || bottomJoin.getJoinType() != JoinRelType.INNER)
        {
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
            createShiftMapping(
                aCount + bCount + cCount,
                cCount, aCount, bCount,
                0, aCount + bCount, cCount);
        List<RexNode> newBottomList = new ArrayList<RexNode>();
        new RexPermuteInputsShuttle(bottomMapping, relC, relB)
            .visitList(nonIntersecting, newBottomList);
        final Mappings.TargetMapping bottomBottomMapping =
            createShiftMapping(
                aCount + bCount + cCount,
                0, aCount + bCount, cCount,
                cCount, aCount, bCount);
        new RexPermuteInputsShuttle(bottomBottomMapping, relC, relB)
            .visitList(bottomNonIntersecting, newBottomList);
        RexNode newBottomCondition =
            RelOptUtil.composeConjunction(
                cluster.getRexBuilder(), newBottomList);
        final JoinRel newBottomJoin =
            new JoinRel(
                cluster, relC, relB, newBottomCondition,
                JoinRelType.INNER, Collections.<String>emptySet());

        // target: | C      | B | A       |
        // source: | A       | B | C      |
        final Mappings.TargetMapping topMapping =
            createShiftMapping(
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
            RelOptUtil.composeConjunction(
                cluster.getRexBuilder(), newTopList);
        @SuppressWarnings("SuspiciousNameCombination")
        final JoinRel newTopJoin =
            new JoinRel(
                cluster, newBottomJoin, relA, newTopCondition,
                JoinRelType.INNER, Collections.<String>emptySet());

        final RelNode newProject =
            CalcRel.createProject(newTopJoin, Mappings.asList(topMapping));

        call.transformTo(newProject);
    }

    /** Splits a condition into conjunctions that do or do not intersect with
     * a given bit set. */
    static void split(
        RexNode condition,
        BitSet bitSet,
        List<RexNode> intersecting,
        List<RexNode> nonIntersecting)
    {
        for (RexNode node : RelOptUtil.conjunctions(condition)) {
            BitSet inputBitSet = RelOptUtil.InputFinder.bits(node);
            if (bitSet.intersects(inputBitSet)) {
                intersecting.add(node);
            } else {
                nonIntersecting.add(node);
            }
        }
    }

    static Mappings.TargetMapping createShiftMapping(
        int sourceCount, int... ints)
    {
        int targetCount = 0;
        assert ints.length % 3 == 0;
        for (int i = 0; i < ints.length; i += 3) {
            final int length = ints[i + 2];
            targetCount += length;
        }
        final Mappings.TargetMapping mapping =
            Mappings.create(
                MappingType.InverseSurjection,
                sourceCount, // aCount + bCount + cCount,
                targetCount); // cCount + bCount

        for (int i = 0; i < ints.length; i += 3) {
            final int target = ints[i];
            final int source = ints[i + 1];
            final int length = ints[i + 2];
            assert source + length <= sourceCount;
            for (int j = 0; j < length; j++) {
                assert mapping.getTargetOpt(source + j) == -1;
                mapping.set(source + j, target + j);
            }
        }
        return mapping;
    }
}

// End PushJoinThroughJoinRule.java
