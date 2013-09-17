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
    public static final RelOptRule INSTANCE = new PushJoinThroughJoinRule();

    private PushJoinThroughJoinRule() {
        super(
            some(
                JoinRel.class, any(JoinRel.class), any(RelNode.class)),
            "RotateJoinRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final JoinRel topJoin = call.rel(0);
        final JoinRel bottomJoin = call.rel(1);
        final RelNode relC = call.rel(2);
        final RelNode relA = bottomJoin.getLeft();
        final RelNode relB = bottomJoin.getRight();

        //        topJoin
        //        /     \
        //   bottomJoin  C
        //    /    \
        //   A      B

        final int aCount = relA.getRowType().getFieldCount();
        final int bCount = relB.getRowType().getFieldCount();
        final int cCount = relC.getRowType().getFieldCount();
        final int aTop = aCount;
        final int bTop = aTop + bCount;

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
        List<RexNode> intersecting = new ArrayList<RexNode>();
        List<RexNode> nonIntersecting = new ArrayList<RexNode>();
        for (RexNode node : RelOptUtil.conjunctions(topJoin.getCondition())) {
            BitSet inputBitSet = RelOptUtil.InputFinder.bits(node);
            if (Util.bitSetBetween(aTop, bTop).intersects(inputBitSet)) {
                intersecting.add(node);
            } else {
                nonIntersecting.add(node);
            }
        }

        // If there's nothing to push down, it's not worth proceeding.
        if (nonIntersecting.isEmpty()) {
            return;
        }

        final RelOptCluster cluster = topJoin.getCluster();
        final Mappings.TargetMapping bottomMapping =
            Mappings.create(
                MappingType.InverseSurjection,
                aCount + bCount + cCount,
                aCount + cCount);
        for (int t = 0; t < bottomMapping.getTargetCount(); t++) {
            int s = t;
            if (t >= aCount) {
                s += bCount;
            }
            bottomMapping.set(s, t);
        }
        RexNode newBottomCondition =
            RelOptUtil.composeConjunction(
                cluster.getRexBuilder(),
                nonIntersecting)
            .accept(new RexPermuteInputsShuttle(bottomMapping, relA, relC));
        final JoinRel newBottomJoin =
            new JoinRel(
                cluster, relA, relC, newBottomCondition,
                JoinRelType.INNER, Collections.<String>emptySet());

        // target: | A       | C      | B |
        // source: | A       | B | C      |
        final Mappings.TargetMapping topMapping =
            Mappings.create(
                MappingType.InverseSurjection,
                aCount + bCount + cCount,
                aCount + bCount + cCount);
        for (int s = 0; s < topMapping.getTargetCount(); s++) {
            int t;
            if (s >= aCount + bCount) {
                t = s - bCount;
            } else if (s >= aCount) {
                t = s + cCount;
            } else {
                t = s;
            }
            topMapping.set(s, t);
        }
        RexNode newTopCondition =
            RelOptUtil.composeConjunction(
                cluster.getRexBuilder(),
                intersecting)
            .accept(
                new RexPermuteInputsShuttle(topMapping, newBottomJoin, relB));
        @SuppressWarnings("SuspiciousNameCombination")
        final JoinRel newTopJoin =
            new JoinRel(
                cluster, newBottomJoin, relB, newTopCondition,
                JoinRelType.INNER,  Collections.<String>emptySet());

        final RelNode newProject =
            CalcRel.createProject(newTopJoin, Mappings.asList(topMapping));

        call.transformTo(newProject);
    }
}

// End PushJoinThroughJoinRule.java
