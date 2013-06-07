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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * PushJoinThroughUnionRule implements the rule for pushing a
 * {@link JoinRel} past a non-distinct {@link UnionRel}.
 */
public class PushJoinThroughUnionRule extends RelOptRule
{
    public static final PushJoinThroughUnionRule instanceUnionOnLeft =
        new PushJoinThroughUnionRule(
            some(
                JoinRel.class, any(UnionRel.class), any(RelNode.class)),
            "union on left");

    public static final PushJoinThroughUnionRule instanceUnionOnRight =
        new PushJoinThroughUnionRule(
            some(
                JoinRel.class, any(RelNode.class), any(UnionRel.class)),
            "union on right");

    public PushJoinThroughUnionRule(RelOptRuleOperand operand, String id)
    {
        super(
            operand,
            "PushJoinThroughUnionRule: " + id);
    }

    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = call.rel(0);
        UnionRel unionRel;
        RelNode otherInput;
        boolean unionOnLeft;
        if (call.rel(1) instanceof UnionRel) {
            unionRel = call.rel(1);
            otherInput = call.rel(2);
            unionOnLeft = true;
        } else {
            otherInput = call.rel(1);
            unionRel = call.rel(2);
            unionOnLeft = false;
        }
        if (!unionRel.all) {
            return;
        }
        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }
        // The UNION ALL cannot be on the null generating side
        // of an outer join (otherwise we might generate incorrect
        // rows for the other side for join keys which lack a match
        // in one or both branches of the union)
        if (unionOnLeft) {
            if (joinRel.getJoinType().generatesNullsOnLeft()) {
                return;
            }
        } else {
            if (joinRel.getJoinType().generatesNullsOnRight()) {
                return;
            }
        }
        List<RelNode> newUnionInputs = new ArrayList<RelNode>();
        RelOptCluster cluster = unionRel.getCluster();
        for (RelNode input : unionRel.getInputs()) {
            RelNode joinLeft, joinRight;
            if (unionOnLeft) {
                joinLeft = input;
                joinRight = otherInput;
            } else {
                joinLeft = otherInput;
                joinRight = input;
            }
            newUnionInputs.add(
                new JoinRel(
                    cluster,
                    joinLeft,
                    joinRight,
                    joinRel.getCondition(),
                    joinRel.getJoinType(),
                    Collections.<String>emptySet()));
        }
        UnionRel newUnionRel = new UnionRel(cluster, newUnionInputs, true);
        call.transformTo(newUnionRel);
    }
}

// End PushJoinThroughUnionRule.java
