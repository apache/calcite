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
 * Rule to add a semijoin into a joinrel. Transformation is as follows:
 *
 * <p>JoinRel(X, Y) -> JoinRel(SemiJoinRel(X, Y), Y)
 */
public class AddRedundantSemiJoinRule
    extends RelOptRule
{
    public static final AddRedundantSemiJoinRule instance =
        new AddRedundantSemiJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AddRedundantSemiJoinRule.
     */
    private AddRedundantSemiJoinRule()
    {
        super(
            any(JoinRel.class));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        JoinRel origJoinRel = call.rel(0);
        if (origJoinRel.isSemiJoinDone()) {
            return;
        }

        // can't process outer joins using semijoins
        if (origJoinRel.getJoinType() != JoinRelType.INNER) {
            return;
        }

        // determine if we have a valid join condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();
        RelOptUtil.splitJoinCondition(
            origJoinRel.getLeft(),
            origJoinRel.getRight(),
            origJoinRel.getCondition(),
            leftKeys,
            rightKeys);
        if (leftKeys.size() == 0) {
            return;
        }

        RelNode semiJoin =
            new SemiJoinRel(
                origJoinRel.getCluster(),
                origJoinRel.getLeft(),
                origJoinRel.getRight(),
                origJoinRel.getCondition(),
                leftKeys,
                rightKeys);

        RelNode newJoinRel =
            new JoinRel(
                origJoinRel.getCluster(),
                semiJoin,
                origJoinRel.getRight(),
                origJoinRel.getCondition(),
                JoinRelType.INNER,
                Collections.<String>emptySet(),
                true,
                origJoinRel.getSystemFieldList());

        call.transformTo(newJoinRel);
    }
}

// End AddRedundantSemiJoinRule.java
