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

import java.util.Arrays;
import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * PullUpAggregateAboveUnionRule implements the rule for pulling {@link
 * AggregateRel}s beneath a {@link UnionRel} so two {@link AggregateRel}s that
 * are used to remove duplicates can be combined into a single {@link
 * AggregateRel}.
 *
 * <p>This rule only handles cases where the {@link UnionRel}s still have only
 * two inputs.
 */
public class PullUpAggregateAboveUnionRule
    extends RelOptRule
{
    public static final PullUpAggregateAboveUnionRule instance =
        new PullUpAggregateAboveUnionRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PullUpAggregateAboveUnionRule.
     */
    private PullUpAggregateAboveUnionRule()
    {
        super(
            some(
                AggregateRel.class,
                some(
                    UnionRel.class,
                    any(RelNode.class),
                    any(RelNode.class))));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        UnionRel unionRel = call.rel(1);

        // If distincts haven't been removed yet, defer invoking this rule
        if (!unionRel.all) {
            return;
        }

        AggregateRel topAggRel = call.rel(0);
        AggregateRel bottomAggRel;

        // We want to apply this rule on the pattern where the AggregateRel
        // is the second input into the UnionRel first.  Hence, that's why the
        // rule pattern matches on generic RelNodes rather than explicit
        // UnionRels.  By doing so, and firing this rule in a bottom-up order,
        // it allows us to only specify a single pattern for this rule.
        List<RelNode> unionInputs;
        if (call.rel(3) instanceof AggregateRel) {
            bottomAggRel = call.rel(3);
            unionInputs = Arrays.asList(
                call.rel(2),
                call.rel(3).getInput(0));
        } else if (call.rel(2) instanceof AggregateRel) {
            bottomAggRel = call.rel(2);
            unionInputs = Arrays.asList(
                call.rel(2).getInput(0),
                call.rel(3));
        } else {
            return;
        }

        // Only pull up aggregates if they are there just to remove distincts
        if (!topAggRel.getAggCallList().isEmpty()
            || !bottomAggRel.getAggCallList().isEmpty())
        {
            return;
        }

        UnionRel newUnionRel =
            new UnionRel(
                unionRel.getCluster(),
                unionInputs,
                true);

        AggregateRel newAggRel =
            new AggregateRel(
                topAggRel.getCluster(),
                newUnionRel,
                topAggRel.getGroupSet(),
                topAggRel.getAggCallList());

        call.transformTo(newAggRel);
    }
}

// End PullUpAggregateAboveUnionRule.java
