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
import org.eigenbase.rex.*;


/**
 * PushFilterPastJoinRule implements the rule for pushing filters above and
 * within a join node into the join node and/or its children nodes.
 */
public class PushFilterPastJoinRule
    extends RelOptRule
{
    public static final PushFilterPastJoinRule instance =
        new PushFilterPastJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushFilterPastJoinRule.
     */
    private PushFilterPastJoinRule()
    {
        super(
            some(
                FilterRel.class, any(JoinRel.class)));
    }

    /**
     * Creates a PushFilterPastJoinRule with an explicit root operand.
     */
    public PushFilterPastJoinRule(
        RelOptRuleOperand operand,
        String id)
    {
        // This rule is fired for either of the following two patterns:
        //
        // RelOptRuleOperand(
        //     FilterRel.class,
        //     new RelOptRuleOperand(JoinRel.class, ANY))
        //
        // RelOptRuleOperand(JoinRel.class, null)
        //
        super(operand, "PushFilterRule: " + id);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FilterRel filterRel;
        JoinRel joinRel;

        if (call.rels.length == 1) {
            filterRel = null;
            joinRel = call.rel(0);
        } else {
            filterRel = call.rel(0);
            joinRel = call.rel(1);
        }

        final List<RexNode> joinFilters =
            RelOptUtil.conjunctions(joinRel.getCondition());

        if (filterRel == null) {
            // There is only the joinRel
            // make sure it does not match a cartesian product joinRel
            // (with "true" condition) otherwise this rule will be applied
            // again on the new cartesian product joinRel.
            boolean onlyTrueFilter = true;
            for (RexNode joinFilter : joinFilters) {
                if (!joinFilter.isAlwaysTrue()) {
                    onlyTrueFilter = false;
                    break;
                }
            }

            if (onlyTrueFilter) {
                return;
            }
        }

        final List<RexNode> aboveFilters =
            filterRel != null
                ? RelOptUtil.conjunctions(filterRel.getCondition())
                : Collections.<RexNode>emptyList();

        List<RexNode> leftFilters = new ArrayList<RexNode>();
        List<RexNode> rightFilters = new ArrayList<RexNode>();

        // TODO - add logic to derive additional filters.  E.g., from
        // (t1.a = 1 AND t2.a = 2) OR (t1.b = 3 AND t2.b = 4), you can
        // derive table filters:
        // (t1.a = 1 OR t1.b = 3)
        // (t2.a = 2 OR t2.b = 4)

        // Try to push down above filters. These are typically where clause
        // filters. They can be pushed down if they are not on the NULL
        // generating side.
        boolean filterPushed = false;
        if (RelOptUtil.classifyFilters(
                joinRel,
                aboveFilters,
                (joinRel.getJoinType() == JoinRelType.INNER),
                !joinRel.getJoinType().generatesNullsOnLeft(),
                !joinRel.getJoinType().generatesNullsOnRight(),
                joinFilters,
                leftFilters,
                rightFilters))
        {
            filterPushed = true;
        }

        // Try to push down filters in ON clause. A ON clause filter can only be
        // pushed down if it does not affect the non-matching set, i.e. it is
        // not on the side which is preserved.
        if (RelOptUtil.classifyFilters(
                joinRel,
                joinFilters,
                false,
                !joinRel.getJoinType().generatesNullsOnRight(),
                !joinRel.getJoinType().generatesNullsOnLeft(),
                joinFilters,
                leftFilters,
                rightFilters))
        {
            filterPushed = true;
        }

        if (!filterPushed) {
            return;
        }

        // create FilterRels on top of the children if any filters were
        // pushed to them
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        RelNode leftRel =
            createFilterOnRel(
                rexBuilder,
                joinRel.getLeft(),
                leftFilters);
        RelNode rightRel =
            createFilterOnRel(
                rexBuilder,
                joinRel.getRight(),
                rightFilters);

        // create the new join node referencing the new children and
        // containing its new join filters (if there are any)
        RexNode joinFilter;

        if (joinFilters.size() == 0) {
            // if nothing actually got pushed and there is nothing leftover,
            // then this rule is a no-op
            if ((leftFilters.size() == 0) && (rightFilters.size() == 0)) {
                return;
            }
            joinFilter = rexBuilder.makeLiteral(true);
        } else {
            joinFilter = RexUtil.andRexNodeList(rexBuilder, joinFilters);
        }
        RelNode newJoinRel =
            new JoinRel(
                joinRel.getCluster(),
                leftRel,
                rightRel,
                joinFilter,
                joinRel.getJoinType(),
                Collections.<String>emptySet(),
                joinRel.isSemiJoinDone(),
                joinRel.getSystemFieldList());

        // create a FilterRel on top of the join if needed
        RelNode newRel =
            createFilterOnRel(rexBuilder, newJoinRel, aboveFilters);

        call.transformTo(newRel);
    }

    /**
     * If the filter list passed in is non-empty, creates a FilterRel on top of
     * the existing RelNode; otherwise, just returns the RelNode
     *
     * @param rexBuilder rex builder
     * @param rel the RelNode that the filter will be put on top of
     * @param filters list of filters
     *
     * @return new RelNode or existing one if no filters
     */
    private RelNode createFilterOnRel(
        RexBuilder rexBuilder,
        RelNode rel,
        List<RexNode> filters)
    {
        RelNode newRel;

        if (filters.size() == 0) {
            newRel = rel;
        } else {
            RexNode andFilters = RexUtil.andRexNodeList(rexBuilder, filters);
            newRel = CalcRel.createFilter(rel, andFilters);
        }
        return newRel;
    }
}

// End PushFilterPastJoinRule.java
