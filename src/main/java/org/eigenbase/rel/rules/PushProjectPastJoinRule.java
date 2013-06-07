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
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * PushProjectPastJoinRule implements the rule for pushing a projection past a
 * join by splitting the projection into a projection on top of each child of
 * the join.
 */
public class PushProjectPastJoinRule
    extends RelOptRule
{
    public static final PushProjectPastJoinRule instance =
        new PushProjectPastJoinRule();

    //~ Instance fields --------------------------------------------------------

    /**
     * Condition for expressions that should be preserved in the projection.
     */
    private final PushProjector.ExprCondition preserveExprCondition;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushProjectPastJoinRule.
     */
    private PushProjectPastJoinRule()
    {
        this(PushProjector.ExprCondition.FALSE);
    }

    /**
     * Creates a PushProjectPastJoinRule with an explicit condition.
     *
     * @param preserveExprCondition Condition for expressions that should be
     * preserved in the projection
     */
    public PushProjectPastJoinRule(
        PushProjector.ExprCondition preserveExprCondition)
    {
        super(
            some(
                ProjectRel.class, any(JoinRel.class)));
        this.preserveExprCondition = preserveExprCondition;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel origProj = call.rel(0);
        JoinRel joinRel = call.rel(1);

        // locate all fields referenced in the projection and join condition;
        // determine which inputs are referenced in the projection and
        // join condition; if all fields are being referenced and there are no
        // special expressions, no point in proceeding any further
        PushProjector pushProject =
            new PushProjector(
                origProj,
                joinRel.getCondition(),
                joinRel,
                preserveExprCondition);
        if (pushProject.locateAllRefs()) {
            return;
        }

        // create left and right projections, projecting only those
        // fields referenced on each side
        RelNode leftProjRel =
            pushProject.createProjectRefsAndExprs(
                joinRel.getLeft(),
                true,
                false);
        RelNode rightProjRel =
            pushProject.createProjectRefsAndExprs(
                joinRel.getRight(),
                true,
                true);

        // convert the join condition to reference the projected columns
        RexNode newJoinFilter = null;
        int [] adjustments = pushProject.getAdjustments();
        if (joinRel.getCondition() != null) {
            List<RelDataTypeField> projJoinFieldList =
                new ArrayList<RelDataTypeField>();
            projJoinFieldList.addAll(
                joinRel.getSystemFieldList());
            projJoinFieldList.addAll(
                leftProjRel.getRowType().getFieldList());
            projJoinFieldList.addAll(
                rightProjRel.getRowType().getFieldList());
            RelDataTypeField [] projJoinFields =
                projJoinFieldList.toArray(
                    new RelDataTypeField[projJoinFieldList.size()]);
            newJoinFilter =
                pushProject.convertRefsAndExprs(
                    joinRel.getCondition(),
                    projJoinFields,
                    adjustments);
        }

        // create a new joinrel with the projected children
        JoinRel newJoinRel =
            new JoinRel(
                joinRel.getCluster(),
                leftProjRel,
                rightProjRel,
                newJoinFilter,
                joinRel.getJoinType(),
                Collections.<String>emptySet(),
                joinRel.isSemiJoinDone(),
                joinRel.getSystemFieldList());

        // put the original project on top of the join, converting it to
        // reference the modified projection list
        ProjectRel topProject =
            pushProject.createNewProject(newJoinRel, adjustments);

        call.transformTo(topProject);
    }
}

// End PushProjectPastJoinRule.java
