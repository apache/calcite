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
 * PushProjectPastSetOpRule implements the rule for pushing a {@link ProjectRel}
 * past a {@link SetOpRel}. The children of the {@link SetOpRel} will project
 * only the {@link RexInputRef}s referenced in the original {@link ProjectRel}.
 */
public class PushProjectPastSetOpRule
    extends RelOptRule
{
    public static final PushProjectPastSetOpRule instance =
        new PushProjectPastSetOpRule();

    //~ Instance fields --------------------------------------------------------

    /**
     * Expressions that should be preserved in the projection
     */
    private PushProjector.ExprCondition preserveExprCondition;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushProjectPastSetOpRule.
     */
    private PushProjectPastSetOpRule()
    {
        this(PushProjector.ExprCondition.FALSE);
    }

    /**
     * Creates a PushProjectPastSetOpRule with an explicit condition whether
     * to preserve expressions.
     *
     * @param preserveExprCondition Condition whether to preserve expressions
     */
    public PushProjectPastSetOpRule(
        PushProjector.ExprCondition preserveExprCondition)
    {
        super(
            some(
                ProjectRel.class, any(SetOpRel.class)));
        this.preserveExprCondition = preserveExprCondition;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel origProj = call.rel(0);
        SetOpRel setOpRel = call.rel(1);

        // cannot push project past a distinct
        if (!setOpRel.all) {
            return;
        }

        // locate all fields referenced in the projection
        PushProjector pushProject =
            new PushProjector(origProj, null, setOpRel, preserveExprCondition);
        pushProject.locateAllRefs();

        List<RelNode> newSetOpInputs = new ArrayList<RelNode>();
        int [] adjustments = pushProject.getAdjustments();

        // push the projects completely below the setop; this
        // is different from pushing below a join, where we decompose
        // to try to keep expensive expressions above the join,
        // because UNION ALL does not have any filtering effect,
        // and it is the only operator this rule currently acts on
        for (RelNode input : setOpRel.getInputs()) {
            // be lazy:  produce two ProjectRels, and let another rule
            // merge them (could probably just clone origProj instead?)
            ProjectRel p =
                pushProject.createProjectRefsAndExprs(
                    input, true, false);
            newSetOpInputs.add(
                pushProject.createNewProject(p, adjustments));
        }

        // create a new setop whose children are the ProjectRels created above
        SetOpRel newSetOpRel =
            setOpRel.copy(setOpRel.getTraitSet(), newSetOpInputs);

        call.transformTo(newSetOpRel);
    }
}

// End PushProjectPastSetOpRule.java
