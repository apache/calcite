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

import com.google.common.collect.ImmutableList;


/**
 * PushProjectPastSortRule implements the rule for pushing a {@link ProjectRel}
 * past a {@link SortRel}. The children of the {@link SortRel} will project
 * only the {@link RexInputRef}s referenced in the original {@link ProjectRel}.
 */
public class PushProjectPastSortRule
    extends RelOptRule
{
    public static final PushProjectPastSortRule INSTANCE =
        new PushProjectPastSortRule();

    //~ Instance fields --------------------------------------------------------

    /**
     * Expressions that should be preserved in the projection
     */
    private PushProjector.ExprCondition preserveExprCondition;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushProjectPastSortRule.
     */
    private PushProjectPastSortRule()
    {
        this(PushProjector.ExprCondition.FALSE);
    }

    /**
     * Creates a PushProjectPastSortRule with an explicit condition whether
     * to preserve expressions.
     *
     * @param preserveExprCondition Condition whether to preserve expressions
     */
    public PushProjectPastSortRule(
        PushProjector.ExprCondition preserveExprCondition)
    {
        super(
            some(
                ProjectRel.class, any(SortRel.class)));
        this.preserveExprCondition = preserveExprCondition;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel origProj = call.rel(0);
        SortRel sort = call.rel(1);

        if (sort.getClass() != SortRel.class) return;

        if (false) { // TODO: rule can't fire if sort is on a non-trivial
            // projected expression
            return;
        }

        // locate all fields referenced in the projection
        PushProjector pushProject =
            new PushProjector(origProj, null, sort, preserveExprCondition);
        pushProject.locateAllRefs();

        List<RelNode> newSortInputs = new ArrayList<RelNode>();
        int [] adjustments = pushProject.getAdjustments();

        // push the projects completely below the Sort; this
        // is different from pushing below a join, where we decompose
        // to try to keep expensive expressions above the join,
        // because UNION ALL does not have any filtering effect,
        // and it is the only operator this rule currently acts on
        // be lazy:  produce two ProjectRels, and let another rule
        // merge them (could probably just clone origProj instead?)
        ProjectRel p =
            pushProject.createProjectRefsAndExprs(
                sort.getChild(), true, false);

        // create a new Sort whose children are the ProjectRels created above
        SortRel newSortRel =
            sort.copy(sort.getTraitSet(), ImmutableList.<RelNode>of(p));

        call.transformTo(newSortRel);
    }
}

// End PushProjectPastSortRule.java
