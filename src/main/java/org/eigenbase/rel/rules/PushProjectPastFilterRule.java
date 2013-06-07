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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * PushProjectPastFilterRule implements the rule for pushing a projection past a
 * filter.
 */
public class PushProjectPastFilterRule
    extends RelOptRule
{
    public static final PushProjectPastFilterRule instance =
        new PushProjectPastFilterRule();

    //~ Instance fields --------------------------------------------------------

    /**
     * Expressions that should be preserved in the projection
     */
    private final PushProjector.ExprCondition preserveExprCondition;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushProjectPastFilterRule.
     */
    private PushProjectPastFilterRule()
    {
        super(
            some(
                ProjectRel.class, any(FilterRel.class)));
        this.preserveExprCondition = PushProjector.ExprCondition.FALSE;
    }

    /**
     * Creates a PushProjectPastFilterRule with an explicit root operand
     * and condition to preserve operands.
     *
     * @param operand root operand, must not be null
     *
     * @param id Part of description
     */
    public PushProjectPastFilterRule(
        RelOptRuleOperand operand,
        PushProjector.ExprCondition preserveExprCondition,
        String id)
    {
        super(operand, "PushProjectPastFilterRule: " + id);
        this.preserveExprCondition = preserveExprCondition;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel origProj;
        FilterRel filterRel;

        if (call.rels.length == 2) {
            origProj = call.rel(0);
            filterRel = call.rel(1);
        } else {
            origProj = null;
            filterRel = call.rel(0);
        }
        RelNode rel = filterRel.getChild();
        RexNode origFilter = filterRel.getCondition();

        if ((origProj != null)
            && RexOver.containsOver(
                origProj.getProjectExps(),
                null))
        {
            // Cannot push project through filter if project contains a windowed
            // aggregate -- it will affect row counts. Abort this rule
            // invocation; pushdown will be considered after the windowed
            // aggregate has been implemented. It's OK if the filter contains a
            // windowed aggregate.
            return;
        }

        PushProjector pushProjector =
            new PushProjector(
                origProj, origFilter, rel, preserveExprCondition);
        ProjectRel topProject = pushProjector.convertProject(null);

        if (topProject != null) {
            call.transformTo(topProject);
        }
    }
}

// End PushProjectPastFilterRule.java
