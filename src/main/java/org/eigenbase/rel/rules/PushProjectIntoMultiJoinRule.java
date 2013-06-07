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


/**
 * PushProjectIntoMultiJoinRule implements the rule for pushing projection
 * information from a {@link ProjectRel} into the {@link MultiJoinRel} that is
 * input into the {@link ProjectRel}.
 */
public class PushProjectIntoMultiJoinRule
    extends RelOptRule
{
    public static final PushProjectIntoMultiJoinRule instance =
        new PushProjectIntoMultiJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushProjectIntoMultiJoinRule.
     */
    private PushProjectIntoMultiJoinRule()
    {
        super(
            some(
                ProjectRel.class, any(MultiJoinRel.class)));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel project = call.rel(0);
        MultiJoinRel multiJoin = call.rel(1);

        // if all inputs have their projFields set, then projection information
        // has already been pushed into each input
        boolean allSet = true;
        for (int i = 0; i < multiJoin.getInputs().size(); i++) {
            if (multiJoin.getProjFields()[i] == null) {
                allSet = false;
                break;
            }
        }
        if (allSet) {
            return;
        }

        // create a new MultiJoinRel that reflects the columns in the projection
        // above the MultiJoinRel
        MultiJoinRel newMultiJoin =
            RelOptUtil.projectMultiJoin(multiJoin, project);
        ProjectRel newProject =
            (ProjectRel) CalcRel.createProject(
                newMultiJoin,
                project.getProjectExpList(),
                project.getRowType().getFieldNames());

        call.transformTo(newProject);
    }
}

// End PushProjectIntoMultiJoinRule.java
