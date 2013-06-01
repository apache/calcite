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
 * PushSemiJoinPastProjectRule implements the rule for pushing semijoins down in
 * a tree past a project in order to trigger other rules that will convert
 * semijoins.
 *
 * <p>SemiJoinRel(ProjectRel(X), Y) --> ProjectRel(SemiJoinRel(X, Y))
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushSemiJoinPastProjectRule
    extends RelOptRule
{
    public static final PushSemiJoinPastProjectRule instance =
        new PushSemiJoinPastProjectRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushSemiJoinPastProjectRule.
     */
    private PushSemiJoinPastProjectRule()
    {
        super(
            new RelOptRuleOperand(
                SemiJoinRel.class,
                new RelOptRuleOperand(ProjectRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        SemiJoinRel semiJoin = (SemiJoinRel) call.rels[0];
        ProjectRel project = (ProjectRel) call.rels[1];

        // convert the LHS semijoin keys to reference the child projection
        // expression; all projection expressions must be RexInputRefs,
        // otherwise, we wouldn't have created this semijoin
        List<Integer> newLeftKeys = new ArrayList<Integer>();
        List<Integer> leftKeys = semiJoin.getLeftKeys();
        List<RexNode> projExprs = project.getProjectExpList();
        for (int i = 0; i < leftKeys.size(); i++) {
            RexInputRef inputRef = (RexInputRef) projExprs.get(leftKeys.get(i));
            newLeftKeys.add(inputRef.getIndex());
        }

        // convert the semijoin condition to reflect the LHS with the project
        // pulled up
        RexNode newCondition = adjustCondition(project, semiJoin);

        SemiJoinRel newSemiJoin =
            new SemiJoinRel(
                semiJoin.getCluster(),
                project.getChild(),
                semiJoin.getRight(),
                newCondition,
                newLeftKeys,
                semiJoin.getRightKeys());

        // Create the new projection.  Note that the projection expressions
        // are the same as the original because they only reference the LHS
        // of the semijoin and the semijoin only projects out the LHS
        RelNode newProject =
            CalcRel.createProject(
                newSemiJoin,
                projExprs,
                project.getRowType().getFieldNames());

        call.transformTo(newProject);
    }

    /**
     * Pulls the project above the semijoin and returns the resulting semijoin
     * condition. As a result, the semijoin condition should be modified such
     * that references to the LHS of a semijoin should now reference the
     * children of the project that's on the LHS.
     *
     * @param project ProjectRel on the LHS of the semijoin
     * @param semiJoin the semijoin
     *
     * @return the modified semijoin condition
     */
    private RexNode adjustCondition(ProjectRel project, SemiJoinRel semiJoin)
    {
        // create two RexPrograms -- the bottom one representing a
        // concatenation of the project and the RHS of the semijoin and the
        // top one representing the semijoin condition

        RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        RelNode rightChild = semiJoin.getRight();

        // for the bottom RexProgram, the input is a concatenation of the
        // child of the project and the RHS of the semijoin
        RelDataType bottomInputRowType =
            typeFactory.createJoinType(
                new RelDataType[] {
                    project.getChild().getRowType(),
                    rightChild.getRowType()
                });
        RexProgramBuilder bottomProgramBuilder =
            new RexProgramBuilder(bottomInputRowType, rexBuilder);

        // add the project expressions, then add input references for the RHS
        // of the semijoin
        RexNode [] projExprs = project.getProjectExps();
        RelDataTypeField [] projFields = project.getRowType().getFields();
        for (int i = 0; i < projExprs.length; i++) {
            bottomProgramBuilder.addProject(
                projExprs[i],
                projFields[i].getName());
        }
        int nLeftFields = project.getChild().getRowType().getFieldCount();
        RelDataTypeField [] rightFields = rightChild.getRowType().getFields();
        int nRightFields = rightFields.length;
        for (int i = 0; i < nRightFields; i++) {
            RexNode inputRef =
                rexBuilder.makeInputRef(
                    rightFields[i].getType(),
                    i + nLeftFields);
            bottomProgramBuilder.addProject(inputRef, rightFields[i].getName());
        }
        RexProgram bottomProgram = bottomProgramBuilder.getProgram();

        // input rowtype into the top program is the concatenation of the
        // project and the RHS of the semijoin
        RelDataType topInputRowType =
            typeFactory.createJoinType(
                new RelDataType[] {
                    project.getRowType(),
                    rightChild.getRowType()
                });
        RexProgramBuilder topProgramBuilder =
            new RexProgramBuilder(
                topInputRowType,
                rexBuilder);
        topProgramBuilder.addIdentity();
        topProgramBuilder.addCondition(semiJoin.getCondition());
        RexProgram topProgram = topProgramBuilder.getProgram();

        // merge the programs and expand out the local references to form
        // the new semijoin condition; it now references a concatenation of
        // the project's child and the RHS of the semijoin
        RexProgram mergedProgram =
            RexProgramBuilder.mergePrograms(
                topProgram,
                bottomProgram,
                rexBuilder);

        return mergedProgram.expandLocalRef(
            mergedProgram.getCondition());
    }
}

// End PushSemiJoinPastProjectRule.java
