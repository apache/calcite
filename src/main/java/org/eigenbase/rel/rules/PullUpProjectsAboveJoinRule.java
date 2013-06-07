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
 * PullUpProjectsAboveJoinRule implements the rule for pulling {@link
 * ProjectRel}s beneath a {@link JoinRel} above the {@link JoinRel}. Projections
 * are pulled up if the {@link ProjectRel} doesn't originate from a null
 * generating input in an outer join.
 */
public class PullUpProjectsAboveJoinRule
    extends RelOptRule
{
    // ~ Static fields/initializers --------------------------------------------

    //~ Static fields/initializers ---------------------------------------------

    public static final PullUpProjectsAboveJoinRule instanceTwoProjectChildren =
        new PullUpProjectsAboveJoinRule(
            some(
                JoinRel.class, any(ProjectRel.class), any(ProjectRel.class)),
            "PullUpProjectsAboveJoinRule: with two ProjectRel children");

    public static final PullUpProjectsAboveJoinRule instanceLeftProjectChild =
        new PullUpProjectsAboveJoinRule(
            some(
                JoinRel.class, any(ProjectRel.class)),
            "PullUpProjectsAboveJoinRule: with ProjectRel on left");

    public static final PullUpProjectsAboveJoinRule instanceRightProjectChild =
        new PullUpProjectsAboveJoinRule(
            some(
                JoinRel.class, any(RelNode.class), any(ProjectRel.class)),
            "PullUpProjectsAboveJoinRule: with ProjectRel on right");

    //~ Constructors -----------------------------------------------------------

    public PullUpProjectsAboveJoinRule(
        RelOptRuleOperand operand,
        String description)
    {
        super(operand, description);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = call.rel(0);
        JoinRelType joinType = joinRel.getJoinType();

        ProjectRel leftProj;
        ProjectRel rightProj;
        RelNode leftJoinChild;
        RelNode rightJoinChild;

        // see if at least one input's projection doesn't generate nulls
        if (hasLeftChild(call) && !joinType.generatesNullsOnLeft()) {
            leftProj = call.rel(1);
            leftJoinChild = getProjectChild(call, leftProj, true);
        } else {
            leftProj = null;
            leftJoinChild = call.rel(1);
        }
        if (hasRightChild(call) && !joinType.generatesNullsOnRight()) {
            rightProj = getRightChild(call);
            rightJoinChild = getProjectChild(call, rightProj, false);
        } else {
            rightProj = null;
            rightJoinChild = joinRel.getRight();
        }
        if ((leftProj == null) && (rightProj == null)) {
            return;
        }

        // Construct two RexPrograms and combine them.  The bottom program
        // is a join of the projection expressions from the left and/or
        // right projects that feed into the join.  The top program contains
        // the join condition.

        // Create a row type representing a concatentation of the inputs
        // underneath the projects that feed into the join.  This is the input
        // into the bottom RexProgram.  Note that the join type is an inner
        // join because the inputs haven't actually been joined yet.
        RelDataType joinChildrenRowType =
            JoinRel.deriveJoinRowType(
                leftJoinChild.getRowType(),
                rightJoinChild.getRowType(),
                JoinRelType.INNER,
                joinRel.getCluster().getTypeFactory(),
                null,
                Collections.<RelDataTypeField>emptyList());

        // Create projection expressions, combining the projection expressions
        // from the projects that feed into the join.  For the RHS projection
        // expressions, shift them to the right by the number of fields on
        // the LHS.  If the join input was not a projection, simply create
        // references to the inputs.
        int nProjExprs = joinRel.getRowType().getFieldCount();
        RexNode [] projExprs = new RexNode[nProjExprs];
        String [] fieldNames = new String[nProjExprs];
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();

        createProjectExprs(
            leftProj,
            leftJoinChild,
            0,
            rexBuilder,
            joinChildrenRowType.getFields(),
            projExprs,
            fieldNames,
            0);

        RelDataTypeField [] leftFields = leftJoinChild.getRowType().getFields();
        int nFieldsLeft = leftFields.length;
        createProjectExprs(
            rightProj,
            rightJoinChild,
            nFieldsLeft,
            rexBuilder,
            joinChildrenRowType.getFields(),
            projExprs,
            fieldNames,
            ((leftProj == null) ? nFieldsLeft
                : leftProj.getProjectExps().length));

        RelDataType [] projTypes = new RelDataType[nProjExprs];
        for (int i = 0; i < nProjExprs; i++) {
            projTypes[i] = projExprs[i].getType();
        }
        RelDataType projRowType =
            rexBuilder.getTypeFactory().createStructType(
                projTypes,
                fieldNames);

        // create the RexPrograms and merge them
        RexProgram bottomProgram =
            RexProgram.create(
                joinChildrenRowType,
                projExprs,
                null,
                projRowType,
                rexBuilder);
        RexProgramBuilder topProgramBuilder =
            new RexProgramBuilder(
                projRowType,
                rexBuilder);
        topProgramBuilder.addIdentity();
        topProgramBuilder.addCondition(joinRel.getCondition());
        RexProgram topProgram = topProgramBuilder.getProgram();
        RexProgram mergedProgram =
            RexProgramBuilder.mergePrograms(
                topProgram,
                bottomProgram,
                rexBuilder);

        // expand out the join condition and construct a new JoinRel that
        // directly references the join children without the intervening
        // ProjectRels
        RexNode newCondition =
            mergedProgram.expandLocalRef(
                mergedProgram.getCondition());
        JoinRel newJoinRel =
            new JoinRel(
                joinRel.getCluster(),
                leftJoinChild,
                rightJoinChild,
                newCondition,
                joinRel.getJoinType(),
                joinRel.getVariablesStopped());

        // expand out the new projection expressions; if the join is an
        // outer join, modify the expressions to reference the join output
        RexNode [] newProjExprs = new RexNode[nProjExprs];
        List<RexLocalRef> projList = mergedProgram.getProjectList();
        RelDataTypeField [] newJoinFields = newJoinRel.getRowType().getFields();
        int nJoinFields = newJoinFields.length;
        int [] adjustments = new int[nJoinFields];
        for (int i = 0; i < nProjExprs; i++) {
            RexNode newExpr = mergedProgram.expandLocalRef(projList.get(i));
            if (joinType == JoinRelType.INNER) {
                newProjExprs[i] = newExpr;
            } else {
                newProjExprs[i] =
                    newExpr.accept(
                        new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            joinChildrenRowType.getFields(),
                            newJoinFields,
                            adjustments));
            }
        }

        // finally, create the projection on top of the join
        RelNode newProjRel =
            CalcRel.createProject(
                newJoinRel,
                newProjExprs,
                fieldNames);

        call.transformTo(newProjRel);
    }

    /**
     * @param call RelOptRuleCall
     *
     * @return true if the rule was invoked with a left project child
     */
    protected boolean hasLeftChild(RelOptRuleCall call)
    {
        return (call.rel(1) instanceof ProjectRel);
    }

    /**
     * @param call RelOptRuleCall
     *
     * @return true if the rule was invoked with 2 children
     */
    protected boolean hasRightChild(RelOptRuleCall call)
    {
        return call.rels.length == 3;
    }

    /**
     * @param call RelOptRuleCall
     *
     * @return ProjectRel corresponding to the right child
     */
    protected ProjectRel getRightChild(RelOptRuleCall call)
    {
        return call.rel(2);
    }

    /**
     * Returns the child of the project that will be used as input into the new
     * JoinRel once the projects are pulled above the JoinRel.
     *
     * @param call RelOptRuleCall
     * @param project project RelNode
     * @param leftChild true if the project corresponds to the left projection
     * @return child of the project that will be used as input into the new
     * JoinRel once the projects are pulled above the JoinRel
     */
    protected RelNode getProjectChild(
        RelOptRuleCall call,
        ProjectRel project,
        boolean leftChild)
    {
        return project.getChild();
    }

    /**
     * Creates projection expressions corresponding to one of the inputs into
     * the join
     *
     * @param projRel the projection input into the join (if it exists)
     * @param joinChild the child of the projection input (if there is a
     * projection); otherwise, this is the join input
     * @param adjustmentAmount the amount the expressions need to be shifted by
     * @param rexBuilder rex builder
     * @param joinChildrenFields concatentation of the fields from the left and
     * right join inputs (once the projections have been removed)
     * @param projExprs array of projection expressions to be created
     * @param fieldNames array of the names of the projection fields
     * @param offset starting index in the arrays to be filled in
     */
    private void createProjectExprs(
        ProjectRel projRel,
        RelNode joinChild,
        int adjustmentAmount,
        RexBuilder rexBuilder,
        RelDataTypeField [] joinChildrenFields,
        RexNode [] projExprs,
        String [] fieldNames,
        int offset)
    {
        RelDataTypeField [] childFields = joinChild.getRowType().getFields();
        if (projRel != null) {
            RexNode [] origProjExprs = projRel.getProjectExps();
            RelDataTypeField [] projFields = projRel.getRowType().getFields();
            int nChildFields = childFields.length;
            int [] adjustments = new int[nChildFields];
            for (int i = 0; i < nChildFields; i++) {
                adjustments[i] = adjustmentAmount;
            }
            for (int i = 0; i < origProjExprs.length; i++) {
                if (adjustmentAmount == 0) {
                    projExprs[i + offset] = origProjExprs[i];
                } else {
                    // shift the references by the adjustment amount
                    RexNode newProjExpr =
                        origProjExprs[i].accept(
                            new RelOptUtil.RexInputConverter(
                                rexBuilder,
                                childFields,
                                joinChildrenFields,
                                adjustments));
                    projExprs[i + offset] = newProjExpr;
                }
                fieldNames[i + offset] = projFields[i].getName();
            }
        } else {
            // no projection; just create references to the inputs
            for (int i = 0; i < childFields.length; i++) {
                projExprs[i + offset] =
                    rexBuilder.makeInputRef(
                        childFields[i].getType(),
                        i + adjustmentAmount);
                fieldNames[i + offset] = childFields[i].getName();
            }
        }
    }
}

// End PullUpProjectsAboveJoinRule.java
