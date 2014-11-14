/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.RelFactories.ProjectFactory;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.Pair;

/**
 * PullUpProjectsAboveJoinRule implements the rule for pulling {@link
 * ProjectRel}s beneath a {@link JoinRelBase} above the {@link JoinRelBase}. Projections
 * are pulled up if the {@link ProjectRel} doesn't originate from a null
 * generating input in an outer join.
 */
public class PullUpProjectsAboveJoinRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final PullUpProjectsAboveJoinRule BOTH_PROJECT =
      new PullUpProjectsAboveJoinRule(
          operand(
              JoinRel.class,
              operand(ProjectRel.class, any()),
              operand(ProjectRel.class, any())),
          "PullUpProjectsAboveJoinRule: with two ProjectRel children");

  public static final PullUpProjectsAboveJoinRule LEFT_PROJECT =
      new PullUpProjectsAboveJoinRule(
          operand(
              JoinRel.class,
              some(
                  operand(ProjectRel.class, any()))),
          "PullUpProjectsAboveJoinRule: with ProjectRel on left");

  public static final PullUpProjectsAboveJoinRule RIGHT_PROJECT =
      new PullUpProjectsAboveJoinRule(
          operand(
              JoinRel.class,
              operand(RelNode.class, any()),
              operand(ProjectRel.class, any())),
          "PullUpProjectsAboveJoinRule: with ProjectRel on right");

  private final ProjectFactory projectFactory;

  //~ Constructors -----------------------------------------------------------
  public PullUpProjectsAboveJoinRule(
      RelOptRuleOperand operand,
      String description) {
    this(operand, description, RelFactories.DEFAULT_PROJECT_FACTORY);
  }

  public PullUpProjectsAboveJoinRule(
      RelOptRuleOperand operand,
      String description, ProjectFactory pFactory) {
    super(operand, description);
    projectFactory = pFactory;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    JoinRelBase joinRel = call.rel(0);
    JoinRelType joinType = joinRel.getJoinType();

    ProjectRelBase leftProj;
    ProjectRelBase rightProj;
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

    // Create a row type representing a concatenation of the inputs
    // underneath the projects that feed into the join.  This is the input
    // into the bottom RexProgram.  Note that the join type is an inner
    // join because the inputs haven't actually been joined yet.
    RelDataType joinChildrenRowType =
        JoinRelBase.deriveJoinRowType(
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
    List<Pair<RexNode, String>> projects =
        new ArrayList<Pair<RexNode, String>>();
    RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();

    createProjectExprs(
        leftProj,
        leftJoinChild,
        0,
        rexBuilder,
        joinChildrenRowType.getFieldList(),
        projects);

    List<RelDataTypeField> leftFields =
        leftJoinChild.getRowType().getFieldList();
    int nFieldsLeft = leftFields.size();
    createProjectExprs(
        rightProj,
        rightJoinChild,
        nFieldsLeft,
        rexBuilder,
        joinChildrenRowType.getFieldList(),
        projects);

    List<RelDataType> projTypes = new ArrayList<RelDataType>();
    for (int i = 0; i < nProjExprs; i++) {
      projTypes.add(projects.get(i).left.getType());
    }
    RelDataType projRowType =
        rexBuilder.getTypeFactory().createStructType(
            projTypes,
            Pair.right(projects));

    // create the RexPrograms and merge them
    RexProgram bottomProgram =
        RexProgram.create(
            joinChildrenRowType,
            Pair.left(projects),
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
    JoinRelBase newJoinRel =
        joinRel.copy(joinRel.getTraitSet(), newCondition,
            leftJoinChild, rightJoinChild, joinRel.getJoinType(),
            joinRel.isSemiJoinDone());

    // expand out the new projection expressions; if the join is an
    // outer join, modify the expressions to reference the join output
    List<RexNode> newProjExprs = new ArrayList<RexNode>();
    List<RexLocalRef> projList = mergedProgram.getProjectList();
    List<RelDataTypeField> newJoinFields =
        newJoinRel.getRowType().getFieldList();
    int nJoinFields = newJoinFields.size();
    int[] adjustments = new int[nJoinFields];
    for (int i = 0; i < nProjExprs; i++) {
      RexNode newExpr = mergedProgram.expandLocalRef(projList.get(i));
      if (joinType != JoinRelType.INNER) {
        newExpr =
            newExpr.accept(
                new RelOptUtil.RexInputConverter(
                    rexBuilder,
                    joinChildrenRowType.getFieldList(),
                    newJoinFields,
                    adjustments));
      }
      newProjExprs.add(newExpr);
    }

    // finally, create the projection on top of the join
    RelNode newProjRel = projectFactory.createProject(newJoinRel, newProjExprs,
        joinRel.getRowType().getFieldNames());

    call.transformTo(newProjRel);
  }

  /**
   * @param call RelOptRuleCall
   * @return true if the rule was invoked with a left project child
   */
  protected boolean hasLeftChild(RelOptRuleCall call) {
    return call.rel(1) instanceof ProjectRelBase;
  }

  /**
   * @param call RelOptRuleCall
   * @return true if the rule was invoked with 2 children
   */
  protected boolean hasRightChild(RelOptRuleCall call) {
    return call.rels.length == 3;
  }

  /**
   * @param call RelOptRuleCall
   * @return ProjectRel corresponding to the right child
   */
  protected ProjectRelBase getRightChild(RelOptRuleCall call) {
    return call.rel(2);
  }

  /**
   * Returns the child of the project that will be used as input into the new
   * JoinRel once the projects are pulled above the JoinRel.
   *
   * @param call      RelOptRuleCall
   * @param project   project RelNode
   * @param leftChild true if the project corresponds to the left projection
   * @return child of the project that will be used as input into the new
   * JoinRel once the projects are pulled above the JoinRel
   */
  protected RelNode getProjectChild(
      RelOptRuleCall call,
      ProjectRelBase project,
      boolean leftChild) {
    return project.getChild();
  }

  /**
   * Creates projection expressions corresponding to one of the inputs into
   * the join
   *
   * @param projRel            the projection input into the join (if it exists)
   * @param joinChild          the child of the projection input (if there is a
   *                           projection); otherwise, this is the join input
   * @param adjustmentAmount   the amount the expressions need to be shifted by
   * @param rexBuilder         rex builder
   * @param joinChildrenFields concatenation of the fields from the left and
   *                           right join inputs (once the projections have been
   *                           removed)
   * @param projects           Projection expressions &amp; names to be created
   */
  private void createProjectExprs(
      ProjectRelBase projRel,
      RelNode joinChild,
      int adjustmentAmount,
      RexBuilder rexBuilder,
      List<RelDataTypeField> joinChildrenFields,
      List<Pair<RexNode, String>> projects) {
    List<RelDataTypeField> childFields =
        joinChild.getRowType().getFieldList();
    if (projRel != null) {
      List<Pair<RexNode, String>> namedProjects =
          projRel.getNamedProjects();
      int nChildFields = childFields.size();
      int[] adjustments = new int[nChildFields];
      for (int i = 0; i < nChildFields; i++) {
        adjustments[i] = adjustmentAmount;
      }
      for (Pair<RexNode, String> pair : namedProjects) {
        RexNode e = pair.left;
        if (adjustmentAmount != 0) {
          // shift the references by the adjustment amount
          e = e.accept(
              new RelOptUtil.RexInputConverter(
                  rexBuilder,
                  childFields,
                  joinChildrenFields,
                  adjustments));
        }
        projects.add(Pair.of(e, pair.right));
      }
    } else {
      // no projection; just create references to the inputs
      for (int i = 0; i < childFields.size(); i++) {
        final RelDataTypeField field = childFields.get(i);
        projects.add(
            Pair.of(
                (RexNode) rexBuilder.makeInputRef(
                    field.getType(),
                    i + adjustmentAmount),
                field.getName()));
      }
    }
  }
}

// End PullUpProjectsAboveJoinRule.java
