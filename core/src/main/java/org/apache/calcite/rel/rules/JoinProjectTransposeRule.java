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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Planner rule that matches a
 * {@link org.apache.calcite.rel.core.Join} one of whose inputs is a
 * {@link org.apache.calcite.rel.logical.LogicalProject}, and
 * pulls the project up.
 *
 * <p>Projections are pulled up if the
 * {@link org.apache.calcite.rel.logical.LogicalProject} doesn't originate from
 * a null generating input in an outer join.
 */
public class JoinProjectTransposeRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final JoinProjectTransposeRule BOTH_PROJECT =
      new JoinProjectTransposeRule(
          operand(LogicalJoin.class,
              operand(LogicalProject.class, any()),
              operand(LogicalProject.class, any())),
          "JoinProjectTransposeRule(Project-Project)");

  public static final JoinProjectTransposeRule LEFT_PROJECT =
      new JoinProjectTransposeRule(
          operand(LogicalJoin.class,
              some(operand(LogicalProject.class, any()))),
          "JoinProjectTransposeRule(Project-Other)");

  public static final JoinProjectTransposeRule RIGHT_PROJECT =
      new JoinProjectTransposeRule(
          operand(
              LogicalJoin.class,
              operand(RelNode.class, any()),
              operand(LogicalProject.class, any())),
          "JoinProjectTransposeRule(Other-Project)");

  public static final JoinProjectTransposeRule BOTH_PROJECT_INCLUDE_OUTER =
      new JoinProjectTransposeRule(
          operand(LogicalJoin.class,
              operand(LogicalProject.class, any()),
              operand(LogicalProject.class, any())),
          "Join(IncludingOuter)ProjectTransposeRule(Project-Project)",
          true, RelFactories.LOGICAL_BUILDER);

  public static final JoinProjectTransposeRule LEFT_PROJECT_INCLUDE_OUTER =
      new JoinProjectTransposeRule(
          operand(LogicalJoin.class,
              some(operand(LogicalProject.class, any()))),
          "Join(IncludingOuter)ProjectTransposeRule(Project-Other)",
          true, RelFactories.LOGICAL_BUILDER);

  public static final JoinProjectTransposeRule RIGHT_PROJECT_INCLUDE_OUTER =
      new JoinProjectTransposeRule(
          operand(
              LogicalJoin.class,
              operand(RelNode.class, any()),
              operand(LogicalProject.class, any())),
          "Join(IncludingOuter)ProjectTransposeRule(Other-Project)",
          true, RelFactories.LOGICAL_BUILDER);

  private final boolean includeOuter;

  //~ Constructors -----------------------------------------------------------

  /** Creates a JoinProjectTransposeRule. */
  public JoinProjectTransposeRule(RelOptRuleOperand operand,
      String description, boolean includeOuter,
      RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
    this.includeOuter = includeOuter;
  }

  /** Creates a JoinProjectTransposeRule with default factory. */
  public JoinProjectTransposeRule(
      RelOptRuleOperand operand,
      String description) {
    this(operand, description, false, RelFactories.LOGICAL_BUILDER);
  }

  @Deprecated // to be removed before 2.0
  public JoinProjectTransposeRule(RelOptRuleOperand operand,
      String description, ProjectFactory projectFactory) {
    this(operand, description, false,
        RelBuilder.proto(Contexts.of(projectFactory)));
  }

  @Deprecated // to be removed before 2.0
  public JoinProjectTransposeRule(RelOptRuleOperand operand,
      String description, boolean includeOuter,
      ProjectFactory projectFactory) {
    this(operand, description, includeOuter,
        RelBuilder.proto(Contexts.of(projectFactory)));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    Join joinRel = call.rel(0);
    JoinRelType joinType = joinRel.getJoinType();

    Project leftProj;
    Project rightProj;
    RelNode leftJoinChild;
    RelNode rightJoinChild;

    // If 1) the rule works on outer joins, or
    //    2) input's projection doesn't generate nulls
    if (hasLeftChild(call)
            && (includeOuter || !joinType.generatesNullsOnLeft())) {
      leftProj = call.rel(1);
      leftJoinChild = getProjectChild(call, leftProj, true);
    } else {
      leftProj = null;
      leftJoinChild = call.rel(1);
    }
    if (hasRightChild(call)
            && (includeOuter || !joinType.generatesNullsOnRight())) {
      rightProj = getRightChild(call);
      rightJoinChild = getProjectChild(call, rightProj, false);
    } else {
      rightProj = null;
      rightJoinChild = joinRel.getRight();
    }
    if ((leftProj == null) && (rightProj == null)) {
      return;
    }

    if (includeOuter) {
      if (leftProj != null && joinType.generatesNullsOnLeft()
          && !Strong.allStrong(leftProj.getProjects())) {
        return;
      }

      if (rightProj != null && joinType.generatesNullsOnRight()
          && !Strong.allStrong(rightProj.getProjects())) {
        return;
      }
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
        SqlValidatorUtil.deriveJoinRowType(
            leftJoinChild.getRowType(),
            rightJoinChild.getRowType(),
            JoinRelType.INNER,
            joinRel.getCluster().getTypeFactory(),
            null,
            Collections.emptyList());

    // Create projection expressions, combining the projection expressions
    // from the projects that feed into the join.  For the RHS projection
    // expressions, shift them to the right by the number of fields on
    // the LHS.  If the join input was not a projection, simply create
    // references to the inputs.
    int nProjExprs = joinRel.getRowType().getFieldCount();
    final List<Pair<RexNode, String>> projects = new ArrayList<>();
    final RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();

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

    final List<RelDataType> projTypes = new ArrayList<>();
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

    // expand out the join condition and construct a new LogicalJoin that
    // directly references the join children without the intervening
    // ProjectRels
    RexNode newCondition =
        mergedProgram.expandLocalRef(
            mergedProgram.getCondition());
    Join newJoinRel =
        joinRel.copy(joinRel.getTraitSet(), newCondition,
            leftJoinChild, rightJoinChild, joinRel.getJoinType(),
            joinRel.isSemiJoinDone());

    // expand out the new projection expressions; if the join is an
    // outer join, modify the expressions to reference the join output
    final List<RexNode> newProjExprs = new ArrayList<>();
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
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(newJoinRel);
    relBuilder.project(newProjExprs, joinRel.getRowType().getFieldNames());
    // if the join was outer, we might need a cast after the
    // projection to fix differences wrt nullability of fields
    if (joinType != JoinRelType.INNER) {
      relBuilder.convert(joinRel.getRowType(), false);
    }

    call.transformTo(relBuilder.build());
  }

  /**
   * @param call RelOptRuleCall
   * @return true if the rule was invoked with a left project child
   */
  protected boolean hasLeftChild(RelOptRuleCall call) {
    return call.rel(1) instanceof Project;
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
   * @return LogicalProject corresponding to the right child
   */
  protected Project getRightChild(RelOptRuleCall call) {
    return call.rel(2);
  }

  /**
   * Returns the child of the project that will be used as input into the new
   * LogicalJoin once the projects are pulled above the LogicalJoin.
   *
   * @param call      RelOptRuleCall
   * @param project   project RelNode
   * @param leftChild true if the project corresponds to the left projection
   * @return child of the project that will be used as input into the new
   * LogicalJoin once the projects are pulled above the LogicalJoin
   */
  protected RelNode getProjectChild(
      RelOptRuleCall call,
      Project project,
      boolean leftChild) {
    return project.getInput();
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
  protected void createProjectExprs(
      Project projRel,
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

// End JoinProjectTransposeRule.java
