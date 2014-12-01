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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.core.SemiJoin} down in a tree past
 * a {@link org.apache.calcite.rel.core.Project}.
 *
 * <p>The intention is to trigger other rules that will convert
 * {@code SemiJoin}s.
 *
 * <p>SemiJoin(LogicalProject(X), Y) &rarr; LogicalProject(SemiJoin(X, Y))
 *
 * @see org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule
 */
public class SemiJoinProjectTransposeRule extends RelOptRule {
  public static final SemiJoinProjectTransposeRule INSTANCE =
      new SemiJoinProjectTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SemiJoinProjectTransposeRule.
   */
  private SemiJoinProjectTransposeRule() {
    super(
        operand(SemiJoin.class,
            some(operand(LogicalProject.class, any()))));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    SemiJoin semiJoin = call.rel(0);
    LogicalProject project = call.rel(1);

    // convert the LHS semijoin keys to reference the child projection
    // expression; all projection expressions must be RexInputRefs,
    // otherwise, we wouldn't have created this semijoin
    List<Integer> newLeftKeys = new ArrayList<Integer>();
    List<Integer> leftKeys = semiJoin.getLeftKeys();
    List<RexNode> projExprs = project.getProjects();
    for (int leftKey : leftKeys) {
      RexInputRef inputRef = (RexInputRef) projExprs.get(leftKey);
      newLeftKeys.add(inputRef.getIndex());
    }

    // convert the semijoin condition to reflect the LHS with the project
    // pulled up
    RexNode newCondition = adjustCondition(project, semiJoin);

    SemiJoin newSemiJoin =
        new SemiJoin(
            semiJoin.getCluster(),
            semiJoin.getCluster().traitSetOf(Convention.NONE),
            project.getInput(),
            semiJoin.getRight(),
            newCondition,
            ImmutableIntList.copyOf(newLeftKeys),
            semiJoin.getRightKeys());

    // Create the new projection.  Note that the projection expressions
    // are the same as the original because they only reference the LHS
    // of the semijoin and the semijoin only projects out the LHS
    RelNode newProject =
        RelOptUtil.createProject(
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
   * @param project  LogicalProject on the LHS of the semijoin
   * @param semiJoin the semijoin
   * @return the modified semijoin condition
   */
  private RexNode adjustCondition(LogicalProject project, SemiJoin semiJoin) {
    // create two RexPrograms -- the bottom one representing a
    // concatenation of the project and the RHS of the semijoin and the
    // top one representing the semijoin condition

    RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    RelNode rightChild = semiJoin.getRight();

    // for the bottom RexProgram, the input is a concatenation of the
    // child of the project and the RHS of the semijoin
    RelDataType bottomInputRowType =
        Join.deriveJoinRowType(
            project.getInput().getRowType(),
            rightChild.getRowType(),
            JoinRelType.INNER,
            typeFactory,
            null,
            semiJoin.getSystemFieldList());
    RexProgramBuilder bottomProgramBuilder =
        new RexProgramBuilder(bottomInputRowType, rexBuilder);

    // add the project expressions, then add input references for the RHS
    // of the semijoin
    for (Pair<RexNode, String> pair : project.getNamedProjects()) {
      bottomProgramBuilder.addProject(pair.left, pair.right);
    }
    int nLeftFields = project.getInput().getRowType().getFieldCount();
    List<RelDataTypeField> rightFields =
        rightChild.getRowType().getFieldList();
    int nRightFields = rightFields.size();
    for (int i = 0; i < nRightFields; i++) {
      final RelDataTypeField field = rightFields.get(i);
      RexNode inputRef =
          rexBuilder.makeInputRef(
              field.getType(), i + nLeftFields);
      bottomProgramBuilder.addProject(inputRef, field.getName());
    }
    RexProgram bottomProgram = bottomProgramBuilder.getProgram();

    // input rowtype into the top program is the concatenation of the
    // project and the RHS of the semijoin
    RelDataType topInputRowType =
        Join.deriveJoinRowType(
            project.getRowType(),
            rightChild.getRowType(),
            JoinRelType.INNER,
            typeFactory,
            null,
            semiJoin.getSystemFieldList());
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

// End SemiJoinProjectTransposeRule.java
