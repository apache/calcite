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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Planner rule that pushes
 * a semi-join or anti-join down in a tree past
 * a {@link org.apache.calcite.rel.core.Project}.
 *
 * <p>The intention is to trigger other rules that will convert
 * {@code SemiJoin}s or {@code AntiJoin}s.
 *
 * <p>SemiJoin(LogicalProject(X), Y) &rarr; LogicalProject(SemiJoin(X, Y))
 *
 * <p>AntiJoin(LogicalProject(X), Y) &rarr; LogicalProject(AntiJoin(X, Y))
 *
 * <p>This rule only transposes a project on the left input. Semi-joins and
 * anti-joins only project fields from their left input.
 *
 * @see org.apache.calcite.rel.rules.SemiJoinFilterTransposeRule
 */
@Value.Enclosing
public class SemiJoinProjectTransposeRule
    extends RelRule<SemiJoinProjectTransposeRule.Config>
    implements TransformationRule {

  /** Creates a SemiJoinProjectTransposeRule. */
  protected SemiJoinProjectTransposeRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final Project project = call.rel(1);

    // Skip when the semi-join condition references one of the project's
    // non-deterministic expressions (e.g. RAND()). Pulling such a project
    // above the semi-join inlines that expression into the join condition
    // via expandLocalRef while the project still re-emits it above,
    // splitting one evaluation into two. Non-deterministic columns that the
    // condition does not reference are safe to pull up. See [CALCITE-7551].
    final ImmutableBitSet conditionRefs =
        RelOptUtil.InputFinder.bits(join.getCondition());
    final List<RexNode> projects = project.getProjects();
    for (int i = 0; i < projects.size(); i++) {
      if (conditionRefs.get(i) && !RexUtil.isDeterministic(projects.get(i))) {
        return;
      }
    }

    // Convert the LHS semi-join keys to reference the child projection
    // expression; all projection expressions must be RexInputRefs,
    // otherwise, we wouldn't have created this semi-join.

    // convert the join condition to reflect the LHS with the project
    // pulled up
    RexNode newCondition = adjustCondition(project, join);

    LogicalJoin newJoin =
        LogicalJoin.create(project.getInput(),
            join.getRight(),
            // No need to copy the hints, the framework would try to do that.
            ImmutableList.of(),
            newCondition,
            ImmutableSet.of(), join.getJoinType());

    // Create the new projection.  Note that the projection expressions
    // are the same as the original because they only reference the LHS
    // of the semi-join or anti-join, which only projects out the LHS.
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(newJoin);
    relBuilder.project(project.getProjects(), project.getRowType().getFieldNames());

    call.transformTo(relBuilder.build());
  }

  /**
   * Pulls the project above the semi-join or anti-join and returns the resulting
   * join condition. As a result, the join condition should be modified such
   * that references to the LHS of the join should now reference the
   * children of the project that's on the LHS.
   *
   * @param project  LogicalProject on the LHS of the join
   * @param join the semi-join or anti-join
   * @return the modified join condition
   */
  private static RexNode adjustCondition(Project project, Join join) {
    // create two RexPrograms -- the bottom one representing a
    // concatenation of the project and the RHS of the join and the
    // top one representing the join condition

    final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
    final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    final RelNode rightChild = join.getRight();

    // for the bottom RexProgram, the input is a concatenation of the
    // child of the project and the RHS of the join
    RelDataType bottomInputRowType =
        SqlValidatorUtil.deriveJoinRowType(
            project.getInput().getRowType(),
            rightChild.getRowType(),
            JoinRelType.INNER,
            typeFactory,
            null,
            join.getSystemFieldList());
    RexProgramBuilder bottomProgramBuilder =
        new RexProgramBuilder(bottomInputRowType, rexBuilder);

    // add the project expressions, then add input references for the RHS
    // of the join
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

    // input row type into the top program is the concatenation of the
    // project and the RHS of the join
    RelDataType topInputRowType =
        SqlValidatorUtil.deriveJoinRowType(
            project.getRowType(),
            rightChild.getRowType(),
            JoinRelType.INNER,
            typeFactory,
            null,
            join.getSystemFieldList());
    RexProgramBuilder topProgramBuilder =
        new RexProgramBuilder(
            topInputRowType,
            rexBuilder);
    topProgramBuilder.addIdentity();
    topProgramBuilder.addCondition(join.getCondition());
    RexProgram topProgram = topProgramBuilder.getProgram();

    // merge the programs and expand out the local references to form
    // the new join condition; it now references a concatenation of
    // the project's child and the RHS of the join
    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder);

    return mergedProgram.expandLocalRef(
        requireNonNull(mergedProgram.getCondition(),
            () -> "mergedProgram.getCondition() for " + mergedProgram));
  }

  private static boolean isSemiOrAntiJoin(Join join) {
    return join.getJoinType() == JoinRelType.SEMI
        || join.getJoinType() == JoinRelType.ANTI;
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableSemiJoinProjectTransposeRule.Config.of()
        .withOperandFor(LogicalJoin.class, LogicalProject.class);

    @Override default SemiJoinProjectTransposeRule toRule() {
      return new SemiJoinProjectTransposeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass,
        Class<? extends Project> projectClass) {
      return withOperandSupplier(b ->
          b.operand(joinClass)
              .predicate(SemiJoinProjectTransposeRule::isSemiOrAntiJoin)
              .inputs(b2 ->
                  b2.operand(projectClass).anyInputs()))
          .as(Config.class);
    }
  }
}
