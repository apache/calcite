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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBeans;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.logical.LogicalProject}
 * past a {@link org.apache.calcite.rel.core.SetOp}.
 *
 * <p>The children of the {@code SetOp} will project
 * only the {@link RexInputRef}s referenced in the original
 * {@code LogicalProject}.
 *
 * @see CoreRules#PROJECT_SET_OP_TRANSPOSE
 */
public class ProjectSetOpTransposeRule
    extends RelRule<ProjectSetOpTransposeRule.Config>
    implements TransformationRule {

  /** Creates a ProjectSetOpTransposeRule. */
  protected ProjectSetOpTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public ProjectSetOpTransposeRule(
      PushProjector.ExprCondition preserveExprCondition,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withPreserveExprCondition(preserveExprCondition));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final LogicalProject origProject = call.rel(0);
    final SetOp setOp = call.rel(1);

    // cannot push project past a distinct
    if (!setOp.all) {
      return;
    }

    // locate all fields referenced in the projection
    final PushProjector pushProjector =
        new PushProjector(origProject, null, setOp,
            config.preserveExprCondition(), call.builder());
    pushProjector.locateAllRefs();

    final List<RelNode> newSetOpInputs = new ArrayList<>();
    final int[] adjustments = pushProjector.getAdjustments();

    final RelNode node;
    if (origProject.containsOver()) {
      // should not push over past set-op but can push its operand down.
      for (RelNode input : setOp.getInputs()) {
        Project p = pushProjector.createProjectRefsAndExprs(input, true, false);
        // make sure that it is not a trivial project to avoid infinite loop.
        if (p.getRowType().equals(input.getRowType())) {
          return;
        }
        newSetOpInputs.add(p);
      }
      final SetOp newSetOp =
          setOp.copy(setOp.getTraitSet(), newSetOpInputs);
      node = pushProjector.createNewProject(newSetOp, adjustments);
    } else {
      // push some expressions below the set-op; this
      // is different from pushing below a join, where we decompose
      // to try to keep expensive expressions above the join,
      // because UNION ALL does not have any filtering effect,
      // and it is the only operator this rule currently acts on
      setOp.getInputs().forEach(input ->
          newSetOpInputs.add(
              pushProjector.createNewProject(
                  pushProjector.createProjectRefsAndExprs(
                      input, true, false), adjustments)));
      node = setOp.copy(setOp.getTraitSet(), newSetOpInputs);
    }

    call.transformTo(node);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .withOperandSupplier(b0 ->
            b0.operand(LogicalProject.class).oneInput(b1 ->
                b1.operand(SetOp.class).anyInputs()))
        .as(Config.class)
        .withPreserveExprCondition(expr -> !(expr instanceof RexOver));

    @Override default ProjectSetOpTransposeRule toRule() {
      return new ProjectSetOpTransposeRule(this);
    }

    /** Defines when an expression should not be pushed. */
    @ImmutableBeans.Property
    PushProjector.ExprCondition preserveExprCondition();

    /** Sets {@link #preserveExprCondition()}. */
    Config withPreserveExprCondition(PushProjector.ExprCondition condition);
  }
}
