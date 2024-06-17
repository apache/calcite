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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

import org.immutables.value.Value;

/**
 * Planner rule that merges a
 * {@link org.apache.calcite.rel.core.Project} and a
 * {@link org.apache.calcite.rel.core.Calc}.
 *
 * <p>The resulting {@link org.apache.calcite.rel.core.Calc} has the
 * same project list as the original
 * {@link org.apache.calcite.rel.core.Project}, but expressed in terms
 * of the original {@link org.apache.calcite.rel.core.Calc}'s inputs.
 *
 * @see FilterCalcMergeRule
 * @see CoreRules#PROJECT_CALC_MERGE
 */
@Value.Enclosing
public class ProjectCalcMergeRule
    extends RelRule<ProjectCalcMergeRule.Config>
    implements TransformationRule {

  /** Creates a ProjectCalcMergeRule. */
  protected ProjectCalcMergeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public ProjectCalcMergeRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Calc calc = call.rel(1);

    // Don't merge a project which contains windowed aggregates onto a
    // calc. That would effectively be pushing a windowed aggregate down
    // through a filter.
    final RelOptCluster cluster = project.getCluster();
    RexProgram program =
        RexProgram.create(
            calc.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            cluster.getRexBuilder());
    if (RexOver.containsOver(program)) {
      return;
    }

    // Create a program containing the project node's expressions.
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RexProgramBuilder progBuilder =
        new RexProgramBuilder(
            calc.getRowType(),
            rexBuilder);
    for (Pair<RexNode, String> field : project.getNamedProjects()) {
      progBuilder.addProject(field.left, field.right);
    }
    RexProgram topProgram = progBuilder.getProgram();
    RexProgram bottomProgram = calc.getProgram();

    // Merge the programs together.
    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder);
    final Calc newCalc =
        calc.copy(calc.getTraitSet(), calc.getInput(), mergedProgram);
    call.transformTo(newCalc);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableProjectCalcMergeRule.Config.of()
        .withOperandFor(LogicalProject.class, LogicalCalc.class);

    @Override default ProjectCalcMergeRule toRule() {
      return new ProjectCalcMergeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
        Class<? extends Calc> calcClass) {
      return withOperandSupplier(b0 ->
          b0.operand(projectClass).oneInput(b1 ->
              b1.operand(calcClass).anyInputs()))
          .as(Config.class);
    }
  }
}
