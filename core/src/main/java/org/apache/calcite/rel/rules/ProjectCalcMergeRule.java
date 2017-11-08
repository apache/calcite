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
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Pair;

/**
 * Planner rule which merges a
 * {@link org.apache.calcite.rel.logical.LogicalProject} and a
 * {@link org.apache.calcite.rel.logical.LogicalCalc}.
 *
 * <p>The resulting {@link org.apache.calcite.rel.logical.LogicalCalc} has the
 * same project list as the original
 * {@link org.apache.calcite.rel.logical.LogicalProject}, but expressed in terms
 * of the original {@link org.apache.calcite.rel.logical.LogicalCalc}'s inputs.
 *
 * @see FilterCalcMergeRule
 */
public class ProjectCalcMergeRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final ProjectCalcMergeRule INSTANCE =
      new ProjectCalcMergeRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ProjectCalcMergeRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public ProjectCalcMergeRule(RelBuilderFactory relBuilderFactory) {
    super(
        operand(
            LogicalProject.class,
            operand(LogicalCalc.class, any())),
        relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    final LogicalCalc calc = call.rel(1);

    // Don't merge a project which contains windowed aggregates onto a
    // calc. That would effectively be pushing a windowed aggregate down
    // through a filter. Transform the project into an identical calc,
    // which we'll have chance to merge later, after the over is
    // expanded.
    final RelOptCluster cluster = project.getCluster();
    RexProgram program =
        RexProgram.create(
            calc.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            cluster.getRexBuilder());
    if (RexOver.containsOver(program)) {
      LogicalCalc projectAsCalc = LogicalCalc.create(calc, program);
      call.transformTo(projectAsCalc);
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
    final LogicalCalc newCalc =
        LogicalCalc.create(calc.getInput(), mergedProgram);
    call.transformTo(newCalc);
  }
}

// End ProjectCalcMergeRule.java
