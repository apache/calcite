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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.Permutation;

import java.util.ArrayList;
import java.util.List;

/**
 * ProjectMergeRule merges a {@link org.apache.calcite.rel.core.Project} into
 * another {@link org.apache.calcite.rel.core.Project},
 * provided the projects aren't projecting identical sets of input references.
 */
public class ProjectMergeRule extends RelOptRule {
  public static final ProjectMergeRule INSTANCE = new ProjectMergeRule();

  //~ Instance fields --------------------------------------------------------

  /** Whether to always merge projects. */
  private final boolean force;

  private final ProjectFactory projectFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ProjectMergeRule.
   */
  private ProjectMergeRule() {
    this(false, RelFactories.DEFAULT_PROJECT_FACTORY);
  }

  /**
   * Creates a ProjectMergeRule, specifying whether to always merge projects.
   *
   * @param force Whether to always merge projects
   */
  public ProjectMergeRule(boolean force, ProjectFactory projectFactory) {
    super(
        operand(Project.class,
            operand(Project.class, any())),
             "ProjectMergeRule" + (force ? ":force_mode" : ""));
    this.force = force;
    this.projectFactory = projectFactory;
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    Project topProject = call.rel(0);
    Project bottomProject = call.rel(1);
    RexBuilder rexBuilder = topProject.getCluster().getRexBuilder();

    // If one or both projects are permutations, short-circuit the complex logic
    // of building a RexProgram.
    final Permutation topPermutation = topProject.getPermutation();
    if (topPermutation != null) {
      if (topPermutation.isIdentity()) {
        // Let ProjectRemoveRule handle this.
        return;
      }
      final Permutation bottomPermutation = bottomProject.getPermutation();
      if (bottomPermutation != null) {
        if (bottomPermutation.isIdentity()) {
          // Let ProjectRemoveRule handle this.
          return;
        }
        final Permutation product = topPermutation.product(bottomPermutation);
        call.transformTo(
            RelOptUtil.projectMapping(bottomProject.getInput(),
                product.inverse(), topProject.getRowType().getFieldNames(),
                projectFactory));
        return;
      }
    }

    // if we're not in force mode and the two projects reference identical
    // inputs, then return and either let FennelRenameRule or
    // ProjectRemoveRule replace the projects
    if (!force) {
      if (RelOptUtil.checkProjAndChildInputs(topProject, false)) {
        return;
      }
    }

    // create a RexProgram for the bottom project
    RexProgram bottomProgram =
        RexProgram.create(
            bottomProject.getInput().getRowType(),
            bottomProject.getProjects(),
            null,
            bottomProject.getRowType(),
            rexBuilder);

    // create a RexProgram for the topmost project
    final List<RexNode> projects = topProject.getProjects();
    RexProgram topProgram =
        RexProgram.create(
            bottomProject.getRowType(),
            projects,
            null,
            topProject.getRowType(),
            rexBuilder);

    // combine the two RexPrograms
    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder);

    // re-expand the topmost projection expressions, now that they
    // reference the children of the bottom-most project
    final int projectCount = projects.size();
    final List<RexNode> newProjects = new ArrayList<>();
    List<RexLocalRef> projectRefs = mergedProgram.getProjectList();
    for (int i = 0; i < projectCount; i++) {
      newProjects.add(mergedProgram.expandLocalRef(projectRefs.get(i)));
    }

    // replace the two projects with a combined projection
    RelNode newProjectRel = projectFactory.createProject(
        bottomProject.getInput(), newProjects,
        topProject.getRowType().getFieldNames());

    call.transformTo(newProjectRel);
  }
}

// End ProjectMergeRule.java
