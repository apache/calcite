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
import org.eigenbase.rex.*;
import org.eigenbase.util.Permutation;

/**
 * MergeProjectRule merges a {@link ProjectRelBase} into
 * another {@link ProjectRelBase},
 * provided the projects aren't projecting identical sets of input references.
 */
public class MergeProjectRule extends RelOptRule {
  public static final MergeProjectRule INSTANCE =
      new MergeProjectRule();

  //~ Instance fields --------------------------------------------------------

  /**
   * if true, always merge projects
   */
  private final boolean force;

  private final ProjectFactory projectFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a MergeProjectRule.
   */
  private MergeProjectRule() {
    this(false, RelFactories.DEFAULT_PROJECT_FACTORY);
  }

  /**
   * Creates a MergeProjectRule, specifying whether to always merge projects.
   *
   * @param force Whether to always merge projects
   */
  public MergeProjectRule(boolean force, ProjectFactory pFactory) {
    super(
        operand(ProjectRelBase.class,
            operand(ProjectRelBase.class, any())),
        "MergeProjectRule" + (force ? ": force mode" : ""));
    this.force = force;
    projectFactory = pFactory;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    ProjectRelBase topProject = call.rel(0);
    ProjectRelBase bottomProject = call.rel(1);
    RexBuilder rexBuilder = topProject.getCluster().getRexBuilder();

    // If one or both projects are permutations, short-circuit the complex logic
    // of building a RexProgram.
    final Permutation topPermutation = topProject.getPermutation();
    if (topPermutation != null) {
      if (topPermutation.isIdentity()) {
        // Let RemoveTrivialProjectRule handle this.
        return;
      }
      final Permutation bottomPermutation = bottomProject.getPermutation();
      if (bottomPermutation != null) {
        if (bottomPermutation.isIdentity()) {
          // Let RemoveTrivialProjectRule handle this.
          return;
        }
        final Permutation product = topPermutation.product(bottomPermutation);
        call.transformTo(
            RelOptUtil.projectMapping(bottomProject.getChild(),
                product.inverse(), topProject.getRowType().getFieldNames(),
                projectFactory));
        return;
      }
    }

    // if we're not in force mode and the two projects reference identical
    // inputs, then return and either let FennelRenameRule or
    // RemoveTrivialProjectRule replace the projects
    if (!force) {
      if (RelOptUtil.checkProjAndChildInputs(topProject, false)) {
        return;
      }
    }

    // create a RexProgram for the bottom project
    RexProgram bottomProgram =
        RexProgram.create(
            bottomProject.getChild().getRowType(),
            bottomProject.getProjects(),
            null,
            bottomProject.getRowType(),
            rexBuilder);

    // create a RexProgram for the topmost project
    List<RexNode> projExprs = topProject.getProjects();
    RexProgram topProgram =
        RexProgram.create(
            bottomProject.getRowType(),
            projExprs,
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
    int nProjExprs = projExprs.size();
    List<RexNode> newProjExprs = new ArrayList<RexNode>();
    List<RexLocalRef> projList = mergedProgram.getProjectList();
    for (int i = 0; i < nProjExprs; i++) {
      newProjExprs.add(mergedProgram.expandLocalRef(projList.get(i)));
    }

    // replace the two projects with a combined projection
    RelNode newProjectRel = projectFactory.createProject(
        bottomProject.getChild(), newProjExprs,
        topProject.getRowType().getFieldNames());

    call.transformTo(newProjectRel);
  }
}

// End MergeProjectRule.java
