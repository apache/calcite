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
package org.apache.optiq.rel.rules;

import java.util.*;

import org.apache.optiq.rel.*;
import org.apache.optiq.rel.RelFactories.ProjectFactory;
import org.apache.optiq.relopt.*;
import org.apache.optiq.rex.*;

/**
 * MergeProjectRule merges a {@link ProjectRelBase} into another {@link ProjectRelBase},
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
        operand(
            ProjectRel.class,
            operand(ProjectRel.class, any())),
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
