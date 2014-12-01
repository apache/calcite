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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;

import com.google.common.collect.ImmutableList;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.logical.LogicalProject}
 * past a {@link org.apache.calcite.rel.core.Sort}.
 *
 * @see org.apache.calcite.rel.rules.SortProjectTransposeRule
 */
public class ProjectSortTransposeRule extends RelOptRule {
  public static final ProjectSortTransposeRule INSTANCE =
      new ProjectSortTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ProjectSortTransposeRule.
   */
  private ProjectSortTransposeRule() {
    super(
        operand(LogicalProject.class,
            operand(Sort.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    LogicalProject project = call.rel(0);
    Sort sort = call.rel(1);
    if (sort.getClass() != Sort.class) {
      return;
    }
    RelNode newProject =
        project.copy(
            project.getTraitSet(), ImmutableList.of(sort.getInput()));
    final Sort newSort =
        sort.copy(
            sort.getTraitSet(),
            newProject,
            sort.getCollation(),
            sort.offset,
            sort.fetch);
    call.transformTo(newSort);
  }
}

// End ProjectSortTransposeRule.java
