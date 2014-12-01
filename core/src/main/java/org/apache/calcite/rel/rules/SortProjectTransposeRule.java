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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.core.Sort}
 * past a {@link org.apache.calcite.rel.logical.LogicalProject}.
 *
 * @see org.apache.calcite.rel.rules.ProjectSortTransposeRule
 */
public class SortProjectTransposeRule extends RelOptRule {
  public static final SortProjectTransposeRule INSTANCE =
      new SortProjectTransposeRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SortProjectTransposeRule.
   */
  private SortProjectTransposeRule() {
    super(
        operand(
            Sort.class,
            operand(LogicalProject.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final Sort sort = call.rel(0);
    final LogicalProject project = call.rel(1);
    final RelOptCluster cluster = project.getCluster();

    if (sort.getConvention() != project.getConvention()) {
      return;
    }

    // Determine mapping between project input and output fields. If sort
    // relies on non-trivial expressions, we can't push.
    final Mappings.TargetMapping map =
        RelOptUtil.permutation(
            project.getProjects(), project.getInput().getRowType());
    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
      if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
        return;
      }
    }
    final RelCollation newCollation =
        cluster.traitSetOf().canonize(
            RexUtil.apply(map, sort.getCollation()));
    final Sort newSort =
        sort.copy(
            sort.getTraitSet().replace(newCollation),
            project.getInput(),
            newCollation,
            sort.offset,
            sort.fetch);
    RelNode newProject =
        project.copy(
            sort.getTraitSet(),
            ImmutableList.<RelNode>of(newSort));
    // Not only is newProject equivalent to sort;
    // newSort is equivalent to project's input
    // (but only if the sort is not also applying an offset/limit).
    Map<RelNode, RelNode> equiv =
        sort.offset == null && sort.fetch == null
            ? ImmutableMap.<RelNode, RelNode>of(newSort, project.getInput())
            : ImmutableMap.<RelNode, RelNode>of();
    call.transformTo(newProject, equiv);
  }
}

// End SortProjectTransposeRule.java
