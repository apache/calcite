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

import java.util.Map;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Planner rule that pushes a {@link SortRel} past a {@link ProjectRel}.
 */
public class PushSortPastProjectRule extends RelOptRule {
  public static final PushSortPastProjectRule INSTANCE =
      new PushSortPastProjectRule();

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a PushSortPastProjectRule.
   */
  private PushSortPastProjectRule() {
    super(
        operand(
            SortRel.class,
            operand(ProjectRel.class, any())));
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final SortRel sort = call.rel(0);
    final ProjectRel project = call.rel(1);
    final RelOptCluster cluster = project.getCluster();

    if (sort.getConvention() != project.getConvention()) {
      return;
    }

    // Determine mapping between project input and output fields. If sort
    // relies on non-trivial expressions, we can't push.
    final Mappings.TargetMapping map =
        RelOptUtil.permutation(
            project.getProjects(), project.getChild().getRowType());
    for (RelFieldCollation fc : sort.getCollation().getFieldCollations()) {
      if (map.getTargetOpt(fc.getFieldIndex()) < 0) {
        return;
      }
    }
    final RelCollation newCollation =
        cluster.traitSetOf().canonize(
            RexUtil.apply(map, sort.getCollation()));
    final SortRel newSort =
        sort.copy(
            sort.getTraitSet().replace(newCollation),
            project.getChild(),
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
            ? ImmutableMap.<RelNode, RelNode>of(newSort, project.getChild())
            : ImmutableMap.<RelNode, RelNode>of();
    call.transformTo(newProject, equiv);
  }
}

// End PushSortPastProjectRule.java
