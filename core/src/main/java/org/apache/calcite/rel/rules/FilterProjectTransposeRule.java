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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.logical.LogicalFilter}
 * past a {@link org.apache.calcite.rel.logical.LogicalProject}.
 */
public class FilterProjectTransposeRule extends RelOptRule {
  /** The default instance of
   * {@link org.apache.calcite.rel.rules.FilterProjectTransposeRule}.
   *
   * <p>It matches any kind of join or filter, and generates the same kind of
   * join and filter. It uses null values for {@code filterFactory} and
   * {@code projectFactory} to achieve this. */
  public static final FilterProjectTransposeRule INSTANCE =
      new FilterProjectTransposeRule(
          Filter.class, null,
          Project.class, null);

  private final RelFactories.FilterFactory filterFactory;
  private final RelFactories.ProjectFactory projectFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FilterProjectTransposeRule.
   *
   * <p>If {@code filterFactory} is null, creates the same kind of filter as
   * matched in the rule. Similarly {@code projectFactory}.</p>
   */
  public FilterProjectTransposeRule(
      Class<? extends Filter> filterClass,
      RelFactories.FilterFactory filterFactory,
      Class<? extends Project> projectClass,
      RelFactories.ProjectFactory projectFactory) {
    super(
        operand(filterClass,
            operand(projectClass, any())));
    this.filterFactory = filterFactory;
    this.projectFactory = projectFactory;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final Filter filterRel = call.rel(0);
    final Project projRel = call.rel(1);

    if (RexOver.containsOver(projRel.getProjects(), null)) {
      // In general a filter cannot be pushed below a windowing calculation.
      // Applying the filter before the aggregation function changes
      // the results of the windowing invocation.
      //
      // When the filter is on the PARTITION BY expression of the OVER clause
      // it can be pushed down. For now we don't support this.
      return;
    }

    // convert the filter to one that references the child of the project
    RexNode newCondition =
        RelOptUtil.pushFilterPastProject(filterRel.getCondition(), projRel);

    RelNode newFilterRel =
        filterFactory == null
            ? filterRel.copy(filterRel.getTraitSet(), projRel.getInput(),
                newCondition)
            : filterFactory.createFilter(projRel.getInput(), newCondition);

    RelNode newProjRel =
        projectFactory == null
            ? projRel.copy(projRel.getTraitSet(), newFilterRel,
                projRel.getProjects(), projRel.getRowType())
            : projectFactory.createProject(newFilterRel, projRel.getProjects(),
                projRel.getRowType().getFieldNames());

    call.transformTo(newProjRel);
  }
}

// End FilterProjectTransposeRule.java
