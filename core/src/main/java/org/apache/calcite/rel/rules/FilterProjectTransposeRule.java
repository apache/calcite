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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.core.Filter}
 * past a {@link org.apache.calcite.rel.core.Project}.
 */
public class FilterProjectTransposeRule extends RelOptRule {
  /** The default instance of
   * {@link org.apache.calcite.rel.rules.FilterProjectTransposeRule}.
   *
   * <p>It matches any kind of {@link org.apache.calcite.rel.core.Join} or
   * {@link org.apache.calcite.rel.core.Filter}, and generates the same kind of
   * Join and Filter.
   *
   * <p>It does not allow a Filter to be pushed past the Project if
   * {@link RexUtil#containsCorrelation there is a correlation condition})
   * anywhere in the Filter, since in some cases it can prevent a
   * {@link org.apache.calcite.rel.core.Correlate} from being de-correlated.
   */
  public static final FilterProjectTransposeRule INSTANCE =
      new FilterProjectTransposeRule(LogicalFilter.class, LogicalProject.class, true, true,
          RelFactories.LOGICAL_BUILDER);

  private final boolean copyFilter;
  private final boolean copyProject;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a FilterProjectTransposeRule.
   *
   * <p>Equivalent to the rule created by
   * {@link #FilterProjectTransposeRule(Class, Predicate, Class, Predicate, boolean, boolean, RelBuilderFactory)}
   * with some default predicates that do not allow a filter to be pushed
   * past the project if there is a correlation condition anywhere in the
   * filter (since in some cases it can prevent a
   * {@link org.apache.calcite.rel.core.Correlate} from being de-correlated).
   */
  public FilterProjectTransposeRule(
      Class<? extends Filter> filterClass,
      Class<? extends Project> projectClass,
      boolean copyFilter, boolean copyProject,
      RelBuilderFactory relBuilderFactory) {
    this(filterClass,
        filter -> !RexUtil.containsCorrelation(filter.getCondition()),
        projectClass, project -> true,
        copyFilter, copyProject, relBuilderFactory);
  }

  /**
   * Creates a FilterProjectTransposeRule.
   *
   * <p>If {@code copyFilter} is true, creates the same kind of Filter as
   * matched in the rule, otherwise it creates a Filter using the RelBuilder
   * obtained by the {@code relBuilderFactory}.
   * Similarly for {@code copyProject}.
   *
   * <p>Defining predicates for the Filter (using {@code filterPredicate})
   * and/or the Project (using {@code projectPredicate} allows making the rule
   * more restrictive.
   */
  public <F extends Filter, P extends Project> FilterProjectTransposeRule(
      Class<F> filterClass,
      Predicate<? super F> filterPredicate,
      Class<P> projectClass,
      Predicate<? super P> projectPredicate,
      boolean copyFilter, boolean copyProject,
      RelBuilderFactory relBuilderFactory) {
    this(
        operandJ(filterClass, null, filterPredicate,
            operandJ(projectClass, null, projectPredicate, any())),
        copyFilter, copyProject, relBuilderFactory);
  }

  @Deprecated // to be removed before 2.0
  public FilterProjectTransposeRule(
      Class<? extends Filter> filterClass,
      RelFactories.FilterFactory filterFactory,
      Class<? extends Project> projectClass,
      RelFactories.ProjectFactory projectFactory) {
    this(filterClass, filter -> !RexUtil.containsCorrelation(filter.getCondition()),
        projectClass, project -> true,
        filterFactory == null,
        projectFactory == null,
        RelBuilder.proto(filterFactory, projectFactory));
  }

  protected FilterProjectTransposeRule(
      RelOptRuleOperand operand,
      boolean copyFilter,
      boolean copyProject,
      RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, null);
    this.copyFilter = copyFilter;
    this.copyProject = copyProject;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean matches(RelOptRuleCall call) {
    return call.rel(0).getConvention() == call.rel(1).getConvention();
  }

  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Project project = call.rel(1);

    if (RexOver.containsOver(project.getProjects(), null)) {
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
        RelOptUtil.pushPastProject(filter.getCondition(), project);

    final RelBuilder relBuilder = call.builder();
    RelNode newFilterRel;
    if (copyFilter) {
      newFilterRel = filter.copy(filter.getTraitSet(), project.getInput(),
          RexUtil.removeNullabilityCast(relBuilder.getTypeFactory(),
              newCondition));
    } else {
      newFilterRel =
          relBuilder.push(project.getInput()).filter(newCondition).build();
    }

    RelNode newProjRel =
        copyProject
            ? project.copy(project.getTraitSet(), newFilterRel,
                project.getProjects(), project.getRowType())
            : relBuilder.push(newFilterRel)
                .project(project.getProjects(), project.getRowType().getFieldNames())
                .build();

    call.transformTo(newProjRel);
  }
}

// End FilterProjectTransposeRule.java
