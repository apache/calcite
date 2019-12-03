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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Project}
 * past a {@link org.apache.calcite.rel.core.Filter}.
 */
public class ProjectFilterTransposeRule extends RelOptRule {
  public static final ProjectFilterTransposeRule INSTANCE =
      new ProjectFilterTransposeRule(LogicalProject.class, LogicalFilter.class,
          RelFactories.LOGICAL_BUILDER, expr -> false);

  //~ Instance fields --------------------------------------------------------

  /**
   * Expressions that should be preserved in the projection
   */
  private final PushProjector.ExprCondition preserveExprCondition;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ProjectFilterTransposeRule.
   *
   * @param preserveExprCondition Condition for expressions that should be
   *                              preserved in the projection
   */
  public ProjectFilterTransposeRule(
      Class<? extends Project> projectClass,
      Class<? extends Filter> filterClass,
      RelBuilderFactory relBuilderFactory,
      PushProjector.ExprCondition preserveExprCondition) {
    this(
        operand(
            projectClass,
            operand(filterClass, any())),
        preserveExprCondition, relBuilderFactory);
  }

  protected ProjectFilterTransposeRule(RelOptRuleOperand operand,
      PushProjector.ExprCondition preserveExprCondition,
      RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, null);
    this.preserveExprCondition = preserveExprCondition;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    Project origProj;
    Filter filter;
    if (call.rels.length >= 2) {
      origProj = call.rel(0);
      filter = call.rel(1);
    } else {
      origProj = null;
      filter = call.rel(0);
    }
    RelNode rel = filter.getInput();
    RexNode origFilter = filter.getCondition();

    if ((origProj != null)
        && RexOver.containsOver(origProj.getProjects(), null)) {
      // Cannot push project through filter if project contains a windowed
      // aggregate -- it will affect row counts. Abort this rule
      // invocation; pushdown will be considered after the windowed
      // aggregate has been implemented. It's OK if the filter contains a
      // windowed aggregate.
      return;
    }

    if ((origProj != null)
        && origProj.getRowType().isStruct()
        && origProj.getRowType().getFieldList().stream()
          .anyMatch(RelDataTypeField::isDynamicStar)) {
      // The PushProjector would change the plan:
      //
      //    prj(**=[$0])
      //    : - filter
      //        : - scan
      //
      // to form like:
      //
      //    prj(**=[$0])                    (1)
      //    : - filter                      (2)
      //        : - prj(**=[$0], ITEM= ...) (3)
      //            :  - scan
      // This new plan has more cost that the old one, because of the new
      // redundant project (3), if we also have FilterProjectTransposeRule in
      // the rule set, it will also trigger infinite match of the ProjectMergeRule
      // for project (1) and (3).
      return;
    }

    PushProjector pushProjector =
        new PushProjector(
            origProj, origFilter, rel, preserveExprCondition, call.builder());
    RelNode topProject = pushProjector.convertProject(null);

    if (topProject != null) {
      call.transformTo(topProject);
    }
  }
}

// End ProjectFilterTransposeRule.java
