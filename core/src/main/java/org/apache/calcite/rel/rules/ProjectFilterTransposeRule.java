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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Planner rule that pushes a {@link org.apache.calcite.rel.core.Project}
 * past a {@link org.apache.calcite.rel.core.Filter}.
 */
public class ProjectFilterTransposeRule extends RelOptRule
    implements TransformationRule {
  public static final ProjectFilterTransposeRule INSTANCE =
      new ProjectFilterTransposeRule(LogicalProject.class, LogicalFilter.class,
          RelFactories.LOGICAL_BUILDER, expr -> false, false, false);

  /** Instance that pushes down project and filter expressions whole, not field
   * references. */
  public static final ProjectFilterTransposeRule EXPRESSION_INSTANCE =
      new ProjectFilterTransposeRule(LogicalProject.class, LogicalFilter.class,
          RelFactories.LOGICAL_BUILDER, expr -> false, true, true);

  /** Instance that pushes down project expressions whole, but pushes down
   * field references for filters. */
  public static final ProjectFilterTransposeRule PROJECT_EXPRESSION_INSTANCE =
      new ProjectFilterTransposeRule(LogicalProject.class, LogicalFilter.class,
          RelFactories.LOGICAL_BUILDER, expr -> false, true, false);

  //~ Instance fields --------------------------------------------------------

  /**
   * Expressions that should be preserved in the projection
   */
  private final PushProjector.ExprCondition preserveExprCondition;
  private final boolean wholeProject;
  private final boolean wholeFilter;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ProjectFilterTransposeRule.
   *
   * @param preserveExprCondition Condition for expressions that should be
   *                              preserved in the projection
   * @param wholeProject Whether to push whole expressions from the project;
   *                   if false, only pushes references
   * @param wholeFilter Whether to push whole expressions;
   *                   if false, only pushes references
   */
  public ProjectFilterTransposeRule(
      Class<? extends Project> projectClass,
      Class<? extends Filter> filterClass,
      RelBuilderFactory relBuilderFactory,
      PushProjector.ExprCondition preserveExprCondition,
      boolean wholeProject, boolean wholeFilter) {
    this(
        operand(
            projectClass,
            operand(filterClass, any())),
        preserveExprCondition, wholeProject, wholeFilter, relBuilderFactory);
  }

  @Deprecated
  public ProjectFilterTransposeRule(
      Class<? extends Project> projectClass,
      Class<? extends Filter> filterClass,
      RelBuilderFactory relBuilderFactory,
      PushProjector.ExprCondition preserveExprCondition) {
    this(projectClass, filterClass, relBuilderFactory, preserveExprCondition,
        false, false);
  }

  protected ProjectFilterTransposeRule(RelOptRuleOperand operand,
      PushProjector.ExprCondition preserveExprCondition, boolean wholeProject,
      boolean wholeFilter, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, null);
    this.preserveExprCondition = preserveExprCondition;
    this.wholeProject = wholeProject;
    this.wholeFilter = wholeFilter;
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final Project origProject;
    final Filter filter;
    if (call.rels.length >= 2) {
      origProject = call.rel(0);
      filter = call.rel(1);
    } else {
      origProject = null;
      filter = call.rel(0);
    }
    final RelNode input = filter.getInput();
    final RexNode origFilter = filter.getCondition();

    if ((origProject != null)
        && RexOver.containsOver(origProject.getProjects(), null)) {
      // Cannot push project through filter if project contains a windowed
      // aggregate -- it will affect row counts. Abort this rule
      // invocation; pushdown will be considered after the windowed
      // aggregate has been implemented. It's OK if the filter contains a
      // windowed aggregate.
      return;
    }

    if ((origProject != null)
        && origProject.getRowType().isStruct()
        && origProject.getRowType().getFieldList().stream()
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

    final RelBuilder builder = call.builder();
    final RelNode topProject;
    if (origProject != null && (wholeProject || wholeFilter)) {
      builder.push(input);

      final Set<RexNode> set = new LinkedHashSet<>();
      final RelOptUtil.InputFinder refCollector = new RelOptUtil.InputFinder();

      if (wholeFilter) {
        set.add(filter.getCondition());
      } else {
        filter.getCondition().accept(refCollector);
      }
      if (wholeProject) {
        set.addAll(origProject.getProjects());
      } else {
        refCollector.visitEach(origProject.getProjects());
      }

      // Build a list with inputRefs, in order, first, then other expressions.
      final List<RexNode> list = new ArrayList<>();
      final ImmutableBitSet refs = refCollector.build();
      for (RexNode field : builder.fields()) {
        if (refs.get(((RexInputRef) field).getIndex()) || set.contains(field)) {
          list.add(field);
        }
      }
      set.removeAll(list);
      list.addAll(set);
      builder.project(list);
      final Replacer replacer = new Replacer(list, builder);
      builder.filter(replacer.visit(filter.getCondition()));
      builder.project(replacer.visitList(origProject.getProjects()),
          origProject.getRowType().getFieldNames());
      topProject = builder.build();
    } else {
      // The traditional mode of operation of this rule: push down field
      // references. The effect is similar to RelFieldTrimmer.
      final PushProjector pushProjector =
          new PushProjector(origProject, origFilter, input,
              preserveExprCondition, builder);
      topProject = pushProjector.convertProject(null);
    }

    if (topProject != null) {
      call.transformTo(topProject);
    }
  }

  /** Replaces whole expressions, or parts of an expression, with references to
   * expressions computed by an underlying Project. */
  private static class Replacer extends RexShuttle {
    final ImmutableMap<RexNode, Integer> map;
    final RelBuilder relBuilder;

    Replacer(Iterable<? extends RexNode> exprs, RelBuilder relBuilder) {
      this.relBuilder = relBuilder;
      final ImmutableMap.Builder<RexNode, Integer> b = ImmutableMap.builder();
      int i = 0;
      for (RexNode expr : exprs) {
        b.put(expr, i++);
      }
      map = b.build();
    }

    RexNode visit(RexNode e) {
      final Integer i = map.get(e);
      if (i != null) {
        return relBuilder.field(i);
      }
      return e.accept(this);
    }

    @Override public void visitList(Iterable<? extends RexNode> exprs,
        List<RexNode> out) {
      for (RexNode expr : exprs) {
        out.add(visit(expr));
      }
    }

    @Override protected List<RexNode> visitList(List<? extends RexNode> exprs,
        boolean[] update) {
      ImmutableList.Builder<RexNode> clonedOperands = ImmutableList.builder();
      for (RexNode operand : exprs) {
        RexNode clonedOperand = visit(operand);
        if ((clonedOperand != operand) && (update != null)) {
          update[0] = true;
        }
        clonedOperands.add(clonedOperand);
      }
      return clonedOperands.build();
    }
  }
}
