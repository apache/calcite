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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;

/**
 * Planner rule that pushes
 * a {@link org.apache.calcite.rel.core.Project}
 * past a {@link org.apache.calcite.rel.core.Sort}.
 *
 * @see org.apache.calcite.rel.rules.SortProjectTransposeRule
 */
@Deprecated // to be removed before 1.25
public class ProjectSortTransposeRule
    extends RelRule<ProjectSortTransposeRule.Config>
    implements TransformationRule {
  public static final ProjectSortTransposeRule INSTANCE =
      Config.DEFAULT.toRule();

  //~ Constructors -----------------------------------------------------------

  /** Creates a ProjectSortTransposeRule. */
  protected ProjectSortTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  protected ProjectSortTransposeRule(RelOptRuleOperand operand) {
    this(Config.DEFAULT.withOperandSupplier(b -> b.exactly(operand))
        .as(Config.class));
  }

  @Deprecated // to be removed before 2.0
  protected ProjectSortTransposeRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier(b -> b.exactly(operand))
        .withDescription(description)
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Sort sort = call.rel(1);
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

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(Project.class, Sort.class);

    @Override default ProjectSortTransposeRule toRule() {
      return new ProjectSortTransposeRule(this);
    }

    /** Defines an operand tree for the given 2 classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
        Class<? extends Sort> sortClass) {
      return withOperandSupplier(b0 ->
          b0.operand(projectClass).oneInput(b1 ->
              b1.operand(sortClass).anyInputs()))
          .as(Config.class);
    }

    /** Defines an operand tree for the given 3 classes. */
    default Config withOperandFor(Class<? extends Project> projectClass,
        Class<? extends Sort> sortClass, Class<? extends RelNode> relClass) {
      return withOperandSupplier(b0 ->
          b0.operand(projectClass).oneInput(b1 ->
              b1.operand(sortClass).oneInput(b2 ->
                  b2.operand(relClass).anyInputs())))
          .as(Config.class);
    }
  }
}
