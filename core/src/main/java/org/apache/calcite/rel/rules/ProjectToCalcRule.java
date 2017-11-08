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
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Rule to convert a
 * {@link org.apache.calcite.rel.logical.LogicalProject} to a
 * {@link org.apache.calcite.rel.logical.LogicalCalc}
 *
 * <p>The rule does not fire if the child is a
 * {@link org.apache.calcite.rel.logical.LogicalProject},
 * {@link org.apache.calcite.rel.logical.LogicalFilter} or
 * {@link org.apache.calcite.rel.logical.LogicalCalc}. If it did, then the same
 * {@link org.apache.calcite.rel.logical.LogicalCalc} would be formed via
 * several transformation paths, which is a waste of effort.</p>
 *
 * @see FilterToCalcRule
 */
public class ProjectToCalcRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  public static final ProjectToCalcRule INSTANCE =
      new ProjectToCalcRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a ProjectToCalcRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public ProjectToCalcRule(RelBuilderFactory relBuilderFactory) {
    super(operand(LogicalProject.class, any()), relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final LogicalProject project = call.rel(0);
    final RelNode input = project.getInput();
    final RexProgram program =
        RexProgram.create(
            input.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            project.getCluster().getRexBuilder());
    final LogicalCalc calc = LogicalCalc.create(input, program);
    call.transformTo(calc);
  }
}

// End ProjectToCalcRule.java
