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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.tools.RelBuilderFactory;

/** Variant of {@link org.apache.calcite.rel.rules.ProjectToCalcRule} for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableProjectToCalcRule extends RelOptRule {

  @Deprecated // to be removed before 2.0
  EnumerableProjectToCalcRule() {
    this(RelFactories.LOGICAL_BUILDER);
  }

  /**
   * Creates an EnumerableProjectToCalcRule.
   *
   * @param relBuilderFactory Builder for relational expressions
   */
  public EnumerableProjectToCalcRule(RelBuilderFactory relBuilderFactory) {
    super(operand(EnumerableProject.class, any()), relBuilderFactory, null);
  }

  public void onMatch(RelOptRuleCall call) {
    final EnumerableProject project = call.rel(0);
    final RelNode input = project.getInput();
    final RexProgram program =
        RexProgram.create(input.getRowType(),
            project.getProjects(),
            null,
            project.getRowType(),
            project.getCluster().getRexBuilder());
    final EnumerableCalc calc = EnumerableCalc.create(input, program);
    call.transformTo(calc);
  }
}

// End EnumerableProjectToCalcRule.java
