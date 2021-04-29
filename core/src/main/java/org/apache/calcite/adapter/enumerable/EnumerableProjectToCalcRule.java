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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.tools.RelBuilderFactory;

/** Variant of {@link org.apache.calcite.rel.rules.ProjectToCalcRule} for
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 *
 * @see EnumerableRules#ENUMERABLE_PROJECT_TO_CALC_RULE */
public class EnumerableProjectToCalcRule extends ProjectToCalcRule {
  /** Creates an EnumerableProjectToCalcRule. */
  protected EnumerableProjectToCalcRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public EnumerableProjectToCalcRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  @Override public void onMatch(RelOptRuleCall call) {
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

  /** Rule configuration. */
  public interface Config extends ProjectToCalcRule.Config {
    Config DEFAULT = ProjectToCalcRule.Config.DEFAULT
        .withOperandSupplier(b ->
            b.operand(EnumerableProject.class).anyInputs())
        .as(Config.class);

    @Override default EnumerableProjectToCalcRule toRule() {
      return new EnumerableProjectToCalcRule(this);
    }
  }
}
