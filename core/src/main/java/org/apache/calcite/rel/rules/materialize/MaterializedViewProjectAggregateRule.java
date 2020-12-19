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
package org.apache.calcite.rel.rules.materialize;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

/** Rule that matches Project on Aggregate.
 *
 * @see MaterializedViewRules#PROJECT_AGGREGATE */
public class MaterializedViewProjectAggregateRule
    extends MaterializedViewAggregateRule<MaterializedViewProjectAggregateRule.Config> {

  private MaterializedViewProjectAggregateRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public MaterializedViewProjectAggregateRule(RelBuilderFactory relBuilderFactory,
      boolean generateUnionRewriting, HepProgram unionRewritingPullProgram) {
    this(Config.create(relBuilderFactory)
        .withGenerateUnionRewriting(generateUnionRewriting)
        .withUnionRewritingPullProgram(unionRewritingPullProgram)
        .as(Config.class));
  }

  @Deprecated // to be removed before 2.0
  public MaterializedViewProjectAggregateRule(RelBuilderFactory relBuilderFactory,
      boolean generateUnionRewriting, HepProgram unionRewritingPullProgram,
      RelOptRule filterProjectTransposeRule,
      RelOptRule filterAggregateTransposeRule,
      RelOptRule aggregateProjectPullUpConstantsRule,
      RelOptRule projectMergeRule) {
    this(Config.create(relBuilderFactory)
        .withGenerateUnionRewriting(generateUnionRewriting)
        .withUnionRewritingPullProgram(unionRewritingPullProgram)
        .as(Config.class)
        .withFilterProjectTransposeRule(filterProjectTransposeRule)
        .withFilterAggregateTransposeRule(filterAggregateTransposeRule)
        .withAggregateProjectPullUpConstantsRule(
            aggregateProjectPullUpConstantsRule)
        .withProjectMergeRule(projectMergeRule)
        .as(Config.class));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Aggregate aggregate = call.rel(1);
    perform(call, project, aggregate);
  }

  /** Rule configuration. */
  public interface Config extends MaterializedViewAggregateRule.Config {
    Config DEFAULT = create(RelFactories.LOGICAL_BUILDER);

    static Config create(RelBuilderFactory relBuilderFactory) {
      return MaterializedViewAggregateRule.Config.create(relBuilderFactory)
          .withGenerateUnionRewriting(true)
          .withUnionRewritingPullProgram(null)
          .withOperandSupplier(b0 ->
              b0.operand(Project.class).oneInput(b1 ->
                  b1.operand(Aggregate.class).anyInputs()))
          .withDescription("MaterializedViewAggregateRule(Project-Aggregate)")
          .as(Config.class);
    }

    @Override default MaterializedViewProjectAggregateRule toRule() {
      return new MaterializedViewProjectAggregateRule(this);
    }
  }
}
