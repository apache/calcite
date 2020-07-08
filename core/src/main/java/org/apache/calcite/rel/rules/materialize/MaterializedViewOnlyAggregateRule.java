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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.tools.RelBuilderFactory;

/** Rule that matches Aggregate. */
public class MaterializedViewOnlyAggregateRule extends MaterializedViewAggregateRule {

  /** @deprecated Use {@link MaterializedViewRules#AGGREGATE}. */
  @Deprecated // to be removed before 1.25
  public static final MaterializedViewOnlyAggregateRule INSTANCE =
      MaterializedViewRules.AGGREGATE;

  public MaterializedViewOnlyAggregateRule(RelBuilderFactory relBuilderFactory,
      boolean generateUnionRewriting, HepProgram unionRewritingPullProgram) {
    super(
        operand(Aggregate.class, any()),
        relBuilderFactory,
        "MaterializedViewAggregateRule(Aggregate)",
        generateUnionRewriting, unionRewritingPullProgram);
  }

  public MaterializedViewOnlyAggregateRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description,
      boolean generateUnionRewriting, HepProgram unionRewritingPullProgram,
      RelOptRule filterProjectTransposeRule,
      RelOptRule filterAggregateTransposeRule,
      RelOptRule aggregateProjectPullUpConstantsRule,
      RelOptRule projectMergeRule) {
    super(
        operand(Aggregate.class, any()),
        relBuilderFactory,
        "MaterializedViewAggregateRule(Aggregate)",
        generateUnionRewriting, unionRewritingPullProgram,
        filterProjectTransposeRule,
        filterAggregateTransposeRule,
        aggregateProjectPullUpConstantsRule,
        projectMergeRule);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    perform(call, null, aggregate);
  }
}
