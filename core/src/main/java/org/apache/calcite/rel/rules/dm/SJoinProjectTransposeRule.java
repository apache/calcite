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
package org.apache.calcite.rel.rules.dm;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.dm.SJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.TransformationRule;

import org.immutables.value.Value;

/**
 * Extension of {@link JoinProjectTransposeRule} which matches {@link SJoin}
 * and avoids transpose if JOIN condition is complex.
 */
@Value.Enclosing
public class SJoinProjectTransposeRule
    extends RelRule<SJoinProjectTransposeRule.Config>
    implements TransformationRule {
  private static final int COMPLEXITY_THRESHOLD = 50;
  private JoinProjectTransposeRule joinRule;

  public SJoinProjectTransposeRule(Config config) {
    super(config);
    joinRule = JoinProjectTransposeRule.Config.DEFAULT.toRule();
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Join join = call.rel(0);
    return join.getCondition().nodeCount() < COMPLEXITY_THRESHOLD;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    joinRule.onMatch(call);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config LEFT = ImmutableSJoinProjectTransposeRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(SJoin.class).inputs(
                b1 -> b1.operand(LogicalProject.class).anyInputs()))
        .withDescription("SJoinProjectTransposeRule(Project-Other)");


    @Override default SJoinProjectTransposeRule toRule() {
      return new SJoinProjectTransposeRule(this);
    }

  }
}
