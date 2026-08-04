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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.logical.LogicalValues;

import org.immutables.value.Value;

/**
 * Rule that moves the outer join semantics of a {@link Correlate} over an
 * {@link Uncollect} onto the {@code Uncollect} itself.
 *
 * <p>Input plan:
 * <pre>
 * Correlate(cor=[$cor0], joinType=[left])
 *   left (any RelNode)
 *   Uncollect(isOuter=[any_boolean])
 *     Project($cor0.f, ...)
 *       LogicalValues(tuples=[[{ 0 }]])
 * </pre>
 *
 * <p>Converted to:
 * <pre>
 * Correlate(cor=[$cor0], joinType=[inner])
 *   left
 *   Uncollect(isOuter=[true])
 *     Project($cor0.f, ...)
 *       LogicalValues(tuples=[[{ 0 }]])
 * </pre>
 *
 * @see CoreRules#CORRELATE_UNCOLLECT_OUTER
 */
@Value.Enclosing
public class CorrelateUncollectOuterRule
    extends RelRule<CorrelateUncollectOuterRule.Config>
    implements TransformationRule {

  protected CorrelateUncollectOuterRule(Config config) {
    super(config);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    final Correlate correlate = call.rel(0);
    if (correlate.getJoinType() != JoinRelType.LEFT) {
      return false;
    }
    // Expect "LogicalValues { 0 }"
    final LogicalValues values = call.rel(4);
    return values.getTuples().size() == 1;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Correlate correlate = call.rel(0);
    final Uncollect uncollect = call.rel(2);

    // Note: this is correct even if uncollect(isOuter=[true]) already
    final Uncollect outerUncollect =
        Uncollect.create(uncollect.getTraitSet(), uncollect.getInput(),
            uncollect.withOrdinality, uncollect.getItemAliases(),
            uncollect.expandStructFields, true);
    final RelNode newCorrelate =
        correlate.copy(correlate.getTraitSet(), correlate.getLeft(),
            outerUncollect, correlate.getCorrelationId(),
            correlate.getRequiredColumns(), JoinRelType.INNER);
    call.transformTo(newCorrelate);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableCorrelateUncollectOuterRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(Correlate.class).inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(Uncollect.class)
                    .oneInput(b3 -> b3.operand(Project.class)
                        .oneInput(b4 -> b4.operand(LogicalValues.class)
                            .anyInputs()))));

    @Override default CorrelateUncollectOuterRule toRule() {
      return new CorrelateUncollectOuterRule(this);
    }
  }
}
