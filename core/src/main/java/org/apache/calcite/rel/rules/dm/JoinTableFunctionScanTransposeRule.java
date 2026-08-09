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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.rules.TransformationRule;

import org.immutables.value.Value;

/**
 * Rule that matches a {@link LogicalJoin} whose right input is a
 * {@link LogicalTableFunctionScan} and transforms it into a
 * {@link org.apache.calcite.rel.logical.LogicalCorrelate}.
 *
 * <p>This is typically used in migration scenarios where a lateral/cross-apply
 * style table function call appears as the right side of a join. Converting it
 * to a {@code Correlate} enables correct SQL generation for target dialects
 * that use {@code LATERAL} or {@code CROSS APPLY} semantics.
 *
 * <p>The rule is only applicable when the join does not generate nulls on the
 * left side (i.e., {@code RIGHT} and {@code FULL} outer joins are excluded),
 * since {@code Correlate} cannot produce NULL-padded rows for the driving side.
 *
 * <p>The actual transformation logic is provided externally via a
 * {@link RuleMatchExtension}, keeping the canonical rule definition in Calcite
 * and the migration-specific logic in the consumer module.
 *
 * <p>Example SQL that would produce a matching rel tree:
 * <blockquote><pre>
 * SELECT t.*, f.*
 * FROM my_table t
 * JOIN TABLE(my_func(t.col)) AS f ON true
 * </pre></blockquote>
 *
 * @see org.apache.calcite.rel.rules.JoinToCorrelateRule
 */
@Value.Enclosing
public class JoinTableFunctionScanTransposeRule
    extends RelRule<JoinTableFunctionScanTransposeRule.Config>
    implements TransformationRule {

  private RuleMatchExtension extension;

  /** Creates a JoinTableFunctionScanTransposeRule. */
  protected JoinTableFunctionScanTransposeRule(Config config) {
    super(config);
  }

  public void setExtension(RuleMatchExtension extension) {
    this.extension = extension;
  }

  /**
   * Checks whether the matched join is eligible for transformation.
   *
   * <p>The rule is rejected if the join type generates nulls on the left side
   * (RIGHT or FULL outer join), because a {@code Correlate} always drives from
   * the left input and cannot emit NULL-padded left rows.
   */
  @Override public boolean matches(RelOptRuleCall call) {
    final Join join = call.rel(0);
    return !join.getJoinType().generatesNullsOnLeft();
  }

  @Override public void onMatch(RelOptRuleCall call) {
    extension.execute(call);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {

    /**
     * Default configuration matching a {@link LogicalJoin} whose left input
     * is any {@link RelNode} and whose right input is a
     * {@link LogicalTableFunctionScan}.
     */
    Config DEFAULT = ImmutableJoinTableFunctionScanTransposeRule.Config.of()
        .withOperandSupplier(b0 ->
            b0.operand(LogicalJoin.class).inputs(
                b1 -> b1.operand(RelNode.class).anyInputs(),
                b2 -> b2.operand(LogicalTableFunctionScan.class).anyInputs()))
        .withDescription("JoinTableFunctionScanTransposeRule")
        .as(Config.class);

    @Override default JoinTableFunctionScanTransposeRule toRule() {
      return new JoinTableFunctionScanTransposeRule(this);
    }
  }
}
