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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

/**
 * Planner rule that replaces {@code IS NOT DISTINCT FROM}
 * in a {@link Join} condition with logically equivalent operations.
 *
 * @see SqlStdOperatorTable#IS_NOT_DISTINCT_FROM
 * @see RelBuilder#isNotDistinctFrom
 */
@Value.Enclosing
public final class JoinConditionExpandIsNotDistinctFromRule
    extends RelRule<JoinConditionExpandIsNotDistinctFromRule.Config>
    implements TransformationRule {

  /** Creates a JoinConditionExpandIsNotDistinctFromRule. */
  JoinConditionExpandIsNotDistinctFromRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RexNode oldJoinCond = join.getCondition();

    RexCall found =
        RexUtil.findOperatorCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        oldJoinCond);

    if (found == null) {
      // no longer contains isNotDistinctFromOperator
      return;
    }

    for (RexNode op : found.getOperands()) {
      if (op.getType().getSqlTypeName() == SqlTypeName.MAP) {
        // map type cannot be compared
        return;
      }
    }

    RemoveIsNotDistinctFromRexShuttle rewriteShuttle =
        new RemoveIsNotDistinctFromRexShuttle(
            join.getCluster().getRexBuilder());

    RelNode newJoin = join.accept(rewriteShuttle);
    assert newJoin instanceof Join;
    if (((Join) newJoin).getCondition().equals(join.getCondition())) {
      return;
    }
    call.transformTo(newJoin);
  }

  //~ Inner Classes ----------------------------------------------------------

  /** The RexShuttle use for converting 'x IS NOT DISTINCT FROM y' to
   * '(COALESCE(x, {zero}) = COALESCE(y, {zero}))
   * AND ((x IS NULL) = (y IS NULL))'. */
  private static class RemoveIsNotDistinctFromRexShuttle extends RexShuttle {
    final RexBuilder rexBuilder;

    RemoveIsNotDistinctFromRexShuttle(
        RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override public RexNode visitCall(RexCall call) {
      RexNode newCall = super.visitCall(call);

      if (call.getOperator()
          == SqlStdOperatorTable.IS_NOT_DISTINCT_FROM) {
        RexCall tmpCall = (RexCall) newCall;

        RexNode operand0 = tmpCall.operands.get(0);
        RexNode operand1 = tmpCall.operands.get(1);

        RexNode operand0Zero = rexBuilder.makeZeroRexNode(operand0.getType());
        RexNode operand1Zero = rexBuilder.makeZeroRexNode(operand1.getType());

        RexNode coalesceCondition =
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeCall(SqlStdOperatorTable.COALESCE,
                operand0, operand0Zero),
            rexBuilder.makeCall(SqlStdOperatorTable.COALESCE,
                operand1, operand1Zero));

        RexNode isNullCondition =
            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, operand0),
            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, operand1));

        newCall =
            rexBuilder.makeCall(SqlStdOperatorTable.AND,
                coalesceCondition, isNullCondition);
      }
      return newCall;
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableJoinConditionExpandIsNotDistinctFromRule.Config.of()
        .withOperandSupplier(b -> b.operand(Join.class).anyInputs());

    @Override default JoinConditionExpandIsNotDistinctFromRule toRule() {
      return new JoinConditionExpandIsNotDistinctFromRule(this);
    }
  }
}
