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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that translates a {@link Minus}
 * to a series of {@link org.apache.calcite.rel.core.Join} that type is
 * {@link JoinRelType#ANTI}. This rule supports 2-way Minus conversion,
 * as this rule can be repeatedly applied during query optimization to
 * refine the plan.
 *
 * <h2>Example</h2>
 *
 * <p>Original plan:
 * <pre>{@code
 * LogicalMinus(all=[false])
 *   LogicalProject(ENAME=[$1])
 *     LogicalFilter(condition=[=($7, 10)])
 *       LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *   LogicalProject(ENAME=[$1])
 *     LogicalFilter(condition=[=($7, 20)])
 *       LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 * }</pre>
 *
 * <p>Plan after conversion:
 * <pre>{@code
 * LogicalAggregate(group=[{0}])
 *   LogicalJoin(condition=[IS NOT DISTINCT FROM($0, $1)], joinType=[anti])
 *     LogicalProject(ENAME=[$1])
 *       LogicalFilter(condition=[=($7, 10)])
 *         LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *     LogicalProject(ENAME=[$1])
 *       LogicalFilter(condition=[=($7, 20)])
 *         LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 * }</pre>
 */
@Value.Enclosing
public class MinusToAntiJoinRule
    extends RelRule<MinusToAntiJoinRule.Config>
    implements TransformationRule {

  /** Creates an MinusToAntiJoinRule. */
  protected MinusToAntiJoinRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Minus minus = call.rel(0);
    if (minus.all) {
      return; // nothing we can do
    }

    List<RelNode> inputs = minus.getInputs();
    if (inputs.size() != 2) {
      return;
    }

    final RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    RelNode left = inputs.get(0);
    RelNode right = inputs.get(1);

    List<RexNode> conditions = new ArrayList<>();
    int fieldCount = left.getRowType().getFieldCount();

    for (int i = 0; i < fieldCount; i++) {
      RelDataType leftFieldType = left.getRowType().getFieldList().get(i).getType();
      RelDataType rightFieldType = right.getRowType().getFieldList().get(i).getType();

      // No further optimization will be performed based on field nullability,
      // as this can be uniformly optimized by other rules.
      conditions.add(
          relBuilder.isNotDistinctFrom(
              rexBuilder.makeInputRef(leftFieldType, i),
              rexBuilder.makeInputRef(rightFieldType, i + fieldCount)));
    }
    RexNode condition = RexUtil.composeConjunction(rexBuilder, conditions);

    relBuilder.push(left)
        .push(right)
        .join(JoinRelType.ANTI, condition)
        .distinct();

    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableMinusToAntiJoinRule.Config.of()
        .withOperandFor(LogicalMinus.class);

    @Override default MinusToAntiJoinRule toRule() {
      return new MinusToAntiJoinRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Minus> minusClass) {
      return withOperandSupplier(b -> b.operand(minusClass).anyInputs())
          .as(Config.class);
    }
  }
}
