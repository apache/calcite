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
 * {@link JoinRelType#ANTI}. This rule supports n-way Minus conversion,
 * as this rule can be repeatedly applied during query optimization to
 * refine the plan.
 *
 * <p>Example for 2-way
 *
 * <p>Original sql:
 * <pre>{@code
 * select ename from emp where deptno = 10
 * except
 * select ename from emp where deptno = 20
 * }</pre>
 *
 * <p>Original plan:
 * <pre>{@code
 * LogicalMinus(all=[false])
 *   LogicalProject(ENAME=[$1])
 *     LogicalFilter(condition=[=($7, 10)])
 *       LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *   LogicalProject(ENAME=[$1])
 *     LogicalFilter(condition=[=($7, 20)])
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
 *
 * <p>Example for n-way
 *
 * <p>Original sql:
 * <pre>{@code
 * select ename from emp where deptno = 10
 * except
 * select deptno from emp where ename in ('a', 'b')
 * except
 * select ename from empnullables
 * }</pre>
 *
 * <p>Original plan:
 * <pre>{@code
 * LogicalMinus(all=[false])
 *   LogicalProject(ENAME=[$1])
 *     LogicalFilter(condition=[=($7, 10)])
 *       LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *   LogicalProject(DEPTNO=[CAST($7):VARCHAR NOT NULL])
 *     LogicalFilter(condition=[OR(=($1, 'a'), =($1, 'b'))])
 *       LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *   LogicalProject(ENAME=[$1])
 *     LogicalTableScan(table=[[CATALOG, SALES, EMPNULLABLES]])
 * }</pre>
 *
 * <p>Plan after conversion:
 * <pre>{@code
 * LogicalProject(ENAME=[CAST($0):VARCHAR])
 *   LogicalAggregate(group=[{0}])
 *     LogicalJoin(condition=[<=>(CAST($0):VARCHAR, CAST($1):VARCHAR)], joinType=[anti])
 *       LogicalJoin(condition=[=(CAST($0):VARCHAR, $1)], joinType=[anti])
 *         LogicalProject(ENAME=[$1])
 *           LogicalFilter(condition=[=($7, 10)])
 *             LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *         LogicalProject(DEPTNO=[CAST($7):VARCHAR NOT NULL])
 *           LogicalFilter(condition=[OR(=($1, 'a'), =($1, 'b'))])
 *             LogicalTableScan(table=[[CATALOG, SALES, EMP]])
 *       LogicalProject(ENAME=[$1])
 *         LogicalTableScan(table=[[CATALOG, SALES, EMPNULLABLES]])
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
    if (inputs.size() < 2) {
      return;
    }

    final RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    final RelDataType leastRowType = minus.getRowType();
    RelNode current = inputs.get(0);
    relBuilder.push(current);

    for (int i = 1; i < inputs.size(); i++) {
      RelNode next = inputs.get(i);
      int fieldCount = current.getRowType().getFieldCount();

      List<RexNode> conditions = new ArrayList<>();
      for (int j = 0; j < fieldCount; j++) {
        RelDataType leftFieldType = current.getRowType().getFieldList().get(j).getType();
        RelDataType rightFieldType = next.getRowType().getFieldList().get(j).getType();
        RelDataType leastFieldType = leastRowType.getFieldList().get(j).getType();

        conditions.add(
            relBuilder.isNotDistinctFrom(
                rexBuilder.makeCast(leastFieldType,
                    rexBuilder.makeInputRef(leftFieldType, j)),
                rexBuilder.makeCast(leastFieldType,
                    rexBuilder.makeInputRef(rightFieldType, j + fieldCount))));
      }
      RexNode condition = RexUtil.composeConjunction(rexBuilder, conditions);

      relBuilder.push(next)
          .join(JoinRelType.ANTI, condition);

      current = relBuilder.peek();
    }

    relBuilder.distinct()
        .convert(leastRowType, true);
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
