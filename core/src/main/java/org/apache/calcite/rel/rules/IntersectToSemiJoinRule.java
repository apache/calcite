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
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that translates a {@link Intersect}
 * to a series of {@link org.apache.calcite.rel.core.Join} that type is
 * {@link JoinRelType#SEMI}. This rule supports n-way Intersect conversion,
 * as this rule can be repeatedly applied during query optimization to
 * refine the plan.
 *
 * <h2>Example</h2>
 *
 <p>Original sql:
 * <pre>{@code
 * select ename from emp where deptno = 10
 * intersect
 * select deptno from emp where ename in ('a', 'b')
 * intersect
 * select ename from empnullables
 * }</pre>
 *
 * <p>Original plan:
 * <pre>{@code
 * LogicalIntersect(all=[false])
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
 *     LogicalJoin(condition=[<=>(CAST($0):VARCHAR, CAST($1):VARCHAR)], joinType=[semi])
 *       LogicalJoin(condition=[=(CAST($0):VARCHAR, $1)], joinType=[semi])
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
public class IntersectToSemiJoinRule
    extends RelRule<IntersectToSemiJoinRule.Config>
    implements TransformationRule {

  /** Creates an IntersectToSemiJoinRule. */
  protected IntersectToSemiJoinRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Intersect intersect = call.rel(0);
    if (intersect.all) {
      return; // nothing we can do
    }

    final RelBuilder builder = call.builder();
    final RexBuilder rexBuilder = builder.getRexBuilder();

    List<RelNode> inputs = intersect.getInputs();
    if (inputs.size() < 2) {
      return;
    }

    final RelDataType leastRowType = intersect.getRowType();
    RelNode current = inputs.get(0);
    builder.push(current);

    for (int i = 1; i < inputs.size(); i++) {
      RelNode next = inputs.get(i);
      List<RexNode> conditions = new ArrayList<>();
      int fieldCount = current.getRowType().getFieldCount();

      for (int j = 0; j < fieldCount; j++) {
        RelDataType leftFieldType = current.getRowType().getFieldList().get(j).getType();
        RelDataType rightFieldType = next.getRowType().getFieldList().get(j).getType();
        RelDataType leastFieldType = leastRowType.getFieldList().get(j).getType();

        conditions.add(
            builder.isNotDistinctFrom(
                rexBuilder.makeCast(leastFieldType,
                    rexBuilder.makeInputRef(leftFieldType, j)),
                rexBuilder.makeCast(leastFieldType,
                    rexBuilder.makeInputRef(rightFieldType, j + fieldCount))));
      }
      RexNode condition = RexUtil.composeConjunction(rexBuilder, conditions);

      builder.push(next)
          .join(JoinRelType.SEMI, condition);
      current = builder.peek();
    }

    builder.distinct()
        .convert(leastRowType, true);
    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableIntersectToSemiJoinRule.Config.of()
        .withOperandFor(LogicalIntersect.class);

    @Override default IntersectToSemiJoinRule toRule() {
      return new IntersectToSemiJoinRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Intersect> intersectClass) {
      return withOperandSupplier(b -> b.operand(intersectClass).anyInputs())
          .as(Config.class);
    }
  }
}
