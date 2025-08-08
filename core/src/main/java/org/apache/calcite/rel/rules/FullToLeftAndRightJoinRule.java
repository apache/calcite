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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import com.google.common.collect.ImmutableList;

import org.immutables.value.Value;

/**
 * Planner rule that matches a {@link Join}
 * that join type is FULL, and convert it to
 * a LEFT JOIN and RIGHT JOIN combination
 * with a UNION ALL above them.
 *
 * <p>The SQL example is as follows:
 *
 * <pre>{@code
 * SELECT *
 * FROM Employees e
 * FULL JOIN Departments d ON e.id = d.id
 * }</pre>
 *
 * <p>rewritten into
 *
 * <pre>{@code
 * SELECT *
 * FROM Employees e
 * LEFT JOIN Departments d ON e.id = d.id
 * UNION ALL
 * SELECT *
 * FROM Employees e
 * RIGHT JOIN Departments d ON e.id = d.id
 * WHERE (e.id = d.id) IS NOT TRUE;
 * }</pre>
 */
@Value.Enclosing
public class FullToLeftAndRightJoinRule
    extends RelRule<FullToLeftAndRightJoinRule.Config>
    implements TransformationRule {

  /** Creates an FullToLeftAndRightJoinRule. */
  protected FullToLeftAndRightJoinRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    RelBuilder relBuilder = call.builder();

    if (!RexUtil.isDeterministic(join.getCondition())) {
      return;
    }

    RexBuilder rexBuilder = relBuilder.getRexBuilder();
    RexNode newCondition =
        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_TRUE, join.getCondition());

    final RexShuttle shuttle = new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        return RexInputRef.of(inputRef.getIndex(), join.getRowType());
      }
    };
    newCondition = shuttle.apply(newCondition);

    RelNode newLeft = relBuilder.push(join.getLeft())
        .push(join.getRight())
        .join(JoinRelType.LEFT, join.getCondition())
        .build();
    RelNode newRight = relBuilder.push(join.getLeft())
        .push(join.getRight())
        .join(JoinRelType.RIGHT, join.getCondition())
        .filter(newCondition)
        .build();

    relBuilder.pushAll(ImmutableList.of(newLeft, newRight))
        .union(true);

    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableFullToLeftAndRightJoinRule.Config.of()
        .withOperandFor(Join.class);

    @Override default FullToLeftAndRightJoinRule toRule() {
      return new FullToLeftAndRightJoinRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass) {
      return withOperandSupplier(b -> b.operand(joinClass)
          .predicate(join -> join.getJoinType() == JoinRelType.FULL)
          .anyInputs())
          .as(Config.class);
    }
  }
}
