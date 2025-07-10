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
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

/**
 * Planner rule that matches a {@link Join} whose join type is RIGHT,
 * and converts it to a LEFT join by swapping the inputs and join type.
 *
 * <p>This transformation is useful because many optimization rules and
 * implementations are written to handle LEFT joins, and RIGHT joins can
 * often be handled by converting them to LEFT joins with swapped inputs.
 *
 * <p>For example, the following SQL:
 *
 * <pre>{@code
 * SELECT *
 * FROM Employees e
 * RIGHT JOIN Departments d ON e.deptno = d.deptno
 * }</pre>
 *
 * <p>is transformed into:
 *
 * <pre>{@code
 * SELECT *
 * FROM Departments d
 * LEFT JOIN Employees e ON e.deptno = d.deptno
 * }</pre>
 *
 * <p>This rule uses {@link JoinCommuteRule#swap} to perform the input swap and
 * join type conversion. The transformation preserves the semantics of the original
 * RIGHT join.
 *
 * <p>Limitations:
 * <ul>
 *   <li>Only applies to joins of type {@link JoinRelType#RIGHT}.</li>
 *   <li>Does not match FULL, LEFT, INNER, SEMI, or ANTI joins.</li>
 *   <li>Column order in the output may be affected by the swap; a projection is used to restore the original order if needed.</li>
 * </ul>
 *
 * <p>See also:
 * <ul>
 *   <li>{@link JoinCommuteRule} - for general join input permutation</li>
 *   <li>{@link CoreRules#RIGHT_TO_LEFT_JOIN_RULE} - the public rule instance</li>
 * </ul>
 *
 * @see JoinCommuteRule
 * @see JoinRelType#RIGHT
 * @see CoreRules#RIGHT_TO_LEFT_JOIN_RULE
 */
@Value.Enclosing
public class RightToLeftJoinRule
    extends RelRule<RightToLeftJoinRule.Config>
    implements TransformationRule {

  /** Creates a RightToLeftJoinRule. */
  protected RightToLeftJoinRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    RelBuilder relBuilder = call.builder();
    RelNode swapped = JoinCommuteRule.swap(join, true, relBuilder);
    if (swapped != null) {
      call.transformTo(swapped);
    }
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableRightToLeftJoinRule.Config.of()
        .withOperandFor(Join.class);

    @Override default RightToLeftJoinRule toRule() {
      return new RightToLeftJoinRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass) {
      return withOperandSupplier(b -> b.operand(joinClass)
          .predicate(join -> join.getJoinType() == JoinRelType.RIGHT)
          .anyInputs())
          .as(Config.class);
    }
  }
}
