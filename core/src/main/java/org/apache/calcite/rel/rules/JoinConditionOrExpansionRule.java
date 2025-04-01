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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that matches a
 * {@link org.apache.calcite.rel.core.Join}
 * and expands OR clauses in join conditions.
 *
 * <p>For example, for the following SQL query:
 * <pre>
 * SELECT *
 * FROM t1
 * JOIN t2
 * ON (t1.id = t2.id OR t1.name = t2.name)
 * </pre>
 *
 * <p>To
 *
 * <pre>
 * SELECT *
 * FROM t1
 * JOIN t2
 * ON t1.id = t2.id
 * UNION ALL
 * SELECT *
 * FROM t1
 * JOIN t2
 * ON t1.name = t2.name
 * </pre>
 *
 * <p>this rule would expand the OR condition into
 * two separate join conditions, allowing the optimizer
 * to handle these conditions more effectively.
 */
@Value.Enclosing
public class JoinConditionOrExpansionRule
    extends RelRule<JoinConditionOrExpansionRule.Config>
    implements TransformationRule {

  /** Creates an JoinConditionExpansionOrRule. */
  protected JoinConditionOrExpansionRule(Config config) {
    super(config);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelBuilder relBuilder = call.builder();
    List<RexNode> orConds = RelOptUtil.disjunctions(join.getCondition());

    if (orConds.size() <= 1) {
      return;
    }

    List<RexNode> extraConds  = new ArrayList<>();
    for (int i = 0; i < orConds.size(); i++) {
      RexNode orCond = orConds.get(i);
      for (int j = 0; j < i; j++) {
        orCond = relBuilder.and(orCond, relBuilder.not(extraConds.get(j)));
      }
      extraConds.add(orCond);
      relBuilder.push(join.getLeft())
          .push(join.getRight())
          .join(join.getJoinType(), orCond);
    }

    relBuilder.union(true, orConds.size());
    call.transformTo(relBuilder.build());
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableJoinConditionOrExpansionRule.Config.of()
        .withOperandFor(Join.class);

    @Override default JoinConditionOrExpansionRule toRule() {
      return new JoinConditionOrExpansionRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass) {
      return withOperandSupplier(b -> b.operand(joinClass).anyInputs())
          .as(Config.class);
    }
  }
}
