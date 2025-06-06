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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Litmus;

import org.immutables.value.Value;

import java.util.Set;

/**
 * Rule to convert an
 * {@link org.apache.calcite.rel.core.Join inner join} to a
 * {@link org.apache.calcite.rel.core.Filter filter} on top of a
 * {@link org.apache.calcite.rel.core.Join cartesian inner join}.
 * This rule is applicable when Join Condition contains Correlation Subquery in EXIST Clause.
 */
@Value.Enclosing
public class JoinCorrelateSubqueryTransposeRule
    extends RelRule<JoinCorrelateSubqueryTransposeRule.Config>
    implements TransformationRule {

  private RuleMatchExtension extension;

  /**
   * Creates an JoinCorrelateSubqueryTransposeRule.
   */
  protected JoinCorrelateSubqueryTransposeRule(JoinCorrelateSubqueryTransposeRule.Config config) {
    super(config);
  }

  public void setExtension(RuleMatchExtension extension) {
    this.extension = extension;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    extension.execute(call);
  }

  /**
   * Rule configuration.
   */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    JoinCorrelateSubqueryTransposeRule.Config DEFAULT =
        ImmutableJoinCorrelateSubqueryTransposeRule.Config.of()
            .withOperandSupplier(b -> b.operand(LogicalJoin.class)
                .predicate(join -> !join.getVariablesSet().isEmpty()
                    && hasExistClause(join.getCondition(), join.getVariablesSet()))
                .anyInputs()).as(JoinCorrelateSubqueryTransposeRule.Config.class);

    @Override default JoinCorrelateSubqueryTransposeRule toRule() {
      return new JoinCorrelateSubqueryTransposeRule(this);
    }

    static boolean hasExistClause(RexNode conditionNode, Set<CorrelationId> correlationIdSet) {
      RexSubQuery rexSubQuery = RexUtil.SubQueryFinder.find(conditionNode);
      return rexSubQuery != null && rexSubQuery.isA(SqlKind.EXISTS)
          && correlationIdSet.stream().anyMatch(correlationId ->
          !RelOptUtil.notContainsCorrelation(rexSubQuery.rel, correlationId, Litmus.IGNORE));
    }
  }
}
