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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;

import org.immutables.value.Value;

/**
 * Rule that transforms a Filter containing LIKE ANY|ALL into REGEX_CONTAINS function
 * and creating LOGICAL_OR aggregation and pushing this aggregation into a subquery Project.
 *
 * This rule is applicable when the Filter contains LIKE ANY|ALL operators.
 */
@Value.Enclosing
public class QuantifierOperatorTransformationRule
    extends RelRule<QuantifierOperatorTransformationRule.Config>
    implements TransformationRule {

  private RuleMatchExtension extension;

  /**
   * Creates an SerializeDistinctStructRule.
   */
  protected QuantifierOperatorTransformationRule(QuantifierOperatorTransformationRule.Config config) {
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
    QuantifierOperatorTransformationRule.Config DEFAULT =
        ImmutableQuantifierOperatorTransformationRule.Config.of()
            .withOperandSupplier(b -> b.operand(Filter.class)
                .predicate(filter -> hasLikeQuantifier(filter.getCondition()))
                .anyInputs()).as(Config.class);

    @Override default QuantifierOperatorTransformationRule toRule() {
      return new QuantifierOperatorTransformationRule(this);
    }

    static boolean hasLikeQuantifier(RexNode conditionNode) {
      if (conditionNode instanceof RexSubQuery
          && ((RexSubQuery) conditionNode).op instanceof SqlQuantifyOperator
          && ((SqlQuantifyOperator) ((RexSubQuery) conditionNode).op).comparisonKind == SqlKind.LIKE) {
        return true;
      } else if (conditionNode instanceof RexCall) {
        return ((RexCall) conditionNode).operands.stream()
          .anyMatch(Config::hasLikeQuantifier);
      }
      return false;
    }
  }
}
