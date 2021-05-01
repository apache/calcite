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
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Rule to convert an
 * {@link org.apache.calcite.rel.core.Join inner join} to a
 * {@link org.apache.calcite.rel.core.Filter filter} on top of a
 * {@link org.apache.calcite.rel.core.Join cartesian inner join}.
 *
 * <p>One benefit of this transformation is that after it, the join condition
 * can be combined with conditions and expressions above the join. It also makes
 * the <code>FennelCartesianJoinRule</code> applicable.
 *
 * <p>The constructor is parameterized to allow any sub-class of
 * {@link org.apache.calcite.rel.core.Join}.</p>
 */
public abstract class AbstractJoinExtractFilterRule
    extends RelRule<AbstractJoinExtractFilterRule.Config>
    implements TransformationRule {
  /** Creates an AbstractJoinExtractFilterRule. */
  protected AbstractJoinExtractFilterRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  protected AbstractJoinExtractFilterRule(RelOptRuleOperand operand,
      RelBuilderFactory relBuilderFactory, String description) {
    this(Config.EMPTY
        .withOperandSupplier(b -> b.exactly(operand))
        .withRelBuilderFactory(relBuilderFactory)
        .withDescription(description)
        .as(Config.class));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);

    if (join.getJoinType() != JoinRelType.INNER) {
      return;
    }

    if (join.getCondition().isAlwaysTrue()) {
      return;
    }

    if (!join.getSystemFieldList().isEmpty()) {
      // FIXME Enable this rule for joins with system fields
      return;
    }

    final RelBuilder builder = call.builder();

    // NOTE jvs 14-Mar-2006:  See JoinCommuteRule for why we
    // preserve attribute semiJoinDone here.

    final RelNode cartesianJoin =
        join.copy(
            join.getTraitSet(),
            builder.literal(true),
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());

    builder.push(cartesianJoin)
        .filter(join.getCondition());

    call.transformTo(builder.build());
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
  }
}
