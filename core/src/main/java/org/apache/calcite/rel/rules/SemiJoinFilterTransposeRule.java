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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/**
 * Planner rule that pushes {@code SemiJoin}s down in a tree past
 * a {@link org.apache.calcite.rel.core.Filter}.
 *
 * <p>The intention is to trigger other rules that will convert
 * {@code SemiJoin}s.
 *
 * <p>SemiJoin(LogicalFilter(X), Y) &rarr; LogicalFilter(SemiJoin(X, Y))
 *
 * @see SemiJoinProjectTransposeRule
 * @see CoreRules#SEMI_JOIN_FILTER_TRANSPOSE
 */
public class SemiJoinFilterTransposeRule
    extends RelRule<SemiJoinFilterTransposeRule.Config>
    implements TransformationRule {
  /** @deprecated Use {@link CoreRules#SEMI_JOIN_FILTER_TRANSPOSE}. */
  @Deprecated // to be removed before 1.25
  public static final SemiJoinFilterTransposeRule INSTANCE =
      Config.DEFAULT.toRule();

  //~ Constructors -----------------------------------------------------------

  /** Creates a SemiJoinFilterTransposeRule. */
  protected SemiJoinFilterTransposeRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public SemiJoinFilterTransposeRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    final Join semiJoin = call.rel(0);
    final Filter filter = call.rel(1);

    final RelNode newSemiJoin =
        LogicalJoin.create(filter.getInput(),
            semiJoin.getRight(),
            // No need to copy the hints, the framework would try to do that.
            ImmutableList.of(),
            semiJoin.getCondition(),
            ImmutableSet.of(),
            JoinRelType.SEMI);

    final RelFactories.FilterFactory factory =
        RelFactories.DEFAULT_FILTER_FACTORY;
    RelNode newFilter =
        factory.createFilter(newSemiJoin, filter.getCondition(),
            ImmutableSet.of());

    call.transformTo(newFilter);
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY.as(Config.class)
        .withOperandFor(LogicalJoin.class, LogicalFilter.class);

    @Override default SemiJoinFilterTransposeRule toRule() {
      return new SemiJoinFilterTransposeRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass,
        Class<? extends Filter> filterClass) {
      return withOperandSupplier(b0 ->
          b0.operand(joinClass).predicate(Join::isSemiJoin).inputs(b1 ->
              b1.operand(filterClass).anyInputs()))
          .as(Config.class);
    }
  }
}
