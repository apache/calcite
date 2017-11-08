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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that pushes
 * {@link org.apache.calcite.rel.core.SemiJoin}s down in a tree past
 * a {@link org.apache.calcite.rel.core.Filter}.
 *
 * <p>The intention is to trigger other rules that will convert
 * {@code SemiJoin}s.
 *
 * <p>SemiJoin(LogicalFilter(X), Y) &rarr; LogicalFilter(SemiJoin(X, Y))
 *
 * @see SemiJoinProjectTransposeRule
 */
public class SemiJoinFilterTransposeRule extends RelOptRule {
  public static final SemiJoinFilterTransposeRule INSTANCE =
      new SemiJoinFilterTransposeRule(RelFactories.LOGICAL_BUILDER);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SemiJoinFilterTransposeRule.
   */
  public SemiJoinFilterTransposeRule(RelBuilderFactory relBuilderFactory) {
    super(
        operand(SemiJoin.class,
            some(operand(LogicalFilter.class, any()))),
        relBuilderFactory, null);
  }

  //~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    SemiJoin semiJoin = call.rel(0);
    LogicalFilter filter = call.rel(1);

    RelNode newSemiJoin =
        SemiJoin.create(filter.getInput(),
            semiJoin.getRight(),
            semiJoin.getCondition(),
            semiJoin.getLeftKeys(),
            semiJoin.getRightKeys());

    final RelFactories.FilterFactory factory =
        RelFactories.DEFAULT_FILTER_FACTORY;
    RelNode newFilter =
        factory.createFilter(newSemiJoin, filter.getCondition());

    call.transformTo(newFilter);
  }
}

// End SemiJoinFilterTransposeRule.java
