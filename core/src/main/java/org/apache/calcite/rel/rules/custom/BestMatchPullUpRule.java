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
package org.apache.calcite.rel.rules.custom;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.BestMatch;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

/**
 * BestMatchPullUpRule pulls up a best-match operator. Basically, the
 * conversion is from `B(R) * S` to `B(R * S)`.
 */
public class BestMatchPullUpRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final BestMatchPullUpRule INSTANCE = new BestMatchPullUpRule(
      operand(Join.class,
          operand(BestMatch.class, any()),
          operand(RelSubset.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public BestMatchPullUpRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public BestMatchPullUpRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    final RelBuilder builder = call.builder();

    // Only applies the rule on cartesian product.
    final Join join = call.rel(0);
    final RexNode condition = join.getCondition();
    if (!condition.equals(builder.literal(true))) {
      LOGGER.debug("The condition is not true");
      return;
    }

    // Constructs the new cartesian product.
    final BestMatch bestMatch = call.rel(1);
    final RelNode outerCartesianJoin =
        join.copy(
            join.getTraitSet(),
            builder.literal(true),  // Uses a literal condition which is always true.
            bestMatch.getInput(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());

    // Builds the new expression.
    RelNode reducedNode = builder.push(outerCartesianJoin).bestMatch().build();
    call.transformTo(reducedNode);
  }
}

// End BestMatchPullUpRule.java
