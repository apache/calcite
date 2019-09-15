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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Nullify;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

/**
 * NullifyPullUpRule pulls up a nullification operator. Basically, the
 * conversion is from `Nullify(R) * S` to `Nullify(R * S)`.
 */
public class NullifyPullUpRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** Instance of the current rule. */
  public static final NullifyPullUpRule INSTANCE = new NullifyPullUpRule(
      operand(Join.class,
          operand(Nullify.class, any()),
          operand(RelSubset.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public NullifyPullUpRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public NullifyPullUpRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    final RelBuilder builder = call.builder();

    // Only applies the rule on cartesian product.
    final Join join = call.rel(0);
    final RexNode condition = join.getCondition();
    if (!condition.equals(builder.literal(true))) {
      throw new AssertionError(condition);
    }

    // Constructs the new cartesian product.
    final Nullify oldNullify = call.rel(1);
    final RelNode cartesianJoin =
        join.copy(
            join.getTraitSet(),
            builder.literal(true),  // Uses a literal condition which is always true.
            oldNullify.getInput(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());

    // Builds the new expression.
    final RexNode oldPredicate = oldNullify.getPredicate();
    final List<RexNode> oldAttributes = oldNullify.getAttributes();
    RelNode reducedNode = builder.push(cartesianJoin).nullify(oldPredicate, oldAttributes).build();
    call.transformTo(reducedNode);
  }
}

// End NullifyPullUpRule.java
