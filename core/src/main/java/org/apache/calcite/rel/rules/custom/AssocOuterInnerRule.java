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
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.trace.CalciteTrace;

import org.slf4j.Logger;

/**
 * AssocOuterInnerRule applies limited associativity on outer join and inner join.
 *
 * Rule 21.
 */
public class AssocOuterInnerRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------
  private static final Logger LOGGER = CalciteTrace.getPlannerTracer();

  /** Instance of the current rule. */
  public static final AssocOuterInnerRule INSTANCE = new AssocOuterInnerRule(
      operand(Join.class,
          operand(Join.class, any()),
          operand(RelSubset.class, any())), null);

  //~ Constructors -----------------------------------------------------------

  public AssocOuterInnerRule(RelOptRuleOperand operand,
      String description, RelBuilderFactory relBuilderFactory) {
    super(operand, relBuilderFactory, description);
  }

  public AssocOuterInnerRule(RelOptRuleOperand operand, String description) {
    this(operand, description, RelFactories.LOGICAL_BUILDER);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(final RelOptRuleCall call) {
    // Gets the two original join operators.
    Join topInnerJoin = call.rel(0);
    Join bottomLeftJoin = call.rel(1);

    // Makes sure the join types match the rule.
    if (topInnerJoin.getJoinType() != JoinRelType.INNER) {
      LOGGER.debug("The top join is not an inner join.");
      return;
    } else if (bottomLeftJoin.getJoinType() != JoinRelType.LEFT) {
      LOGGER.debug("The bottom join is not a left outer join.");
      return;
    }

    // The new operators.
    final Join newBottomInnerJoin = topInnerJoin.copy(
        topInnerJoin.getTraitSet(),
        topInnerJoin.getCondition(),
        bottomLeftJoin.getRight(),
        topInnerJoin.getRight(),
        JoinRelType.INNER,
        topInnerJoin.isSemiJoinDone());
    final Join newTopInnerJoin = bottomLeftJoin.copy(
        bottomLeftJoin.getTraitSet(),
        bottomLeftJoin.getCondition(),
        bottomLeftJoin.getLeft(),
        newBottomInnerJoin,
        JoinRelType.INNER,
        bottomLeftJoin.isSemiJoinDone());

    // Builds the transformed relational tree.
    final RelNode transformedNode = call.builder().push(newTopInnerJoin).build();
    call.transformTo(transformedNode);
  }
}

// End OuterJoinAssociateRule.java
