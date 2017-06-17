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

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * Planner rule that infers predicates from on a
 * {@link org.apache.calcite.rel.core.Join} and creates
 * {@link org.apache.calcite.rel.core.Filter}s if those predicates can be pushed
 * to its inputs.
 *
 * <p>Uses {@link org.apache.calcite.rel.metadata.RelMdPredicates} to infer
 * the predicates,
 * returns them in a {@link org.apache.calcite.plan.RelOptPredicateList}
 * and applies them appropriately.
 */
public class JoinPushTransitivePredicatesRule extends RelOptRule {
  /** The singleton. */
  public static final JoinPushTransitivePredicatesRule INSTANCE =
      new JoinPushTransitivePredicatesRule(Join.class,
          RelFactories.LOGICAL_BUILDER);

  /** Creates a JoinPushTransitivePredicatesRule. */
  public JoinPushTransitivePredicatesRule(Class<? extends Join> clazz,
      RelBuilderFactory relBuilderFactory) {
    super(operand(clazz, any()), relBuilderFactory, null);
  }

  @Deprecated // to be removed before 2.0
  public JoinPushTransitivePredicatesRule(Class<? extends Join> clazz,
      RelFactories.FilterFactory filterFactory) {
    this(clazz, RelBuilder.proto(Contexts.of(filterFactory)));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    RelOptPredicateList preds = mq.getPulledUpPredicates(join);

    if (preds.leftInferredPredicates.isEmpty()
        && preds.rightInferredPredicates.isEmpty()) {
      return;
    }

    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();

    RelNode lChild = join.getLeft();
    if (preds.leftInferredPredicates.size() > 0) {
      RelNode curr = lChild;
      lChild = relBuilder.push(lChild)
          .filter(preds.leftInferredPredicates).build();
      call.getPlanner().onCopy(curr, lChild);
    }

    RelNode rChild = join.getRight();
    if (preds.rightInferredPredicates.size() > 0) {
      RelNode curr = rChild;
      rChild = relBuilder.push(rChild)
          .filter(preds.rightInferredPredicates).build();
      call.getPlanner().onCopy(curr, rChild);
    }

    RelNode newRel = join.copy(join.getTraitSet(), join.getCondition(),
        lChild, rChild, join.getJoinType(), join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newRel);

    call.transformTo(newRel);
  }
}

// End JoinPushTransitivePredicatesRule.java
