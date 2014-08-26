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
package org.eigenbase.rel.rules;

import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.RelFactories;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptPredicateList;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexUtil;

/**
 * A Rule to apply inferred predicates from
 * {@link org.eigenbase.rel.metadata.RelMdPredicates}.
 *
 * <p>Predicates returned in {@link org.eigenbase.relopt.RelOptPredicateList}
 * are applied appropriately.
 */
public class TransitivePredicatesOnJoinRule extends RelOptRule {
  private final RelFactories.FilterFactory filterFactory;

  /** The singleton. */
  public static final TransitivePredicatesOnJoinRule INSTANCE =
      new TransitivePredicatesOnJoinRule(JoinRelBase.class,
          RelFactories.DEFAULT_FILTER_FACTORY);

  public TransitivePredicatesOnJoinRule(Class<? extends JoinRelBase> clazz,
      RelFactories.FilterFactory filterFactory) {
    super(operand(clazz, any()));
    this.filterFactory = filterFactory;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    JoinRelBase join = call.rel(0);
    RelOptPredicateList preds = RelMetadataQuery.getPulledUpPredicates(join);

    if (preds.leftInferredPredicates.isEmpty()
        && preds.rightInferredPredicates.isEmpty()) {
      return;
    }

    RexBuilder rB = join.getCluster().getRexBuilder();
    RelNode lChild = join.getLeft();
    RelNode rChild = join.getRight();

    if (preds.leftInferredPredicates.size() > 0) {
      RelNode curr = lChild;
      lChild = filterFactory.createFilter(lChild,
          RexUtil.composeConjunction(rB, preds.leftInferredPredicates, false));
      call.getPlanner().onCopy(curr, lChild);
    }

    if (preds.rightInferredPredicates.size() > 0) {
      RelNode curr = rChild;
      rChild = filterFactory.createFilter(rChild,
          RexUtil.composeConjunction(rB, preds.rightInferredPredicates, false));
      call.getPlanner().onCopy(curr, rChild);
    }

    RelNode newRel = join.copy(join.getTraitSet(), join.getCondition(),
        lChild, rChild, join.getJoinType(), join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newRel);

    call.transformTo(newRel);
  }
}

// End TransitivePredicatesOnJoinRule.java
