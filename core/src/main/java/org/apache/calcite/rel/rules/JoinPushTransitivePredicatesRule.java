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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import org.immutables.value.Value;

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
 *
 * @see CoreRules#JOIN_PUSH_TRANSITIVE_PREDICATES
 */
@Value.Enclosing
public class JoinPushTransitivePredicatesRule
    extends RelRule<JoinPushTransitivePredicatesRule.Config>
    implements TransformationRule {

  /** Creates a JoinPushTransitivePredicatesRule. */
  protected JoinPushTransitivePredicatesRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public JoinPushTransitivePredicatesRule(Class<? extends Join> joinClass,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(joinClass));
  }

  @Deprecated // to be removed before 2.0
  public JoinPushTransitivePredicatesRule(Class<? extends Join> joinClass,
      RelFactories.FilterFactory filterFactory) {
    this(Config.DEFAULT
        .withRelBuilderFactory(RelBuilder.proto(Contexts.of(filterFactory)))
        .as(Config.class)
        .withOperandFor(joinClass));
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    final RelMetadataQuery mq = call.getMetadataQuery();
    RelOptPredicateList preds = mq.getPulledUpPredicates(join);

    if (preds.leftInferredPredicates.isEmpty()
        && preds.rightInferredPredicates.isEmpty()) {
      return;
    }

    final RelBuilder relBuilder = call.builder();

    RelNode left = join.getLeft();
    if (!preds.leftInferredPredicates.isEmpty()) {
      RelNode curr = left;
      left = relBuilder.push(left)
          .filter(preds.leftInferredPredicates).build();
      call.getPlanner().onCopy(curr, left);
    }

    RelNode right = join.getRight();
    if (!preds.rightInferredPredicates.isEmpty()) {
      RelNode curr = right;
      right = relBuilder.push(right)
          .filter(preds.rightInferredPredicates).build();
      call.getPlanner().onCopy(curr, right);
    }

    RelNode newRel =
        join.copy(join.getTraitSet(), join.getCondition(), left, right,
            join.getJoinType(), join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newRel);

    call.transformTo(newRel);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableJoinPushTransitivePredicatesRule.Config.of()
        .withOperandFor(Join.class);

    @Override default JoinPushTransitivePredicatesRule toRule() {
      return new JoinPushTransitivePredicatesRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass) {
      return withOperandSupplier(b -> b.operand(joinClass).anyInputs())
          .as(Config.class);
    }
  }
}
