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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.tools.RelBuilderFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value;

/**
 * Rule to add a semi-join into a join. Transformation is as follows:
 *
 * <p>LogicalJoin(X, Y) &rarr; LogicalJoin(SemiJoin(X, Y), Y)
 *
 * <p>Can be configured to match any sub-class of
 * {@link org.apache.calcite.rel.core.Join}, not just
 * {@link org.apache.calcite.rel.logical.LogicalJoin}.
 *
 * @see CoreRules#JOIN_ADD_REDUNDANT_SEMI_JOIN
 */
@Value.Enclosing
public class JoinAddRedundantSemiJoinRule
    extends RelRule<JoinAddRedundantSemiJoinRule.Config>
    implements TransformationRule {

  /** Creates a JoinAddRedundantSemiJoinRule. */
  protected JoinAddRedundantSemiJoinRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  public JoinAddRedundantSemiJoinRule(Class<? extends Join> clazz,
      RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withOperandFor(clazz));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void onMatch(RelOptRuleCall call) {
    Join origJoinRel = call.rel(0);
    if (origJoinRel.isSemiJoinDone()) {
      return;
    }

    // can't process outer joins using semi-joins
    if (origJoinRel.getJoinType() != JoinRelType.INNER) {
      return;
    }

    // determine if we have a valid join condition
    final JoinInfo joinInfo = origJoinRel.analyzeCondition();
    if (joinInfo.leftKeys.isEmpty()) {
      return;
    }

    RelNode semiJoin =
        LogicalJoin.create(origJoinRel.getLeft(),
            origJoinRel.getRight(),
            ImmutableList.of(),
            origJoinRel.getCondition(),
            ImmutableSet.of(),
            JoinRelType.SEMI);

    RelNode newJoinRel =
        origJoinRel.copy(
            origJoinRel.getTraitSet(),
            origJoinRel.getCondition(),
            semiJoin,
            origJoinRel.getRight(),
            JoinRelType.INNER,
            true);

    call.transformTo(newJoinRel);
  }

  /** Rule configuration. */
  @Value.Immutable
  public interface Config extends RelRule.Config {
    Config DEFAULT = ImmutableJoinAddRedundantSemiJoinRule.Config.of()
        .withOperandFor(LogicalJoin.class);

    @Override default JoinAddRedundantSemiJoinRule toRule() {
      return new JoinAddRedundantSemiJoinRule(this);
    }

    /** Defines an operand tree for the given classes. */
    default Config withOperandFor(Class<? extends Join> joinClass) {
      return withOperandSupplier(b -> b.operand(joinClass).anyInputs())
          .as(Config.class);
    }
  }
}
