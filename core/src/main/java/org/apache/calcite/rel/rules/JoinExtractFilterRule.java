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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * Rule to convert an {@link JoinRel inner join} to a {@link FilterRel filter}
 * on top of a {@link JoinRel cartesian inner join}.
 *
 * <p>One benefit of this transformation is that after it, the join condition
 * can be combined with conditions and expressions above the join. It also makes
 * the <code>FennelCartesianJoinRule</code> applicable.
 *
 * <p>The constructor is parameterized to allow any sub-class of
 * {@link JoinRelBase}, not just {@link JoinRel}.</p>
 */
public final class ExtractJoinFilterRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------

  /** The singleton. */
  public static final ExtractJoinFilterRule INSTANCE =
      new ExtractJoinFilterRule(JoinRel.class);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an ExtractJoinFilterRule.
   */
  public ExtractJoinFilterRule(Class<? extends JoinRelBase> clazz) {
    super(operand(clazz, any()));
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final JoinRelBase join = call.rel(0);

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

    // NOTE jvs 14-Mar-2006:  See SwapJoinRule for why we
    // preserve attribute semiJoinDone here.

    RelNode cartesianJoinRel =
        join.copy(
            join.getTraitSet(),
            join.getCluster().getRexBuilder().makeLiteral(true),
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());

    RelNode filterRel =
        RelOptUtil.createFilter(
            cartesianJoinRel,
            join.getCondition());

    call.transformTo(filterRel);
  }
}

// End ExtractJoinFilterRule.java
