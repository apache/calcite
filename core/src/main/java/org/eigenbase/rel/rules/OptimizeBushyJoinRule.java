/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.Util;

/**
 * Planner rule that finds an approximately optimal ordering for join operators
 * using a heuristic algorithm.
 *
 * <p>It is triggered by the pattern {@link ProjectRel} ({@link MultiJoinRel}).
 *
 * <p>It is similar to {@link org.eigenbase.rel.rules.LoptOptimizeJoinRule}.
 * {@code LoptOptimizeJoinRule} is only capable of producing left-deep joins;
 * this rule is capable of producing bushy joins.
 */
public class OptimizeBushyJoinRule extends RelOptRule {
  public static final OptimizeBushyJoinRule INSTANCE =
      new OptimizeBushyJoinRule(RelFactories.DEFAULT_JOIN_FACTORY);

  private final RelFactories.JoinFactory joinFactory;

  /** Creates an OptimizeBushyJoinRule. */
  public OptimizeBushyJoinRule(RelFactories.JoinFactory joinFactory) {
    super(operand(MultiJoinRel.class, any()));
    this.joinFactory = joinFactory;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final MultiJoinRel multiJoinRel = call.rel(0);
    final LoptMultiJoin multiJoin = new LoptMultiJoin(multiJoinRel);

    final RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
    Util.discard(multiJoin);
  }
}

// End OptimizeBushyJoinRule.java
