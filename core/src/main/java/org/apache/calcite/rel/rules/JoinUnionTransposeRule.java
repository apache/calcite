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

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * PushJoinThroughUnionRule implements the rule for pushing a
 * {@link JoinRel} past a non-distinct {@link UnionRel}.
 */
public class PushJoinThroughUnionRule extends RelOptRule {
  public static final PushJoinThroughUnionRule LEFT_UNION =
      new PushJoinThroughUnionRule(
          operand(JoinRelBase.class,
              operand(UnionRelBase.class, any()),
              operand(RelNode.class, any())),
          "union on left");

  public static final PushJoinThroughUnionRule RIGHT_UNION =
      new PushJoinThroughUnionRule(
          operand(JoinRelBase.class,
              operand(RelNode.class, any()),
              operand(UnionRelBase.class, any())),
          "union on right");

  private PushJoinThroughUnionRule(RelOptRuleOperand operand, String id) {
    super(operand, "PushJoinThroughUnionRule: " + id);
  }

  public void onMatch(RelOptRuleCall call) {
    final JoinRelBase join = call.rel(0);
    final UnionRelBase unionRel;
    RelNode otherInput;
    boolean unionOnLeft;
    if (call.rel(1) instanceof UnionRel) {
      unionRel = call.rel(1);
      otherInput = call.rel(2);
      unionOnLeft = true;
    } else {
      otherInput = call.rel(1);
      unionRel = call.rel(2);
      unionOnLeft = false;
    }
    if (!unionRel.all) {
      return;
    }
    if (!join.getVariablesStopped().isEmpty()) {
      return;
    }
    // The UNION ALL cannot be on the null generating side
    // of an outer join (otherwise we might generate incorrect
    // rows for the other side for join keys which lack a match
    // in one or both branches of the union)
    if (unionOnLeft) {
      if (join.getJoinType().generatesNullsOnLeft()) {
        return;
      }
    } else {
      if (join.getJoinType().generatesNullsOnRight()) {
        return;
      }
    }
    List<RelNode> newUnionInputs = new ArrayList<RelNode>();
    for (RelNode input : unionRel.getInputs()) {
      RelNode joinLeft;
      RelNode joinRight;
      if (unionOnLeft) {
        joinLeft = input;
        joinRight = otherInput;
      } else {
        joinLeft = otherInput;
        joinRight = input;
      }
      newUnionInputs.add(
          join.copy(
              join.getTraitSet(),
              join.getCondition(),
              joinLeft,
              joinRight,
              join.getJoinType(),
              join.isSemiJoinDone()));
    }
    final SetOpRel newUnionRel =
        unionRel.copy(unionRel.getTraitSet(), newUnionInputs, true);
    call.transformTo(newUnionRel);
  }
}

// End PushJoinThroughUnionRule.java
