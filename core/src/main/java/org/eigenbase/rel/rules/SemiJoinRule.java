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

import java.util.BitSet;
import java.util.List;

import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.JoinInfo;
import org.eigenbase.rel.JoinRelBase;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.Convention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.util.ImmutableIntList;
import org.eigenbase.util.IntList;

import net.hydromatic.optiq.util.BitSets;

import com.google.common.collect.Lists;

/**
 * Planner rule that creates a {@code SemiJoinRule} from a
 * {@link org.eigenbase.rel.JoinRelBase} on top of a
 * {@link org.eigenbase.rel.AggregateRel}.
 */
public class SemiJoinRule extends RelOptRule {
  public static final SemiJoinRule INSTANCE = new SemiJoinRule();

  private SemiJoinRule() {
    super(
        operand(ProjectRelBase.class,
            some(operand(JoinRelBase.class,
                some(operand(RelNode.class, any()),
                    operand(AggregateRelBase.class, any()))))));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ProjectRelBase project = call.rel(0);
    final JoinRelBase join = call.rel(1);
    final RelNode left = call.rel(2);
    final AggregateRelBase aggregate = call.rel(3);
    final BitSet bits = RelOptUtil.InputFinder.bits(project.getProjects(),
        null);
    final BitSet rightBits = BitSets.range(left.getRowType().getFieldCount(),
        join.getRowType().getFieldCount());
    if (bits.intersects(rightBits)) {
      return;
    }
    final JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.rightSet().equals(BitSets.range(aggregate.getGroupCount()))) {
      // Rule requires that aggregate key to be the same as the join key.
      // By the way, neither a super-set nor a sub-set would work.
      return;
    }
    final List<Integer> newRightKeys = Lists.newArrayList();
    final IntList aggregateKeys = BitSets.toList(aggregate.getGroupSet());
    for (int key : joinInfo.rightKeys) {
      newRightKeys.add(aggregateKeys.get(key));
    }
    final SemiJoinRel semiJoin =
        new SemiJoinRel(join.getCluster(),
            join.getCluster().traitSetOf(Convention.NONE),
            left, aggregate.getChild(),
            join.getCondition(), joinInfo.leftKeys,
            ImmutableIntList.copyOf(newRightKeys));
    final ProjectRelBase newProject =
        project.copy(project.getTraitSet(), semiJoin, project.getProjects(),
            project.getRowType());
    call.transformTo(RemoveTrivialProjectRule.strip(newProject));
  }
}

// End SemiJoinRule.java
