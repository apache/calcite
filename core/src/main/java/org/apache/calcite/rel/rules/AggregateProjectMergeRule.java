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

import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.ProjectRelBase;
import org.eigenbase.rel.RelFactories;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexNode;

import net.hydromatic.optiq.util.BitSets;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Planner rule that recognizes a {@link org.eigenbase.rel.AggregateRelBase}
 * on top of a {@link org.eigenbase.rel.ProjectRelBase} and if possible
 * aggregate through the project or removes the project.
 *
 * <p>This is only possible when the grouping expressions and arguments to
 * the aggregate functions are field references (i.e. not expressions).
 *
 * <p>In some cases, this rule has the effect of trimming: the aggregate will
 * use fewer columns than the project did.
 */
public class AggregateProjectMergeRule extends RelOptRule {
  public static final AggregateProjectMergeRule INSTANCE =
      new AggregateProjectMergeRule();

  /** Private constructor. */
  private AggregateProjectMergeRule() {
    super(
        operand(AggregateRelBase.class,
            operand(ProjectRelBase.class, any())));
  }

  public void onMatch(RelOptRuleCall call) {
    final AggregateRelBase aggregate = call.rel(0);
    final ProjectRelBase project = call.rel(1);
    RelNode x = apply(aggregate, project);
    if (x != null) {
      call.transformTo(x);
    }
  }

  public static RelNode apply(AggregateRelBase aggregate,
      ProjectRelBase project) {
    final List<Integer> newKeys = Lists.newArrayList();
    for (int key : BitSets.toIter(aggregate.getGroupSet())) {
      final RexNode rex = project.getProjects().get(key);
      if (rex instanceof RexInputRef) {
        newKeys.add(((RexInputRef) rex).getIndex());
      } else {
        // Cannot handle "GROUP BY expression"
        return null;
      }
    }

    final ImmutableList.Builder<AggregateCall> aggCalls =
        ImmutableList.builder();
    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      final ImmutableList.Builder<Integer> newArgs = ImmutableList.builder();
      for (int arg : aggregateCall.getArgList()) {
        final RexNode rex = project.getProjects().get(arg);
        if (rex instanceof RexInputRef) {
          newArgs.add(((RexInputRef) rex).getIndex());
        } else {
          // Cannot handle "AGG(expression)"
          return null;
        }
      }
      aggCalls.add(aggregateCall.copy(newArgs.build()));
    }

    final BitSet newGroupSet = BitSets.of(newKeys);
    final AggregateRelBase newAggregate =
        aggregate.copy(aggregate.getTraitSet(), project.getChild(), newGroupSet,
            aggCalls.build());

    // Add a project if the group set is not in the same order or
    // contains duplicates.
    RelNode rel = newAggregate;
    if (!BitSets.toList(newGroupSet).equals(newKeys)) {
      final List<Integer> posList = Lists.newArrayList();
      for (int newKey : newKeys) {
        posList.add(BitSets.toList(newGroupSet).indexOf(newKey));
      }
      for (int i = newAggregate.getGroupSet().cardinality();
           i < newAggregate.getRowType().getFieldCount(); i++) {
        posList.add(i);
      }
      rel = RelOptUtil.createProject(RelFactories.DEFAULT_PROJECT_FACTORY,
          rel, posList);
    }

    return rel;
  }
}

// End AggregateProjectMergeRule.java
