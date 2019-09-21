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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Planner rule that creates a {@code AntiJoin} from a
 * {@link Join} on top of a
 * {@link org.apache.calcite.rel.logical.LogicalAggregate}.
 */
public class AntiJoinRule extends RelOptRule {

  private static final Predicate<Join> IS_LEFT_JOIN =
      join -> join.getJoinType() == JoinRelType.LEFT;

  public static final AntiJoinRule INSTANCE = new AntiJoinRule();

  AntiJoinRule() {
    super(
        operand(Project.class,
            operand(Filter.class,
                operandJ(Join.class, null, IS_LEFT_JOIN,
                    some(operand(RelNode.class, any()),
                        operand(Aggregate.class, any()))))),
        RelFactories.LOGICAL_BUILDER, "AntiJoinRule");
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Project project = call.rel(0);
    Filter filter = call.rel(1);
    Join join = call.rel(2);
    RelNode left = call.rel(3);
    Aggregate aggregate = call.rel(4);

    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();

    final ImmutableBitSet bits =
        RelOptUtil.InputFinder.bits(project.getProjects(), null);
    final ImmutableBitSet rightBits =
        ImmutableBitSet.range(left.getRowType().getFieldCount(),
            join.getRowType().getFieldCount());
    if (aggregate.getGroupCount() == 0
        || aggregate.getAggCallList().size() == 0
        || bits.intersects(rightBits)) {
      return;
    }

    List<RexNode> isNullConditionsForAnti = new ArrayList<>();
    List<RexNode> residualConditions = new ArrayList<>();

    for (RexNode cond: RelOptUtil.conjunctions(filter.getCondition())) {
      if (cond instanceof RexCall) {
        final RexCall rexCall = (RexCall) cond;
        if (rexCall.op == SqlStdOperatorTable.IS_NULL) {
          final RexNode operand = rexCall.operands.get(0);
          if (operand instanceof RexInputRef) {
            final RexInputRef inputRef = (RexInputRef) operand;
            final int idxOnAggregate =
                inputRef.getIndex() - left.getRowType().getFieldCount();
            if (!aggregate.getRowType().getFieldList()
                .get(idxOnAggregate).getType().isNullable()) {
              isNullConditionsForAnti.add(cond);
              continue;
            }
          }
        }
      }
      residualConditions.add(cond);
    }

    if (isNullConditionsForAnti.isEmpty()) {
      return;
    }

    final JoinInfo joinInfo = join.analyzeCondition();
    if (!joinInfo.rightSet().equals(
        ImmutableBitSet.range(aggregate.getGroupCount()))) {
      // Rule requires that aggregate key to be the same as the join key.
      // By the way, neither a super-set nor a sub-set would work.
      return;
    }

    if (!joinInfo.isEqui()) {
      return;
    }

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(left);
    final List<Integer> newRightKeyBuilder = new ArrayList<>();
    final List<Integer> aggregateKeys = aggregate.getGroupSet().asList();
    for (int key : joinInfo.rightKeys) {
      newRightKeyBuilder.add(aggregateKeys.get(key));
    }
    final ImmutableIntList newRightKeys = ImmutableIntList.copyOf(newRightKeyBuilder);
    relBuilder.push(aggregate.getInput());

    final RexNode newCondition =
        RelOptUtil.createEquiJoinCondition(relBuilder.peek(2, 0),
            joinInfo.leftKeys, relBuilder.peek(2, 1), newRightKeys,
            rexBuilder);
    relBuilder.antiJoin(newCondition);
    if (!residualConditions.isEmpty()) {
      relBuilder.filter(RexUtil.composeConjunction(rexBuilder, residualConditions));
    }
    if (project != null) {
      relBuilder.project(project.getProjects(), project.getRowType().getFieldNames());
    }
    final RelNode relNode = relBuilder.build();
    call.transformTo(relNode);
  }
}

// End AntiJoinRule.java
