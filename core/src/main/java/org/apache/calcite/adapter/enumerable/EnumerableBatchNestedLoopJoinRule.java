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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Planner rule that converts a
 * {@link org.apache.calcite.rel.logical.LogicalJoin} into an
 * {@link org.apache.calcite.adapter.enumerable.EnumerableBatchNestedLoopJoin}. */
public class EnumerableBatchNestedLoopJoinRule extends RelOptRule {

  private final int batchSize;
  private static final int DEFAULT_BATCH_SIZE = 100;

  /** Creates an EnumerableBatchNestedLoopJoinRule. */
  protected EnumerableBatchNestedLoopJoinRule(Class<? extends Join> clazz,
      RelBuilderFactory relBuilderFactory, int batchSize) {
    super(operand(clazz, any()),
        relBuilderFactory, "EnumerableBatchNestedLoopJoinRule");
    this.batchSize = batchSize;
  }
  /** Creates an EnumerableBatchNestedLoopJoinRule with default batch size of 100. */
  public EnumerableBatchNestedLoopJoinRule(RelBuilderFactory relBuilderFactory) {
    this(LogicalJoin.class, relBuilderFactory, DEFAULT_BATCH_SIZE);
  }

  /** Creates an EnumerableBatchNestedLoopJoinRule with given batch size.
   * Warning: if the batch size is around or bigger than 1000 there
   * can be an error because the generated code exceeds the size limit */
  public EnumerableBatchNestedLoopJoinRule(RelBuilderFactory relBuilderFactory, int batchSize) {
    this(LogicalJoin.class, relBuilderFactory, batchSize);
  }

  @Override public boolean matches(RelOptRuleCall call) {
    Join join = call.rel(0);
    JoinRelType joinType = join.getJoinType();
    return joinType == JoinRelType.INNER
        || joinType == JoinRelType.LEFT
        || joinType == JoinRelType.ANTI
        || joinType == JoinRelType.SEMI;
  }

  @Override public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    final int leftFieldCount = join.getLeft().getRowType().getFieldCount();
    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelBuilder relBuilder = call.builder();

    final Set<CorrelationId> correlationIds = new HashSet<>();
    final ArrayList<RexNode> corrVar = new ArrayList<>();

    for (int i = 0; i < batchSize; i++) {
      CorrelationId correlationId = cluster.createCorrel();
      correlationIds.add(correlationId);
      corrVar.add(
          rexBuilder.makeCorrel(join.getLeft().getRowType(),
              correlationId));
    }

    final ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();

    // Generate first condition
    final RexNode condition = join.getCondition().accept(new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef input) {
        int field = input.getIndex();
        if (field >= leftFieldCount) {
          return rexBuilder.makeInputRef(input.getType(),
              input.getIndex() - leftFieldCount);
        }
        requiredColumns.set(field);
        return rexBuilder.makeFieldAccess(corrVar.get(0), field);
      }
    });

    List<RexNode> conditionList = new ArrayList<>();
    conditionList.add(condition);

    // Add batchSize-1 other conditions
    for (int i = 1; i < batchSize; i++) {
      final int corrIndex = i;
      final RexNode condition2 = condition.accept(new RexShuttle() {
        @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
          return corrVar.get(corrIndex);
        }
      });
      conditionList.add(condition2);
    }

    // Push a filter with batchSize disjunctions
    relBuilder.push(join.getRight()).filter(relBuilder.or(conditionList));
    RelNode right = relBuilder.build();

    JoinRelType joinType = join.getJoinType();
    call.transformTo(
        EnumerableBatchNestedLoopJoin.create(
            convert(join.getLeft(), join.getLeft().getTraitSet()
                .replace(EnumerableConvention.INSTANCE)),
            convert(right, right.getTraitSet()
                .replace(EnumerableConvention.INSTANCE)),
            join.getCondition(),
            requiredColumns.build(),
            correlationIds,
            joinType));
  }
}

// End EnumerableBatchNestedLoopJoinRule.java
