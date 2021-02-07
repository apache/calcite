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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
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
import org.apache.calcite.util.ImmutableBeans;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Rule to convert a {@link LogicalJoin} to an {@link EnumerableBatchNestedLoopJoin}.
 * You may provide a custom config to convert other nodes that extend {@link Join}.
 *
 * @see EnumerableRules#ENUMERABLE_BATCH_NESTED_LOOP_JOIN_RULE
 */
public class EnumerableBatchNestedLoopJoinRule
    extends RelRule<EnumerableBatchNestedLoopJoinRule.Config> {
  /** Creates an EnumerableBatchNestedLoopJoinRule. */
  protected EnumerableBatchNestedLoopJoinRule(Config config) {
    super(config);
  }

  @Deprecated // to be removed before 2.0
  protected EnumerableBatchNestedLoopJoinRule(Class<? extends Join> clazz,
      RelBuilderFactory relBuilderFactory, int batchSize) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .withOperandSupplier(b -> b.operand(clazz).anyInputs())
        .as(Config.class)
        .withBatchSize(batchSize));
  }

  @Deprecated // to be removed before 2.0
  public EnumerableBatchNestedLoopJoinRule(RelBuilderFactory relBuilderFactory) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class));
  }

  @Deprecated // to be removed before 2.0
  public EnumerableBatchNestedLoopJoinRule(RelBuilderFactory relBuilderFactory,
      int batchSize) {
    this(Config.DEFAULT.withRelBuilderFactory(relBuilderFactory)
        .as(Config.class)
        .withBatchSize(batchSize));
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
    final List<RexNode> corrVarList = new ArrayList<>();

    final int batchSize = config.batchSize();
    for (int i = 0; i < batchSize; i++) {
      CorrelationId correlationId = cluster.createCorrel();
      correlationIds.add(correlationId);
      corrVarList.add(
          rexBuilder.makeCorrel(join.getLeft().getRowType(),
              correlationId));
    }
    final RexNode corrVar0 = corrVarList.get(0);

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
        return rexBuilder.makeFieldAccess(corrVar0, field);
      }
    });

    final List<RexNode> conditionList = new ArrayList<>();
    conditionList.add(condition);

    // Add batchSize-1 other conditions
    for (int i = 1; i < batchSize; i++) {
      final int corrIndex = i;
      final RexNode condition2 = condition.accept(new RexShuttle() {
        @Override public RexNode visitCorrelVariable(RexCorrelVariable variable) {
          return variable.equals(corrVar0) ? corrVarList.get(corrIndex) : variable;
        }
      });
      conditionList.add(condition2);
    }

    // Push a filter with batchSize disjunctions
    relBuilder.push(join.getRight()).filter(relBuilder.or(conditionList));
    final RelNode right = relBuilder.build();

    call.transformTo(
        EnumerableBatchNestedLoopJoin.create(
            convert(join.getLeft(), join.getLeft().getTraitSet()
                .replace(EnumerableConvention.INSTANCE)),
            convert(right, right.getTraitSet()
                .replace(EnumerableConvention.INSTANCE)),
            join.getCondition(),
            requiredColumns.build(),
            correlationIds,
            join.getJoinType()));
  }

  /** Rule configuration. */
  public interface Config extends RelRule.Config {
    Config DEFAULT = EMPTY
        .withOperandSupplier(b -> b.operand(LogicalJoin.class).anyInputs())
        .withDescription("EnumerableBatchNestedLoopJoinRule")
        .as(Config.class);

    @Override default EnumerableBatchNestedLoopJoinRule toRule() {
      return new EnumerableBatchNestedLoopJoinRule(this);
    }

    /** Batch size.
     *
     * <p>Warning: if the batch size is around or bigger than 1000 there
     * can be an error because the generated code exceeds the size limit. */
    @ImmutableBeans.Property
    @ImmutableBeans.IntDefault(100)
    int batchSize();

    /** Sets {@link #batchSize()}. */
    Config withBatchSize(int batchSize);
  }
}
