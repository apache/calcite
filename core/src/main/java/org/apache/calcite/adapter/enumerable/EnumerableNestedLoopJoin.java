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

import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelNodes;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

/** Implementation of {@link org.apache.calcite.rel.core.Join} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}
 * that allows conditions that are not just {@code =} (equals). */
public class EnumerableNestedLoopJoin extends Join implements EnumerableRel {
  /** Creates an EnumerableNestedLoopJoin. */
  protected EnumerableNestedLoopJoin(RelOptCluster cluster, RelTraitSet traits,
      RelNode left, RelNode right, RexNode condition,
      Set<CorrelationId> variablesSet, JoinRelType joinType) {
    super(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType);
  }

  @Deprecated // to be removed before 2.0
  protected EnumerableNestedLoopJoin(RelOptCluster cluster, RelTraitSet traits,
      RelNode left, RelNode right, RexNode condition, JoinRelType joinType,
      Set<String> variablesStopped) {
    this(cluster, traits, left, right, condition,
        CorrelationId.setOf(variablesStopped), joinType);
  }

  @Override public EnumerableNestedLoopJoin copy(RelTraitSet traitSet,
      RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
      boolean semiJoinDone) {
    return new EnumerableNestedLoopJoin(getCluster(), traitSet, left, right,
        condition, variablesSet, joinType);
  }

  /** Creates an EnumerableNestedLoopJoin. */
  public static EnumerableNestedLoopJoin create(
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    final RelOptCluster cluster = left.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.enumerableNestedLoopJoin(mq, left, right, joinType));
    return new EnumerableNestedLoopJoin(cluster, traitSet, left, right, condition,
        variablesSet, joinType);
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(this);

    // Joins can be flipped, and for many algorithms, both versions are viable
    // and have the same cost. To make the results stable between versions of
    // the planner, make one of the versions slightly more expensive.
    switch (joinType) {
    case SEMI:
    case ANTI:
      // SEMI and ANTI join cannot be flipped
      break;
    case RIGHT:
      rowCount = RelMdUtil.addEpsilon(rowCount);
      break;
    default:
      if (RelNodes.COMPARATOR.compare(left, right) > 0) {
        rowCount = RelMdUtil.addEpsilon(rowCount);
      }
    }

    final double rightRowCount = mq.getRowCount(right);
    final double leftRowCount = mq.getRowCount(left);
    if (Double.isInfinite(leftRowCount)) {
      rowCount = leftRowCount;
    }
    if (Double.isInfinite(rightRowCount)) {
      rowCount = rightRowCount;
    }

    RelOptCost cost = planner.getCostFactory().makeCost(rowCount, 0, 0);
    // Give it some penalty
    cost = cost.multiplyBy(10);
    return cost;
  }

  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      final RelTraitSet required) {
    // EnumerableNestedLoopJoin traits passdown shall only pass through collation to
    // left input. It is because for EnumerableNestedLoopJoin always
    // uses left input as the outer loop, thus only left input can preserve ordering.
    // Push sort both to left and right inputs does not help right outer join. It's because in
    // implementation, EnumerableNestedLoopJoin produces (null, right_unmatched) all together,
    // which does not preserve ordering from right side.
    return EnumerableTraitsUtils.passThroughTraitsForJoin(
        required, joinType, getLeft().getRowType().getFieldCount(), traitSet);
  }

  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    return EnumerableTraitsUtils.deriveTraitsForJoin(
        childTraits, childId, joinType, traitSet, right.getTraitSet());
  }

  @Override public DeriveMode getDeriveMode() {
    if (joinType == JoinRelType.FULL || joinType == JoinRelType.RIGHT) {
      return DeriveMode.PROHIBITED;
    }

    return DeriveMode.LEFT_FIRST;
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final Result leftResult =
        implementor.visitChild(this, 0, (EnumerableRel) left, pref);
    Expression leftExpression =
        builder.append("left", leftResult.block);
    final Result rightResult =
        implementor.visitChild(this, 1, (EnumerableRel) right, pref);
    Expression rightExpression =
        builder.append("right", rightResult.block);
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(),
            getRowType(),
            pref.preferArray());
    final Expression predicate =
        EnumUtils.generatePredicate(implementor, getCluster().getRexBuilder(), left, right,
            leftResult.physType, rightResult.physType, condition);
    return implementor.result(
        physType,
        builder.append(
            Expressions.call(BuiltInMethod.NESTED_LOOP_JOIN.method,
                leftExpression,
                rightExpression,
                predicate,
                EnumUtils.joinSelector(joinType,
                    physType,
                    ImmutableList.of(leftResult.physType,
                        rightResult.physType)),
                Expressions.constant(EnumUtils.toLinq4jJoinType(joinType))))
            .toBlock());
  }
}
