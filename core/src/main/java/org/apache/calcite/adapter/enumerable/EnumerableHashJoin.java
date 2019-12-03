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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Method;
import java.util.Set;

/** Implementation of {@link org.apache.calcite.rel.core.Join} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableHashJoin extends Join implements EnumerableRel {
  /** Creates an EnumerableHashJoin.
   *
   * <p>Use {@link #create} unless you know what you're doing. */
  protected EnumerableHashJoin(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(
        cluster,
        traits,
        left,
        right,
        condition,
        variablesSet,
        joinType);
  }

  @Deprecated // to be removed before 2.0
  protected EnumerableHashJoin(RelOptCluster cluster, RelTraitSet traits,
      RelNode left, RelNode right, RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, JoinRelType joinType,
      Set<String> variablesStopped) {
    this(cluster, traits, left, right, condition, CorrelationId.setOf(variablesStopped), joinType);
  }

  /** Creates an EnumerableHashJoin. */
  public static EnumerableHashJoin create(
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
                () -> RelMdCollation.enumerableHashJoin(mq, left, right, joinType));
    return new EnumerableHashJoin(cluster, traitSet, left, right, condition,
        variablesSet, joinType);
  }

  @Override public EnumerableHashJoin copy(RelTraitSet traitSet, RexNode condition,
      RelNode left, RelNode right, JoinRelType joinType,
      boolean semiJoinDone) {
    return new EnumerableHashJoin(getCluster(), traitSet, left, right,
        condition, variablesSet, joinType);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
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

    // Cheaper if the smaller number of rows is coming from the LHS.
    // Model this by adding L log L to the cost.
    final double rightRowCount = right.estimateRowCount(mq);
    final double leftRowCount = left.estimateRowCount(mq);
    if (Double.isInfinite(leftRowCount)) {
      rowCount = leftRowCount;
    } else {
      rowCount += Util.nLogN(leftRowCount);
    }
    if (Double.isInfinite(rightRowCount)) {
      rowCount = rightRowCount;
    } else {
      rowCount += rightRowCount;
    }
    if (isSemiJoin()) {
      return planner.getCostFactory().makeCost(rowCount, 0, 0).multiplyBy(.01d);
    } else {
      return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    switch (joinType) {
    case SEMI:
    case ANTI:
      return implementHashSemiJoin(implementor, pref);
    default:
      return implementHashJoin(implementor, pref);
    }
  }

  private Result implementHashSemiJoin(EnumerableRelImplementor implementor, Prefer pref) {
    assert joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI;
    final Method method = joinType == JoinRelType.SEMI
        ? BuiltInMethod.SEMI_JOIN.method
        : BuiltInMethod.ANTI_JOIN.method;
    BlockBuilder builder = new BlockBuilder();
    final Result leftResult =
        implementor.visitChild(this, 0, (EnumerableRel) left, pref);
    Expression leftExpression =
        builder.append(
            "left", leftResult.block);
    final Result rightResult =
        implementor.visitChild(this, 1, (EnumerableRel) right, pref);
    Expression rightExpression =
        builder.append(
            "right", rightResult.block);
    final PhysType physType = leftResult.physType;
    final PhysType keyPhysType =
        leftResult.physType.project(
            joinInfo.leftKeys, JavaRowFormat.LIST);
    Expression predicate = Expressions.constant(null);
    if (!joinInfo.nonEquiConditions.isEmpty()) {
      RexNode nonEquiCondition = RexUtil.composeConjunction(
          getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
      if (nonEquiCondition != null) {
        predicate = EnumUtils.generatePredicate(implementor, getCluster().getRexBuilder(),
            left, right, leftResult.physType, rightResult.physType, nonEquiCondition);
      }
    }
    return implementor.result(
        physType,
        builder.append(
            Expressions.call(
                method,
                Expressions.list(
                    leftExpression,
                    rightExpression,
                    leftResult.physType.generateAccessor(joinInfo.leftKeys),
                    rightResult.physType.generateAccessor(joinInfo.rightKeys),
                    Util.first(keyPhysType.comparer(),
                        Expressions.constant(null)),
                    predicate)))
            .toBlock());
  }

  private Result implementHashJoin(EnumerableRelImplementor implementor, Prefer pref) {
    BlockBuilder builder = new BlockBuilder();
    final Result leftResult =
        implementor.visitChild(this, 0, (EnumerableRel) left, pref);
    Expression leftExpression =
        builder.append(
            "left", leftResult.block);
    final Result rightResult =
        implementor.visitChild(this, 1, (EnumerableRel) right, pref);
    Expression rightExpression =
        builder.append(
            "right", rightResult.block);
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(), getRowType(), pref.preferArray());
    final PhysType keyPhysType =
        leftResult.physType.project(
            joinInfo.leftKeys, JavaRowFormat.LIST);
    Expression predicate = Expressions.constant(null);
    if (!joinInfo.nonEquiConditions.isEmpty()) {
      RexNode nonEquiCondition = RexUtil.composeConjunction(
          getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
      if (nonEquiCondition != null) {
        predicate = EnumUtils.generatePredicate(implementor, getCluster().getRexBuilder(),
            left, right, leftResult.physType, rightResult.physType, nonEquiCondition);
      }
    }
    return implementor.result(
        physType,
        builder.append(
            Expressions.call(
                leftExpression,
                BuiltInMethod.HASH_JOIN.method,
                Expressions.list(
                    rightExpression,
                    leftResult.physType.generateAccessor(joinInfo.leftKeys),
                    rightResult.physType.generateAccessor(joinInfo.rightKeys),
                    EnumUtils.joinSelector(joinType,
                        physType,
                        ImmutableList.of(
                            leftResult.physType, rightResult.physType)))
                    .append(
                        Util.first(keyPhysType.comparer(),
                            Expressions.constant(null)))
                    .append(
                        Expressions.constant(joinType.generatesNullsOnLeft()))
                    .append(
                        Expressions.constant(
                            joinType.generatesNullsOnRight()))
                    .append(predicate)))
            .toBlock());
  }
}

// End EnumerableHashJoin.java
