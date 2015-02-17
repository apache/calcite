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
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.EquiJoin;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableIntList;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/** Implementation of {@link org.apache.calcite.rel.core.Join} in
 * {@link EnumerableConvention enumerable calling convention} using
 * a merge algorithm. */
public class EnumerableMergeJoin extends EquiJoin implements EnumerableRel {
  EnumerableMergeJoin(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      RexNode condition,
      ImmutableIntList leftKeys,
      ImmutableIntList rightKeys,
      JoinRelType joinType,
      Set<String> variablesStopped)
      throws InvalidRelException {
    super(cluster, traits, left, right, condition, leftKeys, rightKeys,
        joinType, variablesStopped);
    final List<RelCollation> collations =
        traits.getTraits(RelCollationTraitDef.INSTANCE);
    assert collations == null || RelCollations.contains(collations, leftKeys);
  }

  public static EnumerableMergeJoin create(RelNode left, RelNode right,
      RexLiteral condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, JoinRelType joinType)
      throws InvalidRelException {
    final RelOptCluster cluster = right.getCluster();
    RelTraitSet traitSet = cluster.traitSet();
    if (traitSet.isEnabled(RelCollationTraitDef.INSTANCE)) {
      final List<RelCollation> collations =
          RelMdCollation.mergeJoin(left, right, leftKeys, rightKeys);
      traitSet = traitSet.replace(collations);
    }
    return new EnumerableMergeJoin(cluster, traitSet, left, right, condition,
        leftKeys, rightKeys, joinType, ImmutableSet.<String>of());
  }

  @Override public EnumerableMergeJoin copy(RelTraitSet traitSet,
      RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
      boolean semiJoinDone) {
    final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
    assert joinInfo.isEqui();
    try {
      return new EnumerableMergeJoin(getCluster(), traitSet, left, right,
          condition, joinInfo.leftKeys, joinInfo.rightKeys, joinType,
          variablesStopped);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // We assume that the inputs are sorted. The price of sorting them has
    // already been paid. The cost of the join is therefore proportional to the
    // input and output size.
    final double rightRowCount = right.getRows();
    final double leftRowCount = left.getRows();
    final double rowCount = RelMetadataQuery.getRowCount(this);
    final double d = leftRowCount + rightRowCount + rowCount;
    return planner.getCostFactory().makeCost(d, 0, 0);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
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
            leftKeys, JavaRowFormat.LIST);
    return implementor.result(
        physType,
        builder.append(
            Expressions.call(
                BuiltInMethod.MERGE_JOIN.method,
                Expressions.list(
                    leftExpression,
                    rightExpression,
                    leftResult.physType.generateAccessor(leftKeys),
                    rightResult.physType.generateAccessor(rightKeys),
                    EnumUtils.joinSelector(joinType,
                        physType,
                        ImmutableList.of(
                            leftResult.physType, rightResult.physType)),
                    Expressions.constant(
                        joinType.generatesNullsOnLeft()),
                    Expressions.constant(
                        joinType.generatesNullsOnRight())))).toBlock());
  }
}

// End EnumerableMergeJoin.java
