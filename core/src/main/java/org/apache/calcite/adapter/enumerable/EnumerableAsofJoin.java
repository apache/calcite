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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AsofJoin;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Implementation of {@link LogicalAsofJoin} in
 * {@link EnumerableConvention enumerable calling convention}. */
public class EnumerableAsofJoin extends AsofJoin implements EnumerableRel {
  /** Creates an EnumerableAsofJoin.
   *
   * <p>Use {@link #create} unless you know what you're doing. */
  protected EnumerableAsofJoin(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      RexNode condition,
      RexNode matchCondition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(
        cluster,
        traits,
        ImmutableList.of(),
        left,
        right,
        condition,
        matchCondition,
        variablesSet,
        joinType);
  }

  /** Creates an EnumerableAsofJoin. */
  public static EnumerableAsofJoin create(
      RelNode left,
      RelNode right,
      RexNode condition,
      RexNode matchCondition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    final RelOptCluster cluster = left.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.enumerableHashJoin(mq, left, right, joinType));
    return new EnumerableAsofJoin(cluster, traitSet, left, right, condition, matchCondition,
        variablesSet, joinType);
  }

  @Override public EnumerableAsofJoin copy(RelTraitSet traitSet, RexNode condition,
                                           RelNode left, RelNode right, JoinRelType joinType,
                                           boolean semiJoinDone) {
    // This method does not know about the matchCondition, so it should not be called
    throw new RuntimeException("This method should not be called");
  }

  @Override public Join copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.size() == 2;
    return new EnumerableAsofJoin(
        getCluster(), traitSet, inputs.get(0), inputs.get(1),
            getCondition(), matchCondition, variablesSet, joinType);
  }

  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      final RelTraitSet required) {
    return EnumerableTraitsUtils.passThroughTraitsForJoin(
        required, joinType, left.getRowType().getFieldCount(), getTraitSet());
  }

  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    // should only derive traits (limited to collation for now) from left join input.
    return EnumerableTraitsUtils.deriveTraitsForJoin(
        childTraits, childId, joinType, getTraitSet(), right.getTraitSet());
  }

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    double rowCount = mq.getRowCount(this);
    return planner.getCostFactory().makeCost(rowCount, 0, 0);
  }

  /** Generates the function that compares two rows from the right collection on
   * their timestamp field.
   *
   * @param rightCollectionType  Type of data in right collection.
   * @param kind                 Comparison kind.
   * @param timestampFieldIndex  Index of the field that is the timestamp field.
   */
  private static Expression generateTimestampComparator(
      PhysType rightCollectionType, SqlKind kind, int timestampFieldIndex) {
    RelFieldCollation.Direction direction;
    switch (kind) {
    case LESS_THAN:
    case LESS_THAN_OR_EQUAL:
      direction = RelFieldCollation.Direction.ASCENDING;
      break;
    case GREATER_THAN:
    case GREATER_THAN_OR_EQUAL:
      direction = RelFieldCollation.Direction.DESCENDING;
      break;
    default:
      throw new RuntimeException("Unexpected timestamp comparison in ASOF join " + kind);
    }

    final List<RelFieldCollation> fieldCollations = new ArrayList<>(1);
    fieldCollations.add(
        new RelFieldCollation(timestampFieldIndex, direction,
            RelFieldCollation.NullDirection.FIRST));
    final RelCollation collation = RelCollations.of(fieldCollations);
    return rightCollectionType.generateComparator(collation);
  }

  /** Extracts from a comparison 'call' the index of the field from
   * the inner collection that is used in the comparison. */
  private int getTimestampFieldIndex(RexCall call) {
    int timestampFieldIndex;
    int leftFieldCount = left.getRowType().getFieldCount();
    List<RexNode> operands = call.getOperands();
    assert operands.size() == 2;
    RexNode compareLeft = operands.get(0);
    RexNode compareRight = operands.get(1);
    assert compareLeft instanceof RexInputRef;
    assert compareRight instanceof RexInputRef;
    RexInputRef leftInputRef = (RexInputRef) compareLeft;
    RexInputRef rightInputRef = (RexInputRef) compareRight;
    // We know for sure that these two come from the inner and outer collection respectively,
    // but we don't know which is which
    if (leftInputRef.getIndex() < leftFieldCount) {
      // Left input comes from the left collection
      timestampFieldIndex = rightInputRef.getIndex() - leftFieldCount;
    } else {
      // Left input comes from the right collection
      timestampFieldIndex = leftInputRef.getIndex() - leftFieldCount;
    }
    return timestampFieldIndex;
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
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
    // ASOF joins conditions are restricted to equalities
    assert joinInfo.nonEquiConditions.isEmpty();

    // From the match condition we need to find out the kind of comparison performed
    // and the timestamp field in the right collection.
    assert matchCondition instanceof RexCall;
    RexCall call = (RexCall) matchCondition;
    SqlKind kind = call.getKind();
    int timestampFieldIndex = getTimestampFieldIndex(call);

    Expression timestampComparator =
        generateTimestampComparator(rightResult.physType, kind, timestampFieldIndex);

    Expression matchPredicate =
        EnumUtils.generatePredicate(implementor, getCluster().getRexBuilder(),
            left, right, leftResult.physType, rightResult.physType, matchCondition);
    return implementor.result(
        physType,
        builder.append(
            Expressions.call(
                leftExpression,
                BuiltInMethod.ASOF_JOIN.method,
                Expressions.list(
                    rightExpression,
                    // outer key selector
                    leftResult.physType.generateAccessorWithoutNulls(joinInfo.leftKeys),
                    // inner key selector
                    rightResult.physType.generateAccessorWithoutNulls(joinInfo.rightKeys),
                    // result selector
                    EnumUtils.joinSelector(joinType,
                        physType,
                        ImmutableList.of(
                            leftResult.physType, rightResult.physType)))
                    // match comparator
                    .append(matchPredicate)
                    // comparator for the columns used as "timestamps"
                    .append(timestampComparator)
                    // generatesNullOnRight
                    .append(Expressions.constant(joinType.generatesNullsOnRight()))))
            .toBlock());
  }
}
