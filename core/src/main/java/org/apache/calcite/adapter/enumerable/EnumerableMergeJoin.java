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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.EnumerableDefaults;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Implementation of {@link org.apache.calcite.rel.core.Join} in
 * {@link EnumerableConvention enumerable calling convention} using
 * a merge algorithm. */
public class EnumerableMergeJoin extends Join implements EnumerableRel {
  EnumerableMergeJoin(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    final List<RelCollation> leftCollations =
        left.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
    final List<RelCollation> rightCollations =
        right.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);

    // If the join keys are not distinct, the sanity check doesn't apply.
    // e.g. t1.a=t2.b and t1.a=t2.c
    boolean isDistinct = Util.isDistinct(joinInfo.leftKeys)
        && Util.isDistinct(joinInfo.rightKeys);

    if (!RelCollations.containsOrderless(leftCollations, joinInfo.leftKeys)
        || !RelCollations.containsOrderless(rightCollations, joinInfo.rightKeys)) {
      if (isDistinct) {
        throw new RuntimeException("wrong collation in left or right input");
      }
    }

    final List<RelCollation> collations =
        traits.getTraits(RelCollationTraitDef.INSTANCE);
    assert collations != null && collations.size() > 0;
    ImmutableIntList rightKeys = joinInfo.rightKeys
        .incr(left.getRowType().getFieldCount());
    // Currently it has very limited ability to represent the equivalent traits
    // due to the flaw of RelCompositeTrait, so the following case is totally
    // legit, but not yet supported:
    // SELECT * FROM foo JOIN bar ON foo.a = bar.c AND foo.b = bar.d;
    // MergeJoin has collation on [a, d], or [b, c]
    if (!RelCollations.containsOrderless(collations, joinInfo.leftKeys)
        && !RelCollations.containsOrderless(collations, rightKeys)) {
      if (isDistinct) {
        throw new RuntimeException("wrong collation for mergejoin");
      }
    }
    if (!isMergeJoinSupported(joinType)) {
      throw new UnsupportedOperationException(
          "EnumerableMergeJoin unsupported for join type " + joinType);
    }
  }

  public static boolean isMergeJoinSupported(JoinRelType joinType) {
    return EnumerableDefaults.isMergeJoinSupported(EnumUtils.toLinq4jJoinType(joinType));
  }

  @Deprecated // to be removed before 2.0
  EnumerableMergeJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left,
      RelNode right, RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    this(cluster, traits, left, right, condition, variablesSet, joinType);
  }

  @Deprecated // to be removed before 2.0
  EnumerableMergeJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left,
      RelNode right, RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, JoinRelType joinType,
      Set<String> variablesStopped) {
    this(cluster, traits, left, right, condition, leftKeys, rightKeys,
        CorrelationId.setOf(variablesStopped), joinType);
  }

  @Override public Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      final RelTraitSet required) {
    // Required collation keys can be subset or superset of merge join keys.
    RelCollation collation = required.getCollation();
    List<Integer> reqKeys = RelCollations.ordinals(collation);
    ImmutableBitSet reqKeySet = ImmutableBitSet.of(reqKeys);

    ImmutableBitSet leftKeySet = ImmutableBitSet.of(joinInfo.leftKeys);
    ImmutableBitSet rightKeySet = ImmutableBitSet.of(joinInfo.rightKeys)
        .shift(left.getRowType().getFieldCount());

    // Only consider exact key match for now
    if (reqKeySet.equals(leftKeySet)) {
      Mappings.TargetMapping mapping = buildMapping(true);
      RelCollation rightCollation = collation.apply(mapping);
      return Pair.of(
          required, ImmutableList.of(required,
          required.replace(rightCollation)));
    } else if (reqKeySet.equals(rightKeySet)) {
      RelCollation rightCollation = RelCollations.shift(collation,
          -left.getRowType().getFieldCount());
      Mappings.TargetMapping mapping = buildMapping(false);
      RelCollation leftCollation = rightCollation.apply(mapping);
      return Pair.of(
          required, ImmutableList.of(
          required.replace(leftCollation),
          required.replace(rightCollation)));
    }
    // TODO: support subset keys and superset keys (CALCITE-4015).
    return null;
  }

  @Override public Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    final int keyCount = joinInfo.leftKeys.size();
    RelCollation collation = childTraits.getCollation();
    final int colCount = collation.getFieldCollations().size();
    if (colCount < keyCount || keyCount == 0) {
      return null;
    }

    if (colCount > keyCount) {
      collation = RelCollations.of(collation.getFieldCollations().subList(0, keyCount));
    }

    ImmutableIntList sourceKeys = childId == 0 ? joinInfo.leftKeys : joinInfo.rightKeys;
    ImmutableBitSet keySet = ImmutableBitSet.of(sourceKeys);
    ImmutableBitSet childCollationKeys = ImmutableBitSet.of(
        RelCollations.ordinals(collation));
    if (!childCollationKeys.equals(keySet)) {
      return null;
    }

    Mappings.TargetMapping mapping = buildMapping(childId == 0);
    RelCollation targetCollation = collation.apply(mapping);

    if (childId == 0) {
      // traits from left child
      RelTraitSet joinTraits = getTraitSet().replace(collation);
      // Forget about the equiv keys for the moment
      return Pair.of(joinTraits,
          ImmutableList.of(childTraits,
          right.getTraitSet().replace(targetCollation)));
    } else {
      // traits from right child
      assert childId == 1;
      RelTraitSet joinTraits = getTraitSet().replace(targetCollation);
      // Forget about the equiv keys for the moment
      return Pair.of(joinTraits,
          ImmutableList.of(joinTraits,
          childTraits.replace(collation)));
    }
  }

  @Override public DeriveMode getDeriveMode() {
    return DeriveMode.BOTH;
  }

  private Mappings.TargetMapping buildMapping(boolean left2Right) {
    ImmutableIntList sourceKeys = left2Right ? joinInfo.leftKeys : joinInfo.rightKeys;
    ImmutableIntList targetKeys = left2Right ? joinInfo.rightKeys : joinInfo.leftKeys;
    Map<Integer, Integer> keyMap = new HashMap<>();
    for (int i = 0; i < joinInfo.leftKeys.size(); i++) {
      keyMap.put(sourceKeys.get(i), targetKeys.get(i));
    }

    Mappings.TargetMapping mapping = Mappings.target(keyMap,
        (left2Right ? left : right).getRowType().getFieldCount(),
        (left2Right ? right : left).getRowType().getFieldCount());
    return mapping;
  }

  public static EnumerableMergeJoin create(RelNode left, RelNode right,
      RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, JoinRelType joinType) {
    final RelOptCluster cluster = right.getCluster();
    RelTraitSet traitSet = cluster.traitSet();
    if (traitSet.isEnabled(RelCollationTraitDef.INSTANCE)) {
      final RelMetadataQuery mq = cluster.getMetadataQuery();
      final List<RelCollation> collations =
          RelMdCollation.mergeJoin(mq, left, right, leftKeys, rightKeys, joinType);
      traitSet = traitSet.replace(collations);
    }
    return new EnumerableMergeJoin(cluster, traitSet, left, right, condition,
        ImmutableSet.of(), joinType);
  }

  @Override public EnumerableMergeJoin copy(RelTraitSet traitSet,
      RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
      boolean semiJoinDone) {
    return new EnumerableMergeJoin(getCluster(), traitSet, left, right,
        condition, variablesSet, joinType);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // We assume that the inputs are sorted. The price of sorting them has
    // already been paid. The cost of the join is therefore proportional to the
    // input and output size.
    final double rightRowCount = right.estimateRowCount(mq);
    final double leftRowCount = left.estimateRowCount(mq);
    final double rowCount = mq.getRowCount(this);
    final double d = leftRowCount + rightRowCount + rowCount;
    return planner.getCostFactory().makeCost(d, 0, 0);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    BlockBuilder builder = new BlockBuilder();
    final Result leftResult =
        implementor.visitChild(this, 0, (EnumerableRel) left, pref);
    final Expression leftExpression =
        builder.append("left", leftResult.block);
    final ParameterExpression left_ =
        Expressions.parameter(leftResult.physType.getJavaRowType(), "left");
    final Result rightResult =
        implementor.visitChild(this, 1, (EnumerableRel) right, pref);
    final Expression rightExpression =
        builder.append("right", rightResult.block);
    final ParameterExpression right_ =
        Expressions.parameter(rightResult.physType.getJavaRowType(), "right");
    final JavaTypeFactory typeFactory = implementor.getTypeFactory();
    final PhysType physType =
        PhysTypeImpl.of(typeFactory, getRowType(), pref.preferArray());
    final List<Expression> leftExpressions = new ArrayList<>();
    final List<Expression> rightExpressions = new ArrayList<>();
    for (Pair<Integer, Integer> pair : Pair.zip(joinInfo.leftKeys, joinInfo.rightKeys)) {
      final RelDataType keyType =
          typeFactory.leastRestrictive(
              ImmutableList.of(
                  left.getRowType().getFieldList().get(pair.left).getType(),
                  right.getRowType().getFieldList().get(pair.right).getType()));
      final Type keyClass = typeFactory.getJavaClass(keyType);
      leftExpressions.add(
          EnumUtils.convert(
              leftResult.physType.fieldReference(left_, pair.left), keyClass));
      rightExpressions.add(
          EnumUtils.convert(
              rightResult.physType.fieldReference(right_, pair.right), keyClass));
    }
    Expression predicate = Expressions.constant(null);
    if (!joinInfo.nonEquiConditions.isEmpty()) {
      final RexNode nonEquiCondition = RexUtil.composeConjunction(
          getCluster().getRexBuilder(), joinInfo.nonEquiConditions, true);
      if (nonEquiCondition != null) {
        predicate = EnumUtils.generatePredicate(implementor, getCluster().getRexBuilder(),
            left, right, leftResult.physType, rightResult.physType, nonEquiCondition);
      }
    }
    final PhysType leftKeyPhysType =
        leftResult.physType.project(joinInfo.leftKeys, JavaRowFormat.LIST);
    final PhysType rightKeyPhysType =
        rightResult.physType.project(joinInfo.rightKeys, JavaRowFormat.LIST);

    // Generate the appropriate key Comparator (keys must be sorted in ascending order, nulls last).
    final int keysSize = joinInfo.leftKeys.size();
    final List<RelFieldCollation> fieldCollations = new ArrayList<>(keysSize);
    for (int i = 0; i < keysSize; i++) {
      fieldCollations.add(
          new RelFieldCollation(i, RelFieldCollation.Direction.ASCENDING,
              RelFieldCollation.NullDirection.LAST));
    }
    final RelCollation collation = RelCollations.of(fieldCollations);
    final Expression comparator = leftKeyPhysType.generateComparator(collation);

    return implementor.result(
        physType,
        builder.append(
            Expressions.call(
                BuiltInMethod.MERGE_JOIN.method,
                Expressions.list(
                    leftExpression,
                    rightExpression,
                    Expressions.lambda(
                        leftKeyPhysType.record(leftExpressions), left_),
                    Expressions.lambda(
                        rightKeyPhysType.record(rightExpressions), right_),
                    predicate,
                    EnumUtils.joinSelector(joinType,
                        physType,
                        ImmutableList.of(
                            leftResult.physType, rightResult.physType)),
                    Expressions.constant(EnumUtils.toLinq4jJoinType(joinType)),
                    comparator))).toBlock());
  }
}
