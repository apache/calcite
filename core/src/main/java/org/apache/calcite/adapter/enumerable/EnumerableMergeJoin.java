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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.calcite.rel.RelCollations.containsOrderless;

import static java.util.Objects.requireNonNull;

/** Implementation of {@link org.apache.calcite.rel.core.Join} in
 * {@link EnumerableConvention enumerable calling convention} using
 * a merge algorithm. */
public class EnumerableMergeJoin extends Join implements EnumerableRel {
  protected EnumerableMergeJoin(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType) {
    super(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    assert getConvention() instanceof EnumerableConvention;
    final List<RelCollation> leftCollations = getCollations(left.getTraitSet());
    final List<RelCollation> rightCollations = getCollations(right.getTraitSet());

    // If the join keys are not distinct, the sanity check doesn't apply.
    // e.g. t1.a=t2.b and t1.a=t2.c
    boolean isDistinct = Util.isDistinct(joinInfo.leftKeys)
        && Util.isDistinct(joinInfo.rightKeys);

    if (!RelCollations.collationsContainKeysOrderless(leftCollations, joinInfo.leftKeys)
        || !RelCollations.collationsContainKeysOrderless(rightCollations, joinInfo.rightKeys)) {
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
    if (!RelCollations.collationsContainKeysOrderless(collations, joinInfo.leftKeys)
        && !RelCollations.collationsContainKeysOrderless(collations, rightKeys)
        && !RelCollations.keysContainCollationsOrderless(joinInfo.leftKeys, collations)
        && !RelCollations.keysContainCollationsOrderless(rightKeys, collations)) {
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

  private static RelCollation getCollation(RelTraitSet traits) {
    return requireNonNull(traits.getCollation(),
        () -> "no collation trait in " + traits);
  }

  private static List<RelCollation> getCollations(RelTraitSet traits) {
    return requireNonNull(traits.getTraits(RelCollationTraitDef.INSTANCE),
        () -> "no collation trait in " + traits);
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

  /**
   * Pass collations through can have three cases:
   *
   * <p>1. If sort keys are equal to either left join keys, or right join keys,
   * collations can be pushed to both join sides with correct mappings.
   * For example, for the query
   *
   * <blockquote><pre>{@code
   *    select * from foo join bar
   *        on foo.a=bar.b
   *    order by foo.a desc
   * }</pre></blockquote>
   *
   * <p>after traits pass through it will be equivalent to
   *
   * <blockquote><pre>{@code
   *    select * from
   *        (select * from foo order by foo.a desc)
   *        join
   *        (select * from bar order by bar.b desc)
   * }</pre></blockquote>
   *
   * <p>2. If sort keys are sub-set of either left join keys, or right join
   * keys, collations have to be extended to cover all joins keys before
   * passing through, because merge join requires all join keys are sorted.
   * For example, for the query
   *
   * <blockquote><pre>{@code
   *    select * from foo join bar
   *        on foo.a=bar.b and foo.c=bar.d
   *    order by foo.a desc
   * }</pre></blockquote>
   *
   * <p>after traits pass through it will be equivalent to
   *
   * <blockquote><pre>{@code
   *    select * from
   *        (select * from foo order by foo.a desc, foo.c)
   *        join
   *        (select * from bar order by bar.b desc, bar.d)
   * }</pre></blockquote>
   *
   * <p>3. If sort keys are super-set of either left join keys, or right join
   * keys, but not both, collations can be completely passed to the join key
   * whose join keys match the prefix of collations. Meanwhile, partial mapped
   * collations can be passed to another join side to make sure join keys are
   * sorted. For example, for the query

   * <blockquote><pre>{@code
   *    select * from foo join bar
   *        on foo.a=bar.b and foo.c=bar.d
   *        order by foo.a desc, foo.c desc, foo.e
   * }</pre></blockquote>
   *
   * <p>after traits pass through it will be equivalent to
   *
   * <blockquote><pre>{@code
   *    select * from
   *        (select * from foo order by foo.a desc, foo.c desc, foo.e)
   *        join
   *        (select * from bar order by bar.b desc, bar.d desc)
   * }</pre></blockquote>
   */
  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      final RelTraitSet required) {
    // Required collation keys can be subset or superset of merge join keys.
    RelCollation collation = getCollation(required);
    int leftInputFieldCount = left.getRowType().getFieldCount();

    List<Integer> reqKeys = RelCollations.ordinals(collation);
    List<Integer> leftKeys = joinInfo.leftKeys.toIntegerList();
    List<Integer> rightKeys =
        joinInfo.rightKeys.incr(leftInputFieldCount).toIntegerList();

    ImmutableBitSet reqKeySet = ImmutableBitSet.of(reqKeys);
    ImmutableBitSet leftKeySet = ImmutableBitSet.of(joinInfo.leftKeys);
    ImmutableBitSet rightKeySet = ImmutableBitSet.of(joinInfo.rightKeys)
        .shift(leftInputFieldCount);

    if (reqKeySet.equals(leftKeySet)) {
      // if sort keys equal to left join keys, we can pass through all collations directly.
      Mappings.TargetMapping mapping = buildMapping(true);
      RelCollation rightCollation = collation.apply(mapping);
      return Pair.of(
          required, ImmutableList.of(required,
          required.replace(rightCollation)));
    } else if (containsOrderless(leftKeys, collation)) {
      // if sort keys are subset of left join keys, we can extend collations to make sure all join
      // keys are sorted.
      collation = extendCollation(collation, leftKeys);
      Mappings.TargetMapping mapping = buildMapping(true);
      RelCollation rightCollation = collation.apply(mapping);
      return Pair.of(
          required, ImmutableList.of(required.replace(collation),
              required.replace(rightCollation)));
    } else if (containsOrderless(collation, leftKeys)
        && reqKeys.stream().allMatch(i -> i < leftInputFieldCount)) {
      // if sort keys are superset of left join keys, and left join keys is prefix of sort keys
      // (order not matter), also sort keys are all from left join input.
      Mappings.TargetMapping mapping = buildMapping(true);
      RelCollation rightCollation =
          RexUtil.apply(
              mapping,
              intersectCollationAndJoinKey(collation, joinInfo.leftKeys));
      return Pair.of(
          required, ImmutableList.of(required,
              required.replace(rightCollation)));
    } else if (reqKeySet.equals(rightKeySet)) {
      // if sort keys equal to right join keys, we can pass through all collations directly.
      RelCollation rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
      Mappings.TargetMapping mapping = buildMapping(false);
      RelCollation leftCollation = rightCollation.apply(mapping);
      return Pair.of(
          required, ImmutableList.of(
          required.replace(leftCollation),
          required.replace(rightCollation)));
    } else if (containsOrderless(rightKeys, collation)) {
      // if sort keys are subset of right join keys, we can extend collations to make sure all join
      // keys are sorted.
      collation = extendCollation(collation, rightKeys);
      RelCollation rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
      Mappings.TargetMapping mapping = buildMapping(false);
      RelCollation leftCollation = RexUtil.apply(mapping, rightCollation);
      return Pair.of(
          required, ImmutableList.of(
              required.replace(leftCollation),
              required.replace(rightCollation)));
    } else if (containsOrderless(collation, rightKeys)
        && reqKeys.stream().allMatch(i -> i >= leftInputFieldCount)) {
      // if sort keys are superset of right join keys, and right join keys is prefix of sort keys
      // (order not matter), also sort keys are all from right join input.
      RelCollation rightCollation = RelCollations.shift(collation, -leftInputFieldCount);
      Mappings.TargetMapping mapping = buildMapping(false);
      RelCollation leftCollation =
          RexUtil.apply(
              mapping,
              intersectCollationAndJoinKey(rightCollation, joinInfo.rightKeys));
      return Pair.of(
          required, ImmutableList.of(
              required.replace(leftCollation),
              required.replace(rightCollation)));
    }

    return null;
  }

  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    final int keyCount = joinInfo.leftKeys.size();
    RelCollation collation = getCollation(childTraits);
    final int colCount = collation.getFieldCollations().size();
    if (colCount < keyCount || keyCount == 0) {
      return null;
    }

    if (colCount > keyCount) {
      collation = RelCollations.of(collation.getFieldCollations().subList(0, keyCount));
    }

    ImmutableIntList sourceKeys = childId == 0 ? joinInfo.leftKeys : joinInfo.rightKeys;
    ImmutableBitSet keySet = ImmutableBitSet.of(sourceKeys);
    ImmutableBitSet childCollationKeys =
        ImmutableBitSet.of(RelCollations.ordinals(collation));
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

    Mappings.TargetMapping mapping =
        Mappings.target(keyMap,
            (left2Right ? left : right).getRowType().getFieldCount(),
            (left2Right ? right : left).getRowType().getFieldCount());
    return mapping;
  }

  /**
   * This function extends collation by appending new collation fields defined on keys.
   */
  private static RelCollation extendCollation(RelCollation collation, List<Integer> keys) {
    List<RelFieldCollation> fieldsForNewCollation = new ArrayList<>(keys.size());
    fieldsForNewCollation.addAll(collation.getFieldCollations());

    ImmutableBitSet keysBitset = ImmutableBitSet.of(keys);
    ImmutableBitSet colKeysBitset = ImmutableBitSet.of(collation.getKeys());
    ImmutableBitSet exceptBitset = keysBitset.except(colKeysBitset);
    for (Integer i : exceptBitset) {
      fieldsForNewCollation.add(new RelFieldCollation(i));
    }
    return RelCollations.of(fieldsForNewCollation);
  }

  /**
   * This function will remove collations that are not defined on join keys.
   * For example:
   *    select * from
   *    foo join bar
   *    on foo.a = bar.a and foo.c=bar.c
   *    order by bar.a, bar.c, bar.b;
   *
   * <p>The collation [bar.a, bar.c, bar.b] can be pushed down to bar. However,
   * only [a, c] can be pushed down to foo. This function will help create [a,
   * c] for foo by removing b from the required collation, because b is not
   * defined on join keys.
   *
   * @param collation collation defined on the JOIN
   * @param joinKeys  the join keys
   */
  private static RelCollation intersectCollationAndJoinKey(
      RelCollation collation, ImmutableIntList joinKeys) {
    List<RelFieldCollation> fieldCollations = new ArrayList<>();
    for (RelFieldCollation rf : collation.getFieldCollations()) {
      if (joinKeys.contains(rf.getFieldIndex())) {
        fieldCollations.add(rf);
      }
    }
    return RelCollations.of(fieldCollations);
  }

  public static EnumerableMergeJoin create(RelNode left, RelNode right,
      RexNode condition, ImmutableIntList leftKeys,
      ImmutableIntList rightKeys, JoinRelType joinType) {
    final RelOptCluster cluster = right.getCluster();
    RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE);
    if (traitSet.isEnabled(RelCollationTraitDef.INSTANCE)) {
      final RelMetadataQuery mq = cluster.getMetadataQuery();
      final List<RelCollation> collations =
          RelMdCollation.mergeJoin(mq, left, right, leftKeys, rightKeys, joinType);
      traitSet = traitSet.replaceIfs(RelCollationTraitDef.INSTANCE, () -> collations);
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

  @Override public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner,
      RelMetadataQuery mq) {
    // We assume that the inputs are sorted. The price of sorting them has
    // already been paid. The cost of the join is therefore proportional to the
    // input and output size.
    final double rightRowCount = mq.getRowCount(right);
    final double leftRowCount = mq.getRowCount(left);
    final double rowCount = mq.getRowCount(this);
    final double d = leftRowCount + rightRowCount + rowCount;
    return planner.getCostFactory().makeCost(d, 0, 0);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
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
      RelDataType leftType = left.getRowType().getFieldList().get(pair.left).getType();
      RelDataType rightType = right.getRowType().getFieldList().get(pair.right).getType();
      final RelDataType keyType =
          requireNonNull(
              typeFactory.leastRestrictive(ImmutableList.of(leftType, rightType)),
              () -> "leastRestrictive returns null for " + leftType
                  + " and " + rightType);
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
      final RexNode nonEquiCondition =
          RexUtil.composeConjunction(getCluster().getRexBuilder(),
              joinInfo.nonEquiConditions, true);
      if (nonEquiCondition != null) {
        predicate =
            EnumUtils.generatePredicate(implementor,
                getCluster().getRexBuilder(), left, right, leftResult.physType,
                rightResult.physType, nonEquiCondition);
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
    final Expression comparator = leftKeyPhysType.generateMergeJoinComparator(collation);

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
                    comparator,
                    Util.first(
                        leftKeyPhysType.comparer(),
                        Expressions.constant(null))))).toBlock());
  }
}
