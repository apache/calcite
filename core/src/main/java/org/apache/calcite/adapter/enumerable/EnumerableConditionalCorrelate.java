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
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.DeriveMode;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.ConditionalCorrelate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.ConditionalCorrelate} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableConditionalCorrelate extends ConditionalCorrelate
    implements EnumerableRel {

  protected EnumerableConditionalCorrelate(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns,
      JoinRelType joinType,
      RexNode condition) {
    super(cluster, traits, ImmutableList.of(), left, right, correlationId,
        requiredColumns, joinType, condition);
  }

  /** Creates an EnumerableConditionalCorrelate. */
  public static EnumerableConditionalCorrelate create(
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns,
      JoinRelType joinType,
      RexNode condition) {
    final RelOptCluster cluster = left.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.enumerableCorrelate(mq, left, right, joinType));
    return new EnumerableConditionalCorrelate(
        cluster,
        traitSet,
        left,
        right,
        correlationId,
        requiredColumns,
        joinType,
        condition);
  }

  @Override public EnumerableConditionalCorrelate copy(
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns,
      JoinRelType joinType,
      RexNode condition) {
    return new EnumerableConditionalCorrelate(
        getCluster(),
        traitSet,
        left,
        right,
        correlationId,
        requiredColumns,
        joinType,
        condition);
  }

  @Override public EnumerableConditionalCorrelate copy(
      RelTraitSet traitSet,
      RelNode left,
      RelNode right,
      CorrelationId correlationId,
      ImmutableBitSet requiredColumns,
      JoinRelType joinType) {
    // This method does not provide the condition as an argument, so it should never be called
    throw new RuntimeException("This method should not be called");
  }

  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> passThroughTraits(
      final RelTraitSet required) {
    // EnumerableConditionalCorrelate traits passdown shall only pass through
    // collation to left input. This is because for EnumerableConditionalCorrelate
    // always uses left input as the outer loop, thus only left input can preserve ordering.
    return EnumerableTraitsUtils.passThroughTraitsForJoin(
        required, joinType, left.getRowType().getFieldCount(), getTraitSet());
  }

  @Override public @Nullable Pair<RelTraitSet, List<RelTraitSet>> deriveTraits(
      final RelTraitSet childTraits, final int childId) {
    // should only derive traits (limited to collation for now) from left input.
    return EnumerableTraitsUtils.deriveTraitsForJoin(
        childTraits, childId, joinType, traitSet, right.getTraitSet());
  }

  @Override public DeriveMode getDeriveMode() {
    return DeriveMode.LEFT_FIRST;
  }

  @Override public Result implement(EnumerableRelImplementor implementor,
      Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final Result leftResult =
        implementor.visitChild(this, 0, (EnumerableRel) left, pref);
    Expression leftExpression =
        builder.append(
            "left", leftResult.block);

    final BlockBuilder corrBlock = new BlockBuilder();
    Type corrVarType = leftResult.physType.getJavaRowType();
    ParameterExpression corrRef; // correlate to be used in inner loop
    ParameterExpression corrArg; // argument to correlate lambda (must be boxed)
    if (!Primitive.is(corrVarType)) {
      corrArg =
          Expressions.parameter(Modifier.FINAL,
              corrVarType, getCorrelVariable());
      corrRef = corrArg;
    } else {
      corrArg =
          Expressions.parameter(Modifier.FINAL,
              Primitive.box(corrVarType), "$box" + getCorrelVariable());
      corrRef =
          (ParameterExpression) corrBlock.append(getCorrelVariable(),
              Expressions.unbox(corrArg));
    }

    implementor.registerCorrelVariable(getCorrelVariable(), corrRef,
        corrBlock, leftResult.physType);

    final Result rightResult =
        implementor.visitChild(this, 1, (EnumerableRel) right, pref);

    implementor.clearCorrelVariable(getCorrelVariable());

    // Generate the condition predicate
    final Expression predicate =
        EnumUtils.generatePredicate(
            implementor,
            getCluster().getRexBuilder(),
            left,
            right,
            leftResult.physType,
            rightResult.physType,
            getCondition(),
            true);

    corrBlock.add(rightResult.block);

    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM));

    if (joinType == JoinRelType.LEFT_MARK) {
      // For LEFT_MARK join, use CORRELATE_LEFT_MARK_JOIN with predicate
      Expression selector =
          EnumUtils.markJoinSelector(physType, leftResult.physType);

      builder.append(
          Expressions.call(leftExpression, BuiltInMethod.CORRELATE_LEFT_MARK_JOIN.method,
              Expressions.lambda(corrBlock.toBlock(), corrArg),
              predicate,
              selector));
    } else {
      // TODO: Support other join types. Currently, ConditionalCorrelate is only created
      //  when rewriting correlated IN/SOME/EXISTS subqueries, and its type is always LEFT_MARK.
      throw new UnsupportedOperationException(
          "EnumerableConditionalCorrelate does not support join type: " + joinType);
    }

    return implementor.result(physType, builder.toBlock());
  }
}
