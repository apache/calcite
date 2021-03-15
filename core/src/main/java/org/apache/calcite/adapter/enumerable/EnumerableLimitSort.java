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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.adapter.enumerable.EnumerableLimit.getExpression;

/**
 * Implementation of {@link org.apache.calcite.rel.core.Sort} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 * It optimizes sorts that have a limit and an optional offset.
 */
public class EnumerableLimitSort extends Sort implements EnumerableRel {

  /**
   * Creates an EnumerableLimitSort.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public EnumerableLimitSort(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    super(cluster, traitSet, input, collation, offset, fetch);
    assert this.getConvention() instanceof EnumerableConvention;
    assert this.getConvention() == input.getConvention();
  }

  /** Creates an EnumerableLimitSort. */
  public static EnumerableLimitSort create(
      RelNode input,
      RelCollation collation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(EnumerableConvention.INSTANCE).replace(
        collation);
    return new EnumerableLimitSort(cluster, traitSet, input, collation, offset, fetch);
  }

  @Override public EnumerableLimitSort copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      @Nullable RexNode offset,
      @Nullable RexNode fetch) {
    return new EnumerableLimitSort(
        this.getCluster(),
        traitSet,
        newInput,
        newCollation,
        offset,
        fetch);
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) this.getInput();
    final Result result = implementor.visitChild(this, 0, child, pref);
    final PhysType physType = PhysTypeImpl.of(
        implementor.getTypeFactory(),
        this.getRowType(),
        result.format);
    final Expression childExp = builder.append("child", result.block);

    final PhysType inputPhysType = result.physType;
    final Pair<Expression, Expression> pair =
        inputPhysType.generateCollationKey(this.collation.getFieldCollations());

    final Expression fetchVal;
    if (this.fetch == null) {
      fetchVal = Expressions.constant(Integer.valueOf(Integer.MAX_VALUE));
    } else {
      fetchVal = getExpression(this.fetch);
    }

    final Expression offsetVal = this.offset == null ? Expressions.constant(Integer.valueOf(0))
        : getExpression(this.offset);

    builder.add(
        Expressions.return_(
            null, Expressions.call(
                BuiltInMethod.ORDER_BY_WITH_FETCH_AND_OFFSET.method, Expressions.list(
                    childExp,
                    builder.append("keySelector", pair.left))
                    .appendIfNotNull(builder.appendIfNotNull("comparator", pair.right))
                    .appendIfNotNull(
                        builder.appendIfNotNull("offset",
                            Expressions.constant(offsetVal)))
                    .appendIfNotNull(
                        builder.appendIfNotNull("fetch",
                            Expressions.constant(fetchVal)))
            )));
    return implementor.result(physType, builder.toBlock());
  }
}
