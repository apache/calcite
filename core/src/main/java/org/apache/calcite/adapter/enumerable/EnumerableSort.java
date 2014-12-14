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

/** Implementation of {@link org.apache.calcite.rel.core.Sort} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableSort extends Sort implements EnumerableRel {
  /**
   * Creates an EnumerableSort.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   */
  public EnumerableSort(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    super(cluster, traitSet, input, collation, offset, fetch);
    assert getConvention() instanceof EnumerableConvention;
    assert getConvention() == input.getConvention();
  }

  /** Creates an EnumerableSort. */
  public static EnumerableSort create(RelNode child, RelCollation collation,
      RexNode offset, RexNode fetch) {
    final RelOptCluster cluster = child.getCluster();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replace(collation);
    return new EnumerableSort(cluster, traitSet, child, collation, offset,
        fetch);
  }

  @Override public EnumerableSort copy(
      RelTraitSet traitSet,
      RelNode newInput,
      RelCollation newCollation,
      RexNode offset,
      RexNode fetch) {
    return new EnumerableSort(getCluster(), traitSet, newInput, newCollation,
        offset, fetch);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInput();
    final Result result = implementor.visitChild(this, 0, child, pref);
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            result.format);
    Expression childExp =
        builder.append("child", result.block);

    PhysType inputPhysType = result.physType;
    final Pair<Expression, Expression> pair =
        inputPhysType.generateCollationKey(
            collation.getFieldCollations());

    builder.add(
        Expressions.return_(null,
            Expressions.call(childExp,
                BuiltInMethod.ORDER_BY.method,
                Expressions.list(
                    builder.append("keySelector", pair.left))
                    .appendIfNotNull(
                        builder.appendIfNotNull("comparator", pair.right)))));
    return implementor.result(physType, builder.toBlock());
  }
}

// End EnumerableSort.java
