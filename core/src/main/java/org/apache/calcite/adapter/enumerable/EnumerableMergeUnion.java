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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.Union} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}.
 *
 * <p>Performs a union (or union all) of all its inputs (which must be already
 * sorted), respecting the order. */
public class EnumerableMergeUnion extends EnumerableUnion {

  protected EnumerableMergeUnion(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelNode> inputs, boolean all) {
    super(cluster, traitSet, inputs, all);
    final List<RelCollation> collations = traitSet.getCollations();
    if (collations.isEmpty() || collations.get(0).getFieldCollations().isEmpty()) {
      throw new IllegalArgumentException("EnumerableMergeUnion with no collation");
    }
    for (RelNode input : inputs) {
      final RelTrait inputCollationTrait =
          input.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
      for (RelCollation collation : collations) {
        if (inputCollationTrait == null || !inputCollationTrait.satisfies(collation)) {
          throw new IllegalArgumentException("EnumerableMergeUnion input does "
              + "not satisfy collation. EnumerableMergeUnion collation: "
              + collation + ". Input collation: " + inputCollationTrait + ". Input: "
              + input);
        }
      }
    }
  }

  public static EnumerableMergeUnion create(RelCollation collation,
      List<RelNode> inputs, boolean all) {
    final RelOptCluster cluster = inputs.get(0).getCluster();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE).replace(collation);
    return new EnumerableMergeUnion(cluster, traitSet, inputs, all);
  }

  @Override public EnumerableMergeUnion copy(RelTraitSet traitSet,
      List<RelNode> inputs, boolean all) {
    return new EnumerableMergeUnion(getCluster(), traitSet, inputs, all);
  }

  @Override public Result implement(EnumerableRelImplementor implementor,
      Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();

    final ParameterExpression inputListExp =
        Expressions.parameter(List.class,
            builder.newName("mergeUnionInputs"
                + Integer.toUnsignedString(this.getId())));
    builder.add(
        Expressions.declare(0, inputListExp, Expressions.new_(ArrayList.class)));

    for (Ord<RelNode> ord : Ord.zip(inputs)) {
      final EnumerableRel input = (EnumerableRel) ord.e;
      final Result result = implementor.visitChild(this, ord.i, input, pref);
      final Expression childExp = builder.append("child" + ord.i, result.block);
      builder.add(
          Expressions.statement(
              Expressions.call(inputListExp,
                  BuiltInMethod.COLLECTION_ADD.method, childExp)));
    }

    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(),
            pref.prefer(JavaRowFormat.CUSTOM));

    final RelCollation collation = getTraitSet().getCollation();
    if (collation == null || collation.getFieldCollations().isEmpty()) {
      // should not happen
      throw new IllegalStateException("EnumerableMergeUnion with no collation");
    }
    final Pair<Expression, Expression> pair =
        physType.generateCollationKey(collation.getFieldCollations());
    final Expression sortKeySelector = pair.left;
    final Expression sortComparator = pair.right;

    final Expression equalityComparator =
        Util.first(physType.comparer(),
            Expressions.call(BuiltInMethod.IDENTITY_COMPARER.method));

    final Expression unionExp =
        Expressions.call(BuiltInMethod.MERGE_UNION.method, inputListExp,
            sortKeySelector, sortComparator,
            Expressions.constant(all, boolean.class), equalityComparator);
    builder.add(unionExp);

    return implementor.result(physType, builder.toBlock());
  }
}
