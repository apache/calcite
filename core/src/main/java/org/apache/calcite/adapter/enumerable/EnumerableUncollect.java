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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.SqlFunctions.FlatProductInputType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.calcite.sql.type.NonNullableAccessors.getComponentTypeOrThrow;

/** Implementation of {@link org.apache.calcite.rel.core.Uncollect} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableUncollect extends Uncollect implements EnumerableRel {
  @Deprecated // to be removed before 2.0
  public EnumerableUncollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child) {
    this(cluster, traitSet, child, false);
  }

  /** Creates an EnumerableUncollect.
   *
   * <p>Use {@link #create} unless you know what you're doing. */
  public EnumerableUncollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child, boolean withOrdinality) {
    super(cluster, traitSet, child, withOrdinality, Collections.emptyList());
    assert getConvention() instanceof EnumerableConvention;
    assert getConvention() == child.getConvention();
  }

  /** Creates an EnumerableUncollect with explicit control over which input
   * fields are passed through unchanged and which are unnested.
   *
   * <p>Use {@link #create} unless you know what you're doing. */
  public EnumerableUncollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child, boolean withOrdinality, ImmutableBitSet passthroughFieldIndices,
      ImmutableBitSet collectionFieldIndices, boolean outer) {
    super(cluster, traitSet, child, withOrdinality, Collections.emptyList(),
        passthroughFieldIndices, collectionFieldIndices, outer);
    assert getConvention() instanceof EnumerableConvention;
    assert getConvention() == child.getConvention();
  }

  /**
   * Creates an EnumerableUncollect.
   *
   * <p>Each field of the input relational expression must be an array or
   * multiset.
   *
   * @param traitSet Trait set
   * @param input    Input relational expression
   * @param withOrdinality Whether output should contain an ORDINALITY column
   */
  public static EnumerableUncollect create(RelTraitSet traitSet, RelNode input,
      boolean withOrdinality) {
    final RelOptCluster cluster = input.getCluster();
    return new EnumerableUncollect(cluster, traitSet, input, withOrdinality);
  }

  /**
   * Creates an EnumerableUncollect that unnests the collection-typed fields at
   * {@code collectionFieldIndices}, passing through the fields at
   * {@code passthroughFieldIndices} unchanged and dropping all others.
   *
   * @param traitSet                Trait set
   * @param input                   Input relational expression
   * @param withOrdinality          Whether output should contain an ORDINALITY column
   * @param passthroughFieldIndices Fields from the input copied unchanged to the output
   * @param collectionFieldIndices  Fields from the input which are unnested
   * @param outer                   if true, preserves input rows with null/empty collections
   *                                (LEFT JOIN); if false, drops them (INNER)
   */
  public static EnumerableUncollect create(RelTraitSet traitSet, RelNode input,
      boolean withOrdinality, ImmutableBitSet passthroughFieldIndices,
      ImmutableBitSet collectionFieldIndices, boolean outer) {
    final RelOptCluster cluster = input.getCluster();
    return new EnumerableUncollect(cluster, traitSet, input, withOrdinality,
        passthroughFieldIndices, collectionFieldIndices, outer);
  }

  @Override public EnumerableUncollect copy(RelTraitSet traitSet,
      RelNode newInput) {
    return new EnumerableUncollect(getCluster(), traitSet, newInput, withOrdinality,
        getPassthroughFieldIndices(), getCollectionFieldIndices(), isOuter);
  }

  /** Implements Uncollect: a subset of input fields (possibly all of them)
   * are unnested, the rest are either passed through unchanged or dropped,
   * and (if {@link #isOuter}) rows with null/empty collections are preserved
   * with NULL-padded element columns. */
  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final ImmutableBitSet passthroughIndices = getPassthroughFieldIndices();
    final ImmutableBitSet collIndices = getCollectionFieldIndices();
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInput();
    final Result childResult = implementor.visitChild(this, 0, child, pref);
    final PhysType physType =
        PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), JavaRowFormat.LIST);

    // Compute element width and kind for each collection field.
    final List<RelDataTypeField> inputFields =
        getInput().getRowType().getFieldList();
    final List<Integer> fieldCounts = new ArrayList<>();
    final List<FlatProductInputType> inputTypes = new ArrayList<>();
    for (int idx : collIndices) {
      final RelDataType ft = inputFields.get(idx).getType();
      if (ft instanceof MapSqlType) {
        fieldCounts.add(2);
        inputTypes.add(FlatProductInputType.MAP);
      } else {
        final RelDataType ct = getComponentTypeOrThrow(ft);
        if (ct.isStruct()) {
          fieldCounts.add(ct.getFieldCount());
          inputTypes.add(FlatProductInputType.LIST);
        } else {
          fieldCounts.add(-1);
          inputTypes.add(FlatProductInputType.SCALAR);
        }
      }
    }

    final Expression childExpression = builder.append("child", childResult.block);

    // SqlFunctions.flatUncollect(passthroughIndices, collectionIndices,
    //     fieldCounts, inputTypes, withOrdinality, inputFieldCount, outer)
    final Expression flatUncollectFn =
        Expressions.call(BuiltInMethod.FLAT_UNCOLLECT.method,
            Expressions.constant(passthroughIndices.toArray()),
            Expressions.constant(collIndices.toArray()),
            // Following converts from a List<Integer> to int[] (not Integer[]).
            Expressions.constant(fieldCounts.stream().mapToInt(i -> i).toArray()),
            Expressions.constant(
                inputTypes.toArray(new FlatProductInputType[0])),
            Expressions.constant(withOrdinality),
            Expressions.constant(inputFields.size()),
            Expressions.constant(isOuter));

    builder.add(
        Expressions.return_(null,
            Expressions.call(childExpression,
                BuiltInMethod.SELECT_MANY.method,
                flatUncollectFn)));

    return implementor.result(physType, builder.toBlock());
  }
}
