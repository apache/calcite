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

  /** Creates an EnumerableUncollect, with explicit control over which input
   * fields are passed through unchanged and which are unnested.
   *
   * <p>Use {@link #create} unless you know what you're doing. */
  public EnumerableUncollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child, boolean withOrdinality, ImmutableBitSet passthroughFieldIndices,
      ImmutableBitSet collectionFieldIndices, boolean expandStructFields,
      boolean isOuter) {
    super(cluster, traitSet, child, withOrdinality, Collections.emptyList(),
        passthroughFieldIndices, collectionFieldIndices, expandStructFields,
        isOuter);
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
   * @param passthroughFieldIndices 0-based indices of the input fields to pass through
   *                                unchanged
   * @param collectionFieldIndices  0-based indices of the input fields whose values are
   *                                collections to unnest
   * @param expandStructFields      If true, a collection whose element type is a struct
   *                                produces one output column per struct field; if false,
   *                                a single column typed as the whole element
   * @param isOuter                 If true, preserves input rows with null/empty
   *                                collections (LEFT JOIN); if false, drops them (INNER)
   */
  public static EnumerableUncollect create(RelTraitSet traitSet, RelNode input,
      boolean withOrdinality, ImmutableBitSet passthroughFieldIndices,
      ImmutableBitSet collectionFieldIndices, boolean expandStructFields,
      boolean isOuter) {
    final RelOptCluster cluster = input.getCluster();
    return new EnumerableUncollect(cluster, traitSet, input, withOrdinality,
        passthroughFieldIndices, collectionFieldIndices, expandStructFields,
        isOuter);
  }

  @Override public EnumerableUncollect copy(RelTraitSet traitSet,
      RelNode newInput) {
    return new EnumerableUncollect(getCluster(), traitSet, newInput,
        withOrdinality, getPassthroughFieldIndices(), getCollectionFieldIndices(),
        expandStructFields, isOuter);
  }

  /** Implements Uncollect: some of the input fields are unnested,
   * and some of them passed through unchanged.
   * If {@link #isOuter} is true, rows with null/empty collections are preserved
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
      final RelDataType type = inputFields.get(idx).getType();
      if (type instanceof MapSqlType) {
        fieldCounts.add(2);
        inputTypes.add(FlatProductInputType.MAP);
      } else {
        final RelDataType elementType = getComponentTypeOrThrow(type);
        if (elementType.isStruct() && expandStructFields) {
          fieldCounts.add(elementType.getFieldCount());
          inputTypes.add(FlatProductInputType.LIST);
        } else if (elementType.isStruct()) {
          // A struct element kept whole occupies a single output column,
          // like a scalar element, but its row value must be converted from
          // the collection's internal list representation to Object[].
          fieldCounts.add(-1);
          inputTypes.add(FlatProductInputType.STRUCT);
        } else {
          fieldCounts.add(-1);
          inputTypes.add(FlatProductInputType.SCALAR);
        }
      }
    }

    final Expression child_ = builder.append("child", childResult.block);

    // final Enumerable<List<Employee>> child = <<child adapter>>;
    // return child.selectMany(
    //     SqlFunctions.flatUncollect(passthroughIndices, collectionIndices,
    //         fieldCounts, inputTypes, withOrdinality, inputFieldCount, outer));
    final Expression lambda =
        Expressions.call(BuiltInMethod.FLAT_UNCOLLECT.method,
            Expressions.constant(passthroughIndices.toArray()),
            Expressions.constant(collIndices.toArray()),
            Expressions.constant(
                fieldCounts.stream().mapToInt(i -> i).toArray()),
            Expressions.constant(
                inputTypes.toArray(new FlatProductInputType[0])),
            Expressions.constant(withOrdinality),
            Expressions.constant(inputFields.size()),
            Expressions.constant(isOuter));
    builder.add(
        Expressions.return_(null,
            Expressions.call(child_,
                BuiltInMethod.SELECT_MANY.method,
                lambda)));
    return implementor.result(physType, builder.toBlock());
  }
}
