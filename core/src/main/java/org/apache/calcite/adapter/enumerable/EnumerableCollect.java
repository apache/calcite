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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.BuiltInMethod;

import static java.util.Objects.requireNonNull;

/** Implementation of {@link org.apache.calcite.rel.core.Collect} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableCollect extends Collect implements EnumerableRel {
  /**
   * Creates an EnumerableCollect.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster   Cluster
   * @param traitSet  Trait set
   * @param input     Input relational expression
   * @param rowType   Row type
   */
  public EnumerableCollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelDataType rowType) {
    super(cluster, traitSet, input, rowType);
    assert getConvention() instanceof EnumerableConvention;
    assert getConvention() == input.getConvention();
  }

  @Deprecated // to be removed before 2.0
  public EnumerableCollect(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, String fieldName) {
    this(cluster, traitSet, input,
        deriveRowType(cluster.getTypeFactory(), SqlTypeName.MULTISET, fieldName,
            input.getRowType()));
  }

  /**
   * Creates an EnumerableCollect.
   *
   * @param input          Input relational expression
   * @param rowType        Row type
   */
  public static Collect create(RelNode input, RelDataType rowType) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet =
        cluster.traitSet().replace(EnumerableConvention.INSTANCE);
    return new EnumerableCollect(cluster, traitSet, input, rowType);
  }

  @Override public EnumerableCollect copy(RelTraitSet traitSet,
      RelNode newInput) {
    return new EnumerableCollect(getCluster(), traitSet, newInput, rowType());
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final EnumerableRel child = (EnumerableRel) getInput();
    // REVIEW zabetak January 7, 2019: Even if we ask the implementor to provide a result
    // where records are represented as arrays (Prefer.ARRAY) this may not be respected.
    final Result result = implementor.visitChild(this, 0, child, Prefer.ARRAY);
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            JavaRowFormat.LIST);

    final SqlTypeName collectionType = getCollectionType();

    // final Enumerable child = <<child adapter>>;
    // final Enumerable<Object[]> converted = child.select(<<conversion code>>);
    // if collectionType is ARRAY or MULTISET: final List<Object[]> list = converted.toList();
    // if collectionType is MAP:               final Map<Object, Object> map = converted.toMap();
    Expression child_ =
        builder.append(
            "child", result.block);

    Expression conv_ = child_;
    Expression collectionExpr;
    switch (collectionType) {
    case ARRAY:
    case MULTISET:
      RelDataType collectionComponentType =
          requireNonNull(rowType().getFieldList().get(0).getType().getComponentType());
      RelDataType childRecordType = result.physType.getRowType().getFieldList().get(0).getType();

      if (!SqlTypeUtil.sameNamedType(collectionComponentType, childRecordType)) {
        // In the internal representation of multisets, every element must be a record.
        // In case the result above is a scalar type we have to wrap it around a
        // physical type capable of representing records.
        // For ARRAY type with a single field, we use SCALAR format to avoid
        // unnecessary wrapping, which allows correct comparison semantics.
        // REVIEW zabetak January 7, 2019: If we can ensure that the input to this operator
        // has the correct physical type (e.g., respecting the Prefer.ARRAY above)
        // then this conversion can be removed.
        JavaRowFormat targetFormat =
            collectionType == SqlTypeName.ARRAY && child.getRowType().getFieldCount() == 1
                ? JavaRowFormat.SCALAR
                : JavaRowFormat.ARRAY;
        conv_ =
            builder.append(
                "converted", result.physType.convertTo(child_, targetFormat));
      }

      collectionExpr =
          builder.append("list",
              Expressions.call(conv_,
                  BuiltInMethod.ENUMERABLE_TO_LIST.method));
      break;
    case MAP:
      // Convert input 'Object[]' to MAP data, we don't specify a comparator, just
      // keep the original order of this map. (the inner map is a LinkedHashMap)
      ParameterExpression input = Expressions.parameter(Object.class, "input");

      // keySelector lambda: input -> ((Object[])input)[0]
      Expression keySelector =
          Expressions.lambda(
              Expressions.arrayIndex(Expressions.convert_(input, Object[].class),
                  Expressions.constant(0)), input);

      // valueSelector lambda: input -> ((Object[])input)[1]
      Expression valueSelector =
          Expressions.lambda(
              Expressions.arrayIndex(Expressions.convert_(input, Object[].class),
                  Expressions.constant(1)), input);

      collectionExpr =
          builder.append("map",
              Expressions.call(conv_,
                  BuiltInMethod.ENUMERABLE_TO_MAP.method, keySelector, valueSelector));
      break;
    default:
      throw new IllegalArgumentException("unknown collection type " + collectionType);
    }

    builder.add(
        Expressions.return_(null,
            Expressions.call(
                BuiltInMethod.SINGLETON_ENUMERABLE.method, collectionExpr)));

    return implementor.result(physType, builder.toBlock());
  }
}
