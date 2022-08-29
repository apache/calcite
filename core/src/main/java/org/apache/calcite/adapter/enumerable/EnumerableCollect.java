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

    // final Enumerable child = <<child adapter>>;
    // final Enumerable<Object[]> converted = child.select(<<conversion code>>);
    // final List<Object[]> list = converted.toList();
    Expression child_ =
        builder.append(
            "child", result.block);

    RelDataType collectionComponentType =
        requireNonNull(rowType().getFieldList().get(0).getType().getComponentType());
    RelDataType childRecordType = result.physType.getRowType().getFieldList().get(0).getType();

    Expression conv_ = child_;
    if (!SqlTypeUtil.sameNamedType(collectionComponentType, childRecordType)) {
      // In the internal representation of multisets , every element must be a record. In case the
      // result above is a scalar type we have to wrap it around a physical type capable of
      // representing records. For this reason the following conversion is necessary.
      // REVIEW zabetak January 7, 2019: If we can ensure that the input to this operator
      // has the correct physical type (e.g., respecting the Prefer.ARRAY above)
      // then this conversion can be removed.
      conv_ =
          builder.append(
              "converted", result.physType.convertTo(child_, JavaRowFormat.ARRAY));
    }

    Expression list_ =
        builder.append("list",
            Expressions.call(conv_,
                BuiltInMethod.ENUMERABLE_TO_LIST.method));

    builder.add(
        Expressions.return_(null,
            Expressions.call(
                BuiltInMethod.SINGLETON_ENUMERABLE.method, list_)));
    return implementor.result(physType, builder.toBlock());
  }
}
