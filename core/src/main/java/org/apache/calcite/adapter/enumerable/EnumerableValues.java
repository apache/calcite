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
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.Values} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableValues extends Values implements EnumerableRel {
  /** Creates an EnumerableValues. */
  private EnumerableValues(RelOptCluster cluster, RelDataType rowType,
      ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet) {
    super(cluster, rowType, tuples, traitSet);
  }

  /** Creates an EnumerableValues. */
  public static EnumerableValues create(RelOptCluster cluster,
      final RelDataType rowType,
      final ImmutableList<ImmutableList<RexLiteral>> tuples) {
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.values(mq, rowType, tuples))
            .replaceIf(RelDistributionTraitDef.INSTANCE,
                () -> RelMdDistribution.values(rowType, tuples));
    return new EnumerableValues(cluster, rowType, tuples, traitSet);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    assert inputs.isEmpty();
    return create(getCluster(), rowType, tuples);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
/*
          return Linq4j.asEnumerable(
              new Object[][] {
                  new Object[] {1, 2},
                  new Object[] {3, 4}
              });
*/
    final JavaTypeFactory typeFactory =
        (JavaTypeFactory) getCluster().getTypeFactory();
    final BlockBuilder builder = new BlockBuilder();
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.preferCustom());
    final Type rowClass = physType.getJavaRowType();

    final List<Expression> expressions = new ArrayList<>();
    final List<RelDataTypeField> fields = rowType.getFieldList();
    for (List<RexLiteral> tuple : tuples) {
      final List<Expression> literals = new ArrayList<>();
      for (Pair<RelDataTypeField, RexLiteral> pair
          : Pair.zip(fields, tuple)) {
        literals.add(
            RexToLixTranslator.translateLiteral(
                pair.right,
                pair.left.getType(),
                typeFactory,
                RexImpTable.NullAs.NULL));
      }
      expressions.add(physType.record(literals));
    }
    builder.add(
        Expressions.return_(
            null,
            Expressions.call(
                BuiltInMethod.AS_ENUMERABLE.method,
                Expressions.newArrayInit(
                    Primitive.box(rowClass), expressions))));
    return implementor.result(physType, builder.toBlock());
  }
}

// End EnumerableValues.java
