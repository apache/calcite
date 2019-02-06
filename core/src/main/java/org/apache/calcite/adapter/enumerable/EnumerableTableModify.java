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
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.TableModify} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableTableModify extends TableModify
    implements EnumerableRel {
  public EnumerableTableModify(RelOptCluster cluster, RelTraitSet traits,
      RelOptTable table, Prepare.CatalogReader catalogReader, RelNode child,
      Operation operation, List<String> updateColumnList,
      List<RexNode> sourceExpressionList, boolean flattened) {
    super(cluster, traits, table, catalogReader, child, operation,
        updateColumnList, sourceExpressionList, flattened);
    assert child.getConvention() instanceof EnumerableConvention;
    assert getConvention() instanceof EnumerableConvention;
    final ModifiableTable modifiableTable =
        table.unwrap(ModifiableTable.class);
    if (modifiableTable == null) {
      throw new AssertionError(); // TODO: user error in validator
    }
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableTableModify(
        getCluster(),
        traitSet,
        getTable(),
        getCatalogReader(),
        sole(inputs),
        getOperation(),
        getUpdateColumnList(),
        getSourceExpressionList(),
        isFlattened());
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    final BlockBuilder builder = new BlockBuilder();
    final Result result = implementor.visitChild(
        this, 0, (EnumerableRel) getInput(), pref);
    Expression childExp =
        builder.append(
            "child", result.block);
    final ParameterExpression collectionParameter =
        Expressions.parameter(Collection.class,
            builder.newName("collection"));
    final Expression expression = table.getExpression(ModifiableTable.class);
    assert expression != null; // TODO: user error in validator
    assert ModifiableTable.class.isAssignableFrom(
        Types.toClass(expression.getType())) : expression.getType();
    builder.add(
        Expressions.declare(
            Modifier.FINAL,
            collectionParameter,
            Expressions.call(
                expression,
                BuiltInMethod.MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION
                    .method)));
    final Expression countParameter =
        builder.append(
            "count",
            Expressions.call(collectionParameter, "size"),
            false);
    Expression convertedChildExp;
    if (!getInput().getRowType().equals(getRowType())) {
      final JavaTypeFactory typeFactory =
          (JavaTypeFactory) getCluster().getTypeFactory();
      final JavaRowFormat format = EnumerableTableScan.deduceFormat(table);
      PhysType physType =
          PhysTypeImpl.of(typeFactory, table.getRowType(), format);
      List<Expression> expressionList = new ArrayList<>();
      final PhysType childPhysType = result.physType;
      final ParameterExpression o_ =
          Expressions.parameter(childPhysType.getJavaRowType(), "o");
      final int fieldCount =
          childPhysType.getRowType().getFieldCount();
      for (int i = 0; i < fieldCount; i++) {
        expressionList.add(
            childPhysType.fieldReference(o_, i, physType.getJavaFieldType(i)));
      }
      convertedChildExp =
          builder.append(
              "convertedChild",
              Expressions.call(
                  childExp,
                  BuiltInMethod.SELECT.method,
                  Expressions.lambda(
                      physType.record(expressionList), o_)));
    } else {
      convertedChildExp = childExp;
    }
    final Method method;
    switch (getOperation()) {
    case INSERT:
      method = BuiltInMethod.INTO.method;
      break;
    case DELETE:
      method = BuiltInMethod.REMOVE_ALL.method;
      break;
    default:
      throw new AssertionError(getOperation());
    }
    builder.add(
        Expressions.statement(
            Expressions.call(
                convertedChildExp, method, collectionParameter)));
    final Expression updatedCountParameter =
        builder.append(
            "updatedCount",
            Expressions.call(collectionParameter, "size"),
            false);
    builder.add(
        Expressions.return_(
            null,
            Expressions.call(
                BuiltInMethod.SINGLETON_ENUMERABLE.method,
                Expressions.convert_(
                    Expressions.condition(
                        Expressions.greaterThanOrEqual(
                            updatedCountParameter, countParameter),
                        Expressions.subtract(
                            updatedCountParameter, countParameter),
                        Expressions.subtract(
                            countParameter, updatedCountParameter)),
                    long.class))));
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref == Prefer.ARRAY
                ? JavaRowFormat.ARRAY : JavaRowFormat.SCALAR);
    return implementor.result(physType, builder.toBlock());
  }

}

// End EnumerableTableModify.java
