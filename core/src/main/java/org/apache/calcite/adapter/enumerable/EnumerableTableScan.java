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

import org.apache.calcite.interpreter.Row;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.TableScan} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableTableScan
    extends TableScan
    implements EnumerableRel {
  private final Class elementType;

  public EnumerableTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, Class elementType) {
    super(cluster, traitSet, table);
    assert getConvention() instanceof EnumerableConvention;
    this.elementType = elementType;
  }

  private Expression getExpression(PhysType physType) {
    final Expression expression = table.getExpression(Queryable.class);
    final Expression expression2 = toEnumerable(expression);
    assert Types.isAssignableFrom(Enumerable.class, expression2.getType());
    Expression expression3 = toRows(physType, expression2);
    return expression3;
  }

  private Expression toEnumerable(Expression expression) {
    final Type type = expression.getType();
    if (Types.isArray(type)) {
      if (Types.toClass(type).getComponentType().isPrimitive()) {
        expression =
            Expressions.call(BuiltInMethod.AS_LIST.method, expression);
      }
      return Expressions.call(BuiltInMethod.AS_ENUMERABLE.method, expression);
    } else if (Types.isAssignableFrom(Iterable.class, type)
        && !Types.isAssignableFrom(Enumerable.class, type)) {
      return Expressions.call(BuiltInMethod.AS_ENUMERABLE2.method,
          expression);
    } else if (Types.isAssignableFrom(Queryable.class, type)) {
      // Queryable extends Enumerable, but it's too "clever", so we call
      // Queryable.asEnumerable so that operations such as take(int) will be
      // evaluated directly.
      return Expressions.call(expression,
          BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);
    }
    return expression;
  }

  private Expression toRows(PhysType physType, Expression expression) {
    final ParameterExpression row_ =
        Expressions.parameter(elementType, "row");
    List<Expression> expressionList = new ArrayList<Expression>();
    final int fieldCount = table.getRowType().getFieldCount();
    if (elementType == Row.class) {
      // Convert Enumerable<Row> to Enumerable<SyntheticRecord>
      for (int i = 0; i < fieldCount; i++) {
        expressionList.add(
            RexToLixTranslator.convert(
                Expressions.call(row_,
                    BuiltInMethod.ROW_VALUE.method,
                    Expressions.constant(i)),
                physType.getJavaFieldType(i)));
      }
    } else if (elementType == Object[].class
        && rowType.getFieldCount() == 1) {
      // Convert Enumerable<Object[]> to Enumerable<SyntheticRecord>
      for (int i = 0; i < fieldCount; i++) {
        expressionList.add(
            RexToLixTranslator.convert(
                Expressions.arrayIndex(row_, Expressions.constant(i)),
                physType.getJavaFieldType(i)));
      }
    } else if (elementType == Object.class) {
      if (!(physType.getJavaRowType()
          instanceof JavaTypeFactoryImpl.SyntheticRecordType)) {
        return expression;
      }
      expressionList.add(
          RexToLixTranslator.convert(row_, physType.getJavaFieldType(0)));
    } else {
      return expression;
    }
    return Expressions.call(expression,
        BuiltInMethod.SELECT.method,
        Expressions.lambda(Function1.class, physType.record(expressionList),
            row_));
  }

  private JavaRowFormat format() {
    if (Object[].class.isAssignableFrom(elementType)) {
      return JavaRowFormat.ARRAY;
    } else {
      return JavaRowFormat.CUSTOM;
    }
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableTableScan(getCluster(), traitSet, table,
        elementType);
  }

  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    // Note that representation is ARRAY. This assumes that the table
    // returns a Object[] for each record. Actually a Table<T> can
    // return any type T. And, if it is a JdbcTable, we'd like to be
    // able to generate alternate accessors that return e.g. synthetic
    // records {T0 f0; T1 f1; ...} and don't box every primitive value.
    final PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            format());
    final Expression expression = getExpression(physType);
    return implementor.result(physType, Blocks.toBlock(expression));
  }
}

// End EnumerableTableScan.java
