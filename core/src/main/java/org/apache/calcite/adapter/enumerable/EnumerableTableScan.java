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
import org.apache.calcite.interpreter.Row;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.collect.ImmutableList;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/** Implementation of {@link org.apache.calcite.rel.core.TableScan} in
 * {@link org.apache.calcite.adapter.enumerable.EnumerableConvention enumerable calling convention}. */
public class EnumerableTableScan
    extends TableScan
    implements EnumerableRel {
  private final Class elementType;

  /** Creates an EnumerableTableScan.
   *
   * <p>Use {@link #create} unless you know what you are doing. */
  public EnumerableTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      RelOptTable table, Class elementType) {
    super(cluster, traitSet, table);
    assert getConvention() instanceof EnumerableConvention;
    this.elementType = elementType;
  }

  /** Creates an EnumerableTableScan. */
  public static EnumerableTableScan create(RelOptCluster cluster,
      RelOptTable relOptTable) {
    final Table table = relOptTable.unwrap(Table.class);
    Class elementType = EnumerableTableScan.deduceElementType(table);
    final RelTraitSet traitSet =
        cluster.traitSetOf(EnumerableConvention.INSTANCE)
            .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
              if (table != null) {
                return table.getStatistic().getCollations();
              }
              return ImmutableList.of();
            });
    return new EnumerableTableScan(cluster, traitSet, relOptTable, elementType);
  }

  /** Returns whether EnumerableTableScan can generate code to handle a
   * particular variant of the Table SPI. */
  public static boolean canHandle(Table table) {
    // FilterableTable and ProjectableFilterableTable cannot be handled in
    // enumerable convention because they might reject filters and those filters
    // would need to be handled dynamically.
    return table instanceof QueryableTable
        || table instanceof ScannableTable;
  }

  public static Class deduceElementType(Table table) {
    if (table instanceof QueryableTable) {
      final QueryableTable queryableTable = (QueryableTable) table;
      final Type type = queryableTable.getElementType();
      if (type instanceof Class) {
        return (Class) type;
      } else {
        return Object[].class;
      }
    } else if (table instanceof ScannableTable
        || table instanceof FilterableTable
        || table instanceof ProjectableFilterableTable
        || table instanceof StreamableTable) {
      return Object[].class;
    } else {
      return Object.class;
    }
  }

  public static JavaRowFormat deduceFormat(RelOptTable table) {
    final Class elementType = deduceElementType(table.unwrap(Table.class));
    return elementType == Object[].class
        ? JavaRowFormat.ARRAY
        : JavaRowFormat.CUSTOM;
  }

  private Expression getExpression(PhysType physType) {
    final Expression expression = table.getExpression(Queryable.class);
    final Expression expression2 = toEnumerable(expression);
    assert Types.isAssignableFrom(Enumerable.class, expression2.getType());
    return toRows(physType, expression2);
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
    if (physType.getFormat() == JavaRowFormat.SCALAR
        && Object[].class.isAssignableFrom(elementType)
        && getRowType().getFieldCount() == 1
        && (table.unwrap(ScannableTable.class) != null
            || table.unwrap(FilterableTable.class) != null
            || table.unwrap(ProjectableFilterableTable.class) != null)) {
      return Expressions.call(BuiltInMethod.SLICE0.method, expression);
    }
    JavaRowFormat oldFormat = format();
    if (physType.getFormat() == oldFormat && !hasCollectionField(rowType)) {
      return expression;
    }
    final ParameterExpression row_ =
        Expressions.parameter(elementType, "row");
    final int fieldCount = table.getRowType().getFieldCount();
    List<Expression> expressionList = new ArrayList<>(fieldCount);
    for (int i = 0; i < fieldCount; i++) {
      expressionList.add(fieldExpression(row_, i, physType, oldFormat));
    }
    return Expressions.call(expression,
        BuiltInMethod.SELECT.method,
        Expressions.lambda(Function1.class, physType.record(expressionList),
            row_));
  }

  private Expression fieldExpression(ParameterExpression row_, int i,
      PhysType physType, JavaRowFormat format) {
    final Expression e =
        format.field(row_, i, null, physType.getJavaFieldType(i));
    final RelDataType relFieldType =
        physType.getRowType().getFieldList().get(i).getType();
    switch (relFieldType.getSqlTypeName()) {
    case ARRAY:
    case MULTISET:
      // We can't represent a multiset or array as a List<Employee>, because
      // the consumer does not know the element type.
      // The standard element type is List.
      // We need to convert to a List<List>.
      final JavaTypeFactory typeFactory =
          (JavaTypeFactory) getCluster().getTypeFactory();
      final PhysType elementPhysType = PhysTypeImpl.of(
          typeFactory, relFieldType.getComponentType(), JavaRowFormat.CUSTOM);
      final MethodCallExpression e2 =
          Expressions.call(BuiltInMethod.AS_ENUMERABLE2.method, e);
      final Expression e3 = elementPhysType.convertTo(e2, JavaRowFormat.LIST);
      return Expressions.call(e3, BuiltInMethod.ENUMERABLE_TO_LIST.method);
    default:
      return e;
    }
  }

  private JavaRowFormat format() {
    int fieldCount = getRowType().getFieldCount();
    if (fieldCount == 0) {
      return JavaRowFormat.LIST;
    }
    if (Object[].class.isAssignableFrom(elementType)) {
      return fieldCount == 1 ? JavaRowFormat.SCALAR : JavaRowFormat.ARRAY;
    }
    if (Row.class.isAssignableFrom(elementType)) {
      return JavaRowFormat.ROW;
    }
    if (fieldCount == 1 && (Object.class == elementType
          || Primitive.is(elementType)
          || Number.class.isAssignableFrom(elementType))) {
      return JavaRowFormat.SCALAR;
    }
    return JavaRowFormat.CUSTOM;
  }

  private boolean hasCollectionField(RelDataType rowType) {
    for (RelDataTypeField field : rowType.getFieldList()) {
      switch (field.getType().getSqlTypeName()) {
      case ARRAY:
      case MULTISET:
        return true;
      }
    }
    return false;
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new EnumerableTableScan(getCluster(), traitSet, table, elementType);
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
