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
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ExpressionType;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.linq4j.tree.UnaryExpression;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.FlatLists;
import org.apache.calcite.runtime.Unit;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.List;

/**
 * How a row is represented as a Java value.
 */
public enum JavaRowFormat {
  CUSTOM {
    Type javaRowClass(
        JavaTypeFactory typeFactory,
        RelDataType type) {
      assert type.getFieldCount() > 1;
      return typeFactory.getJavaClass(type);
    }

    @Override Type javaFieldClass(JavaTypeFactory typeFactory, RelDataType type,
        int index) {
      return typeFactory.getJavaClass(type.getFieldList().get(index).getType());
    }

    public Expression record(
        Type javaRowClass, List<Expression> expressions) {
      switch (expressions.size()) {
      case 0:
        assert javaRowClass == Unit.class;
        return Expressions.field(null, javaRowClass, "INSTANCE");
      default:
        return Expressions.new_(javaRowClass, expressions);
      }
    }

    @Override public MemberExpression field(Expression expression, int field,
        Type fieldType) {
      final Type type = expression.getType();
      if (type instanceof Types.RecordType) {
        Types.RecordType recordType = (Types.RecordType) type;
        Types.RecordField recordField =
            recordType.getRecordFields().get(field);
        return Expressions.field(
            expression,
            recordField.getDeclaringClass(),
            recordField.getName());
      } else {
        return Expressions.field(expression, Types.nthField(field, type));
      }
    }
  },

  SCALAR {
    Type javaRowClass(
        JavaTypeFactory typeFactory,
        RelDataType type) {
      assert type.getFieldCount() == 1;
      return typeFactory.getJavaClass(
          type.getFieldList().get(0).getType());
    }

    @Override Type javaFieldClass(JavaTypeFactory typeFactory, RelDataType type,
        int index) {
      return javaRowClass(typeFactory, type);
    }

    public Expression record(
        Type javaRowClass, List<Expression> expressions) {
      assert expressions.size() == 1;
      return expressions.get(0);
    }

    @Override public Expression field(Expression expression, int field,
        Type fieldType) {
      assert field == 0;
      return expression;
    }
  },

  /** A list that is comparable and immutable. Useful for records with 0 fields
   * (empty list is a good singleton) but sometimes also for records with 2 or
   * more fields that you need to be comparable, say as a key in a lookup. */
  LIST {
    Type javaRowClass(
        JavaTypeFactory typeFactory,
        RelDataType type) {
      return FlatLists.ComparableList.class;
    }

    @Override Type javaFieldClass(JavaTypeFactory typeFactory, RelDataType type,
        int index) {
      return Object.class;
    }

    public Expression record(
        Type javaRowClass, List<Expression> expressions) {
      switch (expressions.size()) {
      case 0:
        return Expressions.field(
          null,
          FlatLists.class,
          "COMPARABLE_EMPTY_LIST");
      case 2:
        return Expressions.convert_(
            Expressions.call(
                List.class,
                null,
                BuiltInMethod.LIST2.method,
                expressions),
            List.class);
      case 3:
        return Expressions.convert_(
            Expressions.call(
                List.class,
                null,
                BuiltInMethod.LIST3.method,
                expressions),
            List.class);
      default:
        return Expressions.convert_(
            Expressions.call(
                List.class,
                null,
                BuiltInMethod.LIST_N.method,
                Expressions.newArrayInit(
                    Object.class,
                    expressions)),
            List.class);
      }
    }

    @Override public Expression field(Expression expression, int field,
        Type fieldType) {
      return RexToLixTranslator.convert(
          Expressions.call(expression,
              BuiltInMethod.LIST_GET.method,
              Expressions.constant(field)),
          fieldType);
    }
  },

  /**
   * See {@link org.apache.calcite.interpreter.Row}
   */
  ROW {
    @Override
    Type javaRowClass(JavaTypeFactory typeFactory, RelDataType type) {
      return Row.class;
    }

    @Override
    Type javaFieldClass(JavaTypeFactory typeFactory, RelDataType type,
        int index) {
      return Object.class;
    }

    @Override
    public Expression record(Type javaRowClass, List<Expression> expressions) {
      return Expressions.call(BuiltInMethod.ROW_AS_COPY.method, expressions);
    }

    @Override
    public Expression field(Expression expression, int field, Type fieldType) {
      return RexToLixTranslator.convert(
          Expressions.call(expression,
              BuiltInMethod.ROW_VALUE.method,
              Expressions.constant(field)),
          fieldType);
    }
  },

  ARRAY {
    Type javaRowClass(
        JavaTypeFactory typeFactory,
        RelDataType type) {
      assert type.getFieldCount() > 1;
      return Object[].class;
    }

    @Override Type javaFieldClass(JavaTypeFactory typeFactory, RelDataType type,
        int index) {
      return Object.class;
    }

    public Expression record(
        Type javaRowClass, List<Expression> expressions) {
      return Expressions.newArrayInit(
          Object.class, stripCasts(expressions));
    }

    @Override public Expression comparer() {
      return Expressions.call(BuiltInMethod.ARRAY_COMPARER.method);
    }

    @Override public Expression field(Expression expression, int field,
        Type fieldType) {
      return RexToLixTranslator.convert(
          Expressions.arrayIndex(expression, Expressions.constant(field)),
          fieldType);
    }
  };

  public JavaRowFormat optimize(RelDataType rowType) {
    switch (rowType.getFieldCount()) {
    case 0:
      return LIST;
    case 1:
      return SCALAR;
    default:
      if (this == SCALAR) {
        return LIST;
      }
      return this;
    }
  }

  abstract Type javaRowClass(JavaTypeFactory typeFactory, RelDataType type);

  /**
   * Returns the java class that is used to physically store the given field.
   * For instance, a non-null int field can still be stored in a field of type
   * {@code Object.class} in {@link JavaRowFormat#ARRAY} case.
   *
   * @param typeFactory type factory to resolve java types
   * @param type row type
   * @param index field index
   * @return java type used to store the field
   */
  abstract Type javaFieldClass(JavaTypeFactory typeFactory, RelDataType type,
      int index);

  public abstract Expression record(
      Type javaRowClass, List<Expression> expressions);

  private static List<Expression> stripCasts(
      final List<Expression> expressions) {
    return new AbstractList<Expression>() {
      public Expression get(int index) {
        Expression expression = expressions.get(index);
        while (expression.getNodeType() == ExpressionType.Convert) {
          expression = ((UnaryExpression) expression).expression;
        }
        return expression;
      }

      public int size() {
        return expressions.size();
      }
    };
  }

  public Expression comparer() {
    return null;
  }

  public abstract Expression field(Expression expression, int field,
      Type fieldType);
}

// End JavaRowFormat.java
