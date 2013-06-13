/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.reltype.RelDataType;

import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.Collections;
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

    public Expression record(
        Type javaRowClass, List<Expression> expressions) {
      return Expressions.new_(javaRowClass, expressions);
    }

    public MemberExpression field(
        Expression expression, int field, Class fieldType) {
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
        return Expressions.field(
            expression, Types.nthField(field, type));
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

    public Expression record(
        Type javaRowClass, List<Expression> expressions) {
      assert expressions.size() == 1;
      return expressions.get(0);
    }

    public Expression field(
        Expression expression, int field, Class fieldType) {
      assert field == 0;
      return expression;
    }
  },

  EMPTY_LIST() {
    Type javaRowClass(
        JavaTypeFactory typeFactory,
        RelDataType type) {
      assert type.getFieldCount() == 0;
      return Collections.EMPTY_LIST.getClass();
    }

    public Expression record(
        Type javaRowClass, List<Expression> expressions) {
      return Expressions.field(
          null,
          Collections.class,
          "EMPTY_LIST");
    }

    @Override
    public Expression field(
        Expression expression, int field, Class fieldType) {
      throw new UnsupportedOperationException();
    }
  },

  ARRAY {
    Type javaRowClass(
        JavaTypeFactory typeFactory,
        RelDataType type) {
      assert type.getFieldCount() > 1;
      return Object[].class;
    }

    public Expression record(
        Type javaRowClass, List<Expression> expressions) {
      return Expressions.newArrayInit(
          Object.class, stripCasts(expressions));
    }

    @Override
    public Expression comparer() {
      return Expressions.call(
          null, BuiltinMethod.ARRAY_COMPARER.method);
    }

    @Override
    public Expression field(Expression expression, int field, Class fieldType) {
      return RexToLixTranslator.convert(
          Expressions.arrayIndex(expression, Expressions.constant(field)),
          fieldType);
    }
  };

  public JavaRowFormat optimize(RelDataType rowType) {
    switch (rowType.getFieldCount()) {
    case 0:
      return EMPTY_LIST;
    case 1:
      return SCALAR;
    default:
      return this;
    }
  }

  abstract Type javaRowClass(JavaTypeFactory typeFactory, RelDataType type);

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

  public abstract Expression field(
      Expression expression, int field, Class fieldType);
}

// End JavaRowFormat.java
