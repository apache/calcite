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
package net.hydromatic.linq4j.expressions;

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Functions;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;

/**
 * Represents an expression that has a constant value.
 */
public class ConstantExpression extends Expression {
  public final Object value;

  public ConstantExpression(Type type, Object value) {
    super(ExpressionType.Constant, type);
    this.value = value;
    if (value != null) {
      if (type instanceof Class) {
        Class clazz = (Class) type;
        Primitive primitive = Primitive.of(clazz);
        if (primitive != null) {
          clazz = primitive.boxClass;
        }
        if (!clazz.isInstance(value)) {
          throw new AssertionError(
              "value " + value + " does not match type " + type);
        }
      }
    }
  }

  public Object evaluate(Evaluator evaluator) {
    return value;
  }

  @Override
  public Expression accept(Visitor visitor) {
    return visitor.visit(this);
  }

  @Override
  void accept(ExpressionWriter writer, int lprec, int rprec) {
    if (value == null) {
      if (!writer.requireParentheses(this, lprec, rprec)) {
        writer.append("(").append(type).append(") null");
      }
      return;
    }
    write(writer, value, type);
  }

  private static ExpressionWriter write(ExpressionWriter writer,
      final Object value, Type type) {
    if (value == null) {
      return writer.append("null");
    }
    if (type == null) {
      type = value.getClass();
      if (Primitive.isBox(type)) {
        type = Primitive.ofBox(type).primitiveClass;
      }
    }
    if (value instanceof String) {
      escapeString(writer.getBuf(), (String) value);
      return writer;
    }
    final Primitive primitive = Primitive.of(type);
    if (primitive != null) {
      switch (primitive) {
      case BYTE:
        return writer.append("(byte)").append(((Byte) value).intValue());
      case CHAR:
        return writer.append("(char)")
            .append((int) ((Character) value).charValue());
      case SHORT:
        return writer.append("(short)").append(((Short) value).intValue());
      case LONG:
        return writer.append(value).append("L");
      case FLOAT:
        return writer.append(value).append("F");
      case DOUBLE:
        return writer.append(value).append("D");
      default:
        return writer.append(value);
      }
    }
    final Primitive primitive2 = Primitive.ofBox(type);
    if (primitive2 != null) {
      writer.append(primitive2.boxClass.getSimpleName() + ".valueOf(");
      write(writer, value, primitive2.primitiveClass);
      return writer.append(")");
    }
    if (value instanceof Enum) {
      return writer.append(((Enum) value).getDeclaringClass())
          .append('.')
          .append(((Enum) value).name());
    }
    if (value instanceof BigDecimal) {
      BigDecimal bigDecimal = ((BigDecimal) value).stripTrailingZeros();
      try {
        final int scale = bigDecimal.scale();
        final long exact = bigDecimal.scaleByPowerOfTen(scale).longValueExact();
        writer.append("new java.math.BigDecimal(").append(exact).append("L");
        if (scale != 0) {
          writer.append(", ").append(scale);
        }
        return writer.append(")");
      } catch (ArithmeticException e) {
        return writer.append("new java.math.BigDecimal(\"").append(
            bigDecimal.toString()).append("\")");
      }
    }
    if (value instanceof BigInteger) {
      BigInteger bigInteger = (BigInteger) value;
      return writer.append("new java.math.BigInteger(\"").append(
          bigInteger.toString()).append("\")");
    }
    if (value instanceof Class) {
      Class clazz = (Class) value;
      return writer.append(clazz.getCanonicalName()).append(".class");
    }
    if (value instanceof Types.RecordType) {
      final Types.RecordType recordType = (Types.RecordType) value;
      return writer.append(recordType.getName()).append(".class");
    }
    if (value.getClass().isArray()) {
      writer.append("new ").append(value.getClass().getComponentType());
      list(writer, Primitive.asList(value), "[] {\n", ",\n", "}");
      return writer;
    }
    Constructor constructor = matchingConstructor(value);
    if (constructor != null) {
      final Field[] fields = value.getClass().getFields();
      writer.append("new ").append(value.getClass());
      list(writer,
          Functions.adapt(fields,
              new Function1<Field, Object>() {
                public Object apply(Field field) {
                  try {
                    return field.get(value);
                  } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                  }
                }
              }),
          "(\n", ",\n", ")");
      return writer;
    }
    return writer.append(value);
  }

  private static void list(ExpressionWriter writer, List list,
      String begin, String sep, String end) {
    writer.begin(begin);
    for (int i = 0; i < list.size(); i++) {
      Object value = list.get(i);
      if (i > 0) {
        writer.append(sep).indent();
      }
      write(writer, value, null);
    }
    writer.end(end);
  }

  private static Constructor matchingConstructor(Object value) {
    final Field[] fields = value.getClass().getFields();
    for (Constructor<?> constructor : value.getClass().getConstructors()) {
      if (argsMatchFields(fields, constructor.getParameterTypes())) {
        return constructor;
      }
    }
    return null;
  }

  private static boolean argsMatchFields(Field[] fields,
      Class<?>[] parameterTypes) {
    if (parameterTypes.length != fields.length) {
      return false;
    }
    for (int i = 0; i < fields.length; i++) {
      if (fields[i].getType() != parameterTypes[i]) {
        return false;
      }
    }
    return true;
  }

  private static void escapeString(StringBuilder buf, String s) {
    buf.append('"');
    int n = s.length();
    char lastChar = 0;
    for (int i = 0; i < n; ++i) {
      char c = s.charAt(i);
      switch (c) {
      case '\\':
        buf.append("\\\\");
        break;
      case '"':
        buf.append("\\\"");
        break;
      case '\n':
        buf.append("\\n");
        break;
      case '\r':
        if (lastChar != '\n') {
          buf.append("\\r");
        }
        break;
      default:
        buf.append(c);
        break;
      }
      lastChar = c;
    }
    buf.append('"');
  }

  @Override
  public boolean equals(Object o) {
    // REVIEW: Should constants with the same value and different type
    // (e.g. 3L and 3) be considered equal.
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    ConstantExpression that = (ConstantExpression) o;

    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (value != null ? value.hashCode() : 0);
    return result;
  }
}

// End ConstantExpression.java
