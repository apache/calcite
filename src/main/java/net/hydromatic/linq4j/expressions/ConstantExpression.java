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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.AbstractList;
import java.util.List;

/**
 * Represents an expression that has a constant value.
 */
public class ConstantExpression extends Expression {
    public final Object value;

    public ConstantExpression(Type type, Object value) {
        super(ExpressionType.Constant, type);
        this.value = value;
    }

    public Object evaluate(Evaluator evaluator) {
        return value;
    }

    @Override
    public ConstantExpression accept(Visitor visitor) {
        return visitor.visit(this);
    }

    @Override
    void accept(ExpressionWriter writer, int lprec, int rprec) {
        write(writer, value, type);
    }

    private static ExpressionWriter write(
        ExpressionWriter writer, final Object value, Type type)
    {
        if (value == null) {
            return writer.append("null");
        }
        if (value instanceof String) {
            escapeString(writer.getBuf(), (String) value);
            return writer;
        }
        final Primitive primitive = Primitive.of(type);
        if (primitive != null) {
            switch (primitive) {
            case FLOAT:
                return writer.append(value).append("f");
            case DOUBLE:
                return writer.append(value).append("d");
            case LONG:
                return writer.append(value).append("l");
            default:
                return writer.append(value);
            }
        }
        final Primitive primitive2 = Primitive.ofBox(type);
        if (primitive2 != null) {
            return writer.append(
                primitive2.boxClass.getSimpleName() + ".valueOf(" + value
                + ")");
        }
        if (value instanceof Class) {
            Class clazz = (Class) value;
            return writer.append(clazz.getCanonicalName()).append(".class");
        }
        if (value.getClass().isArray()) {
            writer.append("new ")
                .append(value.getClass().getComponentType());
            list(
                writer,
                new AbstractList<Object>() {
                    public Object get(int index) {
                        return Array.get(value, index);
                    }

                    public int size() {
                        return Array.getLength(value);
                    }
                }, "[] {\n", ",\n", "}");
            return writer;
        }
        Constructor constructor = matchingConstructor(value);
        if (constructor != null) {
            final Field[] fields = value.getClass().getFields();
            writer
                .append("new ")
                .append(value.getClass());
            list(
                writer,
                new AbstractList<Object>() {
                    public Object get(int index) {
                        try {
                            return fields[index].get(value);
                        } catch (IllegalAccessException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    public int size() {
                        return fields.length;
                    }
                },
                "(\n", ",\n", ")");
            return writer;
        }
        return writer.append(value);
    }

    private static void list(
        ExpressionWriter writer,
        List<Object> list,
        String begin,
        String sep,
        String end)
    {
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

    private static boolean argsMatchFields(
        Field[] fields, Class<?>[] parameterTypes)
    {
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
            switch  (c) {
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
}

// End ConstantExpression.java
