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
package net.hydromatic.optiq.impl.java;

import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;

import net.hydromatic.optiq.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.sql.type.SqlTypeUtil;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Implementation of {@link net.hydromatic.optiq.Schema} that exposes the public
 * fields and methods in a Java object.
 */
public class ReflectiveSchema
    extends MapSchema
{
    private final RelDataType type;

    /**
     * Creates a ReflectiveSchema.
     *
     * @param o The object whose fields will be sub-objects
     * @param typeFactory Type factory
     */
    public ReflectiveSchema(
        Object o,
        JavaTypeFactory typeFactory)
    {
        super(typeFactory);
        this.type = typeFactory.createType(o.getClass());
        Class<?> clazz = o.getClass();
        for (Field field : clazz.getFields()) {
            map.put(field.getName(), fieldRelation(o, field, typeFactory));
        }
        for (Method method : clazz.getMethods()) {
            putMulti(
                method.getName(),
                methodFunction(o, method, typeFactory));
        }
    }

    public RelDataType getType() {
        return type;
    }

    private void putMulti(String name, Function function) {
        SchemaObject x = map.put(name, function);
        if (x == null) {
            return;
        }
        OverloadImpl overload;
        if (x instanceof OverloadImpl) {
            overload = (OverloadImpl) x;
        } else {
            overload = new OverloadImpl(name);
        }
        overload.list.add(function);
    }

    public Expression getExpression(
        Expression schemaExpression,
        SchemaObject schemaObject,
        String name,
        List<Expression> arguments)
    {
        return ((FunctionPlus) schemaObject).getExpression(
            schemaExpression, arguments);
    }

    private interface FunctionPlus extends Function {
        public Expression getExpression(
            Expression schemaExpression,
            List<Expression> argumentExpressions);
    }

    private static class OverloadImpl implements Overload {
        final List<Function> list = new ArrayList<Function>();
        private final String name;

        public OverloadImpl(String name) {
            this.name = name;
        }

        public Function resolve(List<RelDataType> argumentTypes) {
            final List<Function> matches = new ArrayList<Function>();
            for (Function function : list) {
                if (matches(function, argumentTypes)) {
                    matches.add(function);
                }
            }
            return null; // TODO:
        }

        private boolean matches(
            Function function,
            List<RelDataType> argumentTypes)
        {
            List<Parameter> parameters = function.getParameters();
            if (parameters.size() != argumentTypes.size()) {
                return false;
            }
            for (int i = 0; i < argumentTypes.size(); i++) {
                RelDataType argumentType = argumentTypes.get(i);
                Parameter parameter = parameters.get(i);
                if (!canConvert(argumentType, parameter.getType())) {
                    return false;
                }
            }
            return true;
        }

        private boolean canConvert(RelDataType fromType, RelDataType toType) {
            return SqlTypeUtil.canAssignFrom(toType, fromType);
        }

        public String getName() {
            return name;
        }
    }

    private Function methodFunction(
        final Object o,
        final Method method,
        final RelDataTypeFactory typeFactory)
    {
        return new FunctionPlus() {
            final Class<?>[] parameterTypes = method.getParameterTypes();
            public List<Parameter> getParameters() {
                return new AbstractList<Parameter>() {
                    public Parameter get(final int index) {
                        return new Parameter() {
                            public int getOrdinal() {
                                return index;
                            }

                            public String getName() {
                                return "arg" + index;
                            }

                            public RelDataType getType() {
                                return typeFactory.createJavaType(
                                    method.getParameterTypes()[index]);
                            }
                        };
                    }

                    public int size() {
                        return parameterTypes.length;
                    }
                };
            }

            public RelDataType getType() {
                return typeFactory.createJavaType(method.getReturnType());
            }

            public Object evaluate(List<Object> arguments) {
                try {
                    return method.invoke(o, arguments.toArray());
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(
                        "Error while invoking method " + method, e);
                } catch (InvocationTargetException e) {
                    throw new RuntimeException(
                        "Error while invoking method " + method, e);
                }
            }

            public String getName() {
                return method.getName();
            }

            public Expression getExpression(
                Expression schemaExpression,
                List<Expression> argumentExpressions)
            {
                return Expressions.call(
                    schemaExpression, method, argumentExpressions);
            }
        };
    }

    private SchemaObject fieldRelation(
        final Object o,
        final Field field,
        JavaTypeFactory typeFactory)
    {
        final RelDataType type = typeFactory.createType(field.getType());
        return new FunctionPlus() {
            public List<Parameter> getParameters() {
                return Collections.emptyList();
            }

            public RelDataType getType() {
                return type;
            }

            public Object evaluate(List<Object> arguments) {
                assert arguments == null || arguments.isEmpty();
                try {
                    return field.get(o);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(
                        "Error while accessing field " + field, e);
                }
            }

            public String getName() {
                return field.getName();
            }

            public Expression getExpression(
                Expression schemaExpression,
                List<Expression> argumentExpressions)
            {
                assert argumentExpressions.isEmpty();
                return Expressions.field(schemaExpression, field);
            }
        };
    }

}

// End ReflectiveSchema.java
