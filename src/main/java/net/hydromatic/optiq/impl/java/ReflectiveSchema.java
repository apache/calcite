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

import net.hydromatic.linq4j.Linq4j;
import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;

import net.hydromatic.optiq.*;

import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Implementation of {@link net.hydromatic.optiq.Schema} that exposes the public
 * fields and methods in a Java object.
 */
public class ReflectiveSchema
    extends MapSchema
{
    final Class clazz;
    private final Method parentMethod;

    /**
     * Creates a ReflectiveSchema.
     *
     * @param target Object whose fields will be sub-objects
     * @param typeFactory Type factory
     */
    public ReflectiveSchema(
        Object target,
        JavaTypeFactory typeFactory)
    {
        this(target, typeFactory, null);
    }

    /**
     * Creates a ReflectiveSchema that is optionally a sub-schema.
     *
     * @param target Object whose fields will be sub-objects
     * @param typeFactory Type factory
     */
    protected ReflectiveSchema(
        Object target,
        JavaTypeFactory typeFactory,
        Method parentMethod)
    {
        super(typeFactory);
        this.clazz = target.getClass();
        this.parentMethod = parentMethod;
        for (Field field : clazz.getFields()) {
            putMulti(
                membersMap,
                field.getName(),
                fieldRelation(field, typeFactory));
        }
        for (Method method : clazz.getMethods()) {
            if (method.getDeclaringClass() == Object.class) {
                continue;
            }
            putMulti(
                membersMap,
                method.getName(),
                methodMember(method, typeFactory));
        }
    }

    public Expression getMemberExpression(
        Expression schemaExpression, Member member, List<Expression> arguments)
    {
        return ((Schemas.MemberPlus) member).getExpression(
            schemaExpression, arguments);
    }

    @Override
    public Expression getSubSchemaExpression(
        Expression schemaExpression, Schema schema, String name)
    {
        return super.getSubSchemaExpression(schemaExpression, schema, name);
    }

    @Override
    public Object getSubSchemaInstance(
        Object schemaInstance,
        String subSchemaName,
        Schema subSchema)
    {
        return ((ReflectiveSchema) subSchema)
            .instanceFromParent(schemaInstance);
    }

    private Object instanceFromParent(Object parentSchemaInstance) {
        try {
            return parentMethod.invoke(parentSchemaInstance);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public static Member methodMember(
        final Method method, final RelDataTypeFactory typeFactory)
    {
        return new Schemas.MemberPlus() {
            final Class<?>[] parameterTypes = method.getParameterTypes();

            @Override
            public String toString() {
                return "Member {method=" + method + "}";
            }

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

            public Queryable evaluate(Object target, List<Object> arguments) {
                try {
                    Object o = method.invoke(target, arguments.toArray());
                    return toQueryable(o);
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

    private Member fieldRelation(
        final Field field,
        JavaTypeFactory typeFactory)
    {
        final RelDataType type = typeFactory.createType(field.getType());
        return new Schemas.MemberPlus() {
            @Override
            public String toString() {
                return "Relation {field=" + field.getName() + "}";
            }

            public List<Parameter> getParameters() {
                return Collections.emptyList();
            }

            public RelDataType getType() {
                return type;
            }

            public Queryable evaluate(Object target, List<Object> arguments) {
                assert arguments == null || arguments.isEmpty();
                try {
                    Object o = field.get(target);
                    return toQueryable(o);
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

    private static Queryable toQueryable(Object o) {
        if (o.getClass().isArray()) {
            if (o instanceof Object[]) {
                return Linq4j.asEnumerable((Object[]) o)
                    .asQueryable();
            }
            // TODO: adapter for primitive arrays, e.g. float[].
            throw new UnsupportedOperationException();
        }
        if (o instanceof Iterable) {
            return Linq4j.asEnumerable((Iterable) o).asQueryable();
        }
        throw new RuntimeException(
            "Cannot convert " + o.getClass() + " into a Queryable");
    }

}

// End ReflectiveSchema.java
