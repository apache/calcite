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
import net.hydromatic.linq4j.expressions.MethodCallExpression;
import net.hydromatic.optiq.*;

import org.eigenbase.reltype.RelDataType;

import java.lang.reflect.Method;
import java.util.*;

/**
 * Implementation of {@link Schema} backed by a {@link HashMap}.
 *
 * @author jhyde
 */
public class MapSchema implements MutableSchema {
    private static final Method MAP_GET_METHOD;

    static {
        try {
            MAP_GET_METHOD = Map.class.getMethod("get", Object.class);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    protected final Map<String, SchemaObject> map =
        new HashMap<String, SchemaObject>();
    private final JavaTypeFactory typeFactory;

    public MapSchema(JavaTypeFactory typeFactory) {
        this.typeFactory = typeFactory;
    }

    public SchemaObject get(String name) {
        return map.get(name);
    }

    public void add(SchemaObject schemaObject) {
        map.put(schemaObject.getName(), schemaObject);
    }

    public void add(final String name, Schema schema) {
        map.put(name, new SchemaLink(name, schema));
    }

    public Map<String, SchemaObject> asMap() {
        return Collections.unmodifiableMap(map);
    }

    public Expression getExpression(
        Expression schemaExpression,
        SchemaObject schemaObject,
        String name,
        List<Expression> arguments)
    {
        // (Type) schemaExpression.get("name")
        Expression call =
            Expressions.call(
                schemaExpression,
                MAP_GET_METHOD,
                Collections.<Expression>singletonList(
                    Expressions.constant(name)));
        Object o = schemaObject;
        if (schemaObject instanceof SchemaLink) {
            o = ((SchemaLink) schemaObject).schema;
            call =
                Expressions.field(
                    Expressions.convert_(
                        call,
                        SchemaLink.class),
                    "schema");
        }
        Class clazz = deduceClass(o);
        if (clazz != null && clazz != Object.class) {
            return Expressions.convert_(call, clazz);
        }
        return call;
    }

    private Class deduceClass(Object schemaObject) {
        // REVIEW: Can we remove the dependency on RelDataType and work in
        //   terms of Class?
        if (schemaObject instanceof Function) {
            RelDataType type = ((Function) schemaObject).getType();
            return typeFactory.getJavaClass(type);
        }
        if (schemaObject instanceof ReflectiveSchema) {
            RelDataType type =
                ((ReflectiveSchema) schemaObject).getType();
            return typeFactory.getJavaClass(type);
        }
        return null;
    }
}

// End MapSchema.java
