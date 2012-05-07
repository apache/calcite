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
import net.hydromatic.optiq.MutableSchema;
import net.hydromatic.optiq.Schema;
import net.hydromatic.optiq.SchemaLink;
import net.hydromatic.optiq.SchemaObject;

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

    public MapSchema() {
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
        // schemaExpression.get("name")
        return Expressions.call(
            schemaExpression, MAP_GET_METHOD,
            Arrays.<Expression>asList(
                (Expression) Expressions.constant(name)));
    }
}

// End MapSchema.java
