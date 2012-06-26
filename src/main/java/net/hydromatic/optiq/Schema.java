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
package net.hydromatic.optiq;

import net.hydromatic.linq4j.expressions.Expression;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

/**
 * A namespace that returns relations and functions.
 *
 * <p>Most of the time, {@link #get(String)} return a {@link Function}.
 * The most common and important type of Function is the one with no
 * arguments and a result type that is a collection of records. It is
 * equivalent to a table in a relational database.</p>
 *
 * <p>For example, the query</p>
 *
 * <blockquote>select * from sales.emps</blockquote>
 *
 * <p>is valid if "sales" is a registered
 * schema and "emps" is a function with zero parameters and a result type
 * of {@code Collection(Record(int: "empno", String: "name"))}.</p>
 *
 * <p>If there are overloaded functions, then there may be more than one
 * object with a particular name. In this case, {@link #get} returns
 * an {@link Overload}.</p>
 *
 * <p>A schema is a {@link SchemaObject}, which implies that schemas can
 * be nested within schemas.</p>
 */
public interface Schema {
    /**
     * Returns a sub-object with the given name, or null if there is no such
     * object. The sub-object might be a {@link Schema}, or a {@link Function},
     * or a {@link Overload} if there are several functions with the same name
     * but different signatures.
     *
     * @param name Name of sub-object
     * @return Sub-object, or null
     */
    SchemaObject get(String name);

    /**
     * Returns a map whose keys are the names of available sub-objects.
     *
     * @return Map from sub-object names to sub-objects.
     */
    Map<String, SchemaObject> asMap();

    /**
     * Returns the expression for a sub-object "name" or "name(argument, ...)",
     * given an expression for the schema.
     *
     * <p>For example, given schema based on reflection, if the schema is
     * based on an object of type "class Foodmart" called "foodmart", then
     * the "emps" member would be accessed via "foodmart.emps". If the schema
     * were a map, the expression would be
     * "(Enumerable&lt;Employee&gt;) foodmart.get(&quot;emps&quot;)".</p>
     *
     * @param schemaExpression Expression for schema
     * @param name Name of schema object
     * @param arguments Arguments to schema object (null means no argument list)
     * @return Expression for a given schema object
     */
    Expression getExpression(
        Expression schemaExpression,
        SchemaObject schemaObject,
        String name,
        List<Expression> arguments);

    /**
     * Given an object that is an instance of this schema,
     * returns an object that is an instance of the named sub-object of this
     * schema.
     *
     * @param schema Schema
     * @param name Name of sub-object
     * @param parameterTypes Parameter types (to resolve overloaded functions)
     * @return Sub-object
     */
    Object getSubSchema(
        Object schema,
        String name,
        List<Type> parameterTypes);
}

// End Schema.java
