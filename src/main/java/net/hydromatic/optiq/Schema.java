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

import java.util.List;

/**
 * A namespace for relations and functions.
 *
 * <p>A schema can contain sub-schemas, to any level of nesting. Most
 * providers have a limited number of levels; for example, most JDBC databases
 * have either one level ("schemas") or two levels ("database" and
 * "catalog").</p>
 *
 * <p>Other than sub-schemas, the contents of a schema are called members.
 * A member may or may not have parameters, and its result may or may not be
 * a set.</p>
 *
 * <p>There may be multiple overloaded members with the same name but
 * different numbers or types of parameters.
 * For this reason, {@link #getMembers} returns a list of all
 * members with the same name. Optiq will call
 * {@link Schemas#resolve(java.util.List, java.util.List)} to choose the
 * appropriate one.</p>
 *
 * <p>The most common and important type of member is the one with no
 * arguments and a result type that is a collection of records. This is called a
 * <dfn>relation</dfn>. It is equivalent to a table in a relational
 * database.</p>
 *
 * <p>For example, the query</p>
 *
 * <blockquote>select * from sales.emps</blockquote>
 *
 * <p>is valid if "sales" is a registered
 * schema and "emps" is a member with zero parameters and a result type
 * of <code>Collection(Record(int: "empno", String: "name"))</code>.</p>
 *
 * <p>A schema is a {@link SchemaObject}, which implies that schemas can
 * be nested within schemas.</p>
 */
public interface Schema {
    /**
     * Returns a sub-schema with a given name, or null.
     */
    Schema getSubSchema(String name);

    /**
     * Returns a list of members in this schema with the given name, or empty
     * list if there is no such member.
     *
     * @param name Name of member
     * @return List of members with given name, or empty list
     */
    List<Member> getMembers(String name);

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
     * @param member Member
     * @param arguments Arguments to schema object (null means no argument list)
     * @return Expression for a given schema object
     */
    Expression getMemberExpression(
        Expression schemaExpression,
        Member member,
        List<Expression> arguments);

    Expression getSubSchemaExpression(
        Expression schemaExpression,
        Schema schema,
        String name);

    /**
     * Given an object that is an instance of this schema,
     * returns an object that is an instance of a sub-schema.
     *
     * @param schemaInstance Object that is an instance of this Schema
     * @param subSchemaName Name of sub-schema
     * @param subSchema Sub-schema
     * @return Object that is an instance of the sub-schema
     */
    Object getSubSchemaInstance(
        Object schemaInstance,
        String subSchemaName,
        Schema subSchema);
}

// End Schema.java
