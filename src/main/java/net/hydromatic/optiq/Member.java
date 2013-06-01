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

import net.hydromatic.linq4j.Queryable;

import org.eigenbase.reltype.RelDataType;

import java.util.List;

/**
 * A named expression in a schema.
 *
 * <h2>Examples of members</h2>
 *
 * <p>Several kinds of members crop up in real life. They all implement the
 * {@code Member} interface, but tend to be treated differently by the
 * back-end system if not by Optiq.</p>
 *
 * <p>A member that has zero arguments and a type that is a collection of
 * records is referred to as a <i>relation</i>. In schemas backed by a
 * relational database, tables and views will appear as relations.</p>
 *
 * <p>A member that has one or more arguments and a type that is a collection
 * of records is referred to as a <i>parameterized relation</i>. Some relational
 * databases support these; for example, Oracle calls them "table
 * functions".</p>
 *
 * <p>Members may be also more typical of programming-language functions:
 * they take zero or more arguments, and return a result of arbitrary type.</p>
 *
 * <p>From the above definitions, you can see that a member is a special
 * kind of function. This makes sense, because even though it has no
 * arguments, it is "evaluated" each time it is used in a query.</p>
 */
public interface Member {
  /**
   * The name of this function.
   */
  String getName();

  /**
   * Returns the parameters of this member.
   *
   * @return Parameters; never null
   */
  List<Parameter> getParameters();

  /**
   * Returns the type of this function's result.
   *
   * @return Type of result; never null
   */
  RelDataType getType();

  /**
   * Evaluates this member to yield a result. The result is a
   * {@link net.hydromatic.linq4j.Queryable}.
   *
   * @param schemaInstance Object that is an instance of the containing
   *                       {@link net.hydromatic.optiq.Schema}
   * @param arguments List of arguments to the call; must match
   *                  {@link #getParameters() parameters} in number and type
   *
   * @return An instance of this schema object, as a Queryable
   */
  Queryable evaluate(
      Object schemaInstance,
      List<Object> arguments);
}

// End Member.java
