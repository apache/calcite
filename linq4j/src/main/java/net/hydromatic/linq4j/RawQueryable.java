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
package net.hydromatic.linq4j;

import net.hydromatic.linq4j.expressions.Expression;

import java.lang.reflect.Type;

/**
 * Core methods that define a {@link Queryable}.
 *
 * <p>The other methods in {@link Queryable}, defined in
 * {@link ExtendedQueryable}, can easily be implemented by calling the
 * corresponding static methods in {@link Extensions}.
 *
 * @param <T> Element type
 */
public interface RawQueryable<T> extends Enumerable<T> {
  /**
   * Gets the type of the element(s) that are returned when the expression
   * tree associated with this Queryable is executed.
   */
  Type getElementType();

  /**
   * Gets the expression tree that is associated with this Queryable.
   */
  Expression getExpression();

  /**
   * Gets the query provider that is associated with this data source.
   */
  QueryProvider getProvider();
}

// End RawQueryable.java
