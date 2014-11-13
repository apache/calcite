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
 * Defines methods to create and execute queries that are described by a
 * {@link Queryable} object.
 *
 * <p>Analogous to LINQ's System.Linq.QueryProvider.</p>
 */
public interface QueryProvider {
  /**
   * Constructs a {@link Queryable} object that can evaluate the query
   * represented by a specified expression tree.
   *
   * <p>NOTE: The {@link net.hydromatic.linq4j.Queryable#getExpression()}
   * property of the returned {@link Queryable} object is equal to
   * {@code expression}.</p>
   *
   * @param expression Expression
   * @param rowType Row type
   * @param <T> Row type
   *
   * @return Queryable
   */
  <T> Queryable<T> createQuery(Expression expression, Class<T> rowType);

  /**
   * Constructs a {@link Queryable} object that can evaluate the query
   * represented by a specified expression tree. The row type may contain
   * generic information.
   *
   * @param expression Expression
   * @param rowType Row type
   * @param <T> Row type
   *
   * @return Queryable
   */
  <T> Queryable<T> createQuery(Expression expression, Type rowType);

  /**
   * Executes the query represented by a specified expression tree.
   *
   * <p>This method executes queries that return a single value
   * (instead of an enumerable sequence of values). Expression trees that
   * represent queries that return enumerable results are executed when the
   * {@link Queryable} object that contains the expression tree is
   * enumerated.</p>
   *
   * <p>The Queryable standard query operator methods that return singleton
   * results call {@code execute}. They pass it a
   * {@link net.hydromatic.linq4j.expressions.MethodCallExpression}
   * that represents a linq4j query.
   */
  <T> T execute(Expression expression, Class<T> type);

  /**
   * Executes the query represented by a specified expression tree.
   * The row type may contain type parameters.
   */
  <T> T execute(Expression expression, Type type);

  /**
   * Executes a queryable, and returns an enumerator over the
   * rows that it yields.
   *
   * @param queryable Queryable
   *
   * @return Enumerator over rows
   */
  <T> Enumerator<T> executeQuery(Queryable<T> queryable);
}

// End QueryProvider.java
