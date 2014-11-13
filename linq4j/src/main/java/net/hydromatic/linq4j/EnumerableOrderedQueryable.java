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
import net.hydromatic.linq4j.expressions.FunctionExpression;
import net.hydromatic.linq4j.function.*;

import java.util.Comparator;

/**
 * Implementation of {@link OrderedQueryable} by an
 * {@link net.hydromatic.linq4j.Enumerable}.
 *
 * @param <T> Element type
 */
class EnumerableOrderedQueryable<T> extends EnumerableQueryable<T>
    implements OrderedQueryable<T> {
  EnumerableOrderedQueryable(Enumerable<T> enumerable, Class<T> rowType,
      QueryProvider provider, Expression expression) {
    super(provider, rowType, expression, enumerable);
  }

  public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenBy(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return QueryableDefaults.thenBy(asOrderedQueryable(), keySelector);
  }

  public <TKey> OrderedQueryable<T> thenBy(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return QueryableDefaults.thenBy(asOrderedQueryable(), keySelector,
        comparator);
  }

  public <TKey extends Comparable<TKey>> OrderedQueryable<T> thenByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector) {
    return QueryableDefaults.thenByDescending(asOrderedQueryable(),
        keySelector);
  }

  public <TKey> OrderedQueryable<T> thenByDescending(
      FunctionExpression<Function1<T, TKey>> keySelector,
      Comparator<TKey> comparator) {
    return QueryableDefaults.thenByDescending(asOrderedQueryable(), keySelector,
        comparator);
  }
}

// End EnumerableOrderedQueryable.java
