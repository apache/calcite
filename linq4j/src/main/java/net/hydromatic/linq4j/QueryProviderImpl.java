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
 * Partial implementation of {@link QueryProvider}.
 *
 * <p>Derived class needs to implement {@link #executeQuery}.</p>
 */
public abstract class QueryProviderImpl implements QueryProvider {
  /**
   * Creates a QueryProviderImpl.
   */
  public QueryProviderImpl() {
    super();
  }

  public <T> Queryable<T> createQuery(Expression expression, Class<T> rowType) {
    return new QueryableImpl<T>(this, rowType, expression);
  }

  public <T> Queryable<T> createQuery(Expression expression, Type rowType) {
    return new QueryableImpl<T>(this, rowType, expression);
  }

  public <T> T execute(Expression expression, Class<T> type) {
    throw new UnsupportedOperationException();
  }

  public <T> T execute(Expression expression, Type type) {
    throw new UnsupportedOperationException();
  }

  /**
   * Binds an expression to this query provider.
   */
  public static class QueryableImpl<T> extends BaseQueryable<T> {
    public QueryableImpl(QueryProviderImpl provider, Type elementType,
        Expression expression) {
      super(provider, elementType, expression);
    }

    @Override
    public String toString() {
      return "Queryable(expr=" + expression + ")";
    }
  }
}

// End QueryProviderImpl.java
