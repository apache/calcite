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
import java.util.Iterator;

/**
 * Skeleton implementation of {@link Queryable}.
 *
 * <p>The default implementation of {@link #enumerator()} calls the provider's
 * {@link QueryProvider#executeQuery(Queryable)} method, but the derived class
 * can override.
 *
 * @param <TSource> Element type
 */
public abstract class BaseQueryable<TSource>
    extends AbstractQueryable<TSource> {
  protected final QueryProvider provider;
  protected final Type elementType;
  protected final Expression expression;

  public BaseQueryable(QueryProvider provider, Type elementType,
      Expression expression) {
    this.provider = provider;
    this.elementType = elementType;
    this.expression = expression;
  }

  public QueryProvider getProvider() {
    return provider;
  }

  public Type getElementType() {
    return elementType;
  }

  public Expression getExpression() {
    return expression;
  }

  public Iterator<TSource> iterator() {
    return Linq4j.enumeratorIterator(enumerator());
  }

  public Enumerator<TSource> enumerator() {
    return provider.executeQuery(this);
  }
}

// End BaseQueryable.java
