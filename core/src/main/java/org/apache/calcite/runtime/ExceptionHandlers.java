/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.runtime;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.DelegatingEnumerator;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.util.BuiltInMethod;

/**
 * Utilities pertaining to {@link org.apache.calcite.runtime.ExceptionHandler}
 */
@Experimental
public final class ExceptionHandlers {

  private ExceptionHandlers() {}

  public static Expression wrapEnumerableExpression(Expression delegate, Expression dataContext) {
    return Expressions.call(BuiltInMethod.EXCEPTION_HANDLER_WRAP_ENUMERABLE.method, delegate,
        dataContext);
  }

  public static <T> Enumerable<T> wrapEnumerable(Enumerable<T> delegate, DataContext dataContext) {
    ExceptionHandler handler = DataContext.Variable.EXCEPTION_HANDLER.get(dataContext);
    if (handler == null) {
      // fall back to default behavior
      handler = ExceptionHandlerEnum.THROW;
    }
    if (handler == ExceptionHandlerEnum.THROW) {
      // no need to wrap;
      return delegate;
    }
    return wrapEnumerable_(delegate, handler);
  }


  /**
   * Wraps an {@link Enumerable} with an {@link ExceptionHandler}.
   * @param delegate The {@link Enumerable} to be wrapped.
   * @param exceptionHandler The {@link ExceptionHandler} to be called when exceptions are
   *                         encountered.
   * @param <T> Element type of the input {@link Enumerable}.
   * @return An {@link Enumerable} instance with any produced exception to be handled by this
   * {@link ExceptionHandler}.
   */
  private static <T> Enumerable<T> wrapEnumerable_(Enumerable<T> delegate,
                                                   ExceptionHandler exceptionHandler) {
    return new AbstractEnumerable<T>() {
      @Override public Enumerator<T> enumerator() {
        return new DelegatingEnumerator<T>(delegate.enumerator()) {
          private T current;
          @Override public boolean moveNext() {
            boolean hasNext;
            for (;;) {
              try {
                hasNext = delegate.moveNext();
                if (!hasNext) {
                  return false;
                }
                current = delegate.current();
              } catch (Throwable t) {
                exceptionHandler.handleException(t);
                continue;
              }
              return true;
            }
          }

          @Override public T current() {
            return current;
          }
        };
      }
    };
  }
}

// End ExceptionHandlers.java
