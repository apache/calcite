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
package org.apache.calcite.util;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.runtime.ExceptionHandler;
import org.apache.calcite.runtime.ExceptionHandlerEnum;

/**
 * Utilities pertaining to {@link org.apache.calcite.runtime.ExceptionHandler}
 */
public final class ExceptionHandlerUtil {

  private ExceptionHandlerUtil() {}

  public static <T> Enumerable<T> wrapEnumerable(Enumerable<T> delegate, DataContext dataContext) {
    ExceptionHandler handler = DataContext.Variable.EXCEPTION_HANDLER.get(dataContext);
    if (handler == null) {
      // fall back to default behavior
      handler = ExceptionHandlerEnum.THROW;
    }
    return handler.wrap(delegate);
  }

  public static Expression wrapEnumerableExpression(Expression delegate, Expression dataContext) {
    return Expressions.call(BuiltInMethod.EXCEPTION_HANDLER_WRAP_ENUMERABLE.method, delegate,
        dataContext);
  }
}

// End ExceptionHandlerUtil.java
