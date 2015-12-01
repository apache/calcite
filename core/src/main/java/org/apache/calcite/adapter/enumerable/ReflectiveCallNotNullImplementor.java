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
package org.apache.calcite.adapter.enumerable;

import org.apache.calcite.linq4j.tree.ConstantUntypedNull;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.SqlFunctions;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of
 * {@link org.apache.calcite.adapter.enumerable.NotNullImplementor}
 * that calls a given {@link java.lang.reflect.Method}.
 *
 * <p>When method is not static, a new instance of the required class is
 * created.
 */
public class ReflectiveCallNotNullImplementor implements NotNullImplementor {
  protected final Method method;

  /**
   * Constructor of {@link ReflectiveCallNotNullImplementor}
   * @param method method that is used to implement the call
   */
  public ReflectiveCallNotNullImplementor(Method method) {
    this.method = method;
  }

  public Expression implement(RexToLixTranslator translator,
      RexCall call, List<Expression> translatedOperands) {
    translatedOperands = fromInternal(translatedOperands);
    if ((method.getModifiers() & Modifier.STATIC) != 0) {
      return Expressions.call(method, translatedOperands);
    } else {
      // The UDF class must have a public zero-args constructor.
      // Assume that the validator checked already.
      final NewExpression target =
          Expressions.new_(method.getDeclaringClass());
      return Expressions.call(target, method, translatedOperands);
    }
  }

  protected List<Expression> fromInternal(List<Expression> expressions) {
    final List<Expression> list = new ArrayList<>();
    final Class[] types = method.getParameterTypes();
    for (int i = 0; i < expressions.size(); i++) {
      list.add(fromInternal(expressions.get(i), types[i]));
    }
    return list;
  }

  protected Expression fromInternal(Expression e, Class<?> targetType) {
    if (e == ConstantUntypedNull.INSTANCE) {
      return e;
    }
    if (!(e.getType() instanceof Class)) {
      return e;
    }
    if (targetType.isAssignableFrom((Class) e.getType())) {
      return e;
    }
    if (targetType == java.sql.Date.class) {
      return Expressions.call(SqlFunctions.class, "internalToDate", e);
    }
    if (targetType == java.sql.Time.class) {
      return Expressions.call(SqlFunctions.class, "internalToTime", e);
    }
    if (targetType == java.sql.Timestamp.class) {
      return Expressions.call(SqlFunctions.class, "internalToTimestamp", e);
    }
    return e;
  }
}

// End ReflectiveCallNotNullImplementor.java
