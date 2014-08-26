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
package net.hydromatic.optiq.rules.java;

import net.hydromatic.linq4j.expressions.Expression;
import net.hydromatic.linq4j.expressions.Expressions;
import net.hydromatic.linq4j.expressions.NewExpression;

import org.eigenbase.rex.RexCall;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

/**
 * Implementation of {@link net.hydromatic.optiq.rules.java
 * .NotNullImplementor} that calls given {@link java.lang.reflect.Method}.
 * When method is not static, a new instance of the required class is created.
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
}

// End ReflectiveCallNotNullImplementor.java
