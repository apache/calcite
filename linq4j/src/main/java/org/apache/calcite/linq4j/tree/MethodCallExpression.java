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
package org.apache.calcite.linq4j.tree;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;

/**
 * Represents a call to either a static or an instance method.
 */
public class MethodCallExpression extends Expression {
  public final Method method;
  public final Expression targetExpression; // null for call to static method
  public final List<Expression> expressions;
  /**
   * Cache the hash code for the expression
   */
  private int hash;

  MethodCallExpression(Type returnType, Method method,
      Expression targetExpression, List<Expression> expressions) {
    super(ExpressionType.Call, returnType);
    assert expressions != null : "expressions should not be null";
    assert method != null : "method should not be null";
    assert (targetExpression == null) == Modifier.isStatic(
        method.getModifiers());
    assert Types.toClass(returnType) == method.getReturnType();
    this.method = method;
    this.targetExpression = targetExpression;
    this.expressions = expressions;
  }

  MethodCallExpression(Method method, Expression targetExpression,
      List<Expression> expressions) {
    this(method.getReturnType(), method, targetExpression, expressions);
  }

  @Override public Expression accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    Expression targetExpression = Expressions.accept(this.targetExpression,
        shuttle);
    List<Expression> expressions = Expressions.acceptExpressions(
        this.expressions, shuttle);
    return shuttle.visit(this, targetExpression, expressions);
  }

  public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override public Object evaluate(Evaluator evaluator) {
    final Object target;
    if (targetExpression == null) {
      target = null;
    } else {
      target = targetExpression.evaluate(evaluator);
    }
    final Object[] args = new Object[expressions.size()];
    for (int i = 0; i < expressions.size(); i++) {
      Expression expression = expressions.get(i);
      args[i] = expression.evaluate(evaluator);
    }
    try {
      return method.invoke(target, args);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException("error while evaluating " + this, e);
    }
  }

  @Override void accept(ExpressionWriter writer, int lprec, int rprec) {
    if (writer.requireParentheses(this, lprec, rprec)) {
      return;
    }
    if (targetExpression != null) {
      // instance method
      targetExpression.accept(writer, lprec, nodeType.lprec);
    } else {
      // static method
      writer.append(method.getDeclaringClass());
    }
    writer.append('.').append(method.getName()).append('(');
    int k = 0;
    for (Expression expression : expressions) {
      if (k++ > 0) {
        writer.append(", ");
      }
      expression.accept(writer, 0, 0);
    }
    writer.append(')');
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    MethodCallExpression that = (MethodCallExpression) o;

    if (!expressions.equals(that.expressions)) {
      return false;
    }
    if (!method.equals(that.method)) {
      return false;
    }
    if (targetExpression != null ? !targetExpression.equals(that
        .targetExpression) : that.targetExpression != null) {
      return false;
    }

    return true;
  }

  @Override public int hashCode() {
    int result = hash;
    if (result == 0) {
      result =
          Objects.hash(nodeType, type, method, targetExpression, expressions);
      if (result == 0) {
        result = 1;
      }
      hash = result;
    }
    return result;
  }
}

// End MethodCallExpression.java
