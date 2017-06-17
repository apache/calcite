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

import java.lang.reflect.Type;
import java.util.Objects;

/**
 * Represents an expression that has a unary operator.
 */
public class UnaryExpression extends Expression {
  public final Expression expression;

  UnaryExpression(ExpressionType nodeType, Type type, Expression expression) {
    super(nodeType, type);
    assert expression != null : "expression should not be null";
    this.expression = expression;
  }

  @Override public Expression accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    Expression expression = this.expression.accept(shuttle);
    return shuttle.visit(this, expression);
  }

  public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  void accept(ExpressionWriter writer, int lprec, int rprec) {
    switch (nodeType) {
    case Convert:
      if (!writer.requireParentheses(this, lprec, rprec)) {
        writer.append("(").append(type).append(") ");
        expression.accept(writer, nodeType.rprec, rprec);
      }
      return;
    }
    if (nodeType.postfix) {
      expression.accept(writer, lprec, nodeType.rprec);
      writer.append(nodeType.op);
    } else {
      writer.append(nodeType.op);
      expression.accept(writer, nodeType.lprec, rprec);
    }
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

    UnaryExpression that = (UnaryExpression) o;

    if (!expression.equals(that.expression)) {
      return false;
    }

    return true;
  }

  @Override public int hashCode() {
    return Objects.hash(nodeType, type, expression);
  }
}

// End UnaryExpression.java
