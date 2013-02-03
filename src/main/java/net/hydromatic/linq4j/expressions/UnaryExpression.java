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
package net.hydromatic.linq4j.expressions;

import java.lang.reflect.Type;

/**
 * Represents an expression that has a unary operator.
 */
public class UnaryExpression extends Expression {
  public final Expression expression;

  UnaryExpression(ExpressionType nodeType, Type type, Expression expression) {
    super(nodeType, type);
    this.expression = expression;
  }

  @Override
  public Expression accept(Visitor visitor) {
    Expression expression = this.expression.accept(visitor);
    return visitor.visit(this, expression);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj instanceof UnaryExpression) {
      final UnaryExpression unaryExpression = (UnaryExpression) obj;
      return nodeType == unaryExpression.nodeType
          && type.equals(unaryExpression.type)
          && expression.equals(unaryExpression.expression);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return nodeType.hashCode() ^ expression.hashCode();
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
}

// End UnaryExpression.java
