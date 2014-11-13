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
 * Represents an expression that has a ternary operator.
 */
public class TernaryExpression extends Expression {
  public final Expression expression0;
  public final Expression expression1;
  public final Expression expression2;

  TernaryExpression(ExpressionType nodeType, Type type, Expression expression0,
      Expression expression1, Expression expression2) {
    super(nodeType, type);
    assert expression0 != null : "expression0 should not be null";
    assert expression1 != null : "expression1 should not be null";
    assert expression2 != null : "expression2 should not be null";
    this.expression0 = expression0;
    this.expression1 = expression1;
    this.expression2 = expression2;
  }

  @Override
  public Expression accept(Visitor visitor) {
    visitor = visitor.preVisit(this);
    Expression expression0 = this.expression0.accept(visitor);
    Expression expression1 = this.expression1.accept(visitor);
    Expression expression2 = this.expression2.accept(visitor);
    return visitor.visit(this, expression0, expression1, expression2);
  }

  void accept(ExpressionWriter writer, int lprec, int rprec) {
    if (writer.requireParentheses(this, lprec, rprec)) {
      return;
    }
    expression0.accept(writer, lprec, nodeType.lprec);
    writer.append(nodeType.op);
    expression1.accept(writer, nodeType.rprec, nodeType.lprec);
    writer.append(nodeType.op2);
    expression2.accept(writer, nodeType.rprec, rprec);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    TernaryExpression that = (TernaryExpression) o;

    if (!expression0.equals(that.expression0)) {
      return false;
    }
    if (!expression1.equals(that.expression1)) {
      return false;
    }
    if (!expression2.equals(that.expression2)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + expression0.hashCode();
    result = 31 * result + expression1.hashCode();
    result = 31 * result + expression2.hashCode();
    return result;
  }
}

// End TernaryExpression.java
