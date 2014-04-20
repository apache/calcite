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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

/**
 * Represents accessing a field or property.
 */
public class MemberExpression extends Expression {
  public final Expression expression;
  public final PseudoField field;

  public MemberExpression(Expression expression, Field field) {
    this(expression, Types.field(field));
  }

  public MemberExpression(Expression expression, PseudoField field) {
    super(ExpressionType.MemberAccess, field.getType());
    assert field != null : "field should not be null";
    assert expression != null || Modifier.isStatic(field.getModifiers())
        : "must specify expression if field is not static";
    this.expression = expression;
    this.field = field;
  }

  @Override
  public Expression accept(Visitor visitor) {
    visitor = visitor.preVisit(this);
    Expression expression1 = expression == null
        ? null
        : expression.accept(visitor);
    return visitor.visit(this, expression1);
  }

  public Object evaluate(Evaluator evaluator) {
    final Object o = expression == null
        ? null
        : expression.evaluate(evaluator);
    try {
      return field.get(o);
    } catch (IllegalAccessException e) {
      throw new RuntimeException("error while evaluating " + this, e);
    }
  }

  @Override
  void accept(ExpressionWriter writer, int lprec, int rprec) {
    if (writer.requireParentheses(this, lprec, rprec)) {
      return;
    }
    if (expression != null) {
      expression.accept(writer, lprec, nodeType.lprec);
    } else {
      assert (field.getModifiers() & Modifier.STATIC) != 0;
      writer.append(field.getDeclaringClass());
    }
    writer.append('.').append(field.getName());
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

    MemberExpression that = (MemberExpression) o;

    if (expression != null ? !expression.equals(that.expression) : that
        .expression != null) {
      return false;
    }
    if (!field.equals(that.field)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (expression != null ? expression.hashCode() : 0);
    result = 31 * result + field.hashCode();
    return result;
  }
}

// End MemberExpression.java
