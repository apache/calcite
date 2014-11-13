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
import java.util.List;

/**
 * Represents creating a new array and possibly initializing the elements of the
 * new array.
 */
public class NewArrayExpression extends Expression {
  public final int dimension;
  public final Expression bound;
  public final List<Expression> expressions;
  /**
   * Cache the hash code for the expression
   */
  private int hash;

  public NewArrayExpression(Type type, int dimension, Expression bound,
      List<Expression> expressions) {
    super(ExpressionType.NewArrayInit, Types.arrayType(type, dimension));
    this.dimension = dimension;
    this.bound = bound;
    this.expressions = expressions;
  }

  @Override
  public Expression accept(Visitor visitor) {
    visitor = visitor.preVisit(this);
    List<Expression> expressions =
        this.expressions == null
            ? null
            : Expressions.acceptExpressions(this.expressions, visitor);
    Expression bound = Expressions.accept(this.bound, visitor);
    return visitor.visit(this, dimension, bound, expressions);
  }

  @Override
  void accept(ExpressionWriter writer, int lprec, int rprec) {
    writer.append("new ").append(Types.getComponentTypeN(type));
    for (int i = 0; i < dimension; i++) {
      if (i == 0 && bound != null) {
        writer.append('[').append(bound).append(']');
      } else {
        writer.append("[]");
      }
    }
    if (expressions != null) {
      writer.list(" {\n", ",\n", "}", expressions);
    }
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

    NewArrayExpression that = (NewArrayExpression) o;

    if (dimension != that.dimension) {
      return false;
    }
    if (bound != null ? !bound.equals(that.bound) : that.bound != null) {
      return false;
    }
    if (expressions != null ? !expressions.equals(that.expressions) : that
        .expressions != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = hash;
    if (result == 0) {
      result = super.hashCode();
      result = 31 * result + dimension;
      result = 31 * result + (bound != null ? bound.hashCode() : 0);
      result = 31 * result + (expressions != null ? expressions.hashCode() : 0);
      if (result == 0) {
        result = 1;
      }
      hash = result;
    }
    return result;
  }
}

// End NewArrayExpression.java
