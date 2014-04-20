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

/**
 * Represents a {@code throw} statement.
 */
public class ThrowStatement extends Statement {
  public final Expression expression;

  public ThrowStatement(Expression expression) {
    super(ExpressionType.Throw, Void.TYPE);
    this.expression = expression;
  }

  @Override
  public Statement accept(Visitor visitor) {
    visitor = visitor.preVisit(this);
    Expression expression = this.expression.accept(visitor);
    return visitor.visit(this, expression);
  }

  @Override
  void accept0(ExpressionWriter writer) {
    writer.append("throw ").append(expression).append(';').newlineAndIndent();
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

    ThrowStatement that = (ThrowStatement) o;

    if (expression != null ? !expression.equals(that.expression) : that
        .expression != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (expression != null ? expression.hashCode() : 0);
    return result;
  }
}

// End ThrowStatement.java
