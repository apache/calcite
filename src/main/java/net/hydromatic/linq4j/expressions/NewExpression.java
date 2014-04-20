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
 * Represents a constructor call.
 *
 * <p>If {@link #memberDeclarations} is not null (even if empty) represents
 * an anonymous class.</p>
 */
public class NewExpression extends Expression {
  public final Type type;
  public final List<Expression> arguments;
  public final List<MemberDeclaration> memberDeclarations;
  /**
   * Cache the hash code for the expression
   */
  private int hash;

  public NewExpression(Type type, List<Expression> arguments,
      List<MemberDeclaration> memberDeclarations) {
    super(ExpressionType.New, type);
    this.type = type;
    this.arguments = arguments;
    this.memberDeclarations = memberDeclarations;
  }

  @Override
  public Expression accept(Visitor visitor) {
    visitor = visitor.preVisit(this);
    final List<Expression> arguments = Expressions.acceptExpressions(
        this.arguments, visitor);
    final List<MemberDeclaration> memberDeclarations =
        Expressions.acceptMemberDeclarations(this.memberDeclarations, visitor);
    return visitor.visit(this, arguments, memberDeclarations);
  }

  @Override
  void accept(ExpressionWriter writer, int lprec, int rprec) {
    writer.append("new ").append(type).list("(\n", ",\n", ")", arguments);
    if (memberDeclarations != null) {
      writer.list("{\n", "", "}", memberDeclarations);
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

    NewExpression that = (NewExpression) o;

    if (arguments != null ? !arguments.equals(that.arguments)
        : that.arguments != null) {
      return false;
    }
    if (memberDeclarations
        != null ? !memberDeclarations.equals(that.memberDeclarations)
        : that.memberDeclarations != null) {
      return false;
    }
    if (!type.equals(that.type)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = hash;
    if (result == 0) {
      result = super.hashCode();
      result = 31 * result + type.hashCode();
      result = 31 * result + (arguments != null ? arguments.hashCode() : 0);
      result = 31 * result + (
          memberDeclarations != null ? memberDeclarations.hashCode() : 0);
      if (result == 0) {
        result = 1;
      }
      hash = result;
    }
    return result;
  }
}

// End NewExpression.java
