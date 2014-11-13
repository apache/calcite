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

import java.lang.reflect.Modifier;

/**
 * Expression that declares and optionally initializes a variable.
 */
public class DeclarationStatement extends Statement {
  public final int modifiers;
  public final ParameterExpression parameter;
  public final Expression initializer;

  public DeclarationStatement(int modifiers, ParameterExpression parameter,
      Expression initializer) {
    super(ExpressionType.Declaration, Void.TYPE);
    assert parameter != null : "parameter should not be null";
    this.modifiers = modifiers;
    this.parameter = parameter;
    this.initializer = initializer;
  }

  @Override
  public DeclarationStatement accept(Visitor visitor) {
    visitor = visitor.preVisit(this);
    // do not visit parameter - visit may not return a ParameterExpression
    Expression initializer = this.initializer != null
        ? this.initializer.accept(visitor)
        : null;
    return visitor.visit(this, initializer);
  }

  @Override
  void accept0(ExpressionWriter writer) {
    final String modifiers = Modifier.toString(this.modifiers);
    if (!modifiers.isEmpty()) {
      writer.append(modifiers).append(' ');
    }
    writer.append(parameter.type).append(' ').append(parameter.name);
    if (initializer != null) {
      writer.append(" = ").append(initializer);
    }
    writer.append(';');
    writer.newlineAndIndent();
  }

  public void accept2(ExpressionWriter writer, boolean withType) {
    if (withType) {
      final String modifiers = Modifier.toString(this.modifiers);
      if (!modifiers.isEmpty()) {
        writer.append(modifiers).append(' ');
      }
      writer.append(parameter.type).append(' ');
    } else {
      writer.append(", ");
    }
    writer.append(parameter.name);
    if (initializer != null) {
      writer.append(" = ").append(initializer);
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

    DeclarationStatement that = (DeclarationStatement) o;

    if (modifiers != that.modifiers) {
      return false;
    }
    if (initializer != null ? !initializer.equals(that.initializer) : that
        .initializer != null) {
      return false;
    }
    if (!parameter.equals(that.parameter)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + modifiers;
    result = 31 * result + parameter.hashCode();
    result = 31 * result + (initializer != null ? initializer.hashCode() : 0);
    return result;
  }
}

// End DeclarationStatement.java
