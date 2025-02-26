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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Declaration of a constructor.
 */
public class ConstructorDeclaration extends MemberDeclaration {
  public final int modifier;
  public final Type resultType;
  public final List<ParameterExpression> parameters;
  public final BlockStatement body;
  /** Cached hash code for the expression. */
  private int hash;

  public ConstructorDeclaration(int modifier, Type declaredAgainst,
      List<ParameterExpression> parameters, BlockStatement body) {
    this.modifier = modifier;
    this.resultType = requireNonNull(declaredAgainst, "declaredAgainst");
    this.parameters = requireNonNull(parameters, "parameters");
    this.body = requireNonNull(body, "body");
  }

  @Override public MemberDeclaration accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    // do not visit parameters
    final BlockStatement body = this.body.accept(shuttle);
    return shuttle.visit(this, body);
  }

  @Override public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override public void accept(ExpressionWriter writer) {
    String modifiers = Modifier.toString(modifier);
    writer.append(modifiers);
    if (!modifiers.isEmpty()) {
      writer.append(' ');
    }
    //noinspection unchecked
    writer
        .append(resultType)
        .list("(", ", ", ")",
            () -> (Iterator) parameters.stream().map(parameter -> {
              final String modifiers1 =
                  Modifier.toString(parameter.modifier);
              return modifiers1 + (modifiers1.isEmpty() ? "" : " ")
                  + Types.className(parameter.getType()) + " "
                  + parameter.name;
            }).iterator())
        .append(' ').append(body);
    writer.newlineAndIndent();
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ConstructorDeclaration that = (ConstructorDeclaration) o;
    return modifier == that.modifier
        && body.equals(that.body)
        && parameters.equals(that.parameters)
        && resultType.equals(that.resultType);
  }

  @Override public int hashCode() {
    int result = hash;
    if (result == 0) {
      result = Objects.hash(modifier, resultType, parameters, body);
      if (result == 0) {
        result = 1;
      }
      hash = result;
    }
    return result;
  }
}
