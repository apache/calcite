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
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Declaration of a field.
 */
public class FieldDeclaration extends MemberDeclaration {
  public final int modifier;
  public final ParameterExpression parameter;
  public final @Nullable Expression initializer;

  public FieldDeclaration(int modifier, ParameterExpression parameter,
      @Nullable Expression initializer) {
    this.modifier = modifier;
    this.parameter = requireNonNull(parameter, "parameter");
    this.initializer = initializer;
  }

  @Override public MemberDeclaration accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    // do not visit parameter - visit may not return a ParameterExpression
    final Expression initializer =
        this.initializer == null ? null : this.initializer.accept(shuttle);
    return shuttle.visit(this, initializer);
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
    writer.append(parameter.type).append(' ').append(parameter.name);
    if (initializer != null) {
      writer.append(" = ").append(initializer);
    }
    writer.append(';');
    writer.newlineAndIndent();
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    FieldDeclaration that = (FieldDeclaration) o;
    return modifier == that.modifier
        && Objects.equals(initializer, that.initializer)
        && parameter.equals(that.parameter);
  }

  @Override public int hashCode() {
    return Objects.hash(modifier, parameter, initializer);
  }
}
