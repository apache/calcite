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

import com.google.common.collect.Lists;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;

/**
 * Declaration of a method.
 */
public class MethodDeclaration extends MemberDeclaration {
  public final int modifier;
  public final String name;
  public final Type resultType;
  public final List<ParameterExpression> parameters;
  public final BlockStatement body;

  public MethodDeclaration(int modifier, String name, Type resultType,
      List<ParameterExpression> parameters, BlockStatement body) {
    assert name != null : "name should not be null";
    assert resultType != null : "resultType should not be null";
    assert parameters != null : "parameters should not be null";
    assert body != null : "body should not be null";
    this.modifier = modifier;
    this.name = name;
    this.resultType = resultType;
    this.parameters = parameters;
    this.body = body;
  }

  @Override public MemberDeclaration accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    // do not visit parameters
    final BlockStatement body = this.body.accept(shuttle);
    return shuttle.visit(this, body);
  }

  public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  public void accept(ExpressionWriter writer) {
    String modifiers = Modifier.toString(modifier);
    writer.append(modifiers);
    if (!modifiers.isEmpty()) {
      writer.append(' ');
    }
    writer
        .append(resultType)
        .append(' ')
        .append(name)
        .list("(", ", ", ")",
            Lists.transform(parameters, ParameterExpression::declString))
        .append(' ')
        .append(body);
    writer.newlineAndIndent();
  }

  @Override public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MethodDeclaration that = (MethodDeclaration) o;

    if (modifier != that.modifier) {
      return false;
    }
    if (!body.equals(that.body)) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    if (!parameters.equals(that.parameters)) {
      return false;
    }
    if (!resultType.equals(that.resultType)) {
      return false;
    }

    return true;
  }

  @Override public int hashCode() {
    return Objects.hash(modifier, name, resultType, parameters, body);
  }
}

// End MethodDeclaration.java
