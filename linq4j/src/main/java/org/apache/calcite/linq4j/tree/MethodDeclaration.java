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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.table.codesplit.JavaCodeSplitter;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

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
    this.modifier = modifier;
    this.name = requireNonNull(name, "name");
    this.resultType = requireNonNull(resultType, "resultType");
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
    // Conditionally serialize the method declaration directly to the supplied writer if method
    // splitting is enabled or to a temporary writer that will generate code for the method that
    // can be split before being serialized to the supplied writer.
    final ExpressionWriter writerForUnsplitMethod = writer.usesMethodSplitting()
        ? writer.duplicateState() : writer;

    final String modifiers = Modifier.toString(modifier);
    writerForUnsplitMethod.append(modifiers);
    if (!modifiers.isEmpty()) {
      writerForUnsplitMethod.append(' ');
    }
    //noinspection unchecked
    writerForUnsplitMethod
        .append(resultType)
        .append(' ')
        .append(name)
        .list("(", ", ", ")",
            () -> (Iterator) parameters.stream().map(ParameterExpression::declString).iterator())
        .append(' ')
        .append(body);

    if (writer.usesMethodSplitting()) {
      //  Specifies a threshold where generated code will be split into sub-function calls.
      //  Java has a maximum method length of 64 KB. This setting allows for finer granularity if
      //  necessary.
      //  Default value is 4000 instead of 64KB as by default JIT refuses to work on methods with
      //  more than 8K byte code.
      final int defaultMaxGeneratedCodeLength = 4000;
      final int defaultMaxMembersGeneratedCode = 10000;

      writer.append(
          StringUtils.stripStart(
              JavaCodeSplitter.split(writerForUnsplitMethod.toString(),
                  defaultMaxGeneratedCodeLength, defaultMaxMembersGeneratedCode),
              " "));
    }
    writer.newlineAndIndent();
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MethodDeclaration that = (MethodDeclaration) o;
    return modifier == that.modifier
        && body.equals(that.body)
        && name.equals(that.name)
        && parameters.equals(that.parameters)
        && resultType.equals(that.resultType);
  }

  @Override public int hashCode() {
    return Objects.hash(modifier, name, resultType, parameters, body);
  }
}
