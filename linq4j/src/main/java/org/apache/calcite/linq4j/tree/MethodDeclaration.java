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

import org.apache.flink.table.codesplit.JavaCodeSplitter;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.Iterator;
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

  @Override public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override public void accept(ExpressionWriter writer) {
    final ExpressionWriter tempWriter = writer.duplicateState();
    String modifiers = Modifier.toString(modifier);
    tempWriter.append(modifiers);
    if (!modifiers.isEmpty()) {
      tempWriter.append(' ');
    }
    //noinspection unchecked
    tempWriter
        .append(resultType)
        .append(' ')
        .append(name)
        .list("(", ", ", ")",
            () -> (Iterator) parameters.stream().map(ParameterExpression::declString).iterator())
        .append(' ')
        .append(body);

    // From Flink TableConfigOptions.MAX_LENGTH_GENERATED_CODE
    // Note that Flink uses 4000 rather than 64KB explicitly:
    // "Specifies a threshold where generated code will be split into sub-function calls.
    //  Java has a maximum method length of 64 KB. This setting allows for finer granularity if
    //  necessary.
    //  Default value is 4000 instead of 64KB as by default JIT refuses to work on methods with
    //  more than 8K byte code."
    final int flinkDefaultMaxGeneratedCodeLength = 4000;

    // From Flink TableConfigOptions.MAX_MEMBERS_GENERATED_CODE
    final int flinkDefaultMaxMembersGeneratedCode = 10000;

    // TODO: Use code splitter conditionally based on configuration.
    writer.append(
        JavaCodeSplitter.split(tempWriter.toString(), flinkDefaultMaxGeneratedCodeLength,
        flinkDefaultMaxMembersGeneratedCode));
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
