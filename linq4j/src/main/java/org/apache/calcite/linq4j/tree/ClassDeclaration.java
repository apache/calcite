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
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Declaration of a class.
 */
public class ClassDeclaration extends MemberDeclaration {
  public final int modifier;
  public final String classClass = "class";
  public final String name;
  public final List<MemberDeclaration> memberDeclarations;
  public final @Nullable Type extended;
  public final List<Type> implemented;

  public ClassDeclaration(int modifier, String name, @Nullable Type extended,
      List<Type> implemented, List<MemberDeclaration> memberDeclarations) {
    this.modifier = modifier;
    this.name = requireNonNull(name, "name");
    this.memberDeclarations = memberDeclarations;
    this.extended = extended;
    this.implemented = implemented;
  }

  @Override public void accept(ExpressionWriter writer) {
    // Conditionally serialize the class declaration directly to the supplied writer if method
    // splitting is disabled or to a temporary writer that will generate code for the method that
    // can be split before being serialized to the supplied writer.
    final ExpressionWriter writerForClassWithoutSplitting = writer.usesMethodSplitting()
        ? writer.duplicateState() : writer;

    String modifiers = Modifier.toString(modifier);
    writerForClassWithoutSplitting.append(modifiers);
    if (!modifiers.isEmpty()) {
      writerForClassWithoutSplitting.append(' ');
    }
    writerForClassWithoutSplitting.append(classClass).append(' ').append(name);
    if (extended != null) {
      writerForClassWithoutSplitting.append(" extends ").append(extended);
    }
    if (!implemented.isEmpty()) {
      writerForClassWithoutSplitting.list(" implements ", ", ", "", implemented);
    }
    writerForClassWithoutSplitting.list(" {\n", "", "}", memberDeclarations);

    if (writer.usesMethodSplitting()) {
      final int defaultMaxMembersGeneratedCode = 10000;

      writer.append(
          StringUtils.stripStart(
              JavaCodeSplitter.split(writerForClassWithoutSplitting.toString(),
                  writer.getMaxMethodLengthInChars(), defaultMaxMembersGeneratedCode),
              " "));
    }
    writer.newlineAndIndent();
  }

  @Override public ClassDeclaration accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    final List<MemberDeclaration> members1 =
        Expressions.acceptMemberDeclarations(memberDeclarations, shuttle);
    return shuttle.visit(this, members1);
  }

  @Override public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ClassDeclaration that = (ClassDeclaration) o;

    if (modifier != that.modifier) {
      return false;
    }
    if (!classClass.equals(that.classClass)) {
      return false;
    }
    return Objects.equals(extended, that.extended)
        && implemented.equals(that.implemented)
        && memberDeclarations.equals(that.memberDeclarations)
        && name.equals(that.name);
  }

  @Override public int hashCode() {
    return Objects.hash(modifier, classClass, name, memberDeclarations,
        extended, implemented);
  }
}
