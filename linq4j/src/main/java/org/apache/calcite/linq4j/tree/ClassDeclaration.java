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

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Objects;

/**
 * Declaration of a class.
 */
public class ClassDeclaration extends MemberDeclaration {
  public final int modifier;
  public final String classClass = "class";
  public final String name;
  public final List<MemberDeclaration> memberDeclarations;
  public final Type extended;
  public final List<Type> implemented;

  public ClassDeclaration(int modifier, String name, Type extended,
      List<Type> implemented, List<MemberDeclaration> memberDeclarations) {
    assert name != null : "name should not be null";
    this.modifier = modifier;
    this.name = name;
    this.memberDeclarations = memberDeclarations;
    this.extended = extended;
    this.implemented = implemented;
  }

  public void accept(ExpressionWriter writer) {
    String modifiers = Modifier.toString(modifier);
    writer.append(modifiers);
    if (!modifiers.isEmpty()) {
      writer.append(' ');
    }
    writer.append(classClass).append(' ').append(name);
    if (extended != null) {
      writer.append(" extends ").append(extended);
    }
    if (!implemented.isEmpty()) {
      writer.list(" implements ", ", ", "", implemented);
    }
    writer.list(" {\n", "", "}", memberDeclarations);
    writer.newlineAndIndent();
  }

  public ClassDeclaration accept(Shuttle shuttle) {
    shuttle = shuttle.preVisit(this);
    final List<MemberDeclaration> members1 =
        Expressions.acceptMemberDeclarations(memberDeclarations, shuttle);
    return shuttle.visit(this, members1);
  }

  public <R> R accept(Visitor<R> visitor) {
    return visitor.visit(this);
  }

  @Override public boolean equals(Object o) {
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
    if (extended != null ? !extended.equals(that.extended) : that.extended
        != null) {
      return false;
    }
    if (implemented != null ? !implemented.equals(that.implemented) : that
        .implemented != null) {
      return false;
    }
    if (memberDeclarations != null ? !memberDeclarations.equals(that
        .memberDeclarations) : that.memberDeclarations != null) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }

    return true;
  }

  @Override public int hashCode() {
    return Objects.hash(modifier, classClass, name, memberDeclarations,
        extended, implemented);
  }
}

// End ClassDeclaration.java
