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
import java.lang.reflect.Type;
import java.util.List;

/**
 * Declaration of a class.
 */
public class ClassDeclaration extends MemberDeclaration {
  public final int modifier;
  public final String classClass = "class";
  public final String name;
  public final List<MemberDeclaration> memberDeclarations;
  private final Type extended;
  private final List<Type> implemented;

  public ClassDeclaration(int modifier, String name, Type extended,
      List<Type> implemented, List<MemberDeclaration> memberDeclarations) {
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

  public ClassDeclaration accept(Visitor visitor) {
    final List<MemberDeclaration> members1 =
        Expressions.acceptMemberDeclarations(memberDeclarations, visitor);
    return visitor.visit(this, members1);
  }
}

// End ClassDeclaration.java
