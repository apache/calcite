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
import java.util.AbstractList;
import java.util.List;

/**
 * Declaration of a constructor.
 */
public class ConstructorDeclaration extends MemberDeclaration {
  public final int modifier;
  public final Type resultType;
  public final List<ParameterExpression> parameters;
  public final BlockExpression body;

  public ConstructorDeclaration(int modifier, Type declaredAgainst,
      List<ParameterExpression> parameters, BlockExpression body) {
    this.modifier = modifier;
    this.resultType = declaredAgainst;
    this.parameters = parameters;
    this.body = body;
  }

  @Override
  public MemberDeclaration accept(Visitor visitor) {
    // do not visit parameters
    final BlockExpression body = this.body.accept(visitor);
    return visitor.visit(this, parameters, body);
  }

  public void accept(ExpressionWriter writer) {
    String modifiers = Modifier.toString(modifier);
    writer.append(modifiers);
    if (!modifiers.isEmpty()) {
      writer.append(' ');
    }
    writer.append(resultType).list("(", ", ", ")", new AbstractList<String>() {
      public String get(int index) {
        ParameterExpression parameter = parameters.get(index);
        final String modifiers = Modifier.toString(parameter.modifier);
        return modifiers + (modifiers.isEmpty() ? "" : " ")
               + Types.className(parameter.getType()) + " " + parameter.name;
      }

      public int size() {
        return parameters.size();
      }
    }).append(' ').append(body);
    writer.newlineAndIndent();
  }
}

// End ConstructorDeclaration.java
