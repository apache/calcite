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

import net.hydromatic.linq4j.function.Function1;
import net.hydromatic.linq4j.function.Functions;

import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;

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
    this.name = name;
    this.resultType = resultType;
    this.parameters = parameters;
    this.body = body;
  }

  @Override
  public MemberDeclaration accept(Visitor visitor) {
    // do not visit parameters
    final BlockStatement body = this.body.accept(visitor);
    return visitor.visit(this, parameters, body);
  }

  public void accept(ExpressionWriter writer) {
    String modifiers = Modifier.toString(modifier);
    writer.append(modifiers);
    if (!modifiers.isEmpty()) {
      writer.append(' ');
    }
    writer.append(resultType).append(' ').append(name).list("(", ", ", ")",
        Functions.adapt(parameters, new Function1<ParameterExpression,
            String>() {
          public String apply(ParameterExpression a0) {
            return a0.declString();
          }
        })
    ).append(' ').append(body);
    writer.newlineAndIndent();
  }
}

// End MethodDeclaration.java
