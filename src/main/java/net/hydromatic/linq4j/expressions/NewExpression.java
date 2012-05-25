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

import java.lang.reflect.Constructor;
import java.lang.reflect.Member;
import java.util.List;

/**
 * Represents a constructor call.
 *
 * <p>If {@link #memberDeclarations} is not null (even if empty) represents
 * an anonymous class.</p>
 */
public class NewExpression extends Expression {
    public final Constructor constructor;
    public final List<Expression> arguments;
    public final List<MemberDeclaration> memberDeclarations;

    public NewExpression(
        Constructor constructor,
        List<Expression> arguments,
        List<Member> members, // not used
        List<MemberDeclaration> memberDeclarations)
    {
        super(ExpressionType.New, constructor.getDeclaringClass());
        this.constructor = constructor;
        this.arguments = arguments;
        this.memberDeclarations = memberDeclarations;
    }

    @Override
    void accept(ExpressionWriter writer, int lprec, int rprec) {
        writer.append("new ")
            .append(constructor.getDeclaringClass())
            .list("(\n", ",\n", ")", arguments);
        if (memberDeclarations != null) {
            writer.list("{\n", "\n\n", "}", memberDeclarations);
        }
    }
}

// End NewExpression.java
