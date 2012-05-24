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
    public final Expression body;

    public MethodDeclaration(
        int modifier,
        String name,
        Type resultType,
        List<ParameterExpression> parameters,
        Expression body)
    {
        this.modifier = modifier;
        this.name = name;
        this.resultType = resultType;
        this.parameters = parameters;
        this.body = body;
    }
}

// End MethodDeclaration.java
