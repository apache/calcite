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

import net.hydromatic.linq4j.function.Function;

/**
 * Represents a strongly typed lambda expression as a data structure in the form
 * of an expression tree. This class cannot be inherited.
 */
public final class FunctionExpression<F extends Function<?>>
    extends LambdaExpression
{
    private final Class<F> clazz;
    private final F function;

    public FunctionExpression(
        ExpressionType nodeType,
        Class<F> clazz,
        F function)
    {
        super(nodeType);
        this.clazz = clazz;
        this.function = function;
    }

    public F getFunction() {
        return function;
    }
}

// End FunctionExpression.java
