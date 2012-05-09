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

import java.lang.reflect.*;
import java.util.*;

/**
 * Represents a strongly typed lambda expression as a data structure in the form
 * of an expression tree. This class cannot be inherited.
 */
public final class FunctionExpression<F extends Function<?>>
    extends LambdaExpression
{
    private final F function;
    private final Expression body;
    private final List<ParameterExpression> parameterList;
    private F dynamicFunction;

    private FunctionExpression(
        Class<F> type,
        F function,
        Expression body,
        List<ParameterExpression> parameterList)
    {
        super(ExpressionType.Lambda, type);
        assert type != null;
        assert function != null || body != null;
        this.function = function;
        this.body = body;
        this.parameterList = parameterList;
    }

    public FunctionExpression(F function) {
        this(
            (Class) function.getClass(), function, null,
            Collections.<ParameterExpression>emptyList());
    }

    public FunctionExpression(
        Class<F> type,
        Expression body,
        List<ParameterExpression> parameters)
    {
        this(type, null, body, parameters);
    }

    public Invokable compile() {
        return new Invokable() {
            public Object dynamicInvoke(Object... args) {
                final Evaluator evaluator = new Evaluator();
                for (int i = 0; i < args.length; i++) {
                    evaluator.push(parameterList.get(i), args[i]);
                }
                return evaluator.evaluate(body);
            }
        };
    }

    public F getFunction() {
        if (function != null) {
            return function;
        }
        if (dynamicFunction == null) {
            final Invokable x = compile();

            //noinspection unchecked
            dynamicFunction = (F) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class[] {type},
                new InvocationHandler() {
                    public Object invoke(
                        Object proxy,
                        Method method,
                        Object[] args) throws Throwable
                    {
                        return x.dynamicInvoke(args);
                    }
                }
            );
        }
        return dynamicFunction;
    }

    @Override
    void accept(ExpressionWriter writer, int lprec, int rprec) {
        /*
        "new Function1() {
            Result apply(T1 p1, ...) {
                <body>
            }
        }
         */
        writer.append("new ")
            .append(type)
            .append("()");
        writer.begin(" {\n")
            .append(body.getType())
            .append(" apply(");
        int k = 0;
        for (ParameterExpression parameterExpression : parameterList) {
            if (k++ > 0) {
                writer.append(", ");
            }
            writer.append(parameterExpression.type)
                .append(" ")
                .append(parameterExpression.name);
        }
        writer.begin(") {\n");
        body.accept0(writer);
        writer.end("}\n");
        writer.end("}\n");
    }

    public interface Invokable {
        Object dynamicInvoke(Object... args);
    }
}

// End FunctionExpression.java
