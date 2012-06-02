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
    public final F function;
    public final Expression body;
    public final List<ParameterExpression> parameterList;
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
                new Class[] {Types.toClass(type)},
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
        // "new Function1() {
        //    public Result apply(T1 p1, ...) {
        //        <body>
        //    }
        //    // bridge method
        //    public Object apply(Object p1, ...) {
        //        return apply((T1) p1, ...);
        //    }
        // }
        List<String> params = new ArrayList<String>();
        List<String> bridgeParams = new ArrayList<String>();
        List<String> bridgeArgs = new ArrayList<String>();
        for (ParameterExpression parameterExpression : parameterList) {
            params.add(
                Types.boxClassName(parameterExpression.getType())
                + " "
                + parameterExpression.name);
            bridgeParams.add(
                "Object "
                + parameterExpression.name);
            bridgeArgs.add(
                "("
                + Types.boxClassName(parameterExpression.getType())
                + ") "
                + parameterExpression.name);
        }
        writer.append("new ")
            .append(type)
            .append("()")
            .begin(" {\n")
            .append("public ")
            .append(Types.boxClassName(body.getType()))
            .list(" apply(", ", ", ") ", params)
            .append(toFunctionBlock(body));
        if (true) {
            writer.list("public Object apply(", ", ", ") ", bridgeParams)
                .begin("{\n")
                .list("return apply(\n", ",\n", ");\n", bridgeArgs)
                .end("}\n");
        }
        writer.end("}\n");
    }

    static BlockExpression toFunctionBlock(Expression body) {
        if (body instanceof BlockExpression) {
            return (BlockExpression) body;
        }
        if (!(body instanceof GotoExpression)
            && Types.toClass(body.getType()) != Void.TYPE)
        {
            body = Expressions.return_(null, body);
        }
        return Expressions.block(body);
    }

    static BlockExpression toBlock(Expression body) {
        if (body instanceof BlockExpression) {
            return (BlockExpression) body;
        }
        return Expressions.block(body);
    }

    public interface Invokable {
        Object dynamicInvoke(Object... args);
    }
}

// End FunctionExpression.java
