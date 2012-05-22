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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.List;

/**
 * Represents a call to either a static or an instance method.
 */
public class MethodCallExpression extends Expression {
    public final Method method;
    public final Expression targetExpression; // null for call to static method
    public final List<Expression> expressions;

    MethodCallExpression(
        Type returnType,
        Method method,
        Expression targetExpression,
        List<Expression> expressions)
    {
        super(ExpressionType.Call, returnType);
        this.method = method;
        this.targetExpression = targetExpression;
        this.expressions = expressions;
        assert expressions != null;
        assert returnType != null;
        assert (targetExpression == null)
               == Modifier.isStatic(method.getModifiers());
        assert Types.toClass(returnType) == method.getReturnType();
    }

    MethodCallExpression(
        Method method,
        Expression targetExpression,
        List<Expression> expressions)
    {
        this(method.getReturnType(), method, targetExpression, expressions);
    }

    @Override
    public Object evaluate(Evaluator evaluator) {
        final Object target;
        if (targetExpression == null) {
            target = null;
        } else {
            target = targetExpression.evaluate(evaluator);
        }
        final Object[] args = new Object[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            Expression expression = expressions.get(i);
            args[i] = expression.evaluate(evaluator);
        }
        try {
            return method.invoke(target, args);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("error while evaluating " + this, e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("error while evaluating " + this, e);
        }
    }

    @Override
    void accept(ExpressionWriter writer, int lprec, int rprec) {
        if (writer.requireParentheses(this, lprec, rprec)) {
            return;
        }
        if (targetExpression != null) {
            // instance method
            targetExpression.accept(writer, lprec, nodeType.lprec);
        } else {
            // static method
            writer.append(method.getDeclaringClass());
        }
        writer.append('.')
            .append(method.getName())
            .append('(');
        int k = 0;
        for (Expression expression : expressions) {
            if (k++ > 0) {
                writer.append(", ");
            }
            expression.accept(writer, 0, 0);
        }
        writer.append(')');
    }
}

// End MethodCallExpression.java
