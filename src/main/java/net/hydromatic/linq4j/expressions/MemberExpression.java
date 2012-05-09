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

import java.lang.reflect.Field;

/**
 * Represents accessing a field or property.
 */
public class MemberExpression extends Expression {
    private final Expression expression;
    private final Field field;

    public MemberExpression(Expression expression, Field field) {
        super(ExpressionType.MemberAccess, field.getType());
        this.expression = expression;
        this.field = field;
    }

    public Object evaluate(Evaluator evaluator) {
        final Object o = expression.evaluate(evaluator);
        try {
            return field.get(o);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("error while evaluating " + this, e);
        }
    }

    @Override
    void accept(ExpressionWriter writer, int lprec, int rprec) {
        if (writer.requireParentheses(this, lprec, rprec)) {
            return;
        }
        expression.accept(writer, lprec, nodeType.lprec);
        writer.append('.')
            .append(field.getName());
    }
}

// End MemberExpression.java
