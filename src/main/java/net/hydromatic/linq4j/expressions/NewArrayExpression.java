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
 * Represents creating a new array and possibly initializing the elements of the
 * new array.
 */
public class NewArrayExpression extends Expression {
    public final List<Expression> expressions;

    public NewArrayExpression(Type type, List<Expression> expressions) {
        super(ExpressionType.NewArrayInit, Types.arrayType(type));
        this.expressions = expressions;
    }

    @Override
    public Expression accept(Visitor visitor) {
        List<Expression> expressions =
            Expressions.acceptExpressions(this.expressions, visitor);
        return visitor.visit(this, expressions);
    }

    @Override
    void accept(ExpressionWriter writer, int lprec, int rprec) {
        writer.append("new ")
            .append(type)
            .list(" {\n", ",\n", "}", expressions);
    }
}

// End NewArrayExpression.java
