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
 * Represents a block that contains a sequence of expressions where variables
 * can be defined.
 */
public class BlockExpression extends Expression {
    private final List<Expression> expressions;

    BlockExpression(List<Expression> expressions, Type type) {
        super(ExpressionType.Block, type);
        this.expressions = expressions;
    }

    @Override
    void accept(ExpressionWriter writer, int lprec, int rprec) {
        if (expressions.isEmpty()) {
            writer.append("{}");
            return;
        }
        writer.begin("{\n");
        for (Expression expression : expressions) {
            expression.accept(writer, 0, 0);
            if (expression instanceof WhileExpression
                || expression instanceof ConditionalExpression
                || expression instanceof BlockExpression)
            {
                // contain blocks, therefore do not need semicolon
            } else {
                writer.append(';').newlineAndIndent();
            }
        }
        writer.end("}\n");
    }
}

// End BlockExpression.java
