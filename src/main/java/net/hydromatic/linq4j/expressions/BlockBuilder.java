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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Builder for {@link BlockExpression}.
 *
 * <p>Has methods that help ensure that variable names are unique.</p>
 *
 * @author jhyde
 */
public class BlockBuilder {
    final List<Statement> statements = new ArrayList<Statement>();
    final Set<String> variables = new HashSet<String>();

    /** Appends a block to a list of statements and returns an expression
     * (possibly a variable) that represents the result of the newly added
     * block. */
    public Expression append(
        String name,
        BlockExpression block)
    {
        if (statements.size() > 0) {
            Statement lastStatement = statements.get(statements.size() - 1);
            if (lastStatement instanceof GotoExpression) {
                // convert "return expr;" into "expr;"
                statements.set(
                    statements.size() - 1,
                    Expressions.statement(
                        ((GotoExpression) lastStatement).expression));
            }
        }
        Expression result = null;
        for (int i = 0; i < block.statements.size(); i++) {
            Statement statement = block.statements.get(i);
            add(statement);
            if (i == block.statements.size() - 1) {
                if (statement instanceof DeclarationExpression) {
                    result = ((DeclarationExpression) statement).parameter;
                } else if (statement instanceof GotoExpression) {
                    statements.remove(statements.size() - 1);
                    result = ((GotoExpression) statement).expression;
                    if (result instanceof ParameterExpression
                        || result instanceof ConstantExpression)
                    {
                        // already simple; no need to declare a variable or
                        // even to evaluate the expression
                    } else {
                        DeclarationExpression declare =
                            Expressions.declare(
                                Modifier.FINAL, newName(name), result);
                        add(declare);
                        result = declare.parameter;
                    }
                } else {
                    // not an expression -- result remains null
                }
            }
        }
        return result;
    }

    /** Appends an expression to a list of statements, and returns an expression
     * (possibly a variable) that represents the result of the newly added
     * block. */
    public Expression append(
        String name,
        Expression block)
    {
        if (statements.size() > 0) {
            Statement lastStatement = statements.get(statements.size() - 1);
            if (lastStatement instanceof GotoExpression) {
                // convert "return expr;" into "expr;"
                statements.set(
                    statements.size() - 1,
                    Expressions.statement(
                        ((GotoExpression) lastStatement).expression));
            }
        }
        DeclarationExpression declare =
            Expressions.declare(
                Modifier.FINAL, name, block);
        add(declare);
        return block;
    }

    public void add(Statement statement) {
        statements.add(statement);
        if (statement instanceof DeclarationExpression) {
            String name =
                ((DeclarationExpression) statement).parameter.name;
            if (!variables.add(name)) {
                throw new AssertionError("duplicate variable " + name);
            }
        }
    }

    /** Returns a block consisting of the current list of statements. */
    public BlockExpression toBlock() {
        return Expressions.block(statements);
    }

    /** Creates a name for a new variable, unique within this block. */
    private String newName(String suggestion) {
        int i = 0;
        String candidate = suggestion;
        for (;;) {
            if (!variables.contains(candidate)) {
                return candidate;
            }
            candidate = suggestion + (i++);
        }
    }

    public BlockBuilder append(Expression expression) {
        statements.add(Expressions.statement(expression));
        return this;
    }
}

// End BlockBuilder.java
