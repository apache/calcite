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
import java.util.*;

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
        Map<ParameterExpression, ParameterExpression> replacements =
            new HashMap<ParameterExpression, ParameterExpression>();
        final Visitor visitor =
            new SubstituteVariableVisitor(replacements);
        for (int i = 0; i < block.statements.size(); i++) {
            Statement statement = block.statements.get(i);
            if (!replacements.isEmpty()) {
                // Save effort, and only substitute variables if there are some.
                statement = statement.accept(visitor);
            }
            if (statement instanceof DeclarationExpression) {
                DeclarationExpression declaration =
                    (DeclarationExpression) statement;
                if (variables.contains(declaration.parameter.name)) {
                    append(
                        newName(declaration.parameter.name),
                        declaration.initializer);
                    statement = statements.get(statements.size() - 1);
                    ParameterExpression parameter2 =
                        ((DeclarationExpression) statement).parameter;
                    replacements.put(declaration.parameter, parameter2);
                } else {
                    add(statement);
                }
            } else {
                add(statement);
            }
            if (i == block.statements.size() - 1) {
                if (statement instanceof DeclarationExpression) {
                    result = ((DeclarationExpression) statement).parameter;
                } else if (statement instanceof GotoExpression) {
                    statements.remove(statements.size() - 1);
                    result =
                        append_(name, ((GotoExpression) statement).expression);
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
        return append_(name, block);
    }

    private Expression append_(String name, Expression expression) {
        // We treat "1" and "null" as atoms, but not "(Comparator) null".
        if (expression instanceof ParameterExpression
            || (expression instanceof ConstantExpression
                && (((ConstantExpression) expression).value != null
                    || expression.type == Object.class)))
        {
            // already simple; no need to declare a variable or
            // even to evaluate the expression
            return expression;
        } else {
            DeclarationExpression declare =
                Expressions.declare(
                    Modifier.FINAL, newName(name), expression);
            add(declare);
            return declare.parameter;
        }
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

    public void add(Expression expression) {
        add(Expressions.return_(null, expression));
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
        add(expression);
        return this;
    }

    private static class SubstituteVariableVisitor extends Visitor {
        private final Map<ParameterExpression, ParameterExpression> map;

        public SubstituteVariableVisitor(
            Map<ParameterExpression, ParameterExpression> map)
        {
            this.map = map;
        }

        @Override
        public ParameterExpression visit(
            ParameterExpression parameterExpression)
        {
            ParameterExpression e = map.get(parameterExpression);
            if (e != null) {
                return e;
            }
            return super.visit(parameterExpression);
        }
    }
}

// End BlockBuilder.java
