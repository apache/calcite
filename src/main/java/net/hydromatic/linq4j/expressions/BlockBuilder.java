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
    private final boolean optimizing;

    /** Creates a non-optimizing BlockBuilder. */
    public BlockBuilder() {
        this(true);
    }

    /** Creates a BlockBuilder.
     *
     * @param optimizing Whether to eliminate common sub-expressions
     */
    public BlockBuilder(boolean optimizing) {
        this.optimizing = optimizing;
    }

    /** Clears this BlockBuilder. */
    public void clear() {
        statements.clear();
        variables.clear();
    }

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
        Map<ParameterExpression, Expression> replacements =
            new HashMap<ParameterExpression, Expression>();
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
        }
        if (optimizing) {
            for (Statement statement : statements) {
                if (statement instanceof DeclarationExpression) {
                    DeclarationExpression decl =
                        (DeclarationExpression) statement;
                    if ((decl.modifiers & Modifier.FINAL) != 0
                        && decl.initializer != null
                        && decl.initializer.equals(expression))
                    {
                        return decl.parameter;
                    }
                }
            }
        }
        DeclarationExpression declare =
            Expressions.declare(
                Modifier.FINAL, newName(name), expression);
        add(declare);
        return declare.parameter;
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
        if (optimizing) {
            optimize();
        }
        return Expressions.block(statements);
    }

    /** Optimizes the list of statements. If an expression is used only once,
     * it is inlined. */
    private void optimize() {
        List<Slot> slots = new ArrayList<Slot>();
        final UseCounter useCounter = new UseCounter();
        for (Statement statement : statements) {
            if (statement instanceof DeclarationExpression) {
                final Slot slot =
                    new Slot((DeclarationExpression) statement);
                useCounter.map.put(slot.parameter, slot);
                slots.add(slot);
            }
        }
        for (Statement statement : statements) {
            statement.accept(useCounter);
        }
        final Map<ParameterExpression, Expression> subMap =
            new HashMap<ParameterExpression, Expression>();
        final SubstituteVariableVisitor visitor =
            new SubstituteVariableVisitor(subMap);
        final ArrayList<Statement> oldStatements =
            new ArrayList<Statement>(statements);
        statements.clear();
        for (Statement oldStatement : oldStatements) {
            if (oldStatement instanceof DeclarationExpression) {
                DeclarationExpression statement =
                    (DeclarationExpression) oldStatement;
                final Slot slot = useCounter.map.get(statement.parameter);
                int count = slot.count;
                if (slot.expression instanceof ConstantExpression
                    && ((ConstantExpression) slot.expression).value == null)
                {
                    // Don't allow 'final Type t = null' to be inlined. There
                    // is an implicit cast.
                    count = 100;
                }
                if (slot.expression instanceof NewExpression
                    && ((NewExpression) slot.expression).memberDeclarations
                       != null)
                {
                    // Don't inline anonymous inner classes. Janino gets
                    // confused referencing variables from deeply nested
                    // anonymous classes.
                    count = 100;
                }
                switch (count) {
                case 0:
                    // Only declared, never used. Throw away declaration.
                    break;
                case 1:
                    // declared, used once. inline it.
                    subMap.put(slot.parameter, slot.expression);
                    break;
                default:
                    statements.add(statement);
                    break;
                }
            } else {
                statements.add(oldStatement.accept(visitor));
            }
        }
        if (!subMap.isEmpty()) {
            oldStatements.clear();
            oldStatements.addAll(statements);
            statements.clear();
            for (Statement oldStatement : oldStatements) {
                statements.add(oldStatement.accept(visitor));
            }
        }
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
        private final Map<ParameterExpression, Expression> map;
        private final Map<ParameterExpression, Boolean> actives =
            new IdentityHashMap<ParameterExpression, Boolean>();

        public SubstituteVariableVisitor(
            Map<ParameterExpression, Expression> map)
        {
            this.map = map;
        }

        @Override
        public Expression visit(ParameterExpression parameterExpression) {
            Expression e = map.get(parameterExpression);
            if (e != null) {
                try {
                    final Boolean put = actives.put(parameterExpression, true);
                    if (put != null) {
                        throw new AssertionError(
                            "recursive expansion of " + parameterExpression
                            + " in " + actives.keySet());
                    }
                    // recursively substitute
                    return e.accept(this);
                } finally {
                    actives.remove(parameterExpression);
                }
            }
            return super.visit(parameterExpression);
        }
    }

    private static class UseCounter extends Visitor {
        private final Map<ParameterExpression, Slot> map =
            new HashMap<ParameterExpression, Slot>();

        public Expression visit(ParameterExpression parameter) {
            final Slot slot = map.get(parameter);
            if (slot != null) {
                // Count use of parameter, if it's registered. It's OK if
                // parameter is not registered. It might be beyond the control
                // of this block.
                slot.count++;
            }
            return super.visit(parameter);
        }
    }

    /** Workspace for optimization. */
    private static class Slot {
        private final ParameterExpression parameter;
        private final Expression expression;
        private int count;

        public Slot(DeclarationExpression declarationExpression) {
            this.parameter = declarationExpression.parameter;
            this.expression = declarationExpression.initializer;
        }
    }
}

// End BlockBuilder.java
