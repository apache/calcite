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
import java.util.List;

/**
 * <p>Helper methods concerning {@link BlockExpression}s.</p>
 */
public final class Blocks {
    private Blocks() {
        throw new AssertionError("no blocks for you!");
    }


    private static BlockExpression toFunctionBlock(Node body, boolean function)
    {
        if (body instanceof BlockExpression) {
            return (BlockExpression) body;
        }
        Statement statement;
        if (body instanceof Statement) {
            statement = (Statement) body;
        } else if (body instanceof Expression) {
            if (Types.toClass(body.getType()) == Void.TYPE && function) {
                statement = Expressions.statement((Expression) body);
            } else {
                statement = Expressions.return_(null, (Expression) body);
            }
        } else {
            throw new AssertionError(
                "block cannot contain node that is neither statement nor "
                + "expression: " + body);
        }
        return Expressions.block(statement);
    }

    public static BlockExpression toFunctionBlock(Node body) {
        return toFunctionBlock(body, true);
    }

    public static BlockExpression toBlock(Node body) {
        return toFunctionBlock(body, false);
    }

    /** Prepends a statement to a block. */
    public static BlockExpression create(
        Statement statement,
        BlockExpression block)
    {
        return Expressions.block(
            Expressions.list(statement)
                .appendAll(block.statements));
    }

    /** Prepends a list of statements to a block. */
    public static BlockExpression create(
        Iterable<Statement> statements,
        BlockExpression block)
    {
        return Expressions.block(
            Expressions.list(statements)
                .appendAll(block.statements));
    }

    /** Creates a block from a list of nodes. Nodes that are expressions are
     * converted to statements; nodes that are blocks are merged into the
     * greater block. */
    public static BlockExpression create(Node... nodes) {
        Expressions.FluentList<Statement> list = Expressions.list();
        for (Node node : nodes) {
            add(list, node);
        }
        return Expressions.block(list);
    }

    private static void add(Expressions.FluentList<Statement> list, Node node) {
        if (node instanceof BlockExpression) {
            for (Statement statement : ((BlockExpression) node).statements) {
                add(list, statement);
            }
        } else if (node instanceof Expression) {
            list.add(Expressions.statement((Expression) node));
        } else {
            list.add((Statement) node);
        }
    }

    /** Appends a block to a list of statements and returns an expression
     * (possibly a variable) that represents the result of the newly added
     * block. */
    public static Expression append(
        List<Statement> statements,
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
            statements.add(statement);
            if (i == block.statements.size() - 1) {
                if (statement instanceof DeclarationExpression) {
                    result = ((DeclarationExpression) statement).parameter;
                } else if (statement instanceof GotoExpression) {
                    statements.remove(statements.size() - 1);
                    result = ((GotoExpression) statement).expression;
                    if (result instanceof ParameterExpression
                        || result instanceof ConstantExpression)
                    {
                        // already simple; no need to declare a variable or even
                        // to evaluate the expression
                    } else {
                        DeclarationExpression declare =
                            Expressions.declare(
                                Modifier.FINAL, name, result);
                        statements.add(declare);
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
    public static Expression append(
        List<Statement> statements,
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
        statements.add(declare);
        return block;
    }


    /** Converts a simple "{ return expr; }" block into "expr"; otherwise
     * throws. */
    public static Expression simple(BlockExpression block) {
        if (block.statements.size() == 1) {
            Statement statement = block.statements.get(0);
            if (statement instanceof GotoExpression) {
                return ((GotoExpression) statement).expression;
            }
        }
        throw new AssertionError("not a simple block: " + block);
    }
}

// End Blocks.java
