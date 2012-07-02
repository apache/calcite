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

import java.util.List;

/**
 * Node visitor.
 *
 * @author jhyde
 */
public class Visitor {
    public Statement visit(
        WhileExpression whileExpression,
        Expression condition,
        Statement body)
    {
        return (condition != whileExpression.condition
            || body != whileExpression.body)
            ? Expressions.while_(condition,  body)
            : whileExpression;
    }

    public BlockExpression visit(
        BlockExpression blockExpression, List<Statement> statements)
    {
        return statements.equals(blockExpression.statements)
            ? blockExpression
            : Expressions.block(statements);
    }

    public Statement visit(
        GotoExpression gotoExpression, Expression expression)
    {
        return expression != gotoExpression.expression
            ? Expressions.goto_(
                gotoExpression.labelTarget,
                gotoExpression.expression)
            : gotoExpression;
    }

    public LabelExpression visit(LabelExpression labelExpression) {
        return labelExpression;
    }

    public LoopExpression visit(LoopExpression loopExpression) {
        return loopExpression;
    }

    public Statement visit(
        DeclarationExpression declarationExpression,
        ParameterExpression parameter,
        Expression initializer)
    {
        return declarationExpression.parameter == parameter
            && declarationExpression.initializer == initializer
            ? declarationExpression
            : Expressions.declare(
                declarationExpression.modifiers,
                declarationExpression.parameter,
                declarationExpression.initializer);
    }

    public Expression visit(LambdaExpression lambdaExpression) {
        return lambdaExpression;
    }

    public Expression visit(
        FunctionExpression functionExpression,
        BlockExpression body,
        List<ParameterExpression> parameterList)
    {
        return functionExpression.body.equals(body)
            && functionExpression.parameterList.equals(parameterList)
            ? functionExpression
            : Expressions.lambda(body, parameterList);
    }

    public Expression visit(
        BinaryExpression binaryExpression,
        Expression expression0,
        Expression expression1)
    {
        return binaryExpression.expression0 == expression0
            && binaryExpression.expression1 == expression1
            ? binaryExpression
            : Expressions.makeBinary(
                binaryExpression.nodeType,
                binaryExpression.expression0,
                binaryExpression.expression1);
    }

    public Expression visit(
        IndexExpression indexExpression,
        Expression array,
        List<Expression> indexExpressions)
    {
        return indexExpression.array == array
            && indexExpression.indexExpressions == indexExpressions
            ? indexExpression
            : new IndexExpression(array, indexExpressions);
    }

    public Expression visit(
        UnaryExpression unaryExpression,
        Expression expression)
    {
        return unaryExpression.expression == expression
            ? unaryExpression
            : Expressions.makeUnary(unaryExpression.nodeType, expression);
    }

    public Expression visit(
        MethodCallExpression methodCallExpression,
        Expression targetExpression,
        List<Expression> expressions)
    {
        return methodCallExpression.targetExpression == targetExpression
            && methodCallExpression.expressions.equals(expressions)
            ? methodCallExpression
            : Expressions.call(
                methodCallExpression.targetExpression,
                methodCallExpression.method,
                methodCallExpression.expressions);
    }

    public Expression visit(DefaultExpression defaultExpression) {
        return defaultExpression;
    }

    public Expression visit(DynamicExpression dynamicExpression) {
        return dynamicExpression;
    }

    public Expression visit(
        MemberExpression memberExpression, Expression expression)
    {
        return memberExpression.expression == expression
            ? memberExpression
            : Expressions.field(expression, memberExpression.field);
    }
}

// End Visitor.java
