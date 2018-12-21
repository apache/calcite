/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.linq4j.tree;

import java.util.List;
import java.util.Objects;

/**
 * Extension to {@link Visitor} that returns a mutated tree.
 */
public class Shuttle {
  public Shuttle preVisit(WhileStatement whileStatement) {
    return this;
  }

  public Statement visit(WhileStatement whileStatement, Expression condition,
      Statement body) {
    return condition == whileStatement.condition
           && body == whileStatement.body
        ? whileStatement
        : Expressions.while_(condition, body);
  }

  public Shuttle preVisit(ConditionalStatement conditionalStatement) {
    return this;
  }

  public Statement visit(ConditionalStatement conditionalStatement,
      List<Node> list) {
    return list.equals(conditionalStatement.expressionList)
        ? conditionalStatement
        : Expressions.ifThenElse(list);
  }

  public Shuttle preVisit(BlockStatement blockStatement) {
    return this;
  }

  public BlockStatement visit(BlockStatement blockStatement,
      List<Statement> statements) {
    return statements.equals(blockStatement.statements)
        ? blockStatement
        : Expressions.block(statements);
  }

  public Shuttle preVisit(GotoStatement gotoStatement) {
    return this;
  }

  public Statement visit(GotoStatement gotoStatement, Expression expression) {
    return expression == gotoStatement.expression
        ? gotoStatement
        : Expressions.makeGoto(
            gotoStatement.kind, gotoStatement.labelTarget,
            expression);
  }

  public LabelStatement visit(LabelStatement labelStatement) {
    return labelStatement;
  }

  public Shuttle preVisit(ForStatement forStatement) {
    return this;
  }

  public ForStatement visit(ForStatement forStatement,
      List<DeclarationStatement> declarations, Expression condition,
      Expression post, Statement body) {
    return declarations.equals(forStatement.declarations)
        && condition == forStatement.condition
        && post == forStatement.post
        && body == forStatement.body
        ? forStatement
        : Expressions.for_(declarations, condition, post, body);
  }

  public Shuttle preVisit(ForEachStatement forEachStatement) {
    return this;
  }

  public ForEachStatement visit(ForEachStatement forEachStatement,
      ParameterExpression parameter, Expression iterable, Statement body) {
    return parameter.equals(forEachStatement.parameter)
        && iterable.equals(forEachStatement.iterable)
        && body == forEachStatement.body
        ? forEachStatement
        : Expressions.forEach(parameter, iterable, body);
  }

  public Shuttle preVisit(ThrowStatement throwStatement) {
    return this;
  }

  public Statement visit(ThrowStatement throwStatement, Expression expression) {
    return expression == throwStatement.expression
        ? throwStatement
        : Expressions.throw_(expression);
  }

  public Shuttle preVisit(DeclarationStatement declarationStatement) {
    return this;
  }

  public DeclarationStatement visit(DeclarationStatement declarationStatement,
      Expression initializer) {
    return declarationStatement.initializer == initializer
        ? declarationStatement
        : Expressions.declare(
            declarationStatement.modifiers, declarationStatement.parameter,
            initializer);
  }

  public Expression visit(LambdaExpression lambdaExpression) {
    return lambdaExpression;
  }

  public Shuttle preVisit(FunctionExpression functionExpression) {
    return this;
  }

  public Expression visit(FunctionExpression functionExpression,
      BlockStatement body) {
    return functionExpression.body.equals(body)
        ? functionExpression
        : Expressions.lambda(body, functionExpression.parameterList);
  }

  public Shuttle preVisit(BinaryExpression binaryExpression) {
    return this;
  }

  public Expression visit(BinaryExpression binaryExpression,
      Expression expression0, Expression expression1) {
    return binaryExpression.expression0 == expression0
           && binaryExpression.expression1 == expression1
        ? binaryExpression
        : Expressions.makeBinary(binaryExpression.nodeType, expression0,
            expression1);
  }

  public Shuttle preVisit(TernaryExpression ternaryExpression) {
    return this;
  }

  public Expression visit(TernaryExpression ternaryExpression,
      Expression expression0, Expression expression1, Expression expression2) {
    return ternaryExpression.expression0 == expression0
           && ternaryExpression.expression1 == expression1
           && ternaryExpression.expression2 == expression2
        ? ternaryExpression
        : Expressions.makeTernary(ternaryExpression.nodeType, expression0,
            expression1, expression2);
  }

  public Shuttle preVisit(IndexExpression indexExpression) {
    return this;
  }

  public Expression visit(IndexExpression indexExpression, Expression array,
      List<Expression> indexExpressions) {
    return indexExpression.array == array
           && indexExpression.indexExpressions.equals(indexExpressions)
        ? indexExpression
        : new IndexExpression(array, indexExpressions);
  }

  public Shuttle preVisit(UnaryExpression unaryExpression) {
    return this;
  }

  public Expression visit(UnaryExpression unaryExpression,
      Expression expression) {
    return unaryExpression.expression == expression
        ? unaryExpression
        : Expressions.makeUnary(unaryExpression.nodeType, expression,
            unaryExpression.type, null);
  }

  public Shuttle preVisit(MethodCallExpression methodCallExpression) {
    return this;
  }

  public Expression visit(MethodCallExpression methodCallExpression,
      Expression targetExpression, List<Expression> expressions) {
    return methodCallExpression.targetExpression == targetExpression
           && methodCallExpression.expressions.equals(expressions)
        ? methodCallExpression
        : Expressions.call(targetExpression, methodCallExpression.method,
            expressions);
  }

  public Expression visit(DefaultExpression defaultExpression) {
    return defaultExpression;
  }

  public Expression visit(DynamicExpression dynamicExpression) {
    return dynamicExpression;
  }

  public Shuttle preVisit(MemberExpression memberExpression) {
    return this;
  }

  public Expression visit(MemberExpression memberExpression,
      Expression expression) {
    return memberExpression.expression == expression
        ? memberExpression
        : Expressions.field(expression, memberExpression.field);
  }

  public Expression visit(InvocationExpression invocationExpression) {
    return invocationExpression;
  }

  public Shuttle preVisit(NewArrayExpression newArrayExpression) {
    return this;
  }

  public Expression visit(NewArrayExpression newArrayExpression, int dimension,
      Expression bound, List<Expression> expressions) {
    return Objects.equals(expressions, newArrayExpression.expressions)
        && Objects.equals(bound, newArrayExpression.bound)
        ? newArrayExpression
        : expressions == null
        ? Expressions.newArrayBounds(
            Types.getComponentTypeN(newArrayExpression.type), dimension, bound)
        : Expressions.newArrayInit(
            Types.getComponentTypeN(newArrayExpression.type),
            dimension, expressions);
  }

  public Expression visit(ListInitExpression listInitExpression) {
    return listInitExpression;
  }

  public Shuttle preVisit(NewExpression newExpression) {
    return this;
  }

  public Expression visit(NewExpression newExpression,
      List<Expression> arguments, List<MemberDeclaration> memberDeclarations) {
    return arguments.equals(newExpression.arguments)
        && Objects.equals(memberDeclarations, newExpression.memberDeclarations)
        ? newExpression
        : Expressions.new_(newExpression.type, arguments, memberDeclarations);
  }

  public Statement visit(SwitchStatement switchStatement) {
    return switchStatement;
  }

  public Shuttle preVisit(TryStatement tryStatement) {
    return this;
  }

  public Statement visit(TryStatement tryStatement,
      Statement body, List<CatchBlock> catchBlocks, Statement fynally) {
    return body.equals(tryStatement.body)
           && Objects.equals(catchBlocks, tryStatement.catchBlocks)
           && Objects.equals(fynally, tryStatement.fynally)
           ? tryStatement
           : new TryStatement(body, catchBlocks, fynally);
  }

  public Expression visit(MemberInitExpression memberInitExpression) {
    return memberInitExpression;
  }

  public Shuttle preVisit(TypeBinaryExpression typeBinaryExpression) {
    return this;
  }

  public Expression visit(TypeBinaryExpression typeBinaryExpression,
      Expression expression) {
    return typeBinaryExpression.expression == expression
        ? typeBinaryExpression
        : new TypeBinaryExpression(expression.getNodeType(), expression,
            expression.type);
  }

  public Shuttle preVisit(MethodDeclaration methodDeclaration) {
    return this;
  }

  public MemberDeclaration visit(MethodDeclaration methodDeclaration,
      BlockStatement body) {
    return body.equals(methodDeclaration.body)
        ? methodDeclaration
        : Expressions.methodDecl(methodDeclaration.modifier,
            methodDeclaration.resultType, methodDeclaration.name,
            methodDeclaration.parameters, body);
  }

  public Shuttle preVisit(FieldDeclaration fieldDeclaration) {
    return this;
  }

  public MemberDeclaration visit(FieldDeclaration fieldDeclaration,
      Expression initializer) {
    return Objects.equals(initializer, fieldDeclaration.initializer)
        ? fieldDeclaration
        : Expressions.fieldDecl(fieldDeclaration.modifier,
            fieldDeclaration.parameter, initializer);
  }

  public Expression visit(ParameterExpression parameterExpression) {
    return parameterExpression;
  }

  public ConstantExpression visit(ConstantExpression constantExpression) {
    return constantExpression;
  }

  public Shuttle preVisit(ClassDeclaration classDeclaration) {
    return this;
  }

  public ClassDeclaration visit(ClassDeclaration classDeclaration,
      List<MemberDeclaration> memberDeclarations) {
    return Objects.equals(memberDeclarations,
        classDeclaration.memberDeclarations)
        ? classDeclaration
        : Expressions.classDecl(classDeclaration.modifier,
            classDeclaration.name, classDeclaration.extended,
            classDeclaration.implemented, memberDeclarations);
  }

  public Shuttle preVisit(ConstructorDeclaration constructorDeclaration) {
    return this;
  }

  public MemberDeclaration visit(ConstructorDeclaration constructorDeclaration,
      BlockStatement body) {
    return body.equals(constructorDeclaration.body)
        ? constructorDeclaration
        : Expressions.constructorDecl(constructorDeclaration.modifier,
            constructorDeclaration.resultType,
            constructorDeclaration.parameters,
            body);
  }
}

// End Shuttle.java
