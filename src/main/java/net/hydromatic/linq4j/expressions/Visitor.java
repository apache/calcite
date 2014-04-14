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

import net.hydromatic.linq4j.Linq4j;

import java.util.List;

/**
 * Node visitor.
 */
public class Visitor {
  public Statement visit(WhileStatement whileStatement, Expression condition,
      Statement body) {
    return condition == whileStatement.condition
           && body == whileStatement.body
        ? whileStatement
        : Expressions.while_(condition, body);
  }

  public Statement visit(ConditionalStatement conditionalStatement,
                   List<Node> list) {
    return list.equals(conditionalStatement.expressionList)
        ? conditionalStatement
        : Expressions.ifThenElse(list);
  }

  public BlockStatement visit(BlockStatement blockStatement,
      List<Statement> statements) {
    return statements.equals(blockStatement.statements)
        ? blockStatement
        : Expressions.block(statements);
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

  public Statement visit(ThrowStatement throwStatement, Expression expression) {
    return expression == throwStatement.expression
        ? throwStatement
        : Expressions.throw_(expression);
  }

  public DeclarationStatement visit(DeclarationStatement declarationStatement,
      ParameterExpression parameter, Expression initializer) {
    return declarationStatement.parameter == parameter
           && declarationStatement.initializer == initializer
        ? declarationStatement
        : Expressions.declare(
            declarationStatement.modifiers, parameter,
            initializer);
  }

  public Expression visit(LambdaExpression lambdaExpression) {
    return lambdaExpression;
  }

  public Expression visit(FunctionExpression functionExpression,
      BlockStatement body, List<ParameterExpression> parameterList) {
    return functionExpression.body.equals(body)
           && functionExpression.parameterList.equals(parameterList)
        ? functionExpression
        : Expressions.lambda(body, parameterList);
  }

  public Expression visit(BinaryExpression binaryExpression,
      Expression expression0, Expression expression1) {
    return binaryExpression.expression0 == expression0
           && binaryExpression.expression1 == expression1
        ? binaryExpression
        : Expressions.makeBinary(binaryExpression.nodeType, expression0,
            expression1);
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

  public Expression visit(IndexExpression indexExpression, Expression array,
      List<Expression> indexExpressions) {
    return indexExpression.array == array
           && indexExpression.indexExpressions.equals(indexExpressions)
        ? indexExpression
        : new IndexExpression(array, indexExpressions);
  }

  public Expression visit(UnaryExpression unaryExpression,
      Expression expression) {
    return unaryExpression.expression == expression
        ? unaryExpression
        : Expressions.makeUnary(unaryExpression.nodeType, expression,
            unaryExpression.type, null);
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

  public Expression visit(MemberExpression memberExpression,
      Expression expression) {
    return memberExpression.expression == expression
        ? memberExpression
        : Expressions.field(expression, memberExpression.field);
  }

  public Expression visit(InvocationExpression invocationExpression) {
    return invocationExpression;
  }

  static <T> boolean eq(T t0, T t1) {
    return t0 == t1 || t0 != null && t1 != null && t0.equals(t1);
  }

  public Expression visit(NewArrayExpression newArrayExpression, int dimension,
      Expression bound, List<Expression> expressions) {
    return eq(expressions, newArrayExpression.expressions)
        && eq(bound, newArrayExpression.bound)
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

  public Expression visit(NewExpression newExpression,
      List<Expression> arguments, List<MemberDeclaration> memberDeclarations) {
    return arguments.equals(newExpression.arguments)
        && Linq4j.equals(memberDeclarations, newExpression.memberDeclarations)
        ? newExpression
        : Expressions.new_(newExpression.type, arguments, memberDeclarations);
  }

  public Statement visit(SwitchStatement switchStatement) {
    return switchStatement;
  }

  public Statement visit(TryStatement tryStatement) {
    return tryStatement;
  }

  public Expression visit(MemberInitExpression memberInitExpression) {
    return memberInitExpression;
  }

  public Expression visit(TypeBinaryExpression typeBinaryExpression,
      Expression expression) {
    return typeBinaryExpression;
  }

  public MemberDeclaration visit(MethodDeclaration methodDeclaration,
      List<ParameterExpression> parameters, BlockStatement body) {
    return parameters.equals(methodDeclaration.parameters)
        && body.equals(methodDeclaration.body)
        ? methodDeclaration
        : Expressions.methodDecl(methodDeclaration.modifier,
             methodDeclaration.resultType, methodDeclaration.name, parameters,
             body);
  }

  public MemberDeclaration visit(FieldDeclaration fieldDeclaration,
      ParameterExpression parameter, Expression initializer) {
    return parameter.equals(fieldDeclaration.parameter)
        && eq(initializer, fieldDeclaration.initializer)
        ? fieldDeclaration
        : Expressions.fieldDecl(fieldDeclaration.modifier, parameter,
            initializer);
  }

  public Expression visit(ParameterExpression parameterExpression) {
    return parameterExpression;
  }

  public ConstantExpression visit(ConstantExpression constantExpression) {
    return constantExpression;
  }

  public ClassDeclaration visit(ClassDeclaration classDeclaration,
      List<MemberDeclaration> memberDeclarations) {
    return classDeclaration;
  }

  public MemberDeclaration visit(ConstructorDeclaration constructorDeclaration,
      List<ParameterExpression> parameters, BlockStatement body) {
    return constructorDeclaration;
  }
}

// End Visitor.java
