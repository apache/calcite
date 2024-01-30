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

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

/**
 * Default implementation of {@link Visitor}, which traverses a tree but does
 * nothing. In a derived class you can override selected methods.
 *
 * @param <R> Return type
 */
@SuppressWarnings("unused")
public class VisitorImpl<@Nullable R> implements Visitor<R> {
  public VisitorImpl() {
    super();
  }

  @Override public R visit(BinaryExpression binaryExpression) {
    R r0 = binaryExpression.expression0.accept(this);
    R r1 = binaryExpression.expression1.accept(this);
    return r1;
  }

  @Override public R visit(BlockStatement blockStatement) {
    return Expressions.acceptNodes(blockStatement.statements, this);
  }

  @Override public R visit(ClassDeclaration classDeclaration) {
    return Expressions.acceptNodes(classDeclaration.memberDeclarations, this);
  }

  @Override public R visit(ConditionalExpression conditionalExpression) {
    return Expressions.acceptNodes(conditionalExpression.expressionList, this);
  }

  @Override public R visit(ConditionalStatement conditionalStatement) {
    return Expressions.acceptNodes(conditionalStatement.expressionList, this);
  }

  @Override public R visit(ConstantExpression constantExpression) {
    return null;
  }

  @Override public R visit(ConstructorDeclaration constructorDeclaration) {
    R r0 = Expressions.acceptNodes(constructorDeclaration.parameters, this);
    return constructorDeclaration.body.accept(this);
  }

  @Override public R visit(DeclarationStatement declarationStatement) {
    R r = declarationStatement.parameter.accept(this);
    if (declarationStatement.initializer != null) {
      r = declarationStatement.initializer.accept(this);
    }
    return r;
  }

  @Override public R visit(DefaultExpression defaultExpression) {
    return null;
  }

  @Override public R visit(DynamicExpression dynamicExpression) {
    return null;
  }

  @Override public R visit(FieldDeclaration fieldDeclaration) {
    R r0 = fieldDeclaration.parameter.accept(this);
    return fieldDeclaration.initializer == null ? null
        : fieldDeclaration.initializer.accept(this);
  }

  @Override public R visit(ForStatement forStatement) {
    R r0 = Expressions.acceptNodes(forStatement.declarations, this);
    R r1 = forStatement.condition == null ? null : forStatement.condition.accept(this);
    R r2 = forStatement.post == null ? null : forStatement.post.accept(this);
    return forStatement.body.accept(this);
  }

  @Override public R visit(ForEachStatement forEachStatement) {
    R r0 = forEachStatement.parameter.accept(this);
    R r1 = forEachStatement.iterable.accept(this);
    return forEachStatement.body.accept(this);
  }

  @Override public R visit(FunctionExpression functionExpression) {
    @SuppressWarnings("unchecked") final List<Node> parameterList =
        functionExpression.parameterList;
    R r0 = Expressions.acceptNodes(parameterList, this);
    return functionExpression.body == null ? null : functionExpression.body.accept(this);
  }

  @Override public R visit(GotoStatement gotoStatement) {
    return gotoStatement.expression == null ? null
        : gotoStatement.expression.accept(this);
  }

  @Override public R visit(IndexExpression indexExpression) {
    R r0 = indexExpression.array.accept(this);
    return Expressions.acceptNodes(indexExpression.indexExpressions, this);
  }

  @Override public R visit(InvocationExpression invocationExpression) {
    return null;
  }

  @Override public R visit(LabelStatement labelStatement) {
    return labelStatement.defaultValue.accept(this);
  }

  @Override public R visit(LambdaExpression lambdaExpression) {
    return null;
  }

  @Override public R visit(ListInitExpression listInitExpression) {
    return null;
  }

  @Override public R visit(MemberExpression memberExpression) {
    R r = null;
    if (memberExpression.expression != null) {
      r = memberExpression.expression.accept(this);
    }
    return r;
  }

  @Override public R visit(MemberInitExpression memberInitExpression) {
    return null;
  }

  @Override public R visit(MethodCallExpression methodCallExpression) {
    R r = null;
    if (methodCallExpression.targetExpression != null) {
      r = methodCallExpression.targetExpression.accept(this);
    }
    return Expressions.acceptNodes(methodCallExpression.expressions, this);
  }

  @Override public R visit(MethodDeclaration methodDeclaration) {
    R r0 = Expressions.acceptNodes(methodDeclaration.parameters, this);
    return methodDeclaration.body.accept(this);
  }

  @Override public R visit(NewArrayExpression newArrayExpression) {
    R r = null;
    if (newArrayExpression.bound != null) {
      r = newArrayExpression.bound.accept(this);
    }
    return Expressions.acceptNodes(newArrayExpression.expressions, this);
  }

  @Override public R visit(NewExpression newExpression) {
    R r0 = Expressions.acceptNodes(newExpression.arguments, this);
    return Expressions.acceptNodes(newExpression.memberDeclarations, this);
  }

  @Override public R visit(ParameterExpression parameterExpression) {
    return null;
  }

  @Override public R visit(SwitchStatement switchStatement) {
    return null;
  }

  @Override public R visit(TernaryExpression ternaryExpression) {
    R r0 = ternaryExpression.expression0.accept(this);
    R r1 = ternaryExpression.expression1.accept(this);
    return ternaryExpression.expression2.accept(this);
  }

  @Override public R visit(ThrowStatement throwStatement) {
    return throwStatement.expression.accept(this);
  }

  @Override public R visit(TryStatement tryStatement) {
    R r = tryStatement.body.accept(this);
    for (CatchBlock catchBlock : tryStatement.catchBlocks) {
      r = catchBlock.parameter.accept(this);
      r = catchBlock.body.accept(this);
    }
    if (tryStatement.fynally != null) {
      r = tryStatement.fynally.accept(this);
    }
    return r;
  }

  @Override public R visit(TypeBinaryExpression typeBinaryExpression) {
    return typeBinaryExpression.expression.accept(this);
  }

  @Override public R visit(UnaryExpression unaryExpression) {
    return unaryExpression.expression.accept(this);
  }

  @Override public R visit(WhileStatement whileStatement) {
    R r0 = whileStatement.condition.accept(this);
    return whileStatement.body.accept(this);
  }

}
