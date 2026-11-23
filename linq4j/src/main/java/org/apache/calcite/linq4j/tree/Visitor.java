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

import org.jspecify.annotations.Nullable;

/**
 * Node visitor.
 *
 * @param <R> Return type
 */
public interface Visitor<R extends @Nullable Object> {
  @Nullable R visit(BinaryExpression binaryExpression);
  @Nullable R visit(BlockStatement blockStatement);
  @Nullable R visit(ClassDeclaration classDeclaration);
  @Nullable R visit(ConditionalExpression conditionalExpression);
  @Nullable R visit(ConditionalStatement conditionalStatement);
  @Nullable R visit(ConstantExpression constantExpression);
  @Nullable R visit(ConstructorDeclaration constructorDeclaration);
  @Nullable R visit(DeclarationStatement declarationStatement);
  @Nullable R visit(DefaultExpression defaultExpression);
  @Nullable R visit(DynamicExpression dynamicExpression);
  @Nullable R visit(FieldDeclaration fieldDeclaration);
  @Nullable R visit(ForStatement forStatement);
  @Nullable R visit(ForEachStatement forEachStatement);
  @Nullable R visit(FunctionExpression functionExpression);
  @Nullable R visit(GotoStatement gotoStatement);
  @Nullable R visit(IndexExpression indexExpression);
  @Nullable R visit(InvocationExpression invocationExpression);
  @Nullable R visit(LabelStatement labelStatement);
  @Nullable R visit(LambdaExpression lambdaExpression);
  @Nullable R visit(ListInitExpression listInitExpression);
  @Nullable R visit(MemberExpression memberExpression);
  @Nullable R visit(MemberInitExpression memberInitExpression);
  @Nullable R visit(MethodCallExpression methodCallExpression);
  @Nullable R visit(MethodDeclaration methodDeclaration);
  @Nullable R visit(NewArrayExpression newArrayExpression);
  @Nullable R visit(NewExpression newExpression);
  @Nullable R visit(ParameterExpression parameterExpression);
  @Nullable R visit(SwitchStatement switchStatement);
  @Nullable R visit(TernaryExpression ternaryExpression);
  @Nullable R visit(ThrowStatement throwStatement);
  @Nullable R visit(TryStatement tryStatement);
  @Nullable R visit(TypeBinaryExpression typeBinaryExpression);
  @Nullable R visit(UnaryExpression unaryExpression);
  @Nullable R visit(WhileStatement whileStatement);
}
