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

/**
 * Node visitor.
 *
 * @param <R> Return type
 */
public interface Visitor<R> {
  R visit(BinaryExpression binaryExpression);
  R visit(BlockStatement blockStatement);
  R visit(ClassDeclaration classDeclaration);
  R visit(ConditionalExpression conditionalExpression);
  R visit(ConditionalStatement conditionalStatement);
  R visit(ConstantExpression constantExpression);
  R visit(ConstructorDeclaration constructorDeclaration);
  R visit(DeclarationStatement declarationStatement);
  R visit(DefaultExpression defaultExpression);
  R visit(DynamicExpression dynamicExpression);
  R visit(FieldDeclaration fieldDeclaration);
  R visit(ForStatement forStatement);
  R visit(ForEachStatement forEachStatement);
  R visit(FunctionExpression functionExpression);
  R visit(GotoStatement gotoStatement);
  R visit(IndexExpression indexExpression);
  R visit(InvocationExpression invocationExpression);
  R visit(LabelStatement labelStatement);
  R visit(LambdaExpression lambdaExpression);
  R visit(ListInitExpression listInitExpression);
  R visit(MemberExpression memberExpression);
  R visit(MemberInitExpression memberInitExpression);
  R visit(MethodCallExpression methodCallExpression);
  R visit(MethodDeclaration methodDeclaration);
  R visit(NewArrayExpression newArrayExpression);
  R visit(NewExpression newExpression);
  R visit(ParameterExpression parameterExpression);
  R visit(SwitchStatement switchStatement);
  R visit(TernaryExpression ternaryExpression);
  R visit(ThrowStatement throwStatement);
  R visit(TryStatement tryStatement);
  R visit(TypeBinaryExpression typeBinaryExpression);
  R visit(UnaryExpression unaryExpression);
  R visit(WhileStatement whileStatement);
}

// End Visitor.java
