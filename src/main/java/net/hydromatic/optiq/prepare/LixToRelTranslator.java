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
package net.hydromatic.optiq.prepare;

import net.hydromatic.linq4j.Queryable;
import net.hydromatic.linq4j.expressions.*;
import net.hydromatic.optiq.BuiltinMethod;
import net.hydromatic.optiq.impl.java.JavaTypeFactory;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptTable;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates a tree of linq4j {@link Queryable} nodes to a tree of
 * {@link RelNode} planner nodes.
 *
 * @see QueryableRelBuilder
 */
class LixToRelTranslator implements RelOptTable.ToRelContext {
  final RelOptCluster cluster;
  private final Prepare preparingStmt;
  final JavaTypeFactory typeFactory;

  public LixToRelTranslator(RelOptCluster cluster, Prepare preparingStmt) {
    this.cluster = cluster;
    this.preparingStmt = preparingStmt;
    this.typeFactory = (JavaTypeFactory) cluster.getTypeFactory();
  }

  public RelOptCluster getCluster() {
    return cluster;
  }

  public Prepare getPreparingStmt() {
    return preparingStmt;
  }

  public <T> RelNode translate(Queryable<T> queryable) {
    QueryableRelBuilder<T> translatorQueryable =
        new QueryableRelBuilder<T>(this);
    return translatorQueryable.toRel(queryable);
  }

  public RelNode translate(Expression expression) {
    if (expression instanceof MethodCallExpression) {
      MethodCallExpression call = (MethodCallExpression) expression;
      BuiltinMethod method = BuiltinMethod.lookup(call.method);
      if (method == null) {
        throw new UnsupportedOperationException(
            "unknown method " + call.method);
      }
      RelNode child;
      switch (method) {
      case SELECT:
        child = translate(call.targetExpression);
        return new ProjectRel(
            cluster,
            child,
            toRex(
                child,
                (FunctionExpression) call.expressions.get(0)),
            null,
            ProjectRel.Flags.Boxed);

      case WHERE:
        child = translate(call.targetExpression);
        return new FilterRel(
            cluster,
            child,
            toRex(
                (FunctionExpression) call.expressions.get(0),
                child));

      case AS_QUERYABLE:
        return new TableAccessRel(
            cluster,
            new OptiqPrepareImpl.RelOptTableImpl(
                null,
                typeFactory.createJavaType(
                    Types.toClass(
                        Types.getElementType(
                            call.targetExpression.getType()))),
                new String[0],
                call.targetExpression));

      case DATA_CONTEXT_GET_TABLE:
        return new TableAccessRel(
            cluster,
            new OptiqPrepareImpl.RelOptTableImpl(
                null,
                typeFactory.createJavaType(
                    (Class)
                        ((ConstantExpression) call.expressions.get(1))
                            .value),
                new String[0],
                call.targetExpression));

      default:
        throw new UnsupportedOperationException(
            "unknown method " + call.method);
      }
    }
    throw new UnsupportedOperationException(
        "unknown expression type " + expression.getNodeType());
  }

  private RexNode[] toRex(
      RelNode child, FunctionExpression expression) {
    List<RexNode> list = new ArrayList<RexNode>();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    for (RelNode input : new RelNode[]{child}) {
      list.add(rexBuilder.makeRangeReference(input.getRowType()));
    }
    OptiqPrepareImpl.ScalarTranslator translator =
        OptiqPrepareImpl.EmptyScalarTranslator
            .empty(rexBuilder)
            .bind(expression.parameterList, list);
    final List<RexNode> rexList = new ArrayList<RexNode>();
    final Expression simple = Blocks.simple(expression.body);
    for (Expression expression1 : fieldExpressions(simple)) {
      rexList.add(translator.toRex(expression1));
    }
    return rexList.toArray(new RexNode[rexList.size()]);
  }

  List<Expression> fieldExpressions(Expression expression) {
    if (expression instanceof NewExpression) {
      // Note: We are assuming that the arguments to the constructor
      // are the same order as the fields of the class.
      return ((NewExpression) expression).arguments;
    }
    throw new RuntimeException(
        "unsupported expression type " + expression);
  }

  List<RexNode> toRexList(
      FunctionExpression expression,
      RelNode... inputs) {
    List<RexNode> list = new ArrayList<RexNode>();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    for (RelNode input : inputs) {
      list.add(rexBuilder.makeRangeReference(input.getRowType()));
    }
    return OptiqPrepareImpl.EmptyScalarTranslator.empty(rexBuilder)
        .bind(expression.parameterList, list)
        .toRexList(expression.body);
  }

  RexNode toRex(
      FunctionExpression expression,
      RelNode... inputs) {
    List<RexNode> list = new ArrayList<RexNode>();
    RexBuilder rexBuilder = cluster.getRexBuilder();
    for (RelNode input : inputs) {
      list.add(rexBuilder.makeRangeReference(input.getRowType()));
    }
    return OptiqPrepareImpl.EmptyScalarTranslator.empty(rexBuilder)
        .bind(expression.parameterList, list)
        .toRex(expression.body);
  }
}

// End LixToRelTranslator.java
