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
package org.apache.calcite.piglet;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Litmus;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.relational.LOGenerate;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;
import org.apache.pig.newplan.logical.relational.LogicalSchema;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Visits Pig logical operators of Pig inner logical plans
 * (in {@link org.apache.pig.newplan.logical.relational.LOForEach})
 * and converts them into corresponding relational algebra plans.
 */
class PigRelOpInnerVisitor extends PigRelOpVisitor {
  // The relational algebra operator corresponding to the input of LOForeach operator.
  private final RelNode inputRel;

  // Stack contains correlation id required for processing inner plan.
  private final Deque<CorrelationId> corStack = new ArrayDeque<>();

  /**
   *
   * @param plan Pig inner logical plan
   * @param walker The walker over Pig logical plan
   * @param builder Relational algebra builder
   * @throws FrontendException Exception during processing Pig operators
   */
  PigRelOpInnerVisitor(OperatorPlan plan, PlanWalker walker, PigRelBuilder builder)
      throws FrontendException {
    super(plan, walker, builder);
    this.inputRel = builder.peek();
  }

  @Override public void visit(LOGenerate gen) throws FrontendException {
    // @LOGenerate is the root of the inner plan, meaning if we reach here, all operators
    // except this node have been converted into relational algebra nodes stored in the builder.
    // Here we do the final step of generating the relational algebra output node for the
    // @LOForEach operator.

    // First rejoin all results of columns processed in nested block, if any, using correlation ids
    // we remembered before (in visit(LOForeach)).
    makeCorrelates();

    // The project all expressions in the generate command, but ignore flattened columns now
    final List<Integer> multisetFlattens = new ArrayList<>();
    final List<String> flattenOutputAliases = new ArrayList<>();
    doGenerateWithoutMultisetFlatten(gen, multisetFlattens, flattenOutputAliases);
    if (multisetFlattens.size() > 0) {
      builder.multiSetFlatten(multisetFlattens, flattenOutputAliases);
    }
  }

  /**
   * Rejoins all multiset (bag) columns that have been processed in the nested
   * foreach block.
   *
   * @throws FrontendException Exception during processing Pig operators
   */
  private void makeCorrelates() throws FrontendException {
    List<CorrelationId> corIds = new ArrayList<>();
    List<RelNode> rightRels =  new ArrayList<>();

    // First pull out all correlation ids we remembered from the InnerLoads
    while (!corStack.isEmpty()) {
      final CorrelationId corId = corStack.pop();
      corIds.add(0, corId);

      final List<RelNode> corRels = new ArrayList<>(); // All output rels from same inner load
      while (!RelOptUtil.notContainsCorrelation(builder.peek(), corId, Litmus.IGNORE)) {
        corRels.add(0, builder.build());
      }

      assert corRels.size() > 0;
      builder.push(corRels.get(0));
      builder.collect();
      // Now collapse these rels to a single multiset row and join them together
      for (int i = 1; i < corRels.size(); i++) {
        builder.push(corRels.get(i));
        builder.collect();
        builder.join(JoinRelType.INNER, builder.literal(true));
      }

      rightRels.add(0, builder.build());
    }

    // The do correlate join
    for (int i = 0; i < corIds.size(); i++) {
      builder.push(rightRels.get(i));
      builder.join(JoinRelType.INNER, builder.literal(true), ImmutableSet.of(corIds.get(i)));
    }
  }

  /**
   * Projects all expressions in LOGenerate output expressions, but not consider flatten
   * multiset columns yet.
   *
   * @param gen Pig logical generate operator
   * @throws FrontendException Exception during processing Pig operators
   */
  private void doGenerateWithoutMultisetFlatten(LOGenerate gen, List<Integer> multisetFlattens,
      List<String> flattenOutputAliases) throws FrontendException {
    final List<LogicalExpressionPlan> pigProjections = gen.getOutputPlans();
    final List<RexNode> innerCols = new ArrayList<>(); // For projection expressions
    final List<String> fieldAlias = new ArrayList<>(); // For projection names/alias

    if (gen.getOutputPlanSchemas() == null) {
      throw new IllegalArgumentException(
          "Generate statement at line " + gen.getLocation().line() + " produces empty schema");
    }

    for (int i = 0; i < pigProjections.size(); i++) {
      final LogicalSchema outputFieldSchema = gen.getOutputPlanSchemas().get(i);
      RexNode rexNode = PigRelExVisitor.translatePigEx(builder, pigProjections.get(i));
      RelDataType dataType = rexNode.getType();
      // If project field in null constant, dataType will by NULL type, need to check the original
      // type of Pig Schema
      if (dataType.getSqlTypeName() == SqlTypeName.NULL) {
        dataType = PigTypes.convertSchema(outputFieldSchema, true);
      }

      if (outputFieldSchema.size() == 1 && !gen.getFlattenFlags()[i]) {
        final RelDataType scriptType = PigTypes.convertSchemaField(
            outputFieldSchema.getField(0));
        if (dataType.getSqlTypeName() == SqlTypeName.ANY
                || !SqlTypeUtil.isComparable(dataType, scriptType)) {
          // Script schema is different from project expression schema, need to do type cast
          rexNode = builder.getRexBuilder().makeCast(scriptType, rexNode);
        }
      }

      if (gen.getFlattenFlags()[i] && dataType.isStruct()
              && (dataType.getFieldCount() > 0 || dataType instanceof DynamicTupleRecordType)) {
        if (dataType instanceof DynamicTupleRecordType) {
          ((DynamicTupleRecordType) dataType).resize(outputFieldSchema.size());
        }
        for (int j = 0; j < dataType.getFieldCount(); j++) {
          innerCols.add(builder.dot(rexNode, j));
          fieldAlias.add(outputFieldSchema.getField(j).alias);
        }
      } else {
        innerCols.add(rexNode);
        String alias = null;
        if (outputFieldSchema.size() == 1) {
          // If simple type, take user alias if available
          alias = outputFieldSchema.getField(0).alias;
        }
        fieldAlias.add(alias);
        if (gen.getFlattenFlags()[i] && dataType.getFamily() instanceof MultisetSqlType) {
          multisetFlattens.add(innerCols.size() - 1);
          for (LogicalSchema.LogicalFieldSchema field : outputFieldSchema.getFields()) {
            String colAlias = field.alias;
            if (colAlias.contains("::")) {
              String[] tokens  = colAlias.split("::");
              colAlias = tokens[tokens.length - 1];
            }
            flattenOutputAliases.add(colAlias);
          }
        }
      }
    }
    builder.project(innerCols, fieldAlias, true);
  }

  @Override public void visit(LOInnerLoad load) throws FrontendException {
    // Inner loads are the first operator the post order walker (@PigRelOpWalker) visits first
    // We first look at the plan structure to see if the inner load is for a simple projection,
    // which will not be processed in the nested block
    List<Operator> succesors = load.getPlan().getSuccessors(load);

    // An inner load is for a simple projection if it is a direct input of the @LOGenerate.
    // Nothing need to be done further here.
    if (succesors.size() == 1 && succesors.get(0) instanceof LOGenerate) {
      return;
    }

    // Now get the index of projected column using its alias
    RelDataType inputType = inputRel.getRowType();
    final String colAlias = load.getProjection().getColAlias();
    int index = colAlias != null
                    ? inputType.getFieldNames().indexOf(colAlias)
                    : load.getProjection().getColNum();
    assert index >= 0;

    // The column should have multiset type to serve as input for the inner plan
    assert inputType.getFieldList().get(index).getType().getFamily() instanceof MultisetSqlType;

    // Build a correlated expression from the input row
    final CorrelationId correlId = builder.nextCorrelId();
    final RexNode cor = builder.correl(inputType.getFieldList(), correlId);

    // The project out the column from the correlated expression
    RexNode fieldAccess = builder.getRexBuilder().makeFieldAccess(cor, index);
    builder.push(LogicalValues.createOneRow(builder.getCluster()));
    builder.project(fieldAccess);

    // Flatten the column value so that it can be served as the input relation for the inner plan
    builder.multiSetFlatten();

    // Remember the correlation id, then the walker will walk up successor Pig operators. These
    // operators will be processed in @PigRelOpVisitor until it hits the @LOGenerate operator,
    // which will be processed in this class in visit(LOGenerate)
    corStack.push(correlId);
  }

  @Override public boolean preVisit(LogicalRelationalOperator root) {
    // Do not remember the visited PigOp in the inner plan, otherwise, we have trouble in doing
    // correlate with shared PigOp
    return false;
  }
}

// End PigRelOpInnerVisitor.java
