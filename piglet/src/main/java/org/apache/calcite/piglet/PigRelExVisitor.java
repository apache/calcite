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

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.calcite.util.NlsString;

import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.OperatorPlan;
import org.apache.pig.newplan.PlanWalker;
import org.apache.pig.newplan.logical.expression.AddExpression;
import org.apache.pig.newplan.logical.expression.AndExpression;
import org.apache.pig.newplan.logical.expression.BinCondExpression;
import org.apache.pig.newplan.logical.expression.CastExpression;
import org.apache.pig.newplan.logical.expression.ConstantExpression;
import org.apache.pig.newplan.logical.expression.DereferenceExpression;
import org.apache.pig.newplan.logical.expression.DivideExpression;
import org.apache.pig.newplan.logical.expression.EqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanEqualExpression;
import org.apache.pig.newplan.logical.expression.GreaterThanExpression;
import org.apache.pig.newplan.logical.expression.IsNullExpression;
import org.apache.pig.newplan.logical.expression.LessThanEqualExpression;
import org.apache.pig.newplan.logical.expression.LessThanExpression;
import org.apache.pig.newplan.logical.expression.LogicalExpressionPlan;
import org.apache.pig.newplan.logical.expression.LogicalExpressionVisitor;
import org.apache.pig.newplan.logical.expression.MapLookupExpression;
import org.apache.pig.newplan.logical.expression.ModExpression;
import org.apache.pig.newplan.logical.expression.MultiplyExpression;
import org.apache.pig.newplan.logical.expression.NegativeExpression;
import org.apache.pig.newplan.logical.expression.NotEqualExpression;
import org.apache.pig.newplan.logical.expression.NotExpression;
import org.apache.pig.newplan.logical.expression.OrExpression;
import org.apache.pig.newplan.logical.expression.ProjectExpression;
import org.apache.pig.newplan.logical.expression.RegexExpression;
import org.apache.pig.newplan.logical.expression.ScalarExpression;
import org.apache.pig.newplan.logical.expression.SubtractExpression;
import org.apache.pig.newplan.logical.expression.UserFuncExpression;
import org.apache.pig.newplan.logical.relational.LOInnerLoad;
import org.apache.pig.newplan.logical.relational.LogicalRelationalOperator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Visits pig expression plans and converts them into corresponding RexNodes.
 */
class PigRelExVisitor extends LogicalExpressionVisitor {
  /** Stack used during post-order walking process when processing a Pig
   * expression plan. */
  private final Deque<RexNode> stack = new ArrayDeque<>();

  /** The relational algebra builder customized for Pig. */
  private final PigRelBuilder builder;

  // inputCount and inputOrdinal are used to select which relation in the builder
  // stack to build the projection

  /** Number of inputs. */
  private final int inputCount;

  /** Input ordinal. */
  private final int inputOrdinal;

  /**
   * Creates a PigRelExVisitor.
   *
   * @param expressionPlan Pig expression plan
   * @param walker The walker over Pig expression plan.
   * @param builder Relational algebra builder
   * @param inputCount Number of inputs
   * @param inputOrdinal Input ordinal
   * @throws FrontendException Exception during processing Pig operators
   */
  private PigRelExVisitor(OperatorPlan expressionPlan, PlanWalker walker,
      PigRelBuilder builder, int inputCount, int inputOrdinal)
      throws FrontendException {
    super(expressionPlan, walker);
    this.builder = builder;
    this.inputCount = inputCount;
    this.inputOrdinal = inputOrdinal;
  }

  /**
   * Translates the given pig expression plan into a list of relational algebra
   * expressions.
   *
   * @return Relational algebra expressions
   * @throws FrontendException Exception during processing Pig operators
   */
  private List<RexNode> translate() throws FrontendException {
    currentWalker.walk(this);
    return new ArrayList<>(stack);
  }

  /**
   * Translates a Pig expression plans into relational algebra expressions.
   *
   * @param builder Relational algebra builder
   * @param pigEx Pig expression plan
   * @param inputCount Number of inputs
   * @param inputOrdinal Input ordinal
   * @return Relational algebra expressions
   * @throws FrontendException Exception during processing Pig operators
   */
  static RexNode translatePigEx(PigRelBuilder builder, LogicalExpressionPlan pigEx,
      int inputCount, int inputOrdinal) throws FrontendException {
    final PigRelExWalker walker = new PigRelExWalker(pigEx);
    final PigRelExVisitor exVisitor =
        new PigRelExVisitor(pigEx, walker, builder, inputCount, inputOrdinal);
    final List<RexNode> result = exVisitor.translate();
    assert result.size() == 1;
    return result.get(0);
  }

  /**
   * Translates a Pig expression plans into relational algebra expressions.
   *
   * @param builder Relational algebra builder
   * @param pigEx Pig expression plan
   * @return Relational algebra expressions
   * @throws FrontendException Exception during processing Pig operators
   */
  static RexNode translatePigEx(PigRelBuilder builder, LogicalExpressionPlan pigEx)
      throws FrontendException {
    return translatePigEx(builder, pigEx, 1, 0);
  }

  /**
   * Builds operands for an operator from expressions on the top of the visitor stack.
   *
   * @param numOps number of operands
   * @return List of operand expressions
   */
  private ImmutableList<RexNode> buildOperands(int numOps) {
    List<RexNode> opList = new ArrayList<>();
    for (int i = 0; i < numOps; i++) {
      opList.add(0, stack.pop());
    }
    return ImmutableList.copyOf(opList);
  }

  /**
   * Builds operands for a binary operator.
   *
   * @return List of two operand expressions
   */
  private ImmutableList<RexNode> buildBinaryOperands() {
    return buildOperands(2);
  }

  @Override public void visit(ConstantExpression op) throws FrontendException {
    RelDataType constType = PigTypes.convertSchemaField(op.getFieldSchema(), false);
    stack.push(builder.literal(op.getValue(), constType));
  }

  @Override public void visit(ProjectExpression op) throws FrontendException {
    String fullAlias = op.getFieldSchema().alias;
    if (fullAlias != null) {
      RexNode inputRef;
      try {
        // First try the exact name match for the alias
        inputRef = builder.field(inputCount, inputOrdinal, fullAlias);
      } catch (IllegalArgumentException e) {
        // If not found, look for the field name match only.
        // Note that the full alias may have the format of 'tableName::fieldName'
        final List<String> fieldNames =
            builder.peek(inputCount, inputOrdinal).getRowType().getFieldNames();
        int index = -1;
        for (int i = 0; i < fieldNames.size(); i++) {
          if (fullAlias.endsWith(fieldNames.get(i))) {
            index = i;
            break;
          }
        }
        if (index < 0) {
          String shortAlias = fullAlias;
          if (fullAlias.contains("::")) {
            String[] tokens = fullAlias.split("::");
            shortAlias = tokens[tokens.length - 1];
          }
          for (int i = 0; i < fieldNames.size(); i++) {
            if (fieldNames.get(i).equals(shortAlias)) {
              index = i;
              break;
            }
          }
          if (index < 0) {
            throw new IllegalArgumentException(
                "field [" + fullAlias + "] not found; input fields are: " + fieldNames);
          }
        }
        inputRef = builder.field(inputCount, inputOrdinal, index);
      }
      stack.push(inputRef);
    } else {
      // Alias not provided, get data from input of LOGenerate
      assert op.getInputNum() >= 0;
      final Operator pigRelOp = op.getAttachedRelationalOp();
      final LogicalRelationalOperator childOp = (LogicalRelationalOperator)
          pigRelOp.getPlan().getPredecessors(pigRelOp).get(op.getInputNum());
      if (builder.checkMap(childOp)) {
        // Inner plan that has been processed before (nested foreach or flatten)
        builder.push(builder.getRel(childOp));
        final List<RexNode> fields = builder.getFields(inputCount, inputOrdinal, op.getColNum());

        for (int i = fields.size() - 1; i >= 0; i--) {
          stack.push(fields.get(i));
        }

        builder.build();
      } else {
        // Simple inner load
        assert childOp instanceof LOInnerLoad;
        visit(((LOInnerLoad) childOp).getProjection());
      }
    }
  }

  @Override public void visit(NegativeExpression op) throws FrontendException {
    final RexNode operand = stack.pop();
    if (operand instanceof RexLiteral) {
      final Comparable value = ((RexLiteral) operand).getValue();
      assert value instanceof BigDecimal;
      stack.push(builder.literal(((BigDecimal) value).negate()));
    } else {
      stack.push(builder.call(SqlStdOperatorTable.UNARY_MINUS, operand));
    }
  }

  @Override public void visit(EqualExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.EQUALS, buildBinaryOperands()));
  }

  @Override public void visit(NotEqualExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.NOT_EQUALS, buildBinaryOperands()));
  }

  @Override public void visit(LessThanExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.LESS_THAN, buildBinaryOperands()));
  }

  @Override public void visit(LessThanEqualExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, buildBinaryOperands()));
  }

  @Override public void visit(GreaterThanExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.GREATER_THAN, buildBinaryOperands()));
  }

  @Override public void visit(GreaterThanEqualExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, buildBinaryOperands()));
  }

  @Override public void visit(RegexExpression op) throws FrontendException {
    RexNode operand1 = replacePatternIfPossible(stack.pop());
    RexNode operand2 = replacePatternIfPossible(stack.pop());
    stack.push(builder.call(SqlStdOperatorTable.LIKE, ImmutableList.of(operand2, operand1)));
  }

  /**
   * Replaces Pig regular expressions with SQL regular expressions in a string.
   *
   * @param rexNode The string literal
   * @return New string literal with Pig regular expressions replaced by SQL regular expressions
   */
  private static RexNode replacePatternIfPossible(RexNode rexNode) {
    // Until
    //   [CALCITE-3194] Convert Pig string patterns into SQL string patterns
    // is fixed, return the pattern unchanged.
    return rexNode;
  }

  @Override public void visit(IsNullExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.IS_NULL, stack.pop()));
  }

  @Override public void visit(NotExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.NOT, stack.pop()));
  }

  @Override public void visit(AndExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.AND, buildBinaryOperands()));
  }

  @Override public void visit(OrExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.OR, buildBinaryOperands()));
  }

  @Override public void visit(AddExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.PLUS, buildBinaryOperands()));
  }

  @Override public void visit(SubtractExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.MINUS, buildBinaryOperands()));
  }

  @Override public void visit(MultiplyExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.MULTIPLY, buildBinaryOperands()));
  }

  @Override public void visit(ModExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.MOD, buildBinaryOperands()));
  }

  @Override public void visit(DivideExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.DIVIDE, buildBinaryOperands()));
  }

  @Override public void visit(BinCondExpression op) throws FrontendException {
    stack.push(builder.call(SqlStdOperatorTable.CASE, buildOperands(3)));
  }

  @Override public void visit(UserFuncExpression op) throws FrontendException {
    if (op.getFuncSpec().getClassName().equals("org.apache.pig.impl.builtin.IdentityColumn")) {
      // Skip this Pig dummy function
      return;
    }
    final int numAgrs = optSize(op.getPlan().getSuccessors(op))
        + optSize(op.getPlan().getSoftLinkSuccessors(op));

    final RelDataType returnType = PigTypes.convertSchemaField(op.getFieldSchema());
    stack.push(
        PigRelUdfConverter.convertPigFunction(
            builder, op.getFuncSpec(), buildOperands(numAgrs), returnType));

    String className = op.getFuncSpec().getClassName();
    SqlOperator sqlOp = ((RexCall) stack.peek()).getOperator();
    if (sqlOp instanceof SqlUserDefinedFunction) {
      ScalarFunctionImpl sqlFunc =
          (ScalarFunctionImpl) ((SqlUserDefinedFunction) sqlOp).getFunction();
      // "Exec" method can be implemented from the parent class.
      className = sqlFunc.method.getDeclaringClass().getName();
    }
    builder.registerPigUDF(className, op.getFuncSpec());
  }

  private static int optSize(List<Operator> list) {
    return list != null ? list.size() : 0;
  }

  @Override public void visit(DereferenceExpression op) throws FrontendException {
    final RexNode parentField = stack.pop();
    List<Integer> cols = op.getBagColumns();
    assert cols != null && cols.size() > 0;

    if (parentField.getType() instanceof MultisetSqlType) {
      // Calcite does not support projection on Multiset type. We build
      // our own multiset projection in @PigRelSqlUDFs and use it here
      final RexNode[] rexCols = new RexNode[cols.size() + 1];
      // First parent field
      rexCols[0] = parentField;
      // The sub-fields to be projected from parent field
      for (int i = 0; i < cols.size(); i++) {
        rexCols[i + 1] = builder.literal(cols.get(i));
      }
      stack.push(builder.call(PigRelSqlUdfs.MULTISET_PROJECTION, rexCols));
    } else {
      if (cols.size() == 1) {
        // Single field projection
        stack.push(builder.dot(parentField, cols.get(0)));
      } else {
        // Multiple field projection, build a sub struct from the parent struct
        List<RexNode> relFields = new ArrayList<>();
        for (Object col : cols) {
          relFields.add(builder.dot(parentField, col));
        }

        final RelDataType newRelType = RexUtil.createStructType(
            PigTypes.TYPE_FACTORY,
            relFields);
        stack.push(
            builder.getRexBuilder().makeCall(newRelType, SqlStdOperatorTable.ROW, relFields));
      }
    }
  }

  @Override public void visit(CastExpression op) throws FrontendException {
    final RelDataType relType = PigTypes.convertSchemaField(op.getFieldSchema());
    final RexNode castOperand = stack.pop();
    if (castOperand instanceof RexLiteral
        && ((RexLiteral) castOperand).getValue() == null) {
      if (!relType.isStruct() && relType.getComponentType() == null) {
        stack.push(builder.getRexBuilder().makeNullLiteral(relType));
      } else {
        stack.push(castOperand);
      }
    } else {
      stack.push(builder.getRexBuilder().makeCast(relType, castOperand));
    }
  }

  @Override public void visit(MapLookupExpression op) throws FrontendException {
    final RexNode relKey = builder.literal(op.getLookupKey());
    final RexNode relMap = stack.pop();
    stack.push(builder.call(SqlStdOperatorTable.ITEM, relMap, relKey));
  }

  @Override public void visit(ScalarExpression op) throws FrontendException {
    // First operand is the path to the materialized view
    RexNode operand1 = stack.pop();
    assert operand1 instanceof RexLiteral
               && ((RexLiteral) operand1).getValue() instanceof NlsString;

    // Second operand is the projection index
    RexNode operand2 = stack.pop();
    assert operand2 instanceof RexLiteral
               && ((RexLiteral) operand2).getValue() instanceof BigDecimal;
    final int index = ((BigDecimal) ((RexLiteral) operand2).getValue()).intValue();

    RelNode referencedRel = builder.getRel(
        ((LogicalRelationalOperator) op.getImplicitReferencedOperator()).getAlias());
    builder.push(referencedRel);
    List<RexNode> projectCol = Lists.newArrayList(builder.field(index));
    builder.project(projectCol);

    stack.push(RexSubQuery.scalar(builder.build()));
  }
}
