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
package org.apache.calcite.test.verifier;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

import com.microsoft.z3.ArithExpr;
import com.microsoft.z3.BoolExpr;
import com.microsoft.z3.Context;
import com.microsoft.z3.Expr;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Convert Rex Node to Symbolic Column.
 **/

public class RexToSymbolicColumn {

  private RexToSymbolicColumn() {
     // not called
  }

  public static List<SqlKind> logicSQL = Arrays.asList(
      SqlKind.AND, SqlKind.OR, SqlKind.NOT, SqlKind.IN);

  public static List<SqlKind> isNullSQL = Arrays.asList(
      SqlKind.IS_NULL, SqlKind.IS_NOT_NULL);

  public static List<SqlKind> binaryArithmetic = Arrays.asList(
      SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL,
      SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL,
      SqlKind.EQUALS, SqlKind.NOT_EQUALS, SqlKind.PLUS, SqlKind.MINUS,
      SqlKind.TIMES, SqlKind.DIVIDE);

  public static SymbolicColumn rexToColumn(RexNode node, List<SymbolicColumn> columns,
      Context z3Context, List<BoolExpr> env) {
    if (node instanceof RexLiteral) {
      RexLiteral constant = (RexLiteral) node;
      return constantToColumn(constant, z3Context);
    } else if (node instanceof RexInputRef) {
      RexInputRef inputRef = (RexInputRef) node;
      return refToColumn(inputRef, columns);
    } else if (node instanceof RexCall) {
      RexCall rexCall = (RexCall) node;
      return rexCallToColumn(rexCall, columns, z3Context, env);
    }
    /** unhandled cases **/
    return null;
  }

  private static SymbolicColumn constantToColumn(RexLiteral constant, Context z3Context) {
    SqlTypeName type = constant.getTypeName();
    if (type.equals(SqlTypeName.NULL)) {
      Expr dummyValue = SymbolicColumn.dummyValue(constant, z3Context);
      return new SymbolicColumn(dummyValue, z3Context.mkTrue());
    } else {
      Expr symbolicValue = constantNotNullToExpr(constant, z3Context);
      return new SymbolicColumn(symbolicValue, z3Context.mkFalse());
    }
  }

  private static Expr constantNotNullToExpr(RexLiteral constant, Context z3Context) {
    SqlTypeName type = constant.getTypeName();
    if (SqlTypeName.APPROX_TYPES.contains(type)) {
      return approxToExpr(constant, z3Context);
    }
    if (SqlTypeName.INT_TYPES.contains(type)) {
      BigDecimal value = (BigDecimal) constant.getValue();
      int intConstant = value.intValue();
      return z3Context.mkInt(intConstant);
    }
    if (type.equals(SqlTypeName.DECIMAL)) {
      BigDecimal value = (BigDecimal) constant.getValue();
      return z3Context.mkReal(value.toString());
    }
    if (SqlTypeName.BOOLEAN_TYPES.contains(type)) {
      Boolean value = (Boolean) constant.getValue();
      return z3Context.mkBool(value);
    }
    /** unhandled cases **/
    return z3Context.mkInt(0);

  }

  private static Expr approxToExpr(RexLiteral constant, Context z3Context) {
    SqlTypeName type = constant.getTypeName();
    if (SqlTypeName.APPROX_TYPES.contains(type)) {
      if (constant.getValue() instanceof Double) {
        Double value = (Double) constant.getValue();
        return z3Context.mkReal(value.toString());
      }
      if (constant.getValue() instanceof Float) {
        Float value = (Float) constant.getValue();
        return z3Context.mkReal(value.toString());
      }
    }
    BigDecimal value = (BigDecimal) constant.getValue();
    return z3Context.mkReal(value.toString());
  }

  private static SymbolicColumn refToColumn(RexInputRef inputRef, List<SymbolicColumn> columns) {
    int index = inputRef.getIndex();
    return columns.get(index);
  }

  private static SymbolicColumn rexCallToColumn(RexCall rexCall, List<SymbolicColumn> columns,
      Context z3Context, List<BoolExpr> env) {
    if (rexCall.isA(logicSQL)) {
      return boolCombineToColumn(rexCall, columns, z3Context, env);
    } else if (rexCall.isA(isNullSQL)) {
      return nullFunToColumn(rexCall, columns, z3Context, env);
    } else if (rexCall.isA(binaryArithmetic)) {
      return binaryArithmeticToColumn(rexCall, columns, z3Context, env);
    }
    /** unhandled rex call node **/
    return null;
  }

  private static SymbolicColumn boolCombineToColumn(RexCall rexCall, List<SymbolicColumn> columns,
      Context z3Context, List<BoolExpr> env) {
    SqlKind sqlKind = rexCall.getKind();
    switch (sqlKind) {
    case NOT: {
      return notToColumn(rexCall, columns, z3Context, env);
    }
    case AND: {
      return andToColumn(rexCall, columns, z3Context, env);
    }
    case OR: {
      return orToColumn(rexCall, columns, z3Context, env);
    }
    default: {
      /** unhandled cases **/
      return null;
    }
    }
  }

  private static SymbolicColumn notToColumn(RexCall rexCall, List<SymbolicColumn> columns,
      Context z3Context, List<BoolExpr> env) {
    RexNode operand = rexCall.getOperands().get(0);
    SymbolicColumn symbolicColumn = rexToColumn(operand, columns, z3Context, env);
    Expr value = symbolicColumn.getSymbolicValue();
    BoolExpr isNull = symbolicColumn.getSymbolicNull();
    return new SymbolicColumn(z3Context.mkNot((BoolExpr) value), isNull);
  }

  private static SymbolicColumn andToColumn(RexCall rexCall, List<SymbolicColumn> columns,
      Context z3Context, List<BoolExpr> env) {
    List<BoolExpr> values = new ArrayList<>();
    List<BoolExpr> isNulls = new ArrayList<>();
    List<BoolExpr> falseValues = new ArrayList<>();
    for (RexNode operand: rexCall.getOperands()) {
      SymbolicColumn symbolicColumn = rexToColumn(operand, columns, z3Context, env);
      BoolExpr value = (BoolExpr) symbolicColumn.getSymbolicValue();
      BoolExpr isNull = symbolicColumn.getSymbolicNull();
      values.add(value);
      isNulls.add(isNull);
      falseValues.add(z3Context.mkOr(isNull, value));
    }
    BoolExpr outputValue = Z3Utility.mkAnd(values, z3Context);
    BoolExpr oneIsNull = Z3Utility.mkOr(isNulls, z3Context);
    BoolExpr allIsNotNull = Z3Utility.mkAnd(falseValues, z3Context);
    BoolExpr outputNull = z3Context.mkAnd(oneIsNull, allIsNotNull);
    return new SymbolicColumn(outputValue, outputNull);
  }

  private static SymbolicColumn orToColumn(RexCall rexCall, List<SymbolicColumn> columns,
      Context z3Context, List<BoolExpr> env) {
    List<BoolExpr> values = new ArrayList<>();
    List<BoolExpr> isNulls = new ArrayList<>();
    List<BoolExpr> falseValues = new ArrayList<>();
    for (RexNode operand: rexCall.getOperands()) {
      SymbolicColumn symbolicColumn = rexToColumn(operand, columns, z3Context, env);
      BoolExpr value = (BoolExpr) symbolicColumn.getSymbolicValue();
      BoolExpr isNull = symbolicColumn.getSymbolicNull();
      values.add(value);
      isNulls.add(isNull);
      falseValues.add(z3Context.mkOr(isNull, z3Context.mkNot(value)));
    }
    BoolExpr outputValue = Z3Utility.mkOr(values, z3Context);
    BoolExpr allNull = Z3Utility.mkAnd(falseValues, z3Context);
    BoolExpr oneIsNull = Z3Utility.mkOr(isNulls, z3Context);
    BoolExpr outputNull = z3Context.mkAnd(allNull, oneIsNull);
    return new SymbolicColumn(outputValue, outputNull);
  }

  private static SymbolicColumn nullFunToColumn(RexCall rexCall, List<SymbolicColumn> columns,
      Context z3Context, List<BoolExpr> env) {
    RexNode operand = rexCall.getOperands().get(0);
    SqlKind sqlKind = rexCall.getKind();
    SymbolicColumn symbolicColumn = rexToColumn(operand, columns, z3Context, env);
    if (sqlKind.equals(SqlKind.IS_NULL)) {
      return new SymbolicColumn(symbolicColumn.getSymbolicNull(), z3Context.mkFalse());
    }
    if (sqlKind.equals(SqlKind.IS_NOT_NULL)) {
      Expr negationValue = z3Context.mkNot(symbolicColumn.getSymbolicNull());
      return new SymbolicColumn(negationValue, z3Context.mkFalse());
    }
    /** this should not happen **/
    return null;
  }

  private static SymbolicColumn binaryArithmeticToColumn(RexCall rexCall,
      List<SymbolicColumn> columns, Context z3Context, List<BoolExpr> env) {
    RexNode left = rexCall.getOperands().get(0);
    RexNode right = rexCall.getOperands().get(1);
    SymbolicColumn leftColumn = rexToColumn(left, columns, z3Context, env);
    SymbolicColumn rightColumn = rexToColumn(right, columns, z3Context, env);
    SqlKind sqlKind = rexCall.getKind();
    Expr leftValue = leftColumn.getSymbolicValue();
    Expr rightValue = rightColumn.getSymbolicValue();
    Expr newValue = binaryArithmeticToValue(sqlKind, leftValue, rightValue, z3Context);
    BoolExpr leftIsNull = leftColumn.getSymbolicNull();
    BoolExpr rightIsNull = rightColumn.getSymbolicNull();
    BoolExpr newIsNull = z3Context.mkOr(leftIsNull, rightIsNull);
    return new SymbolicColumn(newValue, newIsNull);

  }

  private static Expr binaryArithmeticToValue(SqlKind sqlKind, Expr leftValue,
      Expr rightValue, Context z3Context) {
    switch (sqlKind) {
    case PLUS: return z3Context.mkAdd((ArithExpr) leftValue, (ArithExpr) rightValue);
    case MINUS: return z3Context.mkSub((ArithExpr) leftValue, (ArithExpr) rightValue);
    case DIVIDE: return z3Context.mkDiv((ArithExpr) leftValue, (ArithExpr) rightValue);
    case TIMES: return z3Context.mkMul((ArithExpr) leftValue, (ArithExpr) rightValue);
    case LESS_THAN: return z3Context.mkLt((ArithExpr) leftValue, (ArithExpr) rightValue);
    case LESS_THAN_OR_EQUAL: return z3Context.mkLe((ArithExpr) leftValue, (ArithExpr) rightValue);
    case GREATER_THAN:
      return z3Context.mkGt((ArithExpr) leftValue, (ArithExpr) rightValue);
    case GREATER_THAN_OR_EQUAL:
      return z3Context.mkGe((ArithExpr) leftValue, (ArithExpr) rightValue);
    case EQUALS: return z3Context.mkEq(leftValue, rightValue);
    case NOT_EQUALS: return z3Context.mkNot(z3Context.mkEq(leftValue, rightValue));
    default:
      /** unhandled cases **/
      return null;

    }
  }

}
