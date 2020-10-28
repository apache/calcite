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
 * Convert Rex to Symbolic Column Based on Filter Conditions
 **/

public class Rex2SymbolicColumn {

  static public List<SqlKind> logicSQL = Arrays.asList(
      SqlKind.AND, SqlKind.OR, SqlKind.NOT, SqlKind.IN);

  static public List<SqlKind> isNullSQL = Arrays.asList(
      SqlKind.IS_NULL, SqlKind.IS_NOT_NULL);

  static public List<SqlKind> binaryArithmetic = Arrays.asList(
      SqlKind.LESS_THAN, SqlKind.LESS_THAN_OR_EQUAL,
      SqlKind.GREATER_THAN, SqlKind.GREATER_THAN_OR_EQUAL,
      SqlKind.EQUALS, SqlKind.NOT_EQUALS, SqlKind.PLUS, SqlKind.MINUS,
      SqlKind.TIMES, SqlKind.DIVIDE);

  public static SymbolicColumn rex2SymbolicColumn
       (RexNode node, List<SymbolicColumn> columns, Context z3Context, List<BoolExpr> env) {
    if (node instanceof RexLiteral) {
      RexLiteral constant = (RexLiteral) node;
      return constant2Column(constant, columns, z3Context);
    } else if (node instanceof RexInputRef) {
      RexInputRef inputRef = (RexInputRef) node;
      return ref2Column(inputRef, columns);
    } else if (node instanceof RexCall) {
      RexCall rexCall = (RexCall) node;
      return rexCall2Column(rexCall, columns, z3Context, env);
    }
    /** unhandled cases **/
    return null;
  }

  private static SymbolicColumn constant2Column
      (RexLiteral constant, List<SymbolicColumn> columns, Context z3Context) {
    SqlTypeName type = constant.getTypeName();
    if (type.equals(SqlTypeName.NULL)) {
      Expr dummyValue = SymbolicColumn.dummyValue(constant, z3Context);
      return new SymbolicColumn(dummyValue, z3Context.mkTrue());
    }else{
      Expr symbolicValue = constantNotNull2Expr(constant, z3Context);
      return new SymbolicColumn(symbolicValue, z3Context.mkFalse());
    }
  }

  private static Expr constantNotNull2Expr(RexLiteral constant, Context z3Context){
    SqlTypeName type = constant.getTypeName();
    if(SqlTypeName.APPROX_TYPES.contains(type)){
      return approx2Expr(constant, z3Context);
    }
    if(SqlTypeName.INT_TYPES.contains(type)){
      BigDecimal value=(BigDecimal)constant.getValue();
      int intConstant = value.intValue();
      return z3Context.mkInt(intConstant);
    }
    if(type.equals(SqlTypeName.DECIMAL)){
      BigDecimal value=(BigDecimal)constant.getValue();
      return z3Context.mkReal(value.toString());
    }
    if(SqlTypeName.BOOLEAN_TYPES.contains(type)){
      Boolean value = (Boolean)constant.getValue();
      return z3Context.mkBool(value);
    }
    /** unhandled cases **/
    return z3Context.mkInt(0);

  }

  private static Expr approx2Expr(RexLiteral constant, Context z3Context){
    SqlTypeName type = constant.getTypeName();
    if(SqlTypeName.APPROX_TYPES.contains(type)){
      if(constant.getValue() instanceof Double){
        Double value = (Double) constant.getValue();
        return z3Context.mkReal(value.toString());
      }
      if(constant.getValue() instanceof Float){
        Float value = (Float) constant.getValue();
        return z3Context.mkReal(value.toString());
      }
    }
    BigDecimal value = (BigDecimal)constant.getValue();
    return z3Context.mkReal(value.toString());
  }

  private static SymbolicColumn ref2Column (RexInputRef inputRef, List<SymbolicColumn> columns) {
    int index = inputRef.getIndex();
    return columns.get(index);
  }

  private static SymbolicColumn rexCall2Column
      (RexCall rexCall, List<SymbolicColumn> columns, Context z3Context, List<BoolExpr> env) {
    if (rexCall.isA(logicSQL)) {
      return boolCombine2Column(rexCall, columns, z3Context, env);
    } else if (rexCall.isA(isNullSQL)) {
      return nullFun2Column(rexCall, columns, z3Context, env);
    } else if (rexCall.isA(binaryArithmetic)) {
      return binaryArithmetic2Column(rexCall, columns, z3Context, env);
    }
    /** unhandled rex call node **/
    return null;
  }

  private static SymbolicColumn boolCombine2Column
      (RexCall rexCall, List<SymbolicColumn> columns, Context z3Context, List<BoolExpr> env) {
    SqlKind sqlKind = rexCall.getKind();
    switch (sqlKind){
      case NOT: {
        return not2Column(rexCall, columns, z3Context, env);
      }
      case AND: {
        return and2Column(rexCall, columns, z3Context, env);
      }
      case OR: {
        return or2Column(rexCall, columns, z3Context, env);
      }
      default:{
        /** unhandled cases **/
        return null;
      }
    }
  }

  private static SymbolicColumn not2Column
      (RexCall rexCall, List<SymbolicColumn> columns, Context z3Context, List<BoolExpr> env) {
    RexNode operand = rexCall.getOperands().get(0);
    SymbolicColumn symbolicColumn = rex2SymbolicColumn(operand, columns, z3Context, env);
    Expr value = symbolicColumn.getSymbolicValue();
    BoolExpr isNull = symbolicColumn.getSymbolicNull();
    return new SymbolicColumn(z3Context.mkNot((BoolExpr) value), isNull);
  }

  private static SymbolicColumn and2Column
      (RexCall rexCall, List<SymbolicColumn> columns, Context z3Context, List<BoolExpr> env) {
    List<BoolExpr> values = new ArrayList<>();
    List<BoolExpr> isNulls = new ArrayList<>();
    List<BoolExpr> falseValues = new ArrayList<>();
    for(RexNode operand: rexCall.getOperands()){
      SymbolicColumn symbolicColumn = rex2SymbolicColumn(operand, columns, z3Context, env);
      BoolExpr value = (BoolExpr)symbolicColumn.getSymbolicValue();
      BoolExpr isNull = symbolicColumn.getSymbolicNull();
      values.add(value);
      isNulls.add(isNull);
      falseValues.add(z3Context.mkOr(isNull, value));
    }
    BoolExpr outputValue = Z3Utility.mkAnd(values, z3Context);
    BoolExpr oneIsNull = Z3Utility.mkOr(isNulls, z3Context);
    BoolExpr allIsNotNull = Z3Utility.mkAnd(falseValues, z3Context);
    BoolExpr outputNull = z3Context.mkAnd(oneIsNull, allIsNotNull);
    return (new SymbolicColumn(outputValue, outputNull));
  }

  private static SymbolicColumn or2Column
      (RexCall rexCall, List<SymbolicColumn> columns, Context z3Context, List<BoolExpr> env) {
    List<BoolExpr> values = new ArrayList<>();
    List<BoolExpr> isNulls = new ArrayList<>();
    List<BoolExpr> falseValues = new ArrayList<>();
    for(RexNode operand: rexCall.getOperands()){
      SymbolicColumn symbolicColumn = rex2SymbolicColumn(operand, columns, z3Context, env);
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
    return new SymbolicColumn(outputValue,outputNull);
  }

  private static SymbolicColumn nullFun2Column
      (RexCall rexCall, List<SymbolicColumn> columns, Context z3Context, List<BoolExpr> env) {
    RexNode operand = rexCall.getOperands().get(0);
    SqlKind sqlKind = rexCall.getKind();
    SymbolicColumn symbolicColumn = rex2SymbolicColumn(operand, columns, z3Context, env);
    if(sqlKind.equals(SqlKind.IS_NULL)){
      return new SymbolicColumn(symbolicColumn.getSymbolicNull(), z3Context.mkFalse());
    }
    if(sqlKind.equals(SqlKind.IS_NOT_NULL)){
      Expr negationValue = z3Context.mkNot(symbolicColumn.getSymbolicNull());
      return new SymbolicColumn(negationValue, z3Context.mkFalse());
    }
    /** this should not happen **/
    return null;
  }

  private static SymbolicColumn binaryArithmetic2Column
      (RexCall rexCall, List<SymbolicColumn> columns, Context z3Context, List<BoolExpr> env) {
    RexNode left = rexCall.getOperands().get(0);
    RexNode right = rexCall.getOperands().get(1);
    SymbolicColumn leftColumn = rex2SymbolicColumn(left, columns, z3Context, env);
    SymbolicColumn rightColumn = rex2SymbolicColumn(right, columns, z3Context, env);
    SqlKind sqlKind = rexCall.getKind();
    Expr leftValue = leftColumn.getSymbolicValue();
    Expr rightValue = rightColumn.getSymbolicValue();
    Expr newValue = binaryArithmetic2Value(sqlKind, leftValue, rightValue, z3Context);
    BoolExpr leftIsNull = leftColumn.getSymbolicNull();
    BoolExpr rightIsNull = rightColumn.getSymbolicNull();
    BoolExpr newIsNull = z3Context.mkOr(leftIsNull, rightIsNull);
    return new SymbolicColumn(newValue, newIsNull);

  }

  private static Expr binaryArithmetic2Value
      (SqlKind sqlKind, Expr leftValue, Expr rightValue, Context z3Context) {
    switch (sqlKind){
      case PLUS: return z3Context.mkAdd((ArithExpr)leftValue, (ArithExpr)rightValue);
      case MINUS: return z3Context.mkSub((ArithExpr)leftValue, (ArithExpr)rightValue);
      case DIVIDE: return z3Context.mkDiv((ArithExpr)leftValue, (ArithExpr)rightValue);
      case TIMES: return z3Context.mkMul((ArithExpr)leftValue, (ArithExpr)rightValue);
      case LESS_THAN: return z3Context.mkLt((ArithExpr)leftValue, (ArithExpr)rightValue);
      case LESS_THAN_OR_EQUAL: return z3Context.mkLe((ArithExpr)leftValue, (ArithExpr)rightValue);
      case GREATER_THAN: return z3Context.mkGt((ArithExpr)leftValue, (ArithExpr)rightValue);
      case GREATER_THAN_OR_EQUAL:
        return z3Context.mkGe((ArithExpr)leftValue, (ArithExpr)rightValue);
      case EQUALS: return z3Context.mkEq(leftValue, rightValue);
      case NOT_EQUALS: return z3Context.mkNot(z3Context.mkEq(leftValue, rightValue));
    default:
      /** unhandled cases **/
      return null;

    }
  }

}
