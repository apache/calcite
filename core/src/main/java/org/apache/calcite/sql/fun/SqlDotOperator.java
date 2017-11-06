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
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Arrays;

/**
 * The dot operator {@code .}, used to access a field of a
 * record. For example, {@code a.b}.
 */
public class SqlDotOperator extends SqlSpecialOperator {
  public SqlDotOperator() {
    super("DOT",
        SqlKind.DOT,
        100,
        true,
        null,
        null,
        null);
  }

  @Override public ReduceResult reduceExpr(int ordinal, TokenSequence list) {
    SqlNode left = list.node(ordinal - 1);
    SqlNode right = list.node(ordinal + 1);
    return new ReduceResult(ordinal - 1,
        ordinal + 2,
        createCall(
            SqlParserPos.sum(
                Arrays.asList(left.getParserPosition(),
                    right.getParserPosition(),
                    list.pos(ordinal))),
            left,
            right));
  }

  @Override public void unparse(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.IDENTIFIER);
    call.operand(0).unparse(writer, leftPrec, 0);
    writer.sep(".");
    call.operand(1).unparse(writer, 0, 0);
    writer.endList(frame);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final SqlNode left = callBinding.operand(0);
    final SqlNode right = callBinding.operand(1);
    RelDataType type =
        callBinding.getValidator().deriveType(
            callBinding.getScope(),
            left);
    if (SqlTypeName.ROW != type.getSqlTypeName()) {
      return false;
    }
    final RelDataType operandType = callBinding.getOperandType(0);
    final SqlSingleOperandTypeChecker checker = getChecker(operandType);
    return checker.checkSingleOperandType(callBinding, right, 0,
        throwOnFailure);
  }

  private SqlSingleOperandTypeChecker getChecker(RelDataType operandType) {
    switch (operandType.getSqlTypeName()) {
    case ROW:
      return OperandTypes.family(SqlTypeFamily.STRING);
    default:
      throw new AssertionError(operandType.getSqlTypeName());
    }
  }

  @Override public String getAllowedSignatures(String name) {
    return "<A>.<B>";
  }

  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType operandType = opBinding.getOperandType(0);
    switch (operandType.getSqlTypeName()) {
    case ROW:
      if (opBinding instanceof SqlCallBinding) {
        return typeFactory.createTypeWithNullability(
            opBinding.getOperandType(0).getField(
                ((SqlCallBinding) opBinding).operand(1).toString().replaceAll("^'|'$", ""),
                false, false)
            .getType(), true);
      } else if (opBinding instanceof RexCallBinding) {
        return typeFactory.createTypeWithNullability(
            opBinding.getOperandType(0).getField(
                ((RexCallBinding) opBinding).getStringLiteralOperand(1).toString(), false, false)
            .getType(), true);
      } else {
        throw new AssertionError();
      }
    default:
      throw new AssertionError();
    }
  }
}

// End SqlDotOperator.java
