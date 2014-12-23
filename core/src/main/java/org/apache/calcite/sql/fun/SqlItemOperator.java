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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * The item operator {@code [ ... ]}, used to access a given element of an
 * array or map. For example, {@code myArray[3]} or {@code "myMap['foo']"}.
 */
class SqlItemOperator extends SqlSpecialOperator {

  private static final SqlSingleOperandTypeChecker ARRAY_OR_MAP =
      OperandTypes.or(
          OperandTypes.family(SqlTypeFamily.ARRAY),
          OperandTypes.family(SqlTypeFamily.MAP),
          OperandTypes.family(SqlTypeFamily.ANY));

  public SqlItemOperator() {
    super("ITEM", SqlKind.OTHER_FUNCTION, 100, true, null, null, null);
  }

  @Override public int reduceExpr(int ordinal, List<Object> list) {
    SqlNode left = (SqlNode) list.get(ordinal - 1);
    SqlNode right = (SqlNode) list.get(ordinal + 1);
    final SqlParserUtil.ToTreeListItem treeListItem =
        (SqlParserUtil.ToTreeListItem) list.get(ordinal);
    SqlParserUtil.replaceSublist(
        list,
        ordinal - 1,
        ordinal + 2,
        createCall(
            left.getParserPosition()
                .plus(right.getParserPosition())
                .plus(treeListItem.getPos()),
            left,
            right));
    return ordinal - 1;
  }

  @Override public void unparse(
      SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    call.operand(0).unparse(writer, leftPrec, 0);
    final SqlWriter.Frame frame = writer.startList("[", "]");
    call.operand(1).unparse(writer, 0, 0);
    writer.endList(frame);
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final SqlNode left = callBinding.getCall().operand(0);
    final SqlNode right = callBinding.getCall().operand(1);
    if (!ARRAY_OR_MAP.checkSingleOperandType(callBinding, left, 0,
        throwOnFailure)) {
      return false;
    }
    final RelDataType operandType = callBinding.getOperandType(0);
    final SqlSingleOperandTypeChecker checker = getChecker(operandType);
    return checker.checkSingleOperandType(callBinding, right, 0,
        throwOnFailure);
  }

  private SqlSingleOperandTypeChecker getChecker(RelDataType operandType) {
    switch (operandType.getSqlTypeName()) {
    case ARRAY:
      return OperandTypes.family(SqlTypeFamily.INTEGER);
    case MAP:
      return OperandTypes.family(
          operandType.getKeyType().getSqlTypeName().getFamily());
    case ANY:
      return OperandTypes.or(
          OperandTypes.family(SqlTypeFamily.INTEGER),
          OperandTypes.family(SqlTypeFamily.CHARACTER));
    default:
      throw new AssertionError(operandType.getSqlTypeName());
    }
  }

  @Override public String getAllowedSignatures(String name) {
    return "<ARRAY>[<INTEGER>]\n"
        + "<MAP>[<VALUE>]";
  }

  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType operandType = opBinding.getOperandType(0);
    switch (operandType.getSqlTypeName()) {
    case ARRAY:
      return typeFactory.createTypeWithNullability(
          operandType.getComponentType(), true);
    case MAP:
      return typeFactory.createTypeWithNullability(operandType.getValueType(),
          true);
    case ANY:
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.ANY), true);
    default:
      throw new AssertionError();
    }
  }
}

// End SqlItemOperator.java
