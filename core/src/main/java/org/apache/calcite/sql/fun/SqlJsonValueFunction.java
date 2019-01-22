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
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * The <code>JSON_VALUE</code> function.
 */
public class SqlJsonValueFunction extends SqlFunction {
  private final boolean returnAny;

  public SqlJsonValueFunction(String name, boolean returnAny) {
    super(name, SqlKind.OTHER_FUNCTION, null,
        (callBinding, returnType, operandTypes) -> {
          RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
          for (int i = 0; i < operandTypes.length; ++i) {
            operandTypes[i] = typeFactory.createSqlType(SqlTypeName.ANY);
          }
        },
        null, SqlFunctionCategory.SYSTEM);
    this.returnAny = returnAny;
  }

  @Override public SqlCall createCall(SqlLiteral functionQualifier,
      SqlParserPos pos, SqlNode... operands) {
    List<SqlNode> operandList = new ArrayList<>();
    operandList.add(operands[0]);
    if (operands[1] == null) {
      operandList.add(
          SqlLiteral.createSymbol(SqlJsonValueEmptyOrErrorBehavior.NULL, pos));
      operandList.add(SqlLiteral.createNull(pos));
    } else {
      operandList.add(operands[1]);
      operandList.add(operands[2]);
    }
    if (operands[3] == null) {
      operandList.add(
          SqlLiteral.createSymbol(SqlJsonValueEmptyOrErrorBehavior.NULL, pos));
      operandList.add(SqlLiteral.createNull(pos));
    } else {
      operandList.add(operands[3]);
      operandList.add(operands[4]);
    }
    if (operands.length == 6 && operands[5] != null) {
      if (returnAny) {
        throw new IllegalArgumentException(
            "illegal returning clause in json_value_any function");
      }
      operandList.add(operands[5]);
    } else if (!returnAny) {
      SqlDataTypeSpec defaultTypeSpec =
          new SqlDataTypeSpec(new SqlIdentifier("VARCHAR", pos), 2000, -1,
              null, null, pos);
      operandList.add(defaultTypeSpec);
    }
    return super.createCall(functionQualifier, pos,
        operandList.toArray(SqlNode.EMPTY_ARRAY));
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(5, 6);
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final SqlValidator validator = callBinding.getValidator();
    RelDataType defaultValueOnEmptyType =
        validator.getValidatedNodeType(callBinding.operand(2));
    RelDataType defaultValueOnErrorType =
        validator.getValidatedNodeType(callBinding.operand(4));
    RelDataType returnType =
        validator.deriveType(callBinding.getScope(), callBinding.operand(5));
    if (!canCastFrom(callBinding, throwOnFailure, defaultValueOnEmptyType,
        returnType)) {
      return false;
    }
    if (!canCastFrom(callBinding, throwOnFailure, defaultValueOnErrorType,
        returnType)) {
      return false;
    }
    return true;
  }

  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    assert opBinding.getOperandCount() == 5
        || opBinding.getOperandCount() == 6;
    RelDataType ret;
    if (opBinding.getOperandCount() == 6) {
      ret = opBinding.getOperandType(5);
    } else {
      ret = opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY);
    }
    return opBinding.getTypeFactory().createTypeWithNullability(ret, true);
  }

  @Override public String getSignatureTemplate(int operandsCount) {
    assert operandsCount == 5 || operandsCount == 6;
    if (operandsCount == 6) {
      return "{0}({1} RETURNING {6} {2} {3} ON EMPTY {4} {5} ON ERROR)";
    }
    return "{0}({1} {2} {3} ON EMPTY {4} {5} ON ERROR)";
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    assert call.operandCount() == 5 || call.operandCount() == 6;
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, 0, 0);
    if (!returnAny) {
      writer.keyword("RETURNING");
      call.operand(5).unparse(writer, 0, 0);
    }
    unparseEnum(writer, call.operand(1));
    if (isDefaultLiteral(call.operand(1))) {
      call.operand(2).unparse(writer, 0, 0);
    }
    writer.keyword("ON");
    writer.keyword("EMPTY");
    unparseEnum(writer, call.operand(3));
    if (isDefaultLiteral(call.operand(3))) {
      call.operand(4).unparse(writer, 0, 0);
    }
    writer.keyword("ON");
    writer.keyword("ERROR");
    writer.endFunCall(frame);
  }

  private void unparseEnum(SqlWriter writer, SqlLiteral literal) {
    writer.keyword(((Enum) literal.getValue()).name());
  }

  private boolean isDefaultLiteral(SqlLiteral literal) {
    return literal.getValueAs(SqlJsonValueEmptyOrErrorBehavior.class)
        == SqlJsonValueEmptyOrErrorBehavior.DEFAULT;
  }

  private boolean canCastFrom(SqlCallBinding callBinding,
      boolean throwOnFailure, RelDataType inType, RelDataType outType) {
    if (SqlTypeUtil.canCastFrom(outType, inType, true)) {
      return true;
    }
    if (throwOnFailure) {
      throw callBinding.newError(
          RESOURCE.cannotCastValue(inType.toString(),
              outType.toString()));
    }
    return false;
  }
}

// End SqlJsonValueFunction.java
