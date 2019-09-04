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
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
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
    super(name, SqlKind.OTHER_FUNCTION,
        ReturnTypes.cascade(
            opBinding -> {
              assert opBinding.getOperandCount() == 6
                  || opBinding.getOperandCount() == 7;
              RelDataType ret;
              if (opBinding.getOperandCount() == 7) {
                ret = opBinding.getOperandType(6);
              } else {
                ret = opBinding.getTypeFactory().createSqlType(SqlTypeName.ANY);
              }
              return opBinding.getTypeFactory().createTypeWithNullability(ret, true);
            }, SqlTypeTransforms.FORCE_NULLABLE),
        (callBinding, returnType, operandTypes) -> {
          RelDataTypeFactory typeFactory = callBinding.getTypeFactory();
          operandTypes[3] = typeFactory.createSqlType(SqlTypeName.ANY);
          operandTypes[5] = typeFactory.createSqlType(SqlTypeName.ANY);
        },
        OperandTypes.family(SqlTypeFamily.ANY,
            SqlTypeFamily.CHARACTER, SqlTypeFamily.ANY, SqlTypeFamily.ANY, SqlTypeFamily.ANY,
            SqlTypeFamily.ANY, SqlTypeFamily.ANY),
        SqlFunctionCategory.SYSTEM);
    this.returnAny = returnAny;
  }

  @Override public SqlCall createCall(SqlLiteral functionQualifier,
      SqlParserPos pos, SqlNode... operands) {
    List<SqlNode> operandList = new ArrayList<>();
    operandList.add(operands[0]);
    operandList.add(operands[1]);
    if (operands[2] == null) {
      // empty behavior
      operandList.add(
          SqlLiteral.createSymbol(SqlJsonValueEmptyOrErrorBehavior.NULL, pos));
      operandList.add(SqlLiteral.createNull(pos));
    } else {
      operandList.add(operands[2]);
      operandList.add(operands[3]);
    }
    if (operands[4] == null) {
      // error behavior
      operandList.add(
          SqlLiteral.createSymbol(SqlJsonValueEmptyOrErrorBehavior.NULL, pos));
      operandList.add(SqlLiteral.createNull(pos));
    } else {
      operandList.add(operands[4]);
      operandList.add(operands[5]);
    }
    if (operands.length == 7 && operands[6] != null) {
      if (returnAny) {
        throw new IllegalArgumentException(
            "illegal returning clause in json_value_any function");
      }
      operandList.add(operands[6]);
    } else if (!returnAny) {
      SqlDataTypeSpec defaultTypeSpec =
          new SqlDataTypeSpec(
              new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, 2000, pos),
              pos);
      operandList.add(defaultTypeSpec);
    }
    return super.createCall(functionQualifier, pos,
        operandList.toArray(SqlNode.EMPTY_ARRAY));
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(6, 7);
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final SqlValidator validator = callBinding.getValidator();
    RelDataType defaultValueOnEmptyType =
        validator.getValidatedNodeType(callBinding.operand(3));
    RelDataType defaultValueOnErrorType =
        validator.getValidatedNodeType(callBinding.operand(5));
    RelDataType returnType =
        validator.deriveType(callBinding.getScope(), callBinding.operand(6));
    if (!canCastFrom(callBinding, throwOnFailure, defaultValueOnEmptyType,
        returnType)) {
      return false;
    }
    if (!canCastFrom(callBinding, throwOnFailure, defaultValueOnErrorType,
        returnType)) {
      return false;
    }
    return super.checkOperandTypes(callBinding, throwOnFailure);
  }

  @Override public String getSignatureTemplate(int operandsCount) {
    assert operandsCount == 6 || operandsCount == 7;
    if (operandsCount == 7) {
      return "{0}({1} RETURNING {6} {2} {3} ON EMPTY {4} {5} ON ERROR)";
    }
    return "{0}({1} {2} {3} ON EMPTY {4} {5} ON ERROR)";
  }

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
    assert call.operandCount() == 6 || call.operandCount() == 7;
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, 0, 0);
    writer.sep(",", true);
    call.operand(1).unparse(writer, 0, 0);
    if (!returnAny) {
      writer.keyword("RETURNING");
      call.operand(6).unparse(writer, 0, 0);
    }
    unparseEnum(writer, call.operand(2));
    if (isDefaultLiteral(call.operand(2))) {
      call.operand(3).unparse(writer, 0, 0);
    }
    writer.keyword("ON");
    writer.keyword("EMPTY");
    unparseEnum(writer, call.operand(4));
    if (isDefaultLiteral(call.operand(4))) {
      call.operand(5).unparse(writer, 0, 0);
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
