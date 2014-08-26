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
package org.eigenbase.sql.fun;

import java.math.*;
import java.util.List;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;

import net.hydromatic.linq4j.Ord;

import com.google.common.collect.ImmutableList;

/**
 * Definition of the "SUBSTRING" builtin SQL function.
 */
public class SqlSubstringFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  /**
   * Creates the SqlSubstringFunction.
   */
  SqlSubstringFunction() {
    super(
        "SUBSTRING",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE_VARYING,
        null,
        null,
        SqlFunctionCategory.STRING);
  }

  //~ Methods ----------------------------------------------------------------

  public String getSignatureTemplate(final int operandsCount) {
    switch (operandsCount) {
    case 2:
      return "{0}({1} FROM {2})";
    case 3:
      return "{0}({1} FROM {2} FOR {3})";
    default:
      throw new AssertionError();
    }
  }

  public String getAllowedSignatures(String opName) {
    StringBuilder ret = new StringBuilder();
    for (Ord<SqlTypeName> typeName : Ord.zip(SqlTypeName.STRING_TYPES)) {
      if (typeName.i > 0) {
        ret.append(NL);
      }
      ret.append(
          SqlUtil.getAliasedSignature(this, opName,
              ImmutableList.of(typeName.e, SqlTypeName.INTEGER)));
      ret.append(NL);
      ret.append(
          SqlUtil.getAliasedSignature(this, opName,
              ImmutableList.of(typeName.e, SqlTypeName.INTEGER,
                  SqlTypeName.INTEGER)));
    }
    return ret.toString();
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    SqlCall call = callBinding.getCall();
    SqlValidator validator = callBinding.getValidator();
    SqlValidatorScope scope = callBinding.getScope();

    final List<SqlNode> operands = call.getOperandList();
    int n = operands.size();
    assert (3 == n) || (2 == n);
    if (!OperandTypes.STRING.checkSingleOperandType(
        callBinding,
        operands.get(0),
        0,
        throwOnFailure)) {
      return false;
    }
    if (2 == n) {
      if (!OperandTypes.NUMERIC.checkSingleOperandType(
          callBinding,
          operands.get(1),
          0,
          throwOnFailure)) {
        return false;
      }
    } else {
      RelDataType t1 = validator.deriveType(scope, operands.get(1));
      RelDataType t2 = validator.deriveType(scope, operands.get(2));

      if (SqlTypeUtil.inCharFamily(t1)) {
        if (!OperandTypes.STRING.checkSingleOperandType(
            callBinding,
            operands.get(1),
            0,
            throwOnFailure)) {
          return false;
        }
        if (!OperandTypes.STRING.checkSingleOperandType(
            callBinding,
            operands.get(2),
            0,
            throwOnFailure)) {
          return false;
        }

        if (!SqlTypeUtil.isCharTypeComparable(callBinding, operands,
            throwOnFailure)) {
          return false;
        }
      } else {
        if (!OperandTypes.NUMERIC.checkSingleOperandType(
            callBinding,
            operands.get(1),
            0,
            throwOnFailure)) {
          return false;
        }
        if (!OperandTypes.NUMERIC.checkSingleOperandType(
            callBinding,
            operands.get(2),
            0,
            throwOnFailure)) {
          return false;
        }
      }

      if (!SqlTypeUtil.inSameFamily(t1, t2)) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
        return false;
      }
    }
    return true;
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 3);
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep("FROM");
    call.operand(1).unparse(writer, leftPrec, rightPrec);

    if (3 == call.operandCount()) {
      writer.sep("FOR");
      call.operand(2).unparse(writer, leftPrec, rightPrec);
    }

    writer.endFunCall(frame);
  }

  public SqlMonotonicity getMonotonicity(
      SqlCall call,
      SqlValidatorScope scope) {
    // SUBSTRING(x FROM 0 FOR constant) has same monotonicity as x
    final List<SqlNode> operands = call.getOperandList();
    if (operands.size() == 3) {
      final SqlNode op0 = operands.get(0);
      final SqlNode op1 = operands.get(1);
      final SqlNode op2 = operands.get(2);
      final SqlMonotonicity mono0 = op0.getMonotonicity(scope);
      if ((mono0 != SqlMonotonicity.NOT_MONOTONIC)
          && op1.getMonotonicity(scope) == SqlMonotonicity.CONSTANT
          && op1 instanceof SqlLiteral
          && ((SqlLiteral) op1).bigDecimalValue().equals(BigDecimal.ZERO)
          && op2.getMonotonicity(scope) == SqlMonotonicity.CONSTANT) {
        return mono0.unstrict();
      }
    }
    return super.getMonotonicity(call, scope);
  }
}

// End SqlSubstringFunction.java
