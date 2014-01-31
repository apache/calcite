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
package org.eigenbase.sql.fun;

import java.math.*;

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

    int n = call.operands.length;
    assert (3 == n) || (2 == n);
    if (!OperandTypes.STRING.checkSingleOperandType(
        callBinding,
        call.operands[0],
        0,
        throwOnFailure)) {
      return false;
    }
    if (2 == n) {
      if (!OperandTypes.NUMERIC.checkSingleOperandType(
          callBinding,
          call.operands[1],
          0,
          throwOnFailure)) {
        return false;
      }
    } else {
      RelDataType t1 = validator.deriveType(scope, call.operands[1]);
      RelDataType t2 = validator.deriveType(scope, call.operands[2]);

      if (SqlTypeUtil.inCharFamily(t1)) {
        if (!OperandTypes.STRING.checkSingleOperandType(
            callBinding,
            call.operands[1],
            0,
            throwOnFailure)) {
          return false;
        }
        if (!OperandTypes.STRING.checkSingleOperandType(
            callBinding,
            call.operands[2],
            0,
            throwOnFailure)) {
          return false;
        }

        if (!SqlTypeUtil.isCharTypeComparable(
            callBinding,
            callBinding.getCall().getOperands(),
            throwOnFailure)) {
          return false;
        }
      } else {
        if (!OperandTypes.NUMERIC.checkSingleOperandType(
            callBinding,
            call.operands[1],
            0,
            throwOnFailure)) {
          return false;
        }
        if (!OperandTypes.NUMERIC.checkSingleOperandType(
            callBinding,
            call.operands[2],
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
      SqlNode[] operands,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    operands[0].unparse(writer, leftPrec, rightPrec);
    writer.sep("FROM");
    operands[1].unparse(writer, leftPrec, rightPrec);

    if (3 == operands.length) {
      writer.sep("FOR");
      operands[2].unparse(writer, leftPrec, rightPrec);
    }

    writer.endFunCall(frame);
  }

  public SqlMonotonicity getMonotonicity(
      SqlCall call,
      SqlValidatorScope scope) {
    // SUBSTRING(x FROM 0 FOR constant) has same monotonicity as x
    if (call.operands.length == 3) {
      final SqlMonotonicity mono0 =
          call.operands[0].getMonotonicity(scope);
      if ((mono0 != SqlMonotonicity.NotMonotonic)
          && (call.operands[1].getMonotonicity(scope)
          == SqlMonotonicity.Constant)
          && (call.operands[1] instanceof SqlLiteral)
          && ((SqlLiteral) call.operands[1]).bigDecimalValue().equals(
              BigDecimal.ZERO)
          && (call.operands[2].getMonotonicity(scope)
          == SqlMonotonicity.Constant)) {
        return mono0.unstrict();
      }
    }
    return super.getMonotonicity(call, scope);
  }
}

// End SqlSubstringFunction.java
