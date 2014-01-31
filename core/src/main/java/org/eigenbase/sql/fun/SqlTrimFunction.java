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

import java.util.Arrays;
import java.util.List;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.type.*;

import com.google.common.collect.ImmutableList;

/**
 * Definition of the "TRIM" builtin SQL function.
 */
public class SqlTrimFunction extends SqlFunction {
  //~ Enums ------------------------------------------------------------------

  /**
   * Defines the enumerated values "LEADING", "TRAILING", "BOTH".
   */
  public enum Flag implements SqlLiteral.SqlSymbol {
    BOTH(1, 1), LEADING(1, 0), TRAILING(0, 1);

    private final int left;
    private final int right;

    Flag(int left, int right) {
      this.left = left;
      this.right = right;
    }

    public int getLeft() {
      return left;
    }

    public int getRight() {
      return right;
    }
  }

  //~ Constructors -----------------------------------------------------------

  public SqlTrimFunction() {
    super(
        "TRIM",
        SqlKind.TRIM,
        new SqlTypeTransformCascade(
            SqlTypeStrategies.rtiThirdArgType,
            SqlTypeTransforms.TO_NULLABLE,
            SqlTypeTransforms.TO_VARYING),
        null,
        SqlTypeStrategies.and(
            SqlTypeStrategies.family(
                SqlTypeFamily.ANY,
                SqlTypeFamily.STRING,
                SqlTypeFamily.STRING),
            // Arguments 1 and 2 must have same type
            new SameOperandTypeChecker(3) {
              @Override
              protected List<Integer> getOperandList(int operandCount) {
                return ImmutableList.of(1, 2);
              }
            }),
        SqlFunctionCategory.STRING);
  }

  //~ Methods ----------------------------------------------------------------

  public void unparse(
      SqlWriter writer,
      SqlNode[] operands,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    assert operands[0] instanceof SqlLiteral : operands[0];
    operands[0].unparse(writer, leftPrec, rightPrec);
    operands[1].unparse(writer, leftPrec, rightPrec);
    writer.sep("FROM");
    operands[2].unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(frame);
  }

  public String getSignatureTemplate(final int operandsCount) {
    switch (operandsCount) {
    case 3:
      return "{0}([BOTH|LEADING|TRAILING} {1} FROM {2})";
    default:
      throw new AssertionError();
    }
  }

  public SqlCall createCall(
      SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNode... operands) {
    assert functionQualifier == null;
    switch (operands.length) {
    case 1:
      // This variant occurs when someone writes TRIM(string)
      // as opposed to the sugared syntax TRIM(string FROM string).
      operands = new SqlNode[]{
        SqlLiteral.createSymbol(Flag.BOTH, SqlParserPos.ZERO),
        SqlLiteral.createCharString(" ", pos),
        operands[0]
      };
      break;
    case 3:
      assert operands[0] instanceof SqlLiteral
          && ((SqlLiteral) operands[0]).getValue() instanceof Flag;
      if (operands[1] == null) {
        operands[1] = SqlLiteral.createCharString(" ", pos);
      }
      break;
    default:
      throw new IllegalArgumentException(
          "invalid operand count " + Arrays.toString(operands));
    }
    return super.createCall(functionQualifier, pos, operands);
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (!super.checkOperandTypes(callBinding, throwOnFailure)) {
      return false;
    }
    final SqlNode[] operands = callBinding.getCall().getOperands();
    return SqlTypeUtil.isCharTypeComparable(
        callBinding,
        new SqlNode[]{operands[1], operands[2]},
        throwOnFailure);
  }
}

// End SqlTrimFunction.java
