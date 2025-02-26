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

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.Symbolizable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;

/**
 * Definition of the "TRIM" builtin SQL function.
 */
public class SqlTrimFunction extends SqlFunction {
  protected static final SqlTrimFunction INSTANCE =
      new SqlTrimFunction("TRIM", SqlKind.TRIM,
          ReturnTypes.ARG2.andThen(SqlTypeTransforms.TO_NULLABLE)
              .andThen(SqlTypeTransforms.TO_VARYING),
          OperandTypes.ANY_STRING_STRING.and(
              OperandTypes.same(3, 1, 2)));

  //~ Enums ------------------------------------------------------------------

  /**
   * Defines the enumerated values "LEADING", "TRAILING", "BOTH".
   */
  public enum Flag implements Symbolizable {
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

  public SqlTrimFunction(String name, SqlKind kind,
      SqlReturnTypeInference returnTypeInference,
      SqlSingleOperandTypeChecker operandTypeChecker) {
    super(name, kind, returnTypeInference, null, operandTypeChecker,
        SqlFunctionCategory.STRING);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startFunCall(getName());
    assert call.operand(0) instanceof SqlLiteral : call.operand(0);
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.sep("FROM");
    call.operand(2).unparse(writer, leftPrec, rightPrec);
    writer.endFunCall(frame);
  }

  @Override public String getSignatureTemplate(final int operandsCount) {
    switch (operandsCount) {
    case 3:
      return "{0}([BOTH|LEADING|TRAILING] {1} FROM {2})";
    default:
      throw new AssertionError();
    }
  }

  @Override public SqlCall createCall(
      @Nullable SqlLiteral functionQualifier,
      SqlParserPos pos,
      @Nullable SqlNode... operands) {
    assert functionQualifier == null;
    switch (operands.length) {
    case 1:
      // This variant occurs when someone writes TRIM(string)
      // as opposed to the sugared syntax TRIM(string FROM string).
      operands = new SqlNode[]{
        Flag.BOTH.symbol(SqlParserPos.ZERO),
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

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (!super.checkOperandTypes(callBinding, throwOnFailure)) {
      return false;
    }
    switch (kind) {
    case TRIM:
      return SqlTypeUtil.isCharTypeComparable(callBinding,
          ImmutableList.of(callBinding.operand(1), callBinding.operand(2)),
          throwOnFailure);
    default:
      return true;
    }
  }
}
