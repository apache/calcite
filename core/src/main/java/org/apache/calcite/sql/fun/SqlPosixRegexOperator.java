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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeUtil;

import java.util.Arrays;

/**
 * An operator describing the <code>~</code> operator.
 *
 * <p> Syntax: <code>src-value [!] ~ [*] pattern-value</code>
 */
public class SqlPosixRegexOperator extends SqlBinaryOperator {
  // ~ Instance fields --------------------------------------------------------

  private final boolean caseSensitive;
  private final boolean negated;

  // ~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqlPosixRegexOperator.
   *
   * @param name    Operator name
   * @param kind    Kind
   * @param negated Whether this is '!~' or '!~*'
   */
  SqlPosixRegexOperator(
      String name,
      SqlKind kind,
      boolean caseSensitive,
      boolean negated) {
    super(
        name,
        kind,
        32,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.STRING_SAME_SAME_SAME);
    this.caseSensitive = caseSensitive;
    this.negated = negated;
  }

  // ~ Methods ----------------------------------------------------------------

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 3);
  }

  public SqlCall createCall(
      SqlLiteral functionQualifier,
      SqlParserPos pos,
      SqlNode... operands) {
    pos = pos.plusAll(Arrays.asList(operands));
    operands = Arrays.copyOf(operands, operands.length + 1);
    operands[operands.length - 1] = SqlLiteral.createBoolean(caseSensitive, SqlParserPos.ZERO);
    return new SqlBasicCall(this, operands, pos, false, functionQualifier);
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    int operandCount = callBinding.getOperandCount();
    if (operandCount != 2 && operandCount != 3) {
      throw new AssertionError(
          "Unexpected number of args to " + callBinding.getCall() + ": " + operandCount);
    }

    RelDataType op1Type = callBinding.getOperandType(0);
    RelDataType op2Type = callBinding.getOperandType(1);

    if (!SqlTypeUtil.isComparable(op1Type, op2Type)) {
      throw new AssertionError(
          "Incompatible first two operand types " + op1Type + " and " + op2Type);
    }

    return SqlTypeUtil.isCharTypeComparable(
        callBinding,
        callBinding.operands().subList(0, 2),
        throwOnFailure);
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame = writer.startList("", "");
    call.operand(0).unparse(writer, getLeftPrec(), getRightPrec());

    if (this.negated) {
      writer.print("!");
    }
    writer.print("~");
    if (!this.caseSensitive) {
      writer.print("*");
    }
    writer.print(" ");

    call.operand(1).unparse(writer, getLeftPrec(), getRightPrec());
    writer.endList(frame);
  }
}

// End SqlPosixRegexOperator.java
