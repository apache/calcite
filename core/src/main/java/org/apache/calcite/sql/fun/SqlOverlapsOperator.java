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
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeUtil;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * SqlOverlapsOperator represents the SQL:1999 standard {@code OVERLAPS}
 * function. Determines whether two anchored time intervals overlap.
 */
public class SqlOverlapsOperator extends SqlBinaryOperator {
  //~ Constructors -----------------------------------------------------------

  SqlOverlapsOperator(SqlKind kind) {
    super(kind.sql, kind, 30, true, ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        OperandTypes.sequence("'<PERIOD> " + kind.sql + " <PERIOD>'",
            OperandTypes.PERIOD, OperandTypes.PERIOD));
  }

  //~ Methods ----------------------------------------------------------------

  @Override public void unparse(SqlWriter writer, SqlCall call, int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
    arg(writer, call, leftPrec, rightPrec, 0);
    writer.sep(getName());
    arg(writer, call, leftPrec, rightPrec, 1);
    writer.endList(frame);
  }

  void arg(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec, int i) {
    if (SqlUtil.isCallTo(call.operand(i), SqlStdOperatorTable.ROW)) {
      SqlCall row = call.operand(i);
      writer.keyword("PERIOD");
      writer.sep("(", true);
      row.operand(0).unparse(writer, leftPrec, rightPrec);
      writer.sep(",", true);
      row.operand(1).unparse(writer, leftPrec, rightPrec);
      writer.sep(")", true);
    } else {
      call.operand(i).unparse(writer, leftPrec, rightPrec);
    }
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  /**
   * Returns a template describing how the operator signature is to be built.
   *
   * @param operandsCount is used with functions that can take a variable
   *                      number of operands
   * @return signature template, where {0} is the operator name and {1}, {2}, etc are operands
   */
  @Override public @Nullable String getSignatureTemplate(final int operandsCount) {
    // This function can be called in 3 ways:
    // - as a binary operator; format like a binary operator left OP right
    // - as a ternary operator, for (a, b) CONTAINS c
    // - as a quaternary operator, for (a, b) OVERLAPS (c, d)
    if (operandsCount == 2) {
      return "{1} {0} {2}";
    } else if (operandsCount == 3) {
      return "({1}, {2}) {0} {3}";
    } else if (operandsCount == 4) {
      return "({1}, {2}) {0} ({3}, {4})";
    }
    throw new IllegalArgumentException("Unexpected operand count " + operandsCount);
  }

  @Override public String getAllowedSignatures(String opName) {
    final String d = "DT";
    final String i = "INTERVAL";
    String[] typeNames = {
        d, d,
        d, i,
        i, d,
        i, i
    };

    StringBuilder ret = new StringBuilder();
    for (int y = 0; y < typeNames.length; y += 2) {
      if (y > 0) {
        ret.append(NL);
      }
      ret.append(
          SqlUtil.getAliasedSignature(this, opName,
              ImmutableList.of(d, typeNames[y], d, typeNames[y + 1])));
    }
    if (opName.equalsIgnoreCase("contains")) {
      // Two more forms supported: (DT, DT) CONTAINS DT and (DT, INTERVAL) CONTAINS DT
      ret.append(NL);
      ret.append(SqlUtil.getAliasedSignature(this, opName, ImmutableList.of(d, d, d)));
      ret.append(NL);
      ret.append(SqlUtil.getAliasedSignature(this, opName, ImmutableList.of(d, i, d)));
    }
    ret.append(NL);
    ret.append("Where 'DT' is one of 'DATE', 'TIME', or 'TIMESTAMP', "
        + "the same for all arguments.");
    return ret.toString();
  }

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (!OperandTypes.PERIOD.checkSingleOperandType(callBinding,
        callBinding.operand(0), 0, throwOnFailure)) {
      return false;
    }
    final SqlSingleOperandTypeChecker rightChecker;
    switch (kind) {
    case CONTAINS:
      // A ternary call of the form (a, b) CONTAINS c
      // OR a quaternary call of the form (a, b) CONTAINS (c, d)
      rightChecker = OperandTypes.PERIOD_OR_DATETIME;
      break;
    case OVERLAPS:
    case PRECEDES:
    case IMMEDIATELY_PRECEDES:
    case SUCCEEDS:
    case IMMEDIATELY_SUCCEEDS:
    case PERIOD_EQUALS:
      // Always a quaternary call of the form (a, b) OVERLAPS (c, d)
      rightChecker = OperandTypes.PERIOD;
      break;
    default:
      throw new IllegalArgumentException("Unexpected operation " + kind);
    }
    if (!rightChecker.checkSingleOperandType(callBinding,
        callBinding.operand(1), 0, throwOnFailure)) {
      return false;
    }
    final RelDataType t0 = callBinding.getOperandType(0);
    final RelDataType t1 = callBinding.getOperandType(1);
    if (!SqlTypeUtil.isDatetime(t1)) {
      // "quaternary" call, of the form (a, b) OVERLAPS (c, d)
      final RelDataType t00 = t0.getFieldList().get(0).getType();
      final RelDataType t10 = t1.getFieldList().get(0).getType();
      if (!SqlTypeUtil.sameNamedType(t00, t10)) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
        return false;
      }
    } else {
      // "ternary" call, of the form (a, b) CONTAINS c
      final RelDataType t00 = t0.getFieldList().get(0).getType();
      if (!SqlTypeUtil.sameNamedType(t00, t1)) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
        return false;
      }
    }
    return true;
  }
}
