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

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.sql.validate.*;

import com.google.common.collect.ImmutableList;

/**
 * SqlOverlapsOperator represents the SQL:1999 standard {@code OVERLAPS}
 * function. Determines whether two anchored time intervals overlap.
 */
public class SqlOverlapsOperator extends SqlSpecialOperator {
  //~ Static fields/initializers ---------------------------------------------

  private static final SqlWriter.FrameType FRAME_TYPE =
      SqlWriter.FrameTypeEnum.create("OVERLAPS");

  //~ Constructors -----------------------------------------------------------

  public SqlOverlapsOperator() {
    super("OVERLAPS",
        SqlKind.OVERLAPS,
        30,
        true,
        ReturnTypes.BOOLEAN_NULLABLE,
        InferTypes.FIRST_KNOWN,
        null);
  }

  //~ Methods ----------------------------------------------------------------

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    final SqlWriter.Frame frame =
        writer.startList(FRAME_TYPE, "(", ")");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep(",", true);
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.sep(")", true);
    writer.sep(getName());
    writer.sep("(", true);
    call.operand(2).unparse(writer, leftPrec, rightPrec);
    writer.sep(",", true);
    call.operand(3).unparse(writer, leftPrec, rightPrec);
    writer.endList(frame);
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(4);
  }

  public String getSignatureTemplate(int operandsCount) {
    assert 4 == operandsCount;
    return "({1}, {2}) {0} ({3}, {4})";
  }

  public String getAllowedSignatures(String opName) {
    final String d = "DATETIME";
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
    return ret.toString();
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    SqlCall call = callBinding.getCall();
    SqlValidator validator = callBinding.getValidator();
    SqlValidatorScope scope = callBinding.getScope();
    if (!OperandTypes.DATETIME.checkSingleOperandType(
        callBinding,
        call.operand(0),
        0,
        throwOnFailure)) {
      return false;
    }
    if (!OperandTypes.DATETIME.checkSingleOperandType(
        callBinding,
        call.operand(2),
        0,
        throwOnFailure)) {
      return false;
    }

    RelDataType t0 = validator.deriveType(scope, call.operand(0));
    RelDataType t1 = validator.deriveType(scope, call.operand(1));
    RelDataType t2 = validator.deriveType(scope, call.operand(2));
    RelDataType t3 = validator.deriveType(scope, call.operand(3));

    // t0 must be comparable with t2
    if (!SqlTypeUtil.sameNamedType(t0, t2)) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }

    if (SqlTypeUtil.isDatetime(t1)) {
      // if t1 is of DATETIME,
      // then t1 must be comparable with t0
      if (!SqlTypeUtil.sameNamedType(t0, t1)) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
        return false;
      }
    } else if (!SqlTypeUtil.isInterval(t1)) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }

    if (SqlTypeUtil.isDatetime(t3)) {
      // if t3 is of DATETIME,
      // then t3 must be comparable with t2
      if (!SqlTypeUtil.sameNamedType(t2, t3)) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
        return false;
      }
    } else if (!SqlTypeUtil.isInterval(t3)) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }
    return true;
  }
}

// End SqlOverlapsOperator.java
