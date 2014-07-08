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
package org.apache.optiq.sql.type;

import org.apache.optiq.sql.*;
import org.apache.optiq.util.*;

import static org.apache.optiq.util.Static.RESOURCE;

/**
 * Parameter type-checking strategy type must be a literal (whether null is
 * allowed is determined by the constructor). <code>CAST(NULL as ...)</code> is
 * considered to be a NULL literal but not <code>CAST(CAST(NULL as ...) AS
 * ...)</code>
 */
public class LiteralOperandTypeChecker implements SqlSingleOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  private boolean allowNull;

  //~ Constructors -----------------------------------------------------------

  public LiteralOperandTypeChecker(boolean allowNull) {
    this.allowNull = allowNull;
  }

  //~ Methods ----------------------------------------------------------------

  public boolean checkSingleOperandType(
      SqlCallBinding callBinding,
      SqlNode node,
      int iFormalOperand,
      boolean throwOnFailure) {
    Util.discard(iFormalOperand);

    if (SqlUtil.isNullLiteral(node, true)) {
      if (allowNull) {
        return true;
      }
      if (throwOnFailure) {
        throw callBinding.newError(
            RESOURCE.argumentMustNotBeNull(
                callBinding.getOperator().getName()));
      }
      return false;
    }
    if (!SqlUtil.isLiteral(node) && !SqlUtil.isLiteralChain(node)) {
      if (throwOnFailure) {
        throw callBinding.newError(
            RESOURCE.argumentMustBeLiteral(
                callBinding.getOperator().getName()));
      }
      return false;
    }

    return true;
  }

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return checkSingleOperandType(
        callBinding,
        callBinding.getCall().operand(0),
        0,
        throwOnFailure);
  }

  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(1);
  }

  public String getAllowedSignatures(SqlOperator op, String opName) {
    return "<LITERAL>";
  }
}

// End LiteralOperandTypeChecker.java
