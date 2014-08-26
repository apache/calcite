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
package org.eigenbase.sql.type;

import java.util.*;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;

/**
 * Type checking strategy which verifies that types have the required attributes
 * to be used as arguments to comparison operators.
 */
public class ComparableOperandTypeChecker extends SameOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  private final RelDataTypeComparability requiredComparability;

  //~ Constructors -----------------------------------------------------------

  public ComparableOperandTypeChecker(
      int nOperands,
      RelDataTypeComparability requiredComparability) {
    super(nOperands);
    this.requiredComparability = requiredComparability;
  }

  //~ Methods ----------------------------------------------------------------

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    boolean b = true;
    for (int i = 0; i < nOperands; ++i) {
      RelDataType type = callBinding.getOperandType(i);
      if (!checkType(callBinding, throwOnFailure, type)) {
        b = false;
      }
    }
    if (b) {
      b = super.checkOperandTypes(callBinding, false);
      if (!b && throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
    }
    return b;
  }

  private boolean checkType(
      SqlCallBinding callBinding,
      boolean throwOnFailure,
      RelDataType type) {
    if (type.getComparability().ordinal()
        < requiredComparability.ordinal()) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  /**
   * Similar functionality to {@link #checkOperandTypes(SqlCallBinding,
   * boolean)}, but not part of the interface, and cannot throw an error.
   */
  public boolean checkOperandTypes(
      SqlOperatorBinding callBinding) {
    boolean b = true;
    for (int i = 0; i < nOperands; ++i) {
      RelDataType type = callBinding.getOperandType(i);
      boolean result;
      if (type.getComparability().ordinal()
          < requiredComparability.ordinal()) {
        result = false;
      } else {
        result = true;
      }
      if (!result) {
        b = false;
      }
    }
    if (b) {
      b = super.checkOperandTypes(callBinding);
    }
    return b;
  }

  // implement SqlOperandTypeChecker
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return SqlUtil.getAliasedSignature(op, opName,
        Collections.nCopies(nOperands, "COMPARABLE_TYPE"));
  }
}

// End ComparableOperandTypeChecker.java
