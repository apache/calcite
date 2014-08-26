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
import org.eigenbase.util.Util;

import com.google.common.collect.ImmutableList;

import static org.eigenbase.util.Static.RESOURCE;

/**
 * Parameter type-checking strategy where all operand types must be the same.
 */
public class SameOperandTypeChecker implements SqlSingleOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  protected final int nOperands;

  //~ Constructors -----------------------------------------------------------

  public SameOperandTypeChecker(
      int nOperands) {
    this.nOperands = nOperands;
  }

  //~ Methods ----------------------------------------------------------------

  // implement SqlOperandTypeChecker
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    return checkOperandTypesImpl(
        callBinding,
        throwOnFailure,
        callBinding);
  }

  protected List<Integer> getOperandList(int operandCount) {
    return nOperands == -1
        ? Util.range(0, operandCount)
        : Util.range(0, nOperands);
  }

  private boolean checkOperandTypesImpl(
      SqlOperatorBinding operatorBinding,
      boolean throwOnFailure,
      SqlCallBinding callBinding) {
    int nOperandsActual = nOperands;
    if (nOperandsActual == -1) {
      nOperandsActual = operatorBinding.getOperandCount();
    }
    assert !(throwOnFailure && (callBinding == null));
    RelDataType[] types = new RelDataType[nOperandsActual];
    final List<Integer> operandList =
        getOperandList(operatorBinding.getOperandCount());
    for (int i : operandList) {
      if (operatorBinding.isOperandNull(i, false)) {
        if (throwOnFailure) {
          throw callBinding.getValidator().newValidationError(
              callBinding.getCall().operand(i), RESOURCE.nullIllegal());
        } else {
          return false;
        }
      }
      types[i] = operatorBinding.getOperandType(i);
    }
    int prev = -1;
    for (int i : operandList) {
      if (prev >= 0) {
        if (!SqlTypeUtil.isComparable(types[i], types[prev])) {
          if (!throwOnFailure) {
            return false;
          }

          // REVIEW jvs 5-June-2005: Why don't we use
          // newValidationSignatureError() here?  It gives more
          // specific diagnostics.
          throw callBinding.newValidationError(
              RESOURCE.needSameTypeParameter());
        }
      }
      prev = i;
    }
    return true;
  }

  /**
   * Similar functionality to {@link #checkOperandTypes(SqlCallBinding,
   * boolean)}, but not part of the interface, and cannot throw an error.
   */
  public boolean checkOperandTypes(
      SqlOperatorBinding operatorBinding) {
    return checkOperandTypesImpl(operatorBinding, false, null);
  }

  // implement SqlOperandTypeChecker
  public SqlOperandCountRange getOperandCountRange() {
    if (nOperands == -1) {
      return SqlOperandCountRanges.any();
    } else {
      return SqlOperandCountRanges.of(nOperands);
    }
  }

  // implement SqlOperandTypeChecker
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return SqlUtil.getAliasedSignature(op, opName,
        nOperands == -1
            ? ImmutableList.of("EQUIVALENT_TYPE", "EQUIVALENT_TYPE", "...")
            : Collections.nCopies(nOperands, "EQUIVALENT_TYPE"));
  }

  public boolean checkSingleOperandType(
      SqlCallBinding callBinding,
      SqlNode operand,
      int iFormalOperand,
      boolean throwOnFailure) {
    throw new UnsupportedOperationException(); // TODO:
  }
}

// End SameOperandTypeChecker.java
