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
package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

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

  @Override public Consistency getConsistency() {
    return Consistency.NONE;
  }

  @Override public boolean isOptional(int i) {
    return false;
  }

  @Override public boolean checkOperandTypes(
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

  protected boolean checkOperandTypesImpl(
      SqlOperatorBinding operatorBinding,
      boolean throwOnFailure,
      @Nullable SqlCallBinding callBinding) {
    if (throwOnFailure && callBinding == null) {
      throw new IllegalArgumentException(
          "callBinding must be non-null in case throwOnFailure=true");
    }
    int nOperandsActual = nOperands;
    if (nOperandsActual == -1) {
      nOperandsActual = operatorBinding.getOperandCount();
    }
    RelDataType[] types = new RelDataType[nOperandsActual];
    final List<Integer> operandList =
        getOperandList(operatorBinding.getOperandCount());
    for (int i : operandList) {
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
          throw requireNonNull(callBinding, "callBinding").newValidationError(
              RESOURCE.needSameTypeParameter());
        }
      }
      prev = i;
    }
    return true;
  }

  /**
   * Similar functionality to
   * {@link #checkOperandTypes(SqlCallBinding, boolean)}, but not part of the
   * interface, and cannot throw an error.
   */
  public boolean checkOperandTypes(
      SqlOperatorBinding operatorBinding, SqlCallBinding callBinding) {
    return checkOperandTypesImpl(operatorBinding, false, null);
  }

  // implement SqlOperandTypeChecker
  @Override public SqlOperandCountRange getOperandCountRange() {
    if (nOperands == -1) {
      return SqlOperandCountRanges.any();
    } else {
      return SqlOperandCountRanges.of(nOperands);
    }
  }

  @Override public String getAllowedSignatures(SqlOperator op, String opName) {
    final String typeName = getTypeName();
    return SqlUtil.getAliasedSignature(op, opName,
        nOperands == -1
            ? ImmutableList.of(typeName, typeName, "...")
            : Collections.nCopies(nOperands, typeName));
  }

  /** Override to change the behavior of
   * {@link #getAllowedSignatures(SqlOperator, String)}. */
  protected String getTypeName() {
    return "EQUIVALENT_TYPE";
  }

  @Override public boolean checkSingleOperandType(
      SqlCallBinding callBinding,
      SqlNode operand,
      int iFormalOperand,
      boolean throwOnFailure) {
    throw new UnsupportedOperationException(); // TODO:
  }
}
