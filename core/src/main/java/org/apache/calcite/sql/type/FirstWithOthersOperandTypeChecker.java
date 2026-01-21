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
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlOperatorBinding;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * Parameter type-checking strategy where the first operand type should be compatible with others.
 */
public class FirstWithOthersOperandTypeChecker extends ComparableOperandTypeChecker {
  public FirstWithOthersOperandTypeChecker(int nOperands,
      RelDataTypeComparability requiredComparability, Consistency consistency) {
    super(nOperands, requiredComparability, Consistency.NONE);
  }

  @Override protected boolean checkOperandTypesImpl(
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

    if (!checkOperandTypeComparable(types, operandList)) {
      if (!throwOnFailure) {
        return false;
      }

      // REVIEW jvs 5-June-2005: Why don't we use
      // newValidationSignatureError() here?  It gives more
      // specific diagnostics.
      throw requireNonNull(callBinding, "callBinding").newValidationError(
          RESOURCE.needSameTypeParameter());
    }

    return true;
  }

  private boolean checkOperandTypeComparable(
      RelDataType[] types,
      List<Integer> operandList) {
    final int operandListSize = operandList.size();

    if (operandListSize <= 1) {
      return true;
    }

    RelDataType firstOperandType = types[operandList.get(0)];
    // Here we compare the first operand type with others, so first one is skipped
    for (int i = 1; i < operandListSize; i++) {
      if (!SqlTypeUtil.isComparable(firstOperandType, types[operandList.get(i)])) {
        return false;
      }
    }

    return true;
  }
}
