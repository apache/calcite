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

import com.google.common.collect.ImmutableList;

import static org.apache.calcite.sql.type.NonNullableAccessors.getComponentTypeOrThrow;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parameter type-checking strategy where types must be Array and Array element type.
 */
public class ArrayElementOperandTypeChecker implements SqlOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  private final boolean arrayMayBeNull;
  private final boolean elementMayBeNull;

  //~ Constructors -----------------------------------------------------------

  public ArrayElementOperandTypeChecker(boolean arrayMayBeNull, boolean elementMayBeNull) {
    this.arrayMayBeNull = arrayMayBeNull;
    this.elementMayBeNull = elementMayBeNull;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final SqlNode op0 = callBinding.operand(0);
    RelDataType arrayType = SqlTypeUtil.deriveType(callBinding, op0);

    // Check if op0 is allowed to be NULL
    if (!this.arrayMayBeNull && arrayType.getSqlTypeName() == SqlTypeName.NULL) {
      if (throwOnFailure) {
        throw callBinding.getValidator().newValidationError(op0, RESOURCE.nullIllegal());
      } else {
        return false;
      }
    }

    // Check that op0 is an ARRAY type
    if (!OperandTypes.ARRAY.checkSingleOperandType(
        callBinding,
        op0,
        0,
        throwOnFailure)) {
      return false;
    }
    RelDataType arrayComponentType =
        getComponentTypeOrThrow(SqlTypeUtil.deriveType(callBinding, op0));

    final SqlNode op1 = callBinding.operand(1);
    RelDataType elementType = SqlTypeUtil.deriveType(callBinding, op1);

    // Check if elementType is allowed to be NULL
    if (!this.elementMayBeNull && elementType.getSqlTypeName() == SqlTypeName.NULL) {
      if (throwOnFailure) {
        throw callBinding.getValidator().newValidationError(op1, RESOURCE.nullIllegal());
      } else {
        return false;
      }
    }

    RelDataType biggest =
        callBinding.getTypeFactory().leastRestrictive(
            ImmutableList.of(arrayComponentType, elementType));
    if (biggest == null) {
      if (throwOnFailure) {
        throw callBinding.newError(
            RESOURCE.typeNotComparable(
                arrayComponentType.toString(), elementType.toString()));
      }

      return false;
    }
    return true;
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(2);
  }

  @Override public String getAllowedSignatures(SqlOperator op, String opName) {
    return "<ARRAY> " + opName + " <ARRAY>";
  }
}
