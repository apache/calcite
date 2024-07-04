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
import org.apache.calcite.sql.SqlUtil;

import com.google.common.collect.ImmutableList;

import static org.apache.calcite.sql.type.NonNullableAccessors.getComponentTypeOrThrow;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Parameter type checking strategy, where the operand size is 3, and the first type is array,
 * the second type is integer, and the third type is the component type of the first array (
 * strictly matched without leastRestrictive conversion).
 */
public class ArrayInsertOperandTypeChecker extends SameOperandTypeChecker {

  public ArrayInsertOperandTypeChecker() {
    super(3);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    // all operands can't be null (without cast)
    for (SqlNode node : callBinding.operands()) {
      if (SqlUtil.isNullLiteral(node, false)) {
        if (throwOnFailure) {
          throw callBinding.getValidator().newValidationError(node,
              RESOURCE.argumentMustNotBeNull(callBinding.getOperator().getName()));
        } else {
          return false;
        }
      }
    }

    // op0 is an array type
    final SqlNode op0 = callBinding.operand(0);
    if (!OperandTypes.ARRAY.checkSingleOperandType(
        callBinding,
        op0,
        0,
        throwOnFailure)) {
      return false;
    }

    // op1 is an Integer type
    final SqlNode op1 = callBinding.operand(1);
    RelDataType op1Type = SqlTypeUtil.deriveType(callBinding, op1);
    SqlTypeName op1TypeName = op1Type.getSqlTypeName();
    if (op1TypeName != SqlTypeName.INTEGER) {
      if (throwOnFailure) {
        throw callBinding.newError(
            RESOURCE.typeNotComparable(
                op1TypeName.getName(), SqlTypeName.INTEGER.getName()));
      }
      return false;
    }

    // op2 must has same type with op0 component type strictly
    final RelDataType op0ComponentType =
        getComponentTypeOrThrow(SqlTypeUtil.deriveType(callBinding, op0));
    SqlNode op2 = callBinding.operand(2);
    RelDataType op2Type = SqlTypeUtil.deriveType(callBinding, op2);
    RelDataType biggest =
        callBinding.getTypeFactory().leastRestrictive(
            ImmutableList.of(op0ComponentType, op2Type));
    if (biggest == null) {
      if (throwOnFailure) {
        throw callBinding.newError(
            RESOURCE.typeNotComparable(
                op0ComponentType.getSqlTypeName().getName(),
                op2Type.getSqlTypeName().getName()));
      }
      return false;
    }

    return true;
  }
}
