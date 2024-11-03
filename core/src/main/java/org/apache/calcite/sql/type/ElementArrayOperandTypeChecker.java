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
 * Parameter type-checking strategy where types must be Array element and Array type.
 */
public class ElementArrayOperandTypeChecker implements SqlOperandTypeChecker {

  @Override public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
    final SqlNode op1 = callBinding.operand(1);
    if (!OperandTypes.ARRAY.checkSingleOperandType(
        callBinding,
        op1,
        1,
        throwOnFailure)) {
      return false;
    }

    RelDataType arrayComponentType =
        getComponentTypeOrThrow(SqlTypeUtil.deriveType(callBinding, op1));
    final SqlNode op0 = callBinding.operand(0);
    RelDataType aryType0 = SqlTypeUtil.deriveType(callBinding, op0);

    RelDataType biggest =
        callBinding.getTypeFactory().leastRestrictive(
            ImmutableList.of(arrayComponentType, aryType0));
    if (biggest == null) {
      if (throwOnFailure) {
        throw callBinding.newError(
            RESOURCE.typeNotComparable(
                arrayComponentType.toString(), aryType0.toString()));
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
