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

import java.util.List;

import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.Pair;

import net.hydromatic.linq4j.Ord;

import com.google.common.collect.ImmutableList;

/**
 * AssignableOperandTypeChecker implements {@link SqlOperandTypeChecker} by
 * verifying that the type of each argument is assignable to a predefined set of
 * parameter types (under the SQL definition of "assignable").
 */
public class AssignableOperandTypeChecker implements SqlOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  private final List<RelDataType> paramTypes;

  //~ Constructors -----------------------------------------------------------

  /**
   * Instantiates this strategy with a specific set of parameter types.
   *
   * @param paramTypes parameter types for operands; index in this array
   *                   corresponds to operand number
   */
  public AssignableOperandTypeChecker(List<RelDataType> paramTypes) {
    this.paramTypes = ImmutableList.copyOf(paramTypes);
  }

  //~ Methods ----------------------------------------------------------------

  // implement SqlOperandTypeChecker
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(paramTypes.size());
  }

  // implement SqlOperandTypeChecker
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    final List<SqlNode> operands = callBinding.getCall().getOperandList();
    for (Pair<RelDataType, SqlNode> pair : Pair.zip(paramTypes, operands)) {
      RelDataType argType =
          callBinding.getValidator().deriveType(
              callBinding.getScope(),
              pair.right);
      if (!SqlTypeUtil.canAssignFrom(pair.left, argType)) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        } else {
          return false;
        }
      }
    }
    return true;
  }

  // implement SqlOperandTypeChecker
  public String getAllowedSignatures(SqlOperator op, String opName) {
    StringBuilder sb = new StringBuilder();
    sb.append(opName);
    sb.append("(");
    for (Ord<RelDataType> paramType : Ord.zip(paramTypes)) {
      if (paramType.i > 0) {
        sb.append(", ");
      }
      sb.append("<");
      sb.append(paramType.e.getFamily());
      sb.append(">");
    }
    sb.append(")");
    return sb.toString();
  }
}

// End AssignableOperandTypeChecker.java
