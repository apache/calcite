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

import net.hydromatic.linq4j.Ord;

import com.google.common.collect.ImmutableList;

import static org.eigenbase.util.Static.RESOURCE;

/**
 * Operand type-checking strategy which checks operands for inclusion in type
 * families.
 */
public class FamilyOperandTypeChecker implements SqlSingleOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  protected final ImmutableList<SqlTypeFamily> families;

  //~ Constructors -----------------------------------------------------------

  /**
   * Package private. Create using {@link OperandTypes#family}.
   */
  FamilyOperandTypeChecker(List<SqlTypeFamily> families) {
    this.families = ImmutableList.copyOf(families);
  }

  //~ Methods ----------------------------------------------------------------

  // implement SqlSingleOperandTypeChecker
  public boolean checkSingleOperandType(
      SqlCallBinding callBinding,
      SqlNode node,
      int iFormalOperand,
      boolean throwOnFailure) {
    SqlTypeFamily family = families.get(iFormalOperand);
    if (family == SqlTypeFamily.ANY) {
      // no need to check
      return true;
    }
    if (SqlUtil.isNullLiteral(node, false)) {
      if (throwOnFailure) {
        throw callBinding.getValidator().newValidationError(node,
            RESOURCE.nullIllegal());
      } else {
        return false;
      }
    }
    RelDataType type =
        callBinding.getValidator().deriveType(
            callBinding.getScope(),
            node);
    SqlTypeName typeName = type.getSqlTypeName();

    // Pass type checking for operators if it's of type 'ANY'.
    if (typeName.getFamily() == SqlTypeFamily.ANY) {
      return true;
    }

    if (!family.getTypeNames().contains(typeName)) {
      if (throwOnFailure) {
        throw callBinding.newValidationSignatureError();
      }
      return false;
    }
    return true;
  }

  // implement SqlOperandTypeChecker
  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (families.size() != callBinding.getOperandCount()) {
      // assume this is an inapplicable sub-rule of a composite rule;
      // don't throw
      return false;
    }

    for (Ord<SqlNode> op : Ord.zip(callBinding.getCall().getOperandList())) {
      if (!checkSingleOperandType(
          callBinding,
          op.e,
          op.i,
          throwOnFailure)) {
        return false;
      }
    }
    return true;
  }

  // implement SqlOperandTypeChecker
  public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.of(families.size());
  }

  // implement SqlOperandTypeChecker
  public String getAllowedSignatures(SqlOperator op, String opName) {
    return SqlUtil.getAliasedSignature(op, opName, families);
  }
}

// End FamilyOperandTypeChecker.java
