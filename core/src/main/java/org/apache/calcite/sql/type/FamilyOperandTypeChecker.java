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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Predicate;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Operand type-checking strategy which checks operands for inclusion in type
 * families.
 */
public class FamilyOperandTypeChecker implements SqlSingleOperandTypeChecker,
    ImplicitCastOperandTypeChecker {
  //~ Instance fields --------------------------------------------------------

  protected final ImmutableList<SqlTypeFamily> families;
  protected final Predicate<Integer> optional;

  //~ Constructors -----------------------------------------------------------

  /**
   * Package private. Create using {@link OperandTypes#family}.
   */
  FamilyOperandTypeChecker(List<SqlTypeFamily> families,
      Predicate<Integer> optional) {
    this.families = ImmutableList.copyOf(families);
    this.optional = optional;
  }

  //~ Methods ----------------------------------------------------------------

  public boolean isOptional(int i) {
    return optional.test(i);
  }

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
      if (callBinding.getValidator().isTypeCoercionEnabled()) {
        return true;
      } else if (throwOnFailure) {
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

  public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (families.size() != callBinding.getOperandCount()) {
      // assume this is an inapplicable sub-rule of a composite rule;
      // don't throw
      return false;
    }
    for (Ord<SqlNode> op : Ord.zip(callBinding.operands())) {
      if (!checkSingleOperandType(
          callBinding,
          op.e,
          op.i,
          false)) {
        // try to coerce type if it is allowed.
        boolean coerced = false;
        if (callBinding.getValidator().isTypeCoercionEnabled()) {
          TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
          ImmutableList.Builder<RelDataType> builder = ImmutableList.builder();
          for (int i = 0; i < callBinding.getOperandCount(); i++) {
            builder.add(callBinding.getOperandType(i));
          }
          ImmutableList<RelDataType> dataTypes = builder.build();
          coerced = typeCoercion.builtinFunctionCoercion(callBinding, dataTypes, families);
        }
        // re-validate the new nodes type.
        for (Ord<SqlNode> op1 : Ord.zip(callBinding.operands())) {
          if (!checkSingleOperandType(
              callBinding,
              op1.e,
              op1.i,
              throwOnFailure)) {
            return false;
          }
        }
        return coerced;
      }
    }
    return true;
  }

  @Override public boolean checkOperandTypesWithoutTypeCoercion(SqlCallBinding callBinding,
      boolean throwOnFailure) {
    if (families.size() != callBinding.getOperandCount()) {
      // assume this is an inapplicable sub-rule of a composite rule;
      // don't throw exception.
      return false;
    }

    for (Ord<SqlNode> op : Ord.zip(callBinding.operands())) {
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

  @Override public SqlTypeFamily getOperandSqlTypeFamily(int iFormalOperand) {
    return families.get(iFormalOperand);
  }

  public SqlOperandCountRange getOperandCountRange() {
    final int max = families.size();
    int min = max;
    while (min > 0 && optional.test(min - 1)) {
      --min;
    }
    return SqlOperandCountRanges.between(min, max);
  }

  public String getAllowedSignatures(SqlOperator op, String opName) {
    return SqlUtil.getAliasedSignature(op, opName, families);
  }

  public Consistency getConsistency() {
    return Consistency.NONE;
  }
}

// End FamilyOperandTypeChecker.java
