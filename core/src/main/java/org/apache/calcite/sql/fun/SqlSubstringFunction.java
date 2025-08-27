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
package org.apache.calcite.sql.fun;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlSingleOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlMonotonicity;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Definition of the "SUBSTRING" builtin SQL function.
 */
public class SqlSubstringFunction extends SqlFunction {
  /** Type checker for 3 argument calls. Put the STRING_INTEGER_INTEGER checker
   * first because almost every other type can be coerced to STRING. */
  private static final SqlSingleOperandTypeChecker CHECKER3 =
      OperandTypes.STRING_INTEGER_INTEGER;
          // Not yet implemented
          // .or(OperandTypes.STRING_STRING_STRING);

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates the SqlSubstringFunction.
   */
  SqlSubstringFunction() {
    super(
        "SUBSTRING",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_FORCE_NULLABLE.andThen(SqlTypeTransforms.TO_VARYING),
        null,
        null,
        SqlFunctionCategory.STRING);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public String getSignatureTemplate(final int operandsCount) {
    switch (operandsCount) {
    case 2:
      return "{0}({1} FROM {2})";
    case 3:
      return "{0}({1} FROM {2} FOR {3})";
    default:
      throw new AssertionError("Incorrect " + getName() + " signature, operands "
          + "count = " + operandsCount);
    }
  }

  @Override public String getAllowedSignatures(String opName) {
    StringBuilder ret = new StringBuilder();
    for (Ord<SqlTypeName> typeName : Ord.zip(SqlTypeName.STRING_TYPES)) {
      if (typeName.i > 0) {
        ret.append(NL);
      }
      ret.append(
          SqlUtil.getAliasedSignature(this, opName,
              ImmutableList.of(typeName.e, SqlTypeName.INTEGER)));
      ret.append(NL);
      ret.append(
          SqlUtil.getAliasedSignature(this, opName,
              ImmutableList.of(typeName.e, SqlTypeName.INTEGER,
                  SqlTypeName.INTEGER)));
    }
    return ret.toString();
  }

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    switch (callBinding.operands().size()) {
    default:
      throw new AssertionError();
    case 2:
      return OperandTypes.STRING_NUMERIC
          .checkOperandTypes(callBinding, throwOnFailure);
    case 3:
      if (!CHECKER3
          .checkOperandTypes(callBinding, throwOnFailure)) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
        return false;
      }
      final RelDataType t1 = callBinding.getOperandType(1);
      final RelDataType t2 = callBinding.getOperandType(2);
      if (!(SqlTypeUtil.isIntType(t1) || SqlTypeUtil.isNull(t1))) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
      }
      if (!SqlTypeUtil.inSameFamilyOrNull(t1, t2)) {
        if (throwOnFailure) {
          throw callBinding.newValidationSignatureError();
        }
        return false;
      }
      return true;
    }
  }

  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.between(2, 3);
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    // SUBSTRING(x FROM 0 FOR constant) has same monotonicity as x
    if (call.getOperandCount() == 3) {
      final SqlMonotonicity mono0 = call.getOperandMonotonicity(0);
      if (mono0 != null
          && mono0 != SqlMonotonicity.NOT_MONOTONIC
          && call.getOperandMonotonicity(1) == SqlMonotonicity.CONSTANT
          && Objects.equals(call.getOperandLiteralValue(1, BigDecimal.class), BigDecimal.ZERO)
          && call.getOperandMonotonicity(2) == SqlMonotonicity.CONSTANT) {
        return mono0.unstrict();
      }
    }
    return super.getMonotonicity(call);
  }
}
