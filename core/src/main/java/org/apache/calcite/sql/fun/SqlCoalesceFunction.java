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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

import static java.util.Objects.requireNonNull;

/**
 * The <code>COALESCE</code> function.
 */
public class SqlCoalesceFunction extends SqlFunction {
  //~ Constructors -----------------------------------------------------------

  public SqlCoalesceFunction() {
    // NOTE jvs 26-July-2006:  We fill in the type strategies here,
    // but normally they are not used because the validator invokes
    // rewriteCall to convert COALESCE into CASE early.  However,
    // validator rewrite can optionally be disabled, in which case these
    // strategies are used.
    super("COALESCE",
        SqlKind.COALESCE,
        null,
        // Operand type inference
        // We explicitly cast each operand to the return type while inferring the return
        // type in inferReturnType, so the type of each operand is the return type.
        InferTypes.RETURN_TYPE,
        null,
        SqlFunctionCategory.SYSTEM);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public RelDataType deriveType(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    // Do not try to derive the types of the operands. We will do that
    // later, top down, when we infer the return type
    return validateOperands(validator, scope, call);
  }

  @Override public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    // REVIEW jvs 4-June-2005:  can't these be unified?
    if (!(opBinding instanceof SqlCallBinding)) {
      return inferTypeFromOperands(opBinding);
    }
    return inferTypeFromValidator((SqlCallBinding) opBinding);
  }

  private static RelDataType inferTypeFromValidator(
      SqlCallBinding callBinding) {
    SqlCall coalesceCall = callBinding.getCall();
    SqlNodeList arglist = new SqlNodeList(coalesceCall.getOperandList(),
        coalesceCall.getParserPosition());
    ArrayList<SqlNode> nullList = new ArrayList<>();
    List<RelDataType> argTypes = new ArrayList<>();

    final RelDataTypeFactory typeFactory = callBinding.getTypeFactory();


    // Coalesce can only return null if all the input argument types are nullable
    // For example, assume we have a call like
    // Coalesce(A, B, C .... Non_null_col .. X, Y, Z)
    // If any of the columns prior to non_null_col are not null for a particular index,
    // then we select that column. If all of the columns prior to non_null_col are null and we
    // reach non_null_column, then we always select non_null_col, as it is never null.
    // Therefore, for each index of the output, we will always have a non-null value,
    // meaning the overall return type is not null.
    boolean retTypeIsNullable = true;
    for (int i = 0; i < arglist.size(); i++) {
      SqlNode node = arglist.get(i);
      RelDataType type = SqlTypeUtil.deriveType(callBinding, node);
      argTypes.add(type);
      if (!type.isNullable()) {
        retTypeIsNullable = false;
      }
      if (SqlUtil.isNullLiteral(node, false)) {
        // We need to keep track of the nulls, so we can later
        nullList.add(node);
      }
    }

    RelDataType ret = typeFactory.leastRestrictive(argTypes);
    if (null == ret) {
      boolean coerced = false;
      if (callBinding.isTypeCoercionEnabled()) {
        TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
        RelDataType commonType = typeCoercion.getWiderTypeFor(argTypes, true);
        // commonType is always with nullability as false, we do not consider the
        // nullability when deducing the common type. Use the deduced type
        // (with the correct nullability) in SqlValidator
        // instead of the commonType as the return type.
        if (null != commonType) {
          coerced = typeCoercion.coalesceCoercion(callBinding);
          if (coerced) {
            ret = SqlTypeUtil.deriveType(callBinding);
          }
        }
      }
      if (!coerced) {
        throw callBinding.newValidationError(RESOURCE.illegalMixingOfTypes());
      }
    }
    final SqlValidatorImpl validator =
        (SqlValidatorImpl) callBinding.getValidator();
    requireNonNull(ret, () -> "return type for " + callBinding);
    for (SqlNode node : nullList) {
      validator.setValidatedNodeType(node, ret);
    }
    return typeFactory.createTypeWithNullability(ret, retTypeIsNullable);
  }

  private static RelDataType inferTypeFromOperands(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final List<RelDataType> argTypes = opBinding.collectOperandTypes();
    List<RelDataType> thenTypes = new ArrayList<>();

    // Coalesce can only return null if all the input argument types are nullable
    // For example, assume we have a call like
    // Coalesce(A, B, C .... Non_null_col .. X, Y, Z)
    // If any of the columns prior to non_null_col are not null for a particular index,
    // then we select that column. If all of the columns prior to non_null_col are null and we
    // reach non_null_column, then we always select non_null_col, as it is never null.
    // Therefore, for each index of the output, we will always have a non-null value,
    // meaning the overall return type is not null.
    boolean retTypeIsNullable = true;
    for (int j = 0; j < argTypes.size(); j += 1) {
      RelDataType argType = argTypes.get(j);
      thenTypes.add(argType);
      if (!argType.isNullable()) {
        retTypeIsNullable = false;
      }
    }

    RelDataType retType = requireNonNull(
        typeFactory.leastRestrictive(thenTypes),
        () -> "Can't find leastRestrictive type for " + thenTypes);

    return typeFactory.createTypeWithNullability(retType, retTypeIsNullable);
  }

  @Override public boolean checkOperandTypes(
      SqlCallBinding callBinding,
      boolean throwOnFailure) {
    // Again, we omit checking the operand types at this stage
    // all the checking occurs in inferReturnType
    return true;
  }


  @Override public SqlOperandCountRange getOperandCountRange() {
    return SqlOperandCountRanges.any();
  }

  // override SqlOperator
  @Override public SqlNode rewriteCall(SqlValidator validator, SqlCall call) {
    validateQuantifier(validator, call); // check DISTINCT/ALL

    List<SqlNode> operands = call.getOperandList();

    if (operands.size() == 1) {
      // No CASE needed
      return operands.get(0);
    }

    SqlParserPos pos = call.getParserPosition();

    SqlNodeList whenList = new SqlNodeList(pos);
    SqlNodeList thenList = new SqlNodeList(pos);

    // todo: optimize when know operand is not null.

    for (SqlNode operand : Util.skipLast(operands)) {
      whenList.add(
          SqlStdOperatorTable.IS_NOT_NULL.createCall(pos, operand));
      thenList.add(SqlNode.clone(operand));
    }
    SqlNode elseExpr = Util.last(operands);
    assert call.getFunctionQuantifier() == null;
    return SqlCase.createSwitched(pos, null, whenList, thenList, elseExpr);
  }
}
