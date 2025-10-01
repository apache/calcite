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
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.Util;

import org.checkerframework.checker.nullness.qual.NonNull;

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
        ReturnTypes.LEAST_RESTRICTIVE.andThen(SqlTypeTransforms.LEAST_NULLABLE),
        null,
        OperandTypes.SAME_VARIADIC,
        SqlFunctionCategory.SYSTEM);
  }

  //~ Methods ----------------------------------------------------------------

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
      thenList.add(SqlInternalOperators.CAST_NOT_NULL.createCall(pos, SqlNode.clone(operand)));
    }
    SqlNode elseExpr = Util.last(operands);
    assert call.getFunctionQuantifier() == null;
    return SqlCase.createSwitched(pos, null, whenList, thenList, elseExpr);
  }

  @Override @NonNull public RelDataType inferReturnType(
      SqlOperatorBinding opBinding) {
    RelDataType returnType = getReturnTypeInference().inferReturnType(opBinding);
    if (returnType == null
        && opBinding instanceof SqlCallBinding
        && ((SqlCallBinding) opBinding).isTypeCoercionEnabled()) {
      SqlCallBinding callBinding = (SqlCallBinding) opBinding;
      List<RelDataType> argTypes = new ArrayList<>();
      for (SqlNode operand : callBinding.operands()) {
        RelDataType type = SqlTypeUtil.deriveType(callBinding, operand);
        argTypes.add(type);
      }
      TypeCoercion typeCoercion = callBinding.getValidator().getTypeCoercion();
      RelDataType commonType = typeCoercion.getWiderTypeFor(argTypes, true);
      if (null != commonType) {
        // COALESCE type coercion, find a common type across all branches and casts
        // operands to this common type if necessary.
        boolean coerced = typeCoercion.caseOrEquivalentCoercion(callBinding);
        if (coerced) {
          return SqlTypeUtil.deriveType(callBinding);
        }
      }
      throw callBinding.newValidationError(RESOURCE.illegalMixingOfTypes());
    }

    if (returnType == null) {
      throw opBinding.newError(
          RESOURCE.cannotInferReturnType(
              opBinding.getOperator().toString(),
              opBinding.collectOperandTypes().toString()));
    }

    return returnType;
  }

  @Override public SqlReturnTypeInference getReturnTypeInference() {
    return requireNonNull(super.getReturnTypeInference(), "returnTypeInference");
  }
}
