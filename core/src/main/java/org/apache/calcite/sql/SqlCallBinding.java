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
package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * <code>SqlCallBinding</code> implements {@link SqlOperatorBinding} by
 * analyzing to the operands of a {@link SqlCall} with a {@link SqlValidator}.
 */
public class SqlCallBinding extends SqlOperatorBinding {
  //~ Instance fields --------------------------------------------------------

  private final SqlValidator validator;
  private final SqlValidatorScope scope;
  private final SqlCall call;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a call binding.
   *
   * @param validator Validator
   * @param scope     Scope of call
   * @param call      Call node
   */
  public SqlCallBinding(
      SqlValidator validator,
      SqlValidatorScope scope,
      SqlCall call) {
    super(
        validator.getTypeFactory(),
        call.getOperator());
    this.validator = validator;
    this.scope = scope;
    this.call = call;
  }

  //~ Methods ----------------------------------------------------------------

  @Override public int getGroupCount() {
    final SelectScope selectScope =
        SqlValidatorUtil.getEnclosingSelectScope(scope);
    if (selectScope == null) {
      // Probably "VALUES expr". Treat same as "SELECT expr GROUP BY ()"
      return 0;
    }
    final SqlSelect select = selectScope.getNode();
    final SqlNodeList group = select.getGroup();
    if (group != null) {
      int n = 0;
      for (SqlNode groupItem : group) {
        if (!(groupItem instanceof SqlNodeList)
            || ((SqlNodeList) groupItem).size() != 0) {
          ++n;
        }
      }
      return n;
    }
    return validator.isAggregate(select) ? 0 : -1;
  }

  /**
   * Returns the validator.
   */
  public SqlValidator getValidator() {
    return validator;
  }

  /**
   * Returns the scope of the call.
   */
  public SqlValidatorScope getScope() {
    return scope;
  }

  /**
   * Returns the call node.
   */
  public SqlCall getCall() {
    return call;
  }

  // implement SqlOperatorBinding
  public String getStringLiteralOperand(int ordinal) {
    SqlNode node = call.operand(ordinal);
    return SqlLiteral.stringValue(node);
  }

  // implement SqlOperatorBinding
  public int getIntLiteralOperand(int ordinal) {
    // todo: move this to SqlTypeUtil
    SqlNode node = call.operand(ordinal);
    if (node instanceof SqlLiteral) {
      SqlLiteral sqlLiteral = (SqlLiteral) node;
      return sqlLiteral.intValue(true);
    } else if (node instanceof SqlCall) {
      final SqlCall c = (SqlCall) node;
      if (c.getKind() == SqlKind.MINUS_PREFIX) {
        SqlNode child = c.operand(0);
        if (child instanceof SqlLiteral) {
          return -((SqlLiteral) child).intValue(true);
        }
      }
    }
    throw Util.newInternal("should never come here");
  }

  // implement SqlOperatorBinding
  public boolean isOperandNull(int ordinal, boolean allowCast) {
    return SqlUtil.isNullLiteral(call.operand(ordinal), allowCast);
  }

  // implement SqlOperatorBinding
  public int getOperandCount() {
    return call.getOperandList().size();
  }

  // implement SqlOperatorBinding
  public RelDataType getOperandType(int ordinal) {
    final SqlNode operand = call.operand(ordinal);
    final RelDataType type = validator.deriveType(scope, operand);
    final SqlValidatorNamespace namespace = validator.getNamespace(operand);
    if (namespace != null) {
      return namespace.getType();
    }
    return type;
  }

  public RelDataType getCursorOperand(int ordinal) {
    final SqlNode operand = call.operand(ordinal);
    if (!SqlUtil.isCallTo(operand, SqlStdOperatorTable.CURSOR)) {
      return null;
    }
    final SqlCall cursorCall = (SqlCall) operand;
    final SqlNode query = cursorCall.operand(0);
    return validator.deriveType(scope, query);
  }

  // implement SqlOperatorBinding
  public String getColumnListParamInfo(
      int ordinal,
      String paramName,
      List<String> columnList) {
    final SqlNode operand = call.operand(ordinal);
    if (!SqlUtil.isCallTo(operand, SqlStdOperatorTable.ROW)) {
      return null;
    }
    for (SqlNode id : ((SqlCall) operand).getOperandList()) {
      columnList.add(((SqlIdentifier) id).getSimple());
    }
    return validator.getParentCursor(paramName);
  }

  public CalciteException newError(
      Resources.ExInst<SqlValidatorException> e) {
    return validator.newValidationError(call, e);
  }

  /**
   * Constructs a new validation signature error for the call.
   *
   * @return signature exception
   */
  public CalciteException newValidationSignatureError() {
    return validator.newValidationError(call,
        RESOURCE.canNotApplyOp2Type(getOperator().getName(),
            call.getCallSignature(validator, scope),
            getOperator().getAllowedSignatures()));
  }

  /**
   * Constructs a new validation error for the call. (Do not use this to
   * construct a validation error for other nodes such as an operands.)
   *
   * @param ex underlying exception
   * @return wrapped exception
   */
  public CalciteException newValidationError(
      Resources.ExInst<SqlValidatorException> ex) {
    return validator.newValidationError(call, ex);
  }
}

// End SqlCallBinding.java
